package pipes

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"go.xchunk.org/anvil/v2/worker"
)

// Pipe reads values of type In from a channel, runs them through a
// middleware and writes the results of type Out to another channel.
type Pipe[In any, Out any] struct {
	in  chan In
	out chan Out

	middleware Middleware[In, Out]
	onError    func(error)
	workerPool *worker.Pool[Out]
	// concurrency limits in-flight middleware calls when there is no worker
	// pool; 0 means the pipe is sequential unless workerPool is set.
	concurrency int

	locked Locked
}

// ErrMiddlewareRequired is returned by Start when a Pipe has no
// middleware and its input and output types differ, so there is no way to
// convert the values.
var ErrMiddlewareRequired = errors.New("pipes: middleware is required when input and output types differ")

// Middleware converts a value of a pipe. It receives the context passed to
// Start. A value for which it returns an error is dropped instead of being
// sent to out, and the error is passed to the handler set with
// WithErrorHandler, if any. In async modes it may be called from several
// goroutines at once.
type Middleware[In any, Out any] func(ctx context.Context, v In) (Out, error)

// Locked is the pause state shared by Read and Write. Its zero value is
// ready to use.
type Locked struct {
	once sync.Once
	cond *sync.Cond
	mu   sync.Mutex
	is   bool
}

func (l *Locked) init() {
	l.once.Do(func() { l.cond = sync.NewCond(&l.mu) })
}

// NewPipe creates a Pipe that moves values from in to out. Without a
// middleware it forwards values unchanged, which requires In and Out to be
// the same type.
func NewPipe[In any, Out any](in chan In, out chan Out, opts ...Option[In, Out]) *Pipe[In, Out] {
	pipe := &Pipe[In, Out]{
		in:  in,
		out: out,
	}

	for _, opt := range opts {
		opt(pipe)
	}

	return pipe
}

// Start processes values until in is closed (returns nil) or ctx is done
// (returns ctx.Err()), then waits for work still in flight and closes out.
// It blocks, so run it in its own goroutine.
//
// By default values are processed one at a time, in order. In async modes
// (WithWorkerPool, WithConcurrency) middleware calls overlap and
// results reach out in completion order, not input order.
//
// Start also returns early with the pool's error if a worker pool set with
// WithWorkerPool has been shut down, and with ErrMiddlewareRequired if the
// pipe has no middleware and In differs from Out.
func (p *Pipe[In, Out]) Start(ctx context.Context) error {
	hasMiddleware := p.middleware != nil
	async := p.workerPool != nil || p.concurrency > 0

	process := p.middleware
	if process == nil {
		if reflect.TypeFor[In]() != reflect.TypeFor[Out]() {
			close(p.out)
			return ErrMiddlewareRequired
		}
		// Without a middleware values are forwarded as they are, which is
		// only possible when both channels carry the same type.
		process = func(_ context.Context, v In) (Out, error) {
			out, _ := any(v).(Out)
			return out, nil
		}
	}

	// handle reports whether the value should be forwarded.
	handle := func(err error) bool {
		if err == nil {
			return true
		}
		if p.onError != nil {
			p.onError(err)
		}
		return false
	}

	var sem chan struct{}
	if p.concurrency > 0 {
		sem = make(chan struct{}, p.concurrency)
	}

	var inflight sync.WaitGroup
	defer func() {
		inflight.Wait()
		close(p.out)
	}()

	for {
		select {
		case v, ok := <-p.in:
			if !ok {
				return nil
			}

			if !async || !hasMiddleware {
				out, err := process(ctx, v)
				if !handle(err) {
					continue
				}
				if !send(ctx, p.out, out) {
					return ctx.Err()
				}
				continue
			}

			if p.workerPool != nil {
				// Buffered so the worker never blocks on a result nobody waits for.
				resultCh := make(chan worker.Response[Out], 1)

				err := p.workerPool.SubmitCtx(ctx, worker.Task[Out]{
					Result: resultCh,
					Exec: func(ctx context.Context) (Out, error) {
						return process(ctx, v)
					},
				})
				if err != nil {
					return err
				}

				inflight.Add(1)
				go func() {
					defer inflight.Done()
					select {
					case res := <-resultCh:
						if handle(res.Err) {
							send(ctx, p.out, res.Value)
						}
					case <-ctx.Done():
					}
				}()

				continue
			}

			if sem != nil {
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			inflight.Add(1)
			go func() {
				defer inflight.Done()
				if sem != nil {
					defer func() { <-sem }()
				}
				out, err := process(ctx, v)
				if handle(err) {
					send(ctx, p.out, out)
				}
			}()
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// send delivers v to ch unless ctx is cancelled first.
func send[T any](ctx context.Context, ch chan<- T, v T) bool {
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}

// Read receives the next result from out. It blocks while the pipe is
// paused (see Pause); ok is false once out is closed. Code that must not be
// affected by Pause can use the channels directly.
func (p *Pipe[In, Out]) Read() (Out, bool) {
	p.locked.Wait()

	v, ok := <-p.out
	return v, ok
}

// Write sends v to the pipe's input. It blocks while the pipe is paused (see
// Pause). Code that must not be affected by Pause can use the channels
// directly.
func (p *Pipe[In, Out]) Write(v In) {
	p.locked.Wait()
	p.in <- v
}

// Pause makes Read and Write block until Resume is called. The processing
// done by Start and direct use of the channels are not affected.
func (p *Pipe[In, Out]) Pause() {
	p.locked.set(true)
}

// Resume releases Read and Write calls blocked by Pause.
func (p *Pipe[In, Out]) Resume() {
	p.locked.set(false)
}

// Lock pauses the pipe.
//
// Deprecated: use Pause. Despite the name this is not a mutual-exclusion
// lock, so it doesn't behave like sync.Locker.
func (p *Pipe[In, Out]) Lock() { p.Pause() }

// Unlock resumes the pipe.
//
// Deprecated: use Resume.
func (p *Pipe[In, Out]) Unlock() { p.Resume() }

func (l *Locked) set(paused bool) {
	l.init()
	l.mu.Lock()
	defer l.mu.Unlock()

	l.is = paused
	l.cond.Broadcast()
}

func (l *Locked) Wait() {
	l.init()
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.is {
		l.cond.Wait()
	}
}
