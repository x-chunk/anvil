package pipes

import (
	"context"
	"errors"
	"reflect"
	"sync"

	"go.xchunk.org/anvil"
)

// TransformPipe reads values of type In from a channel, runs them through a
// middleware and writes the results of type Out to another channel.
type TransformPipe[In any, Out any] struct {
	in  chan In
	out chan Out

	middleware    Middleware[In, Out]
	errMiddleware ErrMiddleware[In, Out]
	onError       func(error)
	workerPool    *anvil.WorkerPool[Out]
	isAsync       bool
	// concurrency limits in-flight middleware calls of an async pipe that
	// has no worker pool; 0 means unlimited.
	concurrency int

	locked Locked
}

// Pipe is a TransformPipe whose input and output types are the same.
type Pipe[T any] = TransformPipe[T, T]

// ErrMiddlewareRequired is returned by Start when a TransformPipe has no
// middleware and its input and output types differ, so there is no way to
// convert the values.
var ErrMiddlewareRequired = errors.New("pipes: middleware is required when input and output types differ")

type Middleware[In any, Out any] func(v In) Out

// ErrMiddleware is a Middleware that receives the context passed to Start
// and can fail. A value for which it returns an error is dropped, and the
// error is passed to the handler set with WithErrorHandler, if any.
type ErrMiddleware[In any, Out any] func(ctx context.Context, v In) (Out, error)

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
// middleware it forwards values unchanged.
func NewPipe[T any](in chan T, out chan T, opts ...PipeOption[T]) *Pipe[T] {
	return NewTransformPipe(in, out, opts...)
}

// NewTransformPipe creates a TransformPipe that moves values from in to out,
// converting them with the configured middleware.
func NewTransformPipe[In any, Out any](in chan In, out chan Out, opts ...TransformPipeOption[In, Out]) *TransformPipe[In, Out] {
	pipe := &TransformPipe[In, Out]{
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
func (p *TransformPipe[In, Out]) Start(ctx context.Context) error {
	hasMiddleware := p.middleware != nil || p.errMiddleware != nil

	process := p.errMiddleware
	switch {
	case process != nil:
	case p.middleware != nil:
		process = func(_ context.Context, v In) (Out, error) { return p.middleware(v), nil }
	case reflect.TypeFor[In]() == reflect.TypeFor[Out]():
		// Without a middleware values are forwarded as they are, which is
		// only possible when both channels carry the same type.
		process = func(_ context.Context, v In) (Out, error) {
			out, _ := any(v).(Out)
			return out, nil
		}
	default:
		close(p.out)
		return ErrMiddlewareRequired
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

			if !p.isAsync || !hasMiddleware {
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
				resultCh := make(chan anvil.Response[Out], 1)

				err := p.workerPool.SubmitCtx(ctx, anvil.Task[Out]{
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

func (p *TransformPipe[In, Out]) Read() (Out, bool) {
	p.locked.Wait()

	v, ok := <-p.out
	return v, ok
}

func (p *TransformPipe[In, Out]) Write(v In) {
	p.locked.Wait()
	p.in <- v
}

func (p *TransformPipe[In, Out]) Pull() (Out, bool) {
	v, ok := <-p.out
	return v, ok
}

func (p *TransformPipe[In, Out]) Push(v In) {
	p.in <- v
}

// Pause makes Read and Write block until Resume is called. Pull, Push and
// the processing done by Start are not affected.
func (p *TransformPipe[In, Out]) Pause() {
	p.locked.set(true)
}

// Resume releases Read and Write calls blocked by Pause.
func (p *TransformPipe[In, Out]) Resume() {
	p.locked.set(false)
}

// Lock pauses the pipe.
//
// Deprecated: use Pause. Despite the name this is not a mutual-exclusion
// lock, so it doesn't behave like sync.Locker.
func (p *TransformPipe[In, Out]) Lock() { p.Pause() }

// Unlock resumes the pipe.
//
// Deprecated: use Resume.
func (p *TransformPipe[In, Out]) Unlock() { p.Resume() }

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
