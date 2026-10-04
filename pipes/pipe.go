package pipes

import (
	"context"
	"sync"

	"go.xchunk.org/anvil"
)

type Pipe[T any] struct {
	in  chan T
	out chan T

	middleware Middleware[T, T]
	workerPool *anvil.WorkerPool[T]
	isAsync    bool

	locked Locked
}

type TransformPipe[In any, Out any] struct {
	in  chan In
	out chan Out

	middleware Middleware[In, Out]

	locked Locked
}

type Middleware[In any, Out any] func(v In) Out

type Locked struct {
	cond *sync.Cond
	mu   sync.Mutex
	is   bool
}

func NewPipe[T any](in chan T, out chan T, opts ...PipeOption[T]) *Pipe[T] {
	pipe := &Pipe[T]{
		in:  in,
		out: out,
	}

	pipe.locked.cond = sync.NewCond(&pipe.locked.mu)

	for _, opt := range opts {
		opt(pipe)
	}

	return pipe
}

func NewTransformPipe[In any, Out any](in chan In, out chan Out, opts ...TransformPipeOption[In, Out]) *TransformPipe[In, Out] {
	pipe := &TransformPipe[In, Out]{
		in:  in,
		out: out,
	}

	pipe.locked.cond = sync.NewCond(&pipe.locked.mu)

	for _, opt := range opts {
		opt(pipe)
	}

	return pipe
}

func (p *Pipe[T]) Start(ctx context.Context) error {
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

			if p.middleware == nil {
				if !send(ctx, p.out, v) {
					return ctx.Err()
				}
				continue
			}

			if !p.isAsync {
				if !send(ctx, p.out, p.middleware(v)) {
					return ctx.Err()
				}
				continue
			}

			if p.workerPool != nil {
				// Buffered so the worker never blocks on a result nobody waits for.
				resultCh := make(chan anvil.Response[T], 1)

				err := p.workerPool.SubmitCtx(ctx, anvil.Task[T]{
					Result: resultCh,
					Exec: func(ctx context.Context) (T, error) {
						return p.middleware(v), nil
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
						send(ctx, p.out, res.Value)
					case <-ctx.Done():
					}
				}()

				continue
			}

			inflight.Add(1)
			go func() {
				defer inflight.Done()
				send(ctx, p.out, p.middleware(v))
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

func (p *Pipe[T]) Read() (T, bool) {
	p.locked.Wait()

	v, ok := <-p.out
	return v, ok
}

func (p *Pipe[T]) Write(v T) {
	p.locked.Wait()
	p.in <- v
}

func (p *Pipe[T]) Pull() (T, bool) {
	v, ok := <-p.out
	return v, ok
}

func (p *Pipe[T]) Push(v T) {
	p.in <- v
}

// Pause makes Read and Write block until Resume is called. Pull, Push and
// the processing done by Start are not affected.
func (p *Pipe[T]) Pause() {
	p.locked.set(true)
}

// Resume releases Read and Write calls blocked by Pause.
func (p *Pipe[T]) Resume() {
	p.locked.set(false)
}

// Lock pauses the pipe.
//
// Deprecated: use Pause. Despite the name this is not a mutual-exclusion
// lock, so it doesn't behave like sync.Locker.
func (p *Pipe[T]) Lock() { p.Pause() }

// Unlock resumes the pipe.
//
// Deprecated: use Resume.
func (p *Pipe[T]) Unlock() { p.Resume() }

func (p *TransformPipe[In, Out]) Start(ctx context.Context) error {
	for {
		select {
		case v, ok := <-p.in:
			if !ok {
				close(p.out)
				return nil
			}

			if !send(ctx, p.out, p.middleware(v)) {
				close(p.out)
				return ctx.Err()
			}
		case <-ctx.Done():
			close(p.out)
			return ctx.Err()
		}
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
	l.mu.Lock()
	defer l.mu.Unlock()

	l.is = paused
	l.cond.Broadcast()
}

func (l *Locked) Wait() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.is {
		l.cond.Wait()
	}
}
