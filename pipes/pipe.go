package pipes

import (
	"context"
	"sync"

	"github.com/x-chunk/anvil"
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
	for {
		select {
		case v, ok := <-p.in:
			if !ok {
				close(p.out)
				return nil
			}

			if p.middleware == nil {
				p.out <- v
				continue
			}

			if !p.isAsync {
				p.middleware(v)
				p.out <- v
				continue
			}

			if p.workerPool != nil {
				resultCh := make(chan anvil.Response[T])

				p.workerPool.Submit(anvil.Task[T]{
					Result: resultCh,
					Exec: func(ctx context.Context) (T, error) {
						return p.middleware(v), nil
					},
				})

				p.out <- v

				continue
			}

			go p.middleware(v)
			p.out <- v
		case <-ctx.Done():
			close(p.out)
			return ctx.Err()
		}
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

func (p *Pipe[T]) Lock() {
	p.locked.mu.Lock()
	defer p.locked.mu.Unlock()

	p.locked.is = true
	p.locked.cond.Broadcast()
}

func (p *Pipe[T]) Unlock() {
	p.locked.mu.Lock()
	defer p.locked.mu.Unlock()

	p.locked.is = false
	p.locked.cond.Broadcast()
}

func (p *TransformPipe[In, Out]) Start(ctx context.Context) error {
	for {
		select {
		case v, ok := <-p.in:
			if !ok {
				close(p.out)
				return nil
			}

			p.out <- p.middleware(v)
		case <-ctx.Done():
			close(p.in)
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

func (p *TransformPipe[In, Out]) Lock() {
	p.locked.mu.Lock()
	defer p.locked.mu.Unlock()

	p.locked.is = true
	p.locked.cond.Broadcast()
}

func (p *TransformPipe[In, Out]) Unlock() {
	p.locked.mu.Lock()
	defer p.locked.mu.Unlock()

	p.locked.is = false
	p.locked.cond.Broadcast()
}

func (l *Locked) Wait() {
	l.mu.Lock()
	defer l.mu.Unlock()

	for l.is {
		l.cond.Wait()
	}
}
