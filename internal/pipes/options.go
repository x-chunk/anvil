package pipes

import "github.com/x-chunk/anvil"

type PipeOption[T any] func(*Pipe[T])

type TransformPipeOption[In any, Out any] func(*TransformPipe[In, Out])

func WithAsync[T any]() PipeOption[T] {
	return func(p *Pipe[T]) {
		p.isAsync = true
	}
}

func WithWorkerPool[T any](wp *anvil.WorkerPool[T]) PipeOption[T] {
	return func(p *Pipe[T]) {
		p.workerPool = wp
	}
}

func WithMiddleware[T any](mw Middleware[T, T]) PipeOption[T] {
	return func(p *Pipe[T]) {
		p.middleware = mw
	}
}

func WithTransformMiddleware[In any, Out any](mw Middleware[In, Out]) TransformPipeOption[In, Out] {
	return func(p *TransformPipe[In, Out]) {
		p.middleware = mw
	}
}
