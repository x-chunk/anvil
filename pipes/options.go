package pipes

import "go.xchunk.org/anvil"

// TransformPipeOption configures a TransformPipe.
type TransformPipeOption[In any, Out any] func(*TransformPipe[In, Out])

// PipeOption configures a Pipe.
type PipeOption[T any] = TransformPipeOption[T, T]

// WithAsync makes a Pipe run its middleware in a separate goroutine per
// value. Results are sent to out as they complete, so order is not kept.
// It has no effect without a middleware.
func WithAsync[T any]() PipeOption[T] {
	return WithTransformAsync[T, T]()
}

// WithTransformAsync is WithAsync for a TransformPipe.
func WithTransformAsync[In any, Out any]() TransformPipeOption[In, Out] {
	return func(p *TransformPipe[In, Out]) {
		p.isAsync = true
	}
}

// WithWorkerPool runs the middleware of an async Pipe on wp instead of on
// new goroutines. It only takes effect together with WithAsync.
//
// WithWorkerPool and WithConcurrency are alternatives: if both are given,
// the last one wins.
func WithWorkerPool[T any](wp *anvil.WorkerPool[T]) PipeOption[T] {
	return WithTransformWorkerPool[T, T](wp)
}

// WithTransformWorkerPool is WithWorkerPool for a TransformPipe.
//
// WithWorkerPool and WithConcurrency are alternatives: if both are given,
// the last one wins.
func WithTransformWorkerPool[In any, Out any](wp *anvil.WorkerPool[Out]) TransformPipeOption[In, Out] {
	return func(p *TransformPipe[In, Out]) {
		p.workerPool = wp
		p.concurrency = 0
	}
}

// WithConcurrency makes a Pipe run its middleware concurrently, with at most
// n calls in flight at a time. It enables async mode by itself, so no
// WithAsync is needed. Results are sent to out as they complete, so order is
// not kept. It panics if n is not positive.
//
// WithConcurrency and WithWorkerPool are alternatives: if both are given,
// the last one wins.
func WithConcurrency[T any](n int) PipeOption[T] {
	return WithTransformConcurrency[T, T](n)
}

// WithTransformConcurrency is WithConcurrency for a TransformPipe.
func WithTransformConcurrency[In any, Out any](n int) TransformPipeOption[In, Out] {
	if n <= 0 {
		panic("pipes: concurrency must be positive")
	}
	return func(p *TransformPipe[In, Out]) {
		p.isAsync = true
		p.concurrency = n
		p.workerPool = nil
	}
}

// WithMiddleware sets the function applied to every value of a Pipe.
func WithMiddleware[T any](mw Middleware[T, T]) PipeOption[T] {
	return WithTransformMiddleware(mw)
}

// WithTransformMiddleware sets the function that converts every value of a
// TransformPipe.
func WithTransformMiddleware[In any, Out any](mw Middleware[In, Out]) TransformPipeOption[In, Out] {
	return func(p *TransformPipe[In, Out]) {
		p.middleware = mw
	}
}
