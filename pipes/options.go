package pipes

import "go.xchunk.org/anvil"

// TransformPipeOption configures a TransformPipe.
type TransformPipeOption[In any, Out any] func(*TransformPipe[In, Out])

// PipeOption configures a Pipe.
type PipeOption[T any] func(*Pipe[T])

// forPipe adapts an option of the underlying TransformPipe[T, T] to a Pipe.
func forPipe[T any](opt TransformPipeOption[T, T]) PipeOption[T] {
	return func(p *Pipe[T]) { opt(p.TransformPipe) }
}

// WithAsync makes a Pipe run its middleware in a separate goroutine per
// value. Results are sent to out as they complete, so order is not kept.
// It has no effect without a middleware.
func WithAsync[T any]() PipeOption[T] {
	return forPipe(WithTransformAsync[T, T]())
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
	return forPipe(WithTransformWorkerPool[T, T](wp))
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
	return forPipe(WithTransformConcurrency[T, T](n))
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
	return forPipe(WithTransformMiddleware(mw))
}

// WithTransformMiddleware sets the function that converts every value of a
// TransformPipe.
//
// WithMiddleware and WithMiddlewareErr are alternatives: if both are given,
// the last one wins.
func WithTransformMiddleware[In any, Out any](mw Middleware[In, Out]) TransformPipeOption[In, Out] {
	return func(p *TransformPipe[In, Out]) {
		p.middleware = mw
		p.errMiddleware = nil
	}
}

// WithMiddlewareErr sets a middleware that receives the context passed to
// Start and may return an error. A value that fails is dropped instead of
// being sent to out, and the error goes to the handler set with
// WithErrorHandler (it is ignored if there is none).
//
// WithMiddleware and WithMiddlewareErr are alternatives: if both are given,
// the last one wins.
func WithMiddlewareErr[T any](mw ErrMiddleware[T, T]) PipeOption[T] {
	return forPipe(WithTransformMiddlewareErr(mw))
}

// WithTransformMiddlewareErr is WithMiddlewareErr for a TransformPipe.
func WithTransformMiddlewareErr[In any, Out any](mw ErrMiddleware[In, Out]) TransformPipeOption[In, Out] {
	return func(p *TransformPipe[In, Out]) {
		p.errMiddleware = mw
		p.middleware = nil
	}
}

// WithErrorHandler sets the function called with every error that makes a
// Pipe drop a value: errors returned by a WithMiddlewareErr middleware, and
// failures reported by a worker pool (such as a panic in the middleware).
//
// In async modes the handler may be called from several goroutines at once,
// so it must be safe for concurrent use.
func WithErrorHandler[T any](fn func(error)) PipeOption[T] {
	return forPipe(WithTransformErrorHandler[T, T](fn))
}

// WithTransformErrorHandler is WithErrorHandler for a TransformPipe.
func WithTransformErrorHandler[In any, Out any](fn func(error)) TransformPipeOption[In, Out] {
	return func(p *TransformPipe[In, Out]) {
		p.onError = fn
	}
}
