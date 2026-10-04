package pipes

import "go.xchunk.org/anvil/v2/worker"

// Option configures a Pipe.
//
// Options that don't mention In or Out in their arguments, such as
// WithConcurrency, need the type arguments spelled out:
// WithConcurrency[int, string](4).
type Option[In any, Out any] func(*Pipe[In, Out])

// WithAsync makes a Pipe run its middleware in a separate goroutine per
// value. Results are sent to out as they complete, so order is not kept.
// It has no effect without a middleware.
func WithAsync[In any, Out any]() Option[In, Out] {
	return func(p *Pipe[In, Out]) {
		p.isAsync = true
	}
}

// WithWorkerPool runs the middleware of an async Pipe on wp instead of on
// new goroutines. It only takes effect together with WithAsync.
//
// WithWorkerPool and WithConcurrency are alternatives: if both are given,
// the last one wins.
func WithWorkerPool[In any, Out any](wp *worker.Pool[Out]) Option[In, Out] {
	return func(p *Pipe[In, Out]) {
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
func WithConcurrency[In any, Out any](n int) Option[In, Out] {
	if n <= 0 {
		panic("pipes: concurrency must be positive")
	}
	return func(p *Pipe[In, Out]) {
		p.isAsync = true
		p.concurrency = n
		p.workerPool = nil
	}
}

// WithMiddleware sets the function that converts every value of a Pipe.
//
// WithMiddleware and WithMiddlewareErr are alternatives: if both are given,
// the last one wins.
func WithMiddleware[In any, Out any](mw Middleware[In, Out]) Option[In, Out] {
	return func(p *Pipe[In, Out]) {
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
func WithMiddlewareErr[In any, Out any](mw ErrMiddleware[In, Out]) Option[In, Out] {
	return func(p *Pipe[In, Out]) {
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
func WithErrorHandler[In any, Out any](fn func(error)) Option[In, Out] {
	return func(p *Pipe[In, Out]) {
		p.onError = fn
	}
}
