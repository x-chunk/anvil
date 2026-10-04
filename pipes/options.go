package pipes

import "go.xchunk.org/anvil/v2/worker"

// Option configures a Pipe.
//
// Options that don't mention In or Out in their arguments, such as
// WithConcurrency, need the type arguments spelled out:
// WithConcurrency[int, string](4).
type Option[In any, Out any] func(*Pipe[In, Out])

// WithWorkerPool makes a Pipe run its middleware on wp, so several calls
// overlap and the pool bounds how many run at once. The pool can be shared
// between pipes. Results are sent to out as they complete, so order is not
// kept.
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
// n calls in flight at a time. Results are sent to out as they complete, so
// order is not kept. It panics if n is not positive.
//
// WithConcurrency and WithWorkerPool are alternatives: if both are given,
// the last one wins.
func WithConcurrency[In any, Out any](n int) Option[In, Out] {
	if n <= 0 {
		panic("pipes: concurrency must be positive")
	}
	return func(p *Pipe[In, Out]) {
		p.concurrency = n
		p.workerPool = nil
	}
}

// WithMiddleware sets the function that converts every value of a Pipe.
func WithMiddleware[In any, Out any](mw Middleware[In, Out]) Option[In, Out] {
	return func(p *Pipe[In, Out]) {
		p.middleware = mw
	}
}

// WithErrorHandler sets the function called with every error that makes a
// Pipe drop a value: errors returned by the middleware, and
// failures reported by a worker pool (such as a panic in the middleware).
//
// In async modes the handler may be called from several goroutines at once,
// so it must be safe for concurrent use.
func WithErrorHandler[In any, Out any](fn func(error)) Option[In, Out] {
	return func(p *Pipe[In, Out]) {
		p.onError = fn
	}
}
