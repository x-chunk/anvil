// Package pipes connects channels through a processing step.
//
// A Pipe[T] (a TransformPipe[T, T] with a simpler set of options) reads values from an input
// channel, passes them through an optional middleware and writes the
// results to an output channel. TransformPipe does the same while converting
// the value type.
//
//	in, out := make(chan int), make(chan string)
//	p := pipes.NewTransformPipe(in, out,
//		pipes.WithTransformMiddleware(strconv.Itoa),
//		pipes.WithTransformConcurrency[int, string](4),
//	)
//	go p.Start(ctx)
//
// Start runs until the input channel is closed or the context is done and
// then closes the output channel. Write/Read (or Push/Pull) are convenience
// wrappers around the channels; the former pair honors Pause and Resume.
//
// Processing is sequential unless WithWorkerPool or WithConcurrency
// is used; async modes don't preserve order. A middleware
// that can fail is set with WithMiddlewareErr, and WithErrorHandler receives
// the errors of the values that were dropped.
package pipes
