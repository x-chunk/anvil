// Package pipes connects channels through a processing step.
//
// A Pipe[In, Out] reads values from an input channel, passes them through a
// middleware and writes the results to an output channel. Without a
// middleware it forwards values unchanged, which requires In and Out to be
// the same type.
//
//	in, out := make(chan int), make(chan string)
//	p := pipes.NewPipe(in, out,
//		pipes.WithMiddleware(func(_ context.Context, v int) (string, error) {
//			return strconv.Itoa(v), nil
//		}),
//		pipes.WithConcurrency[int, string](4),
//	)
//	go p.Start(ctx)
//
// Options that don't mention the value types in their arguments, like
// WithConcurrency, need them spelled out as shown above.
//
// Start runs until the input channel is closed or the context is done and
// then closes the output channel. Write and Read are convenience wrappers
// around the channels that honor Pause and Resume.
//
// Processing is sequential unless WithWorkerPool or WithConcurrency is
// used; the concurrent modes don't preserve order. A middleware that returns
// an error drops the value, and WithErrorHandler receives that error.
package pipes
