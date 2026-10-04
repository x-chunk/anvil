package pipes_test

import (
	"context"
	"fmt"
	"strconv"

	"go.xchunk.org/anvil/v2/pipes"
)

func ExampleNewPipe() {
	in, out := make(chan int), make(chan int)
	p := pipes.NewPipe(in, out, pipes.WithMiddleware(func(v int) int { return v * 10 }))
	go p.Start(context.Background())

	go func() {
		in <- 1
		in <- 2
		close(in)
	}()

	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 10
	// 20
}

func ExampleNewTransformPipe() {
	in, out := make(chan int), make(chan string)
	p := pipes.NewTransformPipe(in, out,
		pipes.WithTransformMiddleware(func(v int) string { return "#" + strconv.Itoa(v) }),
	)
	go p.Start(context.Background())

	go func() {
		in <- 7
		close(in)
	}()

	fmt.Println(<-out)
	// Output: #7
}

func ExampleWithMiddlewareErr() {
	in, out := make(chan string), make(chan int)
	p := pipes.NewTransformPipe(in, out,
		pipes.WithTransformMiddlewareErr(func(_ context.Context, s string) (int, error) {
			return strconv.Atoi(s)
		}),
		pipes.WithTransformErrorHandler[string, int](func(err error) {
			fmt.Println("dropped:", err)
		}),
	)
	go p.Start(context.Background())

	go func() {
		in <- "1"
		in <- "oops"
		in <- "3"
		close(in)
	}()

	for v := range out {
		fmt.Println(v)
	}
	// Output:
	// 1
	// dropped: strconv.Atoi: parsing "oops": invalid syntax
	// 3
}
