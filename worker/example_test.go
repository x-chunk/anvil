package worker_test

import (
	"context"
	"fmt"

	"go.xchunk.org/anvil/v2/worker"
)

func ExamplePool() {
	wp := worker.New[int](2, 4)
	wp.Start(context.Background())
	defer wp.Shutdown()

	result := make(chan worker.Response[int], 1)
	wp.Submit(worker.Task[int]{
		Result: result,
		Exec:   func(ctx context.Context) (int, error) { return 6 * 7, nil },
	})

	res := <-result
	fmt.Println(res.Value, res.Err)
	// Output: 42 <nil>
}
