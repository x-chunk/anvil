package anvil_test

import (
	"context"
	"fmt"
	"strconv"
	"time"

	anvil "go.xchunk.org/anvil/v2"
)

func ExampleCache() {
	c := anvil.NewCache[string, int](time.Minute)
	c.Set("answer", 42)

	v, ok := c.Get("answer")
	fmt.Println(v, ok)

	c.Invalidate("answer")
	_, ok = c.Get("answer")
	fmt.Println(ok)
	// Output:
	// 42 true
	// false
}

func ExampleCache_GetOrSet() {
	c := anvil.NewCache[string, int](time.Minute)

	load := func() (int, error) {
		fmt.Println("loading")
		return 7, nil
	}
	a, _ := c.GetOrSet("k", load) // loads
	b, _ := c.GetOrSet("k", load) // served from the cache
	fmt.Println(a, b)
	// Output:
	// loading
	// 7 7
}

func ExampleWorkerPool() {
	wp := anvil.NewWorkerPool[int](2, 4)
	wp.Start(context.Background())
	defer wp.Shutdown()

	result := make(chan anvil.Response[int], 1)
	wp.Submit(anvil.Task[int]{
		Result: result,
		Exec:   func(ctx context.Context) (int, error) { return 6 * 7, nil },
	})

	res := <-result
	fmt.Println(res.Value, res.Err)
	// Output: 42 <nil>
}

func ExampleOf() {
	r := anvil.Of(strconv.Atoi("12"))
	doubled := anvil.Map(r, func(n int) int { return n * 2 })

	v, err := doubled.Value()
	fmt.Println(v, err)

	fmt.Println(anvil.Of(strconv.Atoi("x")).UnwrapOr(-1))
	// Output:
	// 24 <nil>
	// -1
}
