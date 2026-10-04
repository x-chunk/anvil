package cache_test

import (
	"fmt"
	"time"

	"go.xchunk.org/anvil/v2/cache"
)

func ExampleCache() {
	c := cache.New[string, int](time.Minute)
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
	c := cache.New[string, int](time.Minute)

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
