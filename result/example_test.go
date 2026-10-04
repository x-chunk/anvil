package result_test

import (
	"fmt"
	"strconv"

	"go.xchunk.org/anvil/v2/result"
)

func ExampleOf() {
	r := result.Of(strconv.Atoi("12"))
	doubled := r.Map(func(n int) int { return n * 2 })

	v, err := doubled.Value()
	fmt.Println(v, err)

	fmt.Println(result.Of(strconv.Atoi("x")).UnwrapOr(-1))
	// Output:
	// 24 <nil>
	// -1
}

func ExampleResult_Map() {
	n := result.Of(strconv.Atoi("12")).
		Map(func(n int) int { return n * 2 }).
		UnwrapOr(-1)
	fmt.Println(n)

	// Map can change the type, and an error skips the rest of the chain.
	s := result.Of(strconv.Atoi("x")).
		Map(func(n int) string { return strconv.Itoa(n) }).
		UnwrapOr("failed")
	fmt.Println(s)
	// Output:
	// 24
	// failed
}
