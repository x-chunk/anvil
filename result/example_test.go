package result_test

import (
	"errors"
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

func ExampleResult_OrElse() {
	cache := func(key string) result.Result[string] { return result.Err[string](errors.New("miss")) }
	db := func(key string) result.Result[string] { return result.Ok("from db: " + key) }

	r := cache("alice").OrElse(func(error) result.Result[string] { return db("alice") })
	fmt.Println(r)
	// Output: Ok(from db: alice)
}

func ExampleAll() {
	parse := func(s string) result.Result[int] { return result.Of(strconv.Atoi(s)) }

	fmt.Println(result.All(parse("1"), parse("2"), parse("3")))
	fmt.Println(result.All(parse("1"), parse("x")).IsOk())
	// Output:
	// Ok([1 2 3])
	// false
}
