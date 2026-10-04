package result_test

import (
	"fmt"
	"strconv"

	"go.xchunk.org/anvil/v2/result"
)

func ExampleOf() {
	r := result.Of(strconv.Atoi("12"))
	doubled := result.Map(r, func(n int) int { return n * 2 })

	v, err := doubled.Value()
	fmt.Println(v, err)

	fmt.Println(result.Of(strconv.Atoi("x")).UnwrapOr(-1))
	// Output:
	// 24 <nil>
	// -1
}
