package anvil_test

import (
	"fmt"
	"strconv"

	anvil "go.xchunk.org/anvil/v2"
)

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
