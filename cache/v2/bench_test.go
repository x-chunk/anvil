package cache

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkGet(b *testing.B) {
	c := New[string, int](time.Hour)
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
		c.Set(keys[i], i)
	}
	b.Run("Serial", func(b *testing.B) {
		for i := 0; b.Loop(); i++ {
			c.Get(keys[i&1023])
		}
	})
	b.Run("Parallel", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				c.Get(keys[i&1023])
			}
		})
	})
}
