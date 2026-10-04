package cache

import (
	"strconv"
	"testing"
	"time"
)

// getNoTTL mirrors Get but skips the expiry check.
func (c *Cache[K, V]) getNoTTL(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if item, ok := c.items[key]; ok {
		return item.Value, true
	}
	var zero V
	return zero, false
}

const benchKeys = 1024

func newBenchCache(ttl time.Duration) (*Cache[string, int], []string) {
	c := New[string, int](ttl)
	keys := make([]string, benchKeys)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
		c.Set(keys[i], i)
	}
	return c, keys
}

func BenchmarkGet(b *testing.B) {
	cases := []struct {
		name string
		ttl  time.Duration
	}{{"WithExpiry", time.Hour}, {"NoExpiry", 0}}
	for _, tc := range cases {
		b.Run("TTLCheck/"+tc.name, func(b *testing.B) {
			c, keys := newBenchCache(tc.ttl)
			for i := 0; b.Loop(); i++ {
				c.Get(keys[i&(benchKeys-1)])
			}
		})
		b.Run("NoTTLCheck/"+tc.name, func(b *testing.B) {
			c, keys := newBenchCache(tc.ttl)
			for i := 0; b.Loop(); i++ {
				c.getNoTTL(keys[i&(benchKeys-1)])
			}
		})
	}
}

func BenchmarkGetParallel(b *testing.B) {
	c, keys := newBenchCache(time.Hour)
	b.Run("TTLCheck", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				c.Get(keys[i&(benchKeys-1)])
			}
		})
	})
	b.Run("NoTTLCheck", func(b *testing.B) {
		b.RunParallel(func(pb *testing.PB) {
			for i := 0; pb.Next(); i++ {
				c.getNoTTL(keys[i&(benchKeys-1)])
			}
		})
	})
}
