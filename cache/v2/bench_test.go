package cache

import (
	"strconv"
	"testing"
	"time"
)

// Run with several GOMAXPROCS values; RunParallel starts one goroutine per P,
// so -cpu 1 is the single-threaded case:
//
//	go test ./cache/v2 -run '^$' -bench . -cpu 1,4,8,16

const benchKeyCount = 1 << 14

func benchKeys() []string {
	keys := make([]string, benchKeyCount)
	for i := range keys {
		keys[i] = "key-" + strconv.Itoa(i)
	}
	return keys
}

// benchMissKeys are never stored, built up front so lookups don't allocate.
var benchMissKeys = func() []string {
	keys := make([]string, benchKeyCount)
	for i := range keys {
		keys[i] = "missing-" + strconv.Itoa(i)
	}
	return keys
}()

// benchShardConfigs compares a single lock with the default sharding and a
// fixed, generous shard count. The default depends on GOMAXPROCS, which -cpu
// sets before each run, so the cache is created inside the sub-benchmark.
var benchShardConfigs = []struct {
	name string
	opts []Option
}{
	{"Shards1", []Option{WithShards(1)}},
	{"ShardsDefault", nil},
	{"Shards256", []Option{WithShards(256)}},
}

// benchScenarios describe the operation each goroutine repeats. op receives a
// per-goroutine counter, so goroutines walk the key space independently.
var benchScenarios = []struct {
	name string
	op   func(c *Cache[string, int], keys []string, i int)
}{
	{"GetHit", func(c *Cache[string, int], keys []string, i int) {
		c.Get(keys[i&(benchKeyCount-1)])
	}},
	{"GetMiss", func(c *Cache[string, int], keys []string, i int) {
		c.Get(benchMissKeys[i&(benchKeyCount-1)])
	}},
	{"Read90Write10", func(c *Cache[string, int], keys []string, i int) {
		k := keys[i&(benchKeyCount-1)]
		if i%10 == 0 {
			c.Set(k, i)
		} else {
			c.Get(k)
		}
	}},
	{"Set", func(c *Cache[string, int], keys []string, i int) {
		c.Set(keys[i&(benchKeyCount-1)], i)
	}},
	{"GetOrSetHit", func(c *Cache[string, int], keys []string, i int) {
		c.GetOrSet(keys[i&(benchKeyCount-1)], func() (int, error) { return i, nil })
	}},
}

func BenchmarkCache(b *testing.B) {
	keys := benchKeys()
	for _, sc := range benchScenarios {
		for _, cfg := range benchShardConfigs {
			b.Run(sc.name+"/"+cfg.name, func(b *testing.B) {
				c := New[string, int](time.Hour, cfg.opts...)
				for i, k := range keys {
					c.Set(k, i)
				}
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					// Start goroutines at different offsets so they don't
					// march over the same keys in lockstep.
					i := int(time.Now().UnixNano())
					for pb.Next() {
						sc.op(c, keys, i)
						i++
					}
				})
			})
		}
	}
}

// BenchmarkCacheMaxEntries measures Set into a full bounded cache, where
// every Set of a new key evicts one.
func BenchmarkCacheMaxEntries(b *testing.B) {
	keys := benchKeys()
	b.Run("SetEvicting", func(b *testing.B) {
		c := New[string, int](time.Hour, WithMaxEntries(benchKeyCount/4))
		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := int(time.Now().UnixNano())
			for pb.Next() {
				c.Set(keys[i&(benchKeyCount-1)], i)
				i++
			}
		})
	})
}
