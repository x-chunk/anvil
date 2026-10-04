package cache

import (
	"strconv"
	"sync"
	"sync/atomic"
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

// getOptimized reads the clock only for items that can expire, and does it
// after releasing the lock.
func (c *Cache[K, V]) getOptimized(key K) (V, bool) {
	c.mu.RLock()
	item, ok := c.items[key]
	c.mu.RUnlock()
	if ok && (item.expiresAt.IsZero() || c.now().Before(item.expiresAt)) {
		return item.Value, true
	}
	var zero V
	return zero, false
}

// coarseClock is a process-wide monotonic clock refreshed by a single
// background goroutine. Its value is nanoseconds since clockBase.
var (
	clockBase  = time.Now()
	coarseNow  atomic.Int64
	clockStart sync.Once
)

const clockTick = time.Millisecond

func coarseNanos() int64 {
	clockStart.Do(func() {
		coarseNow.Store(int64(time.Since(clockBase)))
		go func() {
			for range time.Tick(clockTick) {
				coarseNow.Store(int64(time.Since(clockBase)))
			}
		}()
	})
	return coarseNow.Load()
}

// atomicCache is Cache with expiry stored as int64 coarse-clock nanos.
type atomicCache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]atomicItem[V]
}

type atomicItem[V any] struct {
	Value     V
	expiresAt int64 // zero means the item never expires
}

func newAtomicCache[K comparable, V any]() *atomicCache[K, V] {
	return &atomicCache[K, V]{items: make(map[K]atomicItem[V])}
}

func (c *atomicCache[K, V]) set(key K, value V, ttl time.Duration) {
	item := atomicItem[V]{Value: value}
	if ttl > 0 {
		item.expiresAt = coarseNanos() + int64(ttl)
	}
	c.mu.Lock()
	c.items[key] = item
	c.mu.Unlock()
}

func (c *atomicCache[K, V]) get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if item, ok := c.items[key]; ok && (item.expiresAt == 0 || coarseNanos() < item.expiresAt) {
		return item.Value, true
	}
	var zero V
	return zero, false
}

const benchKeys = 1024

type benchScenario struct {
	name string
	ttl  time.Duration
	miss bool // look up keys that were never set
}

var benchScenarios = []benchScenario{
	{name: "HitWithTTL", ttl: time.Hour},
	{name: "HitNoTTL", ttl: 0},
	{name: "Miss", ttl: time.Hour, miss: true},
}

type getFunc func(string) (int, bool)

func benchImpls(sc benchScenario) ([]string, map[string]getFunc) {
	c := New[string, int](sc.ttl)
	ac := newAtomicCache[string, int]()
	keys := make([]string, benchKeys)
	for i := range keys {
		k := "key-" + strconv.Itoa(i)
		c.Set(k, i)
		ac.set(k, i, sc.ttl)
		if sc.miss {
			k = "missing-" + strconv.Itoa(i)
		}
		keys[i] = k
	}
	return keys, map[string]getFunc{
		"1_Current":   c.Get,
		"2_NoTTL":     c.getNoTTL,
		"3_Optimized": c.getOptimized,
		"4_Atomic":    ac.get,
	}
}

var implOrder = []string{"1_Current", "2_NoTTL", "3_Optimized", "4_Atomic"}

func BenchmarkGetCompare(b *testing.B) {
	for _, sc := range benchScenarios {
		keys, impls := benchImpls(sc)
		for _, name := range implOrder {
			get := impls[name]
			b.Run(sc.name+"/"+name, func(b *testing.B) {
				for i := 0; b.Loop(); i++ {
					get(keys[i&(benchKeys-1)])
				}
			})
		}
	}
}

// BenchmarkGetCompareParallel runs GOMAXPROCS goroutines; use -cpu 4.
func BenchmarkGetCompareParallel(b *testing.B) {
	for _, sc := range benchScenarios {
		keys, impls := benchImpls(sc)
		for _, name := range implOrder {
			get := impls[name]
			b.Run(sc.name+"/"+name, func(b *testing.B) {
				b.RunParallel(func(pb *testing.PB) {
					for i := 0; pb.Next(); i++ {
						get(keys[i&(benchKeys-1)])
					}
				})
			})
		}
	}
}
