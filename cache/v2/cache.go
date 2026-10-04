// Package cache provides an in-memory TTL cache that is safe for concurrent
// use.
//
// It has the same API as go.xchunk.org/anvil/v2/cache, but checks expiry
// against a coarse clock instead of calling time.Now on every Get. That makes
// Get of an item with a ttl several times faster, at the cost of precision:
// an item may outlive its ttl by up to the clock's tick (DefaultTick, 1ms, for
// the shared default clock) plus scheduler and GC delays. Items never expire
// early. If you need exact expiry for very short ttls, use the v1 package or
// pass a precise Clock with WithClock.
//
// By default all caches share one process-wide clock driven by a single
// goroutine that is started with the first cache and runs until the process
// exits. No Close is needed and nothing leaks however many caches are
// created. See CoarseClock and WithClock for the details and alternatives.
//
// # Performance
//
// BenchmarkGetCompare and BenchmarkGetCompareParallel in
// go.xchunk.org/anvil/v2/cache (file get_bench_test.go) compare Get of the v1
// cache, the same Get without any expiry check, v1 with the clock read only
// for expiring items, and this package's coarse-clock approach. Mean ns/op
// over 5 runs on a 4-core cloud VM (Go 1.27, Intel Xeon 2.1GHz), where
// time.Now is unusually slow; on bare metal the gap to v1 is smaller:
//
//	Scenario             v1 Get  no check  v1 lazy clock  coarse clock
//	hit, item has ttl     103.3      28.0          110.4          28.5
//	hit, no ttl           104.3      28.1           26.5          27.8
//	miss                   28.1      26.2           25.3          26.4
//	hit, ttl, 4 gorout.   134.2     112.9          123.2         111.0
//	hit, no ttl, 4 gor.   107.4     113.9          105.2         112.9
//	miss, 4 goroutines    110.1     109.0          109.2         110.4
//
// With 4 goroutines all variants are bound by the RWMutex reader count, and
// differences of about 5ns are noise. Reproduce with:
//
//	go test ./cache -run '^$' -bench GetCompare -benchmem -count 5 -cpu 4
package cache

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// Cache is an in-memory key-value store whose items expire ttl after they
// were set. It is safe for concurrent use.
//
// Expiry is checked on Get against the cache's Clock. Memory of expired items
// is reclaimed by Set, which sweeps them at most once per max(ttl, one
// minute), and on demand by Cleanup or RunCleanup.
type Cache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]Item[V]
	ttl   time.Duration
	clock Clock

	nextSweep int64 // clock time when Set should next sweep expired items

	callsMu sync.Mutex
	calls   map[K]*cacheCall[V] // in-flight GetOrSet loads
}

// cacheCall is a GetOrSet load that other callers for the same key wait on.
type cacheCall[V any] struct {
	done  chan struct{}
	value V
	err   error
}

// minSweepInterval bounds how often Set sweeps expired items, so a very short
// ttl doesn't make every Set scan the whole map.
const minSweepInterval = time.Minute

// ErrLoaderPanicked is returned by GetOrSet to callers that waited for a load
// that panicked. The panic itself propagates in the goroutine that ran the
// load.
var ErrLoaderPanicked = errors.New("cache: loader panicked")

// Item is a single item of Cache.
type Item[V any] struct {
	Value V
	// expiresAt is the clock time at which the item expires; zero means it
	// never expires. It takes 8 bytes instead of the 24 of a time.Time,
	// which keeps the map smaller and friendlier to CPU caches.
	expiresAt int64
}

// alive reports whether the item is still valid at clock time now.
func (i Item[V]) alive(now int64) bool {
	return i.expiresAt == 0 || now < i.expiresAt
}

// Option configures a Cache created by New.
type Option func(*options)

type options struct {
	clock Clock
}

// WithClock makes the cache check expiry against clock instead of the shared
// default CoarseClock. Use it to get a different precision (for example a
// NewCoarseClock with a shorter tick, or an exact clock built on time.Now),
// to control the clock's lifetime yourself, or to control time in tests.
//
// The cache keeps using clock for its whole life; if clock is a CoarseClock
// you stop, the cache's items stop expiring. A nil clock is ignored.
func WithClock(clock Clock) Option {
	return func(o *options) {
		if clock != nil {
			o.clock = clock
		}
	}
}

// New returns an instance of Cache whose items live for ttl. A non-positive
// ttl means items never expire.
//
// Unless WithClock is given, the cache uses the shared default clock, whose
// goroutine is started by the first call to New.
func New[K comparable, V any](ttl time.Duration, opts ...Option) *Cache[K, V] {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	if o.clock == nil {
		o.clock = defaultClock()
	}
	return &Cache[K, V]{
		items: make(map[K]Item[V]),
		ttl:   ttl,
		clock: o.clock,
		calls: make(map[K]*cacheCall[V]),
	}
}

// Get returns the item stored under key. It reports false if the key is
// missing or the item has expired. Concurrent Gets don't block each other.
//
// Because the clock is coarse, an item may still be returned for up to one
// clock tick after its ttl has passed.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	// The clock is read only for items that can expire.
	if item, ok := c.items[key]; ok && (item.expiresAt == 0 || c.clock.Nanotime() < item.expiresAt) {
		return item.Value, true
	}
	var zero V
	return zero, false
}

// Set stores value under key and restarts its ttl.
func (c *Cache[K, V]) Set(key K, value V) {
	c.SetWithTTL(key, value, c.ttl)
}

// SetWithTTL stores value under key like Set, but the item lives for ttl
// instead of the cache's default. A non-positive ttl means the item never
// expires. A ttl so large that the expiry time would overflow is treated as
// never expiring too.
//
// ttls shorter than the clock's tick are honored only approximately: the item
// lives for somewhere between ttl and ttl plus one tick.
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.clock.Nanotime()
	if now >= c.nextSweep {
		c.sweep(now)
		c.nextSweep = addNanos(now, max(c.ttl, minSweepInterval))
	}
	item := Item[V]{Value: value}
	if ttl > 0 {
		item.expiresAt = addNanos(now, ttl)
	}
	c.items[key] = item
}

// addNanos returns now+d, saturating at math.MaxInt64 instead of overflowing.
// A saturated expiry is never reached, so such an item effectively never
// expires.
func addNanos(now int64, d time.Duration) int64 {
	if int64(d) > math.MaxInt64-now {
		return math.MaxInt64
	}
	return now + int64(d)
}

// GetOrSet returns the item stored under key. If there is none, it calls
// load, stores the result with the cache's default ttl and returns it.
//
// Concurrent calls for the same missing key share a single load: one
// caller runs it and the others wait for its result. An error from load is
// returned to all of them and is not cached. If load panics, the panic
// propagates in the calling goroutine and waiting callers get
// ErrLoaderPanicked.
//
// GetOrSet waits for a shared load for as long as it takes; use
// GetOrSetContext to bound the wait.
func (c *Cache[K, V]) GetOrSet(key K, load func() (V, error)) (V, error) {
	return c.GetOrSetContext(context.Background(), key, func(context.Context) (V, error) {
		return load()
	})
}

// GetOrSetContext is like GetOrSet, but the wait can be canceled. If ctx is
// done before a result is available, it returns ctx.Err(); a load already
// started by another caller keeps running and its result is still cached.
//
// The caller that runs load passes its own ctx to it. Callers waiting on that
// load get whatever it returns, so if the loading caller's ctx is canceled
// and load returns that error, the waiters get it too, even if their own
// contexts are still alive. They are free to retry.
func (c *Cache[K, V]) GetOrSetContext(ctx context.Context, key K, load func(context.Context) (V, error)) (V, error) {
	if v, ok := c.Get(key); ok {
		return v, nil
	}
	var zero V
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	c.callsMu.Lock()
	if call, ok := c.calls[key]; ok {
		c.callsMu.Unlock()
		select {
		case <-call.done:
			return call.value, call.err
		case <-ctx.Done():
			return zero, ctx.Err()
		}
	}
	call := &cacheCall[V]{done: make(chan struct{})}
	c.calls[key] = call
	c.callsMu.Unlock()

	finished := false
	defer func() {
		if !finished {
			call.err = ErrLoaderPanicked
		}
		c.callsMu.Lock()
		delete(c.calls, key)
		c.callsMu.Unlock()
		close(call.done)
	}()

	// Another call may have stored the value between our Get and
	// becoming the leader.
	if v, ok := c.Get(key); ok {
		call.value, finished = v, true
		return v, nil
	}

	call.value, call.err = load(ctx)
	finished = true
	if call.err == nil {
		c.Set(key, call.value)
	}
	return call.value, call.err
}

// Invalidate removes the item stored under key, if any.
func (c *Cache[K, V]) Invalidate(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.items, key)
}

// Cleanup removes all expired items and returns how many were removed.
//
// Expired items are never returned by Get, but they stay in memory until
// they are overwritten, invalidated, swept by a later Set or removed by
// Cleanup. Set already sweeps periodically, so calling Cleanup is only
// needed to reclaim memory sooner or when the cache is no longer written to.
func (c *Cache[K, V]) Cleanup() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.sweep(c.clock.Nanotime())
}

// sweep deletes items expired at clock time now. The caller must hold c.mu.
func (c *Cache[K, V]) sweep(now int64) int {
	removed := 0
	for key, item := range c.items {
		if !item.alive(now) {
			delete(c.items, key)
			removed++
		}
	}
	return removed
}

// RunCleanup calls Cleanup every interval until ctx is done. It blocks, so
// run it in its own goroutine. It panics if interval is not positive.
func (c *Cache[K, V]) RunCleanup(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			c.Cleanup()
		case <-ctx.Done():
			return
		}
	}
}
