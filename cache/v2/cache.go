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
// differences of about 5ns are noise. Sharding (see WithShards) was added to
// address that contention; the numbers above were measured before it, with a
// single lock. Reproduce with:
//
//	go test ./cache -run '^$' -bench GetCompare -benchmem -count 5 -cpu 4
package cache

import (
	"context"
	"errors"
	"hash/maphash"
	"math"
	"math/bits"
	"runtime"
	"sync"
	"time"
)

// Cache is an in-memory key-value store whose items expire ttl after they
// were set. It is safe for concurrent use.
//
// Expiry is checked on Get against the cache's Clock. Memory of expired items
// is reclaimed gradually by Set and fully, on demand, by Cleanup or
// RunCleanup.
//
// Set sweeps the shard it writes to at most once per max(ttl, one minute),
// examining a bounded batch of items, so a Set never holds a lock for long
// regardless of the cache's size. While batches keep finding many expired
// items, following Sets keep sweeping. This reclaims memory in step with
// writes but is probabilistic: a few expired items may linger until
// overwritten, invalidated or removed by Cleanup. If memory must be
// reclaimed promptly and completely, run RunCleanup; note that Cleanup holds
// each shard's lock while it scans that whole shard.
//
// Items are spread over independent shards by a hash of the key, each with
// its own lock, so goroutines working with different keys rarely contend.
// See WithShards.
type Cache[K comparable, V any] struct {
	shards []shard[K, V]
	mask   uint64 // len(shards)-1; len(shards) is a power of two
	seed   maphash.Seed
	ttl    time.Duration
	clock  Clock

	shardCap int // max items per shard; 0 means unlimited

	callsMu sync.Mutex
	calls   map[K]*cacheCall[V] // in-flight GetOrSet loads
}

// shard is an independently locked part of a Cache.
type shard[K comparable, V any] struct {
	mu        sync.RWMutex
	items     map[K]Item[V]
	nextSweep int64 // clock time when Set should next sweep this shard

	// Padding keeps neighboring shards' locks off a shared CPU cache line,
	// so taking one lock doesn't slow down readers of another.
	_ [64]byte
}

// shardsPerProc is how many default shards there are per GOMAXPROCS. Lock
// contention falls roughly in proportion to 1/shards: with 4 goroutines Get
// keeps getting faster up to about 64 shards per P and writes beyond that.
const shardsPerProc = 64

// maxDefaultShards caps the default shard count on machines with many CPUs,
// bounding the memory of an empty cache (about 155 bytes per shard).
const maxDefaultShards = 1024

// defaultShards returns the smallest power of two that is at least
// shardsPerProc times GOMAXPROCS, capped at maxDefaultShards.
func defaultShards() int {
	return min(nextPowerOfTwo(shardsPerProc*runtime.GOMAXPROCS(0)), maxDefaultShards)
}

// nextPowerOfTwo returns the smallest power of two that is at least n, or 1
// if n is not positive.
func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	return 1 << bits.Len(uint(n-1))
}

// cacheCall is a GetOrSet load that other callers for the same key wait on.
type cacheCall[V any] struct {
	done  chan struct{}
	value V
	err   error
}

// sweepBatch is how many items a single Set examines when it sweeps a
// shard. It bounds how long a Set holds the shard's lock, however big the
// shard is.
const sweepBatch = 256

// sweepAgainRatio: if at least 1/sweepAgainRatio of a full batch was expired,
// the shard likely holds many more expired items, so the next Set sweeps
// again instead of waiting for the next interval.
const sweepAgainRatio = 4

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
	clock      Clock
	shards     int
	maxEntries int
}

// WithMaxEntries bounds the number of items the cache holds to n. Without
// it the cache grows without limit, which is risky when keys come from
// untrusted input. A non-positive n means no limit.
//
// The limit is enforced per shard: each shard holds at most n divided by the
// number of shards, so the cache as a whole never exceeds n, but with an
// uneven spread of keys a shard may start evicting before the cache is full.
// To keep shards from getting too small, a bounded cache uses fewer shards
// than WithShards or the default would give, if needed, so that each shard
// holds at least 64 items; a cache bounded to fewer than 128 items has a
// single shard. Small bounded caches therefore trade some parallelism for
// precise and cheap eviction.
//
// When a new key is stored in a full shard, the cache evicts a small batch
// of items at once: up to 8, but at most 1/16 of the shard's capacity, so
// shards that hold fewer than 32 items evict one at a time. It samples four
// random items per item to evict, removes every expired one in the sample,
// and fills the rest of the batch with the live sampled items that would
// expire soonest, treating items without a ttl as expiring last. Batching
// makes eviction cheaper, at the cost of a full shard briefly holding a few
// items fewer than its limit.
// This is an approximation, not LRU: reads don't affect what gets evicted,
// which keeps Get free of writes. With a single ttl for all items it behaves
// close to evicting the oldest item. Overwriting an existing key never
// evicts.
func WithMaxEntries(n int) Option {
	return func(o *options) {
		o.maxEntries = max(n, 0)
	}
}

// Eviction removes items in batches: starting a map iteration costs about
// as much as visiting three or four items, so paying it once per batch
// instead of once per evicted item makes eviction much cheaper.
const (
	// maxEvictBatch is the most items one eviction removes.
	maxEvictBatch = 8
	// evictBatchDivisor keeps a batch at most 1/evictBatchDivisor of the
	// shard capacity, so small shards evict one item at a time and a
	// shard never drops far below its limit.
	evictBatchDivisor = 16
	// evictSamplePerItem is how many items are sampled per item evicted.
	// Picking the soonest-expiring quarter of a random sample keeps eviction
	// close to "soonest expiry first" without scanning the shard.
	evictSamplePerItem = 4
	// maxEvictSample bounds the sample and sizes the candidate buffer.
	maxEvictSample = maxEvictBatch * evictSamplePerItem
)

// minBoundedShardCap is the fewest items a shard of a bounded cache is made
// to hold. Smaller shards evict one item at a time instead of in batches, and
// with only a few items per shard an uneven spread of keys makes shards
// evict long before the cache as a whole is full.
const minBoundedShardCap = 64

// evictBatch returns how many items to evict at once from a full shard with
// the given capacity.
func evictBatch(shardCap int) int {
	return max(1, min(maxEvictBatch, shardCap/evictBatchDivisor))
}

// WithShards sets the number of shards the cache is split into, rounded up to
// a power of two. A non-positive n is ignored.
//
// Each shard has its own lock. Even a read lock writes to shared memory, so
// goroutines that touch the same shard at the same time slow each other
// down, and the chance of that falls roughly in proportion to 1/shards. The
// default is 64 times GOMAXPROCS at the time New is called, rounded up to a
// power of two and capped at 1024: 64 shards with GOMAXPROCS=1, 256 with 4,
// 1024 with 16 or more. WithMaxEntries may lower the count further.
//
// Measured with 4 goroutines on 4 cores, in ns/op:
//
//	shards   Get   90% Get + 10% Set   Set
//	     4  53.8                86.2   183.8
//	    16  27.4                78.7   102.9
//	    64  19.7                63.2    72.5
//	   256  18.0                52.0    57.1
//	  1024  18.3                40.5    51.9
//
// How to choose:
//
//   - Used by one goroutine at a time: WithShards(1). Any other count only
//     adds the cost of hashing the key, about 5-10ns per operation.
//   - Many goroutines, mostly reads: the default. Get stops improving past
//     about 64 shards per core actually used.
//   - Many goroutines with frequent writes: consider more, up to 1024.
//     Writes hold the lock exclusively and keep benefiting from more shards.
//   - Many small caches (say, one per client): fewer, such as 4-16. An empty
//     shard costs about 155 bytes, so an empty cache with 1024 shards takes
//     about 155KB and a thousand of them 155MB; for a cache of 1000 items,
//     256 shards nearly double its memory compared to 16.
//
// Too few shards make goroutines queue on the same locks, which shows up as
// Get and Set latency growing with the number of goroutines. Too many waste
// memory on small caches and spread a bounded cache thin (see
// WithMaxEntries); they don't slow down operations.
func WithShards(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.shards = n
		}
	}
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
	if o.shards == 0 {
		o.shards = defaultShards()
	}
	n := nextPowerOfTwo(o.shards)
	if o.maxEntries > 0 {
		// Keep shards big enough to evict in batches and to even out an
		// uneven spread of keys.
		for n > 1 && o.maxEntries/n < minBoundedShardCap {
			n /= 2
		}
	}
	c := &Cache[K, V]{
		shards: make([]shard[K, V], n),
		mask:   uint64(n - 1),
		seed:   maphash.MakeSeed(),
		ttl:    ttl,
		clock:  o.clock,
		calls:  make(map[K]*cacheCall[V]),
	}
	if o.maxEntries > 0 {
		c.shardCap = o.maxEntries / n
	}
	for i := range c.shards {
		c.shards[i].items = make(map[K]Item[V])
	}
	return c
}

// shardFor returns the shard that holds key.
func (c *Cache[K, V]) shardFor(key K) *shard[K, V] {
	if c.mask == 0 {
		return &c.shards[0]
	}
	return &c.shards[maphash.Comparable(c.seed, key)&c.mask]
}

// Get returns the item stored under key. It reports false if the key is
// missing or the item has expired. Concurrent Gets don't block each other.
//
// Because the clock is coarse, an item may still be returned for up to one
// clock tick after its ttl has passed.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	s := c.shardFor(key)
	s.mu.RLock()
	defer s.mu.RUnlock()
	// The clock is read only for items that can expire.
	if item, ok := s.items[key]; ok && (item.expiresAt == 0 || c.clock.Nanotime() < item.expiresAt) {
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
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	now := c.clock.Nanotime()
	if now >= s.nextSweep && !s.sweepSome(now) {
		s.nextSweep = addNanos(now, max(c.ttl, minSweepInterval))
	}
	item := Item[V]{Value: value}
	if ttl > 0 {
		item.expiresAt = addNanos(now, ttl)
	}
	// Check the length first: looking the key up costs a map access, and
	// it only matters once the shard is full.
	if c.shardCap > 0 && len(s.items) >= c.shardCap {
		if _, exists := s.items[key]; !exists {
			for len(s.items) >= c.shardCap {
				s.evict(now, evictBatch(c.shardCap))
			}
		}
	}
	s.items[key] = item
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
	s := c.shardFor(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, key)
}

// Cleanup removes all expired items and returns how many were removed.
//
// Expired items are never returned by Get, but they stay in memory until
// they are overwritten, invalidated, swept by a later Set or removed by
// Cleanup. Set already sweeps periodically, so calling Cleanup is only
// needed to reclaim memory sooner or when the cache is no longer written to.
func (c *Cache[K, V]) Cleanup() int {
	removed := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.Lock()
		removed += s.sweep(c.clock.Nanotime())
		s.mu.Unlock()
	}
	return removed
}

// sweepSome examines up to sweepBatch items and deletes those expired at
// clock time now. It reports whether the shard likely has many more expired
// items, in which case the caller should sweep again soon. Go randomizes the
// starting point of map iteration, so repeated calls examine different
// items. The caller must hold s.mu.
func (s *shard[K, V]) sweepSome(now int64) (more bool) {
	scanned, removed := 0, 0
	for key, item := range s.items {
		if scanned == sweepBatch {
			break
		}
		scanned++
		if !item.alive(now) {
			delete(s.items, key)
			removed++
		}
	}
	return scanned == sweepBatch && removed*sweepAgainRatio >= scanned
}

// evict removes up to batch items, and at least one, to make room for new
// ones. It samples up to batch*evictSamplePerItem items, removing every
// expired one it sees; if that frees fewer than batch slots, it also removes
// the live sampled items that would expire soonest, treating items without a
// ttl as expiring last. Go randomizes the starting point of map iteration,
// so the sample differs between calls. The caller must hold s.mu, s.items
// must not be empty, and batch must be in [1, maxEvictBatch].
func (s *shard[K, V]) evict(now int64, batch int) {
	type candidate struct {
		key K
		exp int64
	}
	var (
		buf     [maxEvictSample]candidate // on the stack: no allocation
		live    = buf[:0]
		removed int
		sampled int
	)
	for key, item := range s.items {
		if sampled == batch*evictSamplePerItem {
			break
		}
		sampled++
		if !item.alive(now) {
			delete(s.items, key)
			removed++
			continue
		}
		exp := item.expiresAt
		if exp == 0 {
			exp = math.MaxInt64 // never expires: evict last
		}
		live = append(live, candidate{key, exp})
	}

	// Remove the soonest-expiring live candidates by partial selection
	// sort: the sample is at most maxEvictSample items, so this is cheap.
	for i := 0; removed < batch && i < len(live); i++ {
		minIdx := i
		for j := i + 1; j < len(live); j++ {
			if live[j].exp < live[minIdx].exp {
				minIdx = j
			}
		}
		live[i], live[minIdx] = live[minIdx], live[i]
		delete(s.items, live[i].key)
		removed++
	}
}

// sweep deletes all items expired at clock time now. The caller must hold
// s.mu.
func (s *shard[K, V]) sweep(now int64) int {
	removed := 0
	for key, item := range s.items {
		if !item.alive(now) {
			delete(s.items, key)
			removed++
		}
	}
	return removed
}

// len returns the number of items in the cache, expired or not.
func (c *Cache[K, V]) len() int {
	n := 0
	for i := range c.shards {
		s := &c.shards[i]
		s.mu.RLock()
		n += len(s.items)
		s.mu.RUnlock()
	}
	return n
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
