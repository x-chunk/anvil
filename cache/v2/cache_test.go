package cache

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a Clock that only moves when Advance is called.
type fakeClock struct{ t atomic.Int64 }

func (c *fakeClock) Nanotime() int64         { return c.t.Load() }
func (c *fakeClock) Advance(d time.Duration) { c.t.Add(int64(d)) }

func newTestCache[K comparable, V any](ttl time.Duration, opts ...Option) (*Cache[K, V], *fakeClock) {
	clk := &fakeClock{}
	clk.t.Store(1_000_000 * int64(time.Second))
	return New[K, V](ttl, append([]Option{WithClock(clk)}, opts...)...), clk
}

func TestCacheGetSet(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)

	if _, ok := c.Get("a"); ok {
		t.Fatal("Get on empty cache returned ok")
	}
	c.Set("a", 1)
	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get = %d, %v; want 1, true", v, ok)
	}
	c.Set("a", 2)
	if v, _ := c.Get("a"); v != 2 {
		t.Fatalf("Get after overwrite = %d, want 2", v)
	}
}

func TestCacheExpiry(t *testing.T) {
	c, clk := newTestCache[string, int](time.Minute)
	c.Set("a", 1)

	clk.Advance(time.Minute - time.Nanosecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("item expired too early")
	}
	clk.Advance(time.Nanosecond)
	if _, ok := c.Get("a"); ok {
		t.Fatal("item should be expired at exactly ttl")
	}
}

func TestCacheInvalidate(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)
	c.Set("a", 1)
	c.Set("b", 2)

	c.Invalidate("a")
	c.Invalidate("missing") // must not panic

	if _, ok := c.Get("a"); ok {
		t.Fatal("invalidated item is still returned")
	}
	if _, ok := c.Get("b"); !ok {
		t.Fatal("unrelated item was removed")
	}
}

func TestCacheInvalidateFreesEntry(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)
	c.Set("a", 1)

	c.Invalidate("a")

	if n := c.len(); n != 0 {
		t.Fatalf("%d entries left in the map after Invalidate, want 0", n)
	}
}

func TestCacheCleanup(t *testing.T) {
	c, clk := newTestCache[string, int](time.Minute)
	c.Set("old", 1)
	clk.Advance(40 * time.Second)
	c.Set("fresh", 2)
	clk.Advance(30 * time.Second) // old: 70s, fresh: 30s

	if n := c.Cleanup(); n != 1 {
		t.Fatalf("Cleanup removed %d items, want 1", n)
	}
	if c.len() != 1 {
		t.Fatalf("%d items left, want 1", c.len())
	}
	if _, ok := c.Get("fresh"); !ok {
		t.Fatal("fresh item was removed")
	}
	if n := c.Cleanup(); n != 0 {
		t.Fatalf("second Cleanup removed %d items, want 0", n)
	}
}

func TestCacheRunCleanup(t *testing.T) {
	c := New[string, int](5 * time.Millisecond) // shared coarse clock
	c.Set("a", 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.RunCleanup(ctx, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		n := c.len()
		if n == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("expired item was never cleaned up")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunCleanup did not stop after cancel")
	}
}

func TestCacheSetWithTTL(t *testing.T) {
	c, clk := newTestCache[string, int](time.Minute)
	c.SetWithTTL("short", 1, 10*time.Second)
	c.SetWithTTL("long", 2, time.Hour)
	c.Set("default", 3)

	clk.Advance(30 * time.Second)
	if _, ok := c.Get("short"); ok {
		t.Fatal("short item should have expired after its own ttl")
	}
	if _, ok := c.Get("default"); !ok {
		t.Fatal("default item expired too early")
	}

	clk.Advance(time.Minute) // 90s in total
	if _, ok := c.Get("default"); ok {
		t.Fatal("default item should be expired")
	}
	if _, ok := c.Get("long"); !ok {
		t.Fatal("long item should outlive the cache default")
	}
}

func TestCacheGetOrSet(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)
	loads := 0
	load := func() (int, error) { loads++; return 7, nil }

	for i := 0; i < 3; i++ {
		if v, err := c.GetOrSet("a", load); v != 7 || err != nil {
			t.Fatalf("GetOrSet = %d, %v", v, err)
		}
	}
	if loads != 1 {
		t.Fatalf("load ran %d times, want 1", loads)
	}
	if v, ok := c.Get("a"); !ok || v != 7 {
		t.Fatal("GetOrSet did not store the value")
	}
}

func TestCacheGetOrSetErrorIsNotCached(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)

	_, err := c.GetOrSet("a", func() (int, error) { return 0, errTest })
	if !errors.Is(err, errTest) {
		t.Fatalf("err = %v, want errTest", err)
	}
	if _, ok := c.Get("a"); ok {
		t.Fatal("failed load was cached")
	}
	if v, err := c.GetOrSet("a", func() (int, error) { return 1, nil }); v != 1 || err != nil {
		t.Fatalf("retry = %d, %v; want 1, nil", v, err)
	}
}

func TestCacheGetOrSetSharesConcurrentLoads(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)

	var loads atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	load := func() (int, error) {
		if loads.Add(1) == 1 {
			close(started)
		}
		<-release
		return 5, nil
	}

	const n = 10
	var wg sync.WaitGroup
	results := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := c.GetOrSet("k", load)
			results <- v
		}()
	}
	<-started
	time.Sleep(50 * time.Millisecond) // let the others queue up behind the loader
	close(release)
	wg.Wait()
	close(results)

	if got := loads.Load(); got != 1 {
		t.Fatalf("load ran %d times, want 1", got)
	}
	for v := range results {
		if v != 5 {
			t.Fatalf("a caller got %d, want 5", v)
		}
	}
}

func TestCacheGetOrSetLoaderPanic(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)

	started, release := make(chan struct{}), make(chan struct{})
	panicked := make(chan any, 1)
	go func() {
		defer func() { panicked <- recover() }()
		c.GetOrSet("k", func() (int, error) {
			close(started)
			<-release
			panic("boom")
		})
	}()
	<-started

	waiter := make(chan error, 1)
	go func() {
		_, err := c.GetOrSet("k", func() (int, error) { return 0, nil })
		waiter <- err
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)

	if p := <-panicked; p != "boom" {
		t.Fatalf("leader recovered %v, want the original panic", p)
	}
	if err := <-waiter; !errors.Is(err, ErrLoaderPanicked) {
		t.Fatalf("waiter err = %v, want ErrLoaderPanicked", err)
	}
	// The key must be usable again afterwards.
	if v, err := c.GetOrSet("k", func() (int, error) { return 2, nil }); v != 2 || err != nil {
		t.Fatalf("after panic: %d, %v", v, err)
	}
}

func TestCacheSetSweepsExpiredItems(t *testing.T) {
	c, clk := newTestCache[int, int](10*time.Minute, WithShards(1))
	for i := 0; i < 100; i++ {
		c.Set(i, i)
	}

	// Everything is expired and the sweep interval (the ttl) has passed.
	clk.Advance(11 * time.Minute)
	c.Set(1000, 1)

	if n := c.len(); n != 1 {
		t.Fatalf("%d items in the map after a sweeping Set, want 1", n)
	}
}

func TestCacheSetSweepIsRateLimited(t *testing.T) {
	c, clk := newTestCache[int, int](time.Second, WithShards(1)) // sweep interval floors at a minute
	c.Set(0, 0)                                                  // first Set sweeps and schedules the next sweep
	c.Set(1, 1)

	clk.Advance(5 * time.Second) // both expired, but a sweep isn't due yet
	c.Set(2, 2)
	if n := c.len(); n != 3 {
		t.Fatalf("%d items, want 3: Set swept earlier than the minimum interval", n)
	}

	clk.Advance(time.Minute)
	c.Set(3, 3)
	if n := c.len(); n != 1 {
		t.Fatalf("%d items, want 1 after the interval elapsed", n)
	}
}

func TestCacheNonPositiveTTLNeverExpires(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second} {
		c, clk := newTestCache[string, int](ttl)
		c.Set("a", 1)
		c.SetWithTTL("b", 2, ttl)

		clk.Advance(1000 * time.Hour)
		c.Set("trigger-sweep", 3)
		c.Cleanup()

		for _, k := range []string{"a", "b"} {
			if _, ok := c.Get(k); !ok {
				t.Fatalf("ttl=%v: item %q expired, want it to live forever", ttl, k)
			}
		}
	}
}

func TestCacheSetWithTTLZeroOverridesDefault(t *testing.T) {
	c, clk := newTestCache[string, int](time.Minute)
	c.SetWithTTL("forever", 1, 0)
	c.Set("default", 2)

	clk.Advance(time.Hour)
	if _, ok := c.Get("forever"); !ok {
		t.Fatal("item set with ttl 0 expired")
	}
	if _, ok := c.Get("default"); ok {
		t.Fatal("item with the default ttl should have expired")
	}
}

var errTest = errors.New("test error")

func TestCacheHugeTTLDoesNotOverflow(t *testing.T) {
	c, clk := newTestCache[string, int](time.Minute)
	c.SetWithTTL("a", 1, math.MaxInt64)

	clk.Advance(1000 * time.Hour)
	if _, ok := c.Get("a"); !ok {
		t.Fatal("item with a huge ttl expired, want it to live forever")
	}
}

func TestCoarseClock(t *testing.T) {
	clk := NewCoarseClock(time.Millisecond)
	defer clk.Stop()

	start := clk.Nanotime()
	deadline := time.After(2 * time.Second)
	for clk.Nanotime() <= start {
		select {
		case <-deadline:
			t.Fatal("CoarseClock never advanced")
		case <-time.After(time.Millisecond):
		}
	}

	clk.Stop()
	clk.Stop() // must not panic
	time.Sleep(5 * time.Millisecond)
	frozen := clk.Nanotime()
	time.Sleep(5 * time.Millisecond)
	if got := clk.Nanotime(); got != frozen {
		t.Fatalf("Nanotime moved from %d to %d after Stop", frozen, got)
	}
}

func TestNewCoarseClockPanicsOnNonPositiveTick(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("NewCoarseClock(0) did not panic")
		}
	}()
	NewCoarseClock(0)
}

func TestWithClockNilUsesDefault(t *testing.T) {
	c := New[string, int](time.Minute, WithClock(nil))
	if c.clock != Clock(defaultClock()) {
		t.Fatal("WithClock(nil) did not fall back to the default clock")
	}
}

func TestCacheGetOrSetContextWaiterCanLeave(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)

	started, release := make(chan struct{}), make(chan struct{})
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		c.GetOrSet("k", func() (int, error) {
			close(started)
			<-release
			return 3, nil
		})
	}()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := c.GetOrSetContext(ctx, "k", func(context.Context) (int, error) {
		t.Error("waiter ran its own load")
		return 0, nil
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiter err = %v, want context.DeadlineExceeded", err)
	}

	// The abandoned load still completes and is cached.
	close(release)
	<-leaderDone
	if v, ok := c.Get("k"); !ok || v != 3 {
		t.Fatalf("Get after load = %d, %v; want 3, true", v, ok)
	}
}

func TestCacheGetOrSetContextPassesContext(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")

	_, err := c.GetOrSetContext(ctx, "k", func(got context.Context) (int, error) {
		if got.Value(ctxKey{}) != "v" {
			t.Error("load did not get the caller's context")
		}
		return 1, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCacheGetOrSetContextDoneBeforeLoad(t *testing.T) {
	c, _ := newTestCache[string, int](time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := c.GetOrSetContext(ctx, "k", func(context.Context) (int, error) {
		t.Error("load ran with a done context")
		return 0, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}

	// A cached item is still served with a done context.
	c.Set("k", 1)
	if v, err := c.GetOrSetContext(ctx, "k", nil); v != 1 || err != nil {
		t.Fatalf("GetOrSetContext = %d, %v; want 1, nil", v, err)
	}
}

func TestCacheShardCount(t *testing.T) {
	for _, tc := range []struct{ in, want int }{{1, 1}, {3, 4}, {16, 16}, {17, 32}, {0, defaultShards()}, {-5, defaultShards()}} {
		c := New[int, int](time.Minute, WithShards(tc.in))
		if got := len(c.shards); got != tc.want {
			t.Errorf("WithShards(%d): %d shards, want %d", tc.in, got, tc.want)
		}
	}
	if n := defaultShards(); n < 4 || n > maxDefaultShards || n&(n-1) != 0 {
		t.Fatalf("defaultShards() = %d, want a power of two in [4, %d]", n, maxDefaultShards)
	}
}

func TestCacheManyShards(t *testing.T) {
	c, clk := newTestCache[int, int](time.Minute, WithShards(64))
	for i := range 1000 {
		c.Set(i, i)
	}
	for i := range 1000 {
		if v, ok := c.Get(i); !ok || v != i {
			t.Fatalf("Get(%d) = %d, %v; want %d, true", i, v, ok, i)
		}
	}
	used := 0
	for i := range c.shards {
		if len(c.shards[i].items) > 0 {
			used++
		}
	}
	if used < len(c.shards)/2 {
		t.Fatalf("keys landed in only %d of %d shards", used, len(c.shards))
	}

	clk.Advance(2 * time.Minute)
	if n := c.Cleanup(); n != 1000 {
		t.Fatalf("Cleanup removed %d items, want 1000", n)
	}
	if n := c.len(); n != 0 {
		t.Fatalf("%d items left after Cleanup, want 0", n)
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	c := New[int, int](time.Minute)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				k := (g*1000 + i) % 300
				c.Set(k, i)
				c.Get(k)
				if i%50 == 0 {
					c.Invalidate(k)
					c.Cleanup()
				}
			}
		}()
	}
	wg.Wait()
}

func TestCacheSetSweepIsIncremental(t *testing.T) {
	c, clk := newTestCache[int, int](time.Minute, WithShards(1))
	const n = 10 * sweepBatch
	for i := range n {
		c.Set(i, i)
	}
	clk.Advance(2 * time.Minute) // everything expired, a sweep is due

	c.Set(-1, 0)
	if left := c.len(); left < n-sweepBatch+1 {
		t.Fatalf("%d items left, want at least %d: one Set swept more than a batch", left, n-sweepBatch+1)
	}

	// Batches keep finding expired items, so following Sets keep sweeping
	// without waiting for the next interval.
	for i := 0; i < 100 && c.len() > sweepBatch; i++ {
		c.Set(-1, 0)
	}
	if left := c.len(); left > sweepBatch {
		t.Fatalf("%d items left, want following Sets to reclaim most of them", left)
	}
}

func TestCacheSetSweepStopsWhenFewExpired(t *testing.T) {
	c, clk := newTestCache[int, int](time.Minute, WithShards(1))
	for i := range 4 * sweepBatch {
		c.SetWithTTL(i, i, time.Hour) // alive
	}
	c.SetWithTTL(-2, 0, time.Second)
	clk.Advance(2 * time.Minute)

	c.Set(-1, 0) // sweeps a batch of mostly alive items
	if s := &c.shards[0]; s.nextSweep <= clk.Nanotime() {
		t.Fatal("next sweep not rescheduled after a mostly alive batch")
	}
}
