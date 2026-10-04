package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestCache[K comparable, V any](ttl time.Duration) (*Cache[K, V], *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	c := New[K, V](ttl)
	c.now = clk.Now
	return c, clk
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

	if n := len(c.items); n != 0 {
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
	if len(c.items) != 1 {
		t.Fatalf("%d items left, want 1", len(c.items))
	}
	if _, ok := c.Get("fresh"); !ok {
		t.Fatal("fresh item was removed")
	}
	if n := c.Cleanup(); n != 0 {
		t.Fatalf("second Cleanup removed %d items, want 0", n)
	}
}

func TestCacheRunCleanup(t *testing.T) {
	c := New[string, int](5 * time.Millisecond) // real clock
	c.Set("a", 1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		c.RunCleanup(ctx, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.After(2 * time.Second)
	for {
		c.mu.RLock()
		n := len(c.items)
		c.mu.RUnlock()
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
	if err := <-waiter; !errors.Is(err, errLoaderPanicked) {
		t.Fatalf("waiter err = %v, want errLoaderPanicked", err)
	}
	// The key must be usable again afterwards.
	if v, err := c.GetOrSet("k", func() (int, error) { return 2, nil }); v != 2 || err != nil {
		t.Fatalf("after panic: %d, %v", v, err)
	}
}

func TestCacheSetSweepsExpiredItems(t *testing.T) {
	c, clk := newTestCache[int, int](10 * time.Minute)
	for i := 0; i < 100; i++ {
		c.Set(i, i)
	}

	// Everything is expired and the sweep interval (the ttl) has passed.
	clk.Advance(11 * time.Minute)
	c.Set(1000, 1)

	if n := len(c.items); n != 1 {
		t.Fatalf("%d items in the map after a sweeping Set, want 1", n)
	}
}

func TestCacheSetSweepIsRateLimited(t *testing.T) {
	c, clk := newTestCache[int, int](time.Second) // sweep interval floors at a minute
	c.Set(0, 0)                                   // first Set sweeps and schedules the next sweep
	c.Set(1, 1)

	clk.Advance(5 * time.Second) // both expired, but a sweep isn't due yet
	c.Set(2, 2)
	if n := len(c.items); n != 3 {
		t.Fatalf("%d items, want 3: Set swept earlier than the minimum interval", n)
	}

	clk.Advance(time.Minute)
	c.Set(3, 3)
	if n := len(c.items); n != 1 {
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
