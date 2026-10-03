package anvil

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time          { return c.t }
func (c *fakeClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestCache[K comparable, V any](ttl time.Duration) (*Cache[K, V], *fakeClock) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	c := NewCache[K, V](ttl)
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
