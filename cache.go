package anvil

import (
	"context"
	"sync"
	"time"
)

// Cache is an in-memory key-value store whose items expire ttl after they
// were set. It is safe for concurrent use.
//
// Expiry is checked on Get; memory of expired items is reclaimed by
// Cleanup or RunCleanup.
type Cache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]CacheItem[V]
	ttl   time.Duration
	now   func() time.Time // replaceable in tests
}

// CacheItem is a single item of Cache.
type CacheItem[V any] struct {
	Value     V
	expiresAt time.Time
}

// NewCache returns an instance of Cache whose items live for ttl. A
// non-positive ttl makes every item expire immediately.
func NewCache[K comparable, V any](ttl time.Duration) *Cache[K, V] {
	return &Cache[K, V]{
		items: make(map[K]CacheItem[V]),
		ttl:   ttl,
		now:   time.Now,
	}
}

// Get returns the item stored under key. It reports false if the key is
// missing or the item has expired. Concurrent Gets don't block each other.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if item, ok := c.items[key]; ok && c.now().Before(item.expiresAt) {
		return item.Value, true
	}
	var zero V
	return zero, false
}

// Set stores value under key and restarts its ttl.
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = CacheItem[V]{
		Value:     value,
		expiresAt: c.now().Add(c.ttl),
	}
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
// they are overwritten, invalidated or removed by Cleanup.
func (c *Cache[K, V]) Cleanup() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.now()
	removed := 0
	for key, item := range c.items {
		if !now.Before(item.expiresAt) {
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
