package anvil

import (
	"context"
	"errors"
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

	callsMu sync.Mutex
	calls   map[K]*cacheCall[V] // in-flight GetOrSet loads
}

// cacheCall is a GetOrSet load that other callers for the same key wait on.
type cacheCall[V any] struct {
	done  chan struct{}
	value V
	err   error
}

var errLoaderPanicked = errors.New("anvil: cache loader panicked")

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
		calls: make(map[K]*cacheCall[V]),
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
	c.SetWithTTL(key, value, c.ttl)
}

// SetWithTTL stores value under key like Set, but the item lives for ttl
// instead of the cache's default. A non-positive ttl makes the item expire
// immediately.
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = CacheItem[V]{
		Value:     value,
		expiresAt: c.now().Add(ttl),
	}
}

// GetOrSet returns the item stored under key. If there is none, it calls
// load, stores the result with the cache's default ttl and returns it.
//
// Concurrent calls for the same missing key share a single load: one
// caller runs it and the others wait for its result. An error from load is
// returned to all of them and is not cached. If load panics, the panic
// propagates in the calling goroutine and waiting callers get an error.
func (c *Cache[K, V]) GetOrSet(key K, load func() (V, error)) (V, error) {
	if v, ok := c.Get(key); ok {
		return v, nil
	}

	c.callsMu.Lock()
	if call, ok := c.calls[key]; ok {
		c.callsMu.Unlock()
		<-call.done
		return call.value, call.err
	}
	call := &cacheCall[V]{done: make(chan struct{})}
	c.calls[key] = call
	c.callsMu.Unlock()

	finished := false
	defer func() {
		if !finished {
			call.err = errLoaderPanicked
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

	call.value, call.err = load()
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
