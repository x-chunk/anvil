package anvil

import (
	"sync"
	"time"
)

// Cache is a store for caching items
type Cache[K comparable, V any] struct {
	mu    sync.RWMutex
	items map[K]*CacheItem[V]
	ttl   time.Duration
}

// CacheItem is a single item of Cache
type CacheItem[V any] struct {
	Value     V
	expiresAt time.Time
}

// NewCache returns an instance of Cache
func NewCache[K comparable, V any](ttl time.Duration) *Cache[K, V] {
	return &Cache[K, V]{
		items: make(map[K]*CacheItem[V]),
		ttl:   ttl,
	}
}

// Get returns item from the Cache by key.
// Allows multi-goroutine reading
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if item, ok := c.items[key]; ok && time.Now().Before(item.expiresAt) {
		return item.Value, true
	}
	var zero V
	return zero, false
}

// Set creates or sets new value into the Cache
func (c *Cache[K, V]) Set(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[key] = &CacheItem[V]{
		Value:     value,
		expiresAt: time.Now().Add(c.ttl),
	}
}

// Invalidate invalidates item from the Cache instantly
func (c *Cache[K, V]) Invalidate(key K) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if item, ok := c.items[key]; ok {
		item.expiresAt = time.Time{}
	}
}
