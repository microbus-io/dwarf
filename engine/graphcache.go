/*
Copyright (c) 2026 Microbus LLC and various contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package engine

import (
	"container/list"
	"sync"
	"time"
)

// graphCacheKey scopes the per-flow graph cache by shard, since flow_id is only unique within a shard.
type graphCacheKey struct {
	shard  int
	flowID int
}

// lruCache is a small thread-safe LRU with a per-entry TTL. It exists so processStep can reuse the
// parsed *workflow.Graph across the many steps of one flow instead of re-unmarshalling the frozen
// graph JSON every step. Mirrors the bounded-entries + TTL semantics of the lru.Cache the foreman used.
type lruCache[K comparable, V any] struct {
	mu         sync.Mutex
	maxEntries int
	ttl        time.Duration
	ll         *list.List // front = most-recently-used
	items      map[K]*list.Element
}

type lruEntry[K comparable, V any] struct {
	key      K
	value    V
	storedAt time.Time
}

// newLRUCache creates a cache bounded to maxEntries with the given per-entry TTL. A non-positive
// maxEntries disables eviction by count; a non-positive ttl disables expiry.
func newLRUCache[K comparable, V any](maxEntries int, ttl time.Duration) *lruCache[K, V] {
	return &lruCache[K, V]{
		maxEntries: maxEntries,
		ttl:        ttl,
		ll:         list.New(),
		items:      make(map[K]*list.Element),
	}
}

// Load returns the value for key if present and not expired, marking it most-recently-used.
func (c *lruCache[K, V]) Load(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.items[key]
	if !ok {
		return zero, false
	}
	ent := el.Value.(*lruEntry[K, V])
	if c.ttl > 0 && time.Since(ent.storedAt) > c.ttl {
		c.ll.Remove(el)
		delete(c.items, key)
		return zero, false
	}
	c.ll.MoveToFront(el)
	return ent.value, true
}

// Store inserts or updates key, evicting the least-recently-used entry when over capacity.
func (c *lruCache[K, V]) Store(key K, value V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		ent := el.Value.(*lruEntry[K, V])
		ent.value = value
		ent.storedAt = time.Now()
		c.ll.MoveToFront(el)
		return
	}
	el := c.ll.PushFront(&lruEntry[K, V]{key: key, value: value, storedAt: time.Now()})
	c.items[key] = el
	if c.maxEntries > 0 && c.ll.Len() > c.maxEntries {
		if oldest := c.ll.Back(); oldest != nil {
			c.ll.Remove(oldest)
			delete(c.items, oldest.Value.(*lruEntry[K, V]).key)
		}
	}
}
