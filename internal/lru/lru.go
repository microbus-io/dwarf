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

// Package lru provides a small thread-safe LRU cache with a per-entry TTL.
//
// A count-bounded cache holds at most a number of entries, each expiring a fixed time after it was stored:
//
//	graphs := lru.New[string, *Graph](4096, 15*time.Minute)
//	graphs.Store(key, g)
//
// A weighted cache is bounded by both a total weight the caller assigns to each entry (bytes, for a cache of
// payloads) and an entry count, and an entry expires once it has gone unread for the idle time:
//
//	payloads := lru.NewWeighted[int, []byte](1<<30, 65536, 5*time.Minute)
//	data, _ = payloads.LoadOrStoreWeighted(id, data, int64(len(data)))
package lru

import (
	"container/list"
	"sync"
	"time"
)

// Cache is a small thread-safe LRU with a per-entry TTL.
type Cache[K comparable, V any] struct {
	mu         sync.Mutex
	maxWeight  int64 // total weight allowed; non-positive = no bound by weight
	maxEntries int   // entries allowed; non-positive = no bound by count
	weight     int64 // total weight held
	ttl        time.Duration
	idle       bool       // the TTL runs from an entry's last load, not from its store
	ll         *list.List // front = most-recently-used
	items      map[K]*list.Element
}

type entry[K comparable, V any] struct {
	key      K
	value    V
	weight   int64
	lastUsed time.Time // when stored, or, for an idle TTL, last loaded
}

// New creates a cache bounded to maxEntries, each entry expiring ttl after it was stored. A non-positive
// maxEntries disables eviction by count; a non-positive ttl disables expiry. Store each entry with Store.
func New[K comparable, V any](maxEntries int, ttl time.Duration) *Cache[K, V] {
	return &Cache[K, V]{
		maxEntries: maxEntries,
		ttl:        ttl,
		ll:         list.New(),
		items:      make(map[K]*list.Element),
	}
}

// NewWeighted creates a cache bounded to a total weight of maxWeight and to maxEntries entries, each entry
// expiring once it has gone unloaded for idle. Store each entry with StoreWeighted or LoadOrStoreWeighted,
// giving its weight. A non-positive bound disables it; a non-positive idle disables expiry.
func NewWeighted[K comparable, V any](maxWeight int64, maxEntries int, idle time.Duration) *Cache[K, V] {
	return &Cache[K, V]{
		maxWeight:  maxWeight,
		maxEntries: maxEntries,
		ttl:        idle,
		idle:       true,
		ll:         list.New(),
		items:      make(map[K]*list.Element),
	}
}

// Load returns the value for key if present and not expired, marking it most-recently-used.
func (c *Cache[K, V]) Load(key K) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.live(key); ok {
		return el.Value.(*entry[K, V]).value, true
	}
	var zero V
	return zero, false
}

// Store inserts or updates key with a weight of one, evicting the least-recently-used entries when over
// capacity.
func (c *Cache[K, V]) Store(key K, value V) {
	c.StoreWeighted(key, value, 1)
}

// StoreWeighted inserts or updates key with the given weight, evicting the least-recently-used entries until
// the cache fits its bounds. A value heavier than the whole weight bound is not stored, and any entry already
// under key is removed. A non-positive weight is charged as one.
func (c *Cache[K, V]) StoreWeighted(key K, value V, weight int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	weight = max(weight, 1)
	el, ok := c.items[key]
	if c.tooHeavy(weight) {
		if ok {
			c.remove(el)
		}
		return
	}
	if ok {
		ent := el.Value.(*entry[K, V])
		c.weight += weight - ent.weight
		ent.value = value
		ent.weight = weight
		ent.lastUsed = time.Now()
		c.ll.MoveToFront(el)
	} else {
		c.insert(key, value, weight)
	}
	c.evict()
}

// LoadOrStoreWeighted returns the live value under key, marking it most-recently-used, and reports true.
// Otherwise it stores value with the given weight, as StoreWeighted does, and returns it with false - even
// when value was too heavy to keep. A non-positive weight is charged as one.
func (c *Cache[K, V]) LoadOrStoreWeighted(key K, value V, weight int64) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.live(key); ok {
		return el.Value.(*entry[K, V]).value, true
	}
	weight = max(weight, 1)
	if !c.tooHeavy(weight) {
		c.insert(key, value, weight)
		c.evict()
	}
	return value, false
}

// AddWeight adds delta to the weight of the entry under key, if present, marking it most-recently-used (and,
// for an idle TTL, used now), and evicts the least-recently-used entries until the cache fits its bounds. An
// entry made heavier than the whole weight bound is removed.
func (c *Cache[K, V]) AddWeight(key K, delta int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return
	}
	ent := el.Value.(*entry[K, V])
	if c.tooHeavy(ent.weight + delta) {
		c.remove(el)
		return
	}
	ent.weight += delta
	c.weight += delta
	if c.idle {
		ent.lastUsed = time.Now()
	}
	c.ll.MoveToFront(el)
	c.evict()
}

// Weight returns the total weight of the entries held, expired ones included until they are removed.
func (c *Cache[K, V]) Weight() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.weight
}

// Len returns the number of entries held, expired ones included until they are removed.
func (c *Cache[K, V]) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}

// live returns key's element if present and not expired, marking it most-recently-used and, for an idle
// TTL, used now. An expired one is removed. Must hold c.mu.
func (c *Cache[K, V]) live(key K) (*list.Element, bool) {
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	ent := el.Value.(*entry[K, V])
	now := time.Now()
	if c.ttl > 0 && now.Sub(ent.lastUsed) > c.ttl {
		c.remove(el)
		return nil, false
	}
	if c.idle {
		ent.lastUsed = now
	}
	c.ll.MoveToFront(el)
	return el, true
}

// tooHeavy reports whether a single entry of this weight exceeds the weight bound. Must hold c.mu.
func (c *Cache[K, V]) tooHeavy(weight int64) bool {
	return c.maxWeight > 0 && weight > c.maxWeight
}

// insert adds a new entry at the front. Must hold c.mu.
func (c *Cache[K, V]) insert(key K, value V, weight int64) {
	c.items[key] = c.ll.PushFront(&entry[K, V]{key: key, value: value, weight: weight, lastUsed: time.Now()})
	c.weight += weight
}

// evict removes least-recently-used entries until the cache fits its bounds. With an idle TTL the list is
// also in order of last use, so expired entries collect at the back and are removed here too. Must hold c.mu.
func (c *Cache[K, V]) evict() {
	now := time.Now()
	for {
		oldest := c.ll.Back()
		if oldest == nil {
			return
		}
		over := (c.maxWeight > 0 && c.weight > c.maxWeight) || (c.maxEntries > 0 && c.ll.Len() > c.maxEntries)
		expired := c.idle && c.ttl > 0 && now.Sub(oldest.Value.(*entry[K, V]).lastUsed) > c.ttl
		if !over && !expired {
			return
		}
		c.remove(oldest)
	}
}

// remove drops one entry. Must hold c.mu.
func (c *Cache[K, V]) remove(el *list.Element) {
	ent := el.Value.(*entry[K, V])
	c.ll.Remove(el)
	delete(c.items, ent.key)
	c.weight -= ent.weight
}
