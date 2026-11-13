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

package staterefs

import (
	"encoding/json"
	"sync"
	"time"
	"unsafe"

	"github.com/microbus-io/dwarf/internal/lru"
)

const (
	// The resolved-bytes cache is bounded in bytes (given to NewCache) and in entries (cacheEntries), and an
	// entry expires once unread for cacheIdle. One cache serves every shard, so the budget goes wherever this
	// replica is dispatching. The entry cap sits high enough that bytes bind: a list caches one entry per
	// contributing step, so a long conversation of small messages is many small entries.
	cacheEntries = 1 << 20
	cacheIdle    = 5 * time.Minute
	// entryOverhead is what an entry costs beyond its bytes, so a cache of small entries is charged its real
	// memory: the LRU's list element (48) and entry (80), the map slot (~70), and the cachedField (48).
	entryOverhead = 256
	// splitOverhead is what one cached split costs beyond its element slices: its map slot and path key.
	splitOverhead = 64
)

// elemSize is the size of one element slice in a cached split. The elements alias the array's bytes, so a
// split costs its slice headers, not a second copy.
const elemSize = int64(unsafe.Sizeof(json.RawMessage(nil)))

// cacheKey identifies one ref'd field's bytes. Immutable once the anchor step settles (a terminal step is
// immutable, and a retry rewrites a step's changes only BEFORE any successor exists), so it is as good a key
// as a content hash. It carries the shard because a step id is unique only within one, and the cache is
// shared by every shard's Linker. It carries the flow's token because a restore or a lossy failover reissues
// step ids, and the flow that gets an id back gets a new random token, so it never matches an entry cached
// from the lost timeline.
type cacheKey struct {
	shard  int
	flow   string
	anchor int
	field  string
	col    byte
}

// Cache holds resolved field bytes for every shard's Linker, under one budget. It is safe for concurrent use.
type Cache struct {
	lru *lru.Cache[cacheKey, *cachedField]
}

// NewCache creates a cache holding at most bytes of resolved field bytes, overheads included. A field larger
// than bytes is never cached, so each resolve of it reads it again.
func NewCache(bytes int64) *Cache {
	return &Cache{lru: lru.NewWeighted[cacheKey, *cachedField](max(bytes, 1), cacheEntries, cacheIdle)}
}

// Bytes reports the bytes the cache is charged for right now, overheads included.
func (c *Cache) Bytes() int64 {
	return c.lru.Weight()
}

// cachedField is one ref'd field's bytes in the resolve cache, with the splits of those bytes that item refs
// have needed. A split's elements are sub-slices of bytes, so holding the splits in the same entry is what
// lets an eviction free the bytes: a split cached on its own would keep an evicted array alive, unseen by the
// cache's byte count.
type cachedField struct {
	bytes json.RawMessage

	mu sync.Mutex
	// splits holds an item-ref'd array's elements, keyed by the path to the array ("data", then "data[3]").
	// Without it every branch of a fan-out would scan the whole array to find its one element - O(N) per
	// branch, O(N^2) per cohort - a cost a plain ref, spliced without decoding, never pays.
	splits map[string][]json.RawMessage
}
