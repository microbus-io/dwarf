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

package lru

import (
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

func TestLRUCache_HitMiss(t *testing.T) {
	assert := testarossa.For(t)

	c := New[int, string](4, 0)
	_, ok := c.Load(1)
	assert.False(ok)

	c.Store(1, "a")
	v, ok := c.Load(1)
	assert.True(ok)
	assert.Equal("a", v)

	// Store on an existing key updates the value.
	c.Store(1, "b")
	v, ok = c.Load(1)
	assert.True(ok)
	assert.Equal("b", v)
}

func TestLRUCache_EvictsLeastRecentlyUsed(t *testing.T) {
	assert := testarossa.For(t)

	c := New[int, string](2, 0)
	c.Store(1, "a")
	c.Store(2, "b")
	// Touch 1 so 2 becomes the least-recently-used.
	_, ok := c.Load(1)
	assert.True(ok)
	// Inserting a third entry evicts 2.
	c.Store(3, "c")

	_, ok = c.Load(2)
	assert.False(ok)
	_, ok = c.Load(1)
	assert.True(ok)
	_, ok = c.Load(3)
	assert.True(ok)
}

func TestLRUCache_TTLExpiry(t *testing.T) {
	assert := testarossa.For(t)

	c := New[int, string](8, 20*time.Millisecond)
	c.Store(1, "a")
	v, ok := c.Load(1)
	assert.True(ok)
	assert.Equal("a", v)

	time.Sleep(40 * time.Millisecond)
	_, ok = c.Load(1)
	assert.False(ok)
}

func TestLRUCache_ZeroMaxEntriesNoEvict(t *testing.T) {
	assert := testarossa.For(t)

	c := New[int, int](0, 0)
	for i := range 1000 {
		c.Store(i, i)
	}
	for i := range 1000 {
		v, ok := c.Load(i)
		assert.True(ok)
		assert.Equal(i, v)
	}
}

func TestLRUCache_WeightedEvictsByWeight(t *testing.T) {
	assert := testarossa.For(t)

	c := NewWeighted[int, string](10, 0, 0)
	c.StoreWeighted(1, "a", 4)
	c.StoreWeighted(2, "b", 4)
	assert.Equal(int64(8), c.Weight())

	// 3 more would make 11: the least-recently-used entry goes.
	c.StoreWeighted(3, "c", 3)
	_, ok := c.Load(1)
	assert.False(ok)
	assert.Equal(int64(7), c.Weight())

	// Several light entries go to make room for one heavy one.
	c.StoreWeighted(4, "d", 9)
	assert.Equal(1, c.Len())
	assert.Equal(int64(9), c.Weight())

	// Updating a key re-charges it.
	c.StoreWeighted(4, "dd", 6)
	assert.Equal(int64(6), c.Weight())
	assert.Equal(1, c.Len())
}

func TestLRUCache_WeightedEvictsByCount(t *testing.T) {
	assert := testarossa.For(t)

	c := NewWeighted[int, string](1000, 2, 0)
	c.StoreWeighted(1, "a", 1)
	c.StoreWeighted(2, "b", 1)
	c.StoreWeighted(3, "c", 1)
	assert.Equal(2, c.Len())
	_, ok := c.Load(1)
	assert.False(ok, "the count bound holds whatever the weight")
	assert.Equal(int64(2), c.Weight())
}

func TestLRUCache_WeightedRejectsOverCapacity(t *testing.T) {
	assert := testarossa.For(t)

	c := NewWeighted[int, string](10, 0, 0)
	c.StoreWeighted(1, "a", 4)
	c.StoreWeighted(2, "huge", 11)
	_, ok := c.Load(2)
	assert.False(ok)
	_, ok = c.Load(1)
	assert.True(ok, "a rejected value evicts nothing")

	// Replacing a key with a value too heavy to hold removes the old entry rather than keeping it stale.
	c.StoreWeighted(1, "huge", 11)
	_, ok = c.Load(1)
	assert.False(ok)
	assert.Equal(int64(0), c.Weight())

	// A non-positive weight is charged as one.
	c.StoreWeighted(5, "zero", 0)
	c.StoreWeighted(6, "negative", -7)
	assert.Equal(int64(2), c.Weight())
}

func TestLRUCache_LoadOrStoreWeighted(t *testing.T) {
	assert := testarossa.For(t)

	c := NewWeighted[int, string](10, 0, 0)
	v, loaded := c.LoadOrStoreWeighted(1, "first", 4)
	assert.False(loaded)
	assert.Equal("first", v)

	// A live entry wins: the second value is neither stored nor charged.
	v, loaded = c.LoadOrStoreWeighted(1, "second", 6)
	assert.True(loaded)
	assert.Equal("first", v)
	assert.Equal(int64(4), c.Weight())

	// A value too heavy to keep is handed back, unstored.
	v, loaded = c.LoadOrStoreWeighted(2, "huge", 11)
	assert.False(loaded)
	assert.Equal("huge", v)
	_, ok := c.Load(2)
	assert.False(ok)
	assert.Equal(int64(4), c.Weight())
}

func TestLRUCache_AddWeight(t *testing.T) {
	assert := testarossa.For(t)

	c := NewWeighted[int, string](10, 0, 0)
	c.StoreWeighted(1, "a", 3)
	c.StoreWeighted(2, "b", 3)

	// 1 is the least recently used, yet growing it marks it used: the other entry goes, not 1.
	c.AddWeight(1, 5)
	_, ok := c.Load(2)
	assert.False(ok)
	v, ok := c.Load(1)
	assert.True(ok)
	assert.Equal("a", v)
	assert.Equal(int64(8), c.Weight())

	// Past the whole weight bound the entry is dropped.
	c.AddWeight(1, 3)
	_, ok = c.Load(1)
	assert.False(ok)
	assert.Equal(int64(0), c.Weight())

	// An absent key is a no-op.
	c.AddWeight(42, 5)
	assert.Equal(0, c.Len())
}

func TestLRUCache_IdleExpiry(t *testing.T) {
	assert := testarossa.For(t)

	c := NewWeighted[int, string](100, 0, 60*time.Millisecond)
	c.StoreWeighted(1, "read", 6)
	c.StoreWeighted(2, "unread", 6)

	// Reading 1 keeps it alive past the idle time it would otherwise have expired at.
	for range 4 {
		time.Sleep(25 * time.Millisecond)
		_, ok := c.Load(1)
		assert.True(ok)
	}
	_, ok := c.Load(2)
	assert.False(ok, "an entry unread for the idle time expires")
	assert.Equal(int64(6), c.Weight())

	// Expired entries collect at the back and are removed by the next store, read or not.
	time.Sleep(80 * time.Millisecond)
	c.StoreWeighted(3, "new", 1)
	assert.Equal(1, c.Len())
	assert.Equal(int64(1), c.Weight())
}

func TestLRUCache_CountTTLRunsFromStore(t *testing.T) {
	assert := testarossa.For(t)

	// A count-bounded cache keeps its TTL from the store: reading does not extend it.
	c := New[int, string](8, 60*time.Millisecond)
	c.Store(1, "a")
	time.Sleep(35 * time.Millisecond)
	_, ok := c.Load(1)
	assert.True(ok)
	time.Sleep(35 * time.Millisecond)
	_, ok = c.Load(1)
	assert.False(ok)
}
