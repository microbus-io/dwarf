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
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

func TestLRUCache_HitMiss(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := newLRUCache[int, string](4, 0)
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
	t.Parallel()
	assert := testarossa.For(t)

	c := newLRUCache[int, string](2, 0)
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
	t.Parallel()
	assert := testarossa.For(t)

	c := newLRUCache[int, string](8, 20*time.Millisecond)
	c.Store(1, "a")
	v, ok := c.Load(1)
	assert.True(ok)
	assert.Equal("a", v)

	time.Sleep(40 * time.Millisecond)
	_, ok = c.Load(1)
	assert.False(ok)
}

func TestLRUCache_ZeroMaxEntriesNoEvict(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := newLRUCache[int, int](0, 0)
	for i := range 1000 {
		c.Store(i, i)
	}
	for i := range 1000 {
		v, ok := c.Load(i)
		assert.True(ok)
		assert.Equal(i, v)
	}
}
