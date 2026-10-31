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

package graphcache

import (
	"encoding/json"
	"runtime"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func graphJSON(t *testing.T, name string) []byte {
	g := workflow.NewGraph(name)
	g.SetURL(name, "cache/"+name)
	g.AddTransition(name, workflow.End)
	b, err := json.Marshal(g)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestCache_FlowsOfOneGraphShareOneParse pins the shared layer's key: the same bytes under the same workflow URL
// are one entry, found by their hash, while other bytes or another URL are another entry.
func TestCache_FlowsOfOneGraphShareOneParse(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := New(16, 16)

	g := graphJSON(t, "First")
	first, err := c.Intern(g, "cache/g", HashOf(g))
	assert.NoError(err)
	again, err := c.Intern(g, "cache/g", HashOf(g))
	assert.NoError(err)
	assert.True(first == again, "the same graph is parsed once")
	shared, ok := c.Shared("cache/g", HashOf(g))
	assert.True(ok)
	assert.True(shared == first, "the hash finds the parse")
	assert.Equal("cache/g", first.WorkflowURL)
	assert.NotNil(first.FanIn)

	other := graphJSON(t, "Second")
	o, err := c.Intern(other, "cache/g", HashOf(other))
	assert.NoError(err)
	assert.True(o != first, "a different graph is a different entry")
	u, err := c.Intern(g, "cache/other", HashOf(g))
	assert.NoError(err)
	assert.True(u != first, "the workflow URL is part of the key")
	assert.Equal("cache/other", u.WorkflowURL)
}

// TestCache_EmptyHash pins the two readings of an empty graph hash: Shared misses on it, and Intern computes it
// from the bytes, so a later lookup by the real hash finds the parse.
func TestCache_EmptyHash(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := New(16, 16)

	g := graphJSON(t, "First")
	e, err := c.Intern(g, "cache/g", "")
	assert.NoError(err)
	_, ok := c.Shared("cache/g", "")
	assert.False(ok, "an empty hash names no graph")
	found, ok := c.Shared("cache/g", HashOf(g))
	assert.True(ok)
	assert.True(found == e)
}

// TestCache_FlowIsKeyedByToken pins that the per-flow layer keys a flow by its token as well as its id: a flow id
// reissued to a new flow, with a new token, does not find the old flow's graph.
func TestCache_FlowIsKeyedByToken(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := New(16, 16)

	g := graphJSON(t, "First")
	e, err := c.Intern(g, "cache/g", "")
	assert.NoError(err)
	c.Point(Key{Shard: 1, FlowID: 7, Token: "aaaaaaaaaaaaaaaa"}, e)
	found, ok := c.Flow(Key{Shard: 1, FlowID: 7, Token: "aaaaaaaaaaaaaaaa"})
	assert.True(ok)
	assert.True(found == e)
	_, ok = c.Flow(Key{Shard: 1, FlowID: 7, Token: "bbbbbbbbbbbbbbbb"})
	assert.False(ok, "the same id under another token is another flow")
	_, ok = c.Flow(Key{Shard: 2, FlowID: 7, Token: "aaaaaaaaaaaaaaaa"})
	assert.False(ok, "the same id on another shard is another flow")
	assert.Equal(1, c.Flows())
}

// TestCache_UnparseableGraphIsNotCached pins that a parse failure is returned and leaves nothing behind.
func TestCache_UnparseableGraphIsNotCached(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := New(16, 16)

	_, err := c.Intern([]byte(`[1]`), "cache/g", "")
	assert.Error(err)
	_, ok := c.Shared("cache/g", HashOf([]byte(`[1]`)))
	assert.False(ok)
}

// TestCache_SharedEvictionReleasesTheParse pins the bound that makes the per-flow layer cheap: it holds weak
// pointers, so a parse the shared layer evicts is released, and its flows read as a miss.
func TestCache_SharedEvictionReleasesTheParse(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := New(16, 1)

	k := Key{Shard: 1, FlowID: 1, Token: "aaaaaaaaaaaaaaaa"}
	func() {
		e, err := c.Intern(graphJSON(t, "First"), "cache/g", "")
		assert.NoError(err)
		c.Point(k, e)
	}()
	_, ok := c.Flow(k)
	assert.True(ok, "held while the shared layer holds it")

	_, err := c.Intern(graphJSON(t, "Second"), "cache/g", "") // evicts the first parse from the one-entry layer
	assert.NoError(err)
	released := false
	for range 10 {
		runtime.GC()
		if _, ok := c.Flow(k); !ok {
			released = true
			break
		}
	}
	assert.True(released, "an evicted parse is not kept alive by the per-flow layer")
}
