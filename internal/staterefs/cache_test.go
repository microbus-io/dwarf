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
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// testFlow is the flow token the tests resolve and seed under.
const testFlow = "0123456789abcdef"

// payload is a JSON string of exactly n bytes, quotes included.
func payload(n int) json.RawMessage {
	return json.RawMessage(`"` + strings.Repeat("x", n-2) + `"`)
}

// charge is what the cache charges an entry of n bytes before any split.
func charge(n int) int64 {
	return int64(n) + entryOverhead
}

// TestLinker_CacheBoundedByBytes pins the bound: the cache is charged each field's bytes plus a per-entry
// overhead, never holds more than its size, and evicts the least recently used field to make room - which
// a later resolve then reads again.
func TestLinker_CacheBoundedByBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const size = 10000
	anchors := &fakeAnchors{rows: map[int]Anchor{}}
	for id := 1; id <= 3; id++ {
		anchors.rows[id] = Anchor{Changes: map[string]json.RawMessage{"doc": payload(size)}}
	}
	// Room for two entries, not three.
	budget := 2*charge(size) + size/2
	linker := New("sqlite", 1, NewCache(budget))

	for id := 1; id <= 3; id++ {
		_, err := linker.Resolve(ctx, testFlow, newState(), Refs{"doc": {Anchor: id}}, nil, anchors.load)
		assert.NoError(err)
		assert.True(linker.cacheBytes() <= budget)
	}
	assert.Equal(2*charge(size), linker.cacheBytes())
	assert.Equal(3, len(anchors.calls))

	// 3 and 2 are cached; 1 was evicted and is read again.
	for _, id := range []int{3, 2, 1} {
		_, err := linker.Resolve(ctx, testFlow, newState(), Refs{"doc": {Anchor: id}}, nil, anchors.load)
		assert.NoError(err)
	}
	assert.Equal(4, len(anchors.calls))
	assert.Equal([]int{1}, anchors.calls[3])
}

// TestLinker_SmallFieldsChargedTheirBytes pins that a small field is charged its bytes and its overhead and no
// more: a list caches one entry per contributing step, so a long conversation of small messages is many small
// entries, and a minimum charge would evict them long before the budget's bytes are used.
func TestLinker_SmallFieldsChargedTheirBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{}}
	for id := 1; id <= 8; id++ {
		anchors.rows[id] = Anchor{Changes: map[string]json.RawMessage{"doc": payload(1024)}}
	}
	linker := New("sqlite", 1, NewCache(8*charge(1024)))
	for id := 1; id <= 8; id++ {
		_, err := linker.Resolve(ctx, testFlow, newState(), Refs{"doc": {Anchor: id}}, nil, anchors.load)
		assert.NoError(err)
	}
	assert.Equal(8, linker.cache.Len())
	assert.Equal(8*(1024+entryOverhead), int(linker.cacheBytes()))
}

// TestLinker_FieldLargerThanCacheIsNotCached pins that a field heavier than the whole cache resolves
// correctly every time, read from its anchor each time, and evicts nothing to make room it cannot use.
func TestLinker_FieldLargerThanCacheIsNotCached(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		1: {Changes: map[string]json.RawMessage{"small": payload(100)}},
		2: {Changes: map[string]json.RawMessage{"big": payload(20000)}},
	}}
	linker := New("sqlite", 1, NewCache(10000))

	_, err := linker.Resolve(ctx, testFlow, newState(), Refs{"small": {Anchor: 1}}, nil, anchors.load)
	assert.NoError(err)
	for range 2 {
		state := newState()
		_, err := linker.Resolve(ctx, testFlow, state, Refs{"big": {Anchor: 2}}, nil, anchors.load)
		assert.NoError(err)
		assert.Len(stateVal(state, "big"), 19998)
	}
	assert.Equal(3, len(anchors.calls), "the big field is read on every resolve")
	assert.Equal(charge(100), linker.cacheBytes(), "the small field stays cached")

	// Seeding an oversized field is declined the same way.
	seeded := newState()
	seeded.SetJSON("big", payload(20000))
	linker.Seed(testFlow, 9, seeded, workflow.NewChanges(), []byte(`{"big":9}`))
	assert.Equal(charge(100), linker.cacheBytes())
}

// TestLinker_SplitChargedToItsArray pins that a split lives and dies with its array's entry: splitting adds
// its element slices to that entry's charge, and evicting the entry leaves nothing of the array behind - the
// next item ref into it reads the anchor again.
func TestLinker_SplitChargedToItsArray(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	elem := string(payload(2000))
	array := json.RawMessage(`[` + elem + `,` + elem + `,` + elem + `]`)
	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Changes: map[string]json.RawMessage{"pages": array}},
		8: {Changes: map[string]json.RawMessage{"other": payload(6000)}},
	}}
	arrayCharge := charge(len(array))
	splitCharge := 3*elemSize + splitOverhead
	linker := New("sqlite", 1, NewCache(arrayCharge+splitCharge+100))

	_, err := linker.Resolve(ctx, testFlow, newState(), Refs{"pages": {Anchor: 7}}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal(arrayCharge, linker.cacheBytes())
	for i := range 3 {
		state := newState()
		_, err := linker.Resolve(ctx, testFlow, state, Refs{"page": {Anchor: 7, Field: "pages", Path: []int{i}}}, nil, anchors.load)
		assert.NoError(err)
		assert.Len(stateVal(state, "page"), 1998)
	}
	assert.Equal(arrayCharge+splitCharge, linker.cacheBytes(), "the split is charged once, to the array's entry")
	assert.Equal(1, len(anchors.calls))

	// Another field pushes the array out, and its split goes with it.
	_, err = linker.Resolve(ctx, testFlow, newState(), Refs{"other": {Anchor: 8}}, nil, anchors.load)
	assert.NoError(err)
	_, ok := linker.cache.Load(linker.key(testFlow, 7, "pages", ColAny))
	assert.False(ok)
	assert.Equal(charge(6000), linker.cacheBytes())
	_, err = linker.Resolve(ctx, testFlow, newState(), Refs{"page": {Anchor: 7, Field: "pages", Path: []int{0}}}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal(3, len(anchors.calls), "an evicted array's split is not served from anywhere")
}

// TestLinker_ArrayAndElementShareOneEntry pins that one read serving both a field and an item ref into it
// stores one entry, so the split made for the element is the one later branches find.
func TestLinker_ArrayAndElementShareOneEntry(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Changes: map[string]json.RawMessage{"pages": json.RawMessage(`["a","b","c"]`)}},
	}}
	linker := New("sqlite", 1, NewCache(testCacheBytes))
	_, err := linker.Resolve(ctx, testFlow, newState(), Refs{"pages": {Anchor: 7}, "page": {Anchor: 7, Field: "pages", Path: []int{1}}}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal(1, linker.cache.Len())
	entry, ok := linker.cache.Load(linker.key(testFlow, 7, "pages", ColAny))
	assert.True(ok)
	_, ok = entry.splits["pages"]
	assert.True(ok, "the cached entry holds the split the element was served from")
	assert.Equal(charge(len(`["a","b","c"]`))+3*elemSize+splitOverhead, linker.cacheBytes())

	// A later store of the same field keeps the existing entry and its split.
	seeded := newState()
	seeded.SetJSON("pages", json.RawMessage(`["a","b","c"]`))
	linker.Seed(testFlow, 7, seeded, workflow.NewChanges(), []byte(`{"pages":7}`))
	again, _ := linker.cache.Load(linker.key(testFlow, 7, "pages", ColAny))
	assert.True(again == entry)
}

// TestLinker_SplitsRaceEviction drives concurrent item-ref resolves into arrays that a small cache keeps
// evicting and re-reading. Under -race it pins that splits, stores and charges do not race, every element
// comes back right, and the cache stays within its bound.
func TestLinker_SplitsRaceEviction(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	elem := string(payload(1000))
	rows := map[int]Anchor{}
	for id := 1; id <= 4; id++ {
		rows[id] = Anchor{Changes: map[string]json.RawMessage{
			"pages": json.RawMessage(`[[` + elem + `,` + elem + `],[` + elem + `,` + elem + `]]`),
		}}
	}
	load := func(ctx context.Context, reads []Read) (map[int]Anchor, error) {
		out := map[int]Anchor{}
		for _, r := range reads {
			out[r.ID] = rows[r.ID]
		}
		return out, nil
	}
	budget := 2 * (charge(4*1000+10) + 6*elemSize + 3*splitOverhead)
	linker := New("sqlite", 1, NewCache(budget))

	var wg sync.WaitGroup
	errs := make(chan error, 64*8)
	for g := range 64 {
		wg.Go(func() {
			for i := range 8 {
				id := 1 + (g+i)%4
				path := []int{i % 2, g % 2}
				state := newState()
				refs := Refs{"page": {Anchor: id, Field: "pages", Path: path}}
				var err error
				if g%2 == 0 {
					_, err = linker.ResolveShared(ctx, testFlow, state, refs, load)
				} else {
					_, err = linker.Resolve(ctx, testFlow, state, refs, nil, load)
				}
				if err == nil && len(stateVal(state, "page").(string)) != 998 {
					err = errors.New("wrong element")
				}
				if err != nil {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		assert.NoError(err)
	}
	assert.True(linker.cacheBytes() <= budget)
}

// cacheBytes is the shared cache's charge, read through a Linker.
func (l *Linker) cacheBytes() int64 {
	return l.cache.Weight()
}

// TestLinker_ShardsShareOneCache pins the shared cache: two shards' Linkers keep the same anchor id apart,
// and draw on one budget - a shard that needs the room evicts another shard's least recently used field.
func TestLinker_ShardsShareOneCache(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const size = 10000
	cache := NewCache(2*charge(size) + size/2)
	one, two := New("sqlite", 1, cache), New("sqlite", 2, cache)
	rows := func(text string) *fakeAnchors {
		return &fakeAnchors{rows: map[int]Anchor{
			7: {Changes: map[string]json.RawMessage{"doc": json.RawMessage(`"` + text + strings.Repeat("x", size-2-len(text)) + `"`)}},
			8: {Changes: map[string]json.RawMessage{"doc": payload(size)}},
		}}
	}
	onRows, twoRows := rows("one"), rows("two")

	// Step 7 on each shard is a different field, cached apart.
	for _, c := range []struct {
		linker *Linker
		rows   *fakeAnchors
		want   string
	}{{one, onRows, "one"}, {two, twoRows, "two"}} {
		state := newState()
		_, err := c.linker.Resolve(ctx, testFlow, state, Refs{"doc": {Anchor: 7}}, nil, c.rows.load)
		assert.NoError(err)
		assert.True(strings.HasPrefix(stateVal(state, "doc").(string), c.want))
	}
	assert.Equal(2*charge(size), cache.Bytes())
	state := newState()
	_, err := one.Resolve(ctx, testFlow, state, Refs{"doc": {Anchor: 7}}, nil, onRows.load)
	assert.NoError(err)
	assert.True(strings.HasPrefix(stateVal(state, "doc").(string), "one"))
	assert.Equal(1, len(onRows.calls), "shard 1's step 7 is served from the cache, not shard 2's")

	// Shard 2 needs room for step 8: the least recently used field goes - shard 2's own step 7, since shard 1's
	// was just read.
	_, err = two.Resolve(ctx, testFlow, newState(), Refs{"doc": {Anchor: 8}}, nil, twoRows.load)
	assert.NoError(err)
	_, ok := cache.lru.Load(two.key(testFlow, 7, "doc", ColAny))
	assert.False(ok)
	_, ok = cache.lru.Load(one.key(testFlow, 7, "doc", ColAny))
	assert.True(ok)
	assert.Equal(2*charge(size), cache.Bytes())
}

// TestLinker_ReissuedStepIDIsNotServedAcrossFlows pins the flow token in the cache key: after a restore or a
// lossy failover reissues a step id to a new flow, that flow's ref to the id reads the new row rather than
// being served the lost flow's bytes from the cache.
func TestLinker_ReissuedStepIDIsNotServedAcrossFlows(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	linker := New("sqlite", 1, NewCache(1<<20))
	lost := &fakeAnchors{rows: map[int]Anchor{7: {Changes: map[string]json.RawMessage{"doc": json.RawMessage(`"lost"`)}}}}
	state := newState()
	_, err := linker.Resolve(ctx, "aaaaaaaaaaaaaaaa", state, Refs{"doc": {Anchor: 7}}, nil, lost.load)
	assert.NoError(err)
	assert.Equal("lost", stateVal(state, "doc"))

	reissued := &fakeAnchors{rows: map[int]Anchor{7: {Changes: map[string]json.RawMessage{"doc": json.RawMessage(`"new"`)}}}}
	state = newState()
	_, err = linker.Resolve(ctx, "bbbbbbbbbbbbbbbb", state, Refs{"doc": {Anchor: 7}}, nil, reissued.load)
	assert.NoError(err)
	assert.Equal("new", stateVal(state, "doc"))
	assert.Equal(1, len(reissued.calls))
}
