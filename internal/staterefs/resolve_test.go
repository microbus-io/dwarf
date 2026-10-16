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
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// testCacheBytes sizes a test Linker's resolve cache: far above anything a test caches unless the test is
// about the bound.
const testCacheBytes = 1 << 20

// fakeAnchors is a Loader over an in-memory anchor table that COUNTS its calls and records the id sets it
// was asked for. Both are assertions in their own right: the size policy prices a whole anchor row, which is
// only sound if k anchors cost one call rather than k.
type fakeAnchors struct {
	rows  map[int]Anchor
	calls [][]int
}

func (f *fakeAnchors) load(ctx context.Context, anchorIDs []int) (map[int]Anchor, error) {
	ids := append([]int(nil), anchorIDs...)
	f.calls = append(f.calls, ids)
	out := map[int]Anchor{}
	for _, id := range ids {
		if row, ok := f.rows[id]; ok {
			out[id] = row
		}
	}
	return out, nil
}

// cols builds an anchor payload column from literal values.
func cols(kv map[string]any) map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for k, v := range kv {
		data, _ := json.Marshal(v)
		out[k] = data
	}
	return out
}

// TestLinker_ResolveReadsBothColumns pins the correction that motivated the whole design: an anchor's bytes
// are NOT always in its `changes`. Two of the three places they sit are the `state` column - the flow's
// INITIAL INPUT at the entry step (no task produced it, so it appears in no changes anywhere) and a fan-in's
// reducer output - and `changes` shadows `state` when both hold the field, being the newer value.
func TestLinker_ResolveReadsBothColumns(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {
			State:   cols(map[string]any{"fromState": "initial input", "shadowed": "old"}),
			Changes: cols(map[string]any{"fromChanges": "task output", "shadowed": "new"}),
		},
	}}
	state := newState()
	refs := Refs{"fromState": {Anchor: 7}, "fromChanges": {Anchor: 7}, "shadowed": {Anchor: 7}}

	_, err := New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, state, refs, nil, anchors.load)
	assert.NoError(err)
	assert.Equal("initial input", stateVal(state, "fromState"), "a field whose bytes are only in the anchor's state column must resolve")
	assert.Equal("task output", stateVal(state, "fromChanges"))
	assert.Equal("new", stateVal(state, "shadowed"), "changes shadows state - it is the newer of the two")
}

// TestLinker_ResolveReadsAnOverwriteBySuffix pins how the anchor's changes name a field the task overwrote:
// with the overwrite suffix. A carried field whose own name ends with the suffix is never read from such a key.
func TestLinker_ResolveReadsAnOverwriteBySuffix(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {
			State:   cols(map[string]any{"doc!": "the input doc", "doc": "old"}),
			Changes: cols(map[string]any{"doc!": "overwritten doc"}),
		},
	}}
	state := newState()
	_, err := New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, state, Refs{"doc": {Anchor: 7}, "doc!": {Anchor: 7}}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal("overwritten doc", stateVal(state, "doc"))
	assert.Equal("the input doc", stateVal(state, "doc!"))
}

// TestLinker_ResolveOneRoundTripPerBatch pins the cost model the free tier is priced on. Several fields at
// one anchor, and several distinct anchors, must all be fetched in ONE Loader call - if resolution degraded
// to one call per ref, "an already-fetched row's other fields are free" would stop being true and the
// per-anchor SUM test in Mint would be pricing something the reader does not actually do.
func TestLinker_ResolveOneRoundTripPerBatch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Changes: cols(map[string]any{"a": "A", "b": "B"})},
		9: {Changes: cols(map[string]any{"c": "C"})},
	}}
	state := newState()

	_, err := New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, state, Refs{"a": {Anchor: 7}, "b": {Anchor: 7}, "c": {Anchor: 9}}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal(1, len(anchors.calls), "three fields across two anchors must cost ONE load, not three")
	assert.Equal(2, len(anchors.calls[0]), "the batch asks for each DISTINCT anchor once")
	assert.Equal("A", stateVal(state, "a"))
	assert.Equal("C", stateVal(state, "c"))
}

// TestLinker_ResolveCachesBytesNotValues pins two things at once. A second resolve of the same field must not
// reach the Loader at all (in a fan-out every branch resolves the SAME anchor set, which is what makes the
// bar fall as 1/N). And each resolve must DECODE its own copy: caching a decoded value would alias one map
// across the branch goroutines about to hand it to concurrent tasks.
func TestLinker_ResolveCachesBytesNotValues(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Changes: cols(map[string]any{"doc": map[string]any{"page": "one"}})},
	}}
	linker := New("sqlite", 1, NewCache(testCacheBytes))

	first := newState()
	_, err := linker.Resolve(ctx, first, Refs{"doc": {Anchor: 7}}, nil, anchors.load)
	assert.NoError(err)
	second := newState()
	_, err = linker.Resolve(ctx, second, Refs{"doc": {Anchor: 7}}, nil, anchors.load)
	assert.NoError(err)

	assert.Equal(1, len(anchors.calls), "the second resolve of a settled anchor must be served from cache")
	firstMap, ok := stateVal(first, "doc").(map[string]any)
	assert.True(ok, "a resolved field is decoded, never a raw JSON message")
	secondMap, ok := stateVal(second, "doc").(map[string]any)
	assert.True(ok)
	// Mutating one branch's copy must not be visible to the other's.
	firstMap["page"] = "mutated"
	assert.Equal("one", secondMap["page"], "two resolves must not share a decoded map")
}

// TestLinker_ResolveAssertsOneHop pins the guard that keeps a violated invariant loud. A ref into a step that
// itself refs the same field is an error, NOT a chain to walk - walking would be the rejected delta design
// wearing a hat, and degrading silently into it is what the assertion exists to prevent.
func TestLinker_ResolveAssertsOneHop(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Refs: Refs{"doc": {Anchor: 3}}}, // holds no bytes for `doc` - it points onward
		3: {Changes: cols(map[string]any{"doc": "the payload"})},
	}}
	_, err := New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, newState(), Refs{"doc": {Anchor: 7}}, nil, anchors.load)
	assert.Error(err)
	assert.Contains(err.Error(), "one-hop")

	// A ref at an anchor that does not exist at all is likewise an error, never a silently absent field.
	_, err = New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, newState(), Refs{"doc": {Anchor: 404}}, nil, anchors.load)
	assert.Error(err)
}

// TestLinker_ResolveWantSelects pins the fan-in's read economy: only the fields a reducer folds are
// materialized, and a merely-CARRIED ref is left untouched for the caller's mint to re-emit. Materializing
// it here would re-anchor the payload at every fan-in and hand back the win in exactly the fan-out graphs
// the design exists for.
func TestLinker_ResolveWantSelects(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Changes: cols(map[string]any{"reduced": "R", "carried": "C"})},
	}}
	state := newState()
	refs := Refs{"reduced": {Anchor: 7}, "carried": {Anchor: 7}}

	err := New("sqlite", 1, NewCache(testCacheBytes)).ResolveReduced(ctx, state, refs, map[string]workflow.Reducer{"reduced": workflow.ReducerAppend}, anchors.load)
	assert.NoError(err)
	assert.Equal("R", stateVal(state, "reduced"), "a field a reducer folds must be materialized, or the fold loses its base")
	assert.False(state.Has("carried"), "a merely-carried ref must cross the fan-in as a ref")

	// No reducer registered for any ref'd field: nothing to fold, so nothing is read at all.
	anchors.calls = nil
	err = New("sqlite", 1, NewCache(testCacheBytes)).ResolveReduced(ctx, newState(), refs, map[string]workflow.Reducer{"other": workflow.ReducerAdd}, anchors.load)
	assert.NoError(err)
	assert.Equal(0, len(anchors.calls))
}

// TestLinker_FlattenSplicesWithoutDecoding pins the detached-snapshot form: every ref'd field is spliced back
// into the state document, and the payload is moved as bytes rather than decoded and re-encoded on the way
// through. A ref'd field is absent from the state column by construction, so splicing only ever ADDS keys.
func TestLinker_FlattenSplicesWithoutDecoding(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Changes: cols(map[string]any{"doc": []any{1, 2, 3}})},
	}}
	flat, err := New("sqlite", 1, NewCache(testCacheBytes)).Flatten(ctx, []byte(`{"kept":"inline"}`), Refs{"doc": {Anchor: 7}}, anchors.load)
	assert.NoError(err)

	var got map[string]any
	assert.NoError(json.Unmarshal(flat, &got))
	assert.Equal("inline", got["kept"], "a literal already in the state column survives the splice")
	assert.Equal([]any{1.0, 2.0, 3.0}, got["doc"], "the ref'd field is spliced back in")

	// With no refs the input document is returned untouched - the overwhelmingly common case allocates nothing.
	same, err := New("sqlite", 1, NewCache(testCacheBytes)).Flatten(ctx, []byte(`{"kept":"inline"}`), nil, anchors.load)
	assert.NoError(err)
	assert.Equal(`{"kept":"inline"}`, string(same))
}

// newState is an initialized empty State - the destination a Resolve writes into. The ZERO State is
// deliberately not that: it allocates nothing, so Resolve refuses it with an error rather than silently
// resolving into a value the caller cannot see.
func newState() workflow.State {
	s, _ := workflow.NewState()
	return s
}

// TestLinker_ConcurrentMissesReadOnce pins ResolveShared's single-flight. A cohort's branches miss on the same
// anchor at the same moment - under a backlog they leave the refiller together - so concurrent ResolveShared
// calls missing one field must read it once between them, and a read that fails must leave the waiters to
// read for themselves rather than fail with it. Resolve, the transaction-safe form, never waits.
func TestLinker_ConcurrentMissesReadOnce(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var mu sync.Mutex
	reads, failFirst := 0, false
	release := make(chan struct{})
	slow := func(ctx context.Context, anchorIDs []int) (map[int]Anchor, error) {
		mu.Lock()
		reads++
		fail := failFirst && reads == 1
		mu.Unlock()
		<-release // hold the read open until every fetch has missed
		if fail {
			return nil, errors.New("read failed")
		}
		return map[int]Anchor{7: {Changes: cols(map[string]any{"pages": []any{"a", "b", "c", "d"}})}}, nil
	}

	resolveAll := func(linker *Linker, n int, shared bool) []error {
		errs := make([]error, n)
		var wg sync.WaitGroup
		for i := range n {
			wg.Go(func() {
				state := newState()
				refs := Refs{"page": {Anchor: 7, Field: "pages", Path: []int{i % 4}}}
				if shared {
					_, errs[i] = linker.ResolveShared(ctx, state, refs, slow)
				} else {
					_, errs[i] = linker.Resolve(ctx, state, refs, nil, slow)
				}
				if errs[i] == nil && stateVal(state, "page") != []string{"a", "b", "c", "d"}[i%4] {
					errs[i] = errors.New("wrong element")
				}
			})
		}
		// Let every fetch miss and start reading or waiting before the held-open read completes.
		time.Sleep(50 * time.Millisecond)
		close(release)
		wg.Wait()
		return errs
	}

	for _, err := range resolveAll(New("sqlite", 1, NewCache(testCacheBytes)), 8, true) {
		assert.NoError(err)
	}
	assert.Equal(1, reads, "eight concurrent misses on one anchor read it once")

	// The read reaches its waiters even when the cache cannot keep the field: they take it from the read itself.
	mu.Lock()
	reads = 0
	mu.Unlock()
	release = make(chan struct{})
	tiny := New("sqlite", 1, NewCache(1))
	for _, err := range resolveAll(tiny, 8, true) {
		assert.NoError(err)
	}
	assert.Equal(1, reads, "a field too large to cache is still read once for concurrent misses")
	assert.Equal(int64(0), tiny.cacheBytes())

	// Resolve reads for itself: a caller inside a transaction must not wait on another goroutine.
	mu.Lock()
	reads = 0
	mu.Unlock()
	release = make(chan struct{})
	for _, err := range resolveAll(New("sqlite", 1, NewCache(testCacheBytes)), 8, false) {
		assert.NoError(err)
	}
	assert.Equal(8, reads, "Resolve never waits on a concurrent read")

	// A failed read errors its own fetch only; the waiters read the row themselves.
	mu.Lock()
	reads, failFirst = 0, true
	mu.Unlock()
	release = make(chan struct{})
	failed := 0
	for _, err := range resolveAll(New("sqlite", 1, NewCache(testCacheBytes)), 8, true) {
		if err != nil {
			failed++
		}
	}
	assert.Equal(1, failed, "only the fetch whose read failed sees the error")
}

// TestLinker_SeedServesTheMintedAnchor pins the write-side half of the cache: after Seed, a successor resolving
// what Mint anchored at the dispatched step reads nothing, including an item ref into an anchored array. A ref
// Mint carried from an earlier anchor is not seeded - this step's state is not where its bytes live.
func TestLinker_SeedServesTheMintedAnchor(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	const anchor = 42

	linker := New("sqlite", 1, NewCache(testCacheBytes))
	merged := mustState(map[string]any{
		"doc":     blob(2048),
		"pages":   []any{blob(600), blob(600)},
		"carried": blob(4096), // materialized at dispatch from anchor 7
		"small":   "s",
	})
	changes := workflow.ChangesOf(mustState(map[string]any{"doc": blob(2048), "pages": []any{blob(600), blob(600)}}))
	_, refsJSON, err := linker.Mint(merged, changes, Refs{"carried": {Anchor: 7}}, anchor, 1, nil, nil)
	assert.NoError(err)
	refs := mustParse(refsJSON)
	assert.Equal(anchor, refs["doc"].Anchor)
	assert.Equal(anchor, refs["pages"].Anchor)
	assert.Equal(7, refs["carried"].Anchor)

	linker.Seed(anchor, merged, refsJSON)

	anchors := &fakeAnchors{rows: map[int]Anchor{}}
	state := newState()
	_, err = linker.Resolve(ctx, state, Refs{"doc": {Anchor: anchor}, "pages": {Anchor: anchor}}, nil, anchors.load)
	assert.NoError(err)
	assert.Len(anchors.calls, 0, "a seeded anchor must be served without a read")
	assert.Equal(blob(2048), state.GetString("doc"))

	item := newState()
	_, err = linker.Resolve(ctx, item, Refs{"page": {Anchor: anchor, Field: "pages", Path: []int{1}}}, nil, anchors.load)
	assert.NoError(err)
	assert.Len(anchors.calls, 0, "an item ref into a seeded array must be served without a read")
	assert.Equal(blob(600), item.GetString("page"))

	// The carried ref's anchor was not seeded: resolving it goes to the Loader.
	anchors.rows[7] = Anchor{State: cols(map[string]any{"carried": blob(4096)})}
	_, err = linker.Resolve(ctx, newState(), Refs{"carried": {Anchor: 7}}, nil, anchors.load)
	assert.NoError(err)
	assert.Len(anchors.calls, 1)

	// Nothing to seed is a no-op, malformed refs included.
	linker.Seed(anchor, merged, []byte("{}"))
	linker.Seed(anchor, merged, []byte("not json"))
}

// TestLinker_ResolveRefusesAZeroState pins that refs with no state to resolve into are an error: a state that
// failed to load would otherwise run its caller on input missing every field the refs point at.
func TestLinker_ResolveRefusesAZeroState(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	load := func(ctx context.Context, ids []int) (map[int]Anchor, error) {
		return map[int]Anchor{7: {State: map[string]json.RawMessage{"doc": json.RawMessage(`"d"`)}}}, nil
	}
	_, err := New("sqlite", 1, NewCache(testCacheBytes)).Resolve(context.Background(), workflow.State{}, Refs{"doc": {Anchor: 7}}, nil, load)
	assert.Error(err)
	n, err := New("sqlite", 1, NewCache(testCacheBytes)).Resolve(context.Background(), workflow.State{}, nil, nil, load)
	assert.NoError(err, "a zero state with no refs has nothing to resolve")
	assert.Equal(0, n)
}
