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
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

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
	refs := Refs{"fromState": 7, "fromChanges": 7, "shadowed": 7}

	_, err := New("sqlite").Resolve(ctx, state, refs, nil, anchors.load)
	assert.NoError(err)
	assert.Equal("initial input", state.Value("fromState"), "a field whose bytes are only in the anchor's state column must resolve")
	assert.Equal("task output", state.Value("fromChanges"))
	assert.Equal("new", state.Value("shadowed"), "changes shadows state - it is the newer of the two")
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

	_, err := New("sqlite").Resolve(ctx, state, Refs{"a": 7, "b": 7, "c": 9}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal(1, len(anchors.calls), "three fields across two anchors must cost ONE load, not three")
	assert.Equal(2, len(anchors.calls[0]), "the batch asks for each DISTINCT anchor once")
	assert.Equal("A", state.Value("a"))
	assert.Equal("C", state.Value("c"))
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
	linker := New("sqlite")

	first := newState()
	_, err := linker.Resolve(ctx, first, Refs{"doc": 7}, nil, anchors.load)
	assert.NoError(err)
	second := newState()
	_, err = linker.Resolve(ctx, second, Refs{"doc": 7}, nil, anchors.load)
	assert.NoError(err)

	assert.Equal(1, len(anchors.calls), "the second resolve of a settled anchor must be served from cache")
	firstMap, ok := first.Value("doc").(map[string]any)
	assert.True(ok, "a resolved field is decoded, never a raw JSON message")
	secondMap, ok := second.Value("doc").(map[string]any)
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
		7: {Refs: Refs{"doc": 3}}, // holds no bytes for `doc` - it points onward
		3: {Changes: cols(map[string]any{"doc": "the payload"})},
	}}
	_, err := New("sqlite").Resolve(ctx, newState(), Refs{"doc": 7}, nil, anchors.load)
	assert.Error(err)
	assert.Contains(err.Error(), "one-hop")

	// A ref at an anchor that does not exist at all is likewise an error, never a silently absent field.
	_, err = New("sqlite").Resolve(ctx, newState(), Refs{"doc": 404}, nil, anchors.load)
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
	refs := Refs{"reduced": 7, "carried": 7}

	err := New("sqlite").ResolveReduced(ctx, state, refs, map[string]workflow.Reducer{"reduced": workflow.ReducerAppend}, anchors.load)
	assert.NoError(err)
	assert.Equal("R", state.Value("reduced"), "a field a reducer folds must be materialized, or the fold loses its base")
	assert.False(state.Contains("carried"), "a merely-carried ref must cross the fan-in as a ref")

	// No reducer registered for any ref'd field: nothing to fold, so nothing is read at all.
	anchors.calls = nil
	err = New("sqlite").ResolveReduced(ctx, newState(), refs, map[string]workflow.Reducer{"other": workflow.ReducerAdd}, anchors.load)
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
	flat, err := New("sqlite").Flatten(ctx, []byte(`{"kept":"inline"}`), Refs{"doc": 7}, anchors.load)
	assert.NoError(err)

	var got map[string]any
	assert.NoError(json.Unmarshal(flat, &got))
	assert.Equal("inline", got["kept"], "a literal already in the state column survives the splice")
	assert.Equal([]any{1.0, 2.0, 3.0}, got["doc"], "the ref'd field is spliced back in")

	// With no refs the input document is returned untouched - the overwhelmingly common case allocates nothing.
	same, err := New("sqlite").Flatten(ctx, []byte(`{"kept":"inline"}`), nil, anchors.load)
	assert.NoError(err)
	assert.Equal(`{"kept":"inline"}`, string(same))
}

// newState is an initialized empty State - the destination a Resolve writes into. The ZERO State is
// deliberately not that: it allocates nothing, so Resolve declines it rather than silently resolving into
// a value the caller cannot see.
func newState() workflow.State {
	s, _ := workflow.NewState()
	return s
}
