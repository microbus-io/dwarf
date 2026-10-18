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

// TestRef_Grammar pins the one ref grammar every form shares: a plain ref, a column, a field and index path,
// a range at the end of the path, and a list of entries joined by ';'. Today's two spellings are sentences of
// it with their meaning unchanged.
func TestRef_Grammar(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	parse := func(s string) Ref {
		var r Ref
		assert.NoError(json.Unmarshal([]byte(s), &r), s)
		return r
	}
	marshal := func(r Ref) string {
		data, err := json.Marshal(r)
		assert.NoError(err)
		return string(data)
	}

	cases := []struct {
		in   string
		ref  Ref
		back string
	}{
		{`1234`, Ref{Anchor: 1234}, `1234`},
		{`"1234c"`, Ref{Anchor: 1234, Col: ColChanges}, `"1234c"`},
		{`"1234s"`, Ref{Anchor: 1234, Col: ColState}, `"1234s"`},
		{`"1234 pages[5]"`, Ref{Anchor: 1234, Field: "pages", Path: []int{5}}, `"1234 pages[5]"`},
		{`"1234c pages[0:2]"`, Ref{Anchor: 1234, Col: ColChanges, Field: "pages", Span: &Span{From: 0, To: 2}}, `"1234c pages[:2]"`},
		{`"1234c[3:]"`, Ref{Anchor: 1234, Col: ColChanges, Span: &Span{From: 3, To: -1}}, `"1234c[3:]"`},
		{`"1234 [1]"`, Ref{Anchor: 1234, Path: []int{1}}, `"1234[1]"`},
		{`"9 data[3][0:4]"`, Ref{Anchor: 9, Field: "data", Path: []int{3}, Span: &Span{From: 0, To: 4}}, `"9 data[3][:4]"`},
		{`"1201s[:2];1202c[:];s[0];1204c"`, Ref{List: []Ref{
			{Anchor: 1201, Col: ColState, Span: &Span{From: 0, To: 2}},
			{Anchor: 1202, Col: ColChanges, Span: &Span{From: 0, To: -1}},
			{Col: ColState, Path: []int{0}},
			{Anchor: 1204, Col: ColChanges},
		}}, `"1201s[:2];1202c[:];s[0];1204c"`},
		{`"1204c;"`, Ref{List: []Ref{{Anchor: 1204, Col: ColChanges}}}, `"1204c;"`},
	}
	for _, c := range cases {
		r := parse(c.in)
		assert.Equal(c.ref, r, c.in)
		assert.Equal(c.back, marshal(r), c.in)
	}

	for _, bad := range []string{
		`"s[1]"`,       // this step's own state, outside a list
		`"c[1];7"`,     // a self entry names the state column only
		`"s;7"`,        // a self entry needs an index or range
		`"1234c[2:1]"`, // a range that runs backwards
		`"1234[1:2][3]"`,
		`"1234x"`,
		`";"`,
		`"7;;8"`,
		`"1234 a b[1]"`,
		`"1234 [1:x]"`,
	} {
		var r Ref
		assert.Error(json.Unmarshal([]byte(bad), &r), bad)
	}
}

// TestLinker_ResolveList pins how a list resolves: its entries concatenated in order, each read from the
// column it names - a range adding its elements, an entry without one adding its value as one element, and a
// self entry reading the elements stored inline in the resolving step's own state. One Loader call serves
// every anchor, and each is asked only for the columns its entries name.
func TestLinker_ResolveList(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		10: {State: cols(map[string]any{"log": []string{"a", "b"}})},
		11: {Changes: cols(map[string]any{"log": []string{"c"}})},
		12: {Changes: cols(map[string]any{"log": "d"})},
		13: {Changes: cols(map[string]any{"log!": []string{"x", "y", "z"}})},
		14: {State: cols(map[string]any{"log": []string{"s1"}}), Changes: cols(map[string]any{"log": []string{"c1"}})},
	}}
	var list Ref
	assert.NoError(json.Unmarshal([]byte(`"10s[:2];11c[:1];s[0];12c;13c[1:];14s[:1];14c[:1]"`), &list))
	want := []any{"a", "b", "c", "inline", "d", "y", "z", "s1", "c1"}

	state := newState()
	state.SetJSON("log", json.RawMessage(`["inline"]`))
	_, err := New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, state, Refs{"log": list}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal(want, stateVal(state, "log"))
	assert.Equal(1, len(anchors.calls), "every anchor of the list in one Loader call")
	byID := map[int]Read{}
	for _, r := range anchors.reads {
		byID[r.ID] = r
	}
	assert.Equal(Read{ID: 10, State: true}, byID[10], "a state entry reads no changes")
	assert.Equal(Read{ID: 11, Changes: true}, byID[11], "a changes entry reads no state - which holds that step's own list")
	assert.Equal(Read{ID: 14, State: true, Changes: true}, byID[14])

	// Flatten splices the same array into the state document.
	flat, err := New("sqlite", 1, NewCache(testCacheBytes)).Flatten(ctx, []byte(`{"log":["inline"],"n":1}`), Refs{"log": list}, anchors.load)
	assert.NoError(err)
	var doc map[string]any
	assert.NoError(json.Unmarshal(flat, &doc))
	assert.Equal(want, doc["log"])
	assert.Equal(float64(1), doc["n"])

	// A range past the array's end is an error, never a shorter array.
	var past Ref
	assert.NoError(json.Unmarshal([]byte(`"11c[:2];10s[:2]"`), &past))
	_, err = New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, newState(), Refs{"log": past}, nil, anchors.load)
	assert.Error(err)
}

// TestLinker_MintFoldList pins how a folded field is stored past the bar: the input's elements where they
// already are, then the step's contribution in its changes - never a copy of the value.
func TestLinker_MintFoldList(t *testing.T) {
	t.Parallel()
	const step = 42

	msgs := func(n int) []any {
		out := make([]any, n)
		for i := range out {
			out[i] = map[string]any{"n": i, "text": blob(300)}
		}
		return out
	}
	mint := func(t *testing.T, merged workflow.State, changes workflow.Changes, inherited Refs, folds map[string]Fold) (workflow.State, Refs) {
		t.Helper()
		assert := testarossa.For(t)
		stateJSON, refsJSON, err := New("sqlite", 1, NewCache(testCacheBytes)).Mint(merged, changes, inherited, step, Linear, nil, nil, folds)
		assert.NoError(err)
		var stored workflow.State
		assert.NoError(stored.UnmarshalJSON(stateJSON))
		return stored, mustParse(refsJSON)
	}
	contributed := func() workflow.Changes {
		c := workflow.NewChanges()
		c.Contributions().SetJSON("log", json.RawMessage(`[]`))
		return c
	}
	spelled := func(t *testing.T, r Ref) string {
		t.Helper()
		data, _ := json.Marshal(r)
		return string(data)
	}

	t.Run("base_inline_here", func(t *testing.T) {
		assert := testarossa.For(t)
		stored, refs := mint(t, mustState(map[string]any{"log": msgs(5)}), contributed(), nil, map[string]Fold{"log": {Base: 3, Parts: []Part{{Step: step, Added: 2}}}})
		assert.Equal(`"42s[:3];42c[:2]"`, spelled(t, refs["log"]))
		assert.False(stored.Has("log"), "the folded value is not copied")
	})

	t.Run("base_by_ref", func(t *testing.T) {
		assert := testarossa.For(t)
		_, refs := mint(t, mustState(map[string]any{"log": msgs(5)}), contributed(), Refs{"log": {Anchor: 7}}, map[string]Fold{"log": {Base: 3, Parts: []Part{{Step: step, Added: 2}}}})
		assert.Equal(`"7[:3];42c[:2]"`, spelled(t, refs["log"]), "the base is read where the input's ref points, by the same lookup")
	})

	t.Run("base_is_a_list", func(t *testing.T) {
		assert := testarossa.For(t)
		var base Ref
		assert.NoError(json.Unmarshal([]byte(`"7c[:2];s[0]"`), &base))
		_, refs := mint(t, mustState(map[string]any{"log": msgs(4)}), contributed(), Refs{"log": base}, map[string]Fold{"log": {Base: 3, Parts: []Part{{Step: step, Single: true}}}})
		assert.Equal(`"7c[:2];42s[0];42c"`, spelled(t, refs["log"]), "a self entry of the input names the input's step once carried")
	})

	t.Run("overwrite", func(t *testing.T) {
		assert := testarossa.For(t)
		_, refs := mint(t, mustState(map[string]any{"log": msgs(4)}), contributed(), Refs{"log": {Anchor: 7}}, map[string]Fold{"log": {Base: 2, Parts: []Part{{Step: step, Overwrite: true, Added: 4}}}})
		assert.Equal(`"42c[:4];"`, spelled(t, refs["log"]), "a list of one entry is marked as a list")
	})

	t.Run("parts_from_several_steps", func(t *testing.T) {
		assert := testarossa.For(t)
		// A fan-in: the spawn's own contribution, then each branch's, in fold order.
		_, refs := mint(t, mustState(map[string]any{"log": msgs(6)}), contributed(), Refs{"log": {Anchor: 7}}, map[string]Fold{"log": {Base: 2, Parts: []Part{
			{Step: step, Added: 1}, {Step: 50, Single: true}, {Step: 51, Added: 2},
		}}})
		assert.Equal(`"7[:2];42c[:1];50c;51c[:2]"`, spelled(t, refs["log"]))
	})

	t.Run("overwrite_drops_what_came_before", func(t *testing.T) {
		assert := testarossa.For(t)
		_, refs := mint(t, mustState(map[string]any{"log": msgs(4)}), contributed(), Refs{"log": {Anchor: 7}}, map[string]Fold{"log": {Base: 4, Parts: []Part{
			{Step: 50, Added: 1}, {Step: step, Overwrite: true, Added: 3}, {Step: 51, Single: true},
		}}})
		assert.Equal(`"42c[:3];51c"`, spelled(t, refs["log"]))
	})

	t.Run("small_stays_inline", func(t *testing.T) {
		assert := testarossa.For(t)
		stored, refs := mint(t, mustState(map[string]any{"log": []any{"a", "b"}}), contributed(), nil, map[string]Fold{"log": {Base: 1, Parts: []Part{{Step: step, Added: 1}}}})
		assert.NotContains(refs, "log")
		assert.True(stored.Has("log"))
	})

	t.Run("count_mismatch_stays_inline", func(t *testing.T) {
		assert := testarossa.For(t)
		stored, refs := mint(t, mustState(map[string]any{"log": msgs(5)}), contributed(), nil, map[string]Fold{"log": {Base: 3, Parts: []Part{{Step: step, Added: 5}}}})
		assert.NotContains(refs, "log", "a list that does not account for every element is never written")
		assert.True(stored.Has("log"))
	})

	t.Run("carried_list_names_the_step", func(t *testing.T) {
		assert := testarossa.For(t)
		var base Ref
		assert.NoError(json.Unmarshal([]byte(`"7c[:2];s[0:2]"`), &base))
		_, refs := mint(t, mustState(map[string]any{"other": 1}), workflow.NewChanges(), Refs{"log": base}, nil)
		assert.Equal(`"7c[:2];42s[:2]"`, spelled(t, refs["log"]))
	})
}

// TestLinker_MintItemThroughList pins a forEach over an array stored as a list: each element points at the row
// the entry holding it names, never at the list (one hop), and stays inline where the ref cannot say where it
// is.
func TestLinker_MintItemThroughList(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	elements := make([]any, 6)
	for i := range elements {
		elements[i] = map[string]any{"n": i, "text": blob(200)}
	}
	mint := func(list string, index int) (workflow.State, Refs) {
		var src Ref
		assert.NoError(json.Unmarshal([]byte(list), &src))
		merged := mustState(map[string]any{"items": elements, "item": elements[index]})
		stateJSON, refsJSON, err := New("sqlite", 1, NewCache(testCacheBytes)).Mint(merged, workflow.NewChanges(), Refs{"items": src}, 42, len(elements), nil, &Item{Key: "item", Source: "items", Index: index}, nil)
		assert.NoError(err)
		var stored workflow.State
		assert.NoError(stored.UnmarshalJSON(stateJSON))
		return stored, mustParse(refsJSON)
	}

	_, refs := mint(`"7c[:2];8c[1:4];9c"`, 3)
	assert.Equal(Ref{Anchor: 8, Col: ColChanges, Field: "items", Path: []int{2}}, refs["item"])
	_, refs = mint(`"7c[:2];8c[1:4];9c"`, 5)
	assert.Equal(Ref{Anchor: 9, Col: ColChanges, Field: "items"}, refs["item"], "a one-element entry's value is the element")

	stored, refs := mint(`"7c[:];8c"`, 4)
	assert.NotContains(refs, "item", "past an open range the index cannot be placed")
	assert.True(stored.Has("item"))
}

// TestLinker_SeedList pins that the minting replica caches a step's contribution under the key a list's entry
// reads it by, so the successor's resolve reads only the rows the minting step did not write.
func TestLinker_SeedList(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	linker := New("sqlite", 1, NewCache(testCacheBytes))
	changes := workflow.NewChanges()
	changes.Contributions().SetJSON("log", json.RawMessage(`["c","d"]`))
	merged := mustState(map[string]any{"log": []string{"a", "b", "c", "d"}})
	linker.Seed(42, merged, changes, []byte(`{"log":"7[:2];42c[:2]"}`))

	anchors := &fakeAnchors{rows: map[int]Anchor{7: {Changes: cols(map[string]any{"log": []string{"a", "b"}})}}}
	var list Ref
	assert.NoError(json.Unmarshal([]byte(`"7[:2];42c[:2]"`), &list))
	state := newState()
	_, err := linker.Resolve(ctx, state, Refs{"log": list}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal([]any{"a", "b", "c", "d"}, stateVal(state, "log"))
	assert.Equal([][]int{{7}}, anchors.calls, "the seeded contribution is not read")
}

// TestRef_Remap pins Fork's remap of a ref through its clone id map: every anchor a ref names, a list's entries
// included, maps, a self entry stays this step's, and an anchor outside the map is reported.
func TestRef_Remap(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	ids := map[int]int{7: 107, 8: 108}
	m := func(a int) (int, bool) { to, ok := ids[a]; return to, ok }
	var list Ref
	assert.NoError(json.Unmarshal([]byte(`"7c[:2];s[0];8c"`), &list))
	got, _, ok := list.Remap(m)
	assert.True(ok)
	data, _ := json.Marshal(got)
	assert.Equal(`"107c[:2];s[0];108c"`, string(data))
	got, _, ok = Ref{Anchor: 8, Field: "pages", Path: []int{2}}.Remap(m)
	assert.True(ok)
	assert.Equal(Ref{Anchor: 108, Field: "pages", Path: []int{2}}, got)
	assert.NoError(json.Unmarshal([]byte(`"7c[:2];9c"`), &list))
	_, missing, ok := list.Remap(m)
	assert.False(ok)
	assert.Equal(9, missing)
}

// TestInlineExcessAnchors_SkipsLists pins that a list neither counts toward maxAnchors nor is inlined: it names
// an anchor per contributing step by design, and its literal is not in hand to inline anyway.
func TestInlineExcessAnchors_SkipsLists(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	refs := Refs{"log": {List: []Ref{{Anchor: 1, Col: ColChanges}, {Anchor: 2, Col: ColChanges}, {Anchor: 3, Col: ColChanges}, {Anchor: 4, Col: ColChanges}, {Anchor: 5, Col: ColChanges}}}}
	fields := map[string]any{}
	for i := range maxAnchors {
		name := string(rune('a' + i))
		refs[name] = Ref{Anchor: 100 + i}
		fields[name] = blob(10)
	}
	raw := map[string]json.RawMessage{}
	inlineExcessAnchors(raw, refs, mustState(fields), threshold)
	assert.Equal(maxAnchors+1, len(refs), "at maxAnchors plain anchors nothing is inlined, whatever the list names")
	assert.Contains(refs, "log")
}

// TestLinker_ResolveReadsAnAddition pins the addition suffix in an anchor's changes: a list entry reading the
// changes column ranges over the elements the step gained, stored under "field+", while a whole-field ref never
// resolves to them - they are not the field's value.
func TestLinker_ResolveReadsAnAddition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		15: {State: cols(map[string]any{"log": []string{"a"}}), Changes: cols(map[string]any{"log+": []string{"x", "y"}})},
	}}
	var list Ref
	assert.NoError(json.Unmarshal([]byte(`"15s[:1];15c[:2]"`), &list))
	state := newState()
	_, err := New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, state, Refs{"log": list}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal([]any{"a", "x", "y"}, stateVal(state, "log"))

	whole := newState()
	_, err = New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, whole, Refs{"log": {Anchor: 15}}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal([]any{"a"}, stateVal(whole, "log"), "a whole-field ref reads the field, not what a step added to it")
}
