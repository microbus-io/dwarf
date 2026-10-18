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
	"fmt"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestRef_WireFormat pins the refs column's two spellings. A plain ref stays a bare number, so a column
// holding no item refs is byte-identical to one written before item refs existed; an item ref is a string
// that names its anchor, the array's field there, and the index path.
func TestRef_WireFormat(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	data, err := json.Marshal(Refs{"theArray": {Anchor: 1234}, "item": {Anchor: 1234, Field: "theArray", Path: []int{5}}})
	assert.NoError(err)
	assert.Equal(`{"item":"1234 theArray[5]","theArray":1234}`, string(data))

	// Both spellings read back, and a column written with plain refs only parses unchanged.
	assert.Equal(Refs{"theArray": {Anchor: 1234}, "item": {Anchor: 1234, Field: "theArray", Path: []int{5}}}, mustParse(data))
	assert.Equal(Refs{"doc": {Anchor: 7}}, mustParse([]byte(`{"doc":7}`)))

	// The format holds any depth of index path, so a deeper ref never needs a new spelling.
	var deep Ref
	assert.NoError(json.Unmarshal([]byte(`"9 data[3][0][12]"`), &deep))
	assert.Equal(Ref{Anchor: 9, Field: "data", Path: []int{3, 0, 12}}, deep)
	again, err := json.Marshal(deep)
	assert.NoError(err)
	assert.Equal(`"9 data[3][0][12]"`, string(again))

	// A column this version cannot read is an error, never "no refs": the fields it points at would be lost.
	for _, col := range []string{`{"item":"x"}`, `{"doc":true}`, `[1]`, `{`} {
		_, err := Parse([]byte(col))
		assert.Error(err, col)
	}
	for _, empty := range []string{``, ` `, `{}`} {
		refs, err := Parse([]byte(empty))
		assert.NoError(err)
		assert.Nil(refs)
	}

	for _, bad := range []string{`""`, `"data[1]"`, `"x data[1]"`, `"0 data[1]"`, `"9 data[1"`, `"9 data[-1]"`, `"9 data[1]x"`, `"9 data[]"`, `true`} {
		var r Ref
		assert.Error(json.Unmarshal([]byte(bad), &r), bad)
	}
}

// TestLinker_MintItem pins when a forEach element becomes an item ref. It does exactly when its source
// array is stored by a ref and the element outweighs the ref; the ref then points at whichever step holds
// the array's bytes, never at a step merely carrying a ref to them.
func TestLinker_MintItem(t *testing.T) {
	t.Parallel()
	const anchor = 42
	const width = 64

	// branch builds a fan-out branch's merged state: the source array plus element i under "item".
	elements := make([]any, width)
	for i := range elements {
		elements[i] = map[string]any{"page": i, "text": blob(200)}
	}
	branch := func(i int) workflow.State {
		return mustState(map[string]any{"items": elements, "item": elements[i], "itemIndex": i, "itemCount": width})
	}
	ordinals := map[string]bool{"itemIndex": true, "itemCount": true}
	mint := func(t *testing.T, merged workflow.State, inherited Refs, item *Item) (workflow.State, Refs) {
		t.Helper()
		assert := testarossa.For(t)
		stateJSON, refsJSON, err := New("sqlite", 1, NewCache(testCacheBytes)).Mint(merged, workflow.NewChanges(), inherited, anchor, width, ordinals, item, nil)
		assert.NoError(err)
		var stored workflow.State
		_ = stored.UnmarshalJSON(stateJSON)
		return stored, mustParse(refsJSON)
	}

	t.Run("array_anchored_here", func(t *testing.T) {
		assert := testarossa.For(t)
		stored, refs := mint(t, branch(5), nil, &Item{Key: "item", Source: "items", Index: 5})
		assert.Equal(Ref{Anchor: anchor}, refs["items"])
		assert.Equal(Ref{Anchor: anchor, Field: "items", Path: []int{5}}, refs["item"])
		assert.False(stored.Has("item"), "the element is stored once, inside the array")
		assert.True(stored.Has("itemIndex"))
		assert.True(stored.Has("itemCount"))
	})

	t.Run("array_carried_by_ref", func(t *testing.T) {
		assert := testarossa.For(t)
		// The array arrived as a ref to step 7: the spawn's row does not hold it, so the element points at 7.
		_, refs := mint(t, branch(5), Refs{"items": {Anchor: 7}}, &Item{Key: "item", Source: "items", Index: 5})
		assert.Equal(Ref{Anchor: 7}, refs["items"])
		assert.Equal(Ref{Anchor: 7, Field: "items", Path: []int{5}}, refs["item"])
	})

	t.Run("small_element_stays_inline", func(t *testing.T) {
		assert := testarossa.For(t)
		ints := make([]any, width)
		for i := range ints {
			ints[i] = i
		}
		merged := mustState(map[string]any{"items": ints, "item": 5, "itemIndex": 5, "itemCount": width})
		stored, refs := mint(t, merged, Refs{"items": {Anchor: 7}}, &Item{Key: "item", Source: "items", Index: 5})
		assert.NotContains(refs, "item", "an element smaller than its ref is cheaper inline")
		assert.True(stored.Has("item"))
	})

	t.Run("inline_array_keeps_element_inline", func(t *testing.T) {
		assert := testarossa.For(t)
		// A narrow fan-out of a small array: the array stays inline, so there is no anchor to point into.
		few := []any{map[string]any{"text": blob(150)}, map[string]any{"text": blob(150)}}
		merged := mustState(map[string]any{"items": few, "item": few[1]})
		stateJSON, refsJSON, err := New("sqlite", 1, NewCache(testCacheBytes)).Mint(merged, workflow.NewChanges(), nil, anchor, 2, nil, &Item{Key: "item", Source: "items", Index: 1}, nil)
		assert.NoError(err)
		refs := mustParse(refsJSON)
		assert.NotContains(refs, "items")
		assert.NotContains(refs, "item")
		var stored workflow.State
		_ = stored.UnmarshalJSON(stateJSON)
		assert.True(stored.Has("item"))
	})

	t.Run("element_of_an_element_extends_the_path", func(t *testing.T) {
		assert := testarossa.For(t)
		// An inner forEach over the outer element: the source is itself an item ref, so the inner element's
		// ref extends its path and points into the same anchor. The outer element is carried as it was.
		outer := Ref{Anchor: 7, Field: "rows", Path: []int{2}}
		merged := mustState(map[string]any{"row": elements, "cell": elements[3]})
		stored, refs := mint(t, merged, Refs{"row": outer}, &Item{Key: "cell", Source: "row", Index: 3})
		assert.Equal(outer, refs["row"], "the outer item ref is carried forward unchanged")
		assert.Equal(Ref{Anchor: 7, Field: "rows", Path: []int{2, 3}}, refs["cell"])
		assert.False(stored.Has("cell"))

		// The inner ref's path is its own: extending it never writes into the outer ref's backing array.
		roomy := Ref{Anchor: 7, Field: "rows", Path: append(make([]int, 0, 8), 2)}
		_, a := mint(t, merged, Refs{"row": roomy}, &Item{Key: "cell", Source: "row", Index: 3})
		_, b := mint(t, merged, Refs{"row": roomy}, &Item{Key: "cell", Source: "row", Index: 4})
		assert.Equal([]int{2, 3}, a["cell"].Path)
		assert.Equal([]int{2, 4}, b["cell"].Path)
	})

	t.Run("as_equals_foreach", func(t *testing.T) {
		assert := testarossa.For(t)
		// forEach(items) as items: the element overwrote the array, so there is no array left to point into.
		merged := mustState(map[string]any{"items": elements[5]})
		stored, refs := mint(t, merged, Refs{"items": {Anchor: 7}}, &Item{Key: "items", Source: "items", Index: 5})
		assert.NotContains(refs, "items", "the element is not the array, so the array's ref must not be carried onto it")
		assert.True(stored.Has("items"))
	})

	t.Run("outer_item_carried_into_inner_fan_out", func(t *testing.T) {
		assert := testarossa.For(t)
		// An inner fan-out over a separately computed array: the outer element rides through as its item ref,
		// and the inner element points into the inner array's anchor.
		page := Ref{Anchor: 7, Field: "pages", Path: []int{4}}
		merged := mustState(map[string]any{"page": elements[4], "lines": elements, "line": elements[9]})
		_, refs := mint(t, merged, Refs{"page": page}, &Item{Key: "line", Source: "lines", Index: 9})
		assert.Equal(page, refs["page"])
		assert.Equal(Ref{Anchor: anchor, Field: "lines", Path: []int{9}}, refs["line"])
	})

	t.Run("over_the_cap_inlines_array_and_element_together", func(t *testing.T) {
		assert := testarossa.For(t)
		// The item ref is grouped under its array's anchor, so when that anchor is the one the cap drops, the
		// array and the element are inlined together and neither is left pointing at a dropped anchor. The
		// array and element together must be below the bar (384 bytes at this width) to be dropped at all.
		small := []any{map[string]any{"page": 0, "text": blob(100)}, map[string]any{"page": 1, "text": blob(100)}}
		merged := mustState(map[string]any{"items": small, "item": small[1], "itemIndex": 1, "itemCount": 2})
		inherited := Refs{"items": {Anchor: 7}}
		for i := range maxAnchors {
			field := fmt.Sprintf("big%d", i)
			_ = merged.Set(field, blob(100_000))
			inherited[field] = Ref{Anchor: 100 + i}
		}
		stored, refs := mint(t, merged, inherited, &Item{Key: "item", Source: "items", Index: 1})
		assert.NotContains(refs, "items")
		assert.NotContains(refs, "item")
		assert.True(stored.Has("items"))
		assert.True(stored.Has("item"))
		assert.Equal(maxAnchors, len(refs))
	})
}

// TestLinker_ResolveItem pins the read side: an item ref resolves to its element's bytes, found under the
// array's field name at the anchor, and a cohort's branches split the array once between them.
func TestLinker_ResolveItem(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {
			State:   cols(map[string]any{"initial": []any{"a", "b"}}),
			Changes: cols(map[string]any{"pages": []any{map[string]any{"n": 0}, map[string]any{"n": 1}, map[string]any{"n": 2}}}),
		},
	}}
	linker := New("sqlite", 1, NewCache(testCacheBytes))
	for i := range 3 {
		state := newState()
		_, err := linker.Resolve(ctx, state, Refs{"page": {Anchor: 7, Field: "pages", Path: []int{i}}, "pages": {Anchor: 7}}, nil, anchors.load)
		assert.NoError(err)
		assert.Equal(map[string]any{"n": float64(i)}, stateVal(state, "page"))
		assert.Len(stateVal(state, "pages"), 3, "the array and its element come from the same anchor read")
	}
	assert.Equal(1, len(anchors.calls), "every branch's element is served from the one cached array")

	// An array already in the bytes cache as a plain ref is split for its first item ref without a read.
	warm := New("sqlite", 1, NewCache(testCacheBytes))
	anchors.calls = nil
	_, err := warm.Resolve(ctx, newState(), Refs{"pages": {Anchor: 7}}, nil, anchors.load)
	assert.NoError(err)
	state := newState()
	_, err = warm.Resolve(ctx, state, Refs{"page": {Anchor: 7, Field: "pages", Path: []int{1}}}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal(map[string]any{"n": float64(1)}, stateVal(state, "page"))
	assert.Equal(1, len(anchors.calls))

	// The element's array may sit in the anchor's state column, like any ref'd field.
	state = newState()
	_, err = linker.Resolve(ctx, state, Refs{"x": {Anchor: 7, Field: "initial", Path: []int{1}}}, nil, anchors.load)
	assert.NoError(err)
	assert.Equal("b", stateVal(state, "x"))

	// Flatten splices the element, exactly as Resolve does.
	flat, err := linker.Flatten(ctx, []byte(`{}`), Refs{"page": {Anchor: 7, Field: "pages", Path: []int{2}}}, anchors.load)
	assert.NoError(err)
	assert.Equal(`{"page":{"n":2}}`, string(flat))

	// An index past the end, a field that is not an array, and a path deeper than the array nests are
	// errors, never a silently absent field.
	_, err = New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, newState(), Refs{"page": {Anchor: 7, Field: "pages", Path: []int{3}}}, nil, anchors.load)
	assert.Error(err)
	anchors.rows[8] = Anchor{Changes: cols(map[string]any{"doc": "not an array"})}
	_, err = New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, newState(), Refs{"page": {Anchor: 8, Field: "doc", Path: []int{0}}}, nil, anchors.load)
	assert.Error(err)
	_, err = New("sqlite", 1, NewCache(testCacheBytes)).Resolve(ctx, newState(), Refs{"page": {Anchor: 7, Field: "pages", Path: []int{0, 0}}}, nil, anchors.load)
	assert.Error(err)
}

// TestLinker_ResolveNestedItem pins a multi-index path: each level indexes into the element the previous
// one selected, all inside the one anchor, and each inner array is split once for all the refs into it.
func TestLinker_ResolveNestedItem(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Changes: map[string]json.RawMessage{"data": json.RawMessage(`[[1,2,3],[4,[5,{"six":6}]]]`)}},
	}}
	linker := New("sqlite", 1, NewCache(testCacheBytes))
	cases := []struct {
		path []int
		want string
	}{
		{[]int{0, 2}, `3`},
		{[]int{1, 0}, `4`},
		{[]int{1, 1}, `[5,{"six":6}]`},
		{[]int{1, 1, 1}, `{"six":6}`},
		{[]int{0, 0}, `1`},
	}
	for _, c := range cases {
		flat, err := linker.Flatten(ctx, []byte(`{}`), Refs{"x": {Anchor: 7, Field: "data", Path: c.path}}, anchors.load)
		assert.NoError(err)
		assert.Equal(`{"x":`+c.want+`}`, string(flat), c.path)
	}
	assert.Equal(1, len(anchors.calls))
	entry, ok := linker.cache.Load(linker.key(7, "data", ColAny))
	assert.True(ok)
	_, ok = entry.splits["data[1][1]"]
	assert.True(ok, "each level's split is kept in the array's entry under the path to it")

	for _, bad := range [][]int{{2, 0}, {0, 3}, {0, 0, 0}} {
		_, err := linker.Flatten(ctx, []byte(`{}`), Refs{"x": {Anchor: 7, Field: "data", Path: bad}}, anchors.load)
		assert.Error(err, bad)
	}
}

// TestLinker_ResolveItemIsExact pins a fidelity property of resolving from the array's stored bytes: an
// integer beyond 2^53 comes back exactly, where a value decoded into an any and re-encoded would round.
func TestLinker_ResolveItemIsExact(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	anchors := &fakeAnchors{rows: map[int]Anchor{
		7: {Changes: map[string]json.RawMessage{"ids": json.RawMessage(`[1,9007199254740993]`)}},
	}}
	flat, err := New("sqlite", 1, NewCache(testCacheBytes)).Flatten(context.Background(), []byte(`{}`), Refs{"id": {Anchor: 7, Field: "ids", Path: []int{1}}}, anchors.load)
	assert.NoError(err)
	assert.Equal(`{"id":9007199254740993}`, string(flat))
}

// TestSplitArray checks the splitter against encoding/json on the shapes that trip a hand-written scanner:
// nesting, strings holding brackets, commas, quotes and escapes, scalars, and whitespace.
func TestSplitArray(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	for _, in := range []string{
		`[]`,
		` [ ] `,
		`[1]`,
		`[1,2.5e3,-3]`,
		`[true,false,null]`,
		`["a","b,c","d]e","f\"g","h\\","i\\\"j"]`,
		`[[1,[2,3]],{"k":[4,{"m":"]"}]},[]]`,
		"[ 1 ,\n\t{\"a\" : [ 1 , 2 ] } , \"x\" ]",
		`[{"}":"{"},["[",["]"]]]`,
	} {
		got, err := splitArray([]byte(in))
		assert.NoError(err, in)
		var want []json.RawMessage
		assert.NoError(json.Unmarshal([]byte(in), &want), in)
		assert.Equal(len(want), len(got), in)
		for i := range want {
			var w, g any
			assert.NoError(json.Unmarshal(want[i], &w), in)
			assert.NoError(json.Unmarshal(got[i], &g), in)
			assert.Equal(w, g, in)
			assert.Equal(string(want[i]), string(got[i]), in)
		}
	}
	for _, bad := range []string{``, `{}`, `"x"`, `[1`, `[1 2]`, `["abc`, `["abc\`, `[[1,2`} {
		_, err := splitArray([]byte(bad))
		assert.Error(err, bad)
	}
}
