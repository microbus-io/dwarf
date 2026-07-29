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

package workflow

import (
	"encoding/json"
	"testing"

	"github.com/microbus-io/testarossa"
)

func TestState_NewPairs(t *testing.T) {
	assert := testarossa.For(t)

	s, _ := NewState("count", 3, "name", "abc")

	var count int
	ok, err := s.Get("count", &count)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal(3, count)

	var name string
	ok, err = s.Get("name", &name)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal("abc", name)
}

func TestState_NewFromMap(t *testing.T) {
	assert := testarossa.For(t)

	base := map[string]any{"a": 1, "b": "two"}
	s, _ := NewState(base)

	// A map is JSON-normalized (round-tripped), not short-circuited: it is not aliased, so mutating the
	// source does not change the State, and numbers decode to float64 - the canonical stored form.
	base["a"] = 99

	var a float64
	ok, err := s.Get("a", &a)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal(float64(1), a)
}

func TestState_NewMapPlusPairsIsRejected(t *testing.T) {
	assert := testarossa.For(t)

	// A leading map/State base combined with pairs is NOT allowed: a map in a name position (arg 0 of a
	// multi-arg call) is a non-string name, so it is an error rather than a base-plus-overrides.
	_, err := NewState(map[string]any{"a": 1}, "b", 2)
	assert.Error(err)
}

func TestState_NewFromStruct(t *testing.T) {
	assert := testarossa.For(t)

	// A single non-map/State argument is normalized via a JSON round trip: a struct becomes a map keyed by
	// json tags, with numbers in float64 (the canonical stored form).
	type order struct {
		ID    int    `json:"id"`
		Name  string `json:"name"`
		Total int    `json:"total"`
	}
	s, err := NewState(order{ID: 7, Name: "abc", Total: 42})
	assert.NoError(err)
	assert.Equal(3, s.Len())

	var id, total float64 // JSON numbers decode to float64
	var name string
	_, _ = s.Get("id", &id)
	_, _ = s.Get("name", &name)
	_, _ = s.Get("total", &total)
	assert.Equal(float64(7), id)
	assert.Equal("abc", name)
	assert.Equal(float64(42), total)
}

func TestState_NewFromNil(t *testing.T) {
	assert := testarossa.For(t)

	// A nil single argument yields an empty, non-nil State.
	s, err := NewState(nil)
	assert.NoError(err)
	assert.Equal(0, s.Len())
	assert.NoError(s.Set("k", "v")) // usable without further init
}

func TestState_NewFromBytes(t *testing.T) {
	assert := testarossa.For(t)

	// A []byte single argument is raw JSON, unmarshaled directly - the one-liner for reading a state column
	// back from the database. Numbers decode to float64, nested objects to map[string]any.
	s, err := NewState([]byte(`{"count":3,"name":"abc","nested":{"x":1}}`))
	assert.NoError(err)
	assert.Equal(3, s.Len())
	var count float64
	var name string
	_, _ = s.Get("count", &count)
	_, _ = s.Get("name", &name)
	assert.Equal(float64(3), count)
	assert.Equal("abc", name)

	// A json.RawMessage is treated identically to []byte (unmarshaled as raw JSON, not marshalled first).
	rm, err := NewState(json.RawMessage(`{"n":7}`))
	assert.NoError(err)
	var n float64
	_, _ = rm.Get("n", &n)
	assert.Equal(float64(7), n)

	// Empty/nil bytes, a JSON null, and a whitespace-padded object all yield an empty, non-nil State - the
	// tolerated non-object inputs, matching how the engine reads an absent/NULL column or a nil-marshaled "null".
	for _, raw := range [][]byte{nil, {}, []byte("null"), []byte("{}"), []byte("  {}\n"), []byte(" null ")} {
		s, err := NewState(raw)
		assert.NoError(err, "expected no error for %q", string(raw))
		assert.Equal(0, s.Len())
		assert.NoError(s.Set("k", "v")) // non-nil, usable
	}

	// A non-object (array/number/string/bool) and malformed bytes are errors, not panics: state must be a
	// JSON object (or one of the empty-yielding forms above).
	for _, raw := range [][]byte{[]byte("[1,2]"), []byte("42"), []byte(`"s"`), []byte("true"), []byte(`{"count":`)} {
		_, err := NewState(raw)
		assert.Error(err, "expected error for %q", string(raw))
	}
}

func TestState_NewErrors(t *testing.T) {
	assert := testarossa.For(t)

	// A single value that does not encode to a JSON object is a normalization error, not a panic.
	_, err := NewState(42)
	assert.Error(err)
	_, err = NewState([]int{1, 2})
	assert.Error(err)

	// NewState normalizes without validating value ranges: an oversized integer round-trips through JSON
	// here without error (state number precision is not enforced - carry a >2^53 id as a string).
	type big struct {
		N int64 `json:"n"`
	}
	_, err = NewState(big{N: int64(1) << 60})
	assert.NoError(err)

	// Malformed pair arguments are errors too.
	_, err = NewState("oddCount")
	assert.Error(err)
	_, err = NewState(map[string]any{"a": 1}, 5, "notAName")
	assert.Error(err)
}

func TestState_SetAndGet(t *testing.T) {
	assert := testarossa.For(t)

	s, _ := NewState()
	err := s.Set("greeting", "hello")
	assert.NoError(err)

	var got string
	ok, err := s.Get("greeting", &got)
	assert.NoError(err)
	assert.True(ok)
	assert.Equal("hello", got)
}

func TestState_Len(t *testing.T) {
	assert := testarossa.For(t)

	var zero State
	assert.Equal(0, zero.Len())

	s, _ := NewState("a", 1, "b", 2)
	assert.Equal(2, s.Len())

	s.Del("a")
	assert.Equal(1, s.Len())
}

func TestState_All(t *testing.T) {
	assert := testarossa.For(t)

	s, _ := NewState("a", 1, "b", 2, "c", 3)

	seen := map[string]any{}
	for k, v := range s.All() {
		seen[k] = v
	}
	assert.Equal(3, len(seen))
	assert.Equal(1, seen["a"])
	assert.Equal(2, seen["b"])
	assert.Equal(3, seen["c"])

	// Early break stops iteration.
	count := 0
	for range s.All() {
		count++
		break
	}
	assert.Equal(1, count)
}

func TestState_Delete(t *testing.T) {
	assert := testarossa.For(t)

	s, _ := NewState("a", 1, "b", 2, "c", 3)
	s.Del("b", "missing")

	var a, b, c int
	ok, _ := s.Get("a", &a)
	assert.True(ok)
	assert.Equal(1, a)

	ok, _ = s.Get("b", &b)
	assert.False(ok)

	ok, _ = s.Get("c", &c)
	assert.True(ok)
	assert.Equal(3, c)
}

func TestState_GetAbsent(t *testing.T) {
	assert := testarossa.For(t)

	s, _ := NewState()
	var got string
	ok, err := s.Get("missing", &got)
	assert.NoError(err)
	assert.False(ok)
	assert.Equal("", got)
}

func TestState_GetTypeMismatch(t *testing.T) {
	assert := testarossa.For(t)

	s, _ := NewState("n", "not a number")
	var got int
	ok, err := s.Get("n", &got)
	assert.Error(err)
	assert.False(ok)
}

func TestState_Clone(t *testing.T) {
	assert := testarossa.For(t)

	orig, _ := NewState("a", 1, "b", 2)
	clone := orig.Clone()

	// Mutating the clone does not affect the original, and vice versa.
	_ = clone.Set("a", 99)
	clone.Del("b")
	_ = orig.Set("c", 3)

	var a, b int
	_, _ = orig.Get("a", &a)
	okB, _ := orig.Get("b", &b)
	assert.Equal(1, a)
	assert.True(okB)
	assert.Equal(2, b)

	var cloneA int
	okC, _ := clone.Get("c", &cloneA)
	assert.False(okC) // orig's later "c" did not leak into the clone
}

func TestState_CloneRestoresAfterFailedMerge(t *testing.T) {
	assert := testarossa.For(t)

	state, _ := NewState("items", []int{1, 2})
	reducers := map[string]Reducer{"items": ReducerAppend}

	safe := state.Clone()
	err := state.MergeReduce(State{d: map[string]any{"items": "not an int"}}, reducers)
	assert.Error(err)

	// Roll back to the pre-Merge snapshot.
	state = safe
	err = state.MergeReduce(State{d: map[string]any{"items": []int{3, 4}}}, reducers)
	assert.NoError(err)

	var items []int
	_, _ = state.Get("items", &items)
	assert.Equal([]int{1, 2, 3, 4}, items)
}

func TestState_Merge(t *testing.T) {
	assert := testarossa.For(t)

	a, _ := NewState("x", 1, "y", 2)
	b, _ := NewState("y", 20, "z", 30)

	err := a.Merge(b)
	assert.NoError(err)

	var x, y, z int
	_, _ = a.Get("x", &x)
	_, _ = a.Get("y", &y)
	_, _ = a.Get("z", &z)
	assert.Equal(1, x)
	assert.Equal(20, y) // last write wins
	assert.Equal(30, z)

	// The incoming State is left unmodified.
	var origY int
	_, _ = b.Get("y", &origY)
	assert.Equal(20, origY)
}

func TestState_MergeKeepsNilUntilDelNils(t *testing.T) {
	assert := testarossa.For(t)

	// Merge accumulates: a nil incoming (a JSON-decoded null) is preserved as a tombstone, not dropped.
	base, _ := NewState("a", 1, "b", 2)
	err := base.Merge(State{d: map[string]any{"b": nil}})
	assert.NoError(err)
	assert.Equal(2, base.Len()) // "b" still present (as a tombstone)

	// DelNils materializes: the pending delete becomes an absence.
	base.DelNils()
	assert.Equal(1, base.Len())
	var a int
	okA, _ := base.Get("a", &a)
	assert.True(okA)
	assert.Equal(1, a)
}

func TestState_MergeReplaceReducerKeepsNil(t *testing.T) {
	assert := testarossa.For(t)

	// An explicitly-mapped Replace behaves like the no-reducer case: the tombstone survives Merge and is
	// enacted only by DelNils.
	base, _ := NewState("a", 1, "b", 2)
	reducers := map[string]Reducer{"b": ReducerReplace}
	err := base.MergeReduce(State{d: map[string]any{"b": nil}}, reducers)
	assert.NoError(err)
	assert.Equal(2, base.Len())
	base.DelNils()
	assert.Equal(1, base.Len())
}

func TestState_MergeCombiningReducerIgnoresNil(t *testing.T) {
	assert := testarossa.For(t)

	// A cleared incoming for a reducer-managed field is the reducer's identity: ignored, leaving the
	// accumulator untouched (a branch deleting a reduced field does not wipe the cohort's contributions).
	base, _ := NewState("items", []int{1, 2})
	err := base.MergeReduce(State{d: map[string]any{"items": nil}}, map[string]Reducer{"items": ReducerAppend})
	assert.NoError(err)
	var items []int
	_, _ = base.Get("items", &items)
	assert.Equal([]int{1, 2}, items) // unchanged
}

func TestState_MergeSequentialOrder(t *testing.T) {
	assert := testarossa.For(t)

	base, _ := NewState("k", "base")
	assert.NoError(base.Merge(State{d: map[string]any{"k": "first"}}))
	assert.NoError(base.Merge(State{d: map[string]any{"k": "last"}}))

	var k string
	_, _ = base.Get("k", &k)
	assert.Equal("last", k)
}

func TestState_JSONRoundTrip(t *testing.T) {
	assert := testarossa.For(t)

	s, _ := NewState("count", 3, "name", "abc", "flag", true)

	data, err := json.Marshal(s)
	assert.NoError(err)

	var back State
	err = json.Unmarshal(data, &back)
	assert.NoError(err)

	// After a JSON round trip, numbers decode to float64, strings to string, bools to bool.
	var count float64
	var name string
	var flag bool
	_, _ = back.Get("count", &count)
	_, _ = back.Get("name", &name)
	_, _ = back.Get("flag", &flag)
	assert.Equal(float64(3), count)
	assert.Equal("abc", name)
	assert.True(flag)
}

func TestState_UnmarshalTolerance(t *testing.T) {
	assert := testarossa.For(t)

	// Empty payload: an empty State with a non-nil backing map.
	var empty State
	err := empty.UnmarshalJSON([]byte(""))
	assert.NoError(err)
	assert.Equal(0, empty.Len())
	assert.False(empty.IsZero(), "an unmarshaled State is initialized, so Set/Merge are safe on it")

	// "{}": non-nil empty map.
	var braces State
	err = braces.UnmarshalJSON([]byte("{}"))
	assert.NoError(err)
	assert.Equal(0, braces.Len())
	assert.False(braces.IsZero(), "an unmarshaled State is initialized, so Set/Merge are safe on it")

	// Whitespace-padded object: tolerated (trimmed before the object check).
	var padded State
	err = padded.UnmarshalJSON([]byte("  {}\n"))
	assert.NoError(err)
	assert.Equal(0, padded.Len())

	// A populated object still decodes.
	var obj State
	err = obj.UnmarshalJSON([]byte(`{"a":1}`))
	assert.NoError(err)
	var a float64
	ok, _ := obj.Get("a", &a)
	assert.True(ok)
	assert.Equal(1.0, a)

	// A JSON null is tolerated as empty state (like empty/nil bytes) - a nil value marshals to "null".
	var null State
	err = null.UnmarshalJSON([]byte("null"))
	assert.NoError(err)
	assert.Equal(0, null.Len())

	// Any other non-object shape or malformed bytes errors rather than silently becoming empty state.
	for _, raw := range []string{"[1,2]", "42", `"s"`, "true", `{"a":`} {
		var s State
		assert.Error(s.UnmarshalJSON([]byte(raw)), "expected error for %q", raw)
	}
}

func TestState_MarshalZeroValue(t *testing.T) {
	assert := testarossa.For(t)

	var s State
	data, err := json.Marshal(s)
	assert.NoError(err)
	assert.Equal("{}", string(data))
}

// TestState_RawFieldIsNotDecodedUntilRead pins the property the whole raw-storage design exists for: a
// field that nothing reads is never expanded into a Go value, and one that IS read is expanded without
// disturbing what is stored.
//
// White-box on purpose. The public surface deliberately cannot tell the two storage forms apart - every
// accessor materializes - so a black-box test of "is it still raw" is impossible to write, which is exactly
// the encapsulation the struct was introduced for.
func TestState_RawFieldIsNotDecodedUntilRead(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s, err := StateFromCanonicalJSON([]byte(`{"doc":{"page":"one"},"n":3,"s":"x"}`))
	assert.NoError(err)

	for _, k := range []string{"doc", "n", "s"} {
		_, isRaw := s.d[k].(json.RawMessage)
		assert.True(isRaw, "field %q must be held raw until something reads it", k)
	}

	// Reading materializes for the CALLER without materializing the STORAGE - there is no memoization, so a
	// read cannot turn into a map write (which would be an unrecoverable throw under concurrent access).
	assert.Equal(3, s.GetInt("n"))
	assert.Equal("x", s.GetString("s"))
	_, stillRaw := s.d["n"].(json.RawMessage)
	assert.True(stillRaw, "a read must not rewrite the stored form")

	// A read hands back a decoded value, never the encoding.
	_, leaked := s.Value("doc").(json.RawMessage)
	assert.False(leaked, "the storage form must never escape through an accessor")
	doc, ok := s.Value("doc").(map[string]any)
	assert.True(ok)
	assert.Equal("one", doc["page"])

	// Two reads of one raw field yield independent values, so a caller mutating what it read cannot corrupt
	// the State or another reader - the same guarantee decoding per branch gave.
	a, _ := s.Value("doc").(map[string]any)
	b, _ := s.Value("doc").(map[string]any)
	a["page"] = "mutated"
	assert.Equal("one", b["page"], "two reads of a raw field must not share a decoded map")
}

// TestState_RawTombstoneIsCleared pins hazard 3. A delete is recorded as a JSON null; held raw it is the
// four bytes `null` rather than a Go nil, and a cleared-check that only knows Go nil lets a delete read
// back from a column silently stop taking effect - DelNils leaves it and Has reports the field present.
// Fails against isCleared(v) == (v == nil).
func TestState_RawTombstoneIsCleared(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s, err := StateFromCanonicalJSON([]byte(`{"gone":null,"kept":1}`))
	assert.NoError(err)

	assert.False(s.Has("gone"), "a raw JSON null is a tombstone, not a value")
	assert.True(s.Contains("gone"), "the delta still speaks about the field")
	assert.True(s.Has("kept"))

	// Merge preserves the tombstone (accumulation); DelNils enacts it (materialization).
	base, _ := NewState("gone", "was here", "other", 2)
	assert.NoError(base.Merge(s))
	assert.True(base.Contains("gone"), "Merge accumulates - the pending delete survives")
	base.DelNils()
	assert.False(base.Contains("gone"), "DelNils enacts the delete")
	assert.True(base.Has("kept"))
	assert.True(base.Has("other"))
}

// TestState_ReducerFoldsRawOperands pins hazard 4. The reducers type-assert on Go values, so a fold has to
// materialize both sides; a raw base or a raw incoming must combine exactly as decoded ones do.
func TestState_ReducerFoldsRawOperands(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	base, err := StateFromCanonicalJSON([]byte(`{"total":10,"list":["a"]}`))
	assert.NoError(err)
	incoming, err := StateFromCanonicalJSON([]byte(`{"total":5,"list":["b"]}`))
	assert.NoError(err)

	err = base.MergeReduce(incoming, map[string]Reducer{"total": ReducerAdd, "list": ReducerAppend})
	assert.NoError(err)
	assert.Equal(15, base.GetInt("total"), "a raw base and a raw incoming must add")
	assert.Equal([]string{"a", "b"}, base.GetStrings("list"))
}

// TestState_CanonicalJSONRoundTripsUntouched pins that carrying a field costs nothing and changes nothing:
// bytes in, the same bytes out, with no decode/re-encode in between.
func TestState_CanonicalJSONRoundTripsUntouched(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Canonical: sorted keys at every level, which is what marshalling a decoded value produces.
	const canonical = `{"a":{"x":1,"y":[1,2,3]},"b":"str","c":null}`
	s, err := StateFromCanonicalJSON([]byte(canonical))
	assert.NoError(err)
	out, err := s.MarshalJSON()
	assert.NoError(err)
	assert.Equal(canonical, string(out), "a carried document must survive byte-identical")

	// FieldJSON hands the stored bytes straight back - the accessor Mint sizes fields with.
	fj, err := s.FieldJSON("a")
	assert.NoError(err)
	assert.Equal(`{"x":1,"y":[1,2,3]}`, string(fj))
}

// TestState_SetCanonicalJSONIsNotAFastPathForSet pins hazard 1, the one that corrupts silently. Set
// round-trips through JSON to CANONICALIZE, and that is load-bearing rather than incidental: reducers
// compare marshalled bytes, and Go marshals a map with sorted keys but a struct in DECLARATION order. So a
// struct stored via Set must come out spelled identically to its decoded twin - which is what lets a union
// reducer recognise the two as one value instead of keeping both.
func TestState_SetCanonicalJSONIsNotAFastPathForSet(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Declaration order here is deliberately the reverse of sorted order.
	type item struct {
		Z string `json:"z"`
		A string `json:"a"`
	}

	viaSet, _ := NewState()
	assert.NoError(viaSet.Set("it", item{Z: "z", A: "a"}))
	setBytes, err := viaSet.FieldJSON("it")
	assert.NoError(err)
	assert.Equal(`{"a":"a","z":"z"}`, string(setBytes), "Set canonicalizes a struct to sorted keys")

	// The same value arriving as a column read is already canonical, and compares byte-equal to the above.
	fromColumn, err := StateFromCanonicalJSON([]byte(`{"it":{"a":"a","z":"z"}}`))
	assert.NoError(err)
	colBytes, err := fromColumn.FieldJSON("it")
	assert.NoError(err)
	assert.Equal(string(setBytes), string(colBytes),
		"a value stored through Set and the same value read back from a column must have ONE spelling, "+
			"or a union reducer keeps both")
}
