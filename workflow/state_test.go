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
	"slices"
	"testing"
	"time"

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

func TestState_Names(t *testing.T) {
	assert := testarossa.For(t)

	s, _ := NewState("a", 1, "b", 2, "c", 3)

	seen := map[string]int{}
	for k := range s.Names() {
		seen[k] = s.GetInt(k)
	}
	assert.Equal(3, len(seen))
	assert.Equal(1, seen["a"])
	assert.Equal(2, seen["b"])
	assert.Equal(3, seen["c"])

	// Early break stops iteration.
	count := 0
	for range s.Names() {
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

	// A base that is not a list is the reachable append failure (see TestMergeReduce_ErrorPropagates).
	state, _ := NewState("items", "not a list")
	reducers := map[string]Reducer{"items": ReducerAppend}

	safe := state.Clone()
	incoming, _ := NewState("items", 3)
	err := state.MergeReduce(incoming, reducers)
	assert.Error(err)

	// Roll back to the pre-Merge snapshot, then merge onto a list base successfully.
	state = safe
	assert.Equal("not a list", state.GetString("items"), "the failed merge left the snapshot intact")
	state, _ = NewState("items", []int{1, 2})
	ok, _ := NewState("items", []int{3, 4})
	err = state.MergeReduce(ok, reducers)
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
	err := base.Merge(mustS("b", nil))
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
	err := base.MergeReduce(mustS("b", nil), reducers)
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
	err := base.MergeReduce(mustS("items", nil), map[string]Reducer{"items": ReducerAppend})
	assert.NoError(err)
	var items []int
	_, _ = base.Get("items", &items)
	assert.Equal([]int{1, 2}, items) // unchanged
}

func TestState_MergeSequentialOrder(t *testing.T) {
	assert := testarossa.For(t)

	base, _ := NewState("k", "base")
	assert.NoError(base.Merge(mustS("k", "first")))
	assert.NoError(base.Merge(mustS("k", "last")))

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

	s, err := NewState([]byte(`{"doc":{"page":"one"},"n":3,"s":"x"}`))
	assert.NoError(err)

	// Every field is stored as its JSON, so GetJSON is a hand-back with nothing to encode.
	assert.Equal(`{"page":"one"}`, string(s.GetJSON("doc")))
	assert.Equal(`3`, string(s.GetJSON("n")))

	// Reading materializes for the CALLER without materializing the STORAGE - there is no memoization, so a
	// read cannot turn into a map write (which would be an unrecoverable throw under concurrent access).
	assert.Equal(3, s.GetInt("n"))
	assert.Equal("x", s.GetString("s"))
	assert.Equal(`3`, string(s.GetJSON("n")), "a read must not rewrite what is stored")

	// A read hands back a decoded value, never the encoding.
	_, leaked := stateVal(s, "doc").(json.RawMessage)
	assert.False(leaked, "the storage form must never escape through an accessor")
	doc, ok := stateVal(s, "doc").(map[string]any)
	assert.True(ok)
	assert.Equal("one", doc["page"])

	// Two reads of one raw field yield independent values, so a caller mutating what it read cannot corrupt
	// the State or another reader - the same guarantee decoding per branch gave.
	a, _ := stateVal(s, "doc").(map[string]any)
	b, _ := stateVal(s, "doc").(map[string]any)
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

	s, err := NewState([]byte(`{"gone":null,"kept":1}`))
	assert.NoError(err)

	assert.False(s.Has("gone"), "a raw JSON null is a tombstone, not a value")
	assert.True(s.IsDeleted("gone"), "the delta records it as a delete")
	assert.True(s.Has("kept"))

	// Merge preserves the tombstone (accumulation); DelNils enacts it (materialization).
	base, _ := NewState("gone", "was here", "other", 2)
	assert.NoError(base.Merge(s))
	assert.True(base.IsDeleted("gone"), "Merge accumulates - the pending delete survives")
	base.DelNils()
	assert.False(base.IsDeleted("gone"), "DelNils enacts the delete")
	assert.False(base.Has("gone"))
	assert.True(base.Has("kept"))
	assert.True(base.Has("other"))
}

// TestState_ReducerFoldsRawOperands pins hazard 4. The reducers type-assert on Go values, so a fold has to
// materialize both sides; a raw base or a raw incoming must combine exactly as decoded ones do.
func TestState_ReducerFoldsRawOperands(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	base, err := NewState([]byte(`{"total":10,"list":["a"]}`))
	assert.NoError(err)
	incoming, err := NewState([]byte(`{"total":5,"list":["b"]}`))
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
	s, err := NewState([]byte(canonical))
	assert.NoError(err)
	out, err := s.MarshalJSON()
	assert.NoError(err)
	assert.Equal(canonical, string(out), "a carried document must survive byte-identical")

	// FieldJSON hands the stored bytes straight back - the accessor Mint sizes fields with.
	fj := s.GetJSON("a")
	assert.Equal(`{"x":1,"y":[1,2,3]}`, string(fj))
}

// TestState_SetNormalizesGoType pins what Set's JSON round trip is actually for, which is NOT byte order.
//
// union dedupes with reflect.DeepEqual, and DeepEqual between a struct and its decoded twin is false - they
// are different Go types. So a caller contributing a value as a struct must have it normalized to the same
// map[string]any/float64 representation a column read produces, or the reducer keeps two copies of one
// value. That is the mechanism behind the Continue-additionalState bug.
//
// Key ORDER is deliberately exercised here too, and deliberately does not matter: the struct's declaration
// order is the reverse of sorted, and it dedupes anyway.
func TestState_SetNormalizesGoType(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	type item struct {
		Z string `json:"z"`
		A string `json:"a"`
	}

	// base holds the value as a column read produces it; incoming contributes the same value as a STRUCT.
	base, err := NewState([]byte(`{"list":[{"a":"a","z":"z"}]}`))
	assert.NoError(err)
	incoming, _ := NewState()
	assert.NoError(incoming.Set("list", []item{{Z: "z", A: "a"}}))

	assert.NoError(base.MergeReduce(incoming, map[string]Reducer{"list": ReducerUnion}))
	out := base.GetJSON("list")
	assert.Equal(`[{"a":"a","z":"z"}]`, string(out),
		"a struct-contributed element must dedupe against its decoded twin, not be kept alongside it")

	// The normalization is why: Set stored a map, not the struct.
	var one []any
	_, err = incoming.Get("list", &one)
	assert.NoError(err)
	_, isMap := one[0].(map[string]any)
	assert.True(isMap, "Set must store the decoded form, or DeepEqual cannot match it against a column read")
}

// TestState_NamesDoesNotDecode pins why Names exists rather than being sugar over All: iterating names must
// not pay the decode All pays to materialize each value. The observable difference is that Names leaves the
// stored form alone AND never needs it to be decodable - so a slot holding bytes that are not valid JSON
// still enumerates, where All would yield nil for it.
func TestState_NamesDoesNotDecode(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s, err := NewState([]byte(`{"a":{"deep":[1,2,3]},"b":2,"c":null}`))
	assert.NoError(err)

	var names []string
	for k := range s.Names() {
		names = append(names, k)
	}
	slices.Sort(names)
	assert.Equal([]string{"a", "b", "c"}, names, "Names enumerates every field, tombstones included")

	// Enumerating names decodes nothing - a field whose value is never read is never decoded.
	assert.Equal(`{"deep":[1,2,3]}`, string(s.GetJSON("a")))

	// Early break stops iteration, as range-over-func requires.
	count := 0
	for range s.Names() {
		count++
		break
	}
	assert.Equal(1, count)

	// A zero State yields nothing rather than panicking.
	var zero State
	for range zero.Names() {
		t.Fatal("a zero State has no fields")
	}
}

// stateVal reads a field as an untyped value. It lives here rather than on State because Get already is
// this, with a type the caller chooses; a one-line untyped read is a test convenience, not API.
func stateVal(s State, name string) any {
	var v any
	_, _ = s.Get(name, &v)
	return v
}

// mustS builds a normalized State from name/value pairs - the shape every state in production has, since
// they all arrive through NewState or a column read.
func mustS(pairs ...any) State {
	s, err := NewState(pairs...)
	if err != nil {
		panic(err)
	}
	return s
}

// TestState_MergeReduceAllMatchesPerMember pins that folding a cohort in one pass is not a different
// computation from folding it member by member. MergeReduceAll exists purely so a combining reducer stops
// re-encoding an accumulator that grows as it goes; if it ever disagreed with MergeReduce on a result, the
// optimization would be silently changing fan-in outcomes.
func TestState_MergeReduceAllMatchesPerMember(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	reducers := map[string]Reducer{
		"list": ReducerAppend, "set": ReducerUnion, "total": ReducerAdd,
		"obj": ReducerMerge, "flag": ReducerOr, "txt": ReducerConcat,
	}
	members := func() []State {
		return []State{
			mustS("list", []int{1}, "set", []string{"a"}, "total", 1, "obj", map[string]any{"x": 1}, "flag", false, "txt", "a", "plain", "first"),
			mustS("list", []int{2}, "set", []string{"a", "b"}, "total", 2, "obj", map[string]any{"y": 2}, "flag", true, "txt", "b", "plain", "second"),
			mustS("list", []int{3}, "set", []string{"b", "c"}, "total", 3, "obj", map[string]any{"x": 9}, "flag", false, "txt", "c"),
		}
	}

	oneByOne := mustS("list", []int{0}, "total", 10, "txt", "z")
	for _, m := range members() {
		assert.NoError(oneByOne.MergeReduce(m, reducers))
	}
	allAtOnce := mustS("list", []int{0}, "total", 10, "txt", "z")
	assert.NoError(allAtOnce.MergeReduceAll(members(), reducers))

	a, err := oneByOne.MarshalJSON()
	assert.NoError(err)
	b, err := allAtOnce.MarshalJSON()
	assert.NoError(err)
	assert.Equal(string(a), string(b), "one pass and per-member folding must agree, byte for byte")

	// Spot-check that the fold actually combined rather than replaced, so an all-replace bug cannot pass.
	assert.Equal(16, allAtOnce.GetInt("total"))
	assert.Equal([]string{"a", "b", "c"}, allAtOnce.GetStrings("set"))
	assert.Equal("second", allAtOnce.GetString("plain"), "an unreduced field is last-write-wins")
}

// TestState_TypedGettersPreserveTypeAcrossJSONStorage pins the contract that pays for storing values as
// JSON: the Go type is not preserved by the STORE, so a typed read is what preserves it. An untyped read is
// float64-domain, and that is the documented price rather than a defect.
func TestState_TypedGettersPreserveTypeAcrossJSONStorage(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s, _ := NewState()
	s.SetInt("n", 42)
	s.SetDuration("d", 1500)

	assert.Equal(42, s.GetInt("n"), "a typed read returns the type asked for")
	assert.Equal(time.Duration(1500), s.GetDuration("d"))

	var untyped any
	ok, err := s.Get("n", &untyped)
	assert.True(ok)
	assert.NoError(err)
	assert.Equal(float64(42), untyped, "an untyped read is float64-domain - JSON has one number type")

	// A typed target of the caller's choosing coerces, which is the escape from the above.
	var asInt int
	_, err = s.Get("n", &asInt)
	assert.NoError(err)
	assert.Equal(42, asInt)
}
