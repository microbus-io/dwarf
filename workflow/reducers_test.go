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
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

func TestMergeReduce_Append(t *testing.T) {
	assert := testarossa.For(t)

	base, _ := NewState("items", []int{1, 2})
	err := base.MergeReduce(st("items", []int{3, 4}), map[string]Reducer{"items": ReducerAppend})
	assert.NoError(err)

	var items []int
	_, _ = base.Get("items", &items)
	assert.Equal([]int{1, 2, 3, 4}, items)
}

func TestMergeReduce_AppendSingleElement(t *testing.T) {
	assert := testarossa.For(t)

	base, _ := NewState("items", []string{"a"})
	err := base.MergeReduce(st("items", "b"), map[string]Reducer{"items": ReducerAppend})
	assert.NoError(err)

	var items []string
	_, _ = base.Get("items", &items)
	assert.Equal([]string{"a", "b"}, items)
}

func TestMergeReduce_IncomingAbsentIsNoOp(t *testing.T) {
	assert := testarossa.For(t)

	base, _ := NewState("items", []int{1, 2})
	err := base.MergeReduce(st("other", 1), map[string]Reducer{"items": ReducerAppend})
	assert.NoError(err)

	var items []int
	_, _ = base.Get("items", &items)
	assert.Equal([]int{1, 2}, items)
}

func TestMergeReduce_BaseAbsentSeedsViaReducer(t *testing.T) {
	assert := testarossa.For(t)

	// base has no "items" yet, so the reducer is called with a nil base and decides how to seed.
	base, _ := NewState("other", 1)
	reducers := map[string]Reducer{"items": ReducerAppend}
	err := base.MergeReduce(st("items", []int{3, 4}), reducers)
	assert.NoError(err)

	var items []int
	_, _ = base.Get("items", &items)
	assert.Equal([]int{3, 4}, items)

	// A second merge accumulates onto the seeded slice.
	err = base.MergeReduce(st("items", 5), reducers)
	assert.NoError(err)
	_, _ = base.Get("items", &items)
	assert.Equal([]int{3, 4, 5}, items)
}

func TestMergeReduce_NoReducerOverwrites(t *testing.T) {
	assert := testarossa.For(t)

	base, _ := NewState("items", []int{1, 2})
	err := base.Merge(st("items", []int{3, 4})) // no reducers
	assert.NoError(err)

	var items []int
	_, _ = base.Get("items", &items)
	assert.Equal([]int{3, 4}, items) // last write wins
}

func TestMergeReduce_ErrorPropagates(t *testing.T) {
	assert := testarossa.For(t)

	// The reachable failure is a BASE that is not a list - a field that held a scalar and was later wired to
	// append. An element-type mismatch is NOT reachable: every base in the live path arrives from JSON as a
	// []any, which accepts any element, so appending a string to a list of numbers is legal there.
	base, _ := NewState("items", "not a list")
	err := base.MergeReduce(st("items", 3), map[string]Reducer{"items": ReducerAppend})
	assert.Error(err)
	assert.Contains(err.Error(), "must be a slice or array")

	// And the confirmation that the other branch is dead in production: a normalized []any base takes a
	// string element without complaint.
	nums, _ := NewState("items", []int{1, 2})
	assert.NoError(nums.MergeReduce(st("items", "three"), map[string]Reducer{"items": ReducerAppend}))
}

func TestMergeReduce_AddByReducerName(t *testing.T) {
	assert := testarossa.For(t)

	base, _ := NewState("total", 10)
	err := base.MergeReduce(st("total", 5), map[string]Reducer{"total": ReducerAdd})
	assert.NoError(err)

	var total int
	_, _ = base.Get("total", &total)
	assert.Equal(15, total)
}

func TestReducerReduce_Replace(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerReplace.Reduce(1, 2)
	assert.NoError(err)
	assert.Equal(2, result)

	// The empty Reducer is replace too.
	result, err = Reducer("").Reduce(1, 2)
	assert.NoError(err)
	assert.Equal(2, result)
}

func TestReducerReduce_Unknown(t *testing.T) {
	assert := testarossa.For(t)

	_, err := Reducer("nope").Reduce(1, 2)
	assert.Error(err)
}

func TestReducerFn_Add(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAdd(10, 5)
	assert.NoError(err)
	assert.Equal(15, result) // int in, int out

	result, err = doAdd(1.5, 2.5)
	assert.NoError(err)
	assert.Equal(4.0, result)

	result, err = doAdd(nil, 7) // seed
	assert.NoError(err)
	assert.Equal(7, result)

	// Native math in the value's own type: an int64 past 2^53 stays exact (no float64 detour).
	result, err = doAdd(int64(9007199254740993), int64(1))
	assert.NoError(err)
	assert.Equal(int64(9007199254740994), result)

	// Mixed Go types are rejected rather than promoted (promotion would lose integer precision).
	_, err = doAdd(3, 1.5)
	assert.Error(err)

	_, err = doAdd([]int{1}, 2)
	assert.Error(err)
}

func TestReducerFn_Duration(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAdd(2*time.Second, 500*time.Millisecond)
	assert.NoError(err)
	assert.Equal(2500*time.Millisecond, result)

	result, err = doMax(1*time.Second, 3*time.Second)
	assert.NoError(err)
	assert.Equal(3*time.Second, result)

	result, err = doMin(nil, 5*time.Second) // seed
	assert.NoError(err)
	assert.Equal(5*time.Second, result)

	// A time.Duration and a bare int64 are different Go types, so mixing them is rejected.
	_, err = doAdd(2*time.Second, int64(1))
	assert.Error(err)
}

func TestReducerFn_MinMax(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doMin(3, 5)
	assert.NoError(err)
	assert.Equal(3, result)

	result, err = doMax(3, 5)
	assert.NoError(err)
	assert.Equal(5, result)

	result, err = doMin(nil, 9) // seed
	assert.NoError(err)
	assert.Equal(9, result)
}

func TestReducerFn_Union(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doUnion([]int{1, 2}, []int{2, 3})
	assert.NoError(err)
	assert.Equal([]int{1, 2, 3}, result) // 2 deduplicated

	result, err = doUnion([]string{"a"}, "a") // single element already present
	assert.NoError(err)
	assert.Equal([]string{"a"}, result)

	result, err = doUnion(nil, []int{1, 1, 2}) // seed dedupes incoming
	assert.NoError(err)
	assert.Equal([]int{1, 2}, result)
}

func TestReducerFn_And(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAnd(true, false)
	assert.NoError(err)
	assert.Equal(false, result)

	result, err = doAnd(true, true)
	assert.NoError(err)
	assert.Equal(true, result)

	result, err = doAnd(nil, true) // seed
	assert.NoError(err)
	assert.Equal(true, result)

	_, err = doAnd(1, true)
	assert.Error(err)
}

func TestReducerFn_Or(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doOr(false, false)
	assert.NoError(err)
	assert.Equal(false, result)

	result, err = doOr(false, true)
	assert.NoError(err)
	assert.Equal(true, result)
}

func TestReducerFn_Concat(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doConcat("foo", "bar")
	assert.NoError(err)
	assert.Equal("foobar", result)

	result, err = doConcat(nil, "seed") // seed
	assert.NoError(err)
	assert.Equal("seed", result)

	_, err = doConcat("foo", 1)
	assert.Error(err)
}

func TestReducerFn_Merge(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doMerge(
		map[string]any{"a": 1, "b": 2},
		map[string]any{"b": 20, "c": 30},
	)
	assert.NoError(err)
	assert.Equal(map[string]any{"a": 1, "b": 20, "c": 30}, result) // incoming wins on "b"

	result, err = doMerge(nil, map[string]any{"x": 1}) // seed
	assert.NoError(err)
	assert.Equal(map[string]any{"x": 1}, result)

	_, err = doMerge([]int{1}, map[string]any{"x": 1})
	assert.Error(err)
}

func TestReducerFn_MergeSeedIsACopy(t *testing.T) {
	assert := testarossa.For(t)

	src := map[string]any{"x": 1}
	result, err := doMerge(nil, src)
	assert.NoError(err)

	// Mutating the source afterward does not change the seeded result.
	src["y"] = 2
	assert.Equal(map[string]any{"x": 1}, result)
}

func TestReducerAppend_NilBaseSeedsFromSlice(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAppend(nil, []int{1, 2})
	assert.NoError(err)
	assert.Equal([]int{1, 2}, result)
}

func TestReducerAppend_NilBaseSeedsFromSingleElement(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAppend(nil, 5)
	assert.NoError(err)
	assert.Equal([]int{5}, result)
}

func TestReducerAppend_NilBaseNilIncomingErrors(t *testing.T) {
	assert := testarossa.For(t)

	_, err := doAppend(nil, nil)
	assert.Error(err)
}

func TestReducerAppend_SpreadSlice(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAppend([]int{1, 2}, []int{3, 4})
	assert.NoError(err)
	assert.Equal([]int{1, 2, 3, 4}, result)
}

func TestReducerAppend_SingleElement(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAppend([]int{1, 2}, 3)
	assert.NoError(err)
	assert.Equal([]int{1, 2, 3}, result)
}

func TestReducerAppend_Strings(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAppend([]string{"a"}, []string{"b", "c"})
	assert.NoError(err)
	assert.Equal([]string{"a", "b", "c"}, result)

	result, err = doAppend([]string{"a"}, "b")
	assert.NoError(err)
	assert.Equal([]string{"a", "b"}, result)
}

func TestReducerAppend_EmptyBase(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAppend([]int{}, []int{1, 2})
	assert.NoError(err)
	assert.Equal([]int{1, 2}, result)
}

func TestReducerAppend_SliceIntoAnyBaseIsSingleElement(t *testing.T) {
	assert := testarossa.For(t)

	// []int into a []any base is ambiguous; the single-element reading wins, so it is appended as one
	// element rather than spread.
	result, err := doAppend([]any{"x"}, []int{1, 2})
	assert.NoError(err)
	assert.Equal([]any{"x", []int{1, 2}}, result)
}

func TestReducerAppend_AnyBaseSpreadsMatchingType(t *testing.T) {
	assert := testarossa.For(t)

	// []any into a []any base spreads, since incoming's type matches base's type exactly.
	result, err := doAppend([]any{"x"}, []any{1, 2})
	assert.NoError(err)
	assert.Equal([]any{"x", 1, 2}, result)
}

func TestReducerAppend_DoesNotMutateBase(t *testing.T) {
	assert := testarossa.For(t)

	base := []int{1, 2, 3}
	base = base[:2] // len 2, cap 3: an in-place append would clobber base[2]
	result, err := doAppend(base, 99)
	assert.NoError(err)
	assert.Equal([]int{1, 2, 99}, result)
	// The original backing array's third slot is untouched.
	full := base[:3]
	assert.Equal(3, full[2])
}

func TestReducerAppend_BaseNotList(t *testing.T) {
	assert := testarossa.For(t)

	_, err := doAppend(42, 1)
	assert.Error(err)
}

func TestReducerAppend_IncompatibleElement(t *testing.T) {
	assert := testarossa.For(t)

	_, err := doAppend([]int{1}, "not an int")
	assert.Error(err)
}

func TestReducerAppend_ArrayBase(t *testing.T) {
	assert := testarossa.For(t)

	result, err := doAppend([3]int{1, 2, 3}, 4)
	assert.NoError(err)
	assert.Equal([]int{1, 2, 3, 4}, result)
}

// st builds a normalized State, the way every state in production is built - through NewState, so values
// arrive in the JSON-canonical Go representation ([]any, float64, map[string]any). Hand-building the inner
// map instead would hand a reducer a []int where a real fan-in always hands it a []any, and the append
// spread rule is element-type sensitive - so such a test would prove nothing about the live path.
func st(pairs ...any) State {
	s, err := NewState(pairs...)
	if err != nil {
		panic(err)
	}
	return s
}
