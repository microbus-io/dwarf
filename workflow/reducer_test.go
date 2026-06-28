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

func TestReducer_Replace(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerReplace.Reduce(json.RawMessage(`"old"`), json.RawMessage(`"new"`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `"new"`)
}

func TestReducer_Append(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerAppend.Reduce(json.RawMessage(`[1,2]`), json.RawMessage(`[3,4]`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `[1,2,3,4]`)

	// Append with different types
	result, err = ReducerAppend.Reduce(json.RawMessage(`["a","b"]`), json.RawMessage(`["c"]`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `["a","b","c"]`)

	// Error on non-array
	_, err = ReducerAppend.Reduce(json.RawMessage(`"not an array"`), json.RawMessage(`[1]`))
	assert.Error(err)
}

func TestReducer_Add(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerAdd.Reduce(json.RawMessage(`10`), json.RawMessage(`5`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `15`)

	// Floating point
	result, err = ReducerAdd.Reduce(json.RawMessage(`1.5`), json.RawMessage(`2.5`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `4`)

	// Error on non-number
	_, err = ReducerAdd.Reduce(json.RawMessage(`"not a number"`), json.RawMessage(`1`))
	assert.Error(err)
}

func TestReducer_Min(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerMin.Reduce(json.RawMessage(`10`), json.RawMessage(`5`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `5`)

	result, err = ReducerMin.Reduce(json.RawMessage(`3`), json.RawMessage(`7`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `3`)

	result, err = ReducerMin.Reduce(json.RawMessage(`-2.5`), json.RawMessage(`-2.5`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `-2.5`)

	// Cleared existing defers to incoming so a missing-base does not collapse to 0.
	result, err = ReducerMin.Reduce(nil, json.RawMessage(`4`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `4`)

	// Cleared incoming preserves existing.
	result, err = ReducerMin.Reduce(json.RawMessage(`9`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `9`)

	_, err = ReducerMin.Reduce(json.RawMessage(`"not a number"`), json.RawMessage(`1`))
	assert.Error(err)
}

func TestReducer_Max(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerMax.Reduce(json.RawMessage(`10`), json.RawMessage(`5`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `10`)

	result, err = ReducerMax.Reduce(json.RawMessage(`3`), json.RawMessage(`7`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `7`)

	result, err = ReducerMax.Reduce(json.RawMessage(`-2.5`), json.RawMessage(`-2.5`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `-2.5`)

	result, err = ReducerMax.Reduce(nil, json.RawMessage(`4`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `4`)

	result, err = ReducerMax.Reduce(json.RawMessage(`9`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `9`)

	_, err = ReducerMax.Reduce(json.RawMessage(`"not a number"`), json.RawMessage(`1`))
	assert.Error(err)
}

func TestReducer_Union(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerUnion.Reduce(json.RawMessage(`[1,2,3]`), json.RawMessage(`[2,3,4]`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `[1,2,3,4]`)

	// Strings
	result, err = ReducerUnion.Reduce(json.RawMessage(`["a","b"]`), json.RawMessage(`["b","c"]`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `["a","b","c"]`)

	// No overlap
	result, err = ReducerUnion.Reduce(json.RawMessage(`[1]`), json.RawMessage(`[2]`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `[1,2]`)

	// Error on non-array
	_, err = ReducerUnion.Reduce(json.RawMessage(`"not an array"`), json.RawMessage(`[1]`))
	assert.Error(err)
}

func TestReducer_And(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerAnd.Reduce(json.RawMessage(`true`), json.RawMessage(`true`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `true`)

	result, err = ReducerAnd.Reduce(json.RawMessage(`true`), json.RawMessage(`false`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `false`)

	result, err = ReducerAnd.Reduce(json.RawMessage(`false`), json.RawMessage(`false`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `false`)

	_, err = ReducerAnd.Reduce(json.RawMessage(`1`), json.RawMessage(`true`))
	assert.Error(err)
}

func TestReducer_Or(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerOr.Reduce(json.RawMessage(`false`), json.RawMessage(`false`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `false`)

	result, err = ReducerOr.Reduce(json.RawMessage(`true`), json.RawMessage(`false`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `true`)

	result, err = ReducerOr.Reduce(json.RawMessage(`true`), json.RawMessage(`true`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `true`)

	_, err = ReducerOr.Reduce(json.RawMessage(`"yes"`), json.RawMessage(`true`))
	assert.Error(err)
}

func TestReducer_Concat(t *testing.T) {
	assert := testarossa.For(t)

	result, err := ReducerConcat.Reduce(json.RawMessage(`"hello "`), json.RawMessage(`"world"`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `"hello world"`)

	result, err = ReducerConcat.Reduce(json.RawMessage(`""`), json.RawMessage(`"x"`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `"x"`)

	_, err = ReducerConcat.Reduce(json.RawMessage(`["a"]`), json.RawMessage(`"b"`))
	assert.Error(err)
}

func TestReducer_EmptyDefault(t *testing.T) {
	assert := testarossa.For(t)

	// Empty string reducer should behave like replace
	result, err := Reducer("").Reduce(json.RawMessage(`"old"`), json.RawMessage(`"new"`))
	assert.NoError(err)
	assert.Expect(string(result.(json.RawMessage)), `"new"`)
}

func TestReducer_Unknown(t *testing.T) {
	assert := testarossa.For(t)

	_, err := Reducer("bogus").Reduce(json.RawMessage(`1`), json.RawMessage(`2`))
	assert.Error(err)
}

func TestReducer_Merge(t *testing.T) {
	assert := testarossa.For(t)

	// New keys win on collision; all keys retained.
	result, err := ReducerMerge.Reduce(
		json.RawMessage(`{"a":1,"b":2}`),
		json.RawMessage(`{"b":3,"c":4}`),
	)
	assert.NoError(err)
	var got map[string]any
	_ = json.Unmarshal(result.(json.RawMessage), &got)
	assert.Expect(got["a"], float64(1))
	assert.Expect(got["b"], float64(3))
	assert.Expect(got["c"], float64(4))

	// Error on non-object
	_, err = ReducerMerge.Reduce(json.RawMessage(`[1,2]`), json.RawMessage(`{"a":1}`))
	assert.Error(err)
	assert.Contains(err.Error(), "merge reducer requires object")
	assert.Contains(err.Error(), "array")
}

func TestReducer_TypeMismatchErrors(t *testing.T) {
	assert := testarossa.For(t)

	_, err := ReducerAdd.Reduce(json.RawMessage(`"hi"`), json.RawMessage(`1`))
	assert.Error(err)
	assert.Contains(err.Error(), "add reducer requires number")
	assert.Contains(err.Error(), "string")

	_, err = ReducerAppend.Reduce(json.RawMessage(`{"a":1}`), json.RawMessage(`[1]`))
	assert.Error(err)
	assert.Contains(err.Error(), "append reducer requires array")
	assert.Contains(err.Error(), "object")

	_, err = ReducerUnion.Reduce(json.RawMessage(`42`), json.RawMessage(`[1]`))
	assert.Error(err)
	assert.Contains(err.Error(), "union reducer requires array")
	assert.Contains(err.Error(), "number")

	_, err = ReducerAnd.Reduce(json.RawMessage(`"true"`), json.RawMessage(`true`))
	assert.Error(err)
	assert.Contains(err.Error(), "and reducer requires bool")
	assert.Contains(err.Error(), "string")

	_, err = ReducerOr.Reduce(json.RawMessage(`0`), json.RawMessage(`true`))
	assert.Error(err)
	assert.Contains(err.Error(), "or reducer requires bool")
	assert.Contains(err.Error(), "number")

	_, err = ReducerConcat.Reduce(json.RawMessage(`42`), json.RawMessage(`"x"`))
	assert.Error(err)
	assert.Contains(err.Error(), "concat reducer requires string")
	assert.Contains(err.Error(), "number")
}

func TestReducer_NullContributionIgnored(t *testing.T) {
	assert := testarossa.For(t)

	// Add: null is identity 0 on either side.
	got, err := ReducerAdd.Reduce(json.RawMessage(`5`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal("5", string(got.(json.RawMessage)))
	got, err = ReducerAdd.Reduce(json.RawMessage(`null`), json.RawMessage(`7`))
	assert.NoError(err)
	assert.Equal("7", string(got.(json.RawMessage)))

	// Append: null contributes nothing.
	got, err = ReducerAppend.Reduce(json.RawMessage(`[1,2]`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal("[1,2]", string(got.(json.RawMessage)))
	got, err = ReducerAppend.Reduce(json.RawMessage(`null`), json.RawMessage(`[3]`))
	assert.NoError(err)
	assert.Equal("[3]", string(got.(json.RawMessage)))

	// Union: null contributes nothing.
	got, err = ReducerUnion.Reduce(json.RawMessage(`["a","b"]`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal(`["a","b"]`, string(got.(json.RawMessage)))

	// Merge: null contributes nothing.
	got, err = ReducerMerge.Reduce(json.RawMessage(`{"k":1}`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal(`{"k":1}`, string(got.(json.RawMessage)))

	// And: a cleared slot is the reducer's identity (true), so it defers to the other side rather than
	// folding in as false.
	got, err = ReducerAnd.Reduce(json.RawMessage(`true`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal("true", string(got.(json.RawMessage)))
	got, err = ReducerAnd.Reduce(json.RawMessage(`null`), json.RawMessage(`false`))
	assert.NoError(err)
	assert.Equal("false", string(got.(json.RawMessage)))
	got, err = ReducerAnd.Reduce(json.RawMessage(`null`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal("null", string(got.(json.RawMessage)))

	// Or: a cleared slot defers to the other side (false is OR's identity).
	got, err = ReducerOr.Reduce(json.RawMessage(`true`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal("true", string(got.(json.RawMessage)))
	got, err = ReducerOr.Reduce(json.RawMessage(`null`), json.RawMessage(`true`))
	assert.NoError(err)
	assert.Equal("true", string(got.(json.RawMessage)))
	got, err = ReducerOr.Reduce(json.RawMessage(`null`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal("null", string(got.(json.RawMessage)))

	// Concat: null is identity "" on either side.
	got, err = ReducerConcat.Reduce(json.RawMessage(`"hi"`), json.RawMessage(`null`))
	assert.NoError(err)
	assert.Equal(`"hi"`, string(got.(json.RawMessage)))
	got, err = ReducerConcat.Reduce(json.RawMessage(`null`), json.RawMessage(`"bye"`))
	assert.NoError(err)
	assert.Equal(`"bye"`, string(got.(json.RawMessage)))
}

func TestMergeState_UnregisteredFieldIsReplace(t *testing.T) {
	assert := testarossa.For(t)

	// Without a registered reducer, every field uses last-write-wins.
	state := map[string]any{
		"count":    json.RawMessage(`10`),
		"items":    json.RawMessage(`["a","b"]`),
		"messages": json.RawMessage(`"old"`),
	}
	changes := map[string]any{
		"count":    json.RawMessage(`5`),
		"items":    json.RawMessage(`["c"]`),
		"messages": json.RawMessage(`"new"`),
	}
	merged, err := MergeState(state, changes, nil)
	if assert.NoError(err) {
		data, _ := marshalAny(merged["count"])
		assert.Expect(string(data), "5")
		data, _ = marshalAny(merged["items"])
		assert.Expect(string(data), `["c"]`)
		data, _ = marshalAny(merged["messages"])
		assert.Expect(string(data), `"new"`)
	}
}

func TestMergeState_Replace(t *testing.T) {
	assert := testarossa.For(t)

	state := map[string]any{"a": 1, "b": 2}
	changes := map[string]any{"b": 3, "c": 4}
	merged, err := MergeState(state, changes, nil)
	if assert.NoError(err) {
		m := merged
		data, _ := marshalAny(m["a"])
		assert.Expect(string(data), "1")
		data, _ = marshalAny(m["b"])
		assert.Expect(string(data), "3")
		data, _ = marshalAny(m["c"])
		assert.Expect(string(data), "4")
	}
}

func TestMergeState_WithReducers(t *testing.T) {
	assert := testarossa.For(t)

	state := map[string]any{
		"count": json.RawMessage(`10`),
		"items": json.RawMessage(`[1,2]`),
		"name":  json.RawMessage(`"old"`),
	}
	changes := map[string]any{
		"count": json.RawMessage(`5`),
		"items": json.RawMessage(`[3]`),
		"name":  json.RawMessage(`"new"`),
	}
	reducers := map[string]Reducer{
		"count": ReducerAdd,
		"items": ReducerAppend,
	}
	merged, err := MergeState(state, changes, reducers)
	if assert.NoError(err) {
		m := merged
		data, _ := marshalAny(m["count"])
		assert.Expect(string(data), "15")
		data, _ = marshalAny(m["items"])
		assert.Expect(string(data), "[1,2,3]")
		data, _ = marshalAny(m["name"])
		assert.Expect(string(data), `"new"`)
	}
}

func TestMergeState_NilInputs(t *testing.T) {
	assert := testarossa.For(t)

	// Nil state
	merged, err := MergeState(nil, map[string]any{"a": 1}, nil)
	if assert.NoError(err) {
		data, _ := marshalAny(merged["a"])
		assert.Expect(string(data), "1")
	}

	// Nil changes
	merged, err = MergeState(map[string]any{"a": 1}, nil, nil)
	if assert.NoError(err) {
		data, _ := marshalAny(merged["a"])
		assert.Expect(string(data), "1")
	}

	// Both nil
	merged, err = MergeState(nil, nil, nil)
	if assert.NoError(err) {
		assert.Expect(len(merged), 0)
	}
}
