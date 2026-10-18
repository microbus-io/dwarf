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
	"math"
	"testing"

	"github.com/microbus-io/testarossa"
)

// Everything a float64 carries exactly round-trips through state, including the 2^53 boundary itself and
// any float. State number precision beyond ±2^53 is not enforced (a large id is carried as a string);
// this pins the values that do go through cleanly.
func TestFlow_SetAcceptsWhatFloat64CarriesExactly(t *testing.T) {
	assert := testarossa.For(t)
	f := NewFlow()

	f.SetInt("boundary", 1<<53)
	assert.Equal(1<<53, f.GetInt("boundary"))

	f.SetInt("millis", 1752459999999) // UnixMilli: ~1.7e12, three orders of magnitude clear
	assert.Equal(1752459999999, f.GetInt("millis"))

	f.SetFloat("huge", 1e300) // float-domain: exact round trip at any magnitude
	assert.Equal(1e300, f.GetFloat("huge"))

	f.SetString("orderID", "1234567890123456789") // the prescribed workaround for a >2^53 id
	assert.Equal("1234567890123456789", f.GetString("orderID"))

	assert.NoError(f.Set("order", map[string]any{"total": 19.99, "lines": []int{1, 2, 3}}))
}

// A control-signal payload is marshalled whatever its shape, a map included rather than passed through as-is.
// Passing a map through would forfeit two things, and both are pinned here.
func TestFlow_PayloadIsValidatedCopiedAndUniform(t *testing.T) {
	t.Run("a map payload is validated, exactly like a struct one", func(t *testing.T) {
		assert := testarossa.For(t)

		// A NaN cannot be marshalled. Accepted from a map, it would surface only later, as an opaque step
		// failure from inside the orchestrator.
		rf := NewRawFlow()
		f := &rf.Flow
		yield, err := f.Interrupt(map[string]any{"score": math.NaN()}, nil)
		assert.Error(err)
		assert.False(yield)
		_, armed := rf.InterruptRequested()
		assert.False(armed, "a rejected payload must not arm the interrupt")

		// Same value, struct shape: same outcome.
		f2 := NewFlow()
		_, err = f2.Interrupt(struct {
			Score float64 `json:"score"`
		}{Score: math.Inf(1)}, nil)
		assert.Error(err)
	})

	t.Run("the payload is copied, not aliased", func(t *testing.T) {
		assert := testarossa.For(t)

		// The caller's map is live. Without the copy, mutating it after the call would edit the payload the
		// orchestrator is about to persist, from under it.
		payload := map[string]any{"question": "approve?"}
		rf := NewRawFlow()
		f := &rf.Flow
		yield, err := f.Interrupt(payload, nil)
		assert.NoError(err)
		assert.True(yield)

		payload["question"] = "MUTATED"
		payload["injected"] = true

		got, armed := rf.InterruptRequested()
		assert.True(armed)
		assert.Equal("approve?", stateVal(got, "question"))
		_, injected := stateVal(got, "injected"), got.Has("injected")
		assert.False(injected)
	})
}

// TestState_FoldRejectsTrailingData pins that a fold rejects a value with more JSON after it. SetJSON does not
// validate, and a fold decodes with a Decoder, which stops after the first value: without the check, a base or
// a contribution of `[1] [2]` would fold as `[1]` and hide the bad bytes.
func TestState_FoldRejectsTrailingData(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	reducers := map[string]Reducer{"f": ReducerUnion}

	base, err := NewState()
	assert.NoError(err)
	base.SetJSON("f", json.RawMessage(`[1] [2]`))
	good, err := NewState([]byte(`{"f":[3]}`))
	assert.NoError(err)
	assert.Error(base.Apply(ChangesOf(good), reducers), "a base with trailing data")

	base, err = NewState([]byte(`{"f":[1]}`))
	assert.NoError(err)
	bad, err := NewState()
	assert.NoError(err)
	bad.SetJSON("f", json.RawMessage(`[3] [4]`))
	assert.Error(base.Apply(ChangesOf(bad), reducers), "a contribution with trailing data")
}

// TestState_FoldsKeepNumbersExact pins that a reduced field's fold writes back every number it did not compute
// as the literal it read, and computes integers exactly, so an id past 2^53 survives every hand-off. Each case
// runs through all three fold paths: Apply, ApplyAll over two contributions, and two writes in one task.
func TestState_FoldsKeepNumbersExact(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		reducer Reducer
		base    string
		first   string
		second  string
		want    string
	}{
		{"merge keeps a carried key's bytes", ReducerMerge, `{"a":{"id":1234567890123456789}}`, `{"b":1}`, `{"c":{"id":1234567890123456791}}`, `{"a":{"id":1234567890123456789},"b":1,"c":{"id":1234567890123456791}}`},
		{"union keeps distinct large ids", ReducerUnion, `[9007199254740993]`, `[9007199254740992]`, `[9007199254740995]`, `[9007199254740993,9007199254740992,9007199254740995]`},
		{"union dedupes an equal large id", ReducerUnion, `[9007199254740993]`, `[9007199254740993]`, `[{"id":1234567890123456789}]`, `[9007199254740993,{"id":1234567890123456789}]`},
		{"union matches a number the database respelled", ReducerUnion, `[0.0000001,1000000000000000000000]`, `[1e-7]`, `[{"n":1e+21}]`, `[0.0000001,1000000000000000000000,{"n":1e+21}]`},
		{"union matches a whole float past 2^53 the database respelled", ReducerUnion, `[100000000000000000000000]`, `[1e+23]`, `[{"n":1e23}]`, `[100000000000000000000000,{"n":1e23}]`},
		{"union matches a respelled whole float the other way", ReducerUnion, `[1e23]`, `[100000000000000000000000]`, `[1.2345678901234567e19]`, `[1e23,1.2345678901234567e19]`},
		{"union matches a whole float under 1e21 the database respelled", ReducerUnion, `[1.2345678901234567e19]`, `[12345678901234567000]`, `[1e23]`, `[1.2345678901234567e19,1e23]`},
		{"union matches equal numbers however spelled", ReducerUnion, `[0,1]`, `[1.0,1e0]`, `[-0]`, `[0,1]`},
		{"union keeps a string apart from a number", ReducerUnion, `[1]`, `["1"]`, `[1e0]`, `[1,"1"]`},
		{"add sums integers exactly", ReducerAdd, `9007199254740993`, `1`, `1`, `9007199254740995`},
		{"add sums floats as float64", ReducerAdd, `0.1`, `0.2`, `0`, `0.30000000000000004`},
		{"add mixes an integer and a float", ReducerAdd, `1`, `0.5`, `1`, `2.5`},
		{"max keeps the larger literal", ReducerMax, `1234567890123456789`, `1`, `1234567890123456788`, `1234567890123456789`},
		{"min keeps the smaller literal", ReducerMin, `1234567890123456790`, `1234567890123456789`, `1234567890123456791`, `1234567890123456789`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)
			reducers := map[string]Reducer{"f": tc.reducer}
			newState := func() State {
				s, err := NewState([]byte(`{"f":` + tc.base + `}`))
				assert.NoError(err)
				return s
			}
			contribution := func(v string) Changes {
				s, err := NewState([]byte(`{"f":` + v + `}`))
				assert.NoError(err)
				return ChangesOf(s)
			}

			applied := newState()
			assert.NoError(applied.Apply(contribution(tc.first), reducers))
			assert.NoError(applied.Apply(contribution(tc.second), reducers))
			assert.Equal(tc.want, string(applied.GetJSON("f")), "Apply")

			all := newState()
			assert.NoError(all.ApplyAll([]Changes{contribution(tc.first), contribution(tc.second)}, reducers))
			assert.Equal(tc.want, string(all.GetJSON("f")), "ApplyAll")

			f := NewRawFlow()
			f.SetRawState(newState())
			f.SetReducers(reducers)
			assert.NoError(f.Set("f", json.RawMessage(tc.first)))
			assert.NoError(f.Set("f", json.RawMessage(tc.second)))
			inTask := newState()
			assert.NoError(inTask.Apply(f.RawChanges(), reducers))
			assert.Equal(tc.want, string(inTask.GetJSON("f")), "two writes in one task")
		})
	}
}
