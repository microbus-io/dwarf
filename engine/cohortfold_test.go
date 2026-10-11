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

package engine

import (
	"encoding/json"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCohortFold_ProjectionEqualsContributions pins, for every reducer, that an inner fan-in's projected part
// folds into the outer cohort exactly as the inner cohort's contributions would have one by one. The outer
// cohort has a member before the inner fan-in - the inner cohort's spawn - whose write folds into the base its
// branches are handed.
func TestCohortFold_ProjectionEqualsContributions(t *testing.T) {
	t.Parallel()

	cases := []struct {
		reducer workflow.Reducer
		outer   any   // the outer fold's base value
		spawn   any   // the inner spawn's own write, an outer member
		inner   []any // the inner cohort's contributions
		own     any   // the inner fan-in task's own write, or nil
	}{
		{workflow.ReducerAppend, []any{"s"}, []any{"c"}, []any{[]any{"a"}, []any{"b", "c"}}, []any{"j"}},
		{workflow.ReducerUnion, []any{"s", "x"}, []any{"x", "c"}, []any{[]any{"x", "a"}, []any{"s", "b", "a"}}, []any{"b", "j"}},
		{workflow.ReducerConcat, "s", "c", []any{"a", "bé"}, "j"},
		{workflow.ReducerMerge, map[string]any{"s": 1}, map[string]any{"c": 2}, []any{map[string]any{"a": 3, "c": 4}, map[string]any{"a": 5}}, map[string]any{"j": 6}},
		{workflow.ReducerAdd, 10, 1, []any{2, 3}, 100},
		{workflow.ReducerMin, 10, 7, []any{9, 4}, 5},
		{workflow.ReducerMax, 1, 3, []any{2, 8}, 6},
		{workflow.ReducerAnd, true, true, []any{true, false}, true},
		{workflow.ReducerOr, false, false, []any{false, true}, false},
		{workflow.ReducerReplace, "s", "c", []any{"a", "b"}, "j"},
		{workflow.ReducerReplace, "s", "c", []any{"a", nil}, nil},
		// An inner cohort that only cleared a reduced field added nothing to it.
		{workflow.ReducerAppend, nil, nil, []any{nil}, nil},
		{workflow.ReducerConcat, nil, nil, []any{nil, nil}, nil},
		{workflow.ReducerMerge, nil, nil, []any{nil}, nil},
		{workflow.ReducerAdd, nil, nil, []any{nil}, nil},
	}
	for _, tc := range cases {
		t.Run(string(tc.reducer), func(t *testing.T) {
			assert := testarossa.For(t)
			reducers := map[string]workflow.Reducer{"f": tc.reducer}
			state := func(v any, present bool) workflow.State {
				s, _ := workflow.NewState()
				if present {
					assert.NoError(s.Set("f", v))
				}
				return s
			}
			outerBase := state(tc.outer, tc.outer != nil)
			spawn := workflow.ChangesOf(state(tc.spawn, tc.spawn != nil))
			var inner []workflow.Changes
			for _, v := range tc.inner {
				inner = append(inner, workflow.ChangesOf(state(v, true))) // a nil is written, as Set(k, nil) writes it
			}
			own := workflow.ChangesOf(state(tc.own, tc.own != nil))

			// One by one: the outer cohort folds the spawn, every inner contribution, then the fan-in's own write.
			flat := outerBase.Clone()
			assert.NoError(flat.ApplyAll(append(append([]workflow.Changes{spawn}, inner...), own), reducers))

			// Nested: the inner fan-in folds its cohort onto the spawn's folded state and records what it added.
			innerState := outerBase.Clone()
			assert.NoError(innerState.Apply(spawn, reducers))
			rec, err := foldCohort(innerState, inner, reducers, true)
			assert.NoError(err)
			// Its row - the state and its state_reduced column - is stored and read back before the outer fold
			// reads it.
			data, err := json.Marshal(innerState)
			assert.NoError(err)
			stored, err := workflow.NewState(data)
			assert.NoError(err)
			var storedRec map[string]*reduced
			assert.NoError(json.Unmarshal(reducedColumn(rec), &storedRec))
			part, err := projectReduced(stored, storedRec, reducers)
			assert.NoError(err)
			contribution, ok, err := memberContribution(part, own, reducers)
			assert.NoError(err)
			assert.True(ok)
			nested := outerBase.Clone()
			assert.NoError(nested.ApplyAll([]workflow.Changes{spawn, contribution}, reducers))

			var want, got any
			_, err = flat.Get("f", &want)
			assert.NoError(err)
			_, err = nested.Get("f", &got)
			assert.NoError(err)
			assert.Equal(want, got)
			assert.Equal(flat.Has("f"), nested.Has("f"))

			// A Fork's leaf freezes the record against the original state and then replaces that state with an
			// override. The frozen record, stored and read back, projects the same part from the new state.
			frozen, err := frozenReduced(stored, storedRec, reducers)
			assert.NoError(err)
			var storedFrozen map[string]*reduced
			assert.NoError(json.Unmarshal(reducedColumn(frozen), &storedFrozen))
			overridden := state("override", true)
			part, err = projectReduced(overridden, storedFrozen, reducers)
			assert.NoError(err)
			contribution, ok, err = memberContribution(part, own, reducers)
			assert.NoError(err)
			assert.True(ok)
			forked := outerBase.Clone()
			assert.NoError(forked.ApplyAll([]workflow.Changes{spawn, contribution}, reducers))
			got = nil
			_, err = forked.Get("f", &got)
			assert.NoError(err)
			assert.Equal(want, got)
			assert.Equal(flat.Has("f"), forked.Has("f"))
		})
	}
}
