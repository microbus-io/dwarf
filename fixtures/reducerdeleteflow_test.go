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

/*
Reducer-managed delete at fan-in. A branch's flow.Del writes a cleared (JSON null) change; at fan-in
the cohort members fold in fan_out_ordinal order via the field's reducer. A cleared incoming for a
reducer-managed field is the reducer's IDENTITY - it is IGNORED, leaving the accumulator untouched - so a
branch that deletes a reduced field never wipes the cohort's contributions to it. The outcome is therefore
order-INDEPENDENT:

  - delete first (ordinal 0), then append: the delete is ignored (the seed survives), then the append
    folds onto it -> ["seed", "b2"].
  - append first (ordinal 0), then delete: the append accumulates, then the delete is ignored -> the same
    ["seed", "b2"].

(Deleting a reducer-managed field is not a supported way to clear it - a delete is a REPLACE-field concern.
This pins that a stray delete on a reduced field cannot silently discard other branches' work.) Both
subtests also assert the materialized final_state carries no "log": null tombstone (a delete never survives
as a null in materialized state). fan_out_ordinal is set by static-transition declaration order, so the two
subtests use graphs that differ only in the order the two branches are declared.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestReducerDeleteflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Two branch tasks: one clears the field, one appends its delta.
	proxy.HandleTask("reducerdelete.verify:428/spawn", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("reducerdelete.verify:428/deleter", func(ctx context.Context, f *workflow.Flow) error {
		f.Del("log")
		return nil
	})
	proxy.HandleTask("reducerdelete.verify:428/appender", func(ctx context.Context, f *workflow.Flow) error {
		f.SetStrings("log", []string{"b2"}) // append reducer: write only the delta
		return nil
	})
	proxy.HandleTask("reducerdelete.verify:428/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	// buildGraph wires Spawn -> {Deleter, Appender} -> Join(fan-in) -> END, declaring the two fan-out
	// transitions in the requested order so the fan_out_ordinal is deterministic.
	buildGraph := func(name string, deleterFirst bool) *workflow.Graph {
		g := workflow.NewGraph(name)
		g.SetEndpoint("Spawn", "reducerdelete.verify:428/spawn")
		g.SetEndpoint("Deleter", "reducerdelete.verify:428/deleter")
		g.SetEndpoint("Appender", "reducerdelete.verify:428/appender")
		g.SetEndpoint("Join", "reducerdelete.verify:428/join")
		g.SetFanIn("Join")
		g.SetReducer("log", workflow.ReducerAppend)
		if deleterFirst {
			g.AddTransition("Spawn", "Deleter")
			g.AddTransition("Spawn", "Appender")
		} else {
			g.AddTransition("Spawn", "Appender")
			g.AddTransition("Spawn", "Deleter")
		}
		g.AddTransition("Deleter", "Join")
		g.AddTransition("Appender", "Join")
		g.AddTransition("Join", workflow.END)
		testarossa.NoError(t, g.Validate())
		return g
	}

	proxy.HandleGraph("reducerdelete.verify:428/delete-first", buildGraph("DeleteFirst", true))
	proxy.HandleGraph("reducerdelete.verify:428/append-first", buildGraph("AppendFirst", false))

	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// stringsOf extracts a []string from a JSON-round-tripped []any, or reports absence.
	stringsOf := func(v any) ([]string, bool) {
		arr, ok := v.([]any)
		if !ok {
			return nil, false
		}
		out := make([]string, 0, len(arr))
		for _, e := range arr {
			out = append(out, e.(string))
		}
		return out, true
	}

	t.Run("delete_first_is_ignored_seed_survives", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "reducerdelete.verify:428/delete-first",
			map[string]any{"log": []string{"seed"}}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		// The ordinal-0 delete of a reduced field is ignored (reducer identity); the seed survives and the
		// ordinal-1 append folds onto it.
		got, present := stringsOf(outcome.State["log"])
		if assert.True(present, "log should be present, got %#v", outcome.State["log"]) {
			assert.Equal([]string{"seed", "b2"}, got)
		}
		// A cleared key never materializes as a null tombstone.
		v, exists := outcome.State["log"]
		assert.True(!exists || v != nil, "final_state must not carry a log:null tombstone")
	})

	t.Run("delete_after_append_is_ignored_accumulator_survives", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "reducerdelete.verify:428/append-first",
			map[string]any{"log": []string{"seed"}}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		// The ordinal-0 append accumulates ["seed","b2"], then the ordinal-1 delete is ignored (identity) -
		// the accumulator survives intact.
		got, present := stringsOf(outcome.State["log"])
		if assert.True(present, "log should be present, got %#v", outcome.State["log"]) {
			assert.Equal([]string{"seed", "b2"}, got)
		}
		v, exists := outcome.State["log"]
		assert.True(!exists || v != nil, "final_state must not carry a log:null tombstone")
	})
}
