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
Reducer-managed delete at fan-in (see _MORETESTS.md D5). A branch's flow.Delete writes a cleared
(JSON null) change; at fan-in the cohort members fold in fan_out_ordinal order via MergeState with
the field's reducer, and MergeState DROPS a cleared key regardless of reducer. So the outcome is
order-dependent:

  - delete first (ordinal 0), then append: the delete drops the field (including its seed), then the
    append folds onto the now-absent key -> ["b2"].
  - append first (ordinal 0), then delete: the append accumulates, then the delete drops the whole
    field -> the key is absent.

Both subtests also assert the materialized final_state carries no "log": null tombstone (delete never
survives as a null in materialized state). fan_out_ordinal is set by static-transition declaration
order, so the two subtests use graphs that differ only in the order the two branches are declared.
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
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Two branch tasks: one clears the field, one appends its delta.
	proxy.HandleTask("reducerdelete.verify:428/spawn", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("reducerdelete.verify:428/deleter", func(ctx context.Context, f *workflow.Flow) error {
		f.Delete("log")
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

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

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

	t.Run("delete_first_then_append_drops_seed_keeps_b2", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "reducerdelete.verify:428/delete-first",
			map[string]any{"log": []string{"seed"}}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		// The ordinal-0 delete drops the seeded key; the ordinal-1 append folds onto the absent key.
		got, present := stringsOf(outcome.State["log"])
		if assert.True(present, "log should be present, got %#v", outcome.State["log"]) {
			assert.Equal([]string{"b2"}, got)
		}
		// A cleared key never materializes as a null tombstone.
		v, exists := outcome.State["log"]
		assert.True(!exists || v != nil, "final_state must not carry a log:null tombstone")
	})

	t.Run("append_first_then_delete_drops_whole_field", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "reducerdelete.verify:428/append-first",
			map[string]any{"log": []string{"seed"}}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		// The ordinal-0 append accumulates ["seed","b2"], then the ordinal-1 delete drops the whole field.
		v, exists := outcome.State["log"]
		assert.True(!exists || v == nil, "log should be absent (dropped by the last-folded delete), got %#v", v)
	})
}
