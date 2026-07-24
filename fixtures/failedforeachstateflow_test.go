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

package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestFailedforeachstateflow pins the final_state of a forEach fan-out that FAILED. A failed cohort never
// reaches its fan-in, so the flow's final_state is the merge of its tail steps - and the completed branch
// siblings are tails (their successor_id was never set to a fan-in that never fired). Their state is a
// BRANCH-local snapshot, so the terminal state must be reconciled to what a fan-out's state means once the
// cohort is behind it, exactly as a converging fan-in does:
//
//   - the forEach source array is still there (it is flow state, not branch bookkeeping);
//   - the branch-private element / index / count are NOT (one arbitrary branch's element would otherwise ride
//     out as the flow's terminal state, deciding by completion order which element the caller sees).
//
// A completed run of the same graph is asserted alongside as the control: whatever a converged fan-in yields
// for these fields, the failed path yields too.
func TestFailedforeachstateflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	graph := workflow.NewGraph("FailedForEachState")
	graph.SetEndpoint("Seed", "failedforeachstateflow.verify:428/seed")
	graph.SetEndpoint("Cell", "failedforeachstateflow.verify:428/cell")
	graph.SetEndpoint("Join", "failedforeachstateflow.verify:428/join")
	graph.SetFanIn("Join")
	graph.SetReducer("seen", workflow.ReducerAppend)
	graph.AddTransitionForEach("Seed", "Cell", "items", "item")
	graph.AddTransitionChain("Cell", "Join", workflow.END)
	proxy.HandleGraph("failedforeachstateflow.verify:428/g", graph)

	proxy.HandleTask("failedforeachstateflow.verify:428/seed", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("failedforeachstateflow.verify:428/cell", func(ctx context.Context, f *workflow.Flow) error {
		// A branch can see the array its own element came from - the engine no longer strips it.
		if len(f.GetStrings("items")) != 3 {
			return errors.New("branch cannot see its forEach source array")
		}
		if f.GetString("item") == "b" && !f.GetBool("fixed") {
			return errors.New("cell b is broken")
		}
		f.Set("seen", []any{f.GetString("item")})
		return nil
	})
	proxy.HandleTask("failedforeachstateflow.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	items := []any{"a", "b", "c"}

	assertNoBookkeeping := func(assert *testarossa.Asserter, state map[string]any) {
		assert.Equal(items, state["items"]) // the source array survives the fan-out
		_, hasItem := state["item"]
		_, hasIndex := state["itemIndex"]
		_, hasCount := state["itemCount"]
		assert.False(hasItem, "branch-private item leaked into final_state")
		assert.False(hasIndex, "branch-private itemIndex leaked into final_state")
		assert.False(hasCount, "branch-private itemCount leaked into final_state")
	}

	t.Run("failed_fan_out", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "failedforeachstateflow.verify:428/g",
			map[string]any{"items": items}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assertNoBookkeeping(assert, outcome.State)
	})

	t.Run("converged_fan_in", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := eng.Run(ctx, "failedforeachstateflow.verify:428/g",
			map[string]any{"items": items, "fixed": true}, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assertNoBookkeeping(assert, outcome.State)
	})
}
