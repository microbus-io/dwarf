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
	"strconv"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestFanInFlowLock_NonFinalArrivalTakesNoFlowRowWrite pins the deferred flow-row lock-grab: a NON-FINAL
// cohort arrival must issue ZERO dwarf_flows statements, so a cohort's flow-row cost is constant rather than
// proportional to its width.
//
// This is a PERFORMANCE invariant with no correctness shadow, which is exactly why it needs its own pin.
// Every arrival used to grab the flow row write-first, serializing an entire cohort on one row - measured at
// fan-out width 64 against Postgres: 20 of 43 active backends queued on that single statement, each holding a
// pool connection - to perform two writes that carry no information (the grab itself, and a step_id rewrite of
// 0 over the 0 a fan-out already carries). Deferring the grab to the arrival that actually resolves the cohort
// measured +20% throughput and -16% p50. Nothing about the flow's OUTCOME depends on it: reintroduce a
// per-arrival flow-row write and the fan-in still fires, the reducers still fold, and every other fixture in
// this repo still passes - the only symptom is the throughput quietly going back.
//
// The assertion is WIDTH-INDEPENDENCE, which is stronger and more durable than any particular count. The
// transition transaction touches the flow row on exactly two dispositions here, neither of which is an
// arrival: the fan-out SOURCE (a push transition, which writes cohort_size and is not a pure arrival) and the
// LAST arrival (which resolves the cohort and so inserts the fan-in step - a write the terminal-status guard
// must sit in front of). Each takes the grab plus the closing step_id advance, so a healthy flow of ANY cohort
// width counts 4. Under the old always-grab behavior it would be 2+2N - so the width-4 and width-16 runs would
// disagree (10 vs 34), which is the regression this test exists to catch.
//
// Single worker, no injected faults: a Transact contention retry re-runs the closure and would legitimately
// re-count, so the exact-count form of this assertion requires a contention-free run.
func TestFanInFlowLock_NonFinalArrivalTakesNoFlowRowWrite(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	// The flow-row writes the transition tx makes for a healthy fan-out flow, independent of cohort width:
	// the fan-out source's grab + step_id advance, and the resolving arrival's grab + step_id advance.
	const expectedFlowRowWrites = 4

	run := func(t *testing.T, width int) int {
		assert := testarossa.For(t)

		e := engine.NewEngineUnderTest(t)
		proxy := engine.NewTestProxy()
		e.SetHost(proxy)
		e.SetWorkers(1) // determinism: one worker, so no sibling races and no contention retries
		assert.NoError(e.Startup(t.Context()))

		g := workflow.NewGraph("Fan")
		g.SetEndpoint("Split", "fl/split")
		g.SetEndpoint("Leg", "fl/leg")
		g.SetEndpoint("Join", "fl/join")
		g.SetFanIn("Join")
		g.SetReducer("legs", workflow.ReducerAdd)
		g.AddTransitionForEach("Split", "Leg", "items", "item")
		g.AddTransitionChain("Leg", "Join", workflow.END)
		assert.NoError(g.Validate())
		proxy.HandleGraph("fl/wf", g)

		proxy.HandleTask("fl/split", func(ctx context.Context, f *workflow.Flow) error { return nil })
		proxy.HandleTask("fl/leg", func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("legs", 1)
			return nil
		})
		proxy.HandleTask("fl/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

		items := make([]int, width)
		for i := range items {
			items[i] = i
		}
		flowKey, outcome, err := e.Run(ctx, "fl/wf", map[string]any{"items": items}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		// Every branch really did arrive - otherwise a low write count would just mean a broken fan-in.
		assert.Equal(float64(width), outcome.State.Value("legs"), "every branch must have arrived at the fan-in")

		_, flowID, _, err := keys.ParseFlowKey(flowKey)
		assert.NoError(err)
		return e.Seams().Visits(seamsJoin(engine.CheckpointFlowRowWrite, strconv.Itoa(flowID)))
	}

	var narrow, wide int
	t.Run("width4", func(t *testing.T) {
		assert := testarossa.For(t)
		narrow = run(t, 4)
		assert.Equal(expectedFlowRowWrites, narrow,
			"a 4-wide cohort must write the flow row only at the fan-out source and the resolving arrival")
	})
	t.Run("width16", func(t *testing.T) {
		assert := testarossa.For(t)
		wide = run(t, 16)
		assert.Equal(expectedFlowRowWrites, wide,
			"a 16-wide cohort must write the flow row only at the fan-out source and the resolving arrival")
	})
	// The load-bearing comparison: flow-row cost must not scale with cohort width. Stated separately so a
	// future change to the constant above cannot hide a reintroduced per-arrival grab.
	assert.Equal(narrow, wide,
		"flow-row writes must be independent of cohort width (%d at width 4 vs %d at width 16); "+
			"a per-arrival flow-row write serializes the whole cohort on one row", narrow, wide)
}
