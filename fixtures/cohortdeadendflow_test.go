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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCohortDeadEndFlow pins that a fan-out branch that matches NO onward transition does not complete the
// whole flow. Only a TRUNK step (outside any cohort) reaches END on a zero-transition dead end; a cohort
// MEMBER that dead-ends has failed to reach its fan-in, so completing the flow there would terminalize it
// while sibling branches are still running and skip the fan-in entirely - silently dropping every downstream
// task and the siblings' work while reporting success.
//
// The graph is one a valid workflow author can write and Validate accepts: the branch's single outgoing edge
// is a `when` that can evaluate false at runtime, which Validate cannot see (it checks only the static
// convergence structure). The branch that dead-ends must fail loudly, and - the load-bearing part - the flow
// must NOT report completed while the fan-in and a live sibling were skipped.
func TestCohortDeadEndFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// A sibling that runs to completion while the dead-end branch is being disposed of. If the old bug were
	// present, the dead end would complete the flow and Run would return before this fired.
	var slowSiblingFinished int32
	var joinRan int32

	graph := workflow.NewGraph("CohortDeadEnd")
	graph.SetEndpoint("Spawn", "cohortdeadendflow.verify:664/spawn")
	graph.SetEndpoint("Work", "cohortdeadendflow.verify:664/work")
	graph.SetEndpoint("Join", "cohortdeadendflow.verify:664/join")
	graph.SetFanIn("Join")
	graph.AddTransitionForEach("Spawn", "Work", "items", "item")
	// A branch's ONLY edge is conditional: item 0 sets ok=false and dead-ends before Join. Validate accepts
	// this - statically every branch converges on Join.
	graph.AddTransitionWhen("Work", "Join", "ok == true")
	graph.AddTransitionChain("Join", workflow.END)
	assert.NoError(graph.Validate())
	proxy.HandleGraph("cohortdeadendflow.verify:664/cohort-dead-end", graph)

	proxy.HandleTask("cohortdeadendflow.verify:664/spawn", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("cohortdeadendflow.verify:664/work", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("item") == 0 {
			// Dead end: no transition matches, so this branch never reaches Join.
			f.SetBool("ok", false)
			return nil
		}
		// A live sibling: sleep so the dead-end branch is disposed of while this one is still executing, then
		// route to the fan-in. The flow must wait for this to settle, never terminalize out from under it.
		time.Sleep(200 * time.Millisecond)
		atomic.StoreInt32(&slowSiblingFinished, 1)
		f.SetBool("ok", true)
		return nil
	})
	proxy.HandleTask("cohortdeadendflow.verify:664/join", func(ctx context.Context, f *workflow.Flow) error {
		atomic.StoreInt32(&joinRan, 1)
		return nil
	})

	_, outcome, err := eng.Run(ctx, "cohortdeadendflow.verify:664/cohort-dead-end",
		map[string]any{"items": []int{0, 1}}, nil)
	assert.NoError(err)

	// The dead-end branch must fail the flow, not complete it. Before the fix, the item-0 branch completed the
	// whole flow (StatusCompleted) the instant it dead-ended.
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.True(strings.Contains(outcome.Error, "fan-in"),
		"the failure must name the missing fan-in convergence, got %q", outcome.Error)
	// The fan-in was never a legitimate convergence for this run.
	assert.Equal(int32(0), atomic.LoadInt32(&joinRan), "Join must not run when a branch dead-ended")
	// The live sibling was not abandoned: the cohort resolved only after it finished, so the flow's failure is
	// an honest terminal outcome rather than an early completion that raced a running branch.
	assert.Equal(int32(1), atomic.LoadInt32(&slowSiblingFinished), "the live sibling must run to completion")
}

// TestCohortDeadEndFlow_TrunkStillCompletes is the negative control: the trunk `Join -> END` path, and an
// ordinary linear dead end outside any cohort, must STILL complete the flow. The trunk guard only fires for a
// cohort member (lineage_id != 0); a fan-in step is trunk (its lineage is its spawn source's own, 0 for a
// top-level cohort), so a normal fan-out that fully converges completes exactly as before.
func TestCohortDeadEndFlow_TrunkStillCompletes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("CohortConverges")
	graph.SetEndpoint("Spawn", "cohortdeadendflow.verify:664/c-spawn")
	graph.SetEndpoint("Work", "cohortdeadendflow.verify:664/c-work")
	graph.SetEndpoint("Join", "cohortdeadendflow.verify:664/c-join")
	graph.SetFanIn("Join")
	graph.SetReducer("done", workflow.ReducerAdd)
	graph.AddTransitionForEach("Spawn", "Work", "items", "item")
	graph.AddTransitionChain("Work", "Join", workflow.END)
	assert.NoError(graph.Validate())
	proxy.HandleGraph("cohortdeadendflow.verify:664/cohort-converges", graph)

	proxy.HandleTask("cohortdeadendflow.verify:664/c-spawn", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("cohortdeadendflow.verify:664/c-work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("done", 1)
		return nil
	})
	proxy.HandleTask("cohortdeadendflow.verify:664/c-join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	_, outcome, err := eng.Run(ctx, "cohortdeadendflow.verify:664/cohort-converges",
		map[string]any{"items": []int{0, 1, 2}}, nil)
	assert.NoError(err)
	// Every branch reaches Join, the cohort converges, and the trunk `Join -> END` completes the flow.
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal(3.0, outcome.State.Value("done"), "all three branches folded through the fan-in")
}
