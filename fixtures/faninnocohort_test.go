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
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestFanInNoCohort_FailsInsteadOfHotLooping pins the runtime guard for a step that arrives at a fan-in
// with no cohort to arrive at (its lineage_id is 0). Graph.Validate now rejects the edges that produce
// this, but a graph frozen onto a flow BEFORE that fix is replayed from the flow row on every dispatch and
// never re-validated - so the guard is the only thing standing between such a flow and an unbounded hot
// loop.
//
// Without the guard: cohort_arrivals is bumped on step_id=0 (zero rows, no error), the follow-up SELECT of
// that step returns sql.ErrNoRows, the transition transaction aborts, the recovery defer rewinds the
// just-completed step to pending, it re-dispatches, and the task RE-RUNS - side effects and all - forever.
// The reviewer measured 2,735 failed transitions in 2 seconds.
//
// The test forges the state that shape produces (a cohort branch whose lineage_id is cleared to 0) rather
// than a graph the validator would now reject, so it exercises the runtime path exactly.
func TestFanInNoCohort_FailsInsteadOfHotLooping(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var workRuns atomic.Int32

	proxy := engine.NewTestProxy()
	g := workflow.NewGraph("NoCohort")
	g.SetEndpoint("Spawn", "nocohort/spawn")
	g.SetEndpoint("Work", "nocohort/work")
	g.SetEndpoint("Join", "nocohort/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Spawn", "Work", "items", "item")
	g.AddTransition("Work", "Join")
	g.AddTransition("Join", workflow.END)
	assert.NoError(g.Validate()) // a legitimate graph: the wedge is forged in the DB below
	proxy.HandleGraph("nocohort/g", g)

	proxy.HandleTask("nocohort/spawn", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("nocohort/work", func(ctx context.Context, f *workflow.Flow) error {
		if workRuns.Add(1) == 1 {
			started <- struct{}{}
			<-release // hold the branch running while the test clears its lineage in the DB
			// lineage_id is read at CLAIM time, so the forged value only takes effect on a re-claim.
			// A retry rewinds this step in place (preserving lineage_id) and re-dispatches it, which is
			// the cheapest way to make the next attempt read the cohort-less row.
			f.Retry(10*time.Millisecond, 1, 10*time.Millisecond, time.Minute)
			return nil
		}
		return nil // second attempt: transitions to the fan-in with lineage_id=0 - no cohort to arrive at
	})
	proxy.HandleTask("nocohort/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := engine.NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "nocohort/g", map[string]any{"items": []int{1}}, nil)
	if !assert.NoError(err) {
		return
	}
	shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.DB().Shard(shardNum)
	if !assert.NoError(err) {
		return
	}

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		assert.True(false, "branch task never started")
		return
	}

	// Forge it: the branch loses its cohort. Its next task is the fan-in, so on completion it arrives at a
	// cohort whose spawn step is id 0 - the shape a pre-fix graph produced from a trunk step.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lineage_id=0 WHERE flow_id=? AND task_name=?",
		flowID, "Work")
	assert.NoError(err)
	close(release)

	// The step must FAIL, not hot-loop. A bounded Await is the assertion: before the guard this never
	// returned, because the flow never left `running`.
	awaitCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	out, err := e.Await(awaitCtx, flowKey)
	if !assert.NoError(err, "the flow must terminate, not hot-loop") {
		return
	}
	assert.Equal(workflow.StatusFailed, out.Status)
	assert.True(strings.Contains(out.Error, "not part of a fan-out cohort"),
		"the failure names the cause (got %q)", out.Error)

	// And the task must not have been re-executed on a loop: the deliberate retry plus its one re-run -
	// emphatically not thousands (the reviewer measured 2,735 failed transitions in 2 seconds).
	time.Sleep(300 * time.Millisecond)
	assert.True(workRuns.Load() <= 4, "the task did not hot-loop (ran %d times)", workRuns.Load())
}
