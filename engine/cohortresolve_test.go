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
	"context"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// The two-transaction resolve rests on one ordering: a member COMMITS its arrival, and only then counts.
// These pin the two halves of what that buys.
//
// The concurrent case needs N goroutines frozen at ONE checkpoint, which seamster supports from v0.2.1
// (before that a second arrival at an armed breakpoint panicked on a re-closed channel).

// TestCohortResolve_SecondResolveIsANoOp pins the arbiter directly: a cohort may be resolved once, and a
// second attempt on the same spawn must change nothing. Under load several members can each observe a
// complete cohort; without the conditional claim on cohort_resolved each would insert a fan-in step and the
// join would run repeatedly on a doubled merge.
func TestCohortResolve_SecondResolveIsANoOp(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy, joinRuns := cohortRaceGraph(t)
	eng := NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	flowKey, err := eng.Create(ctx, "crc/g", map[string]any{"items": []any{"a", "b"}}, nil)
	if !assert.NoError(err) {
		return
	}
	outcome, err := eng.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assertResolvedExactlyOnce(t, ctx, eng, flowKey, joinRuns, 2)

	shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(shardNum)
	if !assert.NoError(err) {
		return
	}
	var spawnID, lastMember int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='Seed'", flowID).Scan(&spawnID))
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT MAX(step_id) FROM dwarf_steps WHERE flow_id=? AND task_name='Work'", flowID).Scan(&lastMember))

	// Re-enter the resolve exactly as a losing member would: the cohort still counts complete, so it gets
	// past the pre-check and reaches the claim, which must match zero rows.
	res, err := eng.resolveCohort(ctx, db, shardNum, flowID, spawnID, lastMember)
	assert.NoError(err)
	assert.Equal(0, res.fanInStepID, "a losing resolver must insert no fan-in step")
	assert.False(res.flowFailed)

	assertResolvedExactlyOnce(t, ctx, eng, flowKey, joinRuns, 2)
	assertInvariants(t, eng)
}

// TestCohortResolve_SimultaneousFinalArrivals freezes EVERY member after its arrival has committed but
// before it counts, then releases them together - so both count a complete cohort and race the claim. This
// is the window the two-transaction split exists for.
//
// The hazard is the SILENT one, opposite to the loud double-resolve above. Were the count taken inside a
// member's own arrival transaction, the last two members would each miss the other's uncommitted mark under
// READ COMMITTED, each conclude "not me", and the cohort would strand forever with every branch arrived and
// nobody resolving - a hung flow, no error anywhere. Committing first makes the last committer's count
// complete by construction.
//
// Negative-verified 2026-07-19 by forcing the pre-check to see only the caller's own mark - what a count
// inside the arrival transaction sees. This test then HANGS rather than failing: the strand, reproduced.
// Note that failure mode, because it is what makes the bug dangerous - no assertion fires, nothing is
// logged, the flow simply never finishes.
func TestCohortResolve_SimultaneousFinalArrivals(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	const width = 4
	proxy, joinRuns := cohortRaceGraph(t)
	eng := NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	eng.seams.Break(checkpointAfterArrivalTx)

	items := make([]any, width)
	for i := range items {
		items[i] = string(rune('a' + i))
	}
	flowKey, err := eng.Create(ctx, "crc/g", map[string]any{"items": items}, nil)
	if !assert.NoError(err) {
		return
	}

	// Wait until EVERY branch is frozen past its arrival commit, so releasing them is a true N-way race and
	// not a staggered walk-through.
	deadline := time.Now().Add(15 * time.Second)
	for eng.seams.Visits(checkpointAfterArrivalTx) < width {
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d branches reached the checkpoint", eng.seams.Visits(checkpointAfterArrivalTx), width)
		}
		time.Sleep(5 * time.Millisecond)
	}

	// While every branch is frozen here, the flow is in a state the old design never produced: every step
	// terminal, yet the flow still `running`, because the fan-in step is not inserted until the resolve. That
	// is the shape detectOrphanedFlows alarms on, and this asserts the window is real so nobody is surprised
	// by it. It is safe in production only because that detector also requires no step to have been touched
	// for orphanFlowThreshold (5m), and the just-completed arrival's updated_at is seconds old - but any
	// invariant sampled without an age guard CAN observe it, so a checker must tolerate it.
	shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(shardNum)
	if !assert.NoError(err) {
		return
	}
	var nonTerminal int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status NOT IN ('completed','failed','cancelled')",
		flowID).Scan(&nonTerminal))
	assert.Equal(0, nonTerminal, "mid-resolve, every step is terminal")
	var flowStatus string
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&flowStatus))
	assert.Equal(workflow.StatusRunning, strings.TrimSpace(flowStatus),
		"...while the flow is still running: the transient orphan SHAPE the resolve window creates")

	eng.seams.Resume(checkpointAfterArrivalTx)

	outcome, err := eng.Await(ctx, flowKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal(width, len(outcome.State["done"].([]any)), "every branch must be merged into the fan-in")
	assertResolvedExactlyOnce(t, ctx, eng, flowKey, joinRuns, width)
	assertInvariants(t, eng)
}

// cohortRaceGraph builds Seed -forEach(items)-> Work -> Join, the minimal two-branch cohort, and returns a
// counter for how many times the fan-in task ran.
func cohortRaceGraph(t *testing.T) (*TestProxy, *int) {
	t.Helper()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	g := workflow.NewGraph("CohortRace")
	g.SetEndpoint("Seed", "crc/seed")
	g.SetEndpoint("Work", "crc/work")
	g.SetEndpoint("Join", "crc/join")
	g.SetFanIn("Join")
	g.SetReducer("done", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Work", "items", "item")
	g.AddTransitionChain("Work", "Join", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("crc/g", g)

	joinRuns := 0
	proxy.HandleTask("crc/seed", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("crc/work", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("done", []any{f.GetString("item")})
		return nil
	})
	proxy.HandleTask("crc/join", func(ctx context.Context, f *workflow.Flow) error { joinRuns++; return nil })
	return proxy, &joinRuns
}

func assertResolvedExactlyOnce(t *testing.T, ctx context.Context, eng *Engine, flowKey string, joinRuns *int, width int) {
	t.Helper()
	assert := testarossa.For(t)

	shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(shardNum)
	if !assert.NoError(err) {
		return
	}

	var joinSteps int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='Join'", flowID).Scan(&joinSteps))
	assert.Equal(1, joinSteps, "the cohort must resolve exactly once")
	assert.Equal(1, *joinRuns, "the fan-in task must execute exactly once")

	var resolved, arrived int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT cohort_resolved FROM dwarf_steps WHERE flow_id=? AND task_name='Seed'", flowID).Scan(&resolved))
	assert.Equal(1, resolved, "the spawn must be marked resolved")
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='Work' AND cohort_arrived>0", flowID).Scan(&arrived))
	assert.Equal(width, arrived, "every branch must be marked arrived")
}
