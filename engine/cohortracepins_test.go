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

// Deterministic race pins for a cohort ARRIVAL against a flow-stopping operation. A non-final arrival takes no
// flow-row lock (the terminal-status guard is deferred to the arrival that resolves the cohort), so the guard sits
// at a different statement on each of the two arrival kinds, and each is pinned here: a late non-final arrival
// writes nothing but its counter, and a late last arrival inserts no fan-in. The graceful-Cancel case pins that a
// last arrival in the completion-to-transition gap still carries the cancellation into the fan-in step it inserts.
package engine

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// cohortRaceRig runs A -> {X, Y} -> J with X and Y each held inside their task until released, so a test decides
// which branch arrives first and which arrives last.
type cohortRaceRig struct {
	e        *Engine
	fk       string
	db       *sequel.DB
	shard    int
	flowID   int
	releaseX func()
	releaseY func()
	jRuns    *atomic.Int32
}

func newCohortRaceRig(t *testing.T, prefix string) *cohortRaceRig {
	assert := testarossa.For(t)

	xGate, yGate := make(chan struct{}), make(chan struct{})
	xStarted, yStarted := make(chan struct{}, 1), make(chan struct{}, 1)
	r := &cohortRaceRig{
		releaseX: sync.OnceFunc(func() { close(xGate) }),
		releaseY: sync.OnceFunc(func() { close(yGate) }),
		jRuns:    &atomic.Int32{},
	}

	mux := hosts.NewMux()
	g := workflow.NewGraph("CohortRace")
	g.SetURL("A", prefix+"/a")
	g.SetURL("X", prefix+"/x")
	g.SetURL("Y", prefix+"/y")
	g.SetURL("J", prefix+"/j")
	g.SetFanIn("J")
	g.AddTransition("A", "X")
	g.AddTransition("A", "Y")
	g.AddTransition("X", "J")
	g.AddTransition("Y", "J")
	g.AddTransition("J", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph(prefix+"/g", g)
	mux.HandleTask(prefix+"/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	hold := func(started chan<- struct{}, gate <-chan struct{}) func(context.Context, *workflow.Flow) error {
		return func(ctx context.Context, f *workflow.Flow) error {
			started <- struct{}{}
			<-gate
			return nil
		}
	}
	mux.HandleTask(prefix+"/x", hold(xStarted, xGate))
	mux.HandleTask(prefix+"/y", hold(yStarted, yGate))
	mux.HandleTask(prefix+"/j", func(ctx context.Context, f *workflow.Flow) error { r.jRuns.Add(1); return nil })

	r.e = NewEngineUnderTest(t.Name())
	assert.NoError(r.e.SetWorkers(4)) // both branches must be in their tasks at once
	r.e.SetHost(mux)
	assert.NoError(r.e.Startup(t.Context()))
	// Cleanups run LIFO: the gates open first, so no worker is left blocked in a task when Shutdown drains.
	t.Cleanup(func() { r.e.Shutdown(context.Background()) })
	t.Cleanup(func() { r.releaseX(); r.releaseY() })

	var err error
	r.fk, err = r.e.Create(t.Context(), prefix+"/g", nil, nil)
	assert.NoError(err)
	r.shard, r.flowID, _, err = keys.ParseFlowKey(r.fk)
	assert.NoError(err)
	r.db, err = r.e.db.Shard(r.shard)
	assert.NoError(err)

	// Both branches inside their tasks means A's transition has committed, so A is past the checkpoint a test
	// arms next and will never freeze there.
	for _, started := range []chan struct{}{xStarted, yStarted} {
		select {
		case <-started:
		case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
			t.Fatal("a branch never started")
		}
	}
	return r
}

// arrivals reads the spawn step's cohort counters.
func (r *cohortRaceRig) arrivals(t *testing.T) (arrivals, size int) {
	assert := testarossa.For(t)
	assert.NoError(r.db.QueryRowContext(context.Background(),
		"SELECT cohort_arrivals, cohort_size FROM dwarf_steps WHERE flow_id=? AND task_name='A'", r.flowID).Scan(&arrivals, &size))
	return arrivals, size
}

// awaitArrivals waits for the spawn's arrival count to reach n, i.e. for the arrival's transaction to commit.
func (r *cohortRaceRig) awaitArrivals(t *testing.T, n int) {
	if !enginetest.WaitUntil(t, 10*time.Second, func() bool { got, _ := r.arrivals(t); return got == n }) {
		got, _ := r.arrivals(t)
		t.Fatalf("cohort_arrivals never reached %d (at %d)", n, got)
	}
}

// flowRow is everything a transition could write to the flow row. touch is included because the lock grab the
// terminal guard rides on flips it where the grab is an UPDATE; an unchanged touch is what proves no flow-row write
// committed.
type flowRow struct {
	status, finalState string
	stepID, touch      int
}

func (r *cohortRaceRig) flowRow(t *testing.T) flowRow {
	assert := testarossa.For(t)
	var row flowRow
	assert.NoError(r.db.QueryRowContext(context.Background(),
		"SELECT status, final_state, stopped_step_id, touch FROM dwarf_flows WHERE flow_id=?", r.flowID).Scan(&row.status, &row.finalState, &row.stepID, &row.touch))
	return row
}

func (r *cohortRaceRig) stepCount(t *testing.T) int {
	return enginetest.CountRows(t, r.e, r.shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", r.flowID)
}

// TestTerminateVsNonFinalCohortArrival_Deterministic pins a non-final arrival landing on a flow that Terminate
// stopped after the arriving step was marked completed. The arrival takes no flow-row lock, so nothing guards it
// on the flow's status: its counter bump commits. What must hold is that the bump is ALL it commits - no write to
// the terminated flow's row, no step inserted - and that the cohort counters stay within cohort_size.
func TestTerminateVsNonFinalCohortArrival_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newCohortRaceRig(t, "tnfa")

	// X completes first, so its arrival is the non-final one. Freeze it before its transition.
	r.e.seams.Break(engineseams.CheckpointBeforeTransitionTx)
	r.releaseX()
	assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeTransitionTx, 10*time.Second), "X never reached checkpoint engineseams.CheckpointBeforeTransitionTx")

	assert.NoError(r.e.Terminate(ctx, r.fk, "test"))
	before, stepsBefore := r.flowRow(t), r.stepCount(t)
	assert.Equal(workflow.StatusTerminated, before.status)

	// Release X: its arrival bumps the spawn's counter on the terminated flow.
	r.e.seams.Resume(engineseams.CheckpointBeforeTransitionTx)
	r.awaitArrivals(t, 1)

	arrivals, size := r.arrivals(t)
	assert.Equal(2, size)
	assert.Equal(1, arrivals)
	assert.Equal(before, r.flowRow(t))        // no flow-row write: status, final_state, step_id and touch untouched
	assert.Equal(stepsBefore, r.stepCount(t)) // nothing inserted into the terminated flow
	assert.Equal(int32(0), r.jRuns.Load())
	enginetest.AssertInvariants(t, r.e) // cohort_arrivals <= cohort_size among them
}

// TestTerminateVsLastCohortArrival_Deterministic pins the deferred terminal-status guard on the arrival that
// resolves the cohort. Terminate stops the flow after the last branch was marked completed but before its
// transition; the arrival's bump commits, the guard's flow-row lock then matches zero rows, and the transition
// must end there - no fan-in step, no write to the flow row. Removing the guard inserts J into the terminated flow.
func TestTerminateVsLastCohortArrival_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newCohortRaceRig(t, "tlca")

	// X arrives normally, leaving Y as the last arrival.
	r.releaseX()
	r.awaitArrivals(t, 1)

	r.e.seams.Break(engineseams.CheckpointBeforeTransitionTx)
	r.releaseY()
	assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeTransitionTx, 10*time.Second), "Y never reached checkpoint engineseams.CheckpointBeforeTransitionTx")

	assert.NoError(r.e.Terminate(ctx, r.fk, "test"))
	before, stepsBefore := r.flowRow(t), r.stepCount(t)
	assert.Equal(workflow.StatusTerminated, before.status)

	// Release Y: its bump resolves the cohort, and the guard turns the rest of the transition into a no-op.
	r.e.seams.Resume(engineseams.CheckpointBeforeTransitionTx)
	r.awaitArrivals(t, 2)

	arrivals, size := r.arrivals(t)
	assert.Equal(2, size)
	assert.Equal(2, arrivals)
	assert.Equal(0, enginetest.CountRows(t, r.e, r.shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='J'", r.flowID))
	assert.Equal(0, enginetest.CountRows(t, r.e, r.shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='Y' AND successor_id<>0", r.flowID))
	assert.Equal(before, r.flowRow(t)) // the guard's lock-grab matched nothing, and nothing after it ran
	assert.Equal(stepsBefore, r.stepCount(t))
	assert.Equal(int32(0), r.jRuns.Load())
	enginetest.AssertInvariants(t, r.e)
}

// TestCancelVsLastCohortArrival_Deterministic pins graceful Cancel landing while the last branch sits between its
// completion write and its transition. No step is in progress for Cancel's mark to reach - every branch is
// already completed - so the cancellation can only arrive through the fan-in step the transition inserts: J must
// be inserted under the cancel mark, preempted without running, and the flow must end cancelled.
func TestCancelVsLastCohortArrival_Deterministic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newCohortRaceRig(t, "cvla")

	r.releaseX()
	r.awaitArrivals(t, 1)

	r.e.seams.Break(engineseams.CheckpointBeforeTransitionTx)
	r.releaseY()
	assert.True(r.e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeTransitionTx, 10*time.Second), "Y never reached checkpoint engineseams.CheckpointBeforeTransitionTx")

	assert.NoError(r.e.Cancel(ctx, r.fk, "withdrawn"))
	assert.Equal(workflow.StatusRunning, r.flowRow(t).status) // Cancel does not stop the flow; the mark does

	r.e.seams.Resume(engineseams.CheckpointBeforeTransitionTx)
	enginetest.AwaitFlowStatus(t, r.e, r.fk, workflow.StatusCancelled, 10*time.Second)

	arrivals, size := r.arrivals(t)
	assert.Equal(2, size)
	assert.Equal(2, arrivals)
	var jStatus string
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='J'", r.flowID).Scan(&jStatus))
	assert.Equal(workflow.StatusCancelled, jStatus) // inserted, then preempted at its claim
	assert.Equal(int32(0), r.jRuns.Load())          // the fan-in task never ran
	out, err := r.e.Snapshot(ctx, r.fk)
	if assert.NoError(err) {
		assert.Equal("withdrawn", out.CancelReason)
	}
	enginetest.AssertInvariants(t, r.e)
}
