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
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestLeaseFence_FailStep pins that failStep's post-execution write is fenced on the dispatch's lease
// generation (lease_seq): a zombie worker holding a stale generation cannot terminalize a flow the current
// owner is healthily re-executing (the "late error → healthy-flow kill" race), while the current owner
// still fails it normally.
func TestLeaseFence_FailStep(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// setup inserts one running root flow (flow_id=1) whose only step (step_id=1) is running under lease
	// generation `ownerSeq` - simulating the worker that legitimately re-claimed after a lease loss.
	setup := func(t *testing.T, e *Engine, ownerSeq int) {
		assert := testarossa.For(t)
		sh, err := e.db.Shard(1)
		assert.Must(assert.NoError(err))
		db := sh.DB()
		// flow_id is auto-increment; the first insert on a fresh per-test DB is flow_id=1 on every driver.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, root_flow_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			"ftok", "u", "W", []byte("{}"), "", workflow.StatusRunning, 1, 1000,
		)
		assert.NoError(err)
		// lease_expires is set well into the future so the test engine's own lease-recovery poll does not
		// reset this deliberately-running step to pending mid-test.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_created_hour, flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, lease_seq, lease_expires) VALUES ((SELECT created_hour FROM dwarf_flows WHERE flow_id=?), ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 60000))",
			1, 1, 1, "stok", "T", "u", workflow.StatusRunning, 1000, ownerSeq,
		)
		assert.NoError(err)
	}

	statuses := func(t *testing.T, e *Engine) (flowStatus, stepStatus string) {
		assert := testarossa.For(t)
		sh, err := e.db.Shard(1)
		assert.Must(assert.NoError(err))
		db := sh.DB()
		db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=1").Scan(&flowStatus)
		db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE step_id=1").Scan(&stepStatus)
		return flowStatus, stepStatus
	}

	t.Run("stale_lease_is_noop", func(t *testing.T) {
		assert := testarossa.For(t)
		e := NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		e.SetHost(hosts.NewMux())
		assert.Must(assert.NoError(e.Startup(t.Context())))
		setup(t, e, 5) // the owner holds generation 5

		// A zombie holding the prior generation 4 returns an error late. It must write nothing.
		fenced, err := e.failStep(ctx, &dispatch{shard: 1, stepID: 1, row: claimRow{leaseSeq: 4, flowID: 1, createdHour: createdHourOf(t, e, 1), flowToken: "ftok", taskName: "T"}}, errors.New("boom"), heldEither)
		assert.NoError(err)
		assert.True(fenced)

		flowStatus, stepStatus := statuses(t, e)
		assert.Equal(workflow.StatusRunning, flowStatus) // healthy flow not terminalized
		assert.Equal(workflow.StatusRunning, stepStatus) // owner's step untouched
	})

	t.Run("current_lease_fails", func(t *testing.T) {
		assert := testarossa.For(t)
		e := NewEngineUnderTest(t.Name())
		defer e.Shutdown(ctx)
		e.SetHost(hosts.NewMux())
		assert.Must(assert.NoError(e.Startup(t.Context())))
		setup(t, e, 5)

		// The current owner (generation 5) fails the step normally.
		fenced, err := e.failStep(ctx, &dispatch{shard: 1, stepID: 1, row: claimRow{leaseSeq: 5, flowID: 1, createdHour: createdHourOf(t, e, 1), flowToken: "ftok", taskName: "T"}}, errors.New("boom"), heldEither)
		assert.NoError(err)
		assert.False(fenced)

		flowStatus, stepStatus := statuses(t, e)
		assert.Equal(workflow.StatusFailed, flowStatus)
		assert.Equal(workflow.StatusFailed, stepStatus)
	})
}

// awaitDispatchesReturned blocks until the named task's step in the flow has returned from n claimed
// dispatches in all, the zombie's among them. The zombie's fenced writes happen inside its dispatch, so once
// it returns, a broken fence has already left its mark. The count is absolute, not counted from the release:
// the peer's dispatch of the same step can still be returning after the flow it drove has completed.
func awaitDispatchesReturned(t *testing.T, eng *Engine, flowKey, taskName string, n int) {
	t.Helper()
	shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		t.Fatal(err)
	}
	sh, err := eng.db.Shard(shard)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	var stepID int
	err = db.QueryRowContext(t.Context(), "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name=?", flowID, taskName).Scan(&stepID)
	if err != nil {
		t.Fatal(err)
	}
	done := diagnostics.Join(diagnostics.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(stepID))
	enginetest.AwaitVisitCount(t, eng.diags.Seams(), n, 15*time.Second, done)
}

// zombieDispatch drives the real dispatch path into the late-error healthy-flow-kill lease-loss race and returns once a peer
// worker has re-claimed and completed the flow while the first ("zombie") dispatch is still blocked. It:
//   - blocks the named task's FIRST dispatch on a release channel (so it sits running under generation N),
//   - force-expires that step's lease and runs recoverExpiredLeases so a free worker re-claims it (generation
//     N+1) and re-runs it to completion, driving the flow to a terminal outcome,
//   - awaits that terminal outcome, then returns a release func the caller invokes to unblock the zombie.
//
// The zombie's late writes then carry the stale generation N and must be fenced. taskCalls counts dispatches
// of the blocking task (ends at 2: zombie + owner).
//
// The caller must `defer unblock()` right after its `defer eng.Shutdown` (defers are LIFO, so it runs first),
// and call it again where the test releases the zombie. Without the defer, a failed assertion that returns
// early leaves the zombie blocked on release, and Shutdown waits on it forever - one failure becomes a
// package-wide timeout that hides the assertion that caused it.
func zombieDispatch(t *testing.T, eng *Engine, flowURL, blockTaskName string, taskCalls *atomic.Int64,
	started chan struct{}, release chan struct{}) (flowKey string, outcome *workflow.FlowOutcome, unblock func()) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()
	unblock = sync.OnceFunc(func() { close(release) })

	var err error
	flowKey, err = eng.Create(ctx, flowURL, nil, nil)
	if !assert.NoError(err) {
		return "", nil, unblock
	}

	select {
	case <-started:
	case <-time.After(15 * time.Second * enginetest.TimeoutScale()):
		unblock() // the caller has not deferred it yet, and a dispatch starting late would block Shutdown
		t.Fatalf("%s's first dispatch never started", blockTaskName)
	}

	// Simulate lease loss on the blocked step only (DB-clock-step / overrun): expire its lease and recover it,
	// so a free worker re-claims it (bumping lease_seq) and re-runs the task to completion.
	expireLease(t, eng, flowKey, blockTaskName)

	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	outcome, err = eng.Await(awaitCtx, flowKey)
	if !assert.NoError(err) {
		return flowKey, nil, unblock
	}
	return flowKey, outcome, unblock
}

// TestLeaseFence_CompletionNoDuplicateSuccessor pins the "late success → duplicate successors"
// variant: a zombie whose completion UPDATE carries a stale lease_seq must not re-complete its step nor
// insert a second successor for the flow a peer already advanced. A -> B -> END; A's first dispatch is the
// zombie, a peer re-runs A to completion (creating exactly one B), then the zombie is released and its
// fenced completion must be a no-op.
func TestLeaseFence_CompletionNoDuplicateSuccessor(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls, bCalls atomic.Int64
	aStarted := make(chan struct{}, 1)
	aRelease := make(chan struct{})

	mux := hosts.NewMux()
	g := workflow.NewGraph("LF-B")
	g.SetURL("A", "lfb/a")
	g.SetURL("B", "lfb/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("lfb/g", g)
	mux.HandleTask("lfb/a", func(ctx context.Context, f *workflow.Flow) error {
		if aCalls.Add(1) == 1 {
			aStarted <- struct{}{}
			<-aRelease // the first (zombie) dispatch blocks until the peer has finished the flow
		}
		return nil
	})
	mux.HandleTask("lfb/b", func(ctx context.Context, f *workflow.Flow) error {
		bCalls.Add(1)
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, outcome, unblock := zombieDispatch(t, eng, "lfb/g", "A", &aCalls, aStarted, aRelease)
	defer unblock()
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie; its late completion UPDATE carries the stale lease_seq and must write nothing.
	unblock()

	_, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	sh, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()
	awaitDispatchesReturned(t, eng, flowKey, "A", 2) // the owner's and the released zombie's
	var bSteps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='B'", flowID).Scan(&bSteps))
	assert.Equal(1, bSteps)               // exactly one successor, not a zombie-inserted duplicate
	assert.Equal(int64(1), bCalls.Load()) // B executed exactly once
	assert.Equal(int64(2), aCalls.Load()) // A ran twice by design (zombie + owner re-dispatch)
}

// TestLeaseFence_CohortNoDoubleArrival pins the fan-out variant: a zombie branch whose completion
// is fenced must not bump cohort_arrivals a second time (which would overshoot cohort_size and re-fire the
// fan-in). A -> {X, Y} -> J (fan-in) -> END; X's first dispatch is the zombie, a peer re-runs X so the
// cohort resolves and J fires exactly once, then the released zombie's fenced arrival must be a no-op.
func TestLeaseFence_CohortNoDoubleArrival(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var xCalls, jCalls atomic.Int64
	xStarted := make(chan struct{}, 1)
	xRelease := make(chan struct{})

	mux := hosts.NewMux()
	g := workflow.NewGraph("LF-C")
	g.SetURL("A", "lfc/a")
	g.SetURL("X", "lfc/x")
	g.SetURL("Y", "lfc/y")
	g.SetURL("J", "lfc/j")
	g.SetFanIn("J")
	g.AddTransition("A", "X")
	g.AddTransition("A", "Y")
	g.AddTransition("X", "J")
	g.AddTransition("Y", "J")
	g.AddTransition("J", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("lfc/g", g)
	mux.HandleTask("lfc/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("lfc/x", func(ctx context.Context, f *workflow.Flow) error {
		if xCalls.Add(1) == 1 {
			xStarted <- struct{}{}
			<-xRelease // the first (zombie) X dispatch blocks until the peer has resolved the cohort
		}
		return nil
	})
	mux.HandleTask("lfc/y", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("lfc/j", func(ctx context.Context, f *workflow.Flow) error {
		jCalls.Add(1)
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(4))
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, outcome, unblock := zombieDispatch(t, eng, "lfc/g", "X", &xCalls, xStarted, xRelease)
	defer unblock()
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie X; its late arrival must be fenced (no second cohort_arrivals bump, no second J).
	unblock()

	_, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	sh, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()
	awaitDispatchesReturned(t, eng, flowKey, "X", 2) // the owner's and the released zombie's
	var arrivals, size, jSteps int
	assert.NoError(db.QueryRowContext(ctx, "SELECT cohort_arrivals, cohort_size FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&arrivals, &size))
	assert.Equal(2, size)     // two branches spawned
	assert.Equal(2, arrivals) // both arrived exactly once; the zombie did not add a third
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='J'", flowID).Scan(&jSteps))
	assert.Equal(1, jSteps)               // fan-in fired exactly once
	assert.Equal(int64(1), jCalls.Load()) // J executed exactly once
	assert.Equal(int64(2), xCalls.Load()) // X ran twice by design (zombie + owner re-dispatch)
}

// TestLeaseFence_RecoveryResetFenced pins the fence on the processStep recovery defer's own reset - the one
// guarding the recovery machinery itself (the leaseSeq>0 gate). Distinct from
// diagnostics.FaultRecoveryResetErr (which tests the reset *failing*): this proves a zombie's reset (completed->pending)
// carrying a stale lease generation matches zero rows, so it cannot rewind a peer's freshly-claimed step.
//
// The completed->pending reset fires when a step is marked `completed` but its follow-up transition
// transaction then fails; the defer rolls it back so it re-dispatches (as a replay - the task does not run
// again). The one thing that takes a `completed` step back from under its worker in production is the orphan
// sweep, and it bumps the generation as it does; the test manufactures the harder case, a take-back that does
// not: freeze the zombie at the reset (its step `completed` under generation N), force the step back to `pending`
// without a bump so a peer re-claims it (bumping to N+1) and drives the flow to completion, then release the
// zombie. Its reset, fenced on generation N, must be a no-op.
func TestLeaseFence_RecoveryResetFenced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls atomic.Int64

	mux := hosts.NewMux()
	g := workflow.NewGraph("LF-RR")
	g.SetURL("A", "lfrr/a")
	g.SetURL("B", "lfrr/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("lfrr/g", g)
	mux.HandleTask("lfrr/a", func(ctx context.Context, f *workflow.Flow) error { aCalls.Add(1); return nil })
	mux.HandleTask("lfrr/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	// The first (zombie) dispatch of A completes, its transition tx fails, and the recovery defer freezes at the
	// reset checkpoint with A `completed` under generation N.
	//
	// The vehicle is LOCK CONTENTION, not an arbitrary write error, and that is the only thing it can be: a
	// non-contention failure is retried in place and then CLASSIFIED (persist / failOnPersistError), so it never
	// reaches this arm of the defer. Contention deliberately still does - terminalizing a flow because the
	// database was busy would be exactly backwards - so it is what drives the completed->pending reset.
	// InjectN(8) is one full round of Transact's retries (transactMaxAttempts), so the transaction gives up and
	// hands a contention error to the defer; the fault is spent, and the peer's re-dispatch runs clean.
	eng.diags.Seams().InjectN(diagnostics.Join(diagnostics.FaultContention, "A"), 8)
	eng.diags.Seams().Break(diagnostics.CheckpointBeforeRecoveryReset)

	flowKey, err := eng.Create(ctx, "lfrr/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.True(eng.diags.Seams().WaitTimeout(ctx, diagnostics.CheckpointBeforeRecoveryReset, 15*time.Second), "engine never reached checkpoint diagnostics.CheckpointBeforeRecoveryReset")

	shardNum, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	sh, err := eng.db.Shard(shardNum)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()

	// Manufacture the peer re-claim the reset fence guards against. Lease recovery resets a lost-lease step
	// running->pending WITHOUT bumping lease_seq; here the step is `completed`, so we force the same pending
	// state directly (the completed->pending reset a real peer can never trigger on its own). The peer's claim
	// then bumps the generation to N+1 and, with the fault already spent, drives A->B->END.
	res, err := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status='"+workflow.StatusPending+"', lease_expires=NOW_UTC() WHERE flow_id=? AND task_name='A' AND status='"+workflow.StatusCompleted+"'",
		flowID)
	assert.NoError(err)
	if n, _ := res.RowsAffected(); !assert.Equal(int64(1), n) { // the zombie really did mark A completed
		return
	}
	// A is pending, so the shard's next piston cycle hands it to a free worker.

	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	outcome, err := eng.Await(awaitCtx, flowKey)
	if !assert.NoError(err) || !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Snapshot the peer's A (completed under N+1) before releasing the zombie.
	var beforeStatus string
	var beforeSeq int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT status, lease_seq FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&beforeStatus, &beforeSeq))
	assert.Equal(workflow.StatusCompleted, beforeStatus)

	// Release the zombie: its reset carries the stale generation N and must match zero rows.
	eng.diags.Seams().Resume(diagnostics.CheckpointBeforeRecoveryReset)

	// A broken fence would rewind A completed->pending inside the zombie's dispatch.
	awaitDispatchesReturned(t, eng, flowKey, "A", 2) // the peer's replay and the released zombie's

	var afterStatus string
	var afterSeq int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT status, lease_seq FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&afterStatus, &afterSeq))
	assert.Equal(workflow.StatusCompleted, afterStatus) // peer's step untouched, not rewound
	assert.Equal(beforeSeq, afterSeq)                   // generation unchanged by the fenced reset
	assert.Equal(int64(1), aCalls.Load())               // the zombie ran A; the peer replayed it, and nothing ran it again
	enginetest.AssertInvariants(t, eng)
}

// TestLeaseFence_RetryRewindFenced pins the retry-rewind fence: a zombie whose task armed flow.Retry must not
// rewind (completed -> pending) a step the current owner already drove to completion. The retry UPDATE is gated
// on `status='running' AND lease_seq=?`, so the zombie's stale generation matches zero rows and its follow-up
// (the deleteSubgraphFlowsRootedAt reap + re-dispatch) never runs. A -> END; A's first dispatch is the zombie, a
// peer re-runs A (arming a *real* retry, then completing), then the released zombie's fenced rewind is a no-op.
func TestLeaseFence_RetryRewindFenced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls atomic.Int64
	aStarted := make(chan struct{}, 1)
	aRelease := make(chan struct{})

	mux := hosts.NewMux()
	g := workflow.NewGraph("LF-Retry")
	g.SetURL("A", "lfr/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("lfr/g", g)
	mux.HandleTask("lfr/a", func(ctx context.Context, f *workflow.Flow) error {
		switch aCalls.Add(1) {
		case 1:
			// The first (zombie) dispatch blocks until the peer has finished the flow, then arms a retry whose
			// rewind must be fenced by the stale lease_seq.
			aStarted <- struct{}{}
			<-aRelease
			f.Retry(0, 1.0, 0, 0)
			return nil
		case 2:
			// The peer (current owner) drives a real, immediate retry, rewinding the step and re-dispatching.
			f.Retry(0, 1.0, 0, 0)
			return nil
		default:
			return nil // the re-dispatch after the real retry completes the flow
		}
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, outcome, unblock := zombieDispatch(t, eng, "lfr/g", "A", &aCalls, aStarted, aRelease)
	defer unblock()
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie; its late retry rewind carries the stale lease_seq and must match zero rows (no rewind
	// of the completed step, no reap, no re-dispatch).
	unblock()

	_, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	sh, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()
	awaitDispatchesReturned(t, eng, flowKey, "A", 3) // the peer's two (retry, then completion) and the released zombie's
	var aStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStatus))
	assert.Equal(workflow.StatusCompleted, aStatus) // still completed, not rewound to pending
	assert.Equal(int64(3), aCalls.Load())           // zombie + peer retry + peer completion; the fenced zombie did not re-dispatch a 4th
	enginetest.AssertInvariants(t, eng)             // a broken fence would leave a completed flow with a pending step (check #1)
}

// TestLeaseFence_SubgraphParkFenced pins the subgraph-park fence: a zombie whose task armed flow.Subgraph must not
// park its step (running -> running+parkedSubgraph) and spawn a *second* child for a flow the current owner already
// completed. The park UPDATE is gated on `status='running' AND lease_seq=?`, so the zombie's stale generation
// matches zero rows, rolling the spawn back before any child is inserted. A (calls a subgraph) -> END; A's first dispatch is the
// zombie, a peer re-runs A so the real child spawns and completes, then the released zombie's fenced park is a no-op.
func TestLeaseFence_SubgraphParkFenced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls, cCalls atomic.Int64
	aStarted := make(chan struct{}, 1)
	aRelease := make(chan struct{})

	mux := hosts.NewMux()
	parent := workflow.NewGraph("LF-SubPark")
	parent.SetURL("A", "lfs/a")
	parent.AddTransition("A", workflow.End)
	assert.NoError(parent.Validate())
	mux.HandleGraph("lfs/g", parent)
	child := workflow.NewGraph("LF-SubParkChild")
	child.SetURL("C", "lfs/c")
	child.AddTransition("C", workflow.End)
	assert.NoError(child.Validate())
	mux.HandleGraph("lfs/child", child)
	mux.HandleTask("lfs/a", func(ctx context.Context, f *workflow.Flow) error {
		if aCalls.Add(1) == 1 {
			// The first (zombie) dispatch blocks until the peer has finished the flow, then arms a subgraph whose
			// park must be fenced by the stale lease_seq (so it spawns no second child).
			aStarted <- struct{}{}
			<-aRelease
		}
		yield, err := f.Subgraph("lfs/child", nil, nil)
		if err != nil {
			return err
		}
		if yield {
			return nil
		}
		return nil // re-entry after the child completed
	})
	mux.HandleTask("lfs/c", func(ctx context.Context, f *workflow.Flow) error {
		cCalls.Add(1)
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(mux)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, outcome, unblock := zombieDispatch(t, eng, "lfs/g", "A", &aCalls, aStarted, aRelease)
	defer unblock()
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie; its late park carries the stale lease_seq and must match zero rows (no re-park, no
	// second child flow).
	unblock()

	_, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	sh, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()
	awaitDispatchesReturned(t, eng, flowKey, "A", 3) // the peer's two (park, then revive) and the released zombie's
	var children int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id=? AND surgraph_flow_id<>0", flowID).Scan(&children))
	assert.Equal(1, children) // exactly one subgraph child, not a zombie-spawned duplicate
	var aStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStatus))
	assert.Equal(workflow.StatusCompleted, aStatus) // caller still completed, not re-parked
	assert.Equal(int64(1), cCalls.Load())           // the child task ran exactly once
	enginetest.AssertInvariants(t, eng)
}

// TestLeaseRecovery_EndToEnd is the end-to-end proof of lease-based crash recovery:
// a worker "crashes" mid-task (its first execution never returns), its lease expires, recoverExpiredLeases
// resets the step, and a free worker re-executes it to completion. It asserts the whole path heals - the
// flow completes on the second execution - and pins two properties: the dwarf_steps_recovered metric fires,
// and lease recovery is NOT a retry (the step's attempt counter stays 0, unlike flow.Retry which bumps it).
func TestLeaseRecovery_EndToEnd(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	var aCalls atomic.Int64
	aStarted := make(chan struct{}, 1)
	aRelease := make(chan struct{})

	mux := hosts.NewMux()
	g := workflow.NewGraph("LR")
	g.SetURL("A", "lr/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("lr/g", g)
	mux.HandleTask("lr/a", func(ctx context.Context, f *workflow.Flow) error {
		if aCalls.Add(1) == 1 {
			aStarted <- struct{}{}
			<-aRelease // the crashed worker: its first execution is leaked, released only at teardown
		}
		f.SetString("ran", "yes")
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(mux)
	eng.SetMeterProvider(mp)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, outcome, unblock := zombieDispatch(t, eng, "lr/g", "A", &aCalls, aStarted, aRelease)
	defer unblock()
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal("yes", stateVal(outcome.State, "ran")) // the recovery re-execution wrote the state

	// The recovery metric fired: recoverExpiredLeases reset the lost-lease step to pending for re-execution.
	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	recovered, ok := sumCounter(rm, "dwarf_steps_recovered", "", "")
	assert.True(ok, "dwarf_steps_recovered should be present")
	assert.True(recovered >= 1, "at least one step recovered by lease expiry, got %d", recovered)

	// Lease recovery is NOT a retry: the re-executed step's attempt stays 0 (flow.Retry would have bumped it).
	_, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	sh, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()
	var attempt int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT attempt FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND status='"+workflow.StatusCompleted+"'",
		flowID).Scan(&attempt))
	assert.Equal(0, attempt)

	// The task ran exactly twice: the crashed (leaked) attempt plus the recovery re-execution - never a third.
	// Once the re-execution's dispatch has returned, A is completed, so no claim can run it again.
	awaitDispatchesReturned(t, eng, flowKey, "A", 1)
	assert.Equal(int64(2), aCalls.Load())
}

// TestFailStep_TerminalStatusGuard pins that failStep's fenced write carries a status guard
// (status IN ('running','completed')), so it fails only a step a worker legitimately holds, never a terminal
// one. terminateSubtree terminalizes a step to `terminated` WITHOUT bumping lease_seq, so the dispatching
// worker's generation still matches; without the status guard, failStep would rewrite that terminated step to `failed` -
// violating step immutability, miscounting dwarf_steps_executed, and (the one that bites) seeding a phantom
// branch failure that a later Fork re-derives from step status. The guard still admits the `completed` case
// failOnPersistError relies on (fail an already-completed step whose transition tx could not be persisted).
func TestFailStep_TerminalStatusGuard(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// setup inserts one flow (flow_id=1) and its single trunk step (step_id=1, lineage_id=0) in the given
	// statuses under lease generation 5, lease far future so the engine's own recovery poll leaves it alone.
	setup := func(t *testing.T, flowStatus, stepStatus string) (*Engine, *sequel.DB) {
		assert := testarossa.For(t)
		e := NewEngineUnderTest(t.Name())
		e.SetHost(hosts.NewMux())
		assert.Must(assert.NoError(e.Startup(t.Context())))
		sh, err := e.db.Shard(1)
		assert.Must(assert.NoError(err))
		db := sh.DB()
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, root_flow_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			"ftok", "u", "W", []byte("{}"), "", flowStatus, 1, 1000,
		)
		assert.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_created_hour, flow_id, step_depth, step_token, task_name, task_url, status, lineage_id, time_budget_ms, lease_seq, lease_expires) VALUES ((SELECT created_hour FROM dwarf_flows WHERE flow_id=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000))",
			1, 1, 1, "stok", "T", "u", stepStatus, 0, 1000, 5,
		)
		assert.NoError(err)
		return e, db
	}
	statuses := func(t *testing.T, db *sequel.DB) (flowStatus, stepStatus string) {
		assert := testarossa.For(t)
		assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=1").Scan(&flowStatus))
		assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE step_id=1").Scan(&stepStatus))
		return flowStatus, stepStatus
	}

	// A step a racing Terminate already terminalized (terminated, same generation) must NOT be re-failed: the
	// write matches zero rows, failStep reports fenced, and both the step and its flow stay terminated.
	t.Run("terminated_step_is_not_refailed", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusTerminated, workflow.StatusTerminated)
		defer e.Shutdown(ctx)

		fenced, err := e.failStep(ctx, &dispatch{shard: 1, stepID: 1, row: claimRow{leaseSeq: 5, flowID: 1, createdHour: createdHourOf(t, e, 1), flowToken: "ftok", taskName: "T"}}, errors.New("boom"), heldEither)
		assert.NoError(err)
		assert.True(fenced, "a terminated step under our generation must fence, not be rewritten to failed")

		flowStatus, stepStatus := statuses(t, db)
		assert.Equal(workflow.StatusTerminated, stepStatus, "the terminated step must stay terminated (immutable)")
		assert.Equal(workflow.StatusTerminated, flowStatus, "the terminated flow must stay terminated")
	})

	// Control: failOnPersistError's escape hatch - a `completed` step whose transition could not be persisted is
	// still failed by the same call, so the guard must admit `completed`.
	t.Run("completed_step_can_still_fail", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusRunning, workflow.StatusCompleted)
		defer e.Shutdown(ctx)

		fenced, err := e.failStep(ctx, &dispatch{shard: 1, stepID: 1, row: claimRow{leaseSeq: 5, flowID: 1, createdHour: createdHourOf(t, e, 1), flowToken: "ftok", taskName: "T"}}, errors.New("could not persist transition"), heldEither)
		assert.NoError(err)
		assert.False(fenced, "a completed step we own must still be failable (failOnPersistError's escape)")

		flowStatus, stepStatus := statuses(t, db)
		assert.Equal(workflow.StatusFailed, stepStatus)
		assert.Equal(workflow.StatusFailed, flowStatus)
	})
}

// TestFailStep_HeldStates pins each persist site's held state both ways: failStep fails a step in the state the
// site's write starts from - a write that did not land leaves it there, and fencing it would turn a permanent failure
// into a task re-run forever - and matches nothing once the write has landed, so a landed outcome whose
// acknowledgement was lost is never failed. A flow already completed (a flow completion that landed) keeps its
// status and is not handed to a parent as a failure.
func TestFailStep_HeldStates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type row struct {
		status          string
		parked          int
		advancing       int
		advanceOnError  int
		flowStatus      string
		parentStepState string // a parked caller step in a parent flow, if set
	}
	cases := []struct {
		name   string
		held   stepHeld
		row    row
		fenced bool
	}{
		{"running_starts_completion", heldRunning, row{status: workflow.StatusRunning}, false},
		{"running_after_completion_landed", heldRunning, row{status: workflow.StatusCompleted, advancing: 1}, true},
		{"running_after_interrupt_landed", heldRunning, row{status: workflow.StatusInterrupted}, true},
		{"running_after_spawn_landed", heldRunning, row{status: workflow.StatusRunning, parked: parkedSubgraph}, true},
		{"running_after_rewind_landed", heldRunning, row{status: workflow.StatusPending}, true},
		{"advancing_starts_transition", heldAdvancing, row{status: workflow.StatusCompleted, advancing: 1}, false},
		{"advancing_after_transition_landed", heldAdvancing, row{status: workflow.StatusCompleted}, true},
		{"rerouting_starts_reroute", heldRerouting, row{status: workflow.StatusCompleted, advancing: 1}, false},
		{"rerouting_after_reroute_landed", heldRerouting, row{status: workflow.StatusCompleted, advancing: 1, advanceOnError: 1}, true},
		{"advancing_after_flow_completion_landed", heldAdvancing, row{status: workflow.StatusCompleted, advancing: 1, flowStatus: workflow.StatusCompleted, parentStepState: workflow.StatusRunning}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)
			e := NewEngineUnderTest(t.Name())
			defer e.Shutdown(ctx)
			e.SetHost(hosts.NewMux())
			assert.Must(assert.NoError(e.Startup(t.Context())))
			sh, err := e.db.Shard(1)
			assert.Must(assert.NoError(err))
			db := sh.DB()

			flowStatus := tc.row.flowStatus
			if flowStatus == "" {
				flowStatus = workflow.StatusRunning
			}
			surgraphFlowID, surgraphStepID := 0, 0
			if tc.row.parentStepState != "" {
				// The parent flow (flow_id=1) and its caller step (step_id=1), parked on the child.
				_, err = db.ExecContext(ctx,
					"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, root_flow_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
					"ptok", "u", "W", []byte("{}"), "", workflow.StatusRunning, 1, 1000,
				)
				assert.NoError(err)
				_, err = db.ExecContext(ctx,
					"INSERT INTO dwarf_steps (flow_created_hour, flow_id, step_depth, step_token, task_name, task_url, status, parked, time_budget_ms, lease_seq, lease_expires) VALUES ((SELECT created_hour FROM dwarf_flows WHERE flow_id=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000))",
					1, 1, 1, "pstok", "P", "u", tc.row.parentStepState, parkedSubgraph, 1000, 1,
				)
				assert.NoError(err)
				surgraphFlowID, surgraphStepID = 1, 1
			}
			_, err = db.ExecContext(ctx,
				"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, root_flow_id, time_budget_ms, surgraph_flow_id, surgraph_step_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
				"ftok", "u", "W", []byte("{}"), "", flowStatus, 0, 1000, surgraphFlowID, surgraphStepID,
			)
			assert.NoError(err)
			var flowID int
			assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE flow_token='ftok'").Scan(&flowID))
			_, err = db.ExecContext(ctx,
				"INSERT INTO dwarf_steps (flow_created_hour, flow_id, step_depth, step_token, task_name, task_url, status, parked, advancing, advance_on_error, time_budget_ms, lease_seq, lease_expires) VALUES ((SELECT created_hour FROM dwarf_flows WHERE flow_id=?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000))",
				flowID, flowID, 1, "stok", "T", "u", tc.row.status, tc.row.parked, tc.row.advancing, tc.row.advanceOnError, 1000, 5,
			)
			assert.NoError(err)
			var stepID int
			assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE step_token='stok'").Scan(&stepID))

			fenced, err := e.failStep(ctx, &dispatch{shard: 1, stepID: stepID, row: claimRow{leaseSeq: 5, flowID: flowID, createdHour: createdHourOf(t, e, flowID), flowToken: "ftok", taskName: "T"}}, errors.New("boom"), tc.held)
			assert.NoError(err)
			assert.Equal(tc.fenced, fenced)

			var stepStatus, gotFlowStatus string
			assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE step_id=?", stepID).Scan(&stepStatus))
			assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&gotFlowStatus))
			switch {
			case tc.fenced:
				assert.Equal(tc.row.status, stepStatus, "a fenced failStep writes nothing")
				assert.Equal(flowStatus, gotFlowStatus)
			case tc.row.flowStatus == workflow.StatusCompleted:
				assert.Equal(workflow.StatusCompleted, gotFlowStatus, "a completed flow keeps its outcome")
				var parentErr []byte
				var parked int
				assert.NoError(db.QueryRowContext(ctx, "SELECT parked, subgraph_error FROM dwarf_steps WHERE step_id=1").Scan(&parked, &parentErr))
				assert.Equal(parkedSubgraph, parked, "no failure is delivered to the parent of a completed child")
				assert.Equal("", subgraphErrorText(parentErr))
			default:
				assert.Equal(workflow.StatusFailed, stepStatus)
				assert.Equal(workflow.StatusFailed, gotFlowStatus)
			}
		})
	}
}

// createdHourOf reads a flow row's created_hour on shard 1, for a test that calls an engine function directly
// with a flow it inserted by hand.
func createdHourOf(t *testing.T, e *Engine, flowID int) int {
	t.Helper()
	assert := testarossa.For(t)
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var hour int
	assert.NoError(db.QueryRowContext(t.Context(), "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&hour))
	return hour
}
