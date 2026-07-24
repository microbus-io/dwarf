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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
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
		db, err := e.db.Shard(1)
		assert.NoError(err)
		// flow_id is auto-increment; the first insert on a fresh per-test DB is flow_id=1 on every driver.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			"ftok", "u", "W", []byte("{}"), workflow.StatusRunning, 1, 1, 1000,
		)
		assert.NoError(err)
		// lease_expires is set well into the future so the test engine's own lease-recovery poll does not
		// reset this deliberately-running step to pending mid-test.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, lease_seq, lease_expires) VALUES (?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 60000))",
			1, 1, "stok", "T", "u", workflow.StatusRunning, 1000, ownerSeq,
		)
		assert.NoError(err)
	}

	statuses := func(t *testing.T, e *Engine) (flowStatus, stepStatus string) {
		assert := testarossa.For(t)
		db, err := e.db.Shard(1)
		assert.NoError(err)
		db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=1").Scan(&flowStatus)
		db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE step_id=1").Scan(&stepStatus)
		return strings.TrimSpace(flowStatus), strings.TrimSpace(stepStatus)
	}

	t.Run("stale_lease_is_noop", func(t *testing.T) {
		assert := testarossa.For(t)
		e := NewEngineUnderTest(t)
		e.SetHost(NewTestProxy())
		if err := e.Startup(t.Context()); err != nil {
			t.Fatal(err)
		}
		setup(t, e, 5) // the owner holds generation 5

		// A zombie holding the prior generation 4 returns an error late. It must write nothing.
		fenced, err := e.failStep(ctx, 1, 1, 4, 1, "ftok", errors.New("boom"), "T")
		assert.NoError(err)
		assert.True(fenced)

		flowStatus, stepStatus := statuses(t, e)
		assert.Equal(workflow.StatusRunning, flowStatus) // healthy flow not terminalized
		assert.Equal(workflow.StatusRunning, stepStatus) // owner's step untouched
	})

	t.Run("current_lease_fails", func(t *testing.T) {
		assert := testarossa.For(t)
		e := NewEngineUnderTest(t)
		e.SetHost(NewTestProxy())
		if err := e.Startup(t.Context()); err != nil {
			t.Fatal(err)
		}
		setup(t, e, 5)

		// The current owner (generation 5) fails the step normally.
		fenced, err := e.failStep(ctx, 1, 1, 5, 1, "ftok", errors.New("boom"), "T")
		assert.NoError(err)
		assert.False(fenced)

		flowStatus, stepStatus := statuses(t, e)
		assert.Equal(workflow.StatusFailed, flowStatus)
		assert.Equal(workflow.StatusFailed, stepStatus)
	})
}

// zombieDispatch drives the real dispatch path into the late-error healthy-flow-kill lease-loss race and returns once a peer
// worker has re-claimed and completed the flow while the first ("zombie") dispatch is still blocked. It:
//   - blocks the named task's FIRST dispatch on a release channel (so it sits running under generation N),
//   - force-expires that step's lease and runs pollPendingSteps so a free worker re-claims it (generation
//     N+1) and re-runs it to completion, driving the flow to a terminal outcome,
//   - awaits that terminal outcome, then returns a release func the caller invokes to unblock the zombie.
//
// The zombie's late writes then carry the stale generation N and must be fenced. taskCalls counts dispatches
// of the blocking task (ends at 2: zombie + owner).
func zombieDispatch(t *testing.T, eng *Engine, flowURL, blockTaskName string, taskCalls *atomic.Int64,
	started chan struct{}, release chan struct{}) (flowKey string, outcome *workflow.FlowOutcome) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()

	var err error
	flowKey, err = eng.Create(ctx, flowURL, nil, nil)
	if !assert.NoError(err) {
		return "", nil
	}

	select {
	case <-started:
	case <-time.After(15 * time.Second):
		t.Fatalf("%s's first dispatch never started", blockTaskName)
	}

	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return flowKey, nil
	}
	// Simulate lease loss on the blocked step only (DB-clock-step / overrun): expire its lease, then run
	// recovery so a free worker re-claims it (bumping lease_seq) and re-runs the task to completion.
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE status='"+workflow.StatusRunning+"' AND task_name=?",
		blockTaskName)
	assert.NoError(err)
	eng.pollPendingSteps(ctx)

	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	outcome, err = eng.Await(awaitCtx, flowKey)
	if !assert.NoError(err) {
		return flowKey, nil
	}
	return flowKey, outcome
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

	proxy := NewTestProxy()
	g := workflow.NewGraph("LF-B")
	g.SetEndpoint("A", "lfb/a")
	g.SetEndpoint("B", "lfb/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("lfb/g", g)
	proxy.HandleTask("lfb/a", func(ctx context.Context, f *workflow.Flow) error {
		if aCalls.Add(1) == 1 {
			aStarted <- struct{}{}
			<-aRelease // the first (zombie) dispatch blocks until the peer has finished the flow
		}
		return nil
	})
	proxy.HandleTask("lfb/b", func(ctx context.Context, f *workflow.Flow) error {
		bCalls.Add(1)
		return nil
	})

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	flowKey, outcome := zombieDispatch(t, eng, "lfb/g", "A", &aCalls, aStarted, aRelease)
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie; its late completion UPDATE carries the stale lease_seq and must write nothing.
	close(aRelease)

	_, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	// Give the released zombie ample time to attempt (and, if the fence were broken, complete) its write.
	// A broken fence inserts a second B within milliseconds; the settle window makes the absence meaningful.
	time.Sleep(1 * time.Second)
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

	proxy := NewTestProxy()
	g := workflow.NewGraph("LF-C")
	g.SetEndpoint("A", "lfc/a")
	g.SetEndpoint("X", "lfc/x")
	g.SetEndpoint("Y", "lfc/y")
	g.SetEndpoint("J", "lfc/j")
	g.SetFanIn("J")
	g.AddTransition("A", "X")
	g.AddTransition("A", "Y")
	g.AddTransition("X", "J")
	g.AddTransition("Y", "J")
	g.AddTransition("J", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("lfc/g", g)
	proxy.HandleTask("lfc/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("lfc/x", func(ctx context.Context, f *workflow.Flow) error {
		if xCalls.Add(1) == 1 {
			xStarted <- struct{}{}
			<-xRelease // the first (zombie) X dispatch blocks until the peer has resolved the cohort
		}
		return nil
	})
	proxy.HandleTask("lfc/y", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("lfc/j", func(ctx context.Context, f *workflow.Flow) error {
		jCalls.Add(1)
		return nil
	})

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetWorkers(4))
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	flowKey, outcome := zombieDispatch(t, eng, "lfc/g", "X", &xCalls, xStarted, xRelease)
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie X; its late arrival must be fenced (no second cohort_arrivals bump, no second J).
	close(xRelease)

	_, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	time.Sleep(1 * time.Second) // let the released zombie attempt its fenced write
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
// guarding the recovery machinery itself, previously reasoned-only (the leaseSeq>0 gate). Distinct from
// faultRecoveryResetErr (which tests the reset *failing*): this proves a zombie's reset (completed->pending)
// carrying a stale lease generation matches zero rows, so it cannot rewind a peer's freshly-claimed step.
//
// The completed->pending reset fires when a step is marked `completed` but its follow-up transition
// transaction then fails; the defer rolls it back so it re-dispatches. A real peer can never re-claim a
// `completed` step, so the race the fence defends against is unreachable in production - the test manufactures
// its precondition: freeze the zombie at the reset (its step `completed` under generation N), force the step
// back to `pending` (as lease recovery would) so a peer re-claims it (bumping to N+1) and drives the flow to
// completion, then release the zombie. Its reset, fenced on generation N, must be a no-op.
func TestLeaseFence_RecoveryResetFenced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls atomic.Int64

	proxy := NewTestProxy()
	g := workflow.NewGraph("LF-RR")
	g.SetEndpoint("A", "lfrr/a")
	g.SetEndpoint("B", "lfrr/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("lfrr/g", g)
	proxy.HandleTask("lfrr/a", func(ctx context.Context, f *workflow.Flow) error { aCalls.Add(1); return nil })
	proxy.HandleTask("lfrr/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	// The first (zombie) dispatch of A completes, its transition tx fails, and the recovery defer freezes at the
	// reset checkpoint with A `completed` under generation N.
	//
	// The vehicle is LOCK CONTENTION, not an arbitrary write error, and that is now the only thing it can be: a
	// non-contention failure is retried in place and then CLASSIFIED (persist / failOnPersistError), so it never
	// reaches this arm of the defer. Contention deliberately still does - terminalizing a flow because the
	// database was busy would be exactly backwards - so it is what drives the completed->pending reset now.
	// InjectN(8) is one full round of Transact's retries (transactMaxAttempts), so the transaction gives up and
	// hands a contention error to the defer; the fault is spent, and the peer's re-dispatch runs clean.
	eng.seams.InjectN(8, faultContention, "A")
	eng.seams.Break(checkpointBeforeRecoveryReset)

	flowKey, err := eng.Create(ctx, "lfrr/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.True(eng.seams.WaitTimeout(ctx, 15*time.Second, checkpointBeforeRecoveryReset), "engine never reached checkpoint checkpointBeforeRecoveryReset")

	shardNum, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(shardNum)
	if !assert.NoError(err) {
		return
	}

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
	eng.pollPendingSteps(ctx) // ring the doorbell so a free worker re-claims the now-pending A

	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
	assert.Equal(workflow.StatusCompleted, strings.TrimSpace(beforeStatus))

	// Release the zombie: its reset carries the stale generation N and must match zero rows.
	eng.seams.Resume(checkpointBeforeRecoveryReset)

	// A broken fence would rewind A completed->pending within milliseconds and re-dispatch it a third time
	// after the flow already completed; the settle window makes the fence's zero-row match observable.
	time.Sleep(1 * time.Second)

	var afterStatus string
	var afterSeq int
	assert.NoError(db.QueryRowContext(ctx,
		"SELECT status, lease_seq FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&afterStatus, &afterSeq))
	assert.Equal(workflow.StatusCompleted, strings.TrimSpace(afterStatus)) // peer's step untouched, not rewound
	assert.Equal(beforeSeq, afterSeq)                                      // generation unchanged by the fenced reset
	assert.Equal(int64(2), aCalls.Load())                                  // zombie + peer only, never a third dispatch
	assertInvariants(t, eng)
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

	proxy := NewTestProxy()
	g := workflow.NewGraph("LF-Retry")
	g.SetEndpoint("A", "lfr/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("lfr/g", g)
	proxy.HandleTask("lfr/a", func(ctx context.Context, f *workflow.Flow) error {
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

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	flowKey, outcome := zombieDispatch(t, eng, "lfr/g", "A", &aCalls, aStarted, aRelease)
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie; its late retry rewind carries the stale lease_seq and must match zero rows (no rewind
	// of the completed step, no reap, no re-dispatch).
	close(aRelease)

	_, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	// A broken fence rewinds the completed step to pending within milliseconds; the settle window makes the
	// absence of that rewind meaningful.
	time.Sleep(1 * time.Second)
	var aStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStatus))
	assert.Equal(workflow.StatusCompleted, strings.TrimSpace(aStatus)) // still completed, not rewound to pending
	assert.Equal(int64(3), aCalls.Load())                              // zombie + peer retry + peer completion; the fenced zombie did not re-dispatch a 4th
	assertInvariants(t, eng)                                           // a broken fence would leave a completed flow with a pending step (check #1)
}

// TestLeaseFence_SubgraphParkFenced pins the subgraph-park fence: a zombie whose task armed flow.Subgraph must not
// park its step (running -> running+parkedSubgraph) and spawn a *second* child for a flow the current owner already
// completed. The park UPDATE is gated on `status='running' AND lease_seq=?`, so the zombie's stale generation
// matches zero rows and createSubgraphFlow never runs. A (calls a subgraph) -> END; A's first dispatch is the
// zombie, a peer re-runs A so the real child spawns and completes, then the released zombie's fenced park is a no-op.
func TestLeaseFence_SubgraphParkFenced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls, cCalls atomic.Int64
	aStarted := make(chan struct{}, 1)
	aRelease := make(chan struct{})

	proxy := NewTestProxy()
	parent := workflow.NewGraph("LF-SubPark")
	parent.SetEndpoint("A", "lfs/a")
	parent.AddTransition("A", workflow.END)
	assert.NoError(parent.Validate())
	proxy.HandleGraph("lfs/g", parent)
	child := workflow.NewGraph("LF-SubParkChild")
	child.SetEndpoint("C", "lfs/c")
	child.AddTransition("C", workflow.END)
	assert.NoError(child.Validate())
	proxy.HandleGraph("lfs/child", child)
	proxy.HandleTask("lfs/a", func(ctx context.Context, f *workflow.Flow) error {
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
	proxy.HandleTask("lfs/c", func(ctx context.Context, f *workflow.Flow) error {
		cCalls.Add(1)
		return nil
	})

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetWorkers(3))
	eng.SetHost(proxy)
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	flowKey, outcome := zombieDispatch(t, eng, "lfs/g", "A", &aCalls, aStarted, aRelease)
	if !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Release the zombie; its late park carries the stale lease_seq and must match zero rows (no re-park, no
	// second child flow).
	close(aRelease)

	_, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	// A broken fence parks the completed caller and spawns a duplicate child within milliseconds; settle first.
	time.Sleep(1 * time.Second)
	var children int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id=? AND surgraph_flow_id<>0", flowID).Scan(&children))
	assert.Equal(1, children) // exactly one subgraph child, not a zombie-spawned duplicate
	var aStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStatus))
	assert.Equal(workflow.StatusCompleted, strings.TrimSpace(aStatus)) // caller still completed, not re-parked
	assert.Equal(int64(1), cCalls.Load())                              // the child task ran exactly once
	assertInvariants(t, eng)
}
