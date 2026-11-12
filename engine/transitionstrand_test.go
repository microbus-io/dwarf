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
	"errors"
	"strconv"
	"strings"
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

// TestOrphanSweep_RecoversLostTransition pins the repair of the rows a replica leaves when it dies between a step's
// completion write and its transition transaction: the step `completed` with advancing=1 and successor_id=0 on a
// `running` flow, and no live worker holding it. Lease recovery cannot see a completed step, so the orphan sweep is
// the only thing that can advance such a flow - and it does so by replay, so the task does not run again.
//
// The death is simulated rather than injected through the fault seams. A failed transition is retried in place by
// persist (holding the lease) and then either succeeds or is classified as a payload fault and fails the step
// cleanly, so no in-process fault combination leaves this shape - only a process that is gone does. Here the task
// blocks, the test writes the completion itself (as the dying worker's completion write would) and bumps lease_seq,
// and the worker's own later writes are then fenced off exactly as a dead worker's never happen.
//
// A fresh replica takes over the database, starting BEFORE the first shuts down: that is the realistic shape (a
// survivor, not a cold restart), and it keeps SQLite's in-memory test database alive, since that is dropped when its
// last connection closes.
func TestOrphanSweep_RecoversLostTransition(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Strand")
	g.SetURL("A", "strand/a")
	g.SetURL("B", "strand/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph("strand/g", g)
	var runsA, runsB atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mux.HandleTask("strand/a", func(ctx context.Context, f *workflow.Flow) error {
		if runsA.Add(1) == 1 {
			started <- struct{}{}
			<-release
		}
		return nil
	})
	mux.HandleTask("strand/b", func(ctx context.Context, f *workflow.Flow) error { runsB.Add(1); return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	var releaseOnce sync.Once
	releaseA := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseA() // below the Shutdown defer, so an early exit frees the held worker before the drain waits on it
	e.SetHost(mux)
	if !assert.NoError(e.Startup(t.Context())) {
		return
	}

	fk, err := e.Create(ctx, "strand/g", nil, nil)
	assert.NoError(err)
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	select {
	case <-started:
	case <-time.After(5 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("A never started")
	}
	sh, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status='"+workflow.StatusCompleted+"', changes=?, advancing=1, lease_seq=lease_seq+1, updated_at=NOW_UTC()"+
			" WHERE flow_id=? AND task_name='A'", []byte(`{"a":1}`), flowID)
	assert.NoError(err)
	var aStepID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStepID))
	aDone := engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(aStepID))
	aDoneBase := e.seams.Visits(aDone)
	releaseA()

	stranded := func(e *Engine) bool {
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND status='"+
			workflow.StatusCompleted+"' AND successor_id=0", flowID) == 1 &&
			enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", flowID) == 1 &&
			enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND status='"+
				workflow.StatusRunning+"'", flowID) == 1
	}
	// The fenced worker's own writes are the only thing that could advance the flow; once its dispatch has
	// returned, they have been attempted.
	enginetest.AwaitVisitCount(t, e.seams, aDoneBase+1, 15*time.Second, aDone)
	assert.True(stranded(e), "expected A completed with no successor on a running flow")

	e2 := NewEngineUnderTest(t.Name())
	defer e2.Shutdown(ctx)
	e2.SetHost(mux)
	if !assert.NoError(e2.Startup(t.Context())) {
		return
	}
	assert.NoError(e.Shutdown(ctx))
	awaitStartupRecoverySweep(t, e2)
	sh2, err := e2.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	db2 := sh2.DB()

	// The lease still holds A for its dead worker, so the sweep waits.
	e2.recoverOrphanedFlows(ctx, db2, shard, true)
	assert.True(stranded(e2), "the sweep rewound a step whose lease was unexpired")

	// Lapse the lease, as time would. A LIGHT pass - the one that runs every tick - rewinds A at once, with the age
	// threshold at its default, since A's lease is the only wait. A is replayed, B runs, and the flow completes.
	_, err = db2.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", flowID)
	assert.NoError(err)
	e2.runRecoverySweep(ctx, false)
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e2.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(1, out.State.GetInt("a"), "the replay carried the recorded output forward")
	}
	assert.Equal(int32(1), runsA.Load(), "A was replayed, not run again")
	assert.Equal(int32(1), runsB.Load())
	enginetest.AssertInvariants(t, e2)
}

// slowWorkerReplay creates a flow at flowURL on a fresh engine and stalls the worker that completes its entry step
// A just before A's transition, exactly as a worker frozen past its lease would be. With the stall held, it lapses
// A's lease and runs the orphan sweep, which must take A back by bumping its lease generation; beforeSweep, if set,
// runs just before the sweep. Then it releases the stalled worker and awaits the flow, which must complete. While
// the stall is held, the worker that replays A may reach the same checkpoint and stall too; the release frees both,
// and whichever commits first, the stalled worker's transition is fenced. e is an engine built by the caller (who
// owns its Shutdown) and not yet started. It returns the flow and when the sweep ran.
func slowWorkerReplay(t *testing.T, e *Engine, mux *hosts.Mux, flowURL string, initial any, beforeSweep func(db *sequel.DB, flowID int)) (shard, flowID int, swept time.Time) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()

	e.SetHost(mux)
	if !assert.NoError(e.Startup(t.Context())) {
		t.FailNow()
	}
	awaitStartupRecoverySweep(t, e)

	e.seams.Break(engineseams.CheckpointBeforeTransitionTx)
	fk, err := e.Create(ctx, flowURL, initial, nil)
	assert.NoError(err)
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeTransitionTx, 10*time.Second), "engine never reached checkpoint engineseams.CheckpointBeforeTransitionTx")
	shard, _, flowID, _, err = keys.ParseFlowKey(fk)
	assert.NoError(err)
	sh, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		t.FailNow()
	}
	db := sh.DB()
	var stalledSeq int
	assert.NoError(db.QueryRowContext(ctx, "SELECT lease_seq FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND status='"+
		workflow.StatusCompleted+"' AND advancing=1", flowID).Scan(&stalledSeq), "the completion write did not set advancing")

	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", flowID)
	assert.NoError(err)
	if beforeSweep != nil {
		beforeSweep(db, flowID)
	}
	swept = time.Now()
	e.recoverOrphanedFlows(ctx, db, shard, true)
	assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='A' AND lease_seq>?", flowID, stalledSeq),
		"the sweep did not take A back")

	// Both of A's claimed dispatches - the stalled worker and the replay - are still to return: the stall holds the
	// first, and the replay cannot get past the same checkpoint before the release.
	var aStepID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStepID))
	aDone := engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(aStepID))
	aDoneBase := e.seams.Visits(aDone)

	e.seams.Resume(engineseams.CheckpointBeforeTransitionTx)
	awaitCtx, cancel := context.WithTimeout(ctx, 15*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	// The flow can complete before the stalled worker returns; a double advance would land by the time it does.
	enginetest.AwaitVisitCount(t, e.seams, aDoneBase+2, 15*time.Second, aDone)
	return shard, flowID, swept
}

// TestOrphanSweep_ReplaysSlowWorkersStep pins that the sweep's rewind of a step whose worker was slow rather than
// dead neither double-advances the flow nor runs the task again, and that the replay takes the transition the task
// chose (see slowWorkerReplay for the scenario). With the fence removed from the transition's step-row write, the
// slow worker advances the flow ahead of the replay and the counts or the invariant check fail; with replay
// disabled, A runs twice.
//
// One graph covers each part of the recorded disposition: A takes a plain edge to B, a Goto edge to C that the
// plain edge would not take, and an onError edge to D (whose recorded changes carry onErr). The sleep case pins that
// a replay keeps the recorded wake time: the deadline is backdated 4s of its 6s, as if the step had sat stranded
// that long, and B must wake about 2s after the sweep, where a restarted sleep would take the full 6s.
func TestOrphanSweep_ReplaysSlowWorkersStep(t *testing.T) {
	cases := []struct {
		name  string
		taskA func(f *workflow.Flow) error
		next  string
	}{
		{name: "plain", taskA: func(f *workflow.Flow) error { return nil }, next: "B"},
		{name: "goto", taskA: func(f *workflow.Flow) error { f.Goto("C"); return nil }, next: "C"},
		{name: "on_error", taskA: func(f *workflow.Flow) error { return errors.New("boom") }, next: "D"},
		{name: "sleep", taskA: func(f *workflow.Flow) error { f.Sleep(6 * time.Second); return nil }, next: "B"},
		// The replay folds the recorded contribution once: B is handed one entry, not two.
		{name: "reduced", taskA: func(f *workflow.Flow) error { return f.Set("log", []string{"a"}) }, next: "B"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)

			mux := hosts.NewMux()
			g := workflow.NewGraph("Slow")
			for _, n := range []string{"A", "B", "C", "D"} {
				g.SetURL(n, "slowstrand/"+strings.ToLower(n))
			}
			g.SetReducer("log", workflow.ReducerAppend)
			g.AddTransition("A", "B")
			g.AddTransitionGoto("A", "C")
			g.AddTransitionOnError("A", "D")
			g.AddTransition("B", workflow.End)
			g.AddTransition("C", workflow.End)
			g.AddTransition("D", workflow.End)
			mux.HandleGraph("slowstrand/g", g)
			runs := map[string]*atomic.Int32{"A": {}, "B": {}, "C": {}, "D": {}}
			var dSawErr atomic.Bool
			var bLogLen atomic.Int32
			mux.HandleTask("slowstrand/a", func(ctx context.Context, f *workflow.Flow) error { runs["A"].Add(1); return tc.taskA(f) })
			mux.HandleTask("slowstrand/b", func(ctx context.Context, f *workflow.Flow) error {
				runs["B"].Add(1)
				bLogLen.Store(int32(len(f.GetStrings("log"))))
				return nil
			})
			mux.HandleTask("slowstrand/c", func(ctx context.Context, f *workflow.Flow) error { runs["C"].Add(1); return nil })
			mux.HandleTask("slowstrand/d", func(ctx context.Context, f *workflow.Flow) error {
				runs["D"].Add(1)
				dSawErr.Store(f.Has("onErr"))
				return nil
			})

			var beforeSweep func(db *sequel.DB, flowID int)
			if tc.name == "sleep" {
				beforeSweep = func(db *sequel.DB, flowID int) {
					_, err := db.ExecContext(context.Background(),
						"UPDATE dwarf_steps SET advance_sleep_until=DATE_ADD_MILLIS(advance_sleep_until, -4000) WHERE flow_id=? AND task_name='A'", flowID)
					assert.NoError(err)
				}
			}
			e := NewEngineUnderTest(strings.ReplaceAll(t.Name(), "/", "_")) // the name keys the test database
			defer e.Shutdown(context.Background())
			shard, flowID, swept := slowWorkerReplay(t, e, mux, "slowstrand/g", nil, beforeSweep)
			if tc.name == "sleep" {
				// The 200ms settle inside slowWorkerReplay is part of the elapsed time.
				waited := time.Since(swept)
				assert.True(waited > 500*time.Millisecond && waited < 5*time.Second, "B woke %v after the sweep; the recorded deadline left ~2s", waited)
			}
			assert.Equal(int32(1), runs["A"].Load(), "A ran again instead of being replayed")
			for _, name := range []string{"B", "C", "D"} {
				want := int32(0)
				if name == tc.next {
					want = 1
				}
				assert.Equal(want, runs[name].Load(), "runs of %s", name)
				assert.Equal(int(want), enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='"+name+"'", flowID), "steps of %s", name)
			}
			if tc.name == "on_error" {
				assert.True(dSawErr.Load(), "the handler did not receive onErr")
			}
			if tc.name == "reduced" {
				assert.Equal(int32(1), bLogLen.Load(), "the replay folded A's contribution more than once")
			}
			enginetest.AssertInvariants(t, e)
		})
	}
}

// TestOrphanSweep_ReplaysSlowWorkersStep_OtherTransitions covers the two transition paths outside the transition
// transaction, each with its own fence: a fan-out source whose forEach array is empty converges on its fan-in
// directly (fireFanInDirect), and a step with no onward transition completes the flow (completeFlowSequential,
// unfenced - safe only because the re-dispatch replays the same decision). In both the task runs once and the flow
// advances once.
func TestOrphanSweep_ReplaysSlowWorkersStep_OtherTransitions(t *testing.T) {
	t.Run("fan_in_direct", func(t *testing.T) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		g := workflow.NewGraph("SlowFanIn")
		g.SetURL("A", "slowfanin/a")
		g.SetURL("B", "slowfanin/b")
		g.SetURL("J", "slowfanin/j")
		g.AddTransitionForEach("A", "B", "items", "item")
		g.AddTransition("B", "J")
		g.SetFanIn("J")
		g.AddTransition("J", workflow.End)
		mux.HandleGraph("slowfanin/g", g)
		var runsA, runsB, runsJ atomic.Int32
		mux.HandleTask("slowfanin/a", func(ctx context.Context, f *workflow.Flow) error { runsA.Add(1); return nil })
		mux.HandleTask("slowfanin/b", func(ctx context.Context, f *workflow.Flow) error { runsB.Add(1); return nil })
		mux.HandleTask("slowfanin/j", func(ctx context.Context, f *workflow.Flow) error { runsJ.Add(1); return nil })

		e := NewEngineUnderTest(strings.ReplaceAll(t.Name(), "/", "_")) // the name keys the test database
		defer e.Shutdown(context.Background())
		shard, flowID, _ := slowWorkerReplay(t, e, mux, "slowfanin/g", map[string]any{"items": []any{}}, nil)
		assert.Equal(int32(1), runsA.Load(), "A ran again instead of being replayed")
		assert.Equal(int32(0), runsB.Load())
		assert.Equal(int32(1), runsJ.Load(), "the fan-in ran other than once")
		assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name='J'", flowID))
		enginetest.AssertInvariants(t, e)
	})
	t.Run("flow_completion", func(t *testing.T) {
		assert := testarossa.For(t)
		mux := hosts.NewMux()
		g := workflow.NewGraph("SlowEnd")
		g.SetURL("A", "slowend/a")
		g.AddTransition("A", workflow.End)
		mux.HandleGraph("slowend/g", g)
		var runsA atomic.Int32
		mux.HandleTask("slowend/a", func(ctx context.Context, f *workflow.Flow) error { runsA.Add(1); f.Set("done", true); return nil })

		e := NewEngineUnderTest(strings.ReplaceAll(t.Name(), "/", "_")) // the name keys the test database
		defer e.Shutdown(context.Background())
		shard, flowID, _ := slowWorkerReplay(t, e, mux, "slowend/g", nil, nil)
		assert.Equal(int32(1), runsA.Load(), "A ran again instead of being replayed")
		assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", flowID))
		// The stalled worker's completion, unfenced, may win and complete the flow while A sits rewound: A is then
		// pending on a closed flow until a claim reaps it to the flow's status (after this replica's 1-2s claim
		// reservation on it lapses). Transient, and the outcome is single, so wait it out before the invariants.
		assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
			return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusCompleted+"'", flowID) == 1
		}), "A was never reaped to the flow's terminal status")
		enginetest.AssertInvariants(t, e)
	})
}

// TestBumpCohortArrivals_Fence pins the two-row cohort bump directly, on every dialect: a stale lease generation is
// fenced (errLeaseFenced) and its transaction's bump rolled back, a current one reads the spawn's counters as
// updated and clears the arriving step's flag, and a source arriving at its own fan-in (spawn == arriving step) is
// served by the one row. The MySQL arm is the one with a separate read, which is what this pins there.
func TestBumpCohortArrivals_Fence(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Bump")
	g.SetURL("A", "bump/a")
	g.SetURL("B1", "bump/b1")
	g.SetURL("B2", "bump/b2")
	g.SetURL("J", "bump/j")
	g.AddTransitionFanOut("A", "B1", "B2")
	g.AddTransition("B1", "J")
	g.AddTransition("B2", "J")
	g.SetFanIn("J")
	g.AddTransition("J", workflow.End)
	mux.HandleGraph("bump/g", g)
	release := make(chan struct{})
	var started atomic.Int32
	branch := func(ctx context.Context, f *workflow.Flow) error { started.Add(1); <-release; return nil }
	mux.HandleTask("bump/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("bump/b1", branch)
	mux.HandleTask("bump/b2", branch)
	mux.HandleTask("bump/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	if !assert.NoError(e.Startup(t.Context())) {
		return
	}

	fk, err := e.Create(ctx, "bump/g", nil, nil)
	assert.NoError(err)
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return started.Load() == 2 }), "branches never started")
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	sh, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	db := sh.DB()
	var spawnID, b1ID, b1Seq int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&spawnID))
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id, lease_seq FROM dwarf_steps WHERE flow_id=? AND task_name='B1'", flowID).Scan(&b1ID, &b1Seq))
	// Flag B1 as a completion write would, so the clear is observable.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET advancing=1 WHERE step_id=?", b1ID)
	assert.NoError(err)

	errRollback := errors.New("roll back")
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		_, _, _, _, _, berr := bumpCohortArrivals(ctx, tx, spawnID, 1, b1ID, b1Seq-1)
		return berr
	})
	assert.True(errors.Is(err, errLeaseFenced), "a stale generation was not fenced: %v", err)

	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		arrivals, size, failures, _, _, berr := bumpCohortArrivals(ctx, tx, spawnID, 1, b1ID, b1Seq)
		if berr != nil {
			return berr
		}
		assert.Equal(1, arrivals)
		assert.Equal(2, size)
		assert.Equal(0, failures)
		var advancing int
		assert.NoError(tx.QueryRowContext(ctx, "SELECT advancing FROM dwarf_steps WHERE step_id=?", b1ID).Scan(&advancing))
		assert.Equal(0, advancing, "the arriving step's flag was not cleared")
		// The source's own row serves as spawn and arriving step at once.
		arrivals, _, _, _, _, berr = bumpCohortArrivals(ctx, tx, spawnID, 1, spawnID, 0)
		if berr != nil {
			return berr
		}
		assert.Equal(2, arrivals)
		return errRollback
	})
	assert.True(errors.Is(err, errRollback), "unexpected error: %v", err)

	// Both transactions rolled back: the cohort is untouched and B1 still carries the flag it was given.
	assert.Equal(1, enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE step_id=? AND cohort_arrivals=0", spawnID))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET advancing=0 WHERE step_id=?", b1ID)
	assert.NoError(err)

	close(release)
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	enginetest.AssertInvariants(t, e)
}

// TestOrphanSweep_RecoversLostTransitionBesideLiveSibling pins that a lost transition is repaired at step
// granularity: a fan-out branch whose worker died between its completion write and its transition is rewound once
// its own lease lapses, while a sibling branch is still live - waiting out a 24h Retry backoff, or interrupted for
// a human, which also leaves the flow `interrupted`. Gated on the whole flow being terminal, the sweep would leave A
// stranded for the sibling's whole lifetime.
//
// A's death is simulated as in TestOrphanSweep_RecoversLostTransition: the test writes the completion the dying
// worker's completion write would, under a bumped lease generation, already lapsed. Dispatch is driven by hand (no
// workers) so each step runs exactly where the scenario needs it.
func TestOrphanSweep_RecoversLostTransitionBesideLiveSibling(t *testing.T) {
	t.Parallel()
	for _, sibling := range []string{"retrying", "interrupted"} {
		t.Run(sibling, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := context.Background()

			mux := hosts.NewMux()
			g := workflow.NewGraph("LiveSibling")
			g.SetURL("Start", "livesibling/start")
			g.SetURL("A", "livesibling/a")
			g.SetURL("B", "livesibling/b")
			g.SetURL("Join", "livesibling/join")
			g.AddTransition("Start", "A")
			g.AddTransition("Start", "B")
			g.AddTransition("A", "Join")
			g.AddTransition("B", "Join")
			g.SetFanIn("Join")
			g.AddTransition("Join", workflow.End)
			assert.NoError(g.Validate())
			mux.HandleGraph("livesibling/g", g)
			var runsA atomic.Int32
			mux.HandleTask("livesibling/start", func(ctx context.Context, f *workflow.Flow) error { return nil })
			mux.HandleTask("livesibling/a", func(ctx context.Context, f *workflow.Flow) error { runsA.Add(1); return f.Set("a", 1) })
			mux.HandleTask("livesibling/b", func(ctx context.Context, f *workflow.Flow) error {
				if sibling == "interrupted" {
					if yield, err := f.Interrupt(nil, nil); yield || err != nil {
						return err
					}
				} else if f.Attempt() == 0 && f.Retry(24*time.Hour, 1, 24*time.Hour, 0) {
					return nil
				}
				return f.Set("b", 1)
			})
			mux.HandleTask("livesibling/join", func(ctx context.Context, f *workflow.Flow) error { return nil })

			e := NewEngineUnderTest(t.Name())
			defer e.Shutdown(ctx)
			e.SetHost(mux)
			assert.NoError(e.SetWorkers(0))
			assert.Must(assert.NoError(e.Startup(t.Context())))
			awaitStartupRecoverySweep(t, e)

			fk, err := e.Create(ctx, "livesibling/g", nil, nil)
			assert.Must(assert.NoError(err))
			shard, _, flowID, _, err := keys.ParseFlowKey(fk)
			assert.Must(assert.NoError(err))
			sh, err := e.db.Shard(shard)
			assert.Must(assert.NoError(err))
			db := sh.DB()
			stepOf := func(task string) int {
				var id int
				assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name=?", flowID, task).Scan(&id))
				return id
			}
			noTurn := func() {}

			assert.NoError(e.processStep(ctx, shard, stepOf("Start"), noTurn))
			aID, bID := stepOf("A"), stepOf("B")
			_, err = db.ExecContext(ctx,
				"UPDATE dwarf_steps SET status='"+workflow.StatusCompleted+"', changes=?, advancing=1, lease_seq=lease_seq+1,"+
					" lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000), updated_at=NOW_UTC() WHERE step_id=?",
				[]byte(`{"a":1}`), aID)
			assert.NoError(err)
			var seqBefore int
			assert.NoError(db.QueryRowContext(ctx, "SELECT lease_seq FROM dwarf_steps WHERE step_id=?", aID).Scan(&seqBefore))
			assert.NoError(e.processStep(ctx, shard, bID, noTurn))
			wantFlow := workflow.StatusRunning
			if sibling == "interrupted" {
				wantFlow = workflow.StatusInterrupted
			}
			var flowStatus string
			assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&flowStatus))
			assert.Equal(wantFlow, flowStatus)

			// A LIGHT pass, the one that runs every tick, takes A back under a new generation while B is live.
			e.recoverOrphanedFlows(ctx, db, shard, false)
			var aStatus string
			var seqAfter int
			assert.NoError(db.QueryRowContext(ctx, "SELECT status, lease_seq FROM dwarf_steps WHERE step_id=?", aID).Scan(&aStatus, &seqAfter))
			assert.Equal(workflow.StatusPending, aStatus, "the sweep left A stranded beside a live sibling")
			assert.True(seqAfter > seqBefore, "the sweep did not rewind A under a new generation")

			// A is replayed and arrives at Join; B then finishes and the flow completes with both contributions.
			assert.NoError(e.processStep(ctx, shard, aID, noTurn))
			if sibling == "interrupted" {
				assert.NoError(e.Resume(ctx, fk, nil))
			} else {
				_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=NOW_UTC() WHERE step_id=?", bID)
				assert.NoError(err)
			}
			assert.NoError(e.processStep(ctx, shard, bID, noTurn))
			assert.NoError(e.processStep(ctx, shard, stepOf("Join"), noTurn))
			out, err := e.Snapshot(ctx, fk)
			if assert.NoError(err) {
				assert.Equal(workflow.StatusCompleted, out.Status)
				assert.Equal(1, out.State.GetInt("a"), "the replay carried A's recorded output forward")
				assert.Equal(1, out.State.GetInt("b"))
			}
			assert.Equal(int32(0), runsA.Load(), "A was replayed, not run")
			enginetest.AssertInvariants(t, e)
		})
	}
}
