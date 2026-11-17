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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// batchedCohort is a four-branch forEach whose branches each append their element to "got", with the test holding
// the cohort's stripe so all four arrivals queue in one batch behind it.
type batchedCohort struct {
	e        *Engine
	db       *sequel.DB
	shard    int
	flowKey  string
	stripe   *cohortStripe
	spawnID  int
	branches atomic.Int32
	joins    atomic.Int32
}

const batchedWidth = 4

// newBatchedCohort starts the flow and returns with all its arrivals batched behind the test's hold on the stripe.
// A budget above zero is the flow's time budget, with the lease margin shortened to 200ms, so a waiter's first lease
// tick comes about the budget after its claim.
func newBatchedCohort(t *testing.T, budget time.Duration) *batchedCohort {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()
	c := &batchedCohort{}

	mux := hosts.NewMux()
	g := workflow.NewGraph("Batch")
	g.SetURL("Seed", "batch/seed")
	g.SetURL("Branch", "batch/branch")
	g.SetURL("Join", "batch/join")
	g.SetFanIn("Join")
	g.SetReducer("got", workflow.ReducerAppend)
	g.AddTransitionForEach("Seed", "Branch", "items", "item")
	g.AddTransitionChain("Branch", "Join", workflow.End)
	mux.HandleGraph("batch/g", g)
	release := make(chan struct{})
	var started atomic.Int32
	mux.HandleTask("batch/seed", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("items", []int{0, 1, 2, 3})
		return nil
	})
	mux.HandleTask("batch/branch", func(ctx context.Context, f *workflow.Flow) error {
		started.Add(1)
		<-release
		c.branches.Add(1)
		f.Set("got", []int{f.GetInt("item")})
		return nil
	})
	mux.HandleTask("batch/join", func(ctx context.Context, f *workflow.Flow) error { c.joins.Add(1); return nil })

	c.e = NewEngineUnderTest(t.Name())
	t.Cleanup(func() { c.e.Shutdown(ctx) })
	c.e.SetHost(mux)
	var opts *workflow.FlowOptions
	if budget > 0 {
		c.e.leaseMargin = 200 * time.Millisecond
		opts = &workflow.FlowOptions{TimeBudget: budget}
	}
	assert.Must(assert.NoError(c.e.Startup(t.Context())))

	var err error
	c.flowKey, err = c.e.Create(ctx, "batch/g", nil, opts)
	assert.Must(assert.NoError(err))
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return started.Load() == batchedWidth }), "branches never started")
	var flowID int
	c.shard, _, flowID, _, err = keys.ParseFlowKey(c.flowKey)
	assert.Must(assert.NoError(err))
	sh, err := c.e.db.Shard(c.shard)
	assert.Must(assert.NoError(err))
	c.db = sh.DB()
	assert.NoError(c.db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='Seed'", flowID).Scan(&c.spawnID))

	c.stripe = &c.e.cohortLocks[cohortLockStripe(c.shard, c.spawnID)]
	assert.True(c.stripe.lock(time.Hour, nil))
	close(release)
	waitQueued(t, c.stripe, 1) // the leader
	waitBatched(t, c.stripe, c.shard, c.spawnID, batchedWidth)
	return c
}

// batch is the open batch the arrivals queued in.
func (c *batchedCohort) batch() *arrivalBatch {
	c.stripe.mu.Lock()
	defer c.stripe.mu.Unlock()
	return c.stripe.batches[arrivalKey{shard: c.shard, spawn: c.spawnID}]
}

// await waits for the flow and checks it fanned in once, on every element folded once and in order, with no branch
// task run twice.
func (c *batchedCohort) await(t *testing.T) {
	t.Helper()
	assert := testarossa.For(t)
	awaitCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := c.e.Await(awaitCtx, c.flowKey)
	if assert.NoError(err) && assert.Equal(workflow.StatusCompleted, out.Status) {
		var got []int
		_, err := out.State.Get("got", &got)
		assert.NoError(err)
		assert.Equal([]int{0, 1, 2, 3}, got, "every arrival folds into the fan-in once")
	}
	assert.Equal(int32(batchedWidth), c.branches.Load(), "a branch task ran twice")
	assert.Equal(int32(1), c.joins.Load())
	var arrivals, size int
	assert.NoError(c.db.QueryRowContext(context.Background(), "SELECT cohort_arrivals, cohort_size FROM dwarf_steps WHERE step_id=?", c.spawnID).Scan(&arrivals, &size))
	assert.Equal(size, arrivals, "the cohort counted an arrival twice or lost one")
	enginetest.AssertInvariants(t, c.e)
}

// TestCohortBatch_QueuedArrivalsLandTogether pins that the pure arrivals of one cohort queued behind its stripe land
// through their batch - every member's outcome is the batch's, none ran its own transaction - and that the fan-in still
// sees every one of them once.
func TestCohortBatch_QueuedArrivalsLandTogether(t *testing.T) {
	c := newBatchedCohort(t, 0)
	b := c.batch()
	c.stripe.unlock()
	c.await(t)
	<-b.done
	testarossa.For(t).Equal([]arrivalOutcome{arrivalLanded, arrivalLanded, arrivalLanded, arrivalLanded}, b.outcomes)
}

// TestCohortBatch_FencedMemberIsLeftOut pins the fence inside a batch: a member whose step was taken back while it
// waited - here the leader itself, whose generation the test bumps as a recovery rewind would - is written nothing and
// counted nothing, while the batch lands the others. The step is then replayed by the orphan repair and arrives on its
// own, so the cohort resolves once with every arrival counted once and no task run twice.
func TestCohortBatch_FencedMemberIsLeftOut(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	c := newBatchedCohort(t, 0)
	leader := c.batch().members[0].stepID
	_, err := c.db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_seq=lease_seq+1, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE step_id=?", leader)
	assert.NoError(err)
	c.stripe.unlock()

	landed := "SELECT COUNT(*) FROM dwarf_steps WHERE lineage_id=? AND task_name='Branch' AND advancing=0"
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return enginetest.CountRows(t, c.e, c.shard, landed, c.spawnID) == batchedWidth-1 }),
		"the batch did not land the members it still owned")
	var arrivals, advancing int
	assert.NoError(c.db.QueryRowContext(ctx, "SELECT cohort_arrivals FROM dwarf_steps WHERE step_id=?", c.spawnID).Scan(&arrivals))
	assert.Equal(batchedWidth-1, arrivals, "a fenced member's arrival was counted")
	assert.NoError(c.db.QueryRowContext(ctx, "SELECT advancing FROM dwarf_steps WHERE step_id=?", leader).Scan(&advancing))
	assert.Equal(1, advancing, "a fenced member's row was written")

	c.e.recoverOrphanedFlows(ctx, c.db, c.shard)
	c.await(t)
}

// TestCohortBatch_FiredFaultLeavesTheBatch pins that the transition's test seams keep their meaning for a batched
// arrival: a fault that fires for one member is consulted once, as that member's own transaction would, and the
// member leaves the batch and meets it in its own transaction - here a failed commit, which the dispatch's recovery
// rewinds and the step's next dispatch replays - while the batch lands the others.
func TestCohortBatch_FiredFaultLeavesTheBatch(t *testing.T) {
	c := newBatchedCohort(t, 0)
	b := c.batch()
	fault := diagnostics.Join(diagnostics.FaultTransitionCommit, "Branch")
	c.e.diags.Seams().Inject(fault)
	c.stripe.unlock()
	c.await(t)
	<-b.done
	assert := testarossa.For(t)
	assert.False(c.e.diags.Seams().IsFault(fault), "the fault never fired")
	solo := 0
	for _, o := range b.outcomes {
		if o == arrivalSolo {
			solo++
		}
	}
	assert.Equal(1, solo, "only the member whose fault fired leaves the batch")
}

// TestCohortBatch_LostCommitAcknowledgement pins a batch whose transaction committed and whose reply was lost. Every
// member is told the batch did not land and arrives on its own, and each must find its arrival already landed: every
// arrival counted once, the cohort converging once, and no task run twice.
func TestCohortBatch_LostCommitAcknowledgement(t *testing.T) {
	c := newBatchedCohort(t, 0)
	b := c.batch()
	members := make([]int, 0, batchedWidth)
	for _, m := range b.members {
		members = append(members, m.stepID)
	}
	l := armLostReply(t, c.e, diagnostics.FaultArrivalBatchCommitLost, "Branch")
	c.stripe.unlock()
	committed := l.committed()
	l.release()
	for _, stepID := range members {
		awaitDispatched(t, c.e, c.shard, stepID) // each member arrived alone after the batch
	}
	c.await(t)
	<-b.done
	assertSettledClean(t, c.e, nil, committed)
}

// TestCohortBatch_LeaderPanicReleasesTheBatch pins the leader's cleanup over its WAIT, not only its work: a lease tick
// that panics while the leader waits for the stripe must still seal the batch and wake its members, or they wait
// forever and later arrivals join a batch nobody leads. Every waiter's first tick panics here; the steps are then
// replayed by the orphan repair once their leases lapse, and the flow completes with nothing counted twice.
func TestCohortBatch_LeaderPanicReleasesTheBatch(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	c := newBatchedCohort(t, time.Second)
	b := c.batch()
	c.e.diags.Seams().InjectN(diagnostics.FaultPanicLeaseTick, batchedWidth)
	select {
	case <-b.done:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("a leader that panicked while waiting never released its batch")
	}
	assert.Nil(c.batch(), "the abandoned batch is still open to new arrivals")
	assert.Equal(make([]arrivalOutcome, batchedWidth), b.outcomes, "a member was landed by a leader that never led")
	c.stripe.unlock()

	// A woken member that finds the stripe free lands on its own; every one still flagged has panicked, and the
	// orphan repair replays it once its lease lapses.
	live := "SELECT COUNT(*) FROM dwarf_steps WHERE lineage_id=? AND task_name='Branch' AND advancing=1 AND lease_expires >= NOW_UTC()"
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return enginetest.CountRows(t, c.e, c.shard, live, c.spawnID) == 0 }),
		"the panicked arrivals' leases never lapsed")
	c.e.recoverOrphanedFlows(ctx, c.db, c.shard)
	c.await(t)
}

// TestCohortBatch_ALandedMemberTakesNoTurn pins the pass held after each batch outcome:
// only a landed member that lost its flow takes a turn back.
func TestCohortBatch_ALandedMemberTakesNoTurn(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		outcome arrivalOutcome
		lost    bool
		retakes bool
	}{
		{"landed", arrivalLanded, false, false},
		{"fenced", arrivalFenced, false, false},
		{"landed_into_a_lost_flow", arrivalLanded, true, true},
		{"handed_back", arrivalSolo, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			sh := database.NewShard(1, nil)
			sh.SetTurnLimit(1)
			e := &Engine{leaseMargin: time.Hour, persistLeaseExtension: time.Hour} // no lease tick during the test
			var stripe cohortStripe
			assert.True(stripe.lock(time.Hour, nil)) // held, so the arrival joins a batch
			b, _ := stripe.join(arrivalKey{shard: 1, spawn: 7}, &dispatch{shard: 1, cohortSpawnID: 7})
			ctx, pass := sh.WaitTurn(context.Background())
			d := &dispatch{shard: 1, cohortSpawnID: 7, pass: pass, leaseFrom: time.Now()}
			got := make(chan arrivalOutcome, 1)
			go func() {
				outcome, _ := e.arriveInBatch(ctx, &stripe, d)
				got <- outcome
			}()
			assert.True(enginetest.WaitUntil(t, 5*time.Second, func() bool { free, _ := sh.Turns(); return free == 1 }),
				"the member kept its turn while it waited")
			if tc.lost {
				d.loss.status = workflow.StatusFailed
			}
			b.outcomes = []arrivalOutcome{arrivalLanded, tc.outcome}
			close(b.done)
			assert.Equal(tc.outcome, <-got)
			free, _ := sh.Turns()
			if tc.retakes {
				assert.Equal(0, free, "the member did not take a turn back for the lost flow's tail")
			} else {
				assert.Equal(1, free, "the member took an unnecessary turn after its batch wait")
			}
			d.pass.Return()
		})
	}
}
