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

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestFault_InterruptStaleWriteRollback pins the interrupt lease-fence — the ONLY fence in the engine that
// rolls its whole transaction back (via errLeaseFenced) instead of no-op'ing on a zero-row match. A subgraph
// child's interrupt re-parks the ancestor caller (running+parkedSubgraph → interrupted) in the SAME combined
// UPDATE that interrupts the leaf; if a zombie holding a stale lease_seq were let through, it would flip the
// caller out of parkedSubgraph and strand the parent revive — so the fence must undo the re-park, which only a
// full rollback can do.
//
// FaultInterruptStaleWrite forces the in-tx leaf lease_seq to look re-granted on the FIRST interrupt attempt
// (exactly as a real peer re-claim would). The transaction rolls back, the worker abandons, and the child step
// stays running-and-leased; once its short lease lapses the background poll re-dispatches it and the SECOND
// attempt (fault consumed) drives the real interrupt up the whole chain with no strand. A root Resume then
// threads back down to the leaf and the tree completes — proving the rollback left the ancestor park intact.
func TestFault_InterruptStaleWriteRollback(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	proxy := NewTestProxy()

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Call", "fisw/call")
	parent.AddTransition("Call", workflow.END)
	proxy.HandleGraph("fisw/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetEndpoint("X", "fisw/x")
	child.AddTransition("X", workflow.END)
	proxy.HandleGraph("fisw/child", child)

	proxy.HandleTask("fisw/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("fisw/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	// Atomic: the worker goroutine writes it while the test goroutine waits on it below.
	var xCalls atomic.Int32
	proxy.HandleTask("fisw/x", func(ctx context.Context, f *workflow.Flow) error {
		xCalls.Add(1)
		yield, err := f.Interrupt(map[string]any{"ask": "approve?"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	e.SetTimeBudget(200 * time.Millisecond)
	e.leaseMargin = 100 * time.Millisecond // lease = budget+margin = 300ms, so lease recovery re-dispatches fast
	assert.NoError(e.Startup(t.Context()))

	e.seams.Inject(FaultInterruptStaleWrite)
	fk, err := e.Create(ctx, "fisw/parent", nil, nil)
	assert.NoError(err)

	// The fenced first attempt rolls back and returns nil (abandon quietly), leaving the child step
	// running-and-leased — it does not schedule its own recovery, so drive the lease-recovery backstop until
	// the re-dispatch lands (see driveLeaseRecovery; this is the site whose fixed-sleep version flaked).
	driveLeaseRecovery(t, e, leaseRecoveryWait, func() bool { return xCalls.Load() >= 2 })

	// The flow reaches interrupted only via that recovered second attempt — the fenced first attempt rolled
	// back, so the interrupt could not take on the first try.
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusInterrupted, 10*time.Second)
	assert.Equal(int32(2), xCalls.Load()) // fenced attempt + recovered real attempt: proof the fence forced a re-dispatch
	enginetest.AssertInvariants(t, e)     // no strand: the rollback undid the ancestor re-park, so no terminal-flow-with-live-step

	// The rollback left a clean, resumable tree: Resume threads down to the leaf and the whole tree completes.
	err = e.Resume(ctx, fk, map[string]any{"answer": "yes"})
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, 10*time.Second)
	enginetest.AssertInvariants(t, e)
}

// TestInterruptFence_LeafResetToPendingDoesNotCommitChainInterrupt pins the leaf-transition fence. handleInterrupt writes the
// leaf's interrupt AND re-parks the whole surgraph chain (ancestor callers running+parkedSubgraph -> interrupted,
// chain flows -> interrupted) in one combined UPDATE guarded on status IN ('running','interrupted'). The only
// post-UPDATE gate USED to be a lease_seq compare. But lease recovery resets an expired leaf running->pending
// WITHOUT bumping lease_seq, so for a zombie whose leaf recovery already reclaimed, the generation still matched
// while the combined UPDATE matched the leaf ZERO rows - and the transaction committed only the OTHER rows: the
// ancestor callers were flipped OUT of parkedSubgraph (stranding the parent's revive, since
// deliverFlowFailureToParent needs status='running' AND parked=parkedSubgraph) and the chain flows were marked
// interrupted, all while the leaf sat pending with no interrupt_done.
//
// The fence now also requires the leaf to have actually transitioned to `interrupted`. The scenario is built
// directly: a parent caller step parked on its subgraph child, whose leaf recovery has reset to `pending` under
// the same generation. handleInterrupt must fence (roll back) and touch nothing.
func TestInterruptFence_LeafResetToPendingDoesNotCommitChainInterrupt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// setup builds a two-flow surgraph tree on shard 1:
	//   flow 1 (root/parent), step 1 = the caller, running+parkedSubgraph (awaiting its child)
	//   flow 2 (child),       step 2 = the leaf, in `leafStatus` under generation 7
	// not_before/lease_expires are far future so the engine's own poll/recovery leave the rows alone.
	setup := func(t *testing.T, leafStatus string) (*Engine, *sequel.DB) {
		assert := testarossa.For(t)
		e := NewEngineUnderTest(t.Name())
		e.SetHost(NewTestProxy())
		assert.NoError(e.Startup(t.Context()))
		db, err := e.db.Shard(1)
		assert.NoError(err)

		// Parent (root) flow and its caller step, parked on the subgraph.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, surgraph_flow_id, surgraph_step_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			"ptok", "u", "P", []byte("{}"), workflow.StatusRunning, 1, 1, 0, 0, 1000,
		)
		assert.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, parked, time_budget_ms, lease_seq, not_before, lease_expires)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000), DATE_ADD_MILLIS(NOW_UTC(), 999000))",
			1, 1, "pstok", "Call", "u", workflow.StatusRunning, parkedSubgraph, 1000, 1,
		)
		assert.NoError(err)

		// Child flow (points back at the caller) and its leaf step in the requested status.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, surgraph_flow_id, surgraph_step_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			"ctok", "u", "C", []byte("{}"), workflow.StatusRunning, 1, 2, 1, 1, 1000,
		)
		assert.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, parked, time_budget_ms, lease_seq, not_before, lease_expires)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000), DATE_ADD_MILLIS(NOW_UTC(), 999000))",
			2, 2, "cstok", "X", "u", leafStatus, parkedNone, 1000, 7,
		)
		assert.NoError(err)
		return e, db
	}

	stepState := func(t *testing.T, db *sequel.DB, stepID int) (status string, parked int) {
		assert := testarossa.For(t)
		assert.NoError(db.QueryRowContext(ctx, "SELECT status, parked FROM dwarf_steps WHERE step_id=?", stepID).Scan(&status, &parked))
		return status, parked
	}
	flowStatus := func(t *testing.T, db *sequel.DB, flowID int) string {
		assert := testarossa.For(t)
		var s string
		assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&s))
		return s
	}

	// The leaf recovery reset to `pending` is no longer ours: the combined UPDATE matches it zero rows, so the
	// fence must roll the whole interrupt back - the ancestor caller stays parkedSubgraph and no flow is interrupted.
	t.Run("pending_leaf_fences_the_chain_interrupt", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusPending)
		defer e.Shutdown(ctx)

		err := e.handleInterrupt(ctx, 1, db, 2, 7, 2, "ctok", "u", []byte(`{"answer":"x"}`), mustState(map[string]any{"ask": "approve?"}))
		assert.NoError(err) // fenced -> abandon quietly

		leafStatus, _ := stepState(t, db, 2)
		assert.Equal(workflow.StatusPending, leafStatus, "the pending leaf must be left exactly as recovery left it")
		callerStatus, callerParked := stepState(t, db, 1)
		assert.Equal(workflow.StatusRunning, callerStatus, "the ancestor caller must NOT be interrupted")
		assert.Equal(parkedSubgraph, callerParked, "the ancestor caller must stay parkedSubgraph so its revive can still land")
		assert.Equal(workflow.StatusRunning, flowStatus(t, db, 1), "the parent flow must not be interrupted")
		assert.Equal(workflow.StatusRunning, flowStatus(t, db, 2), "the child flow must not be interrupted")
	})

	// A FAILED chain UPDATE is a transaction failure, not a fence. The two look identical through the fence
	// read - both leave the leaf `running` - but they demand opposite handling: a fence means a peer owns the
	// step (abandon quietly, return nil), while a failed statement means nothing was written and the interrupt
	// must be RETRIED. Reading them as the same thing wedged parallel subgraph interrupts on SQL Server: two
	// callers interrupting at once deadlock (flipping running->interrupted deletes keys from the three
	// status-filtered dwarf_steps indexes, so disjoint row sets still cycle), the victim's UPDATE applies
	// nothing, and the fence read returned the pre-UPDATE row - so handleInterrupt returned nil, abandoning a
	// leaf it still owned. Its flow sat `running` with no interrupt until the lease lapsed (budget + margin),
	// and a Resume landing in that window flipped the parent back to `running` for good, since the lost branch
	// never marked its caller step interrupted.
	//
	// This pins the contract rather than any one layer's implementation of it: a failed chain write must leave
	// handleInterrupt as a retryable error, never as a fence. Today the engine enforces it by checking the
	// transaction's health before trusting the fence read; if sequel ever makes QueryRow short-circuit like
	// its other statement methods, that check becomes redundant and this test keeps guarding the outcome.
	t.Run("failed_chain_write_is_retryable_not_fenced", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusRunning)
		defer e.Shutdown(ctx)
		e.seams.Inject(FaultInterruptChainWrite)
		defer e.seams.Withdraw(FaultInterruptChainWrite)

		err := e.handleInterrupt(ctx, 1, db, 2, 7, 2, "ctok", "u", []byte(`{"answer":"x"}`), mustState(map[string]any{"ask": "approve?"}))
		assert.Error(err, "a failed chain UPDATE must surface as an error so Transact retries it, NOT be swallowed as a fence")

		// Nothing committed: the leaf is still ours to re-interrupt on the retry.
		leafStatus, _ := stepState(t, db, 2)
		assert.Equal(workflow.StatusRunning, leafStatus, "the leaf we still own must be left running, not abandoned interrupted-less")
		callerStatus, callerParked := stepState(t, db, 1)
		assert.Equal(workflow.StatusRunning, callerStatus)
		assert.Equal(parkedSubgraph, callerParked, "the ancestor caller must stay parkedSubgraph")
		assert.Equal(workflow.StatusRunning, flowStatus(t, db, 1), "the parent flow must not be interrupted")
		assert.Equal(workflow.StatusRunning, flowStatus(t, db, 2), "the child flow must not be interrupted")
	})

	// Control: a `running` leaf we still own (same generation) interrupts the whole chain normally - the fix must
	// not over-fence the legitimate case.
	t.Run("running_leaf_interrupts_the_chain", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusRunning)
		defer e.Shutdown(ctx)

		err := e.handleInterrupt(ctx, 1, db, 2, 7, 2, "ctok", "u", []byte(`{"answer":"x"}`), mustState(map[string]any{"ask": "approve?"}))
		assert.NoError(err)

		leafStatus, _ := stepState(t, db, 2)
		assert.Equal(workflow.StatusInterrupted, leafStatus, "the running leaf we own is interrupted")
		callerStatus, callerParked := stepState(t, db, 1)
		assert.Equal(workflow.StatusInterrupted, callerStatus, "the interrupt propagates up to the ancestor caller")
		assert.Equal(parkedNone, callerParked)
		assert.Equal(workflow.StatusInterrupted, flowStatus(t, db, 1))
		assert.Equal(workflow.StatusInterrupted, flowStatus(t, db, 2))
	})
}

// mustState builds a State from a literal map, for the white-box calls that take one.
func mustState(m map[string]any) workflow.State {
	s, err := workflow.NewState(m)
	if err != nil {
		panic(err)
	}
	return s
}
