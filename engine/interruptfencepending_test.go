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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

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
	ctx := context.Background()

	// setup builds a two-flow surgraph tree on shard 1:
	//   flow 1 (root/parent), step 1 = the caller, running+parkedSubgraph (awaiting its child)
	//   flow 2 (child),       step 2 = the leaf, in `leafStatus` under generation 7
	// not_before/lease_expires are far future so the engine's own poll/recovery leave the rows alone.
	setup := func(t *testing.T, leafStatus string) (*Engine, *sequel.DB) {
		at := testarossa.For(t)
		e := NewEngine()
		e.SetHost(NewTestProxy())
		e.RunInTest(t)
		db, err := e.db.Shard(1)
		at.NoError(err)

		// Parent (root) flow and its caller step, parked on the subgraph.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, surgraph_flow_id, surgraph_step_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			"ptok", "u", "P", []byte("{}"), workflow.StatusRunning, 1, 1, 0, 0, 1000,
		)
		at.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, parked, time_budget_ms, lease_seq, not_before, lease_expires)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000), DATE_ADD_MILLIS(NOW_UTC(), 999000))",
			1, 1, "pstok", "Call", "u", workflow.StatusRunning, parkedSubgraph, 1000, 1,
		)
		at.NoError(err)

		// Child flow (points back at the caller) and its leaf step in the requested status.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, surgraph_flow_id, surgraph_step_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			"ctok", "u", "C", []byte("{}"), workflow.StatusRunning, 1, 2, 1, 1, 1000,
		)
		at.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, parked, time_budget_ms, lease_seq, not_before, lease_expires)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000), DATE_ADD_MILLIS(NOW_UTC(), 999000))",
			2, 2, "cstok", "X", "u", leafStatus, parkedNone, 1000, 7,
		)
		at.NoError(err)
		return e, db
	}

	stepState := func(t *testing.T, db *sequel.DB, stepID int) (status string, parked int) {
		testarossa.For(t).NoError(db.QueryRowContext(ctx, "SELECT status, parked FROM dwarf_steps WHERE step_id=?", stepID).Scan(&status, &parked))
		return strings.TrimSpace(status), parked
	}
	flowStatus := func(t *testing.T, db *sequel.DB, flowID int) string {
		var s string
		testarossa.For(t).NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&s))
		return strings.TrimSpace(s)
	}

	// The leaf recovery reset to `pending` is no longer ours: the combined UPDATE matches it zero rows, so the
	// fence must roll the whole interrupt back - the ancestor caller stays parkedSubgraph and no flow is interrupted.
	t.Run("pending_leaf_fences_the_chain_interrupt", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusPending)

		err := e.handleInterrupt(ctx, 1, db, 2, 7, 2, "ctok", "u", []byte(`{"answer":"x"}`), map[string]any{"ask": "approve?"})
		assert.NoError(err) // fenced -> abandon quietly

		leafStatus, _ := stepState(t, db, 2)
		assert.Equal(workflow.StatusPending, leafStatus, "the pending leaf must be left exactly as recovery left it")
		callerStatus, callerParked := stepState(t, db, 1)
		assert.Equal(workflow.StatusRunning, callerStatus, "the ancestor caller must NOT be interrupted")
		assert.Equal(parkedSubgraph, callerParked, "the ancestor caller must stay parkedSubgraph so its revive can still land")
		assert.Equal(workflow.StatusRunning, flowStatus(t, db, 1), "the parent flow must not be interrupted")
		assert.Equal(workflow.StatusRunning, flowStatus(t, db, 2), "the child flow must not be interrupted")
	})

	// Control: a `running` leaf we still own (same generation) interrupts the whole chain normally - the fix must
	// not over-fence the legitimate case.
	t.Run("running_leaf_interrupts_the_chain", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusRunning)

		err := e.handleInterrupt(ctx, 1, db, 2, 7, 2, "ctok", "u", []byte(`{"answer":"x"}`), map[string]any{"ask": "approve?"})
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
