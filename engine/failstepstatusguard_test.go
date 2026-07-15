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
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestFailStep_TerminalStatusGuard pins finding 9: failStep's fenced write now carries a status guard
// (status IN ('running','completed')), so it fails only a step a worker legitimately holds, never a terminal
// one. cancelSubtree terminalizes a step to `cancelled` WITHOUT bumping lease_seq, so the dispatching worker's
// generation still matches; without the status guard, failStep rewrote that cancelled step to `failed` -
// violating step immutability, miscounting dwarf_steps_executed, and (the one that bites) seeding a phantom
// branch failure that a later Fork re-derives from step status. The guard still admits the `completed` case
// failOnPersistError relies on (fail an already-completed step whose transition tx could not be persisted).
func TestFailStep_TerminalStatusGuard(t *testing.T) {
	ctx := context.Background()

	// setup inserts one flow (flow_id=1) and its single trunk step (step_id=1, lineage_id=0) in the given
	// statuses under lease generation 5, lease far future so the engine's own recovery poll leaves it alone.
	setup := func(t *testing.T, flowStatus, stepStatus string) (*Engine, *sequel.DB) {
		at := testarossa.For(t)
		e := NewEngine()
		e.SetHost(NewTestProxy())
		e.RunInTest(t)
		db, err := e.db.Shard(1)
		at.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
			"ftok", "u", "W", "{}", flowStatus, 1, 1, 1000,
		)
		at.NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, lineage_id, time_budget_ms, lease_seq, lease_expires) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), 999000))",
			1, 1, "stok", "T", "u", stepStatus, 0, 1000, 5,
		)
		at.NoError(err)
		return e, db
	}
	statuses := func(t *testing.T, db *sequel.DB) (flowStatus, stepStatus string) {
		at := testarossa.For(t)
		at.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=1").Scan(&flowStatus))
		at.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE step_id=1").Scan(&stepStatus))
		return strings.TrimSpace(flowStatus), strings.TrimSpace(stepStatus)
	}

	// A step a racing Cancel already terminalized (cancelled, same generation) must NOT be re-failed: the write
	// matches zero rows, failStep reports fenced, and both the step and its flow stay cancelled.
	t.Run("cancelled_step_is_not_refailed", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusCancelled, workflow.StatusCancelled)

		fenced, err := e.failStep(ctx, 1, 1, 5, 1, "ftok", errors.New("boom"), "T")
		assert.NoError(err)
		assert.True(fenced, "a cancelled step under our generation must fence, not be rewritten to failed")

		flowStatus, stepStatus := statuses(t, db)
		assert.Equal(workflow.StatusCancelled, stepStatus, "the cancelled step must stay cancelled (immutable)")
		assert.Equal(workflow.StatusCancelled, flowStatus, "the cancelled flow must stay cancelled")
	})

	// Control: failOnPersistError's escape hatch - a `completed` step whose transition could not be persisted is
	// still failed by the same call, so the guard must admit `completed`.
	t.Run("completed_step_can_still_fail", func(t *testing.T) {
		assert := testarossa.For(t)
		e, db := setup(t, workflow.StatusRunning, workflow.StatusCompleted)

		fenced, err := e.failStep(ctx, 1, 1, 5, 1, "ftok", errors.New("could not persist transition"), "T")
		assert.NoError(err)
		assert.False(fenced, "a completed step we own must still be failable (failOnPersistError's escape)")

		flowStatus, stepStatus := statuses(t, db)
		assert.Equal(workflow.StatusFailed, stepStatus)
		assert.Equal(workflow.StatusFailed, flowStatus)
	})
}
