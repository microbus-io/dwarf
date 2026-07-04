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
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestHandleInterrupt_SkipsNotifyWhenCancelWon pins that handleInterrupt fires the interrupted FlowStopped
// callback only when the flow actually transitioned to interrupted. The interrupt UPDATEs are guarded to
// running/interrupted, so a racing Cancel can win and leave the flow cancelled; before the fix the callback
// fired unconditionally, delivering a spurious interrupted notification alongside Cancel's own cancelled one.
func TestHandleInterrupt_SkipsNotifyWhenCancelWon(t *testing.T) {
	ctx := context.Background()

	insert := func(t *testing.T, e *Engine, flowStatus, stepStatus string) {
		db, err := e.db.Shard(1)
		testarossa.For(t).NoError(err)
		// Do not supply flow_id: it is an IDENTITY/auto-increment column, and an explicit value is
		// rejected on SQL Server (IDENTITY_INSERT OFF). On the fresh per-test DB the first insert is
		// flow_id=1 on every driver - the id the handleInterrupt call below and root_flow_id rely on.
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, status, root_flow_id, thread_id, notify_on_stop, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			"ftok", "u", "W", "{}", flowStatus, 1, 1, 1, 1000,
		)
		testarossa.For(t).NoError(err)
		_, err = db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms) VALUES (?, ?, ?, ?, ?, ?, ?)",
			1, 1, "stok", "T", "u", stepStatus, 1000,
		)
		testarossa.For(t).NoError(err)
	}

	t.Run("cancel_won_no_notify", func(t *testing.T) {
		assert := testarossa.For(t)
		proxy := NewTestProxy()
		var got *workflow.FlowOutcome
		proxy.OnFlowStopped(func(ctx context.Context, flowKey string, outcome *workflow.FlowOutcome) { got = outcome })
		e := NewEngine()
		e.SetHost(proxy)
		e.RunInTest(t)

		// The flow is already cancelled (a Cancel won the race); the interrupt UPDATEs match nothing.
		insert(t, e, workflow.StatusCancelled, workflow.StatusCancelled)

		db, err := e.db.Shard(1)
		assert.NoError(err)
		err = e.handleInterrupt(ctx, 1, db, 1, 1, "ftok", []byte("{}"), map[string]any{"k": "v"})
		assert.NoError(err)
		assert.Nil(got) // no spurious interrupted callback
	})

	t.Run("interrupt_won_notify", func(t *testing.T) {
		assert := testarossa.For(t)
		proxy := NewTestProxy()
		var got *workflow.FlowOutcome
		proxy.OnFlowStopped(func(ctx context.Context, flowKey string, outcome *workflow.FlowOutcome) { got = outcome })
		e := NewEngine()
		e.SetHost(proxy)
		e.RunInTest(t)

		// The flow is running; the interrupt transitions it to interrupted and must notify.
		insert(t, e, workflow.StatusRunning, workflow.StatusRunning)

		db, err := e.db.Shard(1)
		assert.NoError(err)
		err = e.handleInterrupt(ctx, 1, db, 1, 1, "ftok", []byte("{}"), map[string]any{"k": "v"})
		assert.NoError(err)
		if assert.NotNil(got) {
			assert.Equal(workflow.StatusInterrupted, got.Status)
		}
	})
}
