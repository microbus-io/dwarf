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
	"encoding/json"
	"testing"

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestInterruptResume_DeepChain pins that an interrupt and its Resume land in a subgraph chain deeper than one
// statement's bind list can name. Nothing bounds subgraph nesting, and a statement naming every flow or step of the
// chain at once bound 3 per flow at the interrupt and 1 per step at Resume, past SQL Server's 2,100-parameter
// ceiling at 700 flows and 2,100 steps; the interrupt then rolled back whole, and a retry could not shrink it. At
// 2,101 flows every chain-wide statement of that form fails on SQL Server, and on every dialect each list spans
// several chunks, so the pairing of each flow with its stopped step is checked across chunk boundaries.
//
// The chain is built in the database directly - one running flow per level, each parked on its child, the leaf
// running under its lease - as spawning 2,101 subgraphs through the API would take minutes. The interrupt is the
// leaf dispatch's own write (handleInterrupt); the Resume is the public one.
func TestInterruptResume_DeepChain(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(noopHost{})
	assert.NoError(e.SetWorkers(0))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitStartupRecoverySweep(t, e)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))

	g := workflow.NewGraph("Recursive")
	g.SetURL("Call", "deepchain/call")
	g.AddTransition("Call", workflow.End)
	assert.Must(assert.NoError(g.Validate()))
	graphJSON, err := json.Marshal(g)
	assert.Must(assert.NoError(err))

	const depth, token = 2101, "0123456789abcdef"
	flowIDs := make([]int, 0, depth) // root first
	stepIDs := make([]int, 0, depth) // the step each flow stops at: its caller of the next, the leaf last
	var hour int
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		flowIDs, stepIDs = flowIDs[:0], stepIDs[:0]
		parentFlow, parentStep, root := 0, 0, 0
		for i := range depth {
			id, err := tx.InsertReturnID(ctx, "flow_id",
				"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, surgraph_flow_id, surgraph_step_id, root_flow_id) VALUES (?,?,?,?,?,?,?,?,?)",
				token, "deepchain/g", "Recursive", graphJSON, "", workflow.StatusRunning, parentFlow, parentStep, root)
			if err != nil {
				return err
			}
			flowID := int(id)
			if root == 0 {
				root = flowID
				if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET root_flow_id=? WHERE flow_id=?", root, root); err != nil {
					return err
				}
			}
			if parentStep != 0 {
				if _, err := tx.ExecContext(ctx, "UPDATE dwarf_steps SET parked=?, parked_child_flow_id=? WHERE step_id=?", parkedSubgraph, flowID, parentStep); err != nil {
					return err
				}
			}
			sid, err := tx.InsertReturnID(ctx, "step_id",
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, lease_seq, time_budget_ms, lease_expires, flow_created_hour)"+
					" VALUES (?,?,?,?,?,?,?,60000,DATE_ADD_MILLIS(NOW_UTC(), 60000),(SELECT created_hour FROM dwarf_flows WHERE flow_id=?))",
				flowID, i+1, token, "Call", "deepchain/call", workflow.StatusRunning, 1, flowID)
			if err != nil {
				return err
			}
			flowIDs, stepIDs = append(flowIDs, flowID), append(stepIDs, int(sid))
			parentFlow, parentStep = flowID, int(sid)
		}
		return tx.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", root).Scan(&hour)
	})
	assert.Must(assert.NoError(err))
	rootID, leafFlow, leafStep := flowIDs[0], flowIDs[depth-1], stepIDs[depth-1]
	count := func(query string, args ...any) int { return enginetest.CountRows(t, e, 1, query, args...) }

	var leafHour int
	assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", leafFlow).Scan(&leafHour)))
	d := &dispatch{shard: 1, db: db, stepID: leafStep, changesJSON: []byte(`{"asked":true}`),
		row: claimRow{leaseSeq: 1, flowID: leafFlow, createdHour: leafHour, flowToken: token, workflowURL: "deepchain/g"}}
	payload, err := workflow.NewState(map[string]any{"question": "go on?"})
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(e.handleInterrupt(ctx, d, payload)))

	assert.Equal(depth, count("SELECT COUNT(*) FROM dwarf_flows WHERE status='"+workflow.StatusInterrupted+"'"))
	assert.Equal(depth, count("SELECT COUNT(*) FROM dwarf_steps WHERE status='"+workflow.StatusInterrupted+"' AND parked=?", parkedNone))
	assert.Equal(1, count("SELECT COUNT(*) FROM dwarf_steps WHERE step_id=? AND interrupt_done=1", leafStep))
	// The payload's stored bytes are compared decoded: a column written from a Go []byte and a text literal compare
	// unequal on SQLite whatever they hold.
	withPayload := func() int {
		rows, err := db.QueryContext(ctx, "SELECT interrupt_payload FROM dwarf_steps")
		assert.Must(assert.NoError(err))
		defer rows.Close()
		n := 0
		for rows.Next() {
			var raw []byte
			assert.Must(assert.NoError(rows.Scan(&raw)))
			var v map[string]any
			assert.NoError(json.Unmarshal(raw, &v))
			if len(v) > 0 {
				n++
			}
		}
		assert.NoError(rows.Err())
		return n
	}
	assert.Equal(depth, withPayload(), "every chain step carries the payload")
	stoppedRight := 0
	for i, flowID := range flowIDs {
		stoppedRight += count("SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND stopped_step_id=?", flowID, stepIDs[i])
	}
	assert.Equal(depth, stoppedRight, "every chain flow records the step it stopped at")

	assert.Must(assert.NoError(e.Resume(ctx, keys.NewFlowKey(1, hour, rootID, token), map[string]any{"answer": "yes"})))

	assert.Equal(depth, count("SELECT COUNT(*) FROM dwarf_flows WHERE status='"+workflow.StatusRunning+"' AND stopped_step_id=0"))
	assert.Equal(1, count("SELECT COUNT(*) FROM dwarf_steps WHERE step_id=? AND status='"+workflow.StatusPending+"'", leafStep))
	assert.Equal(depth-1, count("SELECT COUNT(*) FROM dwarf_steps WHERE status='"+workflow.StatusRunning+"' AND parked=?", parkedSubgraph))
	assert.Zero(count("SELECT COUNT(*) FROM dwarf_steps WHERE status='" + workflow.StatusInterrupted + "'"))
	assert.Zero(withPayload(), "Resume clears the payload on every chain step")
}
