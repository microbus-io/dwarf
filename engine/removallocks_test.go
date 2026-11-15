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
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestRemoval_ALargeTreeLeavesTheTablesUnlocked archives, and deletes, a closed tree larger than a batch, and holds
// the first transaction that deletes its live rows open after its deletes - the whole move, a staged tree's cleanup
// chunk or a deletion chunk, whichever the dialect takes - while a flow runs beside it. The flow's writes to
// dwarf_flows and dwarf_steps must not wait for the removal. On SQL Server the removal's locks are also read: a
// table-level lock there is an escalation of its row locks. An open flow sits between the tree's rows and the
// probe's, as live work is newer than a closed tree. SQLite is skipped: its one writer holds the whole database by
// design.
func TestRemoval_ALargeTreeLeavesTheTablesUnlocked(t *testing.T) {
	t.Parallel()
	for _, removal := range []string{"archive", "delete"} {
		t.Run(removal, func(t *testing.T) {
			t.Parallel()
			testRemovalLeavesTheTablesUnlocked(t, removal == "delete")
		})
	}
}

func testRemovalLeavesTheTablesUnlocked(t *testing.T, deleting bool) {
	assert := testarossa.For(t)
	ctx := t.Context()

	mux := hosts.NewMux()
	probe := workflow.NewGraph("Probe")
	probe.SetURL("Probe", "removallocks/probe")
	probe.AddTransition("Probe", workflow.End)
	assert.NoError(probe.Validate())
	mux.HandleGraph("removallocks/probe", probe)
	mux.HandleTask("removallocks/probe", func(ctx context.Context, f *workflow.Flow) error { return nil })
	spacer := workflow.NewGraph("Spacer")
	spacer.SetURL("Wait", "removallocks/wait")
	spacer.AddTransition("Wait", workflow.End)
	assert.NoError(spacer.Validate())
	mux.HandleGraph("removallocks/spacer", spacer)
	mux.HandleTask("removallocks/wait", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	assert.NoError(e.SetHost(mux))
	e.archiveGrace, e.sweepOverdue = 0, 0
	e.reapBatchRows = 4000 // the production batch, which the tree is larger than
	assert.Must(assert.NoError(e.Startup(ctx)))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	if db.DriverName() == "sqlite" {
		t.Skip("SQLite's one writer holds the whole database")
	}

	// One root with a child flow, doubled into 4,096 child flows, each with a step, and 5,120 root steps: a tree of
	// 4,097 flows and 9,216 steps, larger than a batch, whose steps removed at once pass the 5,000 row locks at which
	// SQL Server escalates.
	root := seedTrees(t, db, 1, treeShape{rootSteps: 10, childSteps: 1})[0]
	for range 12 {
		_, err := db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, surgraph_flow_id,"+
				" surgraph_step_id, root_flow_id) SELECT flow_token, workflow_url, workflow_name, graph, graph_hash, status,"+
				" surgraph_flow_id, surgraph_step_id, root_flow_id FROM dwarf_flows WHERE root_flow_id=? AND flow_id<>?", root, root)
		assert.Must(assert.NoError(err))
	}
	for range 9 {
		_, err := db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms,"+
				" flow_created_hour) SELECT flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms,"+
				" flow_created_hour FROM dwarf_steps WHERE flow_id=?", root)
		assert.Must(assert.NoError(err))
	}
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, flow_created_hour)"+
			" SELECT flow_id, 1, flow_token, 'T1', 'seed/t', '"+workflow.StatusCompleted+"', 60000, created_hour FROM dwarf_flows"+
			" WHERE root_flow_id=? AND flow_id<>? AND flow_id NOT IN (SELECT flow_id FROM dwarf_steps)", root, root)
	assert.Must(assert.NoError(err)) // every child flow holds a step, as a subgraph child does
	var flows, steps, hour int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id=?", root).Scan(&flows))
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id IN"+
		" (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?)", root).Scan(&steps))
	assert.Must(assert.True(flows+steps > e.reapBatchRows && steps > 5000, "%d flows and %d steps", flows, steps))

	// Open work, newer than the tree, as live flows are: 16,384 interrupted flows of four interrupted steps, which no
	// janitor removes and no worker claims. With it the tree is a fraction of each table, and the statistics say so.
	open := seedTrees(t, db, 1, treeShape{rootSteps: 0, childSteps: -1})[0]
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status='"+workflow.StatusInterrupted+"', workflow_url='seed/open'"+
		" WHERE flow_id=?", open)
	assert.Must(assert.NoError(err))
	for range 14 {
		_, err := db.ExecContext(ctx,
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, surgraph_flow_id,"+
				" surgraph_step_id, root_flow_id) SELECT flow_token, workflow_url, workflow_name, graph, graph_hash, status,"+
				" surgraph_flow_id, surgraph_step_id, root_flow_id FROM dwarf_flows WHERE workflow_url='seed/open'")
		assert.Must(assert.NoError(err))
	}
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET root_flow_id=flow_id WHERE workflow_url='seed/open'")
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, flow_created_hour)"+
			" SELECT f.flow_id, d.n, f.flow_token, 'Open', 'seed/open', '"+workflow.StatusInterrupted+"', 60000, f.created_hour"+
			" FROM dwarf_flows f CROSS JOIN (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4) d"+
			" WHERE f.workflow_url='seed/open'")
	assert.Must(assert.NoError(err))
	for _, table := range []string{"dwarf_flows", "dwarf_steps"} {
		analyze := map[string]string{"pgx": "ANALYZE " + table, "mysql": "ANALYZE TABLE " + table,
			"mariadb": "ANALYZE TABLE " + table, "mssql": "UPDATE STATISTICS " + table}[db.DriverName()]
		if analyze != "" {
			rows, err := db.QueryContext(ctx, analyze) // MySQL and MariaDB answer with a result set
			assert.Must(assert.NoError(err))
			rows.Close()
		}
	}
	assert.NoError(db.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", root).Scan(&hour))
	rootKey := keys.NewFlowKey(1, hour, root, "0123456789abcdef") // seedTrees' token

	spacerKey, err := e.Create(ctx, "removallocks/spacer", nil, nil)
	assert.Must(assert.NoError(err))
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	outcome, err := e.Await(awaitCtx, spacerKey)
	assert.Must(assert.NoError(err))
	assert.Must(assert.Equal(workflow.StatusInterrupted, outcome.Status))
	if deleting {
		assert.Must(assert.NoError(e.Delete(ctx, rootKey)))
	}

	seams := e.diags.Seams()
	seams.Break(diagnostics.CheckpointReapDeleted)
	defer seams.Resume(diagnostics.CheckpointReapDeleted) // below the Shutdown defer, so it runs first
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		e.drainJanitors(context.Background(), 0)
	}()
	assert.Must(assert.True(seams.WaitTimeout(ctx, diagnostics.CheckpointReapDeleted, 60*time.Second*enginetest.TimeoutScale()),
		"a removal ran its deletes"))

	if db.DriverName() == "mssql" {
		rows, err := db.QueryContext(ctx, "SELECT OBJECT_NAME(resource_associated_entity_id), request_mode FROM sys.dm_tran_locks"+
			" WHERE resource_database_id=DB_ID() AND resource_type='OBJECT' AND request_mode IN ('S','U','X','SIX')"+
			" AND resource_associated_entity_id IN (OBJECT_ID('dwarf_steps'), OBJECT_ID('dwarf_flows'))")
		if assert.NoError(err) {
			var tableLocks []string
			for rows.Next() {
				var table, mode string
				if assert.NoError(rows.Scan(&table, &mode)) {
					tableLocks = append(tableLocks, table+" "+mode)
				}
			}
			assert.NoError(rows.Err())
			rows.Close()
			assert.Zero(len(tableLocks), "the removal holds table locks, SQL Server's escalation of its row locks: %v", tableLocks)
		}
	}
	probeCtx, cancelProbe := context.WithTimeout(ctx, 15*time.Second*enginetest.TimeoutScale())
	defer cancelProbe()
	_, outcome, err = e.Run(probeCtx, "removallocks/probe", nil, nil)
	if assert.NoError(err, "a flow beside the removal waited for it") {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}

	seams.Resume(diagnostics.CheckpointReapDeleted)
	select {
	case <-drained:
	case <-time.After(120 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the janitors never finished their pass")
	}
	var left int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id=?", root).Scan(&left))
	assert.Zero(left, "the tree left the live tables")
	_, err = e.Summary(ctx, rootKey)
	if deleting {
		assert.Equal(http.StatusNotFound, errors.StatusCode(err), "the tree is deleted")
	} else {
		assert.NoError(err, "the tree is archived")
	}
}
