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
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// wideLocksEngine starts an engine whose host runs a one-step probe graph, and returns it with shard 1's database.
// SQLite is skipped: its one writer holds the whole database by design.
func wideLocksEngine(t *testing.T) (*Engine, *sequel.DB) {
	assert := testarossa.For(t)
	mux := hosts.NewMux()
	probe := workflow.NewGraph("Probe")
	probe.SetURL("Probe", "widelocks/probe")
	probe.AddTransition("Probe", workflow.End)
	assert.NoError(probe.Validate())
	mux.HandleGraph("widelocks/probe", probe)
	mux.HandleTask("widelocks/probe", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	assert.NoError(e.SetHost(mux))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	if db.DriverName() == "sqlite" {
		t.Skip("SQLite's one writer holds the whole database")
	}
	return e, db
}

// seedWideOpenFlow inserts an interrupted flow of 8,192 interrupted steps - a forEach keeps its branches in one flow,
// so a wide fan-out waiting on its branches is one flow of that many open steps - among 16,384 interrupted flows of
// four interrupted steps, the open work beside it, and returns the wide flow's key. Interrupted steps are open to
// Terminate and Cancel, and no worker claims them. With advancing, the wide flow is running and its steps are
// completed and still advancing - branches queued at their fan-in - under a lease an hour long, which the orphan
// repair leaves alone. One more open flow follows the wide one, as newer live work does on a busy shard: at
// REPEATABLE READ, MySQL and MariaDB lock the gap after a flow's last step in the flow index, where the steps of a
// newer flow go only while there is none.
func seedWideOpenFlow(t *testing.T, db *sequel.DB, advancing bool) (flowKey string) {
	assert := testarossa.For(t)
	ctx := t.Context()

	open := seedTrees(t, db, 1, treeShape{rootSteps: 0, childSteps: -1})[0]
	_, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET status='"+workflow.StatusInterrupted+"', workflow_url='seed/open'"+
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

	wide := seedTrees(t, db, 1, treeShape{rootSteps: 1, childSteps: -1})[0]
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status='"+workflow.StatusInterrupted+"' WHERE flow_id=?", wide)
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET status='"+workflow.StatusInterrupted+"' WHERE flow_id=?", wide)
	assert.Must(assert.NoError(err))
	for range 13 {
		_, err := db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms,"+
				" flow_created_hour) SELECT flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms,"+
				" flow_created_hour FROM dwarf_steps WHERE flow_id=?", wide)
		assert.Must(assert.NoError(err))
	}
	if advancing {
		_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status='"+workflow.StatusRunning+"' WHERE flow_id=?", wide)
		assert.Must(assert.NoError(err))
		_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET status='"+workflow.StatusCompleted+"', advancing=1,"+
			" lease_expires=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?", time.Hour.Milliseconds(), wide)
		assert.Must(assert.NoError(err))
	}
	spacer := seedTrees(t, db, 1, treeShape{rootSteps: 1, childSteps: -1})[0]
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status='"+workflow.StatusInterrupted+"' WHERE flow_id=?", spacer)
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET status='"+workflow.StatusInterrupted+"' WHERE flow_id=?", spacer)
	assert.Must(assert.NoError(err))
	var steps, hour int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", wide).Scan(&steps))
	assert.Must(assert.True(steps > 5000, "%d steps", steps))
	for _, table := range []string{"dwarf_flows", "dwarf_steps"} {
		analyze := map[string]string{"pgx": "ANALYZE " + table, "mysql": "ANALYZE TABLE " + table,
			"mariadb": "ANALYZE TABLE " + table, "mssql": "UPDATE STATISTICS " + table}[db.DriverName()]
		if analyze != "" {
			rows, err := db.QueryContext(ctx, analyze) // MySQL and MariaDB answer with a result set
			assert.Must(assert.NoError(err))
			rows.Close()
		}
	}
	assert.NoError(db.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", wide).Scan(&hour))
	return keys.NewFlowKey(1, hour, wide, "0123456789abcdef") // seedTrees' token
}

// tableLocks lists the table-level S, U, X and SIX locks held on dwarf_flows and dwarf_steps on SQL Server, where
// such a lock is an escalation of row locks; elsewhere it lists none.
func tableLocks(t *testing.T, db *sequel.DB) (locks []string) {
	assert := testarossa.For(t)
	if db.DriverName() != "mssql" {
		return nil
	}
	rows, err := db.QueryContext(t.Context(), "SELECT OBJECT_NAME(resource_associated_entity_id), request_mode FROM sys.dm_tran_locks"+
		" WHERE resource_database_id=DB_ID() AND resource_type='OBJECT' AND request_mode IN ('S','U','X','SIX')"+
		" AND resource_associated_entity_id IN (OBJECT_ID('dwarf_steps'), OBJECT_ID('dwarf_flows'))")
	if !assert.NoError(err) {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var table, mode string
		if assert.NoError(rows.Scan(&table, &mode)) {
			locks = append(locks, table+" "+mode)
		}
	}
	assert.NoError(rows.Err())
	return locks
}

// TestTerminate_AWideOpenFanOutLeavesTheTablesUnlocked terminates a flow of 8,192 steps Terminate writes - open
// steps, or completed steps still advancing, whose lease it bumps - and holds the Terminate's transaction open once it
// has written them, locked the flow rows and rescanned the tree, while a flow runs beside it. The flow's writes to
// dwarf_flows and dwarf_steps must not wait for the Terminate. On SQL Server the Terminate's locks are also read: a
// table-level lock there is an escalation of its row locks.
func TestTerminate_AWideOpenFanOutLeavesTheTablesUnlocked(t *testing.T) {
	t.Parallel()
	for _, steps := range []string{"open", "advancing"} {
		t.Run(steps, func(t *testing.T) {
			t.Parallel()
			testTerminateLeavesTheTablesUnlocked(t, steps == "advancing")
		})
	}
}

func testTerminateLeavesTheTablesUnlocked(t *testing.T, advancing bool) {
	assert := testarossa.For(t)
	ctx := t.Context()
	e, db := wideLocksEngine(t)
	wideKey := seedWideOpenFlow(t, db, advancing)
	_, _, wide, _, _ := keys.ParseFlowKey(wideKey)
	seams := e.diags.Seams()
	seams.Break(diagnostics.CheckpointTerminateAfterRescan)
	defer seams.Resume(diagnostics.CheckpointTerminateAfterRescan) // below the Shutdown cleanup, so it runs first
	terminated := make(chan error, 1)
	go func() { terminated <- e.Terminate(context.Background(), wideKey, "wide") }()
	assert.Must(assert.True(seams.WaitTimeout(ctx, diagnostics.CheckpointTerminateAfterRescan, 60*time.Second*enginetest.TimeoutScale()),
		"the Terminate locked the tree"))

	assert.Zero(len(tableLocks(t, db)), "the Terminate holds table locks, SQL Server's escalation of its row locks: %v", tableLocks(t, db))
	probeCtx, cancelProbe := context.WithTimeout(ctx, 15*time.Second*enginetest.TimeoutScale())
	defer cancelProbe()
	_, outcome, err := e.Run(probeCtx, "widelocks/probe", nil, nil)
	if assert.NoError(err, "a flow beside the Terminate waited for it") {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}

	seams.Resume(diagnostics.CheckpointTerminateAfterRescan)
	select {
	case err := <-terminated:
		assert.NoError(err)
	case <-time.After(60 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("the Terminate never returned")
	}
	var unwritten int
	if advancing {
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND lease_seq=0", wide).Scan(&unwritten))
		assert.Zero(unwritten, "every advancing step of the flow has a new lease generation")
	} else {
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status<>'"+workflow.StatusTerminated+"'", wide).Scan(&unwritten))
		assert.Zero(unwritten, "every step of the flow is terminated")
	}
	outcome, err = e.Snapshot(ctx, wideKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusTerminated, outcome.Status)
	}
}

// TestCancel_AWideOpenFanOutMarksWithoutEscalating cancels a flow of 8,192 open steps. Cancel marks them outside a
// transaction, a statement at a time, so no lock of it is held for a test to meet. On SQL Server the test reads how
// many times a lock on dwarf_steps escalated while the Cancel ran, which must be none; on every dialect, that each
// step was marked.
func TestCancel_AWideOpenFanOutMarksWithoutEscalating(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, db := wideLocksEngine(t)
	wideKey := seedWideOpenFlow(t, db, false)
	_, _, wide, _, _ := keys.ParseFlowKey(wideKey)

	before := stepEscalations(t, db)
	assert.Must(assert.NoError(e.Cancel(ctx, wideKey, "wide")))
	assert.Equal(before, stepEscalations(t, db), "the Cancel's mark escalated its row locks on dwarf_steps to a table lock")
	var unmarked int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND cancelling=0", wide).Scan(&unmarked))
	assert.Zero(unmarked, "every open step of the flow is marked")
}

// stepEscalations is how many times a lock on dwarf_steps has escalated to a table lock, on SQL Server; elsewhere 0.
func stepEscalations(t *testing.T, db *sequel.DB) (n int) {
	assert := testarossa.For(t)
	if db.DriverName() == "mssql" {
		assert.NoError(db.QueryRowContext(t.Context(), "SELECT COALESCE(SUM(index_lock_promotion_count), 0)"+
			" FROM sys.dm_db_index_operational_stats(DB_ID(), OBJECT_ID('dwarf_steps'), NULL, NULL)").Scan(&n))
	}
	return n
}

// TestOrphanRepair_AWideFlowRewindsWithoutEscalating has the orphan repair take back a flow of 8,192 completed steps
// still advancing whose leases lapsed, as a replica that dies with that many arrivals queued at a fan-in leaves them.
// On SQL Server the rewind must not escalate its row locks on dwarf_steps to a table lock; on every dialect, every
// step is taken back.
func TestOrphanRepair_AWideFlowRewindsWithoutEscalating(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, db := wideLocksEngine(t)
	wideKey := seedWideOpenFlow(t, db, true)
	_, _, wide, _, _ := keys.ParseFlowKey(wideKey)
	_, err := db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", wide)
	assert.Must(assert.NoError(err))

	before := stepEscalations(t, db)
	assert.NoError(e.recoverOrphanedFlows(ctx, db, 1))
	assert.Equal(before, stepEscalations(t, db), "the orphan repair's rewind escalated its row locks on dwarf_steps to a table lock")
	var kept int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND lease_seq=0", wide).Scan(&kept))
	assert.Zero(kept, "every step of the flow is taken back")
}

// TestRetry_AWideChildIsReapedWithoutEscalating reaps the child of a subgraph caller whose flow.Retry rewinds it, the
// child holding 8,192 steps, as a forEach inside it leaves. On SQL Server the reap must not escalate its row locks on
// dwarf_steps to a table lock; on every dialect, the child and its steps are gone.
func TestRetry_AWideChildIsReapedWithoutEscalating(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, db := wideLocksEngine(t)
	root := seedTrees(t, db, 1, callShape)[0]
	var caller, child int
	assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=?", root).Scan(&caller)))
	assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_step_id=?", caller).Scan(&child)))
	for range 13 {
		_, err := db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms,"+
				" flow_created_hour) SELECT flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms,"+
				" flow_created_hour FROM dwarf_steps WHERE flow_id=?", child)
		assert.Must(assert.NoError(err))
	}
	if db.DriverName() == "mssql" {
		_, err := db.ExecContext(ctx, "UPDATE STATISTICS dwarf_steps")
		assert.Must(assert.NoError(err))
	}

	before := stepEscalations(t, db)
	assert.NoError(db.Transact(ctx, func(tx *sequel.Tx) error {
		return e.deleteSubgraphFlowsRootedAt(ctx, tx, caller)
	}))
	assert.Equal(before, stepEscalations(t, db), "the reap escalated its row locks on dwarf_steps to a table lock")
	var steps, flows int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", child).Scan(&steps))
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", child).Scan(&flows))
	assert.Zero(steps, "the child's steps are reaped")
	assert.Zero(flows, "the child is reaped")
}
