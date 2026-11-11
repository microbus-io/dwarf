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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// treeKeys runs a parent that calls a child to completion and returns the root's key, its child's, and a step key of
// each.
func treeKeys(t *testing.T, e *Engine) (rootKey, childKey, rootStep, childStep string) {
	t.Helper()
	ctx := t.Context()
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	hist, err := e.History(ctx, fk)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range hist {
		if rootStep == "" {
			rootStep = s.StepKey
		}
		if s.SubgraphKey != "" {
			childKey = s.SubgraphKey
			for _, c := range s.SubHistory {
				childStep = c.StepKey
			}
		}
	}
	if childKey == "" || childStep == "" {
		t.Fatal("the parent called no child")
	}
	return fk, childKey, rootStep, childStep
}

// assertHidden asserts that every read by key of the flow is a 404.
func assertHidden(t *testing.T, e *Engine, flowKey, stepKey, why string) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := t.Context()
	_, err := e.Snapshot(ctx, flowKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "Snapshot: %s", why)
	_, err = e.Summary(ctx, flowKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "Summary: %s", why)
	_, err = e.History(ctx, flowKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "History: %s", why)
	_, _, err = e.Fingerprint(ctx, flowKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "Fingerprint: %s", why)
	_, err = e.Step(ctx, stepKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "Step: %s", why)
}

// closedListed returns how many flows ListClosed lists, subgraph children included.
func closedListed(t *testing.T, e *Engine) int {
	t.Helper()
	got, err := e.ListClosed(t.Context(), enginetest.Recent(workflow.Query{IncludeSubgraphs: true}))
	if err != nil {
		t.Fatal(err)
	}
	return len(got)
}

// TestArchiveVisibility_AChildWithoutItsRootIsHidden pins the universal rule for a copy that is not published, or
// abandoned: an archived child whose root is not in its day is not found by key - by locate's probe or the reader's
// closing read - and is not listed, though its rows are there.
func TestArchiveVisibility_AChildWithoutItsRootIsHidden(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	rootKey, childKey, _, childStep := treeKeys(t, e)
	_, _, childID, _, _ := keys.ParseFlowKey(childKey)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	// The child's rows copied into today, as a staged copy writes them, and gone from the live tables; the root not.
	dayColumns := strings.Join(columnsOf(t, db, migrations.FlowsTable(today)), ", ")
	for _, stmt := range []string{
		"INSERT INTO " + migrations.FlowsTable(today) + " (" + dayColumns + ") SELECT " + dayColumns + " FROM dwarf_flows WHERE flow_id=?",
		"UPDATE " + migrations.FlowsTable(today) + " SET archived_at=NOW_UTC() WHERE flow_id=?",
		"INSERT INTO " + migrations.StepsTable(today) + " SELECT * FROM dwarf_steps WHERE flow_id=?",
		"DELETE FROM dwarf_steps WHERE flow_id=?",
		"DELETE FROM dwarf_flows WHERE flow_id=?",
	} {
		_, err := db.ExecContext(ctx, stmt, childID)
		assert.Must(assert.NoError(err))
	}
	assert.Zero(closedListed(t, e), "the root is live, closed recently; the child hidden")
	assertHidden(t, e, childKey, childStep, "a child whose root is not in its day")
	err = e.Delete(ctx, childKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "Delete: not a 400 that would tell the child is there")
	_, err = e.Snapshot(ctx, rootKey)
	assert.NoError(err, "the live root is unaffected")
}

// TestArchiveVisibility_AMarkedTreeIsHidden pins the archive's deletion mark: a root marked for deletion hides its
// whole tree at once, from every read by key, Fork's included, and from the lists: a marked tree reads as deleted.
func TestArchiveVisibility_AMarkedTreeIsHidden(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	rootKey, childKey, rootStep, childStep := treeKeys(t, e)
	_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
	moveInto(t, e, rootID, today)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(2, closedListed(t, e), "archived and visible")
	_, err = e.Step(ctx, childStep)
	assert.NoError(err)

	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(today)+" SET delete_after_ms=1 WHERE flow_id=?", rootID)
	assert.Must(assert.NoError(err))
	assert.Zero(closedListed(t, e))
	assertHidden(t, e, rootKey, rootStep, "a marked root")
	assertHidden(t, e, childKey, childStep, "a child of a marked root")
	_, err = e.Fork(ctx, rootStep, nil)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "a tree marked for deletion is deleted, so not forked")
}

// TestArchiveVisibility_RootsListingsWalkTheRootsIndex pins the plan of a roots-only archive listing to the day's roots
// index, in the listing's order: without it the listing walks archived_at past every child archived in its window -
// 79-837 ms for a page beside one tree of 300,000 children, against 0.24-3.1 ms with it. On SQLite a status filter is
// written +f.status so the planner does not take the status index instead.
func TestArchiveVisibility_RootsListingsWalkTheRootsIndex(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	var explain, priceOut string
	switch db.DriverName() {
	case "pgx":
		explain, priceOut = "EXPLAIN ", "SET LOCAL enable_seqscan = off"
	case "sqlite":
		explain = "EXPLAIN QUERY PLAN "
	case "mysql", "mariadb":
		explain = "EXPLAIN "
	default:
		t.Skip("the plan shapes pinned here are PostgreSQL's, SQLite's, MySQL's and MariaDB's")
	}
	day := flowTables{flows: migrations.FlowsTable(today), steps: migrations.StepsTable(today)}
	status := "f.status='" + workflow.StatusCompleted + "'"
	if db.DriverName() == "sqlite" {
		status = "+" + status
	}
	for _, where := range []string{"f.surgraph_flow_id=0", "f.surgraph_flow_id=0 AND " + status} {
		stmt := "SELECT f.flow_id FROM " + day.flows + " f" + summaryJoins(day) + " WHERE " + where + " AND " + treeUnstamped +
			" ORDER BY f.archived_at DESC, f.flow_id DESC LIMIT_OFFSET(?, 0)"
		plan := explainPlan(t, db, explain, priceOut, stmt, 100)
		assert.Contains(plan, "idx_dwarf_flows_archive_"+strconv.Itoa(today)+"_roots", "%s: %s", where, plan)
	}
}

// explainPlan returns the plan the database gives for stmt, flattened, after priceOut (statements separated by "; ")
// in the same transaction.
func explainPlan(t *testing.T, db *sequel.DB, explain, priceOut, stmt string, args ...any) string {
	t.Helper()
	ctx := t.Context()
	var plan strings.Builder
	errRollback := errors.New("rollback")
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		for _, set := range strings.Split(priceOut, "; ") {
			if set == "" {
				continue
			}
			if _, err := tx.ExecContext(ctx, set); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, explain+stmt, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			for _, v := range vals {
				if b, ok := v.([]byte); ok {
					v = string(b)
				}
				fmt.Fprint(&plan, v, " ")
			}
			plan.WriteString("\n")
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		t.Fatal(err)
	}
	return plan.String()
}

// columnsOf returns a table's columns in order.
func columnsOf(t *testing.T, db *sequel.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), "SELECT * FROM "+table+" WHERE 1=0")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	return cols
}
