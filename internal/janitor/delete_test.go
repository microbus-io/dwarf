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

package janitor

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestDelete_AStampedTreeIsDeletedWhenItsStampIsDue pins fast deletion: a tree stamped for deletion is deleted whole
// once its stamp is due, counted as reaped for its reason, and one whose stamp is not due yet - DeleteOnCompletion's
// grace - is left where it is.
func TestDelete_AStampedTreeIsDeletedWhenItsStampIsDue(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	r.settings.grace = time.Hour // the closed trees are not archiving's yet
	roots := seedTrees(t, r.db, 3, callShape)
	stampTree(t, r.db, roots[0])
	stampTree(t, r.db, roots[1])
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=? WHERE flow_id=?", time.Hour.Milliseconds(), roots[2])
	assert.NoError(err)
	r.pass(ctx)
	for i, id := range roots {
		flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		if i < 2 {
			assert.Zero(flows+steps, "root %d deleted", i)
		} else {
			assert.Equal(2, flows, "a stamp not due yet waits")
		}
	}
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	assert.Equal(int64(4), counterSum(rm, "dwarf_flows_reaped"), "two trees of two flows")
	day, _ := r.archive.Today(ctx)
	flows, _ := treeCount(t, r, migrations.FlowsTable(day), migrations.StepsTable(day), roots[0])
	assert.Zero(flows, "a stamped tree is never archived")
}

// TestDelete_APublishedLiveCopyIsNotCountedReaped pins the accounting of a published tree's live copy: it is deleted
// like any stamped tree, but it was archived, so dwarf_flows_reaped leaves it out.
func TestDelete_APublishedLiveCopyIsNotCountedReaped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	r.settings.grace = time.Hour
	roots := seedTrees(t, r.db, 2, callShape)
	stampTree(t, r.db, roots[0])
	stampTree(t, r.db, roots[1])
	at, arg := database.SQLTime(r.db.DriverName(), time.Now().Add(-time.Minute))
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+at+" WHERE flow_id=?", arg, roots[0])
	assert.NoError(err)
	r.pass(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows)
	}
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	assert.Equal(int64(2), counterSum(rm, "dwarf_flows_reaped"), "only the unpublished tree's two flows")
}

// TestDelete_ALargeStampedTreeIsPassed pins that a stamped tree too large for a batch is passed - stamped and whole,
// left to the deletion in chunks - and the trees after it are deleted.
func TestDelete_ALargeStampedTreeIsPassed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.grace = time.Hour
	r.settings.batchRows = 5
	large := seedTrees(t, r.db, 1, chunkShape)[0] // 9 rows
	small := seedTrees(t, r.db, 1, callShape)[0]  // 4 rows
	stampTree(t, r.db, large)
	stampTree(t, r.db, small)
	r.pass(ctx)
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", large)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps}, "left whole")
	flows, _ = treeCount(t, r, "dwarf_flows", "dwarf_steps", small)
	assert.Zero(flows)
	assert.Equal(small, r.janitor().del.after)
}

// TestDelete_FastPassesTheBatchItAttempted pins fast deletion's advance: a batch whose deletion fails is passed - left
// behind the position, whole - and the batches after it are reached in the next round.
func TestDelete_FastPassesTheBatchItAttempted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.grace = time.Hour
	r.settings.batchRoots = 2
	roots := seedTrees(t, r.db, 4, callShape)
	for _, root := range roots {
		stampTree(t, r.db, root)
	}
	targeted := join(diagnostics.FaultReapMidTree, strconv.Itoa(roots[0]))
	r.seams.InjectN(targeted, 10000)
	defer r.seams.Withdraw(targeted)
	r.pass(ctx) // the failed batch: a round without progress
	r.pass(ctx)
	for i, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(i < 2, flows == 2, "root %d", i)
	}
	assert.Equal(1, r.logs.count("Janitor: removing stamped trees"))
}

// TestDelete_AStampedTreeIsDeletedWhileTomorrowIsNotWhole pins that deletion needs no archive: while tomorrow's
// tables are missing and the closed trees wait, the stamped ones are deleted.
func TestDelete_AStampedTreeIsDeletedWhileTomorrowIsNotWhole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	closed := seedTrees(t, r.db, 1, callShape)[0]
	stamped := seedTrees(t, r.db, 1, callShape)[0]
	stampTree(t, r.db, stamped)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	for _, table := range []string{migrations.FlowsTable(today + 1), migrations.StepsTable(today + 1)} {
		_, err = r.db.ExecContext(ctx, "DROP TABLE "+table)
		assert.NoError(err)
	}
	assert.NoError(r.archive.Refresh(ctx))
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", closed)
	assert.Equal(2, flows, "the closed tree stays live")
	flows, _ = treeCount(t, r, "dwarf_flows", "dwarf_steps", stamped)
	assert.Zero(flows, "the stamped tree is deleted")
}

// TestDelete_ResidencySplitsTheStampedRoots pins the residency clause of the stamped scan on a shard with two
// dispatchers: a stamped root is selected by its own class while fresh, by any once overdue by the steal grace.
func TestDelete_ResidencySplitsTheStampedRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedTrees(t, r.db, 1, soloShape)[0]
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1 WHERE flow_id=?", root)
	assert.NoError(err)
	selectWith := func(steal time.Duration, ordinal int) []int {
		where, args := stampedResidency(2, ordinal, true, steal)
		roots, err := scanStamped(ctx, r.db, r.seams, where, args, 512)
		assert.NoError(err)
		return roots
	}
	deadline := time.Now().Add(5 * time.Second * timeoutScale)
	for !slices.Contains(selectWith(0, 1-root%2), root) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	assert.True(slices.Contains(selectWith(0, 1-root%2), root), "an overdue stamped root, any class")
	assert.False(slices.Contains(selectWith(time.Hour, 1-root%2), root), "not another class's while fresh")
	assert.True(slices.Contains(selectWith(time.Hour, root%2), root), "its own class's")
}

// TestDelete_TheStampedScanWalksTheStampedIndex pins the stamped scan's plan to idx_dwarf_flows_delete_after - keyed
// on flow_id over the stamped roots alone on PostgreSQL and SQLite, named with FORCE INDEX on MySQL and MariaDB.
// Without it the scan walks the primary key past every unstamped root ahead of the first stamped one: an archive
// outage's whole held backlog, on every round from the front.
func TestDelete_TheStampedScanWalksTheStampedIndex(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	var explain, priceOutScans string
	switch db.DriverName() {
	case "pgx":
		// An empty table's estimates favour a scan or a bitmap read and sort whatever the index; priced out, the plan
		// shows whether the index itself is in the scan's order.
		explain, priceOutScans = "EXPLAIN ", "SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off"
	case "sqlite":
		explain = "EXPLAIN QUERY PLAN "
	case "mysql", "mariadb":
		explain = "EXPLAIN "
	default:
		t.Skip("the plan shapes pinned here are PostgreSQL's, SQLite's, MySQL's and MariaDB's")
	}
	stmt, args := stampedScanSQL(db.DriverName(), "", nil, 512)
	var plan strings.Builder
	errRollback := errors.New("rollback")
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		for _, set := range strings.Split(priceOutScans, "; ") {
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
	assert.True(errors.Is(err, errRollback), "%v", err)
	assert.Contains(plan.String(), "idx_dwarf_flows_delete_after", "%s", plan.String())
	if d := db.DriverName(); d == "pgx" || d == "sqlite" {
		assert.False(strings.Contains(plan.String(), "Sort") || strings.Contains(plan.String(), "TEMP B-TREE"),
			"no sort: %s", plan.String())
	}
}

// TestDelete_TheLockRechecksTheStamp pins the recheck under the root lock of a deletion: a root selected while stamped
// whose stamp was cleared since, or is not due yet, is not deleted.
func TestDelete_TheLockRechecksTheStamp(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	roots := seedTrees(t, r.db, 3, callShape)
	for _, root := range roots {
		stampTree(t, r.db, root)
	}
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=0 WHERE flow_id=?", roots[0])
	assert.NoError(err)
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=? WHERE flow_id=?", time.Hour.Milliseconds(), roots[1])
	assert.NoError(err)
	removed, err := move(ctx, r.db, r.seams, roots, stampedDueSQL, nil, nil)
	assert.NoError(err)
	assert.Equal(2, removed.flows, "only the root still due")
	for i, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(i < 2, flows == 2, "root %d", i)
	}
}
