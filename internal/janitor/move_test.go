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
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestSQL_TerminalStatusesAreTheClosedOnes pins the literal the scans and rechecks inline against the workflow
// package's notion of a closed status.
func TestSQL_TerminalStatusesAreTheClosedOnes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, status := range []string{
		workflow.StatusCreated, workflow.StatusRunning, workflow.StatusInterrupted,
		workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled,
	} {
		assert.Equal(workflow.IsTerminalStatus(status), slices.Contains(terminalStatuses, status), status)
	}
}

// TestMove_ClosedTreeMovesToTodaysTables pins the archiver: once its root has closed, a whole tree - the root, its
// subgraph descendant and every step - leaves the live tables for today's archive tables in one pass, with its ids
// and its outcome as they were.
func TestMove_ClosedTreeMovesToTodaysTables(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	var err error
	rootID := seedTrees(t, db, 1, callShape)[0]

	liveFlows, liveSteps := treeCount(t, r, "dwarf_flows", "dwarf_steps", rootID)
	assert.Equal(2, liveFlows, "the root and its subgraph child")
	assert.True(liveSteps >= 2)
	var finalState string
	assert.NoError(db.QueryRowContext(ctx, "SELECT final_state FROM dwarf_flows WHERE flow_id=?", rootID).Scan(&finalState))

	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	r.pass(ctx)

	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", rootID)
	assert.Equal(0, flows)
	assert.Equal(0, steps)
	// The pass may straddle midnight; the day it read before is the one it archived into unless it did.
	archFlows, archSteps := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), rootID)
	if archFlows == 0 {
		archFlows, archSteps = treeCount(t, r, migrations.FlowsTable(today+1), migrations.StepsTable(today+1), rootID)
		today++
	}
	assert.Equal(liveFlows, archFlows)
	assert.Equal(liveSteps, archSteps)
	var archived string
	assert.NoError(db.QueryRowContext(ctx, "SELECT final_state FROM "+migrations.FlowsTable(today)+" WHERE flow_id=?", rootID).Scan(&archived))
	assert.Equal(finalState, archived)
}

// TestMove_RetentionZeroDeletes pins that a retention of 0 deletes a closed tree rather than archiving it.
func TestMove_RetentionZeroDeletes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	db := r.db
	var err error
	rootID := seedTrees(t, db, 1, callShape)[0]
	today, err := r.archive.Today(ctx)
	assert.NoError(err)

	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", rootID)
	assert.Equal(0, flows)
	assert.Equal(0, steps)
	archFlows, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), rootID)
	assert.Equal(0, archFlows, "a retention of 0 archives nothing")
}

// TestMove_BatchesAreCappedByRowsAndBytes pins how a batch is cut: the leading trees whose rows and bytes both fit
// the batch, and always the first however large - sized in groups, one call each, the first tree read up to its own
// limit of rows and every other up to one past what was left of the batch's rows when its group was sent.
func TestMove_BatchesAreCappedByRowsAndBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	rowsOf := map[int]int{1: 3, 2: 4, 3: 5, 4: 100}
	bytesOf := map[int]int{1: 300, 2: 400, 3: 500, 4: 10000, 5: 9000}
	type call struct{ group, limits []int }
	cut := func(roots []int, batch limit, firstRows int) ([]int, size, []call) {
		var calls []call
		c, first, err := cutWithin(roots, batch, firstRows, func(group, limits []int) ([]size, error) {
			calls = append(calls, call{append([]int(nil), group...), append([]int(nil), limits...)})
			sizes := make([]size, len(group))
			for k, root := range group {
				n := rowsOf[root]
				if n == 0 {
					n = 1
				}
				sizes[k] = size{rows: min(n, limits[k]), bytes: bytesOf[root]}
			}
			return sizes, nil
		})
		assert.NoError(err)
		return c, first, calls
	}
	got, first, calls := cut([]int{1, 2, 3, 4}, capOf(8, 0), 9)
	assert.Equal([]int{1, 2}, got)
	assert.Equal(size{rows: 3, bytes: 300}, first)
	assert.Equal([]call{{[]int{1, 2, 3, 4}, []int{9, 9, 9, 9}}}, calls, "one call; each capped to one past the batch")
	got, _, _ = cut([]int{1, 2, 3, 4}, capOf(12, 0), 13)
	assert.Equal([]int{1, 2, 3}, got)
	got, first, calls = cut([]int{4, 1}, capOf(8, 0), 50)
	assert.Equal([]int{4}, got, "a tree over the budget goes alone")
	assert.Equal(50, first.rows, "read to its limit, not whole")
	assert.Equal(50, calls[0].limits[0])
	got, _, _ = cut([]int{1, 2, 3, 4}, capOf(1000, 0), 1001)
	assert.Equal([]int{1, 2, 3, 4}, got)

	// Bytes cut a batch the rows would let through, and a tree of few rows but many bytes goes alone.
	got, _, _ = cut([]int{1, 2, 3}, capOf(1000, 800), 1001)
	assert.Equal([]int{1, 2}, got, "300 + 400 bytes fit 800, 500 more do not")
	got, first, _ = cut([]int{5, 1}, capOf(1000, 800), 1001)
	assert.Equal([]int{5}, got, "one row of 9,000 bytes goes alone")
	assert.Equal(size{rows: 1, bytes: 9000}, first)

	many := make([]int, 20)
	for i := range many {
		many[i] = 100 + i // trees of one row
	}
	got, _, calls = cut(many, capOf(100, 0), 101)
	assert.Equal(many, got)
	assert.Equal(2, len(calls), "a first group, then the rest")
	assert.Equal(firstGroup, len(calls[0].group))
	assert.Equal(85, calls[1].limits[0], "the later group capped to what was left: 100 - 16 + 1")
}

// TestMove_ABatchHoldsAtMostMaxBatchFlows pins the flow cap of an atomic batch: trees go together only while their
// flows fit MaxBatchFlows, whatever the rows and bytes allow, and a tree of more goes alone.
func TestMove_ABatchHoldsAtMostMaxBatchFlows(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	flowsOf := map[int]int{1: 1000, 2: 1000, 3: 1000, 4: 1000, 5: 1000, 6: 5000}
	sizes := func(group, limits []int) ([]size, error) {
		out := make([]size, len(group))
		for k, root := range group {
			out[k] = size{rows: flowsOf[root], flows: flowsOf[root]}
		}
		return out, nil
	}
	batch := capOf(1<<20, 0)
	batch.flows = MaxBatchFlows
	got, _, err := cutWithin([]int{1, 2, 3, 4, 5}, batch, batch.rows+1, sizes)
	assert.NoError(err)
	assert.Equal([]int{1, 2, 3, 4}, got, "4,000 flows fit, 5,000 do not")
	got, first, err := cutWithin([]int{6, 1}, batch, batch.rows+1, sizes)
	assert.NoError(err)
	assert.Equal([]int{6}, got, "a tree over the cap goes alone")
	assert.Equal(5000, first.flows)
}

// TestMove_ResidencySplitsTheDueRoots pins the residency clause the closed scan takes on a shard with two
// dispatchers: each class selects only its own roots while they are fresh, and either takes the other's once they are
// overdue by the steal grace. The roots are trees of one flow so that both classes hold some.
func TestMove_ResidencySplitsTheDueRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	roots := seedTrees(t, db, 4, soloShape)
	selectWith := func(steal time.Duration, ordinal int) []int {
		residency, residencyArgs := closedResidency(2, ordinal, true, r.settings.grace, steal)
		roots, _, err := scanClosed(ctx, db, r.seams, r.settings.grace, residency, residencyArgs, r.settings.batchRoots)
		assert.NoError(err)
		return rootIDs(roots)
	}
	var both []int
	for _, ordinal := range []int{0, 1} {
		for _, id := range selectWith(time.Hour, ordinal) {
			assert.Equal(ordinal, id%2, "a fresh root is selected only by its own class")
			both = append(both, id)
		}
	}
	slices.Sort(both)
	assert.Equal(roots, both, "and the two classes cover every root once")
	assert.True(slices.ContainsFunc(roots, func(id int) bool { return id%2 == 0 }) &&
		slices.ContainsFunc(roots, func(id int) bool { return id%2 == 1 }), "both classes hold roots: %v", roots)
	all := selectWith(0, 0)
	for _, id := range roots {
		assert.True(slices.Contains(all, id), "an overdue root %d is selected by any class", id)
	}
}

// TestMove_TheScanMergesTheStatusesInCloseOrder pins the closed scan's traversal: the four per-status branches
// merged into one (updated_at, flow_id) order, ties broken by flow_id; a fetch shorter than the due set returning
// exactly the first of that order, however each branch was cut; and a batch trimmed to a prefix in that order, not in
// flow_id order. Each tree is one flow of one step, one row.
func TestMove_TheScanMergesTheStatusesInCloseOrder(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	var err error
	ids := seedTrees(t, db, 4, soloShape)
	a, b, c, d := ids[0], ids[1], ids[2], ids[3]
	closeAt := func(status string, ago time.Duration, flowID int) {
		_, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET status=?, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" WHERE flow_id=?", status, -ago.Milliseconds(), flowID)
		assert.NoError(err)
	}
	closeAt(workflow.StatusFailed, 4*time.Minute, a)
	closeAt(workflow.StatusCancelled, 5*time.Minute, c)
	// A tie: b and d closed in the same millisecond, in different statuses. One statement reads one now.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET status=CASE WHEN flow_id=? THEN '"+workflow.StatusCompleted+
		"' ELSE '"+workflow.StatusTerminated+"' END, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id IN (?, ?)",
		b, -(3 * time.Minute).Milliseconds(), b, d)
	assert.NoError(err)

	roots, _, err := scanClosed(ctx, db, r.seams, 0, "", nil, 10)
	assert.NoError(err)
	assert.Equal([]int{c, a, b, d}, rootIDs(roots), "closed first first, a tie by flow_id")
	assert.True(len(roots) == 4 && roots[2].updatedAt.Equal(roots[3].updatedAt), "b and d tie")
	roots, _, err = scanClosed(ctx, db, r.seams, 0, "", nil, 2)
	assert.NoError(err)
	assert.Equal([]int{c, a}, rootIDs(roots), "a short fetch is the first of the merged order")

	r.settings.batchRows = 4 // two trees of a flow and a step each
	cut, via, err := r.fast().cut(ctx, db, r.settings, rootIDs(roots), true)
	assert.NoError(err)
	assert.Equal([]int{c, a}, cut, "the trim keeps the traversal order")
	assert.Equal(routeMove, via)
}

// TestMove_APassDrainsInSeveralBatches pins the rounds of one pass: with batches far smaller than the due set, and
// a row budget that cuts them further, one pass still moves every closed tree, one batch a round.
func TestMove_APassDrainsInSeveralBatches(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.batchRoots = 2
	r.settings.batchRows = 3
	db := r.db
	roots := seedTrees(t, db, 7, callShape)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for _, id := range roots {
		flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(0, flows+steps, "tree %d left in the live tables", id)
		archFlows, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(2, archFlows, "tree %d not archived whole", id)
	}
}

// TestMove_ABatchThatMeetsContentionIsPassed pins that lock contention past Transact's retries is an archive failure
// like any other: the batch stays whole, neither archived nor deleted, and is passed - left behind the position, for a
// sweep or a janitor starting over, which archives it once contention clears.
func TestMove_ABatchThatMeetsContentionIsPassed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	roots := seedTrees(t, db, 3, callShape)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)

	r.seams.InjectN(FaultContention, 1000)
	r.pass(ctx)
	r.seams.Withdraw(FaultContention)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d stays live", id)
	}
	r.pass(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d is behind the position", id)
	}

	r.restart()
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for _, id := range roots {
		archFlows, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(2, archFlows, "tree %d archived by a janitor starting over", id)
	}
}

// TestMove_NoClosedTreeMovesWhileTomorrowIsNotWhole pins that while tomorrow's tables are missing - so today, whose
// own tables are there, must not be written, or a locate would leave out a day that holds the flow - the closed trees
// wait in the live tables, neither archived nor deleted. Repeated failures never authorize deletion of the retained
// trees.
func TestMove_NoClosedTreeMovesWhileTomorrowIsNotWhole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	var err error
	roots := seedTrees(t, db, 3, callShape)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	// Tomorrow's tables dropped, its migrations still recorded: it cannot be made again, and the archive's record,
	// read again, no longer holds it - today is whole, tomorrow is not.
	for _, table := range []string{migrations.FlowsTable(today + 1), migrations.StepsTable(today + 1)} {
		_, err = db.ExecContext(ctx, "DROP TABLE "+table)
		assert.NoError(err)
	}
	assert.NoError(r.archive.Refresh(ctx))
	assert.True(r.archive.Whole(today), "today is there")

	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d stays live", id)
		archFlows, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(0, archFlows, "tree %d is not archived", id)
	}
	assert.True(r.failing(time.Now().Add(time.Second)) > 0, "an archive failure")

	r.pass(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d remains live while the archive is unavailable", id)
	}
}

// TestMove_AFatalArchiveFaultNeverAuthorizesDeletion pins that an archive failure keeps the trees live, however
// often it repeats, and that a later move clears the failure.
func TestMove_AFatalArchiveFaultNeverAuthorizesDeletion(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	r.settings.batchRoots = 1
	db := r.db
	roots := seedTrees(t, db, 3, callShape)
	r.seams.InjectN(FaultArchiveErr, 10000)
	for range 3 {
		r.pass(t.Context())
	}
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows)
	}
	assert.True(r.failing(time.Now().Add(time.Second)) > 0)
	r.seams.Withdraw(FaultArchiveErr)
	later := seedTrees(t, db, 1, callShape)[0]
	r.pass(t.Context())
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", later)
	assert.Zero(flows)
	assert.Zero(r.failing(time.Now()), "a move clears the failure")
}

// TestMove_ALastingFaultPreservesAndLogs pins that both ordinary errors and contention preserve the trees through
// repeated failures, and that a failed move is logged once per failure episode, not once per attempt.
func TestMove_ALastingFaultPreservesAndLogs(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{FaultArchiveErr, FaultContention} {
		t.Run(fault, func(t *testing.T) {
			assert := testarossa.For(t)
			r := newRig(t, 30)
			r.settings.batchRoots = 1
			logs := r.logs
			roots := seedTrees(t, r.db, 3, callShape)
			r.seams.InjectN(fault, 100000)
			for range 3 {
				r.pass(t.Context())
			}
			for _, id := range roots {
				flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
				assert.Equal(2, flows)
			}
			assert.Equal(1, logs.count("Janitor: archiving closed trees failed"))
			assert.Zero(logs.count("Janitor: removing closed trees"), "a failed move is logged once per episode, not per attempt")
		})
	}
}

// TestMove_AScanFailureAtRetentionZeroIsNoArchiveFailure pins that a failed scan starts the archive failure timer
// only where there is an archive: at a retention of 0 the job deletes, and nothing would ever clear a timer started
// there.
func TestMove_AScanFailureAtRetentionZeroIsNoArchiveFailure(t *testing.T) {
	t.Parallel()
	for _, retention := range []int{0, 30} {
		t.Run(strconv.Itoa(retention), func(t *testing.T) {
			assert := testarossa.For(t)
			r := newRig(t, retention)
			r.seams.InjectN(FaultSelectErr, 100000)
			r.pass(t.Context())
			r.seams.Withdraw(FaultSelectErr)
			failing := r.failing(time.Now().Add(time.Second)) > 0
			assert.Equal(retention > 0, failing)
		})
	}
}

// TestMove_NamesEveryFlowsColumn pins the move's column list against the flows table: every live column
// but archived_at and the live-only archive_attempt_at, in table order, so a column added to dwarf_flows without being
// added to the list fails here rather than being dropped from the archive. The day's flows table has the same columns,
// and its steps table the live steps table's, in the same order.
func TestMove_NamesEveryFlowsColumn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	want := append(strings.Split(archivedFlowColumns, ", "), "archived_at")
	assert.Equal(want, slices.DeleteFunc(columnNames(t, db, "dwarf_flows"), func(c string) bool { return c == liveOnlyColumn }))
	assert.Equal(want, columnNames(t, db, migrations.FlowsTable(today)))
	// The steps move by column order (RETURNING * and SELECT *), so the day's steps table has the live columns in the
	// live order.
	assert.Equal(columnNames(t, db, "dwarf_steps"), columnNames(t, db, migrations.StepsTable(today)))
}

// TestMove_EveryArchivedFlowIsStampedWhenItWasArchived pins archived_at: a live flow holds the not-archived value,
// and every flow of an archived tree - the root and its subgraph child - the database's time of the move.
func TestMove_EveryArchivedFlowIsStampedWhenItWasArchived(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	var err error
	root := seedTrees(t, db, 1, callShape)[0]
	// Ended an hour ago, so a stamp copied from when it ended - rather than taken when it moved - is an hour old.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(updated_at, ?) WHERE root_flow_id=?",
		-time.Hour.Milliseconds(), root)
	assert.NoError(err)
	unset, unsetArg := database.SQLTime(db.DriverName(), time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC))
	var live int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id=? AND archived_at="+unset,
		root, unsetArg).Scan(&live))
	assert.Equal(2, live, "not archived")

	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	r.pass(ctx)
	flows, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	if now, _ := r.archive.Today(ctx); flows == 0 && now != today {
		t.Skip("midnight passed during the pass")
	}
	assert.Equal(2, flows, "archived into today")
	rows, err := db.QueryContext(ctx, "SELECT DATE_DIFF_MILLIS(NOW_UTC(), archived_at) FROM "+migrations.FlowsTable(today)+
		" WHERE root_flow_id=?", root)
	assert.NoError(err)
	defer rows.Close()
	n := 0
	for rows.Next() {
		var ago float64 // fractional on SQLite
		assert.NoError(rows.Scan(&ago))
		assert.True(ago >= 0 && ago < float64(time.Minute.Milliseconds()), "archived %vms ago", ago)
		n++
	}
	assert.NoError(rows.Err())
	assert.Equal(2, n)
}

// TestMove_AMoveAfterItsDayEndedIsStampedWithinTheDay pins the archive time's clamp: a move into a day that has
// already ended - a round that read the day before midnight and moves after it - stamps the day's last millisecond,
// never a time of the next day, so a day holds only rows archived on it.
func TestMove_AMoveAfterItsDayEndedIsStampedWithinTheDay(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	a := r.archive
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, r, today-1))
	db := r.db
	root := seedTrees(t, db, 1, callShape)[0]
	moveInto(t, r, root, today-1)
	last, lastArg := database.SQLTime(db.DriverName(), a.DayEnds(today-1).Add(-time.Millisecond))
	var stamped int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+migrations.FlowsTable(today-1)+
		" WHERE root_flow_id=? AND archived_at="+last, root, lastArg).Scan(&stamped))
	assert.Equal(2, stamped, "the day's last millisecond")
}

// TestMove_NoTreeMovesInTheDaysLastMoments pins the quiet margin: a round with less than the margin left in today
// archives nothing, and the trees go into the next day's tables at its first round. Days of 16 seconds, so the margin
// is two, and the round runs with one to two left: before the margin it would archive.
func TestMove_NoTreeMovesInTheDaysLastMoments(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 100, 16*time.Second)
	r.settings.noQuietMargin = false
	a := r.archive
	assert.Equal(2*time.Second, quietMargin(a.DayLength()))
	assert.Equal(time.Minute, quietMargin(24*time.Hour))
	assert.Zero(quietMargin(8*time.Second), "a day too short for a margin")
	db := r.db
	var err error
	root := seedTrees(t, db, 1, callShape)[0]

	// TodayLeft rounds down, so 1 left means between one and two seconds to go: inside the margin, not past the day.
	deadline := time.Now().Add(time.Minute)
	var day int
	for {
		d, left, err := a.TodayLeft(ctx)
		assert.NoError(err)
		if left == time.Second {
			day = d
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("never saw a day's last second")
		}
		time.Sleep(50 * time.Millisecond)
	}
	r.pass(ctx)
	if now, _ := a.Today(ctx); now != day {
		t.Skip("the day ended during the pass")
	}
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal(2, flows, "left in the live tables")

	dayDeadline := time.Now().Add(a.DayLength() + 10*time.Second*timeoutScale)
	for {
		if now, _ := a.Today(ctx); now > day {
			break
		}
		if time.Now().After(dayDeadline) {
			t.Fatal("the next day never came")
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The days ahead of the new today, as the engine's maintenance makes them.
	_, err = a.Maintain(ctx, r.retention)
	assert.NoError(err)
	r.pass(ctx)
	flows, _ = treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Zero(flows, "archived on the next day")
	flows, _ = treeCount(t, r, migrations.FlowsTable(day), migrations.StepsTable(day), root)
	assert.Zero(flows, "not into the day that ended")
}

// TestMove_AHeldRootIsSkippedNotWaitedFor pins that a held root does not hold back the rest of a batch on the dialects
// with SKIP LOCKED. SQL Server and SQLite wait for it.
func TestMove_AHeldRootIsSkippedNotWaitedFor(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	var err error
	if d := db.DriverName(); d != "pgx" && d != "mysql" && d != "mariadb" {
		t.Skip("a held root is waited for on " + d)
	}
	roots := seedTrees(t, db, 2, callShape)
	held, other := roots[0], roots[1]

	locked := make(chan error, 1)
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- db.Transact(ctx, func(tx *sequel.Tx) error {
			_, err := lockRoots(ctx, tx, live.flows, false, "flow_id=?", held)
			locked <- err
			<-release
			return err
		})
	}()
	assert.NoError(<-locked)

	a := r.archive
	today, err := a.Today(ctx)
	assert.NoError(err)
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second*timeoutScale)
	defer cancel()
	due, dueArgs := closedDueSQL(0)
	moved, err := move(bounded, db, r.seams, []int{held, other}, due, dueArgs, &filingDay{day: today, archive: a})
	close(release)
	assert.NoError(<-holderDone)
	if assert.NoError(err, "the batch did not wait on the held root") {
		assert.Equal(2, moved.flows, "the other tree moved: its root and its child")
	}
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", held)
	assert.Equal(2, flows, "the held tree was left in the live tables")
	flows, _ = treeCount(t, r, "dwarf_flows", "dwarf_steps", other)
	assert.Zero(flows)

	moved, err = move(ctx, db, r.seams, []int{held}, due, dueArgs, &filingDay{day: today, archive: a})
	assert.NoError(err)
	assert.Equal(2, moved.flows, "the held tree moved once it was let go")
}

// TestMove_DirectoryNamesOnlyWhatTheLocateCannotFind pins the directory's rule, through the move: a tree archived on
// its creation day or the day after gets no row - the locate finds it by its key's hour - while one archived two or
// more days after its creation day, or on a day before it, has every flow named. The locate finds each in the day it
// was archived on.
func TestMove_DirectoryNamesOnlyWhatTheLocateCannotFind(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, r, today-1))
	assert.NoError(makeArchiveDay(t, r, today+2))
	db := r.db
	roots := seedTrees(t, db, 4, callShape)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the flows were seeded")
	}
	keyOf := map[int]string{}
	for _, id := range roots {
		var token string
		var hour int
		assert.NoError(db.QueryRowContext(ctx, "SELECT flow_token, created_hour FROM dwarf_flows WHERE flow_id=?", id).Scan(&token, &hour))
		keyOf[id] = keys.NewFlowKey(1, hour, id, token)
	}

	for i, c := range []struct {
		day   int
		named bool
	}{
		{today, false},     // the creation day
		{today + 1, false}, // the day after
		{today + 2, true},  // two days after
		{today - 1, true},  // a day before: a batch still writing the day that ended
	} {
		moveInto(t, r, roots[i], c.day)
		var named int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable+" WHERE day=?", c.day).Scan(&named))
		if c.named {
			assert.Equal(2, named, "day %d: the root and its child are named", c.day)
		} else {
			assert.Equal(0, named, "day %d: nothing is named", c.day)
		}
		day, err := located(t, r, keyOf[roots[i]])
		assert.NoError(err)
		assert.Equal(c.day, day, "located in the day it was archived on")
	}
}

// TestMove_DirectoryBounds pins the move's comparisons at the edges of DirectoryHours: a tree whose creation hour is
// one below the lower bound or at the upper bound is named, one at the lower bound or one below the upper is not.
func TestMove_DirectoryBounds(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	before, from := r.archive.DirectoryHours(today)
	db := r.db
	roots := seedTrees(t, db, 4, callShape)
	for i, c := range []struct {
		hour  int
		named bool
	}{
		{before - 1, true},
		{before, false},
		{from - 1, false},
		{from, true},
	} {
		_, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET created_hour=? WHERE root_flow_id=?", c.hour, roots[i])
		assert.NoError(err)
		moveInto(t, r, roots[i], today)
		var named int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable+" d JOIN "+
			migrations.FlowsTable(today)+" f ON f.flow_id=d.flow_id WHERE f.root_flow_id=?", roots[i]).Scan(&named))
		if c.named {
			assert.Equal(2, named, "hour %d (bounds %d, %d) is named", c.hour, before, from)
		} else {
			assert.Equal(0, named, "hour %d (bounds %d, %d) is not named", c.hour, before, from)
		}
	}
}

// TestMove_TheSizingSeeksEachFlowsSteps pins the sizing count's join on PostgreSQL to a nested loop, each flow's steps
// a seek: with nested loops priced out, a plain join is planned as a hash join over every step in the table. Measured
// under load on a steps table of 121k live and 835k dead rows: 84 ms to count a tree of 10 rows that way, against 0.6
// ms by the seek.
func TestMove_TheSizingSeeksEachFlowsSteps(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	if r.db.DriverName() != "pgx" {
		t.Skip("the plan pinned here is PostgreSQL's")
	}
	stmt := treesSizesUpToSQL(r.db.DriverName(), live, []int{1, 2}, []int{1001, 4097})
	var plan strings.Builder
	errRollback := errors.New("rollback")
	err := r.db.Transact(ctx, func(tx *sequel.Tx) error {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_nestloop = off"); err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, "EXPLAIN "+stmt)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				return err
			}
			plan.WriteString(line + "\n")
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return errRollback
	})
	assert.True(errors.Is(err, errRollback), "%v", err)
	assert.Contains(plan.String(), "Nested Loop")
	assert.NotContains(plan.String(), "Hash")
	assert.NotContains(plan.String(), "Merge Join")
}

// TestMove_TheLockRechecksEveryRoot pins the recheck under the root lock: roots a batch selected and sized are moved
// only if they are still due when locked. A root that has since got an archive attempt - the staged path's - one
// stamped for deletion, and one no longer due are left whole; the rest move.
func TestMove_TheLockRechecksEveryRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	roots := seedTrees(t, db, 4, callShape)
	ascendingCloses(t, r, roots)
	cut, via, err := r.fast().cut(ctx, db, r.settings, roots, true)
	assert.Must(assert.NoError(err))
	assert.Equal(roots, cut, "the batch takes all four")
	assert.Equal(routeMove, via)

	attempted, stamped, early, ok := roots[0], roots[1], roots[2], roots[3]
	at, arg := database.SQLTime(db.DriverName(), time.Now().Add(-time.Minute))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+at+" WHERE flow_id=?", arg, attempted)
	assert.NoError(err)
	stampTree(t, db, stamped)
	backdate(t, r, early, -time.Hour) // closed in the future: not due by any grace

	day := &filingDay{}
	assert.Must(assert.NoError(refreshDay(ctx, db, r.archive, true, day)))
	due, dueArgs := closedDueSQL(0)
	moved, err := move(ctx, db, r.seams, cut, due, dueArgs, day)
	assert.NoError(err)
	assert.Equal(2, moved.flows, "only the root still due moved: its root and its child")
	for _, id := range []int{attempted, stamped, early} {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "root %d left whole", id)
	}
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", ok)
	assert.Zero(flows)
}
