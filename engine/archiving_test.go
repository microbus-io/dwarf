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
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// treeCount returns how many flows and steps of the tree rooted at rootID the two tables hold.
func treeCount(t *testing.T, e *Engine, flowsTable, stepsTable string, rootID int) (flows, steps int) {
	t.Helper()
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	flows, steps = -1, -1
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+flowsTable+" WHERE root_flow_id=?", rootID).Scan(&flows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+stepsTable+" WHERE flow_id IN (SELECT flow_id FROM "+
		flowsTable+" WHERE root_flow_id=?)", rootID).Scan(&steps); err != nil {
		t.Fatal(err)
	}
	return flows, steps
}

// archiveEngine starts an engine whose closed roots are due at once and whose own reaper stays out of the way, so a
// test drives each pass itself.
func archiveEngine(t *testing.T, mux *hosts.Mux, retention int) *Engine {
	t.Helper()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(mux)
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	if err := e.SetRetention(retention); err != nil {
		t.Fatal(err)
	}
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The archive loop passes once as it starts, on its own goroutine; waited out here, it cannot land in the middle of
	// a test and read the catalog - learning a day the test made - at a moment the test does not choose.
	enginetest.AwaitVisitCount(t, e.seams, 1, 10*time.Second, engineseams.CheckpointArchivePassDone)
	return e
}

// treeMux serves a parent graph whose one task calls a one-task child graph, so a root carries a subgraph descendant.
func treeMux() *hosts.Mux {
	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "arch/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("arch/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("Work", "arch/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("arch/child", child)
	mux.HandleTask("arch/call", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("arch/child", map[string]any{"n": 1}, &out)
		if yield || err != nil {
			return err
		}
		f.Set("got", out["done"])
		return nil
	})
	mux.HandleTask("arch/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})
	// arch/solo is the child's graph run as a root: a tree of one flow, so consecutive roots take consecutive ids.
	mux.HandleGraph("arch/solo", child)
	return mux
}

// TestArchiving_ClosedTreeMovesToTodaysTables pins the archiver: once its root has closed, a whole tree - the root, its
// subgraph descendant and every step - leaves the live tables for today's archive tables in one pass, with its ids
// and its outcome as they were.
func TestArchiving_ClosedTreeMovesToTodaysTables(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)

	liveFlows, liveSteps := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Equal(2, liveFlows, "the root and its subgraph child")
	assert.True(liveSteps >= 2)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	var finalState string
	assert.NoError(db.QueryRowContext(ctx, "SELECT final_state FROM dwarf_flows WHERE flow_id=?", rootID).Scan(&finalState))

	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	e.reapDueFlows(ctx)

	flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Equal(0, flows)
	assert.Equal(0, steps)
	// The pass may straddle midnight; the day it read before is the one it archived into unless it did.
	archFlows, archSteps := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), rootID)
	if archFlows == 0 {
		archFlows, archSteps = treeCount(t, e, migrations.FlowsTable(today+1), migrations.StepsTable(today+1), rootID)
		today++
	}
	assert.Equal(liveFlows, archFlows)
	assert.Equal(liveSteps, archSteps)
	var archived string
	assert.NoError(db.QueryRowContext(ctx, "SELECT final_state FROM "+migrations.FlowsTable(today)+" WHERE flow_id=?", rootID).Scan(&archived))
	assert.Equal(finalState, archived)
}

// TestArchiving_RetentionZeroDeletes pins that a retention of 0 deletes a closed tree rather than archiving it.
func TestArchiving_RetentionZeroDeletes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 0)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)

	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Equal(0, flows)
	assert.Equal(0, steps)
	archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), rootID)
	assert.Equal(0, archFlows, "a retention of 0 archives nothing")
}

// TestArchiving_OnlyClosedRootsPastTheGraceLeave pins what the archiver leaves alone: a flow that has not closed (an
// interrupted one waits for a human), and a closed one inside its grace. A DeleteOnCompletion root is deleted, not
// archived.
func TestArchiving_OnlyClosedRootsPastTheGraceLeave(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	mux := treeMux()
	ask := workflow.NewGraph("Ask")
	ask.SetURL("Ask", "arch/ask")
	ask.AddTransition("Ask", workflow.End)
	mux.HandleGraph("arch/ask", ask)
	mux.HandleTask("arch/ask", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"q": "ok?"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	e := archiveEngine(t, mux, 30)
	e.deletionGrace = time.Millisecond // 0 would stamp "keep"

	asked, err := e.Create(ctx, "arch/ask", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, asked, workflow.StatusInterrupted, time.Minute)
	disposable, err := e.Create(ctx, "arch/parent", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, disposable, workflow.StatusCompleted, time.Minute)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)

	// The stamp is due a millisecond after completion on the database clock, so a pass may come too soon.
	_, _, disposableID, _, _ := keys.ParseFlowKey(disposable)
	deadline := time.Now().Add(5 * time.Second * enginetest.TimeoutScale())
	flows := -1
	for flows != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond) // the grace is wall-clock time
		e.reapDueFlows(ctx)
		flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", disposableID)
	}
	assert.Equal(0, flows, "a DeleteOnCompletion root is deleted")
	_, _, askedID, _, _ := keys.ParseFlowKey(asked)
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", askedID)
	assert.Equal(1, flows, "an interrupted flow has not closed")
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now == today {
		archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), disposableID)
		assert.Equal(0, archFlows, "and not archived")
	}

	e.archiveGrace = time.Hour
	fresh, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fresh, workflow.StatusCompleted, time.Minute)
	e.reapDueFlows(ctx)
	_, _, freshID, _, _ := keys.ParseFlowKey(fresh)
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", freshID)
	assert.Equal(2, flows, "a flow inside its grace stays live")
}

// TestArchiving_BatchesAreCappedByRows pins how a batch is cut: the leading trees whose rows fit the budget, and
// always the first however large.
func TestArchiving_BatchesAreCappedByRows(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	size := map[int]int{1: 3, 2: 4, 3: 5, 4: 100}
	assert.Equal([]int{1, 2}, withinRows([]int{1, 2, 3, 4}, size, 8))
	assert.Equal([]int{1, 2, 3}, withinRows([]int{1, 2, 3, 4}, size, 12))
	assert.Equal([]int{4}, withinRows([]int{4, 1}, size, 8), "a tree over the budget goes alone")
	assert.Equal([]int{1, 2, 3, 4}, withinRows([]int{1, 2, 3, 4}, size, 1000))
}

// TestArchiving_ResidencySplitsTheDueRoots pins the residency clause the scans take on a shard with two dispatchers:
// each class selects only its own roots while they are fresh, and either takes the other's once they are overdue by
// the steal grace. A solo test engine never partitions, so the clause is built here as reapResidency builds it, and the
// roots are trees of one flow so that both classes hold some.
func TestArchiving_ResidencySplitsTheDueRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	var roots []int
	var firstKey string
	for i := range 4 {
		fk, err := e.Create(ctx, "arch/solo", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		roots = append(roots, id)
		if i == 0 {
			firstKey = fk
		}
	}
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	selectWith := func(kind reapKind, ordinal int) []int {
		residency, residencyArgs, count, _ := residencyClause(2, ordinal, true, kind)
		stmt, args := kind.selectDue(residency, residencyArgs, reapFetch(count, e.reapBatch))
		rows, err := db.QueryContext(ctx, stmt, args...)
		if !assert.NoError(err) {
			return nil
		}
		defer rows.Close()
		var ids []int
		for rows.Next() {
			var id int
			var ageMs float64
			assert.NoError(rows.Scan(&id, &ageMs))
			ids = append(ids, id)
		}
		return ids
	}
	var both []int
	for _, ordinal := range []int{0, 1} {
		for _, id := range selectWith(e.closedDue(time.Hour), ordinal) {
			assert.Equal(ordinal, id%2, "a fresh root is selected only by its own class")
			both = append(both, id)
		}
	}
	slices.Sort(both)
	assert.Equal(roots, both, "and the two classes cover every root once")
	assert.True(slices.ContainsFunc(roots, func(id int) bool { return id%2 == 0 }) &&
		slices.ContainsFunc(roots, func(id int) bool { return id%2 == 1 }), "both classes hold roots: %v", roots)
	all := selectWith(e.closedDue(0), 0)
	for _, id := range roots {
		assert.True(slices.Contains(all, id), "an overdue root %d is selected by any class", id)
	}
	// The deletion scan takes the same clause: a stamped root is due a millisecond after its stamp, and then
	// selected by its own class while fresh, by any once overdue.
	assert.NoError(e.Delete(ctx, firstKey))
	deadline := time.Now().Add(5 * time.Second * enginetest.TimeoutScale())
	for !slices.Contains(selectWith(e.deletionDue(0), 1-roots[0]%2), roots[0]) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	assert.True(slices.Contains(selectWith(e.deletionDue(0), 1-roots[0]%2), roots[0]), "an overdue stamped root, any class")
	assert.False(slices.Contains(selectWith(e.deletionDue(time.Hour), 1-roots[0]%2), roots[0]), "not another class's while fresh")
	assert.True(slices.Contains(selectWith(e.deletionDue(time.Hour), roots[0]%2), roots[0]), "its own class's")
}

// TestArchiving_ABatchTakesItsOwnClassFirst pins the fill order that keeps two reapers apart once a backlog is older
// than the steal grace and the scan admits both classes: the own class first, a peer's roots only for the room left,
// and a fetch large enough that the own class is rarely short.
func TestArchiving_ABatchTakesItsOwnClassFirst(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.Equal([]int{2, 4, 6, 1}, ownFirst([]int{1, 2, 3, 4, 5, 6}, 2, 0, 4))
	assert.Equal([]int{1, 3, 5, 2}, ownFirst([]int{1, 2, 3, 4, 5, 6}, 2, 1, 4))
	assert.Equal([]int{1, 2, 3}, ownFirst([]int{1, 2, 3}, 0, 0, 4), "no pair: every root is its own")
	assert.Equal(512, reapFetch(0, 512))
	assert.Equal(4*512, reapFetch(2, 512), "twice the classes' share")
}

// TestArchiving_APassDrainsInSeveralBatches pins the loop of one pass: with batches far smaller than the due set, and
// a row budget that cuts them further, one pass still moves every closed tree, consuming one scan's candidates
// across batches and scanning again when they run out.
func TestArchiving_APassDrainsInSeveralBatches(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	e.reapBatch = 2
	e.reapBatchRows = 3
	var roots []int
	for range 7 {
		fk, err := e.Create(ctx, "arch/parent", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		roots = append(roots, id)
	}
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for _, id := range roots {
		flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(0, flows+steps, "tree %d left in the live tables", id)
		archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(2, archFlows, "tree %d not archived whole", id)
	}
}

// TestArchiving_DeleteOfATreeArchivedMeanwhileDeletesItThere pins Delete against the reaper: a tree archived between
// Delete's read and its stamp is deleted from the archive instead, never reported deleted while it stays there.
func TestArchiving_DeleteOfATreeArchivedMeanwhileDeletesItThere(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)

	e.seams.Break(engineseams.CheckpointBeforeDeleteStamp)
	deleted := make(chan error, 1)
	go func() { deleted <- e.Delete(ctx, fk) }()
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeDeleteStamp, 10*time.Second))
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	e.reapDueFlows(ctx)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	flows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), rootID)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); flows == 0 && now != today {
		e.seams.Resume(engineseams.CheckpointBeforeDeleteStamp)
		t.Skip("midnight passed during the pass")
	}
	assert.Equal(2, flows, "the reaper archived the tree while the Delete was held")
	e.seams.Resume(engineseams.CheckpointBeforeDeleteStamp)
	assert.NoError(<-deleted)
	flows, steps := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), rootID)
	assert.Zero(flows, "deleted from the archive")
	assert.Zero(steps)
	_, err = e.Snapshot(ctx, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}

// TestArchiving_ABatchThatMeetsContentionWaitsForTheNextPass pins that lock contention past Transact's retries is an
// archive failure like any other: within the grace the batch stays live, neither archived nor deleted, and the next
// pass archives it. (TestArchiving_ALastingFaultDeletesAndLogsOnce pins contention that outlasts the grace.)
func TestArchiving_ABatchThatMeetsContentionWaitsForTheNextPass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	var roots []int
	for range 3 {
		fk, err := e.Create(ctx, "arch/parent", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		roots = append(roots, id)
	}
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)

	// Contention past Transact's retries: the batch stays live, neither archived nor deleted.
	e.seams.InjectN(engineseams.FaultReapContention, 1000)
	e.reapDueFlows(ctx)
	e.seams.Withdraw(engineseams.FaultReapContention)
	for _, id := range roots {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d stays live", id)
	}

	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for _, id := range roots {
		archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(2, archFlows, "tree %d archived on the next pass", id)
	}
}

// closedRoots creates n closed trees and returns their roots.
func closedRoots(t *testing.T, e *Engine, n int) []int {
	t.Helper()
	assert := testarossa.For(t)
	var roots []int
	for range n {
		fk, err := e.Create(t.Context(), "arch/parent", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		roots = append(roots, id)
	}
	return roots
}

// TestArchiving_NoClosedTreeMovesWhileTomorrowIsNotWhole pins that while tomorrow's tables are missing - so today, whose
// own tables are there, must not be written, or a locate would leave out a day that holds the flow - the closed trees
// wait in the live tables, neither archived nor deleted, while the stamped ones are still deleted: they need no
// archive. Once that has lasted the failure grace, the closed trees are deleted unarchived.
func TestArchiving_NoClosedTreeMovesWhileTomorrowIsNotWhole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	roots := closedRoots(t, e, 3)
	stamped := closedRoots(t, e, 1)[0]
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	// Stamped a second ago, so it is due whatever the clock's millisecond.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, updated_at=DATE_ADD_MILLIS(NOW_UTC(), -1000)"+
		" WHERE flow_id=?", stamped)
	assert.NoError(err)
	// Tomorrow's tables dropped, its migrations still recorded: it cannot be made again, and the archive's record,
	// read again, no longer holds it - today is whole, tomorrow is not.
	for _, table := range []string{migrations.FlowsTable(today + 1), migrations.StepsTable(today + 1)} {
		_, err = db.ExecContext(ctx, "DROP TABLE "+table)
		assert.NoError(err)
	}
	assert.NoError(e.archives.Load().byShard[1].Refresh(ctx))
	assert.True(e.archives.Load().byShard[1].Whole(today), "today is there")

	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for _, id := range roots {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d stays live", id)
		archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(0, archFlows, "tree %d is not archived", id)
	}
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", stamped)
	assert.Equal(0, flows, "the stamped tree is deleted")

	e.archiveFailureGrace = 0
	e.reapDueFlows(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(0, flows, "tree %d is deleted once the grace has passed", id)
	}
}

// TestArchiving_AFailedDeletionScanDoesNotHoldBackArchiving pins that the stamped kind failing does not keep the
// closed kind from its pass.
func TestArchiving_AFailedDeletionScanDoesNotHoldBackArchiving(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	roots := closedRoots(t, e, 2)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)

	// The first scan of the pass is the stamped kind's.
	e.seams.InjectN(engineseams.FaultReapSelectErr, 1)
	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for _, id := range roots {
		archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(2, archFlows, "tree %d archived", id)
	}
}

// TestArchiving_ABatchThatCannotBeArchivedIsDeletedAfterTheGrace pins the reaper's fallback: a batch whose move fails -
// here a primary-key collision in the archive, as reissued ids would give - waits in the live tables on the first
// failure, and is deleted without archiving once archiving has been failing for the grace, so a lasting archive fault
// cannot fill the live tables. The flows' work is recorded already; what is lost is their archive.
func TestArchiving_ABatchThatCannotBeArchivedIsDeletedAfterTheGrace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	e.archiveFailureGrace = 0
	roots := closedRoots(t, e, 6)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx, "INSERT INTO "+migrations.FlowsTable(today)+" SELECT * FROM dwarf_flows WHERE flow_id IN ("+
		intCSV(roots[:3])+")")
	assert.NoError(err)

	e.reapDueFlows(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d waits on the first failure", id)
	}
	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for i, id := range roots {
		flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(0, flows+steps, "tree %d left the live tables", id)
		if i >= 3 {
			archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), id)
			assert.Equal(0, archFlows, "tree %d went with its batch, unarchived", id)
		}
	}
}

// TestArchiving_AFailureWaitsForTheGraceAndASuccessClearsIt pins the failure grace: a move failing on every pass deletes
// nothing while the grace lasts, a pass that archives ends the failure, and a failure after that starts a grace of
// its own rather than deleting at once.
func TestArchiving_AFailureWaitsForTheGraceAndASuccessClearsIt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	e.archiveFailureGrace = time.Hour
	roots := closedRoots(t, e, 3)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)

	e.seams.InjectN(engineseams.FaultReapArchiveErr, 1000)
	e.reapDueFlows(ctx)
	e.reapDueFlows(ctx)
	e.seams.Withdraw(engineseams.FaultReapArchiveErr)
	for _, id := range roots {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d waits within the grace", id)
	}

	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	for _, id := range roots {
		archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(2, archFlows, "tree %d archived once the move works", id)
	}

	// The grace is over, but the success cleared the failure: the next failure waits again.
	e.archiveFailureGrace = 0
	more := closedRoots(t, e, 2)
	e.seams.InjectN(engineseams.FaultReapArchiveErr, 1)
	e.reapDueFlows(ctx)
	for _, id := range more {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d waits: a new failure is a first failure", id)
	}
}

// logCounter is a slog.Handler that counts the records carrying each message.
type logCounter struct {
	mu   sync.Mutex
	seen map[string]int
}

func (l *logCounter) Enabled(context.Context, slog.Level) bool { return true }
func (l *logCounter) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logCounter) WithGroup(string) slog.Handler            { return l }
func (l *logCounter) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	l.seen[r.Message]++
	l.mu.Unlock()
	return nil
}

// count returns how many records carried a message starting with prefix.
func (l *logCounter) count(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for msg, c := range l.seen {
		if strings.HasPrefix(msg, prefix) {
			n += c
		}
	}
	return n
}

// TestArchiving_ALastingFaultDeletesAndLogsOnce pins a fault that outlasts the grace, as a plain error and as lock
// contention: the first failure waits, every failure after the grace deletes, a pass that deleted does not count as
// one that archived (so the deletions go on rather than starting a new grace), and the failure and the start of the
// deletions are each logged once, not every pass.
func TestArchiving_ALastingFaultDeletesAndLogsOnce(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{engineseams.FaultReapArchiveErr, engineseams.FaultReapContention} {
		t.Run(fault, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			logs := &logCounter{seen: map[string]int{}}
			e := NewEngineUnderTest(t.Name())
			t.Cleanup(func() { e.Shutdown(context.Background()) })
			e.SetHost(treeMux())
			assert.NoError(e.SetLogger(slog.New(logs)))
			e.archiveGrace = 0
			e.reapInterval = time.Hour
			e.archiveFailureGrace = 0
			assert.Must(assert.NoError(e.Startup(ctx)))
			e.seams.InjectN(fault, 100000)

			first := closedRoots(t, e, 2)
			e.reapDueFlows(ctx)
			for _, id := range first {
				flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
				assert.Equal(2, flows, "tree %d waits on the first failure", id)
			}
			e.reapDueFlows(ctx)
			for _, id := range first {
				flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
				assert.Equal(0, flows, "tree %d deleted after the grace", id)
			}
			for range 3 {
				more := closedRoots(t, e, 1)
				e.reapDueFlows(ctx)
				flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", more[0])
				assert.Equal(0, flows, "tree %d deleted: the deletions go on", more[0])
			}
			assert.Equal(1, logs.count("Reaper: archiving closed trees failed"), "the failure is logged once")
			assert.Equal(1, logs.count("Reaper: archiving has failed for the grace"), "the deletions' start is logged once")
		})
	}
}

// TestArchiving_AStopIsNotAFailureAndARestartForgetsThem pins two edges of the failure timer: a failure met while the
// pass is being stopped is not recorded, and a restart starts with none.
func TestArchiving_AStopIsNotAFailureAndARestartForgetsThem(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := archiveEngine(t, treeMux(), 30)
	stopped, cancel := context.WithCancel(t.Context())
	cancel()
	assert.False(e.archiveFailed(stopped, 1, "archiving closed trees", errors.New("stopped")))
	assert.Len(e.archiveFailures.byID, 0, "a stop records no failure")

	assert.False(e.archiveFailed(t.Context(), 1, "archiving closed trees", errors.New("failed")))
	assert.Len(e.archiveFailures.byID, 1)
	assert.NoError(e.Shutdown(t.Context()))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.Len(e.archiveFailures.byID, 0, "a restart forgets the failures")
}

// TestArchiving_TheMoveNamesEveryFlowsColumn pins the move's column list against the flows table: every live column
// but archived_at, in table order, so a column added to dwarf_flows without being added to the list fails here rather
// than being dropped from the archive. The day's flows table has the live columns.
func TestArchiving_TheMoveNamesEveryFlowsColumn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	want := append(strings.Split(archivedFlowColumns, ", "), "archived_at")
	assert.Equal(want, columnNames(t, db, "dwarf_flows"))
	assert.Equal(want, columnNames(t, db, migrations.FlowsTable(today)))
}

// columnNames returns a table's columns in order.
func columnNames(t *testing.T, db *sequel.DB, table string) []string {
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

// TestArchiving_EveryArchivedFlowIsStampedWhenItWasArchived pins archived_at: a live flow holds the not-archived value,
// and every flow of an archived tree - the root and its subgraph child - the database's time of the move.
func TestArchiving_EveryArchivedFlowIsStampedWhenItWasArchived(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	root := closedRoots(t, e, 1)[0]
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	// Ended an hour ago, so a stamp copied from when it ended - rather than taken when it moved - is an hour old.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(updated_at, ?) WHERE root_flow_id=?",
		-time.Hour.Milliseconds(), root)
	assert.NoError(err)
	unset, unsetArg := sqlTime(db.DriverName(), time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC))
	var live int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id=? AND archived_at="+unset,
		root, unsetArg).Scan(&live))
	assert.Equal(2, live, "not archived")

	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	e.reapDueFlows(ctx)
	flows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); flows == 0 && now != today {
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

// TestArchiving_AMoveAfterItsDayEndedIsStampedWithinTheDay pins the archive time's clamp: a move into a day that has
// already ended - a pass that read the day before midnight and moves after it - stamps the day's last millisecond,
// never a time of the next day, so a day holds only rows archived on it.
func TestArchiving_AMoveAfterItsDayEndedIsStampedWithinTheDay(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	a := e.archives.Load().byShard[1]
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-1))
	root := closedRoots(t, e, 1)[0]
	moveInto(t, e, root, today-1)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	last, lastArg := sqlTime(db.DriverName(), a.DayEnds(today-1).Add(-time.Millisecond))
	var stamped int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+migrations.FlowsTable(today-1)+
		" WHERE root_flow_id=? AND archived_at="+last, root, lastArg).Scan(&stamped))
	assert.Equal(2, stamped, "the day's last millisecond")
}

// TestArchiving_NoTreeMovesInTheDaysLastMoments pins the reaper's quiet margin: a pass with less than the margin left
// in today archives nothing, and the trees go into the next day's tables at its first pass. Days of 16 seconds, so the
// margin is two, and the pass runs with one to two left: before the margin it would archive.
func TestArchiving_NoTreeMovesInTheDaysLastMoments(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(treeMux())
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	e.testArchiveDay = 16 * time.Second
	e.noReapQuiet = false
	assert.NoError(e.SetRetention(100))
	assert.Must(assert.NoError(e.Startup(ctx)))
	a := e.archives.Load().byShard[1]
	assert.Equal(2*time.Second, reapQuietMargin(a.DayLength()))
	assert.Equal(time.Minute, reapQuietMargin(24*time.Hour))
	assert.Zero(reapQuietMargin(8*time.Second), "a day too short for a margin")
	root := closedRoots(t, e, 1)[0]

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
	e.reapDueFlows(ctx)
	if now, _ := a.Today(ctx); now != day {
		t.Skip("the day ended during the pass")
	}
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
	assert.Equal(2, flows, "left in the live tables")

	dayDeadline := time.Now().Add(a.DayLength() + 10*time.Second*enginetest.TimeoutScale())
	for {
		if now, _ := a.Today(ctx); now > day {
			break
		}
		if time.Now().After(dayDeadline) {
			t.Fatal("the next day never came")
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.NoError(a.Refresh(ctx))
	e.reapDueFlows(ctx)
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
	assert.Zero(flows, "archived on the next day")
	flows, _ = treeCount(t, e, migrations.FlowsTable(day), migrations.StepsTable(day), root)
	assert.Zero(flows, "not into the day that ended")
}

// TestArchiving_AHeldRootIsSkippedNotWaitedFor pins that a batch leaves out a root another transaction holds - a long
// move by another replica's reaper, here a plain lock - and moves the rest at once. Waiting made the holder fail the
// whole batch on MySQL, whose lock wait gives up, and a batch that keeps failing is deleted unarchived after the
// failure grace. SQLite has one writer, and SQL Server waits without a timeout, so both keep the wait (lockRootsIn).
func TestArchiving_AHeldRootIsSkippedNotWaitedFor(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	if d := db.DriverName(); d != "pgx" && d != "mysql" {
		t.Skip("a held root is waited for on " + d)
	}
	roots := closedRoots(t, e, 2)
	held, other := roots[0], roots[1]

	locked := make(chan error, 1)
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- db.Transact(ctx, func(tx *sequel.Tx) error {
			_, err := lockRootsIn(ctx, tx, liveTables.flows, false, "flow_id=?", held)
			locked <- err
			<-release
			return err
		})
	}()
	assert.NoError(<-locked)

	a := e.archives.Load().byShard[1]
	today, err := a.Today(ctx)
	assert.NoError(err)
	bounded, cancel := context.WithTimeout(ctx, 5*time.Second*enginetest.TimeoutScale())
	defer cancel()
	moved, err := e.reapBatchOf(bounded, db, []int{held, other}, e.closedDue(0),
		&reapDay{day: today, stop: time.Now().Add(time.Hour), archive: a})
	close(release)
	assert.NoError(<-holderDone)
	if assert.NoError(err, "the batch did not wait on the held root") {
		assert.Equal(2, moved, "the other tree moved: its root and its child")
	}
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", held)
	assert.Equal(2, flows, "the held tree was left in the live tables")
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", other)
	assert.Zero(flows)

	moved, err = e.reapBatchOf(ctx, db, []int{held}, e.closedDue(0),
		&reapDay{day: today, stop: time.Now().Add(time.Hour), archive: a})
	assert.NoError(err)
	assert.Equal(2, moved, "the held tree moved once it was let go")
}
