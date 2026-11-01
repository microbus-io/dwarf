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
	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
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

// archiveEngine starts an engine whose ended roots are due at once and whose own reaper stays out of the way, so a
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

// TestArchiving_EndedTreeMovesToTodaysTables pins the archiver: once its root has ended, a whole tree - the root, its
// subgraph descendant and every step - leaves the live tables for today's archive tables in one pass, with its ids
// and its outcome as they were.
func TestArchiving_EndedTreeMovesToTodaysTables(t *testing.T) {
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
	assert.NoError(err)
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

// TestArchiving_RetentionZeroDeletes pins that a retention of 0 deletes an ended tree rather than archiving it.
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

// TestArchiving_OnlyEndedRootsPastTheGraceLeave pins what the archiver leaves alone: a flow that has not ended (an
// interrupted one waits for a human), and an ended one inside its grace. A DeleteOnCompletion root is deleted, not
// archived.
func TestArchiving_OnlyEndedRootsPastTheGraceLeave(t *testing.T) {
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
	assert.Equal(1, flows, "an interrupted flow has not ended")
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
	assert.NoError(err)
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
			assert.NoError(rows.Scan(&id))
			ids = append(ids, id)
		}
		return ids
	}
	var both []int
	for _, ordinal := range []int{0, 1} {
		for _, id := range selectWith(e.endedDue(time.Hour), ordinal) {
			assert.Equal(ordinal, id%2, "a fresh root is selected only by its own class")
			both = append(both, id)
		}
	}
	slices.Sort(both)
	assert.Equal(roots, both, "and the two classes cover every root once")
	assert.True(slices.ContainsFunc(roots, func(id int) bool { return id%2 == 0 }) &&
		slices.ContainsFunc(roots, func(id int) bool { return id%2 == 1 }), "both classes hold roots: %v", roots)
	all := selectWith(e.endedDue(0), 0)
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
// a row budget that cuts them further, one pass still moves every ended tree, consuming one scan's candidates
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

// TestArchiving_DeleteOfATreeArchivedMeanwhileIs404 pins Delete against the reaper: a tree archived between Delete's
// read and its stamp is reported not found, never as deleted - it sits in the archive, which Delete cannot reach.
func TestArchiving_DeleteOfATreeArchivedMeanwhileIs404(t *testing.T) {
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
	e.reapDueFlows(ctx)
	e.seams.Resume(engineseams.CheckpointBeforeDeleteStamp)
	assert.Equal(http.StatusNotFound, errors.StatusCode(<-deleted))
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

// endedRoots creates n ended trees and returns their roots.
func endedRoots(t *testing.T, e *Engine, n int) []int {
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

// TestArchiving_NoEndedTreeMovesWhileTodayCannotBeMade pins that while today's or a day ahead's tables cannot be made, the
// ended trees wait in the live tables, neither archived nor deleted, while the stamped ones are still deleted: they
// need no archive. Once that has lasted the failure grace, the ended trees are deleted unarchived.
func TestArchiving_NoEndedTreeMovesWhileTodayCannotBeMade(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	roots := endedRoots(t, e, 3)
	stamped := endedRoots(t, e, 1)[0]
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	// Stamped a second ago, so it is due whatever the clock's millisecond.
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, updated_at=DATE_ADD_MILLIS(NOW_UTC(), -1000)"+
		" WHERE flow_id=?", stamped)
	assert.NoError(err)
	// Tomorrow's tables dropped, its migrations still recorded: it cannot be made again. The shard's archive is
	// replaced by a fresh one, as after a restart: one that has seen the days whole reads only the clock until the
	// day changes.
	for _, table := range []string{migrations.FlowsTable(today + 1), migrations.StepsTable(today + 1)} {
		_, err = db.ExecContext(ctx, "DROP TABLE "+table)
		assert.NoError(err)
	}
	set := e.archives.Load()
	e.archives.Store(&archiveSet{dayLength: set.dayLength, byShard: map[int]*archiver.Archive{
		1: archiver.New(db, set.dayLength, migrations.Archive()),
	}})

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
// ended kind from its pass.
func TestArchiving_AFailedDeletionScanDoesNotHoldBackArchiving(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	roots := endedRoots(t, e, 2)
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
	roots := endedRoots(t, e, 6)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.NoError(err)
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
	roots := endedRoots(t, e, 3)
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
	more := endedRoots(t, e, 2)
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
			assert.NoError(e.Startup(ctx))
			e.seams.InjectN(fault, 100000)

			first := endedRoots(t, e, 2)
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
				more := endedRoots(t, e, 1)
				e.reapDueFlows(ctx)
				flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", more[0])
				assert.Equal(0, flows, "tree %d deleted: the deletions go on", more[0])
			}
			assert.Equal(1, logs.count("Reaper: archiving ended trees failed"), "the failure is logged once")
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
	assert.False(e.archiveFailed(stopped, 1, "archiving ended trees", errors.New("stopped")))
	assert.Len(e.archiveFailures.byID, 0, "a stop records no failure")

	assert.False(e.archiveFailed(t.Context(), 1, "archiving ended trees", errors.New("failed")))
	assert.Len(e.archiveFailures.byID, 1)
	assert.NoError(e.Shutdown(t.Context()))
	assert.NoError(e.Startup(t.Context()))
	assert.Len(e.archiveFailures.byID, 0, "a restart forgets the failures")
}
