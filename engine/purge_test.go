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
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// archivedTreeKeys archives the tree rooted at rootID into today's tables as if it had been created three days ago -
// so the directory names its flows - and returns the root's key and its subgraph child's.
func archivedTreeKeys(t *testing.T, e *Engine, rootID, today int) (rootKey, childKey string) {
	t.Helper()
	ctx := t.Context()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	if _, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET created_hour=created_hour-72 WHERE root_flow_id=?", rootID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE dwarf_steps SET flow_created_hour=flow_created_hour-72"+
		" WHERE flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?)", rootID); err != nil {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT flow_id, flow_token, created_hour FROM dwarf_flows WHERE root_flow_id=?", rootID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id, hour int
		var token string
		if err := rows.Scan(&id, &token, &hour); err != nil {
			t.Fatal(err)
		}
		if id == rootID {
			rootKey = keys.NewFlowKey(1, hour, id, token)
		} else {
			childKey = keys.NewFlowKey(1, hour, id, token)
		}
	}
	rows.Close()
	moveInto(t, e, rootID, today)
	return rootKey, childKey
}

// namedFlows counts the directory's rows for the flows of the tree rooted at rootID, archived in day.
func namedFlows(t *testing.T, e *Engine, day, rootID int) int {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	var n int
	err = db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+archiver.DirectoryTable+" WHERE flow_id=? OR flow_id IN"+
		" (SELECT flow_id FROM "+migrations.FlowsTable(day)+" WHERE root_flow_id=?)", rootID, rootID).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestDelete_AnArchivedFlowIsDeletedInItsDay pins Delete of an archived flow: its whole tree - flows, steps and the
// directory's rows for them - is deleted at once from its day's tables, the flow then reads as not found, and a
// second Delete is a 404. A subgraph child's key is a 400 there too, and a wrong token a 404, the tree untouched.
func TestDelete_AnArchivedFlowIsDeletedInItsDay(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	root := closedRoots(t, e, 1)[0]
	rootKey, childKey := archivedTreeKeys(t, e, root, today)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(2, namedFlows(t, e, today, root), "the directory names the root and its child")

	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.Delete(ctx, childKey)), "a subgraph child's key")
	shard, hour, id, _, err := keys.ParseFlowKey(rootKey)
	assert.NoError(err)
	assert.Equal(http.StatusNotFound, errors.StatusCode(e.Delete(ctx, keys.NewFlowKey(shard, hour, id, "0000000000000000"))),
		"a wrong token")
	flows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(2, flows, "untouched")

	assert.NoError(e.Delete(ctx, rootKey))
	flows, steps := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Zero(flows, "the tree's flows")
	assert.Zero(steps, "and steps")
	assert.Zero(namedFlows(t, e, today, root), "and the directory's rows")
	_, err = e.Snapshot(ctx, rootKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	assert.Equal(http.StatusNotFound, errors.StatusCode(e.Delete(ctx, rootKey)), "deleted already")
}

// TestPurge_OldestArchivedFirstAndOnlyTheArchive pins Purge's order and its done flag over three archive days: the
// archived roots oldest archived first, each tree deleted at once in its day - a chunk of one root to a statement here
// - done on the call that took less than its limit; and the roots closed but not archived yet left as they are, for a
// later call once they are archived.
func TestPurge_OldestArchivedFirstAndOnlyTheArchive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	e.purgeChunk = 1
	a := e.archives.Load().byShard[1]
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-2))
	assert.NoError(makeArchiveDay(t, e, today-1))
	assert.NoError(a.Refresh(ctx))
	roots := closedRoots(t, e, 7)
	moveInto(t, e, roots[0], today-2)
	moveInto(t, e, roots[1], today-1)
	moveInto(t, e, roots[2], today-1)
	moveInto(t, e, roots[3], today)
	stampArchived(t, e, today-2, roots[0], a.DayEnds(today-2).Add(-time.Hour))
	stampArchived(t, e, today-1, roots[1], a.DayEnds(today-1).Add(-2*time.Hour))
	stampArchived(t, e, today-1, roots[2], a.DayEnds(today-1).Add(-time.Hour))
	// roots[4..6] stay live: closed, not archived yet.
	if now, _ := a.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	directory := func() int {
		var n int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable).Scan(&n))
		return n
	}
	assert.Equal(6, directory(), "the three trees archived into a day before their creation day are named")
	window := workflow.Query{FromTime: time.Now().Add(-3 * 24 * time.Hour), ToTime: time.Now().Add(time.Hour)}
	inDay := func(day, root int) int {
		flows, _ := treeCount(t, e, migrations.FlowsTable(day), migrations.StepsTable(day), root)
		return flows
	}
	marked := func(root int) bool {
		sh, err := e.db.Shard(1)
		assert.Must(assert.NoError(err))
		db := sh.DB()
		var after int
		assert.NoError(db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&after))
		return after > 0
	}

	q := window
	q.Limit = 3
	removed, done, err := e.Purge(ctx, q)
	assert.NoError(err)
	assert.Equal(3, removed)
	assert.False(done, "the archive may hold more")
	assert.Zero(inDay(today-2, roots[0]), "the oldest archived, deleted in its day")
	assert.Zero(inDay(today-1, roots[1]), "the next two, both in one day: two chunks of one")
	assert.Zero(inDay(today-1, roots[2]))
	assert.Equal(2, inDay(today, roots[3]), "not yet")
	assert.Zero(directory(), "their directory rows went with them")

	q.Limit = 2
	removed, done, err = e.Purge(ctx, q)
	assert.NoError(err)
	assert.Equal(1, removed, "the last archived root")
	assert.True(done, "the archive held fewer than the limit")
	assert.Zero(inDay(today, roots[3]))
	for _, root := range roots[4:] {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
		assert.Equal(2, flows, "root %d closed but not archived: left in the live tables", root)
		assert.False(marked(root), "and not marked")
	}

	// Once they are archived, the next call takes them.
	e.reapDueFlows(ctx)
	q.Limit = 10
	removed, done, err = e.Purge(ctx, q)
	assert.NoError(err)
	assert.Equal(3, removed)
	assert.True(done)
}

// TestPurge_TheRefusals pins what Purge refuses with a 400 beside a missing window or Limit (TestPurge_EveryListFilterNarrows):
// IncludeSubgraphs, a status that has not closed, and a Limit over the most a call takes.
func TestPurge_TheRefusals(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	window := workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: time.Now().Add(time.Hour), Limit: 10}
	for name, q := range map[string]workflow.Query{
		"subgraphs":  {FromTime: window.FromTime, ToTime: window.ToTime, Limit: 10, IncludeSubgraphs: true},
		"running":    {FromTime: window.FromTime, ToTime: window.ToTime, Limit: 10, Status: workflow.StatusRunning},
		"limit over": {FromTime: window.FromTime, ToTime: window.ToTime, Limit: maxListLimit + 1},
	} {
		_, _, err := e.Purge(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), name)
	}
}

// TestLifecycle_AnArchivedFlowAnswersAsAClosedOne pins Resume, Terminate and Cancel on an archived flow, found in the
// archive when the live read misses: the answer any closed flow gets - Resume a 409, Terminate and Cancel a no-op that
// leaves the outcome as it was - a subgraph child's key a 400, and an unknown token a 404.
func TestLifecycle_AnArchivedFlowAnswersAsAClosedOne(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	root := closedRoots(t, e, 1)[0]
	rootKey, childKey := archivedTreeKeys(t, e, root, today)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
	assert.Zero(flows, "archived")

	assert.Equal(http.StatusConflict, errors.StatusCode(e.Resume(ctx, rootKey, nil)), "Resume: not interrupted")
	assert.NoError(e.Terminate(ctx, rootKey, "late"), "Terminate: a no-op")
	assert.NoError(e.Cancel(ctx, rootKey, "late"), "Cancel: a no-op")
	outcome, err := e.Snapshot(ctx, rootKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, "the outcome as it was")

	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.Resume(ctx, childKey, nil)))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.Terminate(ctx, childKey, "x")))
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.Cancel(ctx, childKey, "x")))

	shard, hour, id, _, err := keys.ParseFlowKey(rootKey)
	assert.NoError(err)
	wrong := keys.NewFlowKey(shard, hour, id, "0000000000000000")
	assert.Equal(http.StatusNotFound, errors.StatusCode(e.Resume(ctx, wrong, nil)))
	assert.Equal(http.StatusNotFound, errors.StatusCode(e.Terminate(ctx, wrong, "x")))
	assert.Equal(http.StatusNotFound, errors.StatusCode(e.Cancel(ctx, wrong, "x")))
}

// TestDelete_AnArchivedTreeWaitsForAForkOfIt pins that a Delete of an archived tree and a Fork of it serialize on the
// root: while Fork holds the archived root, the Delete waits, so the Fork clones a whole tree; once the Fork has
// committed, the Delete deletes the source and the fork stands on its own.
func TestDelete_AnArchivedTreeWaitsForAForkOfIt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	root := closedRoots(t, e, 1)[0]
	rootKey, _ := archivedTreeKeys(t, e, root, today)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	history, err := e.History(ctx, rootKey)
	if !assert.NoError(err) || !assert.True(len(history) > 0) {
		return
	}

	e.seams.Break(engineseams.CheckpointForkRootLocked)
	type result struct {
		fk  string
		err error
	}
	forked := make(chan result, 1)
	go func() {
		fk, err := e.Fork(ctx, history[0].StepKey, nil)
		forked <- result{fk, err}
	}()
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointForkRootLocked, 10*time.Second))
	deleted := make(chan error, 1)
	go func() { deleted <- e.Delete(ctx, rootKey) }()
	// Long enough for a Delete that is not held off to delete the tree.
	time.Sleep(500 * time.Millisecond)
	select {
	case err := <-deleted:
		t.Errorf("the Delete finished (%v) while Fork held the archived root", err)
	default:
	}
	e.seams.Resume(engineseams.CheckpointForkRootLocked)
	r := <-forked
	assert.NoError(r.err)
	assert.NoError(<-deleted)
	flows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Zero(flows, "the source deleted after the fork")
	if r.err == nil {
		out, err := enginetest.BoundedAwait(t, e, r.fk)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, out.Status, "the fork runs whole")
	}
}

// TestPurge_Done pins Purge's done flag: true only when the archive had fewer than the limit and every root taken was
// removed.
func TestPurge_Done(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.True(purgeDone(10, 3, 3), "short of the limit, all removed")
	assert.False(purgeDone(10, 10, 10), "the archive filled the limit")
	assert.False(purgeDone(10, 3, 2), "a root went to a racing Delete first")
	assert.True(purgeDone(10, 0, 0), "nothing matched")
}
