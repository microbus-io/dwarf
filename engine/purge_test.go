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
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET created_hour=created_hour-72 WHERE root_flow_id=?", rootID); err != nil {
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
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
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
	root := endedRoots(t, e, 1)[0]
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

// TestPurge_OldestArchivedFirstThenTheLive pins Purge's order and its done flag over three archive days and the live
// tables: the archived roots oldest archived first, each tree deleted at once in its day - a chunk of one root to a
// statement here - then, with what the limit leaves, the live ended roots oldest ended first, marked for the reaper;
// done only on the call that took less than it could on both sides.
func TestPurge_OldestArchivedFirstThenTheLive(t *testing.T) {
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
	roots := endedRoots(t, e, 7)
	moveInto(t, e, roots[0], today-2)
	moveInto(t, e, roots[1], today-1)
	moveInto(t, e, roots[2], today-1)
	moveInto(t, e, roots[3], today)
	stampArchived(t, e, today-2, roots[0], a.DayEnds(today-2).Add(-time.Hour))
	stampArchived(t, e, today-1, roots[1], a.DayEnds(today-1).Add(-2*time.Hour))
	stampArchived(t, e, today-1, roots[2], a.DayEnds(today-1).Add(-time.Hour))
	// roots[4..6] stay live: ended, not archived yet, oldest ended first.
	if now, _ := a.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	db, err := e.db.Shard(1)
	assert.NoError(err)
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
		db, err := e.db.Shard(1)
		assert.NoError(err)
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
	assert.Equal(2, removed)
	assert.False(done, "the live side may hold more")
	assert.Zero(inDay(today, roots[3]))
	assert.True(marked(roots[4]), "the oldest live ended, marked for the reaper")
	assert.False(marked(roots[5]), "not yet")

	q.Limit = 10
	removed, done, err = e.Purge(ctx, q)
	assert.NoError(err)
	assert.Equal(2, removed, "the two live ones left")
	assert.True(done)
	assert.True(marked(roots[5]) && marked(roots[6]))

	removed, done, err = e.Purge(ctx, q)
	assert.NoError(err)
	assert.Zero(removed)
	assert.True(done, "nothing left")
	live, err := e.ListLive(ctx, workflow.Query{Limit: 10})
	assert.NoError(err)
	assert.Len(live, 0, "marked flows are out of the lists at once")
}

// TestPurge_TheRefusals pins what Purge refuses with a 400 beside a missing window or Limit (TestPurge_EveryListFilterNarrows):
// IncludeSubgraphs, a status that has not ended, and a Limit over the most a call takes.
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

// TestLifecycle_AnArchivedFlowAnswersAsAnEndedOne pins Resume, Terminate and Cancel on an archived flow, found in the
// archive when the live read misses: the answer any ended flow gets - Resume a 409, Terminate and Cancel a no-op that
// leaves the outcome as it was - a subgraph child's key a 400, and an unknown token a 404.
func TestLifecycle_AnArchivedFlowAnswersAsAnEndedOne(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	root := endedRoots(t, e, 1)[0]
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
