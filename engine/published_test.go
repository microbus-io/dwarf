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
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// publish leaves the live tree of rootKey as a staged archival publishes it: the whole tree copied into the archive day
// of the attempt at, and the live root stamped for deletion, keeping the attempt. The live copy is not deleted yet.
func publish(t *testing.T, e *Engine, rootKey string, at time.Time) (day int) {
	t.Helper()
	ctx := t.Context()
	_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	day = archiver.DayOf(at, e.archives.Load().dayLength)
	cols := strings.Join(columnsOf(t, db, migrations.FlowsTable(day)), ", ")
	stamp, arg := database.SQLTime(db.DriverName(), at)
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{"INSERT INTO " + migrations.StepsTable(day) + " SELECT * FROM dwarf_steps WHERE flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?)", []any{rootID}},
		{"INSERT INTO " + migrations.FlowsTable(day) + " (" + cols + ") SELECT " + cols + " FROM dwarf_flows WHERE root_flow_id=?", []any{rootID}},
		{"UPDATE " + migrations.FlowsTable(day) + " SET archived_at=" + stamp + " WHERE root_flow_id=?", []any{arg, rootID}},
		{"UPDATE dwarf_flows SET delete_after_ms=1, archive_attempt_at=" + stamp + " WHERE flow_id=?", []any{arg, rootID}},
	} {
		if _, err := db.ExecContext(ctx, stmt.sql, stmt.args...); err != nil {
			t.Fatal(err)
		}
	}
	return day
}

// attemptCleared reports whether the live root of rootKey holds no archive attempt.
func attemptCleared(t *testing.T, e *Engine, rootKey string) bool {
	t.Helper()
	_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	published, err := livePublished(t.Context(), db, rootID)
	if err != nil {
		t.Fatal(err)
	}
	return !published
}

// archiveMark returns the archive root's delete_after_ms in day, and -1 when the row is not there.
func archiveMark(t *testing.T, e *Engine, rootKey string, day int) int {
	t.Helper()
	_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	mark := -1
	_ = db.QueryRowContext(t.Context(), "SELECT delete_after_ms FROM "+migrations.FlowsTable(day)+" WHERE flow_id=?", rootID).Scan(&mark)
	return mark
}

// TestPublished_ReadsGoToTheArchiveWhileTheLiveCopyIsDeleted pins the routing of a published tree whose live copy is
// still there, stamped with its attempt: a read that needs the whole tree (Step, Summary, History, the lists) is served
// by the archive's copy, Snapshot by the live root, and Fork and Continue take the archive's copy as their source.
func TestPublished_ReadsGoToTheArchiveWhileTheLiveCopyIsDeleted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	rootKey, childKey, rootStep, childStep := treeKeys(t, e)
	publish(t, e, rootKey, time.Now())

	step, err := e.Step(ctx, childStep)
	if assert.NoError(err, "a child's step, from the archive") {
		assert.Equal(workflow.StatusCompleted, step.Status)
	}
	sum, err := e.Summary(ctx, childKey)
	if assert.NoError(err) {
		assert.False(sum.ArchivedAt.IsZero(), "summarized from the archive")
	}
	_, err = e.History(ctx, rootKey)
	assert.NoError(err)
	out, err := e.Snapshot(ctx, rootKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(2, closedListed(t, e), "listed as archived")
	recent, err := e.ListRecentlyClosed(ctx, enginetest.Recent(workflow.Query{IncludeSubgraphs: true}))
	assert.NoError(err)
	assert.Len(recent, 0, "and not as closed recently")

	forked, err := e.Fork(ctx, rootStep, nil)
	if assert.NoError(err, "the archive's copy is Fork's source") {
		enginetest.AwaitFlowStatus(t, e, forked, workflow.StatusCompleted, time.Minute)
	}
	_, err = e.Continue(ctx, rootKey, nil)
	assert.NoError(err, "and Continue's")
}

// TestPublished_DeleteMarksTheArchivesCopy pins Delete of a published tree: in one transaction the archive's root is
// marked, hiding the archived tree, and the live root's attempt is cleared, so its live copy is deleted as an ordinary
// stamped one. A repeated Delete is success, and the archive's copy stays marked.
func TestPublished_DeleteMarksTheArchivesCopy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	rootKey, childKey, _, _ := treeKeys(t, e)
	day := publish(t, e, rootKey, time.Now())

	assert.NoError(e.Delete(ctx, rootKey))
	assert.True(archiveMark(t, e, rootKey, day) > 0, "the archive's root is marked")
	assert.True(attemptCleared(t, e, rootKey))
	assert.Zero(closedListed(t, e))
	_, err := e.Summary(ctx, childKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	assert.NoError(e.Delete(ctx, rootKey), "a repeated Delete")

	e.reapDueFlows(ctx)
	_, err = e.Snapshot(ctx, rootKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "the live copy deleted as an ordinary stamped tree")
}

// TestPublished_DeleteOfACopyAlreadyGone pins Delete of a published tree whose archive copy is gone already: removed by
// Purge, or its day dropped. Either is the archive's deletion done: the attempt is cleared and Delete succeeds - for a
// dropped day in a transaction of its own, since the failed statement ended the first.
func TestPublished_DeleteOfACopyAlreadyGone(t *testing.T) {
	t.Parallel()
	for _, gone := range []string{"purged", "dropped"} {
		t.Run(gone, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			e := archiveEngine(t, treeMux(), 30)
			rootKey, _, _, _ := treeKeys(t, e)
			_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
			at := time.Now()
			if gone == "dropped" {
				at = at.Add(-10 * e.archives.Load().dayLength) // a day whose tables were never made, as a dropped day
				db, err := e.db.Shard(1)
				assert.Must(assert.NoError(err))
				stamp, arg := database.SQLTime(db.DriverName(), at)
				_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, archive_attempt_at="+stamp+" WHERE flow_id=?", arg, rootID)
				assert.Must(assert.NoError(err))
			} else {
				day := publish(t, e, rootKey, at)
				db, err := e.db.Shard(1)
				assert.Must(assert.NoError(err))
				_, err = db.ExecContext(ctx, "DELETE FROM "+migrations.FlowsTable(day)+" WHERE flow_id=?", rootID)
				assert.Must(assert.NoError(err))
			}
			assert.False(attemptCleared(t, e, rootKey))
			assert.NoError(e.Delete(ctx, rootKey))
			assert.True(attemptCleared(t, e, rootKey))
		})
	}
}

// TestPublished_AStampClearsAnAttempt pins the rule that makes "stamped with an attempt" mean published: Delete of a
// tree being staged - unstamped, with an attempt - stamps it and clears the attempt in the same UPDATE, so the copy is
// abandoned and the tree deleted.
func TestPublished_AStampClearsAnAttempt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	rootKey, _, _, _ := treeKeys(t, e)
	_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	stamp, arg := database.SQLTime(db.DriverName(), time.Now())
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+stamp+" WHERE flow_id=?", arg, rootID)
	assert.Must(assert.NoError(err))
	assert.NoError(e.Delete(ctx, rootKey))
	var stamped int
	assert.NoError(db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", rootID).Scan(&stamped))
	assert.True(stamped > 0)
	assert.True(attemptCleared(t, e, rootKey), "stamped without an attempt: an ordinary deletion")
}

// TestArchiveDeletion_ALargeTreeIsGoneAtOnceAndDeletedLater pins Purge and Delete of an archived tree larger than a
// batch: the call marks it and returns, the tree gone from every read and list at once and counted removed; the
// reaper's periodic stage deletes its rows; dwarf_archive_deletions_pending reports none left once it has.
func TestArchiveDeletion_ALargeTreeIsGoneAtOnceAndDeletedLater(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	purged, _, _, _ := treeKeys(t, e)
	deleted, _, _, _ := treeKeys(t, e)
	var ids []int
	for _, fk := range []string{purged, deleted} {
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		moveInto(t, e, id, today)
		ids = append(ids, id)
	}
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	e.reapBatchRows = 1 // every tree is larger than a batch
	e.archiveDeleteInCall = 0

	assert.NoError(e.Delete(ctx, deleted))
	_, err = e.Snapshot(ctx, deleted)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "gone at once")
	assert.Equal(http.StatusNotFound, errors.StatusCode(e.Delete(ctx, deleted)), "deleted already")
	removed, done, err := e.Purge(ctx, enginetest.Recent(workflow.Query{Limit: 10}))
	assert.NoError(err)
	assert.Equal(1, removed, "the other tree, marked")
	assert.True(done)
	assert.Zero(closedListed(t, e))
	for _, id := range ids {
		flows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(2, flows, "the rows of tree %d stay until the periodic stage", id)
	}

	e.reapPass(ctx, e.archiveGrace, true)
	for _, id := range ids {
		flows, steps := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Zero(flows+steps, "tree %d deleted", id)
	}
	pending, ok := gaugeAt(collect(t, reader), "dwarf_archive_deletions_pending", map[string]string{"shard": "1"})
	assert.True(ok)
	assert.Zero(pending)
}

// TestPublished_TheRetentionCleanupLeavesAPublishedTree pins that the retention cleanup takes only unstamped roots, so
// it never takes a published tree under live cleanup for an unpublished one: the live copy of a published tree past
// its retention is deleted as a stamped one, and the archive's copy stands, read as before.
func TestPublished_TheRetentionCleanupLeavesAPublishedTree(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 1)
	rootKey, childKey, _, _ := treeKeys(t, e)
	_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
	day := publish(t, e, rootKey, time.Now())
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE root_flow_id=?",
		-(4 * 24 * time.Hour).Milliseconds(), rootID)
	assert.Must(assert.NoError(err))

	e.reapDueFlows(ctx)
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Zero(flows, "the live copy is deleted")
	assert.Zero(archiveMark(t, e, rootKey, day), "the archive's copy is not marked")
	_, err = e.Summary(ctx, childKey)
	assert.NoError(err, "and is read")
}

// TestPublished_ADeleteRacingAPublication pins Delete against a publication between its read and its stamp: the stamp
// matches nothing, and Delete reads the root again, finds it published, and marks the archive's copy - rather than
// taking the publication's stamp for a racing Delete's and leaving the archive's copy standing.
func TestPublished_ADeleteRacingAPublication(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	rootKey, _, _, _ := treeKeys(t, e)
	e.seams.Break(engineseams.CheckpointBeforeDeleteStamp)
	deleted := make(chan error, 1)
	go func() { deleted <- e.Delete(ctx, rootKey) }()
	assert.Must(assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeDeleteStamp, 10*time.Second*enginetest.TimeoutScale())))
	day := publish(t, e, rootKey, time.Now())
	e.seams.Resume(engineseams.CheckpointBeforeDeleteStamp)
	assert.NoError(<-deleted)
	assert.True(archiveMark(t, e, rootKey, day) > 0, "the archive's copy is marked")
	assert.True(attemptCleared(t, e, rootKey))
	assert.Zero(closedListed(t, e))
}

// TestPublished_ADeleteRacingTheLiveCleanup pins Delete of a published tree whose live copy is deleted between Delete's
// read and its mark: the mark finds no live root to lock, and Delete reads it again, finds it gone, and deletes the
// archive's copy - rather than reporting success while it stays.
func TestPublished_ADeleteRacingTheLiveCleanup(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	rootKey, childKey, _, _ := treeKeys(t, e)
	_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
	publish(t, e, rootKey, time.Now())
	e.seams.Break(engineseams.CheckpointBeforeDeleteStamp)
	deleted := make(chan error, 1)
	go func() { deleted <- e.Delete(ctx, rootKey) }()
	assert.Must(assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointBeforeDeleteStamp, 10*time.Second*enginetest.TimeoutScale())))
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	for _, stmt := range []string{
		"DELETE FROM dwarf_steps WHERE flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?)",
		"DELETE FROM dwarf_flows WHERE root_flow_id=?",
	} {
		_, err := db.ExecContext(ctx, stmt, rootID)
		assert.Must(assert.NoError(err))
	}
	e.seams.Resume(engineseams.CheckpointBeforeDeleteStamp)
	assert.NoError(<-deleted)
	assert.Zero(closedListed(t, e), "the archive's copy is gone")
	_, err = e.Summary(ctx, childKey)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}

// TestStaged_ATreeArchivedInStagesReadsAsOne pins the engine's reads around a staged archive. While the copy is under
// way the tree reads from the live tables and nothing of it is listed as archived; once it is published the archive's
// copy serves every read and list, Fork included, before and after the live copy is deleted.
func TestStaged_ATreeArchivedInStagesReadsAsOne(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	e.reapStageRows, e.reapBatchRows = 1, 2
	rootKey, childKey, rootStep, childStep := treeKeys(t, e)

	// listed counts the archived flows of this tree; a fork's tree is archived too, once it ends.
	listed := func() int {
		got, err := e.ListClosed(ctx, enginetest.Recent(workflow.Query{IncludeSubgraphs: true}))
		assert.NoError(err)
		n := 0
		for _, s := range got {
			if s.RootFlowKey == rootKey {
				n++
			}
		}
		return n
	}
	e.seams.Break(engineseams.CheckpointReapStageChunkDone)
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.reapDueFlows(ctx)
	}()
	assert.Must(assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointReapStageChunkDone, 10*time.Second*enginetest.TimeoutScale())))
	assert.Zero(listed(), "nothing listed as archived mid-copy")
	_, err := e.History(ctx, rootKey)
	assert.NoError(err, "the live tree reads mid-copy")
	_, err = e.Step(ctx, childStep)
	assert.NoError(err)
	_, err = e.Summary(ctx, childKey)
	assert.NoError(err)
	forkKey, err := e.Fork(ctx, rootStep, nil)
	assert.NoError(err, "Fork of a tree mid-copy")
	if err == nil {
		_, err = e.Await(ctx, forkKey)
		assert.NoError(err)
	}
	e.seams.Resume(engineseams.CheckpointReapStageChunkDone)
	<-done

	reads := func(when string) {
		t.Helper()
		assert.Equal(2, listed(), "listed: %s", when)
		_, err := e.History(ctx, rootKey)
		assert.NoError(err, "History: %s", when)
		_, err = e.Step(ctx, childStep)
		assert.NoError(err, "Step: %s", when)
		_, err = e.Summary(ctx, childKey)
		assert.NoError(err, "Summary: %s", when)
		outcome, err := e.Snapshot(ctx, rootKey)
		if assert.NoError(err, "Snapshot: %s", when) {
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}
		forkKey, err := e.Fork(ctx, rootStep, nil)
		assert.NoError(err, "Fork: %s", when)
		if err == nil {
			_, err = e.Await(ctx, forkKey)
			assert.NoError(err)
		}
	}
	reads("published, the live copy still there")
	e.reapDueFlows(ctx)
	_, _, rootID, _, _ := keys.ParseFlowKey(rootKey)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	var n int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE root_flow_id=?", rootID).Scan(&n))
	assert.Zero(n, "the live copy is deleted")
	reads("published, the live copy deleted")
}
