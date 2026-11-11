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

package reaper

import (
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestSQL_TerminalStatusesAreTheClosedOnes pins the literal the reaper's rechecks inline against the workflow
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

// TestDelete_ADroppedDayIsNoError pins the archive-side deletion of a day whose tables are gone: nothing deleted and
// no error, since its trees went with it, while a day still there is deleted and counted.
func TestDelete_ADroppedDayIsNoError(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 2, callShape)
	moveInto(t, r, roots[0], today)
	moveInto(t, r, roots[1], today)

	n, err := r.reaper.DeleteArchivedRoots(ctx, r.db, today-5, roots, 4096, 4096, time.Now().Add(time.Minute)) // never made, as a dropped day
	assert.NoError(err)
	assert.Zero(n)
	n, err = r.reaper.DeleteArchivedRoots(ctx, r.db, today, roots[:1], 4096, 4096, time.Now().Add(time.Minute))
	assert.NoError(err)
	assert.Equal(1, n)
	flows, steps := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), roots[0])
	assert.Zero(flows + steps)
	flows, _ = treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), roots[1])
	assert.Equal(2, flows)
}

// TestDelete_APartialRemovalCountsWhatCommitted pins the archive-side deletion that fails part-way: the trees of the
// transactions that committed are counted with the error, and the failed transaction's trees stay whole.
func TestDelete_APartialRemovalCountsWhatCommitted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 4, callShape)
	for _, root := range roots {
		moveInto(t, r, root, today)
	}
	fault := join(FaultArchiveDelete, strconv.Itoa(roots[2]))
	r.seams.InjectN(fault, 1000)
	defer r.seams.Withdraw(fault)

	n, err := r.reaper.DeleteArchivedRoots(ctx, r.db, today, roots, 4096, 4, time.Now().Add(time.Minute)) // two trees of two rows a transaction
	assert.Error(err)
	assert.Equal(2, n, "the first transaction's trees")
	for i, root := range roots {
		flows, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
		if i < 2 {
			assert.Zero(flows, "tree %d", i)
		} else {
			assert.Equal(2, flows, "tree %d rolled back whole", i)
		}
	}

	n, err = r.reaper.DeleteArchivedTree(ctx, r.db, today, roots[2], 4096, time.Now().Add(time.Minute))
	assert.Error(err, "the fault applies to one tree too")
	assert.Zero(n)
	r.seams.Withdraw(fault)
	n, err = r.reaper.DeleteArchivedTree(ctx, r.db, today, roots[2], 4096, time.Now().Add(time.Minute))
	assert.NoError(err)
	assert.Equal(1, n)
}

// archiveWide seeds a wide closed tree and archives it into today, returning its root, its flows and today.
func archiveWide(t *testing.T, r *rig) (root int, flows []int, today int) {
	t.Helper()
	ctx := t.Context()
	today, err := r.archive.Today(ctx)
	if err != nil {
		t.Fatal(err)
	}
	root = seedWideTree(t, r.db, 2, 6, 3) // 7 flows, 20 steps
	flows = treeFlows(t, r, root)
	if _, err := r.reaper.MoveTrees(ctx, r.db, r.archive, today, []int{root}); err != nil {
		t.Fatal(err)
	}
	return root, flows, today
}

// archivedRows counts the flows and steps of flows in day's tables, and the directory's rows for them.
func archivedRows(t *testing.T, r *rig, day int, flows []int) (f, s, named int) {
	t.Helper()
	ctx := t.Context()
	list := intCSV(flows)
	for _, q := range []struct {
		sql string
		n   *int
	}{
		{"SELECT COUNT(*) FROM " + migrations.FlowsTable(day) + " WHERE flow_id IN (" + list + ")", &f},
		{"SELECT COUNT(*) FROM " + migrations.StepsTable(day) + " WHERE flow_id IN (" + list + ")", &s},
		{"SELECT COUNT(*) FROM " + archiver.DirectoryTable + " WHERE flow_id IN (" + list + ")", &named},
	} {
		if err := r.db.QueryRowContext(ctx, q.sql).Scan(q.n); err != nil {
			t.Fatal(err)
		}
	}
	return f, s, named
}

// TestDelete_ALargeArchivedTreeIsMarkedThenDeleted pins the archive's deletion of a tree larger than a batch: the call
// marks its root and returns, and DeleteMarked finds the mark and deletes the tree in chunks, the directory's rows for
// its flows with them, the root last. Until it is done, the pending count says so.
func TestDelete_ALargeArchivedTreeIsMarkedThenDeleted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root, flows, today := archiveWide(t, r)
	_, err := r.db.ExecContext(ctx, "INSERT INTO "+archiver.DirectoryTable+" (flow_id, day) VALUES (?, ?)", flows[3], today)
	assert.Must(assert.NoError(err))

	n, err := r.reaper.DeleteArchivedTree(ctx, r.db, today, root, 4, time.Now()) // no time to chunk in the call
	assert.NoError(err)
	assert.Equal(1, n, "marked: removed as far as every reader is concerned")
	var mark int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT delete_after_ms FROM "+migrations.FlowsTable(today)+" WHERE flow_id=?", root).Scan(&mark))
	assert.True(mark > 0)
	f, s, _ := archivedRows(t, r, today, flows)
	assert.Equal([2]int{7, 20}, [2]int{f, s}, "its rows are deleted later")
	n, err = r.reaper.DeleteArchivedTree(ctx, r.db, today, root, 4, time.Now())
	assert.NoError(err)
	assert.Zero(n, "a marked tree is not marked or counted again")

	r.cfg.BatchRows = 4
	r.cfg.Budget = 0 // time for the one chunk a call always gives what it finds: the tree is left pending
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	obs := r.reaper.Observe(1, time.Now())
	assert.True(obs.PendingOK)
	assert.Equal(1, obs.Pending, "the partially drained tree")

	r.cfg.Budget = time.Minute
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	f, s, named := archivedRows(t, r, today, flows)
	assert.Zero(f+s+named, "the tree and the directory's rows for it")
	assert.Equal(0, r.reaper.Observe(1, time.Now()).Pending)
}

// TestDelete_APurgeOfALargeTreeCountsItOnce pins the logical-removal count across calls: DeleteArchivedRoots counts a
// large tree it marks, and a later call naming it again counts nothing, the mark already there.
func TestDelete_APurgeOfALargeTreeCountsItOnce(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root, _, today := archiveWide(t, r)
	small := seedTrees(t, r.db, 2, callShape)
	for _, id := range small {
		moveInto(t, r, id, today)
	}
	roots := append([]int{root}, small...)
	n, err := r.reaper.DeleteArchivedRoots(ctx, r.db, today, roots, 4096, 4, time.Now())
	assert.NoError(err)
	assert.Equal(3, n, "the large tree marked, the small ones deleted")
	n, err = r.reaper.DeleteArchivedRoots(ctx, r.db, today, roots, 4096, 4, time.Now())
	assert.NoError(err)
	assert.Zero(n)
}

// TestDelete_AMarkedRootIsNotDeletedAtomically pins that an atomic archive deletion skips a root marked already: a
// selection made before another call marked the tree, the tree shrunk below a batch by its chunks since, would
// otherwise delete it and count it removed a second time. The background finishes it.
func TestDelete_AMarkedRootIsNotDeletedAtomically(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedTrees(t, r.db, 1, callShape)[0]
	moveInto(t, r, root, today)
	_, err = r.db.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(today)+" SET delete_after_ms=1 WHERE flow_id=?", root)
	assert.Must(assert.NoError(err))
	n, err := r.reaper.DeleteArchivedRoots(ctx, r.db, today, []int{root}, 4096, 4096, time.Now().Add(time.Minute))
	assert.NoError(err)
	assert.Zero(n, "not counted again")
	f, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(2, f, "left to the background")
}

// holdArchivedStep locks the archived steps of flow in day from another transaction until the returned release is
// called, so a chunk deleting them waits. SQLite and SQL Server lock differently, and are skipped.
func holdArchivedStep(t *testing.T, r *rig, day, flow int) (release func()) {
	t.Helper()
	return holdRows(t, r, migrations.StepsTable(day), flow)
}

// holdRows locks the rows of table whose flow_id is flow from another transaction until the returned release is
// called. SQLite and SQL Server lock differently, and are skipped.
func holdRows(t *testing.T, r *rig, table string, flow int) (release func()) {
	t.Helper()
	if d := r.db.DriverName(); d != "pgx" && d != "mysql" && d != "mariadb" {
		t.Skip("a held row is a FOR UPDATE on PostgreSQL, MySQL and MariaDB")
	}
	ctx := t.Context()
	locked := make(chan error, 1)
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		_ = r.db.Transact(ctx, func(tx *sequel.Tx) error {
			rows, err := tx.QueryContext(ctx, "SELECT flow_id FROM "+table+" WHERE flow_id=? FOR UPDATE", flow)
			if err == nil {
				rows.Close()
			}
			locked <- err
			<-done
			return err
		})
	}()
	if err := <-locked; err != nil {
		t.Fatal(err)
	}
	return func() {
		close(done)
		<-finished
	}
}

// TestDelete_TheInCallChunksKeepToTheirDeadline pins the in-call allowance on a chunk that waits on a lock: the call
// returns at its deadline, the tree marked, rather than waiting for the lock however long it is held.
func TestDelete_TheInCallChunksKeepToTheirDeadline(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root, flows, today := archiveWide(t, r)
	release := holdArchivedStep(t, r, today, flows[len(flows)-1])
	defer release()
	start := time.Now()
	n, err := r.reaper.DeleteArchivedTree(ctx, r.db, today, root, 4, time.Now().Add(200*time.Millisecond))
	assert.NoError(err)
	assert.Equal(1, n, "marked")
	assert.True(time.Since(start) < 5*time.Second*timeoutScale, "returned at its deadline: %v", time.Since(start))
}

// TestDelete_AStuckMarkedTreeDoesNotStarveTheOthers pins DeleteMarked's rotation: a marked tree whose chunk waits out
// the pass's budget is passed over by the next pass, which starts after it, so the trees behind it are deleted.
func TestDelete_AStuckMarkedTreeDoesNotStarveTheOthers(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	stuck, flows, today := archiveWide(t, r)
	other, otherFlows, _ := archiveWide(t, r)
	for _, root := range []int{stuck, other} {
		_, err := r.db.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(today)+" SET delete_after_ms=1 WHERE flow_id=?", root)
		assert.Must(assert.NoError(err))
	}
	release := holdArchivedStep(t, r, today, flows[len(flows)-1])
	defer release()
	r.cfg.BatchRows = 4
	r.cfg.Attempt = 2 * time.Second       // a chunk waiting on the lock gives up here
	r.cfg.Budget = 100 * time.Millisecond // which is past the pass's budget: one tree a pass
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	// The next pass starts after the stuck tree, on the other, with a budget a slow server deletes it in but still
	// short of the stuck chunk's wait: without the rotation the pass would spend it on the stuck tree again.
	r.cfg.Budget = time.Second
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	f, s, _ := archivedRows(t, r, today, otherFlows)
	assert.Zero(f+s, "the tree behind the stuck one is deleted")
	f, _, _ = archivedRows(t, r, today, flows)
	assert.True(f > 0, "the stuck one is not")
	assert.Equal(1, r.reaper.Observe(1, time.Now()).Pending)
}

// TestDelete_ADayWithoutTheDeletionIndexIsSwept pins DeleteMarked on a day made before its deletion index: on MySQL and
// MariaDB the probe names the index, and a hint naming a missing index is an error there, so the probe without it
// follows - rather than the day's marked trees staying until the day drops.
func TestDelete_ADayWithoutTheDeletionIndexIsSwept(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root, flows, today := archiveWide(t, r)
	_, err := r.db.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(today)+" SET delete_after_ms=1 WHERE flow_id=?", root)
	assert.Must(assert.NoError(err))
	index := "idx_" + migrations.FlowsTable(today) + "_delete_after"
	drop := "DROP INDEX " + index
	if d := r.db.DriverName(); d == "mysql" || d == "mariadb" || d == "mssql" {
		drop += " ON " + migrations.FlowsTable(today)
	}
	_, err = r.db.ExecContext(ctx, drop)
	assert.Must(assert.NoError(err))
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	f, s, named := archivedRows(t, r, today, flows)
	assert.Zero(f+s+named, "the marked tree is deleted")
	assert.Zero(r.reaper.Observe(1, time.Now()).Pending)
}

// TestDelete_EachCallGetsPastWhereTheLastStopped pins DeleteMarked's progress when its budget is spent before it
// starts: it still gives the first tree it finds a chunk, and the next call starts at the next day - so a day whose
// probe is slow enough to spend every pass's budget does not hold back the others.
func TestDelete_EachCallGetsPastWhereTheLastStopped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	assert.Must(assert.NoError(makeArchiveDay(t, r, today-1)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	roots := seedTrees(t, r.db, 2, callShape)
	moveInto(t, r, roots[0], today-1)
	moveInto(t, r, roots[1], today)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the flows were seeded")
	}
	for i, day := range []int{today - 1, today} {
		_, err := r.db.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(day)+" SET delete_after_ms=1 WHERE flow_id=?", roots[i])
		assert.Must(assert.NoError(err))
	}
	r.cfg.Budget = 0
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	f, _ := treeCount(t, r, migrations.FlowsTable(today-1), migrations.StepsTable(today-1), roots[0])
	assert.Zero(f, "the first call deletes the first tree it finds")
	f, _ = treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), roots[1])
	assert.Equal(2, f, "and nothing past its budget")
	obs := r.reaper.Observe(1, time.Now())
	assert.Zero(obs.Pending, "the discovered tree is gone")
	assert.Equal(2, obs.PendingUnknown, "today and the prepared tomorrow were not examined")
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	f, _ = treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), roots[1])
	assert.Zero(f, "the next call starts at the next day")
	r.cfg.Budget = time.Minute
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	obs = r.reaper.Observe(1, time.Now())
	assert.True(obs.PendingOK)
	assert.Zero(obs.Pending+obs.PendingUnknown, "a complete scan finds no work")
	r.reaper.Forget(1)
	assert.False(r.reaper.Observe(1, time.Now()).PendingOK, "forget clears the combined observation")
}

// TestDelete_AHeldMarkedTreeIsPending pins DeleteMarked's count of a tree whose root another transaction holds: a
// chunk that skips a held root reports itself done, and the tree, still there, counts as pending - so a pending
// reading of 0 means the marked trees are gone.
func TestDelete_AHeldMarkedTreeIsPending(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root, _, today := archiveWide(t, r)
	_, err := r.db.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(today)+" SET delete_after_ms=1 WHERE flow_id=?", root)
	assert.Must(assert.NoError(err))
	release := holdRows(t, r, migrations.FlowsTable(today), root)
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	assert.Equal(1, r.reaper.Observe(1, time.Now()).Pending)
	release()
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	assert.Zero(r.reaper.Observe(1, time.Now()).Pending)
}
