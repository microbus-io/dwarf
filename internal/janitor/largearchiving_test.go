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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// stageRig is a rig that archives a tree of more than 8 rows in stages, 4 rows a chunk.
func stageRig(t *testing.T) *rig {
	t.Helper()
	r := newRig(t, 30)
	r.settings.stageRows = 8
	r.settings.batchRows = 4
	return r
}

// startAttempts runs fast archiving's rounds until they make no progress: every due tree over the staging threshold
// has its attempt started, and the rest are moved.
func (r *rig) startAttempts(ctx context.Context) {
	for r.roundOf(ctx, JobFastArchiving) {
	}
}

// largeBatch runs exactly one batch of large-tree archiving - one chunk of the next tree - and reports its progress and
// the root it took.
func (r *rig) largeBatch(ctx context.Context) (progress bool, root int) {
	l := r.large()
	err := r.janitor().batch(JobLargeArchiving)(ctx, func(ctx context.Context, h handle) error {
		progress, root = l.batch(ctx, h, nil)
		return nil
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return progress, root
}

// cleanUp removes the live copies of published trees: fast deletion, started over and with a batch that holds them. A
// stamped tree larger than a batch is passed, behind fast deletion's position, which only a restart starts over from.
func (r *rig) cleanUp(ctx context.Context) {
	r.restart()
	r.settings.batchRows = 4096
	r.pass(ctx)
}

// treeRows returns the flow ids and step ids of the tree rooted at root in the given tables, ascending.
func treeRows(t *testing.T, r *rig, flows, steps string, root int) (flowIDs, stepIDs []int) {
	t.Helper()
	ctx := t.Context()
	read := func(q string) []int {
		rows, err := r.db.QueryContext(ctx, q, root)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			out = append(out, id)
		}
		return out
	}
	flowIDs = read("SELECT flow_id FROM " + flows + " WHERE root_flow_id=? ORDER BY flow_id")
	stepIDs = read("SELECT s.step_id FROM " + steps + " s JOIN " + flows + " f ON f.flow_id=s.flow_id" +
		" WHERE f.root_flow_id=? ORDER BY s.step_id")
	return flowIDs, stepIDs
}

// visibleInDay counts the flows of root's tree in day that the archive's visibility rule shows.
func visibleInDay(t *testing.T, r *rig, day, root int) int {
	t.Helper()
	flows := migrations.FlowsTable(day)
	var n int
	err := r.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM "+flows+" f LEFT JOIN "+flows+
		" rf ON rf.flow_id=NULLIF(f.root_flow_id, f.flow_id) WHERE f.root_flow_id=? AND "+archiver.VisibleSQL("f", "rf"), root).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// attemptAt reads root's archive attempt.
func attemptAt(t *testing.T, r *rig, root int) time.Time {
	t.Helper()
	var at time.Time
	if err := r.db.QueryRowContext(t.Context(), "SELECT archive_attempt_at FROM dwarf_flows WHERE flow_id=?", root).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return database.StoredTime(at)
}

// setAttempt gives root an archive attempt at the given time.
func setAttempt(t *testing.T, r *rig, root int, at time.Time) {
	t.Helper()
	expr, arg := database.SQLTime(r.db.DriverName(), at)
	if _, err := r.db.ExecContext(t.Context(), "UPDATE dwarf_flows SET archive_attempt_at="+expr+" WHERE flow_id=?", arg, root); err != nil {
		t.Fatal(err)
	}
}

// deleteLive does to the live root what a Delete does: stamps it due at once and clears its attempt, keeping the archive
// time a publication copied.
func deleteLive(t *testing.T, r *rig, root int) {
	t.Helper()
	none, arg := NoAttemptSQL(r.db.DriverName())
	if _, err := r.db.ExecContext(t.Context(), "UPDATE dwarf_flows SET delete_after_ms=1, archive_attempt_at="+none+" WHERE flow_id=?", arg, root); err != nil {
		t.Fatal(err)
	}
}

// updatedAt reads root's close time.
func updatedAt(t *testing.T, r *rig, root int) time.Time {
	t.Helper()
	var at time.Time
	if err := r.db.QueryRowContext(t.Context(), "SELECT updated_at FROM dwarf_flows WHERE flow_id=?", root).Scan(&at); err != nil {
		t.Fatal(err)
	}
	return database.StoredTime(at)
}

// holdRows holds a FOR UPDATE lock on flow's row in table until release is called.
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

// filing reads today into a filing day, as a round does.
func filing(t *testing.T, r *rig) *filingDay {
	t.Helper()
	d := &filingDay{}
	if err := refreshDay(t.Context(), r.db, r.archive, true, d); err != nil {
		t.Fatal(err)
	}
	return d
}

// oldestAttempt is dwarf_archive_oldest_attempt_seconds' reading now.
func (r *rig) oldestAttempt() (time.Duration, bool) {
	return r.large().oldestAge(time.Now())
}

// TestStage_ALargeTreeIsPublishedWhole pins the staged path end to end: fast archiving starts the attempt of a tree over
// the staging threshold, and large-tree archiving copies it in chunks of the batch into the day of its attempt and
// publishes it - every flow and step there, the root last - its live root stamped with the attempt kept. Every copied
// flow is visible once the root is in, and the live copy goes with the deletion of stamped trees.
func TestStage_ALargeTreeIsPublishedWhole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3) // 11 flows, 33 steps
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)

	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
	assert.Equal(len(wantFlows), visibleInDay(t, r, today, root))
	var stamp int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp))
	assert.Equal(1, stamp, "published: stamped")
	assert.True(attemptAt(t, r, root).After(noAttempt), "with its attempt kept")

	r.cleanUp(ctx)
	f, s, _ := rowsOf(t, r, root)
	assert.Zero(f+s, "the live tree is removed once published")
}

// TestStage_StampGroupsAreAMillisecondApart pins the archive times of a staged tree: its descendants in flow_id order
// share one time per 4,096, a millisecond apart from the attempt's time on, and the root takes the next millisecond -
// so no staged tree puts more than a group on one time, however its chunks fell.
func TestStage_StampGroupsAreAMillisecondApart(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.settings.batchRows = 1000
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 1, stageGroup+1, 0)
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	rows, err := r.db.QueryContext(ctx, "SELECT archived_at, COUNT(*), MAX(CASE WHEN flow_id=root_flow_id THEN 1 ELSE 0 END)"+
		" FROM "+migrations.FlowsTable(today)+" WHERE root_flow_id=? GROUP BY archived_at ORDER BY archived_at", root)
	assert.Must(assert.NoError(err))
	defer rows.Close()
	var times []time.Time
	var counts, roots []int
	for rows.Next() {
		var at time.Time
		var n, isRoot int
		assert.Must(assert.NoError(rows.Scan(&at, &n, &isRoot)))
		times, counts, roots = append(times, database.StoredTime(at)), append(counts, n), append(roots, isRoot)
	}
	assert.Equal([]int{stageGroup, 1, 1}, counts)
	assert.Equal([]int{0, 0, 1}, roots, "the root last")
	if len(times) == 3 {
		assert.Equal(time.Millisecond, times[1].Sub(times[0]))
		assert.Equal(time.Millisecond, times[2].Sub(times[1]))
	}
}

// TestStage_ARestartJoinsItsAttempt pins a copy cut off part-way: a fresh janitor, which knows nothing of it, joins the
// same attempt and finishes the copy, rewriting what was there with the same rows - none twice, none missing.
func TestStage_ARestartJoinsItsAttempt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	r.startAttempts(ctx)
	for range 3 {
		r.chunk(ctx)
	}
	at := attemptAt(t, r, root)
	assert.True(at.After(noAttempt), "an attempt started")
	copied, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.True(len(copied) > 0 && len(copied) < len(wantFlows), "part copied: %d", len(copied))
	assert.Zero(visibleInDay(t, r, today, root), "nothing visible before the root is in")

	r.restart()
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.True(attemptAt(t, r, root).Equal(at), "the same attempt")
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
}

// TestStage_TwoCopiersShareAnAttempt pins two jobs - two replicas - taking turns on one tree: each joins the one
// attempt, they write the same rows, and the tree is published once, whole.
func TestStage_TwoCopiersShareAnAttempt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	r.startAttempts(ctx)
	first := attemptAt(t, r, root)
	other := r.copier()
	for i := 0; i < 200 && visibleInDay(t, r, today, root) == 0; i++ {
		if i%2 == 0 {
			r.chunk(ctx)
		} else {
			other(ctx)
		}
		assert.True(attemptAt(t, r, root).Equal(first), "the copiers keep one attempt")
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
	assert.Equal(len(wantFlows), visibleInDay(t, r, today, root))
}

// TestStage_ADeleteAbandonsTheCopy pins Delete during staging: it stamps the root and clears the attempt, the copier
// stops at its next chunk, the live tree is deleted, and the rows already copied stay in the day, invisible, since the
// day has no root for them.
func TestStage_ADeleteAbandonsTheCopy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	r.startAttempts(ctx)
	for range 2 {
		r.chunk(ctx)
	}
	copied, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Must(assert.True(len(copied) > 0, "part copied"))

	deleteLive(t, r, root)
	assert.False(attemptAt(t, r, root).After(noAttempt), "the stamp clears the attempt")
	r.cleanUp(ctx)
	f, s, _ := rowsOf(t, r, root)
	assert.Zero(f+s, "the live tree is deleted")
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	after, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(copied, after, "the copier stopped")
	assert.Zero(visibleInDay(t, r, today, root), "the abandoned rows are invisible")
}

// TestStage_AnAttemptKeepsItsDay pins an attempt that started before midnight: the copy goes on into its day, the
// earlier one, every archive time inside that day.
func TestStage_AnAttemptKeepsItsDay(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, _ := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	setAttempt(t, r, root, r.archive.DayEnds(yesterday).Add(-2*time.Minute))
	r.pass(ctx)
	gotFlows, _ := treeRows(t, r, migrations.FlowsTable(yesterday), migrations.StepsTable(yesterday), root)
	assert.Equal(wantFlows, gotFlows, "published into the attempt's day")
	var latest time.Time
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT archived_at FROM "+migrations.FlowsTable(yesterday)+
		" WHERE root_flow_id=? ORDER BY archived_at DESC LIMIT_OFFSET(1, 0)", root).Scan(&latest)))
	assert.True(database.StoredTime(latest).Before(r.archive.DayEnds(yesterday)), "inside the day: %v", latest)
}

// TestStage_ADroppedDayIsReplaced pins an attempt whose day is dropped mid-copy: the next chunk fails on the missing
// table, the attempt is replaced by one into today in a transaction of its own, and the tree is copied again and
// published there.
func TestStage_ADroppedDayIsReplaced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	stale := r.archive.DayEnds(yesterday).Add(-2 * time.Minute)
	setAttempt(t, r, root, stale)
	r.chunk(ctx)
	copied, _ := treeRows(t, r, migrations.FlowsTable(yesterday), migrations.StepsTable(yesterday), root)
	assert.Must(assert.True(len(copied) > 0, "part copied into yesterday"))

	_, err = r.archive.DropBefore(ctx, today)
	assert.Must(assert.NoError(err))
	assert.True(r.chunk(ctx), "the chunk finds the day gone, and the attempt is replaced")
	at := attemptAt(t, r, root)
	assert.False(at.Equal(stale), "replaced")
	assert.Equal(today, archiver.DayOf(at, r.archive.DayLength()))
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
}

// TestStage_AnAttemptedRootIsStagedWhateverItsSize pins the atomic path's fence: an atomic move skips a root whose staged
// archive has started, and large-tree archiving copies it even with staging off for new trees - so a tree is never
// archived both ways.
func TestStage_AnAttemptedRootIsStagedWhateverItsSize(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedTrees(t, r.db, 1, callShape)[0]
	setAttempt(t, r, root, updatedAt(t, r, root))
	due, dueArgs := closedDueSQL(0)
	moved, err := move(ctx, r.db, r.seams, []int{root}, due, dueArgs, filing(t, r))
	assert.NoError(err)
	assert.Zero(moved.flows, "the atomic move skips it")

	r.pass(ctx) // staging is off: no new attempt, but this one goes on
	if d, _ := r.archive.Today(ctx); d != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(2, visibleInDay(t, r, today, root), "published by the staged path")
	_, _, there := rowsOf(t, r, root)
	assert.False(there, "and its live copy deleted, a batch holding it")
}

// TestStage_APublishedTreeIsNotCopiedAgain pins a lost publication response: stamped with its attempt, the tree is
// published, so a chunk that comes back to it finds nothing to stage and copies nothing.
func TestStage_APublishedTreeIsNotCopiedAgain(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	r.pass(ctx)
	pos := stagePos{at: attemptAt(t, r, root)}
	n, copied, done, err := r.large().stageChunk(ctx, r.db, root, &pos)
	assert.NoError(err)
	assert.True(done, "not the copy's: stamped")
	assert.Zero(n)
	assert.Zero(copied)
	assert.False(r.chunk(ctx), "no attempt left to copy")
}

// TestStage_AnOpenFlowHoldsTheTree pins the guard against a tree that holds a flow that is not closed, which only a
// bug leaves: the copier refuses it, and nothing is published.
func TestStage_AnOpenFlowHoldsTheTree(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	var child int
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT MAX(flow_id) FROM dwarf_flows WHERE root_flow_id=?", root).Scan(&child)))
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_flows SET status='running' WHERE flow_id=?", child)
	assert.Must(assert.NoError(err))
	r.pass(ctx)
	assert.Zero(visibleInDay(t, r, today, root))
	f, _, there := rowsOf(t, r, root)
	assert.True(there && f == 11, "the tree stays live")
	assert.True(r.logs.count("Janitor: staging an archive") > 0, "refused, logged")
}

// TestStage_TheDirectoryNamesALateFlow pins the staged copy's directory rows: a flow a locate would not find by its
// creation hour alone is named in the directory with its day, and found.
func TestStage_TheDirectoryNamesALateFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	var child, hour int
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT MAX(flow_id) FROM dwarf_flows WHERE root_flow_id=?", root).Scan(&child)))
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_flows SET created_hour=created_hour-96 WHERE flow_id=?", child)
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", child).Scan(&hour)))
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	day, err := r.archive.LocateFlow(ctx, child, hour)
	assert.NoError(err)
	assert.Equal(today, day)
}

// TestStage_AHalfDroppedDayIsReplaced pins publication against a drop that stopped between the day's two tables: the
// flows table goes first, so the next chunk fails on it and the attempt is replaced, rather than a root being inserted
// beside steps that are gone.
func TestStage_AHalfDroppedDayIsReplaced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	root := seedWideTree(t, r.db, 3, 10, 3)
	stale := r.archive.DayEnds(yesterday).Add(-2 * time.Minute)
	setAttempt(t, r, root, stale)
	r.chunk(ctx)
	_, err = r.db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(yesterday))
	assert.Must(assert.NoError(err))
	r.chunk(ctx)
	assert.False(attemptAt(t, r, root).Equal(stale), "replaced")
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(11, visibleInDay(t, r, today, root), "published into today")
}

// TestStage_AFailedChunkIsRetried pins a staging chunk that fails: the job counts it a failure and moves on, the copy
// keeps its attempt, and a later turn goes on and publishes the tree whole.
func TestStage_AFailedChunkIsRetried(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	r.startAttempts(ctx)
	r.chunk(ctx)
	at := attemptAt(t, r, root)
	fault := diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(root))
	r.seams.InjectN(fault, 1000)
	assert.False(r.chunk(ctx), "a failed chunk is no progress")
	assert.False(r.chunk(ctx), "and the tree is tried once a round")
	r.seams.Withdraw(fault)
	assert.True(attemptAt(t, r, root).Equal(at), "the attempt holds through a failed chunk")
	assert.Equal(2, r.logs.count("Janitor: staging an archive"), "each failed chunk logged")
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
}

// TestStage_AHeldRootKeepsItsCopysPosition pins a copy whose root another transaction holds - another copier, a Fork:
// the chunk yields, and the copy's position is kept, so a later turn goes on rather than rereading what it copied.
func TestStage_AHeldRootKeepsItsCopysPosition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	r.startAttempts(ctx)
	r.chunk(ctx)
	before := r.large().stages[root]
	assert.Must(assert.True(before.seen > 0, "part copied"))
	release := holdRows(t, r, "dwarf_flows", root)
	assert.False(r.chunk(ctx), "held: no progress")
	release()
	assert.Equal(before, r.large().stages[root], "kept while the root was held")
	r.chunk(ctx)
	after := r.large().stages[root]
	assert.True(after.seen > before.seen && after.at.Equal(before.at), "and gone on from: %+v", after)
}

// TestStage_APublishedCopyIsForgotten pins the positions the job keeps: a copy that publishes leaves no position
// behind, and past the bound one position goes, not every one.
func TestStage_APublishedCopyIsForgotten(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	r.pass(ctx)
	_, kept := r.large().stages[root]
	assert.False(kept, "published: forgotten")

	l := r.large()
	for i := range maxStages {
		l.remember(i+1, stagePos{seen: i + 1})
	}
	l.remember(maxStages+1, stagePos{seen: 1})
	assert.Equal(maxStages, len(l.stages), "one went, and only one")
	assert.Equal(1, l.stages[maxStages+1].seen)
}

// TestStage_AnAttemptGoesOnWhenNoneMayStart pins the archive gate against a staged copy already started: with
// tomorrow's tables not whole no new archive starts - neither a move nor an attempt - but an attempt into a retained
// day goes on and publishes there; and an attempt into today copies but does not publish, since a flow is located
// only where the day after its own is whole.
func TestStage_AnAttemptGoesOnWhenNoneMayStart(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	old := seedWideTree(t, r.db, 3, 10, 3)
	setAttempt(t, r, old, r.archive.DayEnds(yesterday).Add(-2*time.Minute))
	fresh := seedWideTree(t, r.db, 3, 10, 3)
	small := seedTrees(t, r.db, 1, callShape)[0]
	_, err = r.db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(today+1))
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}

	r.pass(ctx)
	assert.Equal(11, visibleInDay(t, r, yesterday, old), "the started attempt published into its day")
	assert.False(attemptAt(t, r, fresh).After(noAttempt), "no new attempt")
	_, _, there := rowsOf(t, r, small)
	assert.True(there, "no move")

	setAttempt(t, r, fresh, updatedAt(t, r, fresh))
	r.pass(ctx)
	copied, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), fresh)
	assert.Equal(10, len(copied), "every descendant copied")
	assert.Zero(visibleInDay(t, r, today, fresh), "not published while tomorrow is not whole")
}

// TestStage_ACopierWithAReplacedAttemptWritesNothing pins every chunk's recheck of the attempt: a copier still holding
// an attempt that was replaced finds its lock miss and writes nothing into the old attempt's day.
func TestStage_ACopierWithAReplacedAttemptWritesNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	old := updatedAt(t, r, root)
	setAttempt(t, r, root, old.Add(time.Second)) // the attempt the copier does not hold
	pos := stagePos{at: old}
	n, _, done, err := r.large().stageChunk(ctx, r.db, root, &pos)
	assert.NoError(err)
	assert.True(done)
	assert.Zero(n)
	copied, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Zero(len(copied), "nothing written under the old attempt")
}

// TestStage_TwoReplicasFindingTheDayDroppedReplaceItOnce pins the replacement's guard: a replacement happens only while
// the attempt is still the one found stale, so a second replica that found the same day dropped joins the first one's
// new attempt rather than replacing it again.
func TestStage_TwoReplicasFindingTheDayDroppedReplaceItOnce(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	stale := r.archive.DayEnds(0).Add(-time.Hour) // a day long gone
	setAttempt(t, r, root, stale)
	due, dueArgs := closedDueSQL(0)
	first, ok, err := claimAttempt(ctx, r.db, root, due, dueArgs, filing(t, r), stale)
	assert.NoError(err)
	assert.True(ok && !first.Equal(stale), "replaced")
	time.Sleep(5 * time.Millisecond) // a second replacement would then write another time
	second, ok, err := claimAttempt(ctx, r.db, root, due, dueArgs, filing(t, r), stale)
	assert.NoError(err)
	assert.True(ok && second.Equal(first), "joined, not replaced again")
}

// TestStage_ANewAttemptNeedsRoomInItsDay pins where a new attempt may start: not past the filing day's stop, not with
// tomorrow not whole, and not when its time falls outside the filing day - past midnight - each leaving no attempt.
func TestStage_ANewAttemptNeedsRoomInItsDay(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	due, dueArgs := closedDueSQL(0)
	for name, d := range map[string]*filingDay{
		"past the stop":      func() *filingDay { d := filing(t, r); d.stop = time.Now().Add(-time.Second); return d }(),
		"tomorrow not whole": func() *filingDay { d := filing(t, r); d.notWhole = true; return d }(),
		"not the filing day": func() *filingDay { d := filing(t, r); d.day--; return d }(),
	} {
		_, ok, err := claimAttempt(ctx, r.db, root, due, dueArgs, d, time.Time{})
		assert.NoError(err, name)
		assert.False(ok, name)
		assert.False(attemptAt(t, r, root).After(noAttempt), "%s: no attempt", name)
	}
	_, ok, err := claimAttempt(ctx, r.db, root, due, dueArgs, filing(t, r), time.Time{})
	assert.NoError(err)
	assert.True(ok, "with room, it starts")
}

// TestStage_NoArchiveTimeIsClamped pins the day-end check: a tree whose archive times would run past its attempt's day
// is held, not stamped into the day's last millisecond.
func TestStage_NoArchiveTimeIsClamped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3) // the root's time is T + 1 ms
	setAttempt(t, r, root, r.archive.DayEnds(today).Add(-time.Millisecond))
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Zero(visibleInDay(t, r, today, root), "not published")
	_, _, there := rowsOf(t, r, root)
	assert.True(there, "held live")
}

// TestStage_AStaleDirectoryRowIsMoved pins the directory row of a flow staged into a day after another named it - a
// replaced attempt's dropped day: the row is moved to the day the flow is in, so a locate finds it there.
func TestStage_AStaleDirectoryRowIsMoved(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	var child int
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT MAX(flow_id) FROM dwarf_flows WHERE root_flow_id=?", root).Scan(&child)))
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_flows SET created_hour=created_hour-96 WHERE flow_id=?", child)
	assert.Must(assert.NoError(err))
	_, err = r.db.ExecContext(ctx, "INSERT INTO "+archiver.DirectoryTable+" (flow_id, day) VALUES (?, ?)", child, today-5)
	assert.Must(assert.NoError(err))
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	var day int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT day FROM "+archiver.DirectoryTable+" WHERE flow_id=?", child).Scan(&day))
	assert.Equal(today, day)
}

// TestStage_FlowsLargerThanAChunk pins the copy of a descendant and of a root each larger than a chunk's budget: copied
// a budget at a time, from the position, and the tree published whole.
func TestStage_FlowsLargerThanAChunk(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 10, 3, 9) // children of 9 steps and a root of 10, against a budget of 4
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
}

// TestStage_ConcurrentCopiers pins two replicas copying one tree at the same time: their chunks serialize on the root's
// lock, and the tree is published once, whole.
func TestStage_ConcurrentCopiers(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 30, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	r.startAttempts(ctx)
	copiers := []func(context.Context) bool{r.copier(), r.copier()}
	var wg sync.WaitGroup
	for _, copier := range copiers {
		wg.Go(func() {
			for range 200 {
				if visibleInDay(t, r, today, root) > 0 {
					return
				}
				copier(ctx)
			}
		})
	}
	wg.Wait()
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
	assert.Equal(len(wantFlows), visibleInDay(t, r, today, root))
}

// TestStage_AStartedAttemptBehindABatchIsReached pins how large-tree archiving finds its trees - by their attempts,
// through their index - while no new archive may start: an attempt whose root is behind roots that have none is still
// found and published into its day, while they stay live for the day to open.
func TestStage_AStartedAttemptBehindABatchIsReached(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.settings.batchRoots = 2
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	ahead := seedTrees(t, r.db, 3, callShape) // due first: closed first
	time.Sleep(5 * time.Millisecond)
	root := seedWideTree(t, r.db, 3, 10, 3)
	setAttempt(t, r, root, r.archive.DayEnds(yesterday).Add(-2*time.Minute))
	_, err = r.db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(today+1))
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	r.pass(ctx)
	assert.Equal(11, visibleInDay(t, r, yesterday, root), "the started attempt published into its day")
	for _, a := range ahead {
		_, _, there := rowsOf(t, r, a)
		assert.True(there, "a tree with no attempt waits for the day to open")
	}
}

// TestStage_ADroppedDayIsReplacedWhileWaitingToPublish pins a copy that has finished but waits to publish, the day after
// its own not whole, when its own day is dropped: a statement on the day proves it gone, and the attempt is replaced
// rather than waiting for a day that is never made again.
func TestStage_ADroppedDayIsReplacedWhileWaitingToPublish(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	dest := today - 2 // its next day is not made: the copy finishes and waits
	assert.Must(assert.NoError(makeArchiveDay(t, r, dest)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	root := seedWideTree(t, r.db, 3, 10, 3)
	stale := r.archive.DayEnds(dest).Add(-2 * time.Minute)
	setAttempt(t, r, root, stale)
	r.pass(ctx)
	copied, _ := treeRows(t, r, migrations.FlowsTable(dest), migrations.StepsTable(dest), root)
	assert.Must(assert.Equal(10, len(copied), "copied, the root waiting"))
	assert.Equal(10, r.large().stages[root].seen, "the waiting copy keeps its position")

	_, err = r.archive.DropBefore(ctx, dest+1)
	assert.Must(assert.NoError(err))
	assert.True(r.chunk(ctx), "the wait finds the day gone, and the attempt is replaced")
	assert.False(attemptAt(t, r, root).Equal(stale), "replaced")
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(11, visibleInDay(t, r, today, root), "published into today")
}

// TestStage_ALostPublicationResponse pins chunks, the publication's included, whose commits succeed but whose
// responses are lost: each is a failure of the job, the copy's position is kept from the commit, and a turn on a
// published tree finds it stamped and copies nothing again; the archive holds the tree once, and the live tree goes.
func TestStage_ALostPublicationResponse(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	fault := diagnostics.Join(diagnostics.FaultReapStageLost, strconv.Itoa(root))
	r.seams.InjectN(fault, 1000)
	defer r.seams.Withdraw(fault)
	r.startAttempts(ctx)
	r.chunk(ctx)
	pos, kept := r.large().stages[root]
	assert.True(kept && (pos.seen > 0 || pos.flow != 0), "the position is kept from the commit: %+v", pos)
	for i := 0; i < 50 && visibleInDay(t, r, today, root) == 0; i++ {
		r.chunk(ctx)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.True(r.logs.count("Janitor: staging an archive") > 0, "the lost responses were failures")
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
	r.cleanUp(ctx)
	_, _, there := rowsOf(t, r, root)
	assert.False(there, "the live tree is removed")
}

// TestStage_ARestartAtEveryStep pins a restart between any two steps of the protocol - before the attempt, mid-copy,
// with the copy done, after publication, mid-cleanup: a fresh janitor after every round of each job still archives the
// tree whole, once, and removes it from the live tables.
func TestStage_ARestartAtEveryStep(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	for i := 0; i < 100; i++ {
		if _, _, there := rowsOf(t, r, root); !there {
			break
		}
		if i == 50 {
			r.settings.batchRows = 4096 // the cleanup's batch holds the tree
		}
		r.restart()
		r.roundOf(ctx, JobFastArchiving)
		r.restart()
		r.chunk(ctx)
		r.restart()
		r.roundOf(ctx, JobFastDeletion)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
	assert.Equal(len(wantFlows), visibleInDay(t, r, today, root))
	_, _, there := rowsOf(t, r, root)
	assert.False(there, "the live tree is removed")
}

// TestStage_RestartInsideLargeFlow pins a restart inside a flow larger than a chunk, a root's or a descendant's: a fresh
// janitor before every chunk resumes from what the attempt's day holds, and the tree is published whole.
func TestStage_RestartInsideLargeFlow(t *testing.T) {
	t.Parallel()
	for _, shape := range []struct {
		name                            string
		rootSteps, children, childSteps int
	}{{"root", 20, 0, 0}, {"child", 1, 1, 20}} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			r := stageRig(t)
			today, err := r.archive.Today(ctx)
			assert.NoError(err)
			root := seedWideTree(t, r.db, shape.rootSteps, shape.children, shape.childSteps)
			r.startAttempts(ctx)
			for range 20 {
				r.restart()
				r.chunk(ctx)
			}
			if now, _ := r.archive.Today(ctx); now != today {
				t.Skip("midnight passed during the test")
			}
			assert.Equal(shape.children+1, visibleInDay(t, r, today, root))
		})
	}
}

// TestStage_ADroppedDayDuringTheLiveCleanup pins PurgeBefore - a drop of the day - after a tree is published there,
// while its live copy is still to be removed: the live cleanup goes on and removes it, and the published copy went
// with its day.
func TestStage_ADroppedDayDuringTheLiveCleanup(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	root := seedWideTree(t, r.db, 3, 10, 3)
	setAttempt(t, r, root, r.archive.DayEnds(yesterday).Add(-2*time.Minute))
	r.pass(ctx) // published; the live copy is larger than a batch, so it stays
	assert.Must(assert.Equal(11, visibleInDay(t, r, yesterday, root)))
	_, _, there := rowsOf(t, r, root)
	assert.Must(assert.True(there, "the live copy still there"))

	_, err = r.archive.DropBefore(ctx, today)
	assert.Must(assert.NoError(err))
	errorsBefore := r.logs.count("Janitor:")
	r.cleanUp(ctx)
	_, _, there = rowsOf(t, r, root)
	assert.False(there, "the live cleanup finished")
	assert.Equal(errorsBefore, r.logs.count("Janitor:"), "and logged nothing")
}

// TestStage_SizingCountsOnlyWhatItDecides pins fast archiving's route on real trees: a tree over the staging threshold
// is alone and has its attempt started, a root with an attempt is passed without being sized at all, and a batch's cut
// leaves it out.
func TestStage_SizingCountsOnlyWhatItDecides(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	big := seedWideTree(t, r.db, 3, 30, 3) // 124 rows
	small := seedTrees(t, r.db, 1, callShape)[0]
	cut, via, err := cutClosed(ctx, r.db, r.seams, []int{big, small}, true, r.settings.batchRows, r.settings.batchBytes, r.settings.stageRows, r.settings.stageBytes)
	assert.NoError(err)
	assert.Equal([]int{big}, cut, "over the threshold: alone")
	assert.Equal(routeStart, via)

	setAttempt(t, r, big, time.Now())
	fault := diagnostics.Join(diagnostics.FaultReapSizeErr, strconv.Itoa(big))
	r.seams.Inject(fault)
	defer r.seams.Withdraw(fault)
	cut, via, err = cutClosed(ctx, r.db, r.seams, []int{big, small}, true, r.settings.batchRows, r.settings.batchBytes, r.settings.stageRows, r.settings.stageBytes)
	assert.NoError(err, "a root with an attempt is not sized")
	assert.Equal([]int{small}, cut)
	assert.Equal(routeMove, via)
	cut, via, err = cutClosed(ctx, r.db, r.seams, []int{big}, true, r.settings.batchRows, r.settings.batchBytes, r.settings.stageRows, r.settings.stageBytes)
	assert.NoError(err)
	assert.Zero(len(cut), "only a root with an attempt: passed")
	assert.Equal(routePass, via)
}

// TestStage_TheOldestUnpublishedAttemptIsObserved pins dwarf_archive_oldest_attempt_seconds: a probe that finds an
// attempt not yet published reports its age; once the tree is published it reads zero.
func TestStage_TheOldestUnpublishedAttemptIsObserved(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	setAttempt(t, r, root, updatedAt(t, r, root).Add(-time.Minute))
	_, ok := r.oldestAttempt()
	assert.False(ok, "no reading before a probe")
	fault := diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(root))
	r.seams.InjectN(fault, 1000) // the copy fails, so the attempt stays unpublished
	r.pass(ctx)
	age, ok := r.oldestAttempt()
	assert.True(ok && age >= time.Minute, "observed: %v", age)
	time.Sleep(20 * time.Millisecond)
	later, _ := r.oldestAttempt()
	assert.True(later >= age+20*time.Millisecond, "aging on between probes: %v after %v", later, age)

	r.seams.Withdraw(fault)
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Must(assert.Equal(11, visibleInDay(t, r, today, root), "published"))
	age, _ = r.oldestAttempt()
	assert.Zero(age, "no attempt left: zero")
}

// TestStage_OldestAttemptDuringDayOutage pins dwarf_archive_oldest_attempt_seconds while tomorrow's tables are not whole
// and no new archive may start: the probe goes on reading the oldest unpublished attempt.
func TestStage_OldestAttemptDuringDayOutage(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	setAttempt(t, r, root, updatedAt(t, r, root).Add(-time.Minute))
	r.seams.InjectN(diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(root)), 1000)
	_, err = r.db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(today+1))
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	r.pass(ctx)
	age, ok := r.oldestAttempt()
	assert.True(ok && age >= time.Minute, "an unpublished minute-old attempt: %v", age)
}

// TestStage_TheCopiedRowsAreCounted pins dwarf_archive_staged_rows: every flow and step a staged copy writes is counted
// as its chunk commits, and each once - the tree's whole size by its publication.
func TestStage_TheCopiedRowsAreCounted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	seedWideTree(t, r.db, 3, 10, 3) // 11 flows, 33 steps
	r.startAttempts(ctx)
	for range 3 {
		r.chunk(ctx)
	}
	staged := func() int64 {
		var rm metricdata.ResourceMetrics
		assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
		return counterSum(rm, "dwarf_archive_staged_rows")
	}
	partial := staged()
	assert.True(partial > 0 && partial < 44, "part copied: %d", partial)
	// A position behind what is copied - another replica copied past it - rewrites rows that are there, which counts
	// nothing, and the copy is read back from the day and goes on in the same batch.
	root := r.large().current
	stale := r.large().stages[root]
	r.chunk(ctx)
	before := staged()
	r.large().stages[root] = stale
	ahead := r.large().stages[root]
	progress, took := r.largeBatch(ctx)
	assert.True(progress, "the stale position is read back from the day, and the copy goes on in the same batch")
	assert.Equal(root, took)
	assert.True(staged() > before, "copying what was not there")
	assert.True(r.large().stages[root].seen > ahead.seen, "from past the stale position: %+v", r.large().stages[root])
	r.pass(ctx)
	assert.Equal(int64(44), staged(), "the whole tree, once")
}

// archivedByStages reports whether root's tree was archived into day by stages rather than in one move: a staged
// tree's root is stamped a millisecond or more after its descendants, a moved tree's flows all at one time.
func archivedByStages(t *testing.T, r *rig, day, root int) bool {
	t.Helper()
	var rootAt, childAt time.Time
	flows := migrations.FlowsTable(day)
	if err := r.db.QueryRowContext(t.Context(), "SELECT archived_at FROM "+flows+" WHERE flow_id=?", root).Scan(&rootAt); err != nil {
		t.Fatal(err)
	}
	if err := r.db.QueryRowContext(t.Context(), "SELECT archived_at FROM "+flows+" WHERE root_flow_id=? AND flow_id<>?"+
		" ORDER BY flow_id LIMIT_OFFSET(1, 0)", root, root).Scan(&childAt); err != nil {
		t.Fatal(err)
	}
	return database.StoredTime(rootAt).Sub(database.StoredTime(childAt)) >= time.Millisecond
}

// TestStage_EveryTreeOverTheThresholdIsStaged pins the batch's cap at the staging threshold: trees over it are each
// alone in their batch and staged, even below the batch's own size, rather than moved together in one.
func TestStage_EveryTreeOverTheThresholdIsStaged(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.stageRows = 1 // every tree of more than one row
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 3, chunkShape) // seven rows each, all due together
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	for _, root := range roots {
		assert.Equal(2, visibleInDay(t, r, today, root))
		assert.True(archivedByStages(t, r, today, root), "tree %d staged", root)
	}
}

// TestStage_ATreeIsFinishedBeforeTheNext pins the order: the job copies one tree, chunk after chunk, until it publishes,
// and only then starts the next - so trees that keep arriving cannot leave every copy part-done.
func TestStage_ATreeIsFinishedBeforeTheNext(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	a := seedWideTree(t, r.db, 3, 10, 3)
	b := seedWideTree(t, r.db, 3, 10, 3)
	r.startAttempts(ctx)
	for i := 0; visibleInDay(t, r, today, a) == 0; i++ {
		assert.Must(assert.True(i < 50, "the first tree publishes"))
		r.chunk(ctx)
		copied, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), b)
		assert.Zero(len(copied), "nothing of the second tree before the first publishes")
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	r.chunk(ctx)
	pos := r.large().stages[b]
	assert.True(pos.seen > 0 || pos.flow != 0, "then the second: %+v", pos)
}

// TestStage_PublishedCleanupNotUnarchived pins the accounting of a published tree's live cleanup: its flows count as
// archived once, at publication, and never as reaped - whether its live copy goes on its own or after a Delete - while
// an unpublished tree deleted in the same batch still counts as reaped.
func TestStage_PublishedCleanupNotUnarchived(t *testing.T) {
	t.Parallel()
	for _, deleteAt := range []string{"never", "published"} {
		t.Run(deleteAt, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			r := stageRig(t)
			reader := sdkmetric.NewManualReader()
			r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
			root := seedWideTree(t, r.db, 3, 10, 3)
			r.pass(ctx)
			if deleteAt == "published" {
				deleteLive(t, r, root)
			}
			ordinary := seedTrees(t, r.db, 1, callShape)[0]
			deleteLive(t, r, ordinary)
			r.cleanUp(ctx)
			var rm metricdata.ResourceMetrics
			assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
			assert.Equal(int64(11), counterSum(rm, "dwarf_flows_archived"))
			assert.Equal(int64(2), counterSum(rm, "dwarf_flows_reaped"), "the ordinary tree alone")
			flows, _, _ := rowsOf(t, r, root)
			assert.Zero(flows, "the published live copy is removed")
		})
	}
}

// TestStage_APublishedLiveCopyDeletedInChunksIsNotReaped pins the published accounting on the chunked path: a published
// tree's live copy too large for a batch is deleted by sweep deletion in chunks, and none of its flows is counted reaped -
// they were archived.
func TestStage_APublishedLiveCopyDeletedInChunksIsNotReaped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t) // a batch of 4 rows
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	r.settings.overdue = 0
	root := seedWideTree(t, r.db, 3, 10, 3) // 44 rows
	r.pass(ctx)                             // copied, published, its live copy passed by fast deletion and deleted by sweep deletion in chunks
	f, _, _ := rowsOf(t, r, root)
	assert.Zero(f, "the live copy is deleted")
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	assert.Equal(int64(11), counterSum(rm, "dwarf_flows_archived"))
	assert.Zero(counterSum(rm, "dwarf_flows_reaped"), "archived, so not reaped")
}

// TestSize_AStagingChunkKeepsToItsBytes pins the staged copy's byte budget: a tree staged by its bytes is copied over
// several chunks, each within the batch's bytes, and published whole.
func TestSize_AStagingChunkKeepsToItsBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.stageRows, r.settings.stageBytes = 1000, 5000
	r.settings.batchRows, r.settings.batchBytes = 4096, 7000
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	root := seedWideTree(t, r.db, 2, 3, 2) // 4 flows, 8 steps
	setStepBytes(t, r, root, 3000, 0)      // 24,000 bytes
	r.startAttempts(ctx)
	var copied []int
	for i := 0; i < 40 && visibleInDay(t, r, today, root) == 0; i++ {
		r.chunk(ctx)
		var n int
		assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+migrations.StepsTable(today)).Scan(&n)))
		copied = append(copied, n)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.True(archivedByStages(t, r, today, root), "staged")
	assert.Equal(4, visibleInDay(t, r, today, root), "published whole: its four flows")
	last := 0
	for _, n := range copied {
		assert.True(n-last <= 2, "at most two steps of 3,000 bytes a chunk: %v", copied)
		last = n
	}
}

// TestStage_RetentionZeroArchivesNothing pins large-tree archiving at a retention of 0, where nothing is archived: a
// tree whose attempt an earlier run started is not copied, and fast archiving deletes it as it deletes every closed
// tree, rather than the two racing to publish and delete it.
func TestStage_RetentionZeroArchivesNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	setAttempt(t, r, root, updatedAt(t, r, root))
	assert.False(r.chunk(ctx), "no copy at a retention of 0")
	copied, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Zero(len(copied), "nothing copied")
	r.settings.batchRows = 4096
	r.pass(ctx)
	f, s, _ := rowsOf(t, r, root)
	assert.Zero(f+s, "deleted by fast archiving")
	assert.Zero(visibleInDay(t, r, today, root))
}

// TestStage_AStuckTreeDoesNotHoldTheOthers pins the round's lap: a tree whose chunk fails comes first, and the same round
// goes on to the next tree and copies a chunk of it, rather than the job waiting its idle second on each lap.
func TestStage_AStuckTreeDoesNotHoldTheOthers(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	stuck := seedWideTree(t, r.db, 3, 10, 3)
	other := seedWideTree(t, r.db, 3, 10, 3)
	r.startAttempts(ctx)
	r.seams.InjectN(diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(stuck)), 1000)
	assert.True(r.chunk(ctx), "the round goes on past the stuck tree")
	pos := r.large().stages[other]
	assert.True(pos.seen > 0 || pos.flow != 0, "a chunk of the other tree: %+v", pos)
	r.seams.Withdraw(diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(stuck)))
	assert.Equal(1, r.logs.count("Janitor: staging an archive"), "the stuck tree failed once in the lap")
}

// TestStage_AFailedAttemptStartIsLeftBehind pins fast archiving's attempt start on a root another transaction holds: no
// attempt is started, the root is passed and left behind the position - a sweep's to start - and once its attempt is
// started, large-tree archiving copies and publishes it.
func TestStage_AFailedAttemptStartIsLeftBehind(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	release := holdRows(t, r, "dwarf_flows", root)
	r.startAttempts(ctx)
	release()
	assert.False(attemptAt(t, r, root).After(noAttempt), "no attempt started on a held root")
	assert.Equal(root, r.fast().after.flowID, "passed: the position is past it")
	r.startAttempts(ctx)
	assert.False(attemptAt(t, r, root).After(noAttempt), "behind the position: not fast archiving's again")

	setAttempt(t, r, root, updatedAt(t, r, root)) // the sweep starts it
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(11, visibleInDay(t, r, today, root), "copied and published")
}

// treeIDs returns the flow ids of root's live tree.
func treeIDs(t *testing.T, r *rig, root int) []int {
	t.Helper()
	ids, _ := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	return ids
}

// treeSize sizes the rows of the flows ids in the given tables, and their steps, by their byte counts: by id, since a
// staged copy writes a flow's steps before its row.
func treeSize(t *testing.T, r *rig, flows, steps string, ids []int) size {
	t.Helper()
	var f, s size
	ctx := t.Context()
	list := database.IntList(ids)
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM("+flowBytesSQL+"), 0) FROM "+flows+
		" WHERE flow_id IN ("+list+")").Scan(&f.rows, &f.bytes); err != nil {
		t.Fatal(err)
	}
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM("+stepBytesSQL+"), 0) FROM "+steps+
		" s WHERE s.flow_id IN ("+list+")").Scan(&s.rows, &s.bytes); err != nil {
		t.Fatal(err)
	}
	return f.plus(s)
}

// TestSize_AStagedFlowRowIsChargedItsBytes pins a flow row's bytes in a staging chunk's budget: a flow row goes with the
// last of its steps only when both fit what is left, and otherwise first in the next chunk, so no chunk passes its
// bytes unless it took one row alone. Each tree has heavy flow rows: a flow of two steps of 3,000 bytes and a row of
// 6,000 against a budget of 7,000.
func TestSize_AStagedFlowRowIsChargedItsBytes(t *testing.T) {
	t.Parallel()
	const budget = 7000
	for _, shape := range []struct {
		name                    string
		rootSteps, kids, kidSts int
	}{
		{"one_flow", 2, 0, 0},
		{"a_child", 1, 1, 3}, // the child resumed part done: its last step and its row together pass the budget
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 30)
			r.settings.stageRows, r.settings.batchRows, r.settings.batchBytes = 1, 4096, budget
			today, err := r.archive.Today(ctx)
			assert.Must(assert.NoError(err))
			root := seedWideTree(t, r.db, shape.rootSteps, shape.kids, shape.kidSts)
			setStepBytes(t, r, root, 3000, 6000)
			ids := treeIDs(t, r, root)
			due, dueArgs := closedDueSQL(0)
			at, ok, err := claimAttempt(ctx, r.db, root, due, dueArgs, filing(t, r), time.Time{})
			assert.Must(assert.NoError(err))
			assert.Must(assert.True(ok, "claimed"))
			r.janitor()
			pos := stagePos{at: at}
			before := treeSize(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), ids)
			for i := 0; ; i++ {
				assert.Must(assert.True(i < 20, "not finished in 20 chunks"))
				n, _, done, err := r.large().stageChunk(ctx, r.db, root, &pos)
				assert.Must(assert.NoError(err))
				now := treeSize(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), ids)
				took := size{rows: now.rows - before.rows, bytes: now.bytes - before.bytes}
				assert.True(took.bytes <= budget || took.rows == 1, "a chunk took %d rows of %d bytes", took.rows, took.bytes)
				before = now
				if n > 0 || done {
					break
				}
			}
			if now, _ := r.archive.Today(ctx); now != today {
				t.Skip("midnight passed during the test")
			}
			assert.Equal(shape.kids+1, visibleInDay(t, r, today, root), "published whole")
		})
	}
}

// TestStage_AFailingCopyIsAFailureUntilALapFailsNone pins large-tree archiving's failure episode, reported as
// dwarf_archive_failing_seconds{job="large_archiving"}: it begins at a failed chunk, holds through every round in which
// the tree fails again - another tree's progress ends nothing - and ends once a lap over every started archive fails
// none.
func TestStage_AFailingCopyIsAFailureUntilALapFailsNone(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	failing := seedWideTree(t, r.db, 3, 10, 3)
	seedWideTree(t, r.db, 3, 10, 3) // the other tree, which copies
	r.startAttempts(ctx)
	fault := diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(failing))
	r.seams.InjectN(fault, 1000)
	r.chunk(ctx)
	assert.True(r.large().failingFor(time.Now()) > 0, "a failed chunk begins an episode")
	for range 6 {
		r.chunk(ctx) // the other tree copies; the failing one fails again on every lap
	}
	time.Sleep(20 * time.Millisecond)
	assert.True(r.large().failingFor(time.Now()) >= 20*time.Millisecond, "held through the rounds since")

	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	var labelled bool
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok && m.Name == "dwarf_archive_failing_seconds" {
				for _, dp := range g.DataPoints {
					if job, _ := dp.Attributes.Value("job"); job.AsString() == JobLargeArchiving && dp.Value > 0 {
						labelled = true
					}
				}
			}
		}
	}
	assert.True(labelled, "reported under the job's name")

	r.seams.Withdraw(fault)
	r.pass(ctx)
	assert.Zero(r.large().failingFor(time.Now()), "a lap that failed none ended it")
}

// TestStage_ContinuingATreeIsNoLap pins that only a scan that reads from the start ends a lap - never one that merely
// continues the tree in progress, whatever its flow_id. With the first tree at flow_id 1, continuing it was taken for a
// wrap on every chunk, and once it recovered its next chunk ended the failure episode while the other tree, not yet
// taken again, was still failing.
func TestStage_ContinuingATreeIsNoLap(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	first := seedWideTree(t, r.db, 3, 10, 3)
	other := seedWideTree(t, r.db, 3, 10, 3)
	assert.Must(assert.Equal(1, first, "the first tree is flow_id 1"))
	r.startAttempts(ctx)
	firstFault := diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(first))
	otherFault := diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(other))
	r.seams.InjectN(firstFault, 1000)
	r.seams.InjectN(otherFault, 1000)
	defer r.seams.Withdraw(otherFault)
	r.chunk(ctx)
	assert.Must(assert.True(r.large().failingFor(time.Now()) > 0, "both trees failed"))

	r.seams.Withdraw(firstFault)
	for range 3 {
		r.chunk(ctx) // the first tree recovers, and is continued chunk after chunk
	}
	assert.Equal(first, r.large().current, "still on the first tree")
	assert.True(r.large().failingFor(time.Now()) > 0, "the other tree, not taken again, is still failing")
}
