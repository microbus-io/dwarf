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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// stageRig is a rig that archives a tree of more than 8 rows in stages, 4 rows a chunk.
func stageRig(t *testing.T) *rig {
	t.Helper()
	r := newRig(t, 30)
	r.cfg.StageRows = 8
	r.cfg.BatchRows = 4
	return r
}

// oneChunk makes each walk's slice end after its first chunk and a pass end after one round of its walks, so a pass
// copies a chunk of a staged tree in each of its two archive walks, the fast and the repair.
func oneChunk(r *rig) {
	r.cfg.Slice, r.cfg.Budget = time.Nanosecond, time.Nanosecond
}

// fullPasses undoes oneChunk.
func fullPasses(r *rig) {
	r.cfg.Slice, r.cfg.Budget = 5*time.Second, 30*time.Second
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

// TestStage_ALargeTreeIsPublishedWhole pins the staged path end to end: a tree over StageRows is copied in chunks of
// BatchRows into the day of its attempt, published - every flow and step there, the root last - and then removed from
// the live tables by the deletion kind. Every copied flow is visible once the root is in.
func TestStage_ALargeTreeIsPublishedWhole(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ALargeTreeIsPublishedWhole")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3) // 11 flows, 33 steps
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)

	r.pass(ctx)
	r.pass(ctx) // the deletion walk, which runs first, removes the live tree published by the last pass
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
	assert.Equal(len(wantFlows), visibleInDay(t, r, today, root))
	f, s, _ := rowsOf(t, r, root)
	assert.Zero(f+s, "the live tree is removed once published")
}

// TestStage_StampGroupsAreAMillisecondApart pins the archive times of a staged tree: its descendants in flow_id order
// share one time per 4,096, a millisecond apart from the attempt's time on, and the root takes the next millisecond -
// so no staged tree puts more than a group on one time, however its chunks fell.
func TestStage_StampGroupsAreAMillisecondApart(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_StampGroupsAreAMillisecondApart")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.cfg.BatchRows = 1000
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

// TestStage_ARestartJoinsItsAttempt pins a copy cut off part-way: a fresh Reaper, which knows nothing of it, joins the
// same attempt and finishes the copy, rewriting what was there with the same rows - none twice, none missing.
func TestStage_ARestartJoinsItsAttempt(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ARestartJoinsItsAttempt")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	for range 3 {
		r.pass(ctx)
	}
	at := attemptAt(t, r, root)
	assert.True(at.After(noAttempt), "an attempt started")
	copied, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.True(len(copied) > 0 && len(copied) < len(wantFlows), "part copied: %d", len(copied))
	assert.Zero(visibleInDay(t, r, today, root), "nothing visible before the root is in")

	r.reaper = New() // a restart
	fullPasses(r)
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
}

// TestStage_TwoCopiersShareAnAttempt pins two Reapers - two replicas - taking turns on one tree: each joins the one
// attempt, they write the same rows, and the tree is published once, whole.
func TestStage_TwoCopiersShareAnAttempt(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_TwoCopiersShareAnAttempt")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	a, b := r.reaper, New()
	var first time.Time
	for i := 0; i < 200; i++ {
		copier := a
		if i%2 == 1 {
			copier = b
		}
		copier.Pass(ctx, r.cfg, r.shard())
		if _, _, there := rowsOf(t, r, root); !there {
			break
		}
		at := attemptAt(t, r, root)
		if first.IsZero() {
			first = at
		}
		assert.True(at.Equal(first), "the copiers keep one attempt")
	}
	_, _, there := rowsOf(t, r, root)
	assert.False(there, "published, and the live tree removed")
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
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ADeleteAbandonsTheCopy")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	for range 2 {
		r.pass(ctx)
	}
	copied, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Must(assert.True(len(copied) > 0, "part copied"))

	result, err := r.reaper.DeleteLive(ctx, r.db, r.archive.DayLength(), root)
	assert.NoError(err)
	assert.Equal(1, result.Live)
	assert.False(attemptAt(t, r, root).After(noAttempt), "the stamp clears the attempt")
	fullPasses(r)
	r.cfg.BatchRows = 4096
	r.pass(ctx)
	f, s, _ := rowsOf(t, r, root)
	assert.Zero(f+s, "the live tree is deleted")
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	after, _ := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(copied, after, "the copier stopped")
	assert.Zero(visibleInDay(t, r, today, root), "the abandoned rows are invisible")
}

// TestStage_AnAttemptKeepsItsDay pins an attempt that started before midnight: a later visit joins it and publishes
// into its day, the earlier one, every archive time inside that day.
func TestStage_AnAttemptKeepsItsDay(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AnAttemptKeepsItsDay")
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
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ADroppedDayIsReplaced")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	stale := r.archive.DayEnds(yesterday).Add(-2 * time.Minute)
	setAttempt(t, r, root, stale)
	r.pass(ctx)
	copied, _ := treeRows(t, r, migrations.FlowsTable(yesterday), migrations.StepsTable(yesterday), root)
	assert.Must(assert.True(len(copied) > 0, "part copied into yesterday"))

	_, err = r.archive.DropBefore(ctx, today)
	assert.Must(assert.NoError(err))
	r.pass(ctx) // the chunk finds the day gone, and the attempt is replaced
	at := attemptAt(t, r, root)
	assert.False(at.Equal(stale), "replaced")
	assert.Equal(today, archiver.DayOf(at, r.archive.DayLength()))
	fullPasses(r)
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
}

// TestStage_AnAttemptedRootIsStagedWhateverItsSize pins the atomic path's fence: an atomic move skips a root whose staged
// archive has started, and the walk sends it to the staged path even with staging off for new trees - so a tree is
// never archived both ways.
func TestStage_AnAttemptedRootIsStagedWhateverItsSize(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AnAttemptedRootIsStagedWhateverItsSize")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedTrees(t, r.db, 1, callShape)[0]
	var now time.Time
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT updated_at FROM dwarf_flows WHERE flow_id=?", root).Scan(&now)))
	setAttempt(t, r, root, database.StoredTime(now))
	moved, err := r.reaper.MoveTrees(ctx, r.db, r.archive, today, []int{root})
	assert.NoError(err)
	assert.Zero(moved, "the atomic move skips it")

	r.pass(ctx) // StageRows is 0: no new staging, but this one goes on
	r.pass(ctx)
	if d, _ := r.archive.Today(ctx); d != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(2, visibleInDay(t, r, today, root), "published by the staged path")
	_, _, there := rowsOf(t, r, root)
	assert.False(there)
}

// TestStage_APublishedTreeIsNotCopiedAgain pins a lost publication response: stamped with its attempt, the tree is
// published, so a visit that comes back to it finds nothing to stage and copies nothing.
func TestStage_APublishedTreeIsNotCopiedAgain(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_APublishedTreeIsNotCopiedAgain")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	k := closedDue(0, 0)
	d := &filingDay{}
	assert.Must(assert.NoError(r.p().refreshDay(ctx, d)))
	n, finished, err := r.p().stage(ctx, k, root, d, time.Now())
	assert.NoError(err)
	assert.True(finished)
	assert.Equal(11, n, "published")
	n, finished, err = r.p().stage(ctx, k, root, d, time.Now())
	assert.NoError(err)
	assert.True(finished, "not this walk's: stamped")
	assert.Zero(n)
}

// TestStage_AnOpenFlowHoldsTheTree pins the guard against a tree that holds a flow that is not closed, which only a
// bug leaves: the copier refuses it, and nothing is published.
func TestStage_AnOpenFlowHoldsTheTree(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AnOpenFlowHoldsTheTree")
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
}

// TestStage_TheDirectoryNamesALateFlow pins the staged copy's directory rows: a flow a locate would not find by its
// creation hour alone is named in the directory with its day, and found.
func TestStage_TheDirectoryNamesALateFlow(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_TheDirectoryNamesALateFlow")
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
// flows table goes first, so the publication fails on it and the attempt is replaced, rather than a root being inserted
// beside steps that are gone.
func TestStage_AHalfDroppedDayIsReplaced(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AHalfDroppedDayIsReplaced")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	root := seedWideTree(t, r.db, 3, 10, 3)
	stale := r.archive.DayEnds(yesterday).Add(-2 * time.Minute)
	setAttempt(t, r, root, stale)
	r.pass(ctx)
	_, err = r.db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(yesterday))
	assert.Must(assert.NoError(err))
	r.pass(ctx)
	at := attemptAt(t, r, root)
	assert.False(at.Equal(stale), "replaced")
	fullPasses(r)
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(11, visibleInDay(t, r, today, root), "published into today")
}

// TestStage_AFailedChunkIsRetried pins a staging chunk that fails: it is a failure of the walk like a failed move - the
// walkers pass the tree, and the repair walker's next sweep comes back to it - the copy keeps its attempt, and the
// later try goes on and publishes the tree whole.
func TestStage_AFailedChunkIsRetried(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AFailedChunkIsRetried")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	r.pass(ctx)
	at := attemptAt(t, r, root)
	fault := join(FaultStageChunk, strconv.Itoa(root))
	r.seams.InjectN(fault, 2)
	r.pass(ctx)
	r.seams.Withdraw(fault)
	assert.True(attemptAt(t, r, root).Equal(at), "the attempt holds through a failed chunk")
	fullPasses(r)
	r.pass(ctx) // both walkers passed the failed tree; the repair walker's next sweep comes back to it
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
}

// TestStage_AHeldRootKeepsItsCopysPosition pins a copy whose root another transaction holds - another copier, a Fork:
// the visit yields, and the copy's position is kept, so a later visit goes on rather than rereading what it copied.
func TestStage_AHeldRootKeepsItsCopysPosition(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AHeldRootKeepsItsCopysPosition")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	root := seedWideTree(t, r.db, 3, 10, 3)
	r.pass(ctx)
	key := stageKey{1, root}
	before := r.reaper.stages.get(key)
	assert.Must(assert.True(before.seen > 0, "part copied"))
	release := holdRows(t, r, "dwarf_flows", root)
	r.pass(ctx)
	release()
	assert.Equal(before, r.reaper.stages.get(key), "kept while the root was held")
	// The walkers passed the held root, as any held root; the repair walker's next sweep comes back to it.
	for range 3 {
		r.pass(ctx)
	}
	after := r.reaper.stages.get(key)
	assert.True(after.seen > before.seen && after.at.Equal(before.at), "and gone on from: %+v", after)
}

// TestStage_APublishedCopyIsForgotten pins the cursor map: a copy that publishes leaves no position behind, and past
// the bound the least recently used position goes, not every one.
func TestStage_APublishedCopyIsForgotten(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_APublishedCopyIsForgotten")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	r.pass(ctx)
	assert.Zero(r.reaper.stages.get(stageKey{1, root}), "published: forgotten")

	var s stages
	for i := range maxStages {
		s.put(stageKey{1, i}, stagePos{seen: i + 1})
	}
	s.put(stageKey{1, 0}, stagePos{seen: 100}) // used again: no longer the least recent
	s.put(stageKey{1, maxStages}, stagePos{seen: 1})
	assert.Equal(100, s.get(stageKey{1, 0}).seen)
	assert.Zero(s.get(stageKey{1, 1}).seen, "the least recently used went")
	assert.Equal(3, s.get(stageKey{1, 2}).seen, "and only it")
}

// TestStage_AnAttemptGoesOnWhenNoneMayStart pins the archive gate against a staged copy already started: with
// tomorrow's tables not whole no new archive starts - neither a move nor an attempt - but an attempt into a retained
// day goes on and publishes there; and an attempt into today copies but does not publish, since a flow is located
// only where the day after its own is whole.
func TestStage_AnAttemptGoesOnWhenNoneMayStart(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AnAttemptGoesOnWhenNoneMayStart")
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

	for range 3 {
		r.pass(ctx)
	}
	assert.Equal(11, visibleInDay(t, r, yesterday, old), "the started attempt published into its day")
	assert.False(attemptAt(t, r, fresh).After(noAttempt), "no new attempt")
	_, _, there := rowsOf(t, r, small)
	assert.True(there, "no move")

	var now time.Time
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT updated_at FROM dwarf_flows WHERE flow_id=?", fresh).Scan(&now)))
	setAttempt(t, r, fresh, database.StoredTime(now))
	for range 3 {
		r.pass(ctx)
	}
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
	var now time.Time
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT updated_at FROM dwarf_flows WHERE flow_id=?", root).Scan(&now)))
	old := database.StoredTime(now)
	setAttempt(t, r, root, old.Add(time.Second)) // the attempt the copier does not hold
	pos := stagePos{at: old}
	n, _, done, err := r.p().stageChunk(ctx, root, r.archive, &pos)
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
	d := &filingDay{}
	assert.Must(assert.NoError(r.p().refreshDay(ctx, d)))
	k := closedDue(0, 0)
	first, ok, err := r.p().claimAttempt(ctx, k, root, d, stale)
	assert.NoError(err)
	assert.True(ok && !first.Equal(stale), "replaced")
	time.Sleep(5 * time.Millisecond) // a second replacement would then write another time
	second, ok, err := New().pass1(r).claimAttempt(ctx, k, root, d, stale)
	assert.NoError(err)
	assert.True(ok && second.Equal(first), "joined, not replaced again")
}

// pass1 is a pass of this Reaper over the rig's shard.
func (r *Reaper) pass1(rg *rig) *pass {
	return &pass{r: r, cfg: rg.cfg, shard: rg.shard()}
}

// TestStage_ANewAttemptNeedsRoomInItsDay pins where a new attempt may start: not past the filing day's stop, not with
// tomorrow not whole, and not when its time falls outside the filing day - past midnight - each leaving no attempt.
func TestStage_ANewAttemptNeedsRoomInItsDay(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	k := closedDue(0, 0)
	fresh := func() *filingDay {
		d := &filingDay{}
		assert.Must(assert.NoError(r.p().refreshDay(ctx, d)))
		return d
	}
	for name, d := range map[string]*filingDay{
		"past the stop":      func() *filingDay { d := fresh(); d.stop = time.Now().Add(-time.Second); return d }(),
		"tomorrow not whole": func() *filingDay { d := fresh(); d.notWhole = true; return d }(),
		"not the filing day": func() *filingDay { d := fresh(); d.day--; return d }(),
	} {
		_, ok, err := r.p().claimAttempt(ctx, k, root, d, time.Time{})
		assert.NoError(err, name)
		assert.False(ok, name)
		assert.False(attemptAt(t, r, root).After(noAttempt), "%s: no attempt", name)
	}
	_, ok, err := r.p().claimAttempt(ctx, k, root, fresh(), time.Time{})
	assert.NoError(err)
	assert.True(ok, "with room, it starts")
}

// TestStage_NoArchiveTimeIsClamped pins the day-end check: a tree whose archive times would run past its attempt's day
// is held, not stamped into the day's last millisecond.
func TestStage_NoArchiveTimeIsClamped(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_NoArchiveTimeIsClamped")
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
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AStaleDirectoryRowIsMoved")
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
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_FlowsLargerThanAChunk")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 10, 3, 9) // children of 9 steps and a root of 10, against a budget of 4
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	for range 40 {
		if _, _, there := rowsOf(t, r, root); !there {
			break
		}
		r.pass(ctx)
	}
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
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ConcurrentCopiers")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 30, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			copier := New()
			for range 100 {
				if _, _, there := rowsOf(t, r, root); !there {
					return
				}
				copier.Pass(ctx, r.cfg, r.shard())
			}
		}()
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

// TestStage_AStartedAttemptBehindABatchIsReached pins the scan that goes on with started attempts while no new archive
// may start: an attempt whose root is behind a batch of roots that have none is still found, past them, and published
// into its day, while they stay live for the day to open.
func TestStage_AStartedAttemptBehindABatchIsReached(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_AStartedAttemptBehindABatchIsReached")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.cfg.Batch = 2
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
	for range 5 {
		r.pass(ctx)
	}
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
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ADroppedDayIsReplacedWhileWaitingToPublish")
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

	_, err = r.archive.DropBefore(ctx, dest+1)
	assert.Must(assert.NoError(err))
	oneChunk(r)
	r.p().closed(ctx) // the wait finds the day gone, and the attempt is replaced; the live root is still there to read
	fullPasses(r)
	assert.False(attemptAt(t, r, root).Equal(stale), "replaced")
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(11, visibleInDay(t, r, today, root), "published into today")
}

// TestStage_ALostPublicationResponse pins chunks, the publication's included, whose commits succeed but whose
// responses are lost: each is a failure of the walk, the copy's position is kept from the commit, and a visit to a
// published tree finds it stamped and copies nothing again; the archive holds the tree once, and the live tree goes.
func TestStage_ALostPublicationResponse(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ALostPublicationResponse")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	fault := join(FaultStageLost, strconv.Itoa(root))
	r.seams.InjectN(fault, 1000)
	defer r.seams.Withdraw(fault)
	for range 50 {
		if _, _, there := rowsOf(t, r, root); !there {
			break
		}
		r.pass(ctx)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.True(r.logs.count("Reaper:") > 0, "the lost responses were failures")
	gotFlows, gotSteps := treeRows(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	assert.Equal(wantFlows, gotFlows)
	assert.Equal(wantSteps, gotSteps)
	_, _, there := rowsOf(t, r, root)
	assert.False(there, "the live tree is removed")
}

// TestStage_ARestartAtEveryStep pins a restart between any two steps of the protocol - before the attempt, mid-copy,
// with the copy done, after publication, mid-cleanup: a fresh Reaper after every pass of one chunk each still archives
// the tree whole, once, and removes it from the live tables.
func TestStage_ARestartAtEveryStep(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ARestartAtEveryStep")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	wantFlows, wantSteps := treeRows(t, r, "dwarf_flows", "dwarf_steps", root)
	for range 100 {
		if _, _, there := rowsOf(t, r, root); !there {
			break
		}
		r.reaper = New()
		r.reaper.SetSeams(r.seams)
		r.pass(ctx)
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

// TestStage_ADroppedDayDuringTheLiveCleanup pins PurgeBefore - a drop of the day - after a tree is published there,
// while its live copy is still to be removed: the live cleanup goes on and removes it, and the published copy went
// with its day.
func TestStage_ADroppedDayDuringTheLiveCleanup(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_ADroppedDayDuringTheLiveCleanup")
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
	r.p().closed(ctx) // the archive walk alone: published, its live copy not yet removed
	assert.Must(assert.Equal(11, visibleInDay(t, r, yesterday, root)))
	_, _, there := rowsOf(t, r, root)
	assert.Must(assert.True(there, "the live copy still there"))

	_, err = r.archive.DropBefore(ctx, today)
	assert.Must(assert.NoError(err))
	errorsBefore := r.logs.count("Reaper:")
	r.pass(ctx)
	_, _, there = rowsOf(t, r, root)
	assert.False(there, "the live cleanup finished")
	assert.Equal(errorsBefore, r.logs.count("Reaper:"), "and logged nothing")
}

// TestStage_SizingCountsOnlyWhatItDecides pins the sizing's route on real trees: a tree over the staging threshold is
// alone and staged, a root with an attempt is staged without being counted at all, and a batch's cut ends before one.
// The caps the counts take are TestMove_BatchesAreCappedByRows'.
func TestStage_SizingCountsOnlyWhatItDecides(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	big := seedWideTree(t, r.db, 3, 30, 3) // 124 rows
	small := seedTrees(t, r.db, 1, callShape)[0]
	cut, via, ages, err := r.p().rootsWithin(ctx, []int{big, small}, true)
	assert.NoError(err)
	assert.Equal([]int{big}, cut, "over the threshold: alone")
	assert.Equal(inStages, via)
	assert.Zero(len(ages))

	setAttempt(t, r, big, time.Now())
	r.seams.Inject(join(FaultSizeErr, strconv.Itoa(big)))
	defer r.seams.Withdraw(join(FaultSizeErr, strconv.Itoa(big)))
	cut, via, ages, err = r.p().rootsWithin(ctx, []int{big, small}, true)
	assert.NoError(err, "a root with an attempt is not sized")
	assert.Equal([]int{big}, cut)
	assert.Equal(inStages, via)
	assert.Equal(1, len(ages))
	cut, via, _, err = r.p().rootsWithin(ctx, []int{small, big}, true)
	assert.NoError(err)
	assert.Equal([]int{small}, cut, "the cut ends before a root with an attempt")
	assert.Equal(inBatch, via)
}

// TestStage_TheOldestUnpublishedAttemptIsObserved pins the observation behind dwarf_archive_oldest_attempt_seconds: a
// repair sweep that sees an attempt not yet published reports its age, aging since; once the tree is published and a
// later sweep completes without one, it reads zero.
func TestStage_TheOldestUnpublishedAttemptIsObserved(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_TheOldestUnpublishedAttemptIsObserved")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	root := seedWideTree(t, r.db, 3, 10, 3)
	var now time.Time
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT updated_at FROM dwarf_flows WHERE flow_id=?", root).Scan(&now)))
	setAttempt(t, r, root, database.StoredTime(now).Add(-time.Minute))
	fault := join(FaultStageChunk, strconv.Itoa(root))
	r.seams.InjectN(fault, 1000) // the copy fails, so the attempt stays unpublished
	r.pass(ctx)
	r.pass(ctx)
	obs := r.reaper.Observe(1, time.Now())
	assert.True(obs.Kinds[0].OldestAttempt >= time.Minute, "observed: %v", obs.Kinds[0].OldestAttempt)
	r.seams.Withdraw(fault)
	for range 4 {
		r.pass(ctx)
	}
	_, _, there := rowsOf(t, r, root)
	assert.Must(assert.False(there, "published and cleaned up"))
	assert.Zero(r.reaper.Observe(1, time.Now()).Kinds[0].OldestAttempt, "a sweep with no attempt reads zero")
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

// TestStage_ATreeThatMissesItsDeadlineIsStaged pins the fallback: a tree under the staging threshold whose atomic move
// runs into its deadline is remembered, and its next visit takes the staged path - never another atomic move - once
// its root can be claimed. On MySQL the timed-out statement runs on, holding the root, so the visits in between find
// it held, keep the preference and yield.
func TestStage_ATreeThatMissesItsDeadlineIsStaged(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; preferences go with the attempt deadline at step 7")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.cfg.StageRows = 1 << 20 // far over the tree: size alone would never stage it
	r.cfg.BatchRows = 4096
	r.cfg.Attempt = 500 * time.Millisecond
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	var child int
	assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT MAX(flow_id) FROM dwarf_flows WHERE root_flow_id=?", root).Scan(&child)))
	release := holdRows(t, r, "dwarf_steps", child) // the move waits on it past its deadline
	r.pass(ctx)
	if _, _, there := rowsOf(t, r, root); there && !attemptAt(t, r, root).After(noAttempt) {
		assert.True(r.reaper.prefer.has(stageKey{1, root}), "remembered")
		r.pass(ctx) // a visit that cannot claim the root keeps the preference
		if !attemptAt(t, r, root).After(noAttempt) {
			assert.True(r.reaper.prefer.has(stageKey{1, root}), "still remembered while the root is held")
		}
	}
	release()
	r.cfg.Attempt = 30 * time.Second
	for range 20 {
		if _, _, there := rowsOf(t, r, root); !there {
			break
		}
		time.Sleep(100 * time.Millisecond) // MySQL's timed-out statement ends once the row is released
		r.pass(ctx)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(11, visibleInDay(t, r, today, root))
	assert.True(archivedByStages(t, r, today, root), "archived in stages, not by another move")
	assert.False(r.reaper.prefer.has(stageKey{1, root}), "forgotten once its attempt held it")
}

// TestStage_APreferenceWaitsForItsRoot pins a remembered tree whose root stays held: every visit yields and keeps the
// preference, never moving it atomically, and the trees beside it are archived meanwhile.
func TestStage_APreferenceWaitsForItsRoot(t *testing.T) {
	t.Skip("janitor step 6: closed trees are archived by the janitor's fast and sweep archiving; preferences go with the attempt deadline at step 7")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.cfg.StageRows = 1 << 20
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedTrees(t, r.db, 1, callShape)[0]
	other := seedTrees(t, r.db, 1, callShape)[0]
	r.reaper.prefer.add(stageKey{1, root})
	release := holdRows(t, r, "dwarf_flows", root)
	for range 3 {
		r.pass(ctx)
	}
	release()
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.True(r.reaper.prefer.has(stageKey{1, root}), "kept")
	_, _, there := rowsOf(t, r, root)
	assert.True(there, "not moved")
	assert.Equal(2, visibleInDay(t, r, today, other), "the tree beside it archived")
}

// TestStage_APreferenceIsHarmlessOnceItsTreeIsGone pins a preference whose tree went another way: deleted, or moved
// by a commit whose response was lost. The deletion forgets it, and a full set of preferences makes room by dropping
// those whose trees are gone - never one still pending.
func TestStage_APreferenceIsHarmlessOnceItsTreeIsGone(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	deleted := seedTrees(t, r.db, 1, callShape)[0]
	r.reaper.prefer.add(stageKey{1, deleted})
	_, err = r.reaper.DeleteLive(ctx, r.db, r.archive.DayLength(), deleted)
	assert.NoError(err)
	r.pass(ctx)
	_, _, there := rowsOf(t, r, deleted)
	assert.False(there)
	assert.False(r.reaper.prefer.has(stageKey{1, deleted}), "the deletion forgot it")

	moved := seedTrees(t, r.db, 1, callShape)[0]
	pending := seedTrees(t, r.db, 1, callShape)[0]
	_, err = r.reaper.MoveTrees(ctx, r.db, r.archive, today, []int{moved}) // as if its response was lost
	assert.NoError(err)
	r.reaper.prefer.add(stageKey{1, moved})
	r.reaper.prefer.add(stageKey{1, pending})
	for i := range maxPreferred - 2 {
		r.reaper.prefer.add(stageKey{1, 1_000_000 + i}) // full of trees that are gone
	}
	fresh := seedTrees(t, r.db, 1, callShape)[0]
	r.p().preferStaging(ctx, fresh)
	assert.True(r.reaper.prefer.has(stageKey{1, fresh}), "room made")
	assert.True(r.reaper.prefer.has(stageKey{1, pending}), "a pending preference kept")
	assert.False(r.reaper.prefer.has(stageKey{1, moved}), "a gone tree's dropped")
}

// TestStage_ARestartForgetsOnlyThePreference pins a restart after a preference started its tree's attempt: the fresh
// Reaper knows no preference, but the attempt, being in the database, is resumed and the tree published.
func TestStage_ARestartForgetsOnlyThePreference(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; preferences go with the attempt deadline at step 7")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.cfg.StageRows = 1 << 20
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	root := seedWideTree(t, r.db, 3, 10, 3)
	r.reaper.prefer.add(stageKey{1, root})
	r.pass(ctx)
	assert.Must(assert.True(attemptAt(t, r, root).After(noAttempt), "the preference started an attempt"))
	r.reaper = New()
	r.reaper.SetSeams(r.seams)
	fullPasses(r)
	for range 3 {
		r.pass(ctx)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal(11, visibleInDay(t, r, today, root))
	assert.True(archivedByStages(t, r, today, root))
}

// stagedRows sums dwarf_archive_staged_rows on reader.
func stagedRows(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	var n int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == "dwarf_archive_staged_rows" {
				for _, dp := range sum.DataPoints {
					n += dp.Value
				}
			}
		}
	}
	return n
}

// TestStage_TheCopiedRowsAreCounted pins dwarf_archive_staged_rows: every flow and step a staged copy writes is counted
// as its chunk commits, and each once - the tree's whole size by its publication.
func TestStage_TheCopiedRowsAreCounted(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_TheCopiedRowsAreCounted")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	reader := sdkmetric.NewManualReader()
	assert.Must(assert.NoError(r.reaper.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))))
	oneChunk(r)
	seedWideTree(t, r.db, 3, 10, 3) // 11 flows, 33 steps
	for range 3 {
		r.pass(ctx)
	}
	partial := stagedRows(t, reader)
	assert.True(partial > 0 && partial < 44, "part copied: %d", partial)
	fullPasses(r)
	r.pass(ctx)
	assert.Equal(int64(44), stagedRows(t, reader), "the whole tree, once")
}

// TestStage_EveryTreeOverTheThresholdIsStaged pins the batch's cap at the staging threshold: trees over it are each
// alone in their batch and staged, even below the batch's own size, rather than moved together in one.
func TestStage_EveryTreeOverTheThresholdIsStaged(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_EveryTreeOverTheThresholdIsStaged")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.cfg.StageRows = 1 // every tree of more than one row
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 3, chunkShape) // seven rows each, all due together
	for range 3 {
		r.pass(ctx)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	for _, root := range roots {
		assert.Equal(2, visibleInDay(t, r, today, root))
		assert.True(archivedByStages(t, r, today, root), "tree %d staged", root)
	}
}

// TestStage_TheAttemptGaugeFallsOnceItPublishes pins dwarf_archive_oldest_attempt_seconds while no new archive may
// start: the scan that goes on with started attempts makes progress every pass, and once the attempt has published,
// its wrap is a complete reading without it, so the gauge reads zero rather than aging on.
func TestStage_TheAttemptGaugeFallsOnceItPublishes(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestStage_TheOldestUnpublishedAttemptIsObserved")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	yesterday := today - 1
	assert.Must(assert.NoError(makeArchiveDay(t, r, yesterday)))
	root := seedWideTree(t, r.db, 3, 10, 3)
	seedTrees(t, r.db, 2, callShape) // due roots with no attempt, so the repair sweep cannot finish while blocked
	setAttempt(t, r, root, r.archive.DayEnds(yesterday).Add(-2*time.Minute))
	_, err = r.db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(today+1))
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))

	key := stageKey{1, root}
	last := -1
	for range 40 {
		if _, _, there := rowsOf(t, r, root); !there {
			break
		}
		r.pass(ctx)
		if pos := r.reaper.stages.get(key); pos.seen > 0 {
			assert.True(pos.seen >= last, "progress kept: %d after %d", pos.seen, last)
			last = pos.seen
		}
	}
	assert.Equal(11, visibleInDay(t, r, yesterday, root), "published into its day while blocked")
	for range 3 {
		r.pass(ctx)
	}
	assert.Zero(r.reaper.Observe(1, time.Now()).Kinds[0].OldestAttempt, "no attempt left: the gauge falls")
}
