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
	"testing"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// deleteMarked is the rig's archive deletion sweep.
func (r *rig) deleteMarked() *DeleteMarked {
	r.janitor()
	return r.dm
}

// archiveDeletionRounds runs the archive deletion sweep's rounds until one makes no progress, and returns how many made
// progress.
func (r *rig) archiveDeletionRounds(ctx context.Context) (n int) {
	r.deleteMarked()
	for r.roundOf(ctx, JobArchiveDeletionSweep) {
		n++
	}
	return n
}

// archiveTree moves the tree rooted at root into day's tables and returns its flows.
func archiveTree(t *testing.T, r *rig, root, day int) []int {
	t.Helper()
	flows := treeIDs(t, r, root)
	due, dueArgs := closedDueSQL(0)
	if _, err := move(t.Context(), r.db, r.seams, []int{root}, due, dueArgs, &filingDay{day: day, archive: r.archive}); err != nil {
		t.Fatal(err)
	}
	return flows
}

// markArchived marks the archived root in day for deletion, as a Delete does.
func markArchived(t *testing.T, r *rig, day int, roots ...int) {
	t.Helper()
	if _, err := r.db.ExecContext(t.Context(), "UPDATE "+migrations.FlowsTable(day)+" SET delete_after_ms=1 WHERE flow_id IN ("+
		idsSQL(roots)+")"); err != nil {
		t.Fatal(err)
	}
}

// archivedRows counts the flows and steps of flows in day's tables, and the directory's rows for them.
func archivedRows(t *testing.T, r *rig, day int, flows []int) (f, s, named int) {
	t.Helper()
	list := idsSQL(flows)
	for _, q := range []struct {
		sql string
		n   *int
	}{
		{"SELECT COUNT(*) FROM " + migrations.FlowsTable(day) + " WHERE flow_id IN (" + list + ")", &f},
		{"SELECT COUNT(*) FROM " + migrations.StepsTable(day) + " WHERE flow_id IN (" + list + ")", &s},
		{"SELECT COUNT(*) FROM " + archiver.DirectoryTable + " WHERE flow_id IN (" + list + ")", &named},
	} {
		if err := r.db.QueryRowContext(t.Context(), q.sql).Scan(q.n); err != nil {
			t.Fatal(err)
		}
	}
	return f, s, named
}

// TestDeleteMarked_TheDaySuffixNamesTheDaysTables pins daySuffix to the day tables' names, flows and steps alike.
func TestDeleteMarked_TheDaySuffixNamesTheDaysTables(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.Equal(migrations.FlowsTable(20412), "dwarf_flows"+daySuffix(20412))
	assert.Equal(migrations.StepsTable(20412), "dwarf_steps"+daySuffix(20412))
}

// TestDeleteMarked_SmallTreesGoTogether pins the batching: marked trees a batch holds are deleted together - the
// sweep's first batch takes one, the next two - and the rest of every archived tree is left.
func TestDeleteMarked_SmallTreesGoTogether(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(makeArchiveDay(t, r, today-1)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	day := today - 1 // filed before their creation day: the directory names their flows
	roots := seedTrees(t, r.db, 4, callShape)
	var flows [][]int
	for _, root := range roots {
		flows = append(flows, archiveTree(t, r, root, day))
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the trees were archived")
	}
	markArchived(t, r, day, roots[:3]...)
	before := r.seams.Visits(diagnostics.CheckpointReapArchiveDeleted)
	r.archiveDeletionRounds(ctx)
	assert.Equal(2, r.seams.Visits(diagnostics.CheckpointReapArchiveDeleted)-before, "one root, then two")
	for i := range roots {
		f, s, named := archivedRows(t, r, day, flows[i])
		if i == 3 {
			assert.Equal([3]int{2, 2, 2}, [3]int{f, s, named}, "not marked: left whole")
		} else {
			assert.Zero(f+s+named, "root %d, its directory rows with it", i)
		}
	}
}

// TestDeleteMarked_ALargeTreeInChunksOneAtATime pins a marked tree larger than a batch: deleted in chunks of at most
// the batch's rows on its day's tables, the directory's rows for its flows with them, the root last, and the next large
// tree of the same day only once it is gone. Until a sweep completes the gauges report nothing: zero would claim every
// day examined.
func TestDeleteMarked_ALargeTreeInChunksOneAtATime(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	r.settings.batchRows = 4
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(makeArchiveDay(t, r, today-1)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	day := today - 1                        // filed before their creation day: the directory names their flows
	first := seedWideTree(t, r.db, 2, 6, 3) // 27 rows
	second := seedWideTree(t, r.db, 2, 6, 3)
	firstFlows := archiveTree(t, r, first, day)
	secondFlows := archiveTree(t, r, second, day)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the trees were archived")
	}
	_, _, named := archivedRows(t, r, day, firstFlows)
	assert.Must(assert.Equal(7, named, "the directory names the first tree's flows"))
	markArchived(t, r, day, first, second)
	rows := func(flows []int) int {
		f, s, _ := archivedRows(t, r, day, flows)
		return f + s
	}
	rounds := 0
	for ; rounds < 40 && rows(firstFlows) > 0; rounds++ {
		var rootThere int
		assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+migrations.FlowsTable(day)+" WHERE flow_id=?", first).Scan(&rootThere))
		assert.Equal(1, rootThere, "the root goes last")
		assert.Equal(27, rows(secondFlows), "the second untouched while the first has rows")
		before := rows(firstFlows)
		r.roundOf(ctx, JobArchiveDeletionSweep)
		assert.True(before-rows(firstFlows) <= 4, "a chunk took %d rows of a batch of 4", before-rows(firstFlows))
		if rounds == 0 {
			var rm metricdata.ResourceMetrics
			assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
			_, ok := gaugeOf(rm, "dwarf_archive_deletions_pending", map[string]string{"shard": "1"})
			assert.False(ok, "no reading before a sweep completes")
		}
	}
	assert.True(rounds >= 3, "deleted over several rounds: %d", rounds)
	f, s, named := archivedRows(t, r, day, firstFlows)
	assert.Zero(f+s+named, "the first tree and its directory rows")
	r.archiveDeletionRounds(ctx)
	assert.Zero(rows(secondFlows), "then the second")
}

// TestDeleteMarked_AFailingLargeTreeDoesNotHoldBackTheRest pins a large marked tree whose chunks keep failing: held,
// counted pending, and passed, so a tree behind it in its day is deleted; and deleted itself once the failure clears.
func TestDeleteMarked_AFailingLargeTreeDoesNotHoldBackTheRest(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.batchRows = 4
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	big := seedWideTree(t, r.db, 2, 6, 3)
	small := seedTrees(t, r.db, 1, callShape)[0]
	bigFlows := archiveTree(t, r, big, today)
	smallFlows := archiveTree(t, r, small, today)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the trees were archived")
	}
	markArchived(t, r, today, big, small)
	fault := diagnostics.Join(diagnostics.FaultReapMidTree, strconv.Itoa(big))
	r.seams.InjectN(fault, 10000)
	for range 3 {
		r.archiveDeletionRounds(ctx)
	}
	f, s, _ := archivedRows(t, r, today, smallFlows)
	assert.Zero(f+s, "the tree behind the failing one is deleted")
	f, s, _ = archivedRows(t, r, today, bigFlows)
	assert.Equal(27, f+s, "the failing tree is whole")
	dm := r.deleteMarked()
	dm.mu.Lock()
	assert.Equal(1, dm.pending, "and pending")
	dm.mu.Unlock()

	r.seams.Withdraw(fault)
	r.archiveDeletionRounds(ctx)
	f, s, _ = archivedRows(t, r, today, bigFlows)
	assert.Zero(f + s)
}

// TestDeleteMarked_TheOldestRetainedDayIsSwept pins the range of days a sweep probes: from the oldest the retention
// keeps, today less the retention, through tomorrow.
func TestDeleteMarked_TheOldestRetainedDayIsSwept(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 2)
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(makeArchiveDay(t, r, today-2)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	root := seedTrees(t, r.db, 1, callShape)[0]
	flows := archiveTree(t, r, root, today-2)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the tree was archived")
	}
	markArchived(t, r, today-2, root)
	r.archiveDeletionRounds(ctx)
	f, _, _ := archivedRows(t, r, today-2, flows)
	assert.Zero(f, "the oldest retained day's marked tree")
}

// TestDeleteMarked_AFailingTreeIsPendingAndTheRestGo pins a marked tree whose deletion keeps failing: passed, counted
// pending once the sweep completes, the trees after it deleted, and deleted itself once the failure clears.
func TestDeleteMarked_AFailingTreeIsPendingAndTheRestGo(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	roots := seedTrees(t, r.db, 3, callShape)
	var flows [][]int
	for _, root := range roots {
		flows = append(flows, archiveTree(t, r, root, today))
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the trees were archived")
	}
	markArchived(t, r, today, roots...)
	fault := diagnostics.Join(diagnostics.FaultArchiveDelete, strconv.Itoa(roots[1]))
	r.seams.InjectN(fault, 10000)
	r.archiveDeletionRounds(ctx)
	for i := range roots {
		f, _, _ := archivedRows(t, r, today, flows[i])
		assert.Equal(i == 1, f > 0, "root %d", i)
	}
	pending := func() (float64, bool) {
		var rm metricdata.ResourceMetrics
		assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
		return gaugeOf(rm, "dwarf_archive_deletions_pending", map[string]string{"shard": "1"})
	}
	n, ok := pending()
	assert.True(ok && n == 1, "pending: %v %v", n, ok)

	r.seams.Withdraw(fault)
	r.archiveDeletionRounds(ctx)
	f, _, _ := archivedRows(t, r, today, flows[1])
	assert.Zero(f)
	n, _ = pending()
	assert.Zero(n)
}

// TestDeleteMarked_FailuresShowBeforeTheSweepCompletes pins that the pending count is published as trees fail, not
// only when the sweep completes: a round gives way after sweepFailureLimit failures with the sweep under way, and the
// gauge reads them already. Each held tree is logged under its own correlation id.
func TestDeleteMarked_FailuresShowBeforeTheSweepCompletes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	roots := seedTrees(t, r.db, sweepFailureLimit+2, callShape)
	for _, root := range roots {
		archiveTree(t, r, root, today)
		fault := diagnostics.Join(diagnostics.FaultArchiveDelete, strconv.Itoa(root))
		r.seams.InjectN(fault, 10000)
		defer r.seams.Withdraw(fault)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the trees were archived")
	}
	markArchived(t, r, today, roots...)
	assert.False(r.roundOf(ctx, JobArchiveDeletionSweep), "a round of failures makes no progress")
	assert.True(r.deleteMarked().w.sweeping, "the sweep is under way")
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	n, ok := gaugeOf(rm, "dwarf_archive_deletions_pending", map[string]string{"shard": "1"})
	assert.True(ok && n == sweepFailureLimit, "pending as they fail: %v %v", n, ok)

	var hour int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT created_hour FROM "+migrations.FlowsTable(today)+" WHERE flow_id=?", roots[0]).Scan(&hour))
	r.logs.mu.Lock()
	ids := slices.Clone(r.logs.correlationIDs)
	r.logs.mu.Unlock()
	assert.Must(assert.True(len(ids) > 0))
	assert.Equal(keys.CorrelationID(1, hour, roots[0]), ids[0], "the held tree's own correlation id")
}

// TestDeleteMarked_AHeldTreeIsPending pins a marked tree another transaction holds: skipped, not waited for, and counted
// pending, on PostgreSQL and MySQL.
func TestDeleteMarked_AHeldTreeIsPending(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	root := seedTrees(t, r.db, 1, callShape)[0]
	flows := archiveTree(t, r, root, today)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the tree was archived")
	}
	markArchived(t, r, today, root)
	release := holdRows(t, r, migrations.FlowsTable(today), root)
	r.archiveDeletionRounds(ctx)
	release()
	f, _, _ := archivedRows(t, r, today, flows)
	assert.Equal(2, f, "held, so left")
	r.deleteMarked().mu.Lock()
	pending := r.deleteMarked().pending
	r.deleteMarked().mu.Unlock()
	assert.Equal(1, pending)
}

// TestDeleteMarked_ADroppedDayIsNoErrorAndAnUnreadableOneLogged pins the days a sweep cannot examine: a day whose
// tables are gone is passed, its trees gone with it, and nothing logged; a day whose probe fails is logged.
func TestDeleteMarked_ADroppedDayIsNoErrorAndAnUnreadableOneLogged(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	assert.Must(assert.NoError(makeArchiveDay(t, r, today-1)))
	assert.Must(assert.NoError(r.archive.Refresh(ctx)))
	roots := seedTrees(t, r.db, 2, callShape)
	archiveTree(t, r, roots[0], today-1)
	flows := archiveTree(t, r, roots[1], today)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the trees were archived")
	}
	markArchived(t, r, today-1, roots[0])
	markArchived(t, r, today, roots[1])
	for _, table := range []string{migrations.FlowsTable(today - 1), migrations.StepsTable(today - 1)} {
		_, err := r.db.ExecContext(ctx, "DROP TABLE "+table) // the archive's record still holds the day
		assert.Must(assert.NoError(err))
	}
	r.archiveDeletionRounds(ctx)
	f, _, _ := archivedRows(t, r, today, flows)
	assert.Zero(f, "the day after the dropped one is swept")
	dm := r.deleteMarked()
	dm.mu.Lock()
	assert.Zero(dm.pending, "a dropped day is no failure")
	dm.mu.Unlock()
	assert.Zero(r.logs.count("Janitor: finding marked archive trees"), "nor logged")

	r.seams.InjectN(diagnostics.FaultJanitorMarkedProbe, 10000)
	defer r.seams.Withdraw(diagnostics.FaultJanitorMarkedProbe)
	r.archiveDeletionRounds(ctx)
	assert.True(r.logs.count("Janitor: finding marked archive trees") > 0, "a day whose probe failed is logged")
}

// TestDeleteMarked_ADayWithoutTheDeletionIndexIsSwept pins the probe's fallback on MySQL and MariaDB, where it names the
// day's deletion index: a day made without the index - dropped here - is read without the hint. Elsewhere the probe
// names no index, and the day is swept the same.
func TestDeleteMarked_ADayWithoutTheDeletionIndexIsSwept(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	root := seedWideTree(t, r.db, 2, 6, 3)
	flows := archiveTree(t, r, root, today)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed while the tree was archived")
	}
	markArchived(t, r, today, root)
	index := "idx_" + migrations.FlowsTable(today) + "_delete_after"
	drop := "DROP INDEX " + index
	if d := r.db.DriverName(); d == "mysql" || d == "mariadb" || d == "mssql" {
		drop += " ON " + migrations.FlowsTable(today)
	}
	_, err = r.db.ExecContext(ctx, drop)
	assert.Must(assert.NoError(err))
	r.archiveDeletionRounds(ctx)
	f, s, named := archivedRows(t, r, today, flows)
	assert.Zero(f+s+named, "the marked tree is deleted")
}
