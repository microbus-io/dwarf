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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// fastRounds runs fast archiving's rounds until one makes no progress, the sweep left out.
func (r *rig) fastRounds(ctx context.Context) {
	for r.roundOf(ctx, JobFastArchiving) {
	}
}

// sweepRounds runs sweep archiving's rounds until one makes no progress, and returns how many made progress.
func (r *rig) sweepRounds(ctx context.Context) (n int) {
	for r.roundOf(ctx, JobSweepArchiving) {
		n++
	}
	return n
}

// failFast arms a fault failing the move or deletion of each of roots, so the fast jobs pass them; the returned func
// withdraws them.
func failFast(r *rig, roots ...int) (withdraw func()) {
	var faults []string
	for _, id := range roots {
		f := diagnostics.Join(diagnostics.FaultReapMidTree, strconv.Itoa(id))
		r.seams.InjectN(f, 10000)
		faults = append(faults, f)
	}
	return func() {
		for _, f := range faults {
			r.seams.Withdraw(f)
		}
	}
}

// archived reports whether root's tree is in today's archive and gone from the live tables.
func archived(t *testing.T, r *rig, today, root int) bool {
	t.Helper()
	live, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	there, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), root)
	return live == 0 && there > 0
}

// TestSweep_TheLastRootFastFailedOnIsTheSweeps pins the boundary: fast archiving's position is the last root it was
// done with, failed or not, and a sweep takes the roots at or behind it - that root included.
func TestSweep_TheLastRootFastFailedOnIsTheSweeps(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 3, callShape)
	ascendingCloses(t, r, roots)
	r.settings.batchRoots = 1
	withdraw := failFast(r, roots[2])
	r.fastRounds(ctx)
	r.fastRounds(ctx)
	assert.Equal(roots[2], r.fast().after.flowID, "fast archiving passed the root it failed on")
	assert.False(archived(t, r, today, roots[2]))
	withdraw()

	assert.Equal(1, r.sweepRounds(ctx))
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	assert.True(archived(t, r, today, roots[2]), "the sweep took the root at its end")
}

// TestSweep_ARootWaitsOutTheWindow pins that a sweep takes a root fast archiving passed only once it is overdue by the
// sweep's window past the archive grace: fast archiving may yet be coming back for anything fresher.
func TestSweep_ARootWaitsOutTheWindow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 2, callShape)
	backdate(t, r, roots[0], DefaultSweepOverdue+time.Minute) // overdue past the window
	backdate(t, r, roots[1], time.Second)                     // due, but within the window
	withdraw := failFast(r, roots...)
	r.fastRounds(ctx)
	withdraw()
	assert.Equal(roots[1], r.fast().after.flowID)

	r.sweepRounds(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	assert.True(archived(t, r, today, roots[0]), "overdue past the window")
	assert.False(archived(t, r, today, roots[1]), "within the window")

	backdate(t, r, roots[1], DefaultSweepOverdue+time.Second)
	r.sweepRounds(ctx)
	assert.True(archived(t, r, today, roots[1]), "taken once overdue past the window")
}

// TestSweep_ALateLargeRootGetsItsAttempt pins that a tree over the staging threshold that closed behind fast
// archiving's position - its close committed late, with an older time - has its attempt started by the sweep, and is
// archived in stages.
func TestSweep_ALateLargeRootGetsItsAttempt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.settings.overdue = 0
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	small := seedTrees(t, r.db, 2, callShape)
	ascendingCloses(t, r, small)
	r.fastRounds(ctx)
	assert.Equal(small[1], r.fast().after.flowID)

	large := seedWideTree(t, r.db, 3, 10, 3) // 11 flows, 33 steps: over the staging threshold of 8 rows
	closeTreesAgo(t, r.db, time.Hour, large) // committed late: behind the position
	attempted := func() bool {
		var n int
		assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND "+
			AttemptedSQL(r.db.DriverName()), large).Scan(&n))
		return n == 1
	}
	r.fastRounds(ctx)
	assert.False(attempted(), "behind fast archiving's position: not fast's")

	r.sweepRounds(ctx)
	assert.True(attempted(), "the sweep started its attempt")
	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	flows, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), large)
	assert.Equal(11, flows, "archived in stages")
}

// TestSweep_CompletesUnderContinuousArrivals pins that a sweep's end is fixed when it begins: roots that close while it
// runs - each passed by fast archiving, whose position moves past them - are behind fast archiving's position but
// ahead of the sweep's end, so a sweep completes however fast they arrive.
func TestSweep_CompletesUnderContinuousArrivals(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 4, callShape)
	ascendingCloses(t, r, roots)
	withdraw := failFast(r, roots...)
	r.fastRounds(ctx)
	withdraw()
	assert.Equal(roots[3], r.fast().after.flowID)

	var arrivals []int
	rounds := 0
	for ; rounds < 10; rounds++ {
		if !r.roundOf(ctx, JobSweepArchiving) {
			break
		}
		arrival := seedTrees(t, r.db, 1, callShape)[0] // due at once, as the sweep runs
		withdrawArrival := failFast(r, arrival)
		r.fastRounds(ctx) // fails it and moves its position past it
		withdrawArrival()
		assert.Equal(arrival, r.fast().after.flowID)
		arrivals = append(arrivals, arrival)
	}
	assert.True(rounds < 10, "the sweep completed")
	assert.False(r.sweep().w.sweeping, "the sweep completed")
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	for _, id := range roots {
		assert.True(archived(t, r, today, id), "root %d swept", id)
	}
	for _, id := range arrivals {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "an arrival is the next sweep's: root %d", id)
	}
}

// TestSweep_APoisonTreeDoesNotLoseItsBatchMates pins that a sweep takes a failed batch again a root at a time: its
// healthy roots move, the one that fails alone is held and passed, and once repaired the next sweep moves it.
func TestSweep_APoisonTreeDoesNotLoseItsBatchMates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.batchRoots = 4
	r.settings.overdue = 0
	roots := seedTrees(t, r.db, 9, callShape)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	_, err = r.db.ExecContext(ctx, "INSERT INTO "+migrations.FlowsTable(today)+" ("+archivedFlowColumns+") SELECT "+
		archivedFlowColumns+" FROM dwarf_flows WHERE flow_id=?", roots[1])
	assert.NoError(err)
	for range 3 {
		r.pass(ctx)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	for i, id := range roots {
		flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		if i == 1 {
			assert.Equal(2, flows)
			assert.True(steps > 0)
			continue
		}
		assert.Zero(flows+steps, "healthy tree %d leaves the live tables", i)
		there, _ := treeCount(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), id)
		assert.Equal(2, there, "healthy tree %d is archived", i)
	}
	assert.True(r.logs.count("Janitor: a sweep could not remove a tree") > 0, "the held root is logged")

	_, err = r.db.ExecContext(ctx, "DELETE FROM "+migrations.FlowsTable(today)+" WHERE flow_id=?", roots[1])
	assert.NoError(err)
	r.pass(ctx)
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", roots[1])
	assert.Zero(flows)
}

// TestSweep_PassesFailedTrees pins that a root whose move or sizing fails does not trap its batch-mates or the roots
// after it, and is taken once the fault clears.
func TestSweep_PassesFailedTrees(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{diagnostics.FaultReapMidTree, diagnostics.FaultReapSizeErr} {
		t.Run(fault, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 30)
			r.settings.batchRoots = 4
			r.settings.overdue = 0
			roots := seedTrees(t, r.db, 7, callShape)
			targeted := diagnostics.Join(fault, strconv.Itoa(roots[2]))
			r.seams.InjectN(targeted, 10000)
			for range 3 {
				r.pass(ctx)
			}
			for i, id := range roots {
				flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
				if i == 2 {
					assert.Equal(2, flows)
					assert.True(steps > 0)
				} else {
					assert.Zero(flows+steps, "root %d", i)
				}
			}
			r.seams.Withdraw(targeted)
			r.pass(ctx)
			flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", roots[2])
			assert.Zero(flows + steps)
		})
	}
}

// TestSweep_ABatchThatMeetsContentionIsSwept pins that lock contention past Transact's retries is an archive failure
// like any other: the trees stay whole, and a sweep archives them once contention clears.
func TestSweep_ABatchThatMeetsContentionIsSwept(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	roots := seedTrees(t, r.db, 3, callShape)
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	r.seams.InjectN(diagnostics.FaultReapContention, 1000)
	r.pass(ctx)
	r.seams.Withdraw(diagnostics.FaultReapContention)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d stays live", id)
	}
	assert.Equal(roots[2], r.fast().after.flowID, "fast archiving passed them")

	r.pass(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	for _, id := range roots {
		assert.True(archived(t, r, today, id), "tree %d swept", id)
	}
}

// TestSweep_AtRetentionZeroItDeletes pins that at a retention of 0 a sweep deletes what fast archiving left, as fast
// archiving does: a tree a batch holds deleted, one too large for a batch - its close committed late, behind fast
// archiving's position - stamped for deletion in chunks.
func TestSweep_AtRetentionZeroItDeletes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	r.settings.overdue = 0
	r.settings.batchRows = 4
	small := seedTrees(t, r.db, 2, callShape) // 4 rows each
	big := seedWideTree(t, r.db, 1, 3, 1)     // 4 flows, 4 steps
	ascendingCloses(t, r, small)
	closeTreesAgo(t, r.db, -time.Hour, big) // closed in the future: not due when fast archiving passes it
	withdraw := failFast(r, small...)
	for range 2 {
		r.fastRounds(ctx) // a batch a tree, each failed and passed: a round without progress
	}
	withdraw()
	assert.Equal(small[1], r.fast().after.flowID)
	closeTreesAgo(t, r.db, time.Hour, big) // due, behind the position
	r.fastRounds(ctx)
	var stamp int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", big).Scan(&stamp))
	assert.Zero(stamp, "behind fast archiving's position: not fast's")

	r.sweepRounds(ctx)
	for _, id := range small {
		flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows+steps, "deleted: root %d", id)
	}
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", big).Scan(&stamp))
	assert.Equal(1, stamp, "stamped for deletion in chunks")
}

// TestSweep_FollowsFastArchivingsResidueClass pins that a sweep takes the residue class of the boundary it began at,
// and starts over when fast archiving's changes: the sweep under way is dropped, and the next one sweeps the new class.
func TestSweep_FollowsFastArchivingsResidueClass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	r.settings.batchRoots = 1
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 4, soloShape) // consecutive ids, closed now: no class's roots are stolen
	var even, odd []int
	for _, id := range roots {
		if id%2 == 0 {
			even = append(even, id)
		} else {
			odd = append(odd, id)
		}
	}
	withdrawOdd := failFast(r, odd...)
	withdrawEven := failFast(r, even...)
	r.setResidency(2, 0, true)
	r.fastRounds(ctx)
	r.fastRounds(ctx)
	withdrawEven()
	assert.True(r.roundOf(ctx, JobSweepArchiving), "the sweep in class 0 takes its first root")
	assert.Equal(0, r.sweep().end.ordinal)

	r.setResidency(2, 1, true)
	assert.True(r.roundOf(ctx, JobSweepArchiving), "before fast archiving's next round, the sweep goes on in class 0")
	assert.Equal(0, r.sweep().end.ordinal)
	r.fastRounds(ctx)
	r.fastRounds(ctx)
	withdrawOdd()
	r.sweepRounds(ctx)
	assert.Equal(1, r.sweep().end.ordinal, "the sweep started over in fast archiving's new class")
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	for _, id := range even {
		assert.True(archived(t, r, today, id), "swept in class 0: root %d", id)
	}
	for _, id := range odd {
		assert.True(archived(t, r, today, id), "swept in class 1: root %d", id)
	}
}

// TestSweep_HeldRootsAndTheFailureEpisode pins the sweep's gauges: a root that fails alone is counted held - the
// larger of the current sweep's count and the last complete one's - the oldest due root at the sweep's front is
// reported aging, and the job's failure episode begins at the failure and ends at a sweep that completes failing
// nothing. The series carry the job's own labels.
func TestSweep_HeldRootsAndTheFailureEpisode(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	r.settings.overdue = 0
	roots := seedTrees(t, r.db, 3, callShape)
	ascendingCloses(t, r, roots)
	targeted := diagnostics.Join(diagnostics.FaultReapMidTree, strconv.Itoa(roots[1]))
	r.seams.InjectN(targeted, 10000)
	r.pass(ctx)

	assert.Equal(1, r.sweep().w.held(), "the root that failed alone is held")
	assert.True(r.sweep().failingFor(time.Now()) > 0, "the sweep is failing")
	var rm metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &rm))
	held, ok := gaugeOf(rm, "dwarf_archive_held_roots", map[string]string{"shard": "1", "kind": "closed"})
	assert.True(ok && held == 1, "held roots by kind: %v %v", held, ok)
	failing, ok := gaugeOf(rm, "dwarf_archive_failing_seconds", map[string]string{"shard": "1", "job": JobSweepArchiving})
	assert.True(ok && failing > 0, "the job's own failing series: %v %v", failing, ok)
	assert.Equal(1, r.logs.count("Janitor: sweeping closed trees failed"), "only the episode's first failure is logged")

	r.seams.Withdraw(targeted)
	r.pass(ctx)
	assert.Zero(r.sweep().w.held(), "a clean sweep holds nothing")
	assert.Zero(r.sweep().failingFor(time.Now()), "a sweep that failed nothing ends the episode")
}

// TestSweep_PassingAStuckAttemptIsNoProgress pins that a root the sweep only passes - one with an archive attempt whose
// copy keeps failing - is no progress: every sweep passes it again, so counting it would keep the job from ever idling,
// and a drain from ever returning.
func TestSweep_PassingAStuckAttemptIsNoProgress(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := stageRig(t)
	r.settings.overdue = 0
	root := seedWideTree(t, r.db, 3, 10, 3)
	closeTreesAgo(t, r.db, time.Hour, root)
	r.startAttempts(ctx)
	fault := diagnostics.Join(diagnostics.FaultReapStageChunk, strconv.Itoa(root))
	r.seams.InjectN(fault, 10000)
	defer r.seams.Withdraw(fault)

	drained := make(chan struct{})
	go func() {
		defer close(drained)
		r.pass(ctx)
	}()
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		t.Fatal("the drain did not return: passing the stuck attempt counted as progress")
	}
	assert.Zero(r.sweepRounds(ctx), "a sweep makes no progress on the root")
	assert.Zero(r.sweep().w.held(), "a root with an attempt is not the sweep's to hold")
}

// TestSweep_NothingMovesWhileTomorrowIsNotWhole pins the day's gate on the sweep: with tomorrow's tables missing, the
// roots fast archiving left stay live, and the sweep is failing.
func TestSweep_NothingMovesWhileTomorrowIsNotWhole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, r.db, 2, callShape)
	ascendingCloses(t, r, roots)
	withdraw := failFast(r, roots...)
	r.fastRounds(ctx)
	withdraw()
	for _, table := range []string{migrations.FlowsTable(today + 1), migrations.StepsTable(today + 1)} {
		_, err = r.db.ExecContext(ctx, "DROP TABLE "+table)
		assert.NoError(err)
	}
	assert.NoError(r.archive.Refresh(ctx))

	r.sweepRounds(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(2, flows, "tree %d stays live", id)
	}
	assert.True(r.sweep().failingFor(time.Now().Add(time.Second)) > 0, "an archive failure")
}

// TestSweep_StealsAnotherClasssOverdueRoot pins the steal: a sweep takes another class's root once it is overdue by
// the steal grace past the sweep's window - a slow peer's root, which fast archiving passed - and leaves a fresher one.
func TestSweep_StealsAnotherClasssOverdueRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	today, err := r.archive.Today(ctx)
	assert.NoError(err)
	r.setResidency(2, 0, true)
	roots := seedTrees(t, r.db, 4, soloShape) // consecutive ids, closed now
	var odd []int
	for _, id := range roots {
		if id%2 == 1 {
			odd = append(odd, id)
		}
	}
	stale, fresh := odd[0], odd[1]
	backdate(t, r, stale, 10*time.Minute) // overdue past the steal grace
	withdraw := failFast(r, stale)
	r.fastRounds(ctx)
	r.fastRounds(ctx)
	withdraw()
	assert.False(archived(t, r, today, stale), "fast archiving stole it and failed on it")

	r.sweepRounds(ctx)
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
	assert.True(archived(t, r, today, stale), "the sweep stole it")
	assert.False(archived(t, r, today, fresh), "the other class's, not overdue")
}

// TestSweep_HeldRootsAreVisibleBeforeTheSweepCompletes pins that the held count is published as each root is held, not
// at the sweep's end: a round gives way after sweepFailureLimit failures with the sweep under way, and its count reads
// already.
func TestSweep_HeldRootsAreVisibleBeforeTheSweepCompletes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	roots := seedTrees(t, r.db, sweepFailureLimit+1, callShape)
	ascendingCloses(t, r, roots)
	r.seams.InjectN(diagnostics.FaultReapArchiveErr, 10000)
	defer r.seams.Withdraw(diagnostics.FaultReapArchiveErr)
	r.fastRounds(ctx)
	assert.False(r.roundOf(ctx, JobSweepArchiving), "a round of failures makes no progress")
	assert.Equal(sweepFailureLimit, r.sweep().w.held(), "held as they fail")
	assert.True(r.sweep().w.sweeping, "the sweep is under way")

	r.roundOf(ctx, JobSweepArchiving)
	assert.Equal(sweepFailureLimit+1, r.sweep().w.held(), "the last complete sweep's count")
	assert.False(r.sweep().w.sweeping, "complete")
	assert.Equal(heldLogLimit, r.logs.count("Janitor: a sweep could not remove a tree"), "the first of a sweep logged")
}

// gaugeOf returns the value of the gauge named name at the point carrying attrs, and whether there is one.
func gaugeOf(rm metricdata.ResourceMetrics, name string, attrs map[string]string) (float64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			switch g := m.Data.(type) {
			case metricdata.Gauge[int64]:
				for _, dp := range g.DataPoints {
					if hasAttrs(dp.Attributes.ToSlice(), attrs) {
						return float64(dp.Value), true
					}
				}
			case metricdata.Gauge[float64]:
				for _, dp := range g.DataPoints {
					if hasAttrs(dp.Attributes.ToSlice(), attrs) {
						return dp.Value, true
					}
				}
			}
		}
	}
	return 0, false
}

// hasAttrs reports whether kvs carry every one of attrs.
func hasAttrs(kvs []attribute.KeyValue, attrs map[string]string) bool {
	found := 0
	for _, kv := range kvs {
		if v, ok := attrs[string(kv.Key)]; ok && kv.Value.AsString() == v {
			found++
		}
	}
	return found == len(attrs)
}
