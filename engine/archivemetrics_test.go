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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/internal/reaper"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// meteredArchiveEngine is archiveEngine with a manual metric reader.
func meteredArchiveEngine(t *testing.T, mux *hosts.Mux, retention int) (*Engine, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(mux)
	e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	if err := e.SetRetention(retention); err != nil {
		t.Fatal(err)
	}
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	enginetest.AwaitVisitCount(t, e.diags.Seams(), 1, 10*time.Second, diagnostics.CheckpointArchivePassDone)
	return e, reader
}

// collect reads the engine's metrics now.
func collect(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	return rm
}

// sumAt returns the total of an int64 counter's points carrying every key=value in attrs.
func sumAt(rm metricdata.ResourceMetrics, name string, attrs map[string]string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if m.Name != name || !ok {
				continue
			}
			for _, dp := range sum.DataPoints {
				match := true
				for k, want := range attrs {
					if v, ok := dp.Attributes.Value(attribute.Key(k)); !ok || v.AsString() != want {
						match = false
					}
				}
				if match {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// backdate moves a root's updated_at ago into the past.
func backdate(t *testing.T, e *Engine, rootID int, ago time.Duration) {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		-ago.Milliseconds(), rootID)
	if err != nil {
		t.Fatal(err)
	}
}

// TestArchiveMetrics_TheReaperCountsWhatItMoves pins the archiving instruments: the flows archived, counted per flow
// of each tree; the lag, read at fast archiving's last scan, 0 once none is due; the reaper pass's time; and, while
// archiving fails, the failing time without premature deletion. The lag's value past the grace is pinned by
// internal/janitor's TestFast_TheLagIsTheOldestDueRootPastTheGrace.
func TestArchiveMetrics_TheReaperCountsWhatItMoves(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, treeMux(), 30)
	shard1 := map[string]string{"shard": "1"}

	roots := closedRoots(t, e, 2)
	e.archiveGrace = 30 * time.Second
	backdate(t, e, roots[0], 150*time.Second)
	backdate(t, e, roots[1], 90*time.Second)
	e.reapBatch = 1
	e.reapDueFlows(ctx)
	e.archiveGrace = 0
	rm := collect(t, reader)
	assert.Equal(int64(4), sumAt(rm, "dwarf_flows_archived", shard1), "two trees of two flows")
	lag, ok := gaugeAt(rm, "dwarf_archive_lag_seconds", shard1)
	assert.True(ok)
	assert.Zero(lag, "the last scan found nothing due")
	passes, _ := histCount(rm, "dwarf_reap_pass_seconds", shard1)
	assert.Equal(uint64(1), passes)
	failing, ok := gaugeAt(rm, "dwarf_archive_failing_seconds", shard1)
	assert.True(ok)
	assert.Zero(failing)

	e.reapDueFlows(ctx)
	rm = collect(t, reader)
	lag, _ = gaugeAt(rm, "dwarf_archive_lag_seconds", shard1)
	assert.Zero(lag, "nothing due")

	// Archiving fails across passes: the tree stays live and no deletion is counted. Fast archiving passes it after its
	// first failure, so the second pass's fast round finds nothing - which ends nothing: the sweep fails on it too.
	closedRoots(t, e, 1)
	e.diags.Seams().InjectN(diagnostics.FaultReapArchiveErr, 1000)
	defer e.diags.Seams().Withdraw(diagnostics.FaultReapArchiveErr)
	e.reapDueFlows(ctx)
	time.Sleep(20 * time.Millisecond)
	e.reapDueFlows(ctx)
	rm = collect(t, reader)
	assert.Zero(sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonArchiveFailed}))
	assert.Equal(int64(4), sumAt(rm, "dwarf_flows_archived", shard1), "nothing more archived")
	failing, _ = gaugeAt(rm, "dwarf_archive_failing_seconds", shard1)
	assert.True(failing >= 0.02, "failing since the first pass, got %v", failing)
}

// TestArchiveMetrics_DeletionsAreCountedByReason pins dwarf_flows_reaped's other two reasons - a tree scheduled for
// deletion, and any closed tree at a retention of 0 - and dwarf_flows_removed for a Delete of a live root.
func TestArchiveMetrics_DeletionsAreCountedByReason(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, treeMux(), 0)

	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	assert.NoError(e.Delete(ctx, fk))
	closedRoots(t, e, 1)
	e.reapDueFlows(ctx)

	rm := collect(t, reader)
	assert.Equal(int64(1), sumAt(rm, "dwarf_flows_removed", map[string]string{"shard": "1", "side": removedFromLive, "op": "delete"}))
	assert.Equal(int64(2), sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonDeleted}))
	assert.Equal(int64(2), sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonNoRetention}))
	assert.Zero(sumAt(rm, "dwarf_flows_archived", nil))
}

// TestArchiveMetrics_RemovalsAreCountedBySide pins dwarf_flows_removed for what Delete and Purge take from the
// archive, deleted at once; Purge leaves a flow not archived yet, so nothing is counted on the live side for it.
func TestArchiveMetrics_RemovalsAreCountedBySide(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, treeMux(), 30)

	deleted := closedKeys(t, e, 1)
	purged := closedKeys(t, e, 1)
	e.reapDueFlows(ctx)
	assert.NoError(e.Delete(ctx, deleted[0]))
	live := closedKeys(t, e, 1) // left in the live tables: the reaper does not pass again
	removed, _, err := e.Purge(ctx, workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: time.Now().Add(time.Hour), Limit: 10})
	assert.NoError(err)
	assert.Equal(1, removed, "the archived root %s, not the live one %s", purged[0], live[0])

	rm := collect(t, reader)
	at := func(side, op string) int64 {
		return sumAt(rm, "dwarf_flows_removed", map[string]string{"shard": "1", "side": side, "op": op})
	}
	assert.Equal(int64(1), at(removedFromArchive, "delete"))
	assert.Equal(int64(1), at(removedFromArchive, "purge"))
	assert.Zero(at(removedFromLive, "purge"))
	assert.Zero(at(removedFromLive, "delete"))
}

// closedKeys runs n trees to completion and returns their root keys.
func closedKeys(t *testing.T, e *Engine, n int) []string {
	t.Helper()
	var fks []string
	for range n {
		fk, err := e.Create(t.Context(), "arch/parent", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		fks = append(fks, fk)
	}
	return fks
}

// TestArchiveMetrics_TheDays pins the day readings: the days with tables and the oldest one's age, from the archive's
// record, a drop counted by retention and by PurgeBefore, and a maintenance pass that fails counted as one.
func TestArchiveMetrics_TheDays(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, hosts.NewMux(), 1)
	set := e.archives.Load()
	a := set.byShard[1]
	today, err := a.Today(ctx)
	assert.NoError(err)
	shard1 := map[string]string{"shard": "1"}

	assert.NoError(makeArchiveDay(t, e, today-5))
	assert.NoError(makeArchiveDay(t, e, today-1))
	assert.NoError(a.Refresh(ctx))
	rm := collect(t, reader)
	days, ok := gaugeAt(rm, "dwarf_archive_days", shard1)
	assert.True(ok)
	assert.Equal(float64(5), days, "today-5, yesterday, today and the two days ahead")
	oldest, ok := gaugeAt(rm, "dwarf_archive_oldest_day_age_seconds", shard1)
	assert.True(ok)
	began := time.Since(a.DayEnds(today - 6)).Seconds()
	assert.True(oldest > 0 && oldest <= began && oldest > began-60, "day %d began %vs ago, got %v", today-5, began, oldest)

	e.maintainArchives(ctx, set)
	assert.NoError(e.PurgeBefore(ctx, a.DayEnds(today-1)))
	if now, _ := a.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	rm = collect(t, reader)
	assert.Equal(int64(2), sumAt(rm, "dwarf_archive_days_dropped", shard1), "one by retention, one by PurgeBefore")
	days, _ = gaugeAt(rm, "dwarf_archive_days", shard1)
	assert.Equal(float64(3), days)
	assert.Zero(sumAt(rm, "dwarf_archive_maintain_failed", shard1))

	e.diags.Seams().InjectN(diagnostics.FaultArchiveMaintainErr, 1)
	e.maintainArchives(ctx, set)
	rm = collect(t, reader)
	assert.Equal(int64(1), sumAt(rm, "dwarf_archive_maintain_failed", shard1))

	// A pass cut short by Shutdown is not a failure.
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	e.maintainArchives(cancelled, set)
	rm = collect(t, reader)
	assert.Equal(int64(1), sumAt(rm, "dwarf_archive_maintain_failed", shard1))
}

// TestArchiveMetrics_AReplicaThatStopsReapingForgets pins that a replica forgets a shard's reaper readings once it
// stops dispatching there: when it dispatches again, it reports no old failure or lag, and its next failure starts a
// fresh failure interval.
func TestArchiveMetrics_AReplicaThatStopsReapingForgets(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, treeMux(), 30)
	shard1 := map[string]string{"shard": "1"}
	closedRoots(t, e, 1)
	e.diags.Seams().InjectN(diagnostics.FaultReapArchiveErr, 1000)
	defer e.diags.Seams().Withdraw(diagnostics.FaultReapArchiveErr)
	e.reapDueFlows(ctx)
	time.Sleep(20 * time.Millisecond)
	rm := collect(t, reader)
	failing, _ := gaugeAt(rm, "dwarf_archive_failing_seconds", shard1)
	assert.True(failing > 0, "failing, got %v", failing)

	setRole(e, 1, false)
	e.reapDueFlows(ctx)
	setRole(e, 1, true)
	rm = collect(t, reader)
	failing, _ = gaugeAt(rm, "dwarf_archive_failing_seconds", shard1)
	assert.Zero(failing)
	_, ok := gaugeAt(rm, "dwarf_archive_lag_seconds", shard1)
	assert.False(ok, "no reading until a pass scans again")

	// Returning to the shard does not authorize deletion.
	e.reapDueFlows(ctx)
	rm = collect(t, reader)
	assert.Zero(sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonArchiveFailed}))
}

// An unavailable archive holds records without incrementing either deletion counter.
func TestArchiveMetrics_AnUnreadableDayIsAnArchiveFailure(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, treeMux(), 30)
	closedRoots(t, e, 1)
	a := e.archives.Load().byShard[1]
	today, err := a.Today(ctx)
	assert.NoError(err)
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	for _, table := range []string{migrations.FlowsTable(today + 1), migrations.StepsTable(today + 1)} {
		_, err = db.ExecContext(ctx, "DROP TABLE "+table)
		assert.NoError(err)
	}
	assert.NoError(a.Refresh(ctx))

	e.reapDueFlows(ctx) // the first failure waits
	e.reapDueFlows(ctx)
	if now, _ := a.Today(ctx); now != today {
		t.Skip("midnight passed during the passes")
	}
	rm := collect(t, reader)
	assert.Zero(sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonArchiveFailed}))
	assert.Zero(sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonNoRetention}))
}

func TestArchiveMetrics_HeldWorkIsVisibleBeforeASweepCompletes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e, reader := meteredArchiveEngine(t, treeMux(), 30)
	roots := closedRoots(t, e, 3)
	for _, id := range roots {
		backdate(t, e, id, 2*time.Minute)
	}
	e.reapSlice = 0
	e.reapBatch = 1
	e.diags.Seams().InjectN(diagnostics.FaultReapArchiveErr, 10000)
	e.reapDueFlows(t.Context())
	rm := collect(t, reader)
	attrs := map[string]string{"shard": "1", "kind": "closed"}
	n, ok := gaugeAt(rm, "dwarf_archive_held_roots", attrs)
	assert.True(ok)
	assert.Equal(float64(1), n)
	age, ok := gaugeAt(rm, "dwarf_archive_oldest_held_seconds", attrs)
	assert.True(ok)
	assert.True(age >= 119)
	assert.True(e.reaper.Load().Observe(1, time.Now()).Kinds[0].Sweep > 0, "sweep is incomplete")
	assert.True(sumAt(rm, "dwarf_archive_move_failures", map[string]string{"shard": "1"}) >= 2)
	assert.Zero(sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonArchiveFailed}))
	e.diags.Seams().Withdraw(diagnostics.FaultReapArchiveErr)
	for range 4 {
		e.reapDueFlows(t.Context())
	}
	rm = collect(t, reader)
	n, _ = gaugeAt(rm, "dwarf_archive_held_roots", attrs)
	assert.Zero(n)
	e.forgetReaping(1)
	assert.Equal(reaper.KindObservation{Kind: reaper.KindClosed}, e.reaper.Load().Observe(1, time.Now()).Kinds[0])
}

func TestArchiveMetrics_UnarchivedRetentionDeletionCountsOnlyOnce(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e, reader := meteredArchiveEngine(t, treeMux(), 1)
	root := closedRoots(t, e, 1)[0]
	backdate(t, e, root, 3*24*time.Hour)
	e.diags.Seams().InjectN(diagnostics.FaultReapArchiveErr, 10000)
	e.reapDueFlows(t.Context())
	e.reapDueFlows(t.Context())
	rm := collect(t, reader)
	assert.Equal(int64(2), sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonArchiveFailed}))
	assert.Zero(sumAt(rm, "dwarf_flows_reaped", map[string]string{"shard": "1", "reason": reaper.ReasonDeleted}))
	assert.Zero(sumAt(rm, "dwarf_flows_archived", map[string]string{"shard": "1"}))
}

// TestArchiveMetrics_ATransientFailureEndsAtACleanSweep pins that one failure on a shard with nothing left behind it -
// a failed scan - ends at the next sweep of the closed roots that fails nothing, rather than reading as failing for as
// long as nothing closes.
func TestArchiveMetrics_ATransientFailureEndsAtACleanSweep(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, treeMux(), 30)
	shard1 := map[string]string{"shard": "1"}
	e.diags.Seams().InjectN(diagnostics.FaultReapSelectErr, 1) // the janitor's scan, which runs first
	e.drainJanitors(ctx, e.archiveGrace)
	time.Sleep(20 * time.Millisecond)
	failing, _ := gaugeAt(collect(t, reader), "dwarf_archive_failing_seconds", shard1)
	assert.True(failing > 0, "the failed scan began an episode")
	e.reapDueFlows(ctx) // the janitor's round finds nothing; the reaper's sweep finds nothing, cleanly
	failing, _ = gaugeAt(collect(t, reader), "dwarf_archive_failing_seconds", shard1)
	assert.Zero(failing, "the clean sweep ended it")
}

// TestArchiveMetrics_ASweepWhileTomorrowIsMissingEndsNoFailure pins that a sweep finding nothing due while the day is
// closed to archiving - tomorrow's tables missing - is no recovery: the failure the day causes stays reported, rather
// than being ended by every pass and begun again by the next round.
func TestArchiveMetrics_ASweepWhileTomorrowIsMissingEndsNoFailure(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, treeMux(), 30)
	shard1 := map[string]string{"shard": "1"}
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	a := e.archives.Load().byShard[1]
	today, err := a.Today(ctx)
	assert.Must(assert.NoError(err))
	for _, table := range []string{migrations.FlowsTable(today + 1), migrations.StepsTable(today + 1)} {
		_, err = db.ExecContext(ctx, "DROP TABLE "+table)
		assert.NoError(err)
	}
	assert.NoError(a.Refresh(ctx))
	e.reapDueFlows(ctx)
	time.Sleep(20 * time.Millisecond)
	e.reapDueFlows(ctx)
	if now, _ := a.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	failing, _ := gaugeAt(collect(t, reader), "dwarf_archive_failing_seconds", shard1)
	assert.True(failing >= 0.02, "failing since the first pass, got %v", failing)
}
