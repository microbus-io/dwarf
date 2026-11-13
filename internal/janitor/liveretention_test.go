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
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// closeAt moves the close of the tree rooted at root to at.
func closeAt(t *testing.T, r *rig, root int, at time.Time) {
	t.Helper()
	ts, arg := database.SQLTime(r.db.DriverName(), at)
	if _, err := r.db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at="+ts+" WHERE root_flow_id=?", arg, root); err != nil {
		t.Fatal(err)
	}
}

// retentionRounds runs live retention's rounds until one makes no progress.
func (r *rig) retentionRounds(ctx context.Context) {
	r.liveRetention()
	for r.roundOf(ctx, JobLiveRetention) {
	}
}

// stampsDue backdates the stamps of roots a second, as stampTree does: a stamp made in the millisecond a test then
// looks for it is not due yet on the database clock.
func stampsDue(t *testing.T, r *rig, roots ...int) {
	t.Helper()
	for _, root := range roots {
		if _, err := r.db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=? AND delete_after_ms>0", root); err != nil {
			t.Fatal(err)
		}
	}
}

// reapedFor returns dwarf_flows_reaped's total under reason.
func reapedFor(rm metricdata.ResourceMetrics, reason string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == "dwarf_flows_reaped" {
				for _, dp := range sum.DataPoints {
					if v, ok := dp.Attributes.Value(attribute.Key("reason")); ok && v.AsString() == reason {
						total += dp.Value
					}
				}
			}
		}
	}
	return total
}

// TestLiveRetention_TheCutoffAndAHugeRetention pins the cutoff at a day's end: a root closed in the last millisecond of
// the day a retention of 1 expires is stamped, one closed at its end is not; and a retention too large for any day to
// be that old stamps nothing, without overflowing.
func TestLiveRetention_TheCutoffAndAHugeRetention(t *testing.T) {
	t.Parallel()
	for _, retention := range []int{1, math.MaxInt} {
		t.Run(strconv.Itoa(retention), func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, retention)
			roots := seedTrees(t, r.db, 2, callShape)
			today, err := r.archive.Today(ctx)
			assert.Must(assert.NoError(err))
			edge := r.archive.DayEnds(today - 2)
			for i, id := range roots {
				closeAt(t, r, id, edge.Add(time.Duration(i-1)*time.Millisecond))
			}
			r.retentionRounds(ctx)
			for i, id := range roots {
				assert.Equal(retention == 1 && i == 0, stampOf(t, r, id) == 1, "root %d", i)
			}
		})
	}
}

// TestLiveRetention_CountsFromTheCloseAndTheGrace pins the retention cutoff's grace term and its recheck under the
// root lock: an unarchived root expires with the day it would have been filed in, its close plus the archive grace, so
// a root whose grace runs past the edge of the expiring day is kept; and the lock refuses a root that is not past the
// cutoff whatever a scan saw.
func TestLiveRetention_CountsFromTheCloseAndTheGrace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 1)
	roots := seedTrees(t, r.db, 2, callShape)
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	grace := r.archive.DayLength() / 4
	edge := r.archive.DayEnds(today - 2)              // the last day a retention of 1 drops
	closeAt(t, r, roots[0], edge.Add(-grace-grace/2)) // filed within the expiring day
	closeAt(t, r, roots[1], edge.Add(-grace/2))       // closed within it, filed in the next
	r.settings.grace = grace
	r.retentionRounds(ctx)
	assert.Equal(1, stampOf(t, r, roots[0]), "past the retention counted from its filing day")
	assert.Zero(stampOf(t, r, roots[1]), "its grace runs past the edge: not yet")

	due, dueArgs := r.liveRetention().dueSQL(r.db.DriverName(), edge.Add(-grace))
	stamped, _, err := stampPastRetention(ctx, r.db, r.seams, []int{roots[1]}, due, dueArgs)
	assert.NoError(err)
	assert.Zero(stamped, "the lock rechecks the cutoff")
	assert.Zero(stampOf(t, r, roots[1]))
}

// TestLiveRetention_StampsAndTheDeletionJobsDelete pins the job's part: it stamps every tree past its retention, large or
// small, counting its flows under dwarf_flows_reaped{reason="archive_failed"} as it does - the records given up - and
// deletes nothing; the deletion of stamped trees deletes them, the large one in chunks, counting them again under
// deleted. At a retention of 0 it does nothing: closed trees are deleted as they come due, not retained.
func TestLiveRetention_StampsAndTheDeletionJobsDelete(t *testing.T) {
	t.Parallel()
	for _, retention := range []int{1, 0} {
		t.Run(strconv.Itoa(retention), func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, retention)
			reader := sdkmetric.NewManualReader()
			r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
			r.settings.overdue = 0
			r.settings.batchRows = 4
			small := seedTrees(t, r.db, 2, callShape) // two flows each
			large := seedWideTree(t, r.db, 1, 3, 1)   // four flows, over a batch
			trees := append(small, large)
			for _, id := range trees {
				closeTreesAgo(t, r.db, 4*24*time.Hour, id)
			}
			collect := func() metricdata.ResourceMetrics {
				var rm metricdata.ResourceMetrics
				assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
				return rm
			}
			r.retentionRounds(ctx)
			if retention == 0 {
				assert.Zero(counterSum(collect(), "dwarf_flows_reaped"), "nothing retained, nothing given up")
				for _, id := range trees {
					assert.Zero(stampOf(t, r, id), "root %d", id)
				}
				return
			}
			for _, id := range trees {
				f, _, _ := rowsOf(t, r, id)
				assert.True(f > 0, "root %d not deleted by live retention", id)
				assert.Equal(1, stampOf(t, r, id), "but stamped, due at once")
			}
			assert.Equal(int64(8), reapedFor(collect(), ReasonArchiveFailed), "every flow given up, counted at the stamp")

			stampsDue(t, r, trees...)
			r.pass(ctx)
			for _, id := range trees {
				f, s, _ := rowsOf(t, r, id)
				assert.Zero(f+s, "root %d deleted by the deletion jobs", id)
			}
			rm := collect()
			assert.Equal(int64(8), reapedFor(rm, ReasonArchiveFailed), "counted once")
			assert.Equal(int64(8), reapedFor(rm, ReasonDeleted), "and counted again as they are deleted")
		})
	}
}

// TestLiveRetention_PassesFailedRoots pins that a root past its retention whose stamp fails does not trap its
// batch-mates or the roots after it, is held, and is stamped once the fault clears.
func TestLiveRetention_PassesFailedRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 1)
	r.settings.batchRoots = 4
	roots := seedTrees(t, r.db, 7, callShape)
	for i, id := range roots {
		closeTreesAgo(t, r.db, 4*24*time.Hour-time.Duration(i)*time.Minute, id)
	}
	targeted := diagnostics.Join(diagnostics.FaultJanitorRetentionStamp, strconv.Itoa(roots[2]))
	r.seams.InjectN(targeted, 10000)
	for range 3 {
		r.retentionRounds(ctx)
	}
	for i, id := range roots {
		assert.Equal(i != 2, stampOf(t, r, id) == 1, "root %d", i)
	}
	assert.True(r.liveRetention().w.reading(time.Now()).held > 0, "held")
	r.seams.Withdraw(targeted)
	r.retentionRounds(ctx)
	assert.Equal(1, stampOf(t, r, roots[2]))
}

// TestLiveRetention_LeavesTreesWithinTheirRetention pins that a closed tree archiving has not moved is not touched
// before its retention: an archive failure never becomes permission to delete sooner.
func TestLiveRetention_LeavesTreesWithinTheirRetention(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedTrees(t, r.db, 1, callShape)[0]
	closeTreesAgo(t, r.db, 3*24*time.Hour, root)
	r.retentionRounds(ctx)
	f, _, _ := rowsOf(t, r, root)
	assert.Equal(2, f, "three days old, kept thirty")
	assert.Zero(stampOf(t, r, root), "and not stamped")
}

// TestLiveRetention_TheStampClearsTheAttempt pins that a tree past its retention whose staged copy never published is
// stamped with its attempt cleared, as every stamp but a publication is: left set, the stamped root would read as a
// published one, and its reads would go to an archive copy that is not there.
func TestLiveRetention_TheStampClearsTheAttempt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 1)
	root := seedTrees(t, r.db, 1, callShape)[0]
	closeTreesAgo(t, r.db, 4*24*time.Hour, root)
	at, arg := database.SQLTime(r.db.DriverName(), time.Now().Add(-3*24*time.Hour))
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+at+" WHERE flow_id=?", arg, root)
	assert.Must(assert.NoError(err))
	r.retentionRounds(ctx)
	assert.Equal(1, stampOf(t, r, root), "stamped")
	var attempted int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND "+
		attemptedSQL(r.db.DriverName()), root).Scan(&attempted))
	assert.Zero(attempted, "its attempt cleared")
}

// TestLiveRetention_AHeldRootIsNeitherStampedNorCounted pins that the flows counted archive_failed are those of the
// roots the batch locked: a root another transaction holds is skipped, and so is its count, on PostgreSQL and MySQL.
func TestLiveRetention_AHeldRootIsNeitherStampedNorCounted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 1)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	first := seedTrees(t, r.db, 1, callShape)[0] // two flows
	held := seedTrees(t, r.db, 1, callShape)[0]
	free := seedWideTree(t, r.db, 1, 2, 1) // three flows
	// In close order: the sweep's first batch takes one root, its second the held and the free together.
	closeTreesAgo(t, r.db, 4*24*time.Hour+2*time.Minute, first)
	closeTreesAgo(t, r.db, 4*24*time.Hour+time.Minute, held)
	closeTreesAgo(t, r.db, 4*24*time.Hour, free)
	release := holdRows(t, r, "dwarf_flows", held)
	r.retentionRounds(ctx)
	release()
	assert.Zero(stampOf(t, r, held), "skipped")
	assert.Equal(1, stampOf(t, r, first))
	assert.Equal(1, stampOf(t, r, free))
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	assert.Equal(int64(5), reapedFor(rm, ReasonArchiveFailed), "the stamped trees' flows alone")
}
