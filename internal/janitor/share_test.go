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
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// shareJanitor returns a Manual, active janitor on the rig's shard with the jobs the engine adds for two fast jobs of
// each kind: fast archiving 1 and 2 of 2, each with sweep archiving behind it, and fast deletion 1 and 2 of 2, each with
// sweep deletion behind it, under ShareName's names and with the rig's settings; d reports for it, the rig's logs and
// seams when nil.
func shareJanitor(t *testing.T, r *rig, d *diagnostics.Diags) *Janitor {
	t.Helper()
	if d == nil {
		d = diagnostics.New()
		d.SetLogger(slog.New(r.logs))
		d.SetSeams(r.seams)
	}
	s := r.settings
	j := New(r.shard, d)
	for k := 1; k <= 2; k++ {
		fa := NewFastArchiving(r.archive, r.retention)
		fa.SetShare(k, 2)
		fa.SetGrace(s.grace)
		fa.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
		fa.SetQuietMargin(!s.noQuietMargin)
		j.AddJob(ShareName(JobFastArchiving, k), fa)
		sa := NewSweepArchiving(fa, r.archive, r.retention)
		sa.SetGrace(s.grace)
		sa.SetOverdue(s.overdue)
		sa.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
		sa.SetQuietMargin(!s.noQuietMargin)
		j.AddJob(ShareName(JobSweepArchiving, k), sa)
		fd := NewFastDeletion()
		fd.SetShare(k, 2)
		fd.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
		j.AddJob(ShareName(JobFastDeletion, k), fd)
		sd := NewSweepDeletion(fd)
		sd.SetOverdue(s.overdue)
		sd.SetBatch(s.batchRoots, s.batchRows, s.batchBytes)
		j.AddJob(ShareName(JobSweepDeletion, k), sd)
	}
	j.SetManual(true)
	j.Start(t.Context())
	j.Resume()
	t.Cleanup(j.Stop)
	r.shard.SetResidency(r.count, r.ordinal, r.inClass, time.Now().Add(time.Hour))
	return j
}

// roundsOf runs the rounds of j's job named name until one makes no progress.
func roundsOf(ctx context.Context, j *Janitor, name string) {
	rn := j.jobs[slices.IndexFunc(j.jobs, func(rn *runningJob) bool { return rn.name == name })]
	for j.round(ctx, rn) {
	}
}

// inShare reports whether root is in the k-th share of of under a residue class of count dispatchers, 0 for none.
func inShare(root, k, of, count int) bool {
	return (root/max(count, 1))%of == k-1
}

// shareCases are the residue classes the share tests split under: none, and this replica's of two and of three.
var shareCases = []struct {
	count, ordinal int
	ok             bool
}{{0, 0, false}, {2, 0, true}, {2, 1, true}, {3, 2, true}}

// TestShare_TheSharesSplitTheClosedRoots pins the closed scan's share: two jobs of one replica never take the same
// root, and together take every root the replica's whole scan would - its class's while fresh, every class's once
// overdue by the steal grace - each in the share its flow_id falls to under the class's count; and over every class
// and both shares, each root is taken exactly once.
func TestShare_TheSharesSplitTheClosedRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	roots := seedTrees(t, r.db, 24, soloShape) // consecutive ids
	closeTreesAgo(t, r.db, time.Minute, roots...)
	sel := func(s share, count, ordinal int, ok bool, steal time.Duration) []int {
		where, args := closedResidency(count, ordinal, ok, s, 0, steal)
		found, _, err := scanClosed(ctx, r.db, r.seams, 0, where, args, 512)
		assert.Must(assert.NoError(err))
		return slices.Sorted(slices.Values(rootIDs(found)))
	}
	for _, c := range shareCases {
		for _, steal := range []time.Duration{time.Hour, 0} {
			whole := sel(share{1, 1}, c.count, c.ordinal, c.ok, steal)
			var union []int
			for k := 1; k <= 2; k++ {
				got := sel(share{k, 2}, c.count, c.ordinal, c.ok, steal)
				for _, id := range got {
					assert.True(inShare(id, k, 2, c.count), "root %d in share %d under %v", id, k, c)
					assert.False(slices.Contains(union, id), "root %d taken by two shares under %v", id, c)
				}
				union = append(union, got...)
			}
			assert.Equal(whole, slices.Sorted(slices.Values(union)), "the shares take what the whole scan does under %v", c)
			if steal == 0 || !c.ok {
				assert.Equal(roots, whole, "every root under %v, steal %v", c, steal)
			}
		}
	}
	// Fresh, each root is one class's and one share's.
	for _, count := range []int{2, 3} {
		var all []int
		for ordinal := range count {
			for k := 1; k <= 2; k++ {
				all = append(all, sel(share{k, 2}, count, ordinal, true, time.Hour)...)
			}
		}
		assert.Equal(roots, slices.Sorted(slices.Values(all)), "every root exactly once of %d classes", count)
	}
}

// TestShare_TheSharesSplitTheStampedRoots pins the stamped scan's share, as the closed scan's.
func TestShare_TheSharesSplitTheStampedRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	roots := seedTrees(t, r.db, 24, soloShape)
	for _, id := range roots {
		stampTree(t, r.db, id) // due a second ago
	}
	sel := func(s share, count, ordinal int, ok bool, steal time.Duration) []int {
		where, args := stampedResidency(count, ordinal, ok, s, steal)
		found, err := scanStamped(ctx, r.db, r.seams, where, args, 512)
		assert.Must(assert.NoError(err))
		return rootIDs(found)
	}
	for _, c := range shareCases {
		for _, steal := range []time.Duration{time.Hour, 0} {
			whole := sel(share{1, 1}, c.count, c.ordinal, c.ok, steal)
			var union []int
			for k := 1; k <= 2; k++ {
				got := sel(share{k, 2}, c.count, c.ordinal, c.ok, steal)
				for _, id := range got {
					assert.True(inShare(id, k, 2, c.count), "root %d in share %d under %v", id, k, c)
					assert.False(slices.Contains(union, id), "root %d taken by two shares under %v", id, c)
				}
				union = append(union, got...)
			}
			assert.Equal(whole, slices.Sorted(slices.Values(union)), "the shares take what the whole scan does under %v", c)
			if steal == 0 || !c.ok {
				assert.Equal(roots, whole, "every root under %v, steal %v", c, steal)
			}
		}
	}
}

// TestShare_EachFastJobTakesItsShare pins two fast archiving jobs and two fast deletion jobs of one janitor end to end:
// each removes only its share's roots, and the two remove every root - under a residue class, with another class's
// overdue roots stolen into the share they fall to.
func TestShare_EachFastJobTakesItsShare(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	for _, c := range shareCases[:2] {
		r := newRig(t, 30)
		r.setResidency(c.count, c.ordinal, c.ok)
		today, err := r.archive.Today(ctx)
		assert.Must(assert.NoError(err))
		closed := seedTrees(t, r.db, 12, soloShape)
		stamped := seedTrees(t, r.db, 12, soloShape)
		for _, id := range stamped {
			stampTree(t, r.db, id)
		}
		var stolen []int // another class's roots, overdue past the steal grace before the first round
		for _, id := range append(slices.Clone(closed), stamped...) {
			if c.ok && id%c.count != c.ordinal {
				stolen = append(stolen, id)
			}
		}
		for _, id := range stolen {
			backdate(t, r, id, 10*time.Minute)
		}
		j := shareJanitor(t, r, nil)
		gone := func(id int) bool {
			flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
			return flows == 0
		}
		for k := 1; k <= 2; k++ {
			roundsOf(ctx, j, ShareName(JobFastArchiving, k))
			roundsOf(ctx, j, ShareName(JobFastDeletion, k))
			for _, id := range closed {
				want := (k == 2 || inShare(id, 1, 2, c.count))
				assert.Equal(want, archived(t, r, today, id), "closed root %d after job %d under %v", id, k, c)
			}
			for _, id := range stamped {
				want := (k == 2 || inShare(id, 1, 2, c.count))
				assert.Equal(want, gone(id), "stamped root %d after job %d under %v", id, k, c)
			}
		}
		if now, _ := r.archive.Today(ctx); now != today {
			t.Skip("midnight passed")
		}
	}
}

// TestShare_EachSweepTakesWhatItsFastJobPassed pins each sweep to its own fast job: it sweeps its fast job's share
// behind that job's position, never the other share's roots, though they are behind the position too, and nothing
// before its own fast job has run.
func TestShare_EachSweepTakesWhatItsFastJobPassed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	closed := seedTrees(t, r.db, 8, soloShape)
	stamped := seedTrees(t, r.db, 8, soloShape)
	for _, id := range stamped {
		stampTree(t, r.db, id)
	}
	j := shareJanitor(t, r, nil)
	gone := func(id int) bool {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		return flows == 0
	}
	check := func(when string, removed func(id int) bool) {
		t.Helper()
		for _, id := range closed {
			assert.Equal(removed(id), archived(t, r, today, id), "closed root %d %s", id, when)
		}
		for _, id := range stamped {
			assert.Equal(removed(id), gone(id), "stamped root %d %s", id, when)
		}
	}
	// The first fast jobs fail every move and deletion of their share, and pass them; the second ones do not run.
	r.seams.InjectN(diagnostics.FaultReapMidTree, 10000)
	roundsOf(ctx, j, ShareName(JobFastArchiving, 1))
	roundsOf(ctx, j, ShareName(JobFastDeletion, 1))
	r.seams.Withdraw(diagnostics.FaultReapMidTree)
	check("after the first fast jobs failed", func(int) bool { return false })

	for k := 2; k >= 1; k-- {
		roundsOf(ctx, j, ShareName(JobSweepArchiving, k))
		roundsOf(ctx, j, ShareName(JobSweepDeletion, k))
	}
	check("after the sweeps", func(id int) bool { return inShare(id, 1, 2, 0) })

	r.seams.InjectN(diagnostics.FaultReapMidTree, 10000)
	roundsOf(ctx, j, ShareName(JobFastArchiving, 2))
	roundsOf(ctx, j, ShareName(JobFastDeletion, 2))
	r.seams.Withdraw(diagnostics.FaultReapMidTree)
	roundsOf(ctx, j, ShareName(JobSweepArchiving, 1))
	roundsOf(ctx, j, ShareName(JobSweepDeletion, 1))
	check("after the first sweeps again", func(id int) bool { return inShare(id, 1, 2, 0) })
	roundsOf(ctx, j, ShareName(JobSweepArchiving, 2))
	roundsOf(ctx, j, ShareName(JobSweepDeletion, 2))
	check("after the second sweeps", func(int) bool { return true })
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed")
	}
}

// TestShare_EachFastArchivingJobReportsItsLag pins the lag per job: each fast archiving job probes its own share and
// reports under its own name, so a shard's two series are the oldest due root of each share, and their max the
// shard's.
func TestShare_EachFastArchivingJobReportsItsLag(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.grace = 30 * time.Second
	roots := seedTrees(t, r.db, 2, soloShape) // consecutive ids: one in each share
	backdate(t, r, roots[0], 150*time.Second)
	backdate(t, r, roots[1], 90*time.Second)
	for _, id := range roots {
		r.seams.InjectN(diagnostics.Join(diagnostics.FaultReapSizeErr, strconv.Itoa(id)), 10000)
	}
	reader := sdkmetric.NewManualReader()
	d := diagnostics.New()
	d.SetLogger(slog.New(r.logs))
	d.SetSeams(r.seams)
	d.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	j := shareJanitor(t, r, d)
	for k := 1; k <= 2; k++ {
		roundsOf(ctx, j, ShareName(JobFastArchiving, k))
	}
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	lags := map[string]float64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if g, ok := m.Data.(metricdata.Gauge[float64]); ok && m.Name == "dwarf_archive_lag_seconds" {
				for _, dp := range g.DataPoints {
					job, _ := dp.Attributes.Value("job")
					lags[job.AsString()] = dp.Value
				}
			}
		}
	}
	assert.Len(lags, 2, "a series per job: %v", lags)
	jobOf := func(id int) string {
		if inShare(id, 1, 2, 0) {
			return ShareName(JobFastArchiving, 1)
		}
		return ShareName(JobFastArchiving, 2)
	}
	older, newer := lags[jobOf(roots[0])], lags[jobOf(roots[1])]
	assert.True(older >= 119 && older < 140, "the older root's share: %v", lags)
	assert.True(newer >= 59 && newer < 80, "the newer root's share: %v", lags)
}

// TestShare_TheScansWalkTheirIndexes pins the share's SQL on PostgreSQL, in a populated table with statistics: with a
// share and a residue class, the closed scan walks idx_dwarf_flows_status in order on one process, and the stamped scan
// walks idx_dwarf_flows_delete_after in order. Written as a range, the share is estimated at 0.5% of the rows: the
// closed scan then starts parallel workers, and the stamped scan reads and sorts every stamped root.
func TestShare_TheScansWalkTheirIndexes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	if r.db.DriverName() != "pgx" {
		t.Skip("PostgreSQL's planner")
	}
	const n = 300_000
	_, err := r.db.ExecContext(ctx, "INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash,"+
		" status, surgraph_flow_id, surgraph_step_id, root_flow_id, updated_at) SELECT 'tok', 'seed/tree', 'Seed', $1, '',"+
		" 'completed', 0, 0, 0, (NOW() AT TIME ZONE 'UTC') - make_interval(secs => ($2 - g) * 0.01 + 120)"+
		" FROM generate_series(1, $2) g", []byte("{}"), n)
	assert.Must(assert.NoError(err))
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1 WHERE flow_id % 10 IN (0, 5)")
	assert.Must(assert.NoError(err))
	_, err = r.db.ExecContext(ctx, "ANALYZE dwarf_flows")
	assert.Must(assert.NoError(err))
	plan := func(stmt string, args []any) string {
		rows, err := r.db.QueryContext(ctx, "EXPLAIN "+stmt, args...)
		assert.Must(assert.NoError(err))
		defer rows.Close()
		var b strings.Builder
		for rows.Next() {
			var line string
			assert.Must(assert.NoError(rows.Scan(&line)))
			b.WriteString(line + "\n")
		}
		assert.Must(assert.NoError(rows.Err()))
		return b.String()
	}
	where, args := closedResidency(2, 0, true, share{2, 2}, time.Minute, stealGrace)
	got := plan(closedScanSQL(time.Minute, where, args, defaultBatchRoots))
	assert.Contains(got, "Index Scan using idx_dwarf_flows_status", got)
	assert.False(strings.Contains(got, "Gather") || strings.Contains(got, "Bitmap"), "one process, in index order: %s", got)
	where, args = stampedResidency(2, 0, true, share{2, 2}, stealGrace)
	got = plan(stampedScanSQL(r.db.DriverName(), where, args, defaultBatchRoots))
	assert.Contains(got, "Index Scan using idx_dwarf_flows_delete_after", got)
	assert.False(strings.Contains(got, "Gather") || strings.Contains(got, "Bitmap") || strings.Contains(got, "Sort"),
		"one process, in index order: %s", got)
}
