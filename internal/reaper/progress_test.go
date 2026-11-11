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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/sequel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestStage_RestartInsideLargeFlow(t *testing.T) {
	for _, shape := range []struct {
		name                            string
		rootSteps, children, childSteps int
	}{{"root", 20, 0, 0}, {"child", 1, 1, 20}} {
		t.Run(shape.name, func(t *testing.T) {
			r := stageRig(t)
			oneChunk(r)
			ctx := t.Context()
			today, err := r.archive.Today(ctx)
			if err != nil {
				t.Fatal(err)
			}
			root := seedWideTree(t, r.db, shape.rootSteps, shape.children, shape.childSteps)
			for range 20 {
				r.reaper = New()
				r.reaper.SetSeams(r.seams)
				r.pass(ctx)
			}
			var n int
			if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+migrations.StepsTable(today)).Scan(&n); err != nil {
				t.Fatal(err)
			}
			visible := visibleInDay(t, r, today, root)
			if visible != shape.children+1 {
				t.Fatalf("20 fresh passes: archived steps=%d, visible flows=%d; want %d", n, visible, shape.children+1)
			}
		})
	}
}

func TestStage_OldestAttemptDuringDayOutage(t *testing.T) {
	r := stageRig(t)
	ctx := t.Context()
	today, err := r.archive.Today(ctx)
	if err != nil {
		t.Fatal(err)
	}
	root := seedWideTree(t, r.db, 3, 10, 3)
	setAttempt(t, r, root, time.Now().Add(-time.Minute))
	r.seams.InjectN(join(FaultStageChunk, strconv.Itoa(root)), 1000)
	if _, err = r.db.ExecContext(ctx, "DROP TABLE "+migrations.FlowsTable(today+1)); err != nil {
		t.Fatal(err)
	}
	if err = r.archive.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		r.pass(ctx)
	}
	age := r.reaper.Observe(1, time.Now()).Kinds[0].OldestAttempt
	if age < time.Minute {
		t.Fatalf("unpublished minute-old attempt reports %v during missing-tomorrow outage", age)
	}
}

func TestStage_OldestAttemptAcrossLongSweep(t *testing.T) {
	r := stageRig(t)
	ctx := t.Context()
	roots := seedTrees(t, r.db, 2, callShape)
	setAttempt(t, r, roots[0], time.Now().Add(-time.Hour-10*time.Second))
	setAttempt(t, r, roots[1], time.Now().Add(-20*time.Second))
	k := closedDue(0, 0)
	p := r.p()
	candidates, _, err := p.scanDue(ctx, k, "", nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	end, err := p.end(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := walkKey{1, KindClosed, true}
	r.reaper.walks.put(key, walk{after: candidates[0], end: end, started: time.Now().Add(-time.Hour), batch: 1, attemptAge: 10 * time.Second, attemptSeen: time.Now().Add(-time.Hour)})
	r.seams.InjectN(join(FaultStageChunk, strconv.Itoa(roots[1])), 1000)
	d := &filingDay{}
	if err = p.refreshDay(ctx, d); err != nil {
		t.Fatal(err)
	}
	p.walk(ctx, k, func() (*filingDay, bool) { return d, true }, ReasonNoRetention, true)
	age := r.reaper.Observe(1, time.Now()).Kinds[0].OldestAttempt
	if age < time.Hour {
		t.Fatalf("oldest attempt was replaced by a younger observation: got %v; want at least 1h", age)
	}
}

func TestDelete_MarkedDiscoveryRespectsBudget(t *testing.T) {
	r := newRig(t, 30)
	ctx := t.Context()
	if r.db.DriverName() != "pgx" {
		t.Skip("PostgreSQL table-lock probe")
	}
	today, err := r.archive.Today(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []int{today - 3, today - 2, today - 1} {
		if err = makeArchiveDay(t, r, d); err != nil {
			t.Fatal(err)
		}
	}
	if err = r.archive.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	ready := make(chan error, 1)
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = r.db.Transact(ctx, func(tx *sequel.Tx) error {
			for d := today - 3; d <= today; d++ {
				if _, err := tx.ExecContext(ctx, "LOCK TABLE "+migrations.FlowsTable(d)+" IN ACCESS EXCLUSIVE MODE"); err != nil {
					ready <- err
					return err
				}
			}
			ready <- nil
			<-release
			return nil
		})
	}()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
	defer func() { close(release); <-done }()
	r.cfg.Budget = 30 * time.Millisecond
	r.cfg.Attempt = 100 * time.Millisecond
	start := time.Now()
	r.reaper.DeleteMarked(ctx, r.cfg, r.shard())
	elapsed := time.Since(start)
	if elapsed > 250*time.Millisecond {
		t.Fatalf("discovery continued across four blocked days: elapsed=%v, budget=%v, per-attempt=%v", elapsed, r.cfg.Budget, r.cfg.Attempt)
	}
}

// Publication counts once even when cleanup is atomic, chunked, or interrupted by Delete.
func TestStage_PublishedCleanupNotUnarchived(t *testing.T) {
	for _, budget := range []int{4, 100} {
		for _, deleteAt := range []string{"never", "published", "mid-cleanup"} {
			t.Run(strconv.Itoa(budget)+"/"+deleteAt, func(t *testing.T) {
				r := stageRig(t)
				ctx := t.Context()
				r.cfg.BatchRows = budget
				reader := sdkmetric.NewManualReader()
				provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
				defer provider.Shutdown(ctx)
				if err := r.reaper.SetMeter(provider.Meter("test")); err != nil {
					t.Fatal(err)
				}
				root := seedWideTree(t, r.db, 3, 10, 3)
				p := r.p()
				d := &filingDay{}
				if err := p.refreshDay(ctx, d); err != nil {
					t.Fatal(err)
				}
				n, _, err := p.stage(ctx, closedDue(0, 0), root, d, time.Now())
				if err != nil || n != 11 {
					t.Fatalf("publication: n=%d err=%v", n, err)
				}
				// stage's caller accounts for publication.
				r.reaper.metrics.flowsArchived(ctx, 1, n)
				if deleteAt == "mid-cleanup" {
					partial := r.p()
					partial.cfg.BatchRows = 4
					removed, _, err := partial.deleteChunk(ctx, root, &chunkPos{})
					if err != nil {
						t.Fatal(err)
					}
					r.reaper.metrics.flowsReaped(ctx, 1, ReasonDeleted, removed.unarchived)
				}
				if deleteAt != "never" {
					if _, err := r.reaper.DeleteLive(ctx, r.db, r.archive.DayLength(), root); err != nil {
						t.Fatal(err)
					}
				}
				// A real unarchived deletion in the same batch must still be counted.
				ordinary := seedTrees(t, r.db, 1, callShape)[0]
				if _, err := r.reaper.DeleteLive(ctx, r.db, r.archive.DayLength(), ordinary); err != nil {
					t.Fatal(err)
				}
				for range 3 {
					r.pass(ctx)
				}
				var rm metricdata.ResourceMetrics
				if err := reader.Collect(ctx, &rm); err != nil {
					t.Fatal(err)
				}
				var archived, reaped int64
				for _, sm := range rm.ScopeMetrics {
					for _, m := range sm.Metrics {
						if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
							for _, dp := range sum.DataPoints {
								if m.Name == "dwarf_flows_archived" {
									archived += dp.Value
								}
								if m.Name == "dwarf_flows_reaped" {
									reaped += dp.Value
								}
							}
						}
					}
				}
				if archived != 11 || reaped != 2 {
					t.Fatalf("archived=%d, removed without archiving=%d; want 11 and 2", archived, reaped)
				}
				if flows, _, _ := rowsOf(t, r, root); flows != 0 {
					t.Fatalf("published live copy remains: %d", flows)
				}
			})
		}
	}
}
