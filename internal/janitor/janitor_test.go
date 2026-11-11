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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// noTurn is a Turn that orders nothing.
func noTurn(ctx context.Context) (context.Context, func()) {
	return ctx, func() {}
}

// await polls cond until it holds or the deadline passes.
func await(t *testing.T, within time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestJanitor_EachJobRunsOnItsOwnGoroutine pins one goroutine per job: a job blocked in its round holds back no other.
func TestJanitor_EachJobRunsOnItsOwnGoroutine(t *testing.T) {
	t.Parallel()
	var other atomic.Int64
	blocked := make(chan struct{}, 1)
	j := New(Config{Turn: noTurn, Idle: time.Millisecond, Jobs: []Job{
		{Name: "blocked", Round: func(ctx context.Context, _ Batch) bool {
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return false
		}},
		{Name: "other", Round: func(ctx context.Context, _ Batch) bool {
			other.Add(1)
			return false
		}},
	}})
	j.Start(t.Context())
	defer j.Stop()
	<-blocked
	await(t, 5*time.Second, func() bool { return other.Load() >= 10 }, "a blocked job held back another")
}

// TestJanitor_ARoundWithoutProgressWaits pins the idle wait: a job whose rounds make no progress - its candidates
// held or failing - runs a round per idle period rather than spinning, while one that makes progress runs back to
// back.
func TestJanitor_ARoundWithoutProgressWaits(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	var idle, busy atomic.Int64
	j := New(Config{Turn: noTurn, Idle: 100 * time.Millisecond, Jobs: []Job{
		{Name: "idle", Round: func(ctx context.Context, _ Batch) bool {
			idle.Add(1)
			return false
		}},
		{Name: "busy", Round: func(ctx context.Context, _ Batch) bool {
			busy.Add(1)
			time.Sleep(time.Millisecond)
			return true
		}},
	}})
	j.Start(t.Context())
	time.Sleep(time.Second)
	j.Stop()
	assert.True(idle.Load() >= 5 && idle.Load() <= 12, "%d rounds without progress in a second at 100ms idle", idle.Load())
	assert.True(busy.Load() > 100, "%d rounds with progress in a second", busy.Load())
}

// TestJanitor_DrainReturnsWhereAJobWouldWait pins the test hook: Drain runs a job until a round makes no progress, goes
// on while any job made progress, and returns once a pass over every job makes none - also when every round fails,
// so a test with held or failing roots cannot hang. It starts no goroutine.
func TestJanitor_DrainReturnsWhereAJobWouldWait(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// A job with three rounds of work, and one that always fails.
	var work, failing atomic.Int64
	j := New(Config{Turn: noTurn, Manual: true, Jobs: []Job{
		{Name: "work", Round: func(ctx context.Context, _ Batch) bool { return work.Add(1) <= 3 }},
		{Name: "failing", Round: func(ctx context.Context, _ Batch) bool { failing.Add(1); return false }},
	}})
	j.Start(t.Context())
	defer j.Stop()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(int64(0), work.Load()+failing.Load(), "a Manual janitor runs no goroutine")
	j.Drain(t.Context())
	assert.Equal(int64(5), work.Load(), "three rounds of work, then one without, then a pass without")
	assert.Equal(int64(2), failing.Load(), "a failing job is run once per pass")

	// Two jobs that feed each other: Drain goes on until both are done.
	var a, b atomic.Int64
	var forB, forA atomic.Int64
	forA.Store(1)
	j = New(Config{Turn: noTurn, Manual: true, Jobs: []Job{
		{Name: "a", Round: func(ctx context.Context, _ Batch) bool {
			if forA.Load() == 0 {
				return false
			}
			forA.Add(-1)
			if a.Add(1) < 3 {
				forB.Add(1)
			}
			return true
		}},
		{Name: "b", Round: func(ctx context.Context, _ Batch) bool {
			if forB.Load() == 0 {
				return false
			}
			forB.Add(-1)
			b.Add(1)
			forA.Add(1)
			return true
		}},
	}})
	j.Start(t.Context())
	defer j.Stop()
	j.Drain(t.Context())
	assert.Equal(int64(3), a.Load())
	assert.Equal(int64(2), b.Load())
}

// TestJanitor_StopCancelsABlockedJob pins the stop: a job blocked in its round is cancelled, and Stop returns once it
// has, without waiting out the block.
func TestJanitor_StopCancelsABlockedJob(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	in := make(chan struct{})
	var ended atomic.Bool
	j := New(Config{DB: newRig(t, 30).db, Turn: noTurn, Jobs: []Job{
		{Name: "blocked", Round: func(ctx context.Context, batch Batch) bool {
			_ = batch(ctx, func(ctx context.Context, _ Handle) error {
				close(in)
				select {
				case <-ctx.Done():
				case <-time.After(time.Minute):
				}
				return ctx.Err()
			})
			ended.Store(true)
			<-ctx.Done()
			return false
		}},
	}})
	j.Start(t.Context())
	<-in
	start := time.Now()
	j.Stop()
	assert.True(time.Since(start) < 5*time.Second, "Stop took %v", time.Since(start))
	assert.True(ended.Load(), "Stop returned before the job did")
}

// TestJanitor_EachBatchTakesAFreshClaim pins the turn: each batch takes a claim of its own, so a job's later batch
// is younger than dispatch that arrived meanwhile rather than older than all of it; and a batch run on a context that
// carries a claim already - a batch's own - fails rather than keep that claim's age or nest a turn.
func TestJanitor_EachBatchTakesAFreshClaim(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	set := turnstile.NewSet()
	set.Resize(1, 4)
	turn := func(ctx context.Context) (context.Context, func()) {
		ctx = set.ContextWithPriority(ctx, 1, 10)
		pass := turnstile.WaitTurn(ctx)
		return ctx, pass.Return
	}
	var claims []turnstile.Claim
	var nested error
	j := New(Config{DB: newRig(t, 30).db, Turn: turn, Manual: true, Jobs: []Job{
		{Name: "two", Round: func(ctx context.Context, batch Batch) bool {
			for range 2 {
				err := batch(ctx, func(batchCtx context.Context, _ Handle) error {
					c, ok := turnstile.ClaimFrom(batchCtx)
					assert.True(ok)
					claims = append(claims, c)
					nested = batch(batchCtx, func(context.Context, Handle) error { return nil })
					return nil
				})
				assert.NoError(err)
				time.Sleep(10 * time.Millisecond)
			}
			return false
		}},
	}})
	j.Start(t.Context())
	defer j.Stop()
	j.Drain(t.Context())
	assert.Must(assert.Len(claims, 2))
	assert.NotEqual(claims[0].Seq, claims[1].Seq, "each batch is a job of its own at the turnstile")
	assert.True(claims[1].Since.After(claims[0].Since), "the second batch's claim is as old as the batch, not the round")
	assert.Error(nested, "a batch inside a batch")
}

// TestJanitor_ObserveReportsTimeSinceProgress pins the reading: zero at Start, growing through rounds without
// progress, reset by one with.
func TestJanitor_ObserveReportsTimeSinceProgress(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	var progress atomic.Bool
	j := New(Config{Turn: noTurn, Manual: true, Jobs: []Job{
		{Name: "job", Round: func(ctx context.Context, _ Batch) bool { return progress.Swap(false) }},
	}})
	assert.Len(j.Observe(time.Now()).Jobs, 0, "nothing to report before Start")
	j.Start(t.Context())
	defer j.Stop()
	r := j.Observe(time.Now()).Jobs
	assert.Must(assert.Len(r, 1))
	assert.Equal("job", r[0].Job)
	assert.True(r[0].SinceProgress < time.Second)

	time.Sleep(100 * time.Millisecond)
	j.Drain(t.Context())
	assert.True(j.Observe(time.Now()).Jobs[0].SinceProgress >= 100*time.Millisecond, "a round without progress resets nothing")
	progress.Store(true)
	j.Drain(t.Context())
	assert.True(j.Observe(time.Now()).Jobs[0].SinceProgress < 50*time.Millisecond, "a round with progress resets it")
}

// TestJanitor_ADrainBesideTheGoroutineNeverOverlapsARound pins that one job's rounds never overlap, so a test's Drain
// on a janitor that runs its jobs is safe.
func TestJanitor_ADrainBesideTheGoroutineNeverOverlapsARound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	var inside, overlaps, rounds atomic.Int64
	j := New(Config{Turn: noTurn, Idle: time.Millisecond, Jobs: []Job{
		{Name: "job", Round: func(ctx context.Context, _ Batch) bool {
			if inside.Add(1) > 1 {
				overlaps.Add(1)
			}
			time.Sleep(100 * time.Microsecond)
			inside.Add(-1)
			return rounds.Add(1) < 2000
		}},
	}})
	j.Start(t.Context())
	for rounds.Load() < 2000 {
		j.Drain(t.Context())
	}
	j.Stop()
	assert.Equal(int64(0), overlaps.Load())
}

// TestJanitor_ABatchRunsOnOneConnection pins the batch's connection: every statement of a batch runs on the one
// connection the batch took, so it waits for the pool once rather than once a statement.
func TestJanitor_ABatchRunsOnOneConnection(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	db := newRig(t, 30).db
	var idSQL string
	switch db.DriverName() {
	case "pgx":
		idSQL = "SELECT pg_backend_pid()"
	case "mysql", "mariadb":
		idSQL = "SELECT CONNECTION_ID()"
	case "mssql":
		idSQL = "SELECT @@SPID"
	default:
		t.Skip("SQLite names no connection")
	}
	j := New(Config{DB: db, Turn: noTurn, Manual: true, Jobs: []Job{
		{Name: "job", Round: func(ctx context.Context, batch Batch) bool {
			assert.NoError(batch(ctx, func(ctx context.Context, h Handle) error {
				ids := map[int64]bool{}
				for range 8 {
					var id int64
					if err := h.QueryRowContext(ctx, idSQL).Scan(&id); err != nil {
						return err
					}
					ids[id] = true
				}
				assert.Len(ids, 1, "every statement on one connection")
				return nil
			}))
			return false
		}},
	}})
	j.Start(t.Context())
	defer j.Stop()
	j.Drain(t.Context())
}

// TestJanitor_BatchTimings pins dwarf_janitor_batch_seconds: one sample per batch for each part, by shard and job.
func TestJanitor_BatchTimings(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	reader := sdkmetric.NewManualReader()
	inst, err := NewInstruments(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	assert.Must(assert.NoError(err))
	j := New(Config{Shard: 3, DB: newRig(t, 30).db, Turn: noTurn, Manual: true, Instruments: inst, Jobs: []Job{
		{Name: "job", Round: func(ctx context.Context, batch Batch) bool {
			for range 2 {
				_ = batch(ctx, func(context.Context, Handle) error { time.Sleep(10 * time.Millisecond); return nil })
			}
			return false
		}},
	}})
	j.Start(t.Context())
	defer j.Stop()
	j.Drain(t.Context())

	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(t.Context(), &rm)))
	counts := map[string]uint64{}
	var work float64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			h, ok := m.Data.(metricdata.Histogram[float64])
			if m.Name != "dwarf_janitor_batch_seconds" || !ok {
				continue
			}
			for _, dp := range h.DataPoints {
				shard, _ := dp.Attributes.Value("shard")
				job, _ := dp.Attributes.Value("job")
				part, _ := dp.Attributes.Value("part")
				assert.Equal("3", shard.AsString())
				assert.Equal("job", job.AsString())
				counts[part.AsString()] += dp.Count
				if part.AsString() == partWork {
					work = dp.Sum
				}
			}
		}
	}
	assert.Equal(map[string]uint64{partTurn: 2, partAcquire: 2, partWork: 2}, counts)
	assert.True(work >= 0.02, "work %v", work)
}

// TestJanitor_ABatchThatCannotTakeItsConnectionFails pins the batch's failure to take its connection: the batch
// returns the error without running its work, and records its turn and acquisition but no work.
func TestJanitor_ABatchThatCannotTakeItsConnectionFails(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	reader := sdkmetric.NewManualReader()
	inst, err := NewInstruments(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	assert.Must(assert.NoError(err))
	seams := seamster.New(true)
	seams.InjectN(FaultConnErr, 1)
	ran := false
	var batchErr error
	j := New(Config{DB: newRig(t, 30).db, Turn: noTurn, Manual: true, Instruments: inst, Seams: seams, Jobs: []Job{
		{Name: "job", Round: func(ctx context.Context, batch Batch) bool {
			batchErr = batch(ctx, func(context.Context, Handle) error { ran = true; return nil })
			return false
		}},
	}})
	j.Start(t.Context())
	defer j.Stop()
	j.Drain(t.Context())
	assert.Error(batchErr)
	assert.False(ran, "the work did not run")
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(t.Context(), &rm)))
	parts := map[string]uint64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if h, ok := m.Data.(metricdata.Histogram[float64]); ok && m.Name == "dwarf_janitor_batch_seconds" {
				for _, dp := range h.DataPoints {
					part, _ := dp.Attributes.Value("part")
					parts[part.AsString()] += dp.Count
				}
			}
		}
	}
	assert.Equal(map[string]uint64{partTurn: 1, partAcquire: 1}, parts)
}
