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

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// testJanitor returns a started, active janitor of shard 1 on db, reporting through d, that runs jobs in place of
// its own, set up by set before it starts.
func testJanitor(t *testing.T, db *sequel.DB, d *diagnostics.Diags, set func(j *Janitor), jobs ...*funcJob) *Janitor {
	t.Helper()
	j := New(database.NewShard(1, db), d)
	for _, jb := range jobs {
		j.AddJob(jb.name, jb)
	}
	if set != nil {
		set(j)
	}
	j.Start(t.Context())
	j.Resume()
	t.Cleanup(j.Stop)
	return j
}

// funcJob is a test's job: its round, its idle wait, and what its reset does, nothing when nil.
type funcJob struct {
	name    string
	idle    time.Duration
	roundFn func(ctx context.Context, batch batchFunc) bool
	onReset func()
}

func (f *funcJob) attach(*Janitor, string) {}
func (f *funcJob) detach()                 {}
func (f *funcJob) idleWait() time.Duration { return f.idle }
func (f *funcJob) round(ctx context.Context, batch batchFunc) bool {
	return f.roundFn(ctx, batch)
}
func (f *funcJob) reset() {
	if f.onReset != nil {
		f.onReset()
	}
}

// testJob is a job of round named name, waiting a second after a round without progress, with nothing to reset.
func testJob(name string, round func(ctx context.Context, batch batchFunc) bool) *funcJob {
	return &funcJob{name: name, idle: time.Second, roundFn: round}
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

// blockingSQL is a statement that runs until it is cancelled.
func blockingSQL(driver string) string {
	switch driver {
	case "pgx":
		return "SELECT pg_sleep(600)"
	case "mysql", "mariadb":
		return "SELECT SLEEP(600)"
	case "mssql":
		return "WAITFOR DELAY '00:10:00'"
	default:
		return "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1000000000000) SELECT MAX(x) FROM c"
	}
}

// TestJanitor_EachJobRunsOnItsOwnGoroutine pins one goroutine per job: a job blocked in its round holds back no other.
func TestJanitor_EachJobRunsOnItsOwnGoroutine(t *testing.T) {
	t.Parallel()
	var other atomic.Int64
	blocked := make(chan struct{}, 1)
	testJanitor(t, nil, nil, func(j *Janitor) { j.SetIdle(time.Millisecond) },
		testJob("blocked", func(ctx context.Context, _ batchFunc) bool {
			select {
			case blocked <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return false
		}),
		testJob("other", func(ctx context.Context, _ batchFunc) bool {
			other.Add(1)
			return false
		}),
	)
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
	j := testJanitor(t, nil, nil, func(j *Janitor) { j.SetIdle(100 * time.Millisecond) },
		testJob("idle", func(ctx context.Context, _ batchFunc) bool {
			idle.Add(1)
			return false
		}),
		testJob("busy", func(ctx context.Context, _ batchFunc) bool {
			busy.Add(1)
			time.Sleep(time.Millisecond)
			return true
		}),
	)
	time.Sleep(time.Second)
	j.Stop()
	assert.True(idle.Load() >= 5 && idle.Load() <= 12, "%d rounds without progress in a second at 100ms idle", idle.Load())
	assert.True(busy.Load() > 100, "%d rounds with progress in a second", busy.Load())
}

// TestJanitor_EachJobKeepsItsOwnIdle pins that the idle wait is the job's own, until a test replaces every job's.
func TestJanitor_EachJobKeepsItsOwnIdle(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	var short, long atomic.Int64
	shortJob := testJob("short", func(context.Context, batchFunc) bool { short.Add(1); return false })
	shortJob.idle = 50 * time.Millisecond
	longJob := testJob("long", func(context.Context, batchFunc) bool { long.Add(1); return false })
	longJob.idle = 400 * time.Millisecond
	j := testJanitor(t, nil, nil, nil, shortJob, longJob)
	time.Sleep(time.Second)
	j.Stop()
	assert.True(short.Load() >= 10, "%d rounds at 50ms", short.Load())
	assert.True(long.Load() >= 2 && long.Load() <= 4, "%d rounds at 400ms", long.Load())
}

// TestJanitor_DrainReturnsWhereAJobWouldWait pins the test hook: Drain runs a job until a round makes no progress, goes
// on while any job made progress, and returns once a pass over every job makes none - also when every round fails,
// so a test with held or failing roots cannot hang. A Manual janitor starts no goroutine.
func TestJanitor_DrainReturnsWhereAJobWouldWait(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// A job with three rounds of work, and one that always fails.
	var work, failing atomic.Int64
	j := testJanitor(t, nil, nil, func(j *Janitor) { j.SetManual(true) },
		testJob("work", func(ctx context.Context, _ batchFunc) bool { return work.Add(1) <= 3 }),
		testJob("failing", func(ctx context.Context, _ batchFunc) bool { failing.Add(1); return false }),
	)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(int64(0), work.Load()+failing.Load(), "a Manual janitor runs no goroutine")
	j.Drain(t.Context())
	assert.Equal(int64(5), work.Load(), "three rounds of work, then one without, then a pass without")
	assert.Equal(int64(2), failing.Load(), "a failing job is run once per pass")

	// Two jobs that feed each other: Drain goes on until both are done.
	var a, b atomic.Int64
	var forB, forA atomic.Int64
	forA.Store(1)
	j = testJanitor(t, nil, nil, func(j *Janitor) { j.SetManual(true) },
		testJob("a", func(ctx context.Context, _ batchFunc) bool {
			if forA.Load() == 0 {
				return false
			}
			forA.Add(-1)
			if a.Add(1) < 3 {
				forB.Add(1)
			}
			return true
		}),
		testJob("b", func(ctx context.Context, _ batchFunc) bool {
			if forB.Load() == 0 {
				return false
			}
			forB.Add(-1)
			b.Add(1)
			forA.Add(1)
			return true
		}),
	)
	j.Drain(t.Context())
	assert.Equal(int64(3), a.Load())
	assert.Equal(int64(2), b.Load())
}

// TestJanitor_PauseAndResume pins the janitor's activity: a pause cancels the round in flight; a paused janitor runs
// no round, a Drain included; and a resume starts every job over - its position forgotten - and runs it again.
func TestJanitor_PauseAndResume(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	var rounds, resets, ended atomic.Int64
	in := make(chan struct{}, 1)
	jb := testJob("job", func(ctx context.Context, _ batchFunc) bool {
		rounds.Add(1)
		select {
		case in <- struct{}{}:
		default:
		}
		<-ctx.Done()
		ended.Add(1)
		return false
	})
	jb.onReset = func() { resets.Add(1) }
	j := testJanitor(t, nil, nil, nil, jb)
	<-in
	assert.Equal(int64(1), resets.Load(), "the first resume starts the job over too")

	j.Pause()
	await(t, 5*time.Second, func() bool { return ended.Load() == 1 }, "a pause cancels the round in flight")
	before := rounds.Load()
	time.Sleep(50 * time.Millisecond)
	j.Drain(t.Context())
	assert.Equal(before, rounds.Load(), "a paused janitor runs no round")

	j.Resume()
	<-in
	assert.Equal(int64(2), resets.Load(), "a resume starts the job over")
	assert.Equal(before+1, rounds.Load())
}

// TestJanitor_ABlockedStatementIsCancelled pins cancellation of a job inside a statement that would not end on its own
// - SQL Server's unbounded lock wait is the production case - by a pause and by a stop, and that the pool is usable
// after it.
func TestJanitor_ABlockedStatementIsCancelled(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	db := newRig(t, 30).db
	in := make(chan struct{}, 1)
	out := make(chan error, 1)
	j := testJanitor(t, db, nil, func(j *Janitor) { j.SetIdle(10 * time.Millisecond) },
		testJob("job", func(ctx context.Context, batch batchFunc) bool {
			err := batch(ctx, func(ctx context.Context, h handle) error {
				in <- struct{}{}
				_, err := h.ExecContext(ctx, blockingSQL(h.DriverName()))
				return err
			})
			out <- err
			return false
		}),
	)
	for _, stop := range []func(){j.Pause, j.Stop} {
		<-in
		time.Sleep(200 * time.Millisecond) // into the statement
		start := time.Now()
		stop()
		select {
		case err := <-out:
			assert.Error(err, "the statement was cancelled")
			assert.True(time.Since(start) < 10*time.Second*timeoutScale, "cancelled after %v", time.Since(start))
		case <-time.After(30 * time.Second * timeoutScale):
			t.Fatal("the statement was not cancelled")
		}
		var one int
		assert.NoError(db.QueryRowContext(t.Context(), "SELECT 1").Scan(&one), "the pool is usable after a cancellation")
		j.Resume()
	}
}

// TestJanitor_StopCancelsABlockedJob pins the stop: a job blocked in its round is cancelled, and Stop returns once it
// has, without waiting out the block.
func TestJanitor_StopCancelsABlockedJob(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	in := make(chan struct{})
	var ended atomic.Bool
	j := testJanitor(t, newRig(t, 30).db, nil, nil, testJob("blocked", func(ctx context.Context, batch batchFunc) bool {
		_ = batch(ctx, func(ctx context.Context, _ handle) error {
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
	}))
	<-in
	start := time.Now()
	j.Stop()
	assert.True(time.Since(start) < 5*time.Second, "Stop took %v", time.Since(start))
	assert.True(ended.Load(), "Stop returned before the job did")
}

// TestJanitor_EachBatchTakesAFreshClaim pins the turn: each batch takes a claim of its own, in the common band, so
// a job's later batch is younger than dispatch that arrived meanwhile rather than older than all of it; and a batch run
// on a context that carries a claim already - a batch's own - fails rather than keep that claim's age or nest a turn.
func TestJanitor_EachBatchTakesAFreshClaim(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	var claims []turnstile.Claim
	var nested error
	j := testJanitor(t, newRig(t, 30).db, nil, func(j *Janitor) { j.SetManual(true); j.shard.SetTurnLimit(4) },
		testJob("two", func(ctx context.Context, batch batchFunc) bool {
			for range 2 {
				err := batch(ctx, func(batchCtx context.Context, _ handle) error {
					c, ok := turnstile.ClaimFrom(batchCtx)
					assert.True(ok)
					claims = append(claims, c)
					nested = batch(batchCtx, func(context.Context, handle) error { return nil })
					return nil
				})
				assert.NoError(err)
				time.Sleep(10 * time.Millisecond)
			}
			return false
		}),
	)
	j.Drain(t.Context())
	assert.Must(assert.Len(claims, 2))
	commonCtx, pass := database.NewShard(1, nil).WaitTurn(t.Context())
	pass.Return()
	common, _ := turnstile.ClaimFrom(commonCtx)
	assert.Equal(common.Priority, claims[0].Priority, "the common band")
	assert.NotEqual(claims[0].Seq, claims[1].Seq, "each batch is a job of its own at the turnstile")
	assert.True(claims[1].Since.After(claims[0].Since), "the second batch's claim is as old as the batch, not the round")
	assert.Error(nested, "a batch inside a batch")
}

// TestJanitor_JobAgesReportTimeSinceProgress pins the reading: zero at the resume, growing through rounds without
// progress, reset by one with; nothing before the first resume.
func TestJanitor_JobAgesReportTimeSinceProgress(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	var progress atomic.Bool
	j := New(database.NewShard(1, nil), nil)
	j.AddJob("job", testJob("job", func(ctx context.Context, _ batchFunc) bool { return progress.Swap(false) }))
	j.SetManual(true)
	assert.Len(j.JobAges(time.Now()), 0, "nothing to report before the first resume")
	j.Start(t.Context())
	j.Resume()
	defer j.Stop()
	assert.True(j.JobAges(time.Now())["job"] < time.Second)

	time.Sleep(100 * time.Millisecond)
	j.Drain(t.Context())
	assert.True(j.JobAges(time.Now())["job"] >= 100*time.Millisecond, "a round without progress resets nothing")
	progress.Store(true)
	j.Drain(t.Context())
	assert.True(j.JobAges(time.Now())["job"] < 50*time.Millisecond, "a round with progress resets it")
}

// TestJanitor_ADrainBesideTheGoroutineNeverOverlapsARound pins that one job's rounds never overlap, so a test's Drain
// on a janitor that runs its jobs is safe.
func TestJanitor_ADrainBesideTheGoroutineNeverOverlapsARound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	var inside, overlaps, rounds atomic.Int64
	j := testJanitor(t, nil, nil, func(j *Janitor) { j.SetIdle(time.Millisecond) },
		testJob("job", func(ctx context.Context, _ batchFunc) bool {
			if inside.Add(1) > 1 {
				overlaps.Add(1)
			}
			time.Sleep(100 * time.Microsecond)
			inside.Add(-1)
			return rounds.Add(1) < 2000
		}),
	)
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
	j := testJanitor(t, db, nil, func(j *Janitor) { j.SetManual(true) },
		testJob("job", func(ctx context.Context, batch batchFunc) bool {
			assert.NoError(batch(ctx, func(ctx context.Context, h handle) error {
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
		}),
	)
	j.Drain(t.Context())
}

// batchParts collects dwarf_janitor_batch_seconds: its samples by part, every one of shard 1 and job "job", and the
// work part's total seconds.
func batchParts(t *testing.T, assert *testarossa.Asserter, reader *sdkmetric.ManualReader) (counts map[string]uint64, work float64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(t.Context(), &rm)))
	counts = map[string]uint64{}
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
				assert.Equal("1", shard.AsString())
				assert.Equal("job", job.AsString())
				counts[part.AsString()] += dp.Count
				if part.AsString() == partWork {
					work = dp.Sum
				}
			}
		}
	}
	return counts, work
}

// TestJanitor_BatchTimings pins dwarf_janitor_batch_seconds: one sample per batch for each part, by shard and job.
func TestJanitor_BatchTimings(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	reader := sdkmetric.NewManualReader()
	d := diagnostics.New()
	d.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	j := testJanitor(t, newRig(t, 30).db, d, func(j *Janitor) { j.SetManual(true) }, testJob("job", func(ctx context.Context, batch batchFunc) bool {
		for range 2 {
			_ = batch(ctx, func(context.Context, handle) error { time.Sleep(10 * time.Millisecond); return nil })
		}
		return false
	}))
	j.Drain(t.Context())
	counts, work := batchParts(t, assert, reader)
	assert.Equal(map[string]uint64{partTurn: 2, partAcquire: 2, partWork: 2}, counts)
	assert.True(work >= 0.02, "work %v", work)
}

// TestJanitor_ABatchThatCannotTakeItsConnectionFails pins the batch's failure to take its connection: the batch
// returns the error without running its work, and records its turn and acquisition but no work.
func TestJanitor_ABatchThatCannotTakeItsConnectionFails(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	reader := sdkmetric.NewManualReader()
	seams := seamster.New(true)
	seams.InjectN(diagnostics.FaultJanitorConnErr, 1)
	ran := false
	var batchErr error
	d := diagnostics.New()
	d.SetSeams(seams)
	d.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	j := testJanitor(t, newRig(t, 30).db, d, func(j *Janitor) { j.SetManual(true) }, testJob("job", func(ctx context.Context, batch batchFunc) bool {
		batchErr = batch(ctx, func(context.Context, handle) error { ran = true; return nil })
		return false
	}))
	j.Drain(t.Context())
	assert.Error(batchErr)
	assert.False(ran, "the work did not run")
	counts, _ := batchParts(t, assert, reader)
	assert.Equal(map[string]uint64{partTurn: 1, partAcquire: 1}, counts)
}
