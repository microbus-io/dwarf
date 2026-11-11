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
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/reaper"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// awaitJanitor waits until shard has a janitor, or has none, and returns it.
func awaitJanitor(t *testing.T, e *Engine, shard int, want bool) *janitor.Janitor {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		j := e.janitorOn(shard)
		if (j != nil) == want {
			return j
		}
		if time.Now().After(deadline) {
			t.Fatalf("shard %d: janitor %v, want %v", shard, j != nil, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// blockingSQL is a statement that runs until it is cancelled.
func blockingSQL(driver string) string {
	switch driver {
	case "pgx":
		return "SELECT pg_sleep(600)"
	case "mysql":
		return "SELECT SLEEP(600)"
	case "mssql":
		return "WAITFOR DELAY '00:10:00'"
	default:
		return "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<1000000000000) SELECT MAX(x) FROM c"
	}
}

// TestJanitor_FollowsTheDispatchRole pins the supervision: a shard this replica dispatches has a janitor, one it stops
// dispatching has none - its jobs cancelled and joined - and one it dispatches again has a new janitor. Shutdown
// stops the rest.
func TestJanitor_FollowsTheDispatchRole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	var stopped atomic.Int64
	entered := make(chan struct{}, 2)
	e.janitorManual = false // the jobs run in the background, as in production
	e.janitorTestJobs = func(shard int, db *sequel.DB) []janitor.Job {
		return []janitor.Job{{Name: "test", Round: func(ctx context.Context, _ janitor.Batch) bool {
			entered <- struct{}{}
			<-ctx.Done()
			stopped.Add(1)
			return false
		}}}
	}
	assert.Must(assert.NoError(e.Startup(t.Context())))

	// Each janitor's job is waited into its round before it is stopped: one stopped before its first round never runs.
	first := awaitJanitor(t, e, 1, true)
	<-entered
	e.dispatchFlag(1).Store(false)
	awaitJanitor(t, e, 1, false)
	deadline := time.Now().Add(10 * time.Second)
	for stopped.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(int64(1), stopped.Load(), "the janitor of a shard no longer dispatched is stopped and joined")
	e.dispatchFlag(1).Store(true)
	second := awaitJanitor(t, e, 1, true)
	assert.True(first != second, "a shard dispatched again gets a new janitor")
	<-entered

	assert.NoError(e.Shutdown(t.Context()))
	assert.Equal(int64(2), stopped.Load(), "Shutdown stops and joins the janitors")
	assert.Nil(e.janitorOn(1))
}

// TestJanitor_AJobBlockedInAStatementIsCancelled pins cancellation of a job inside a statement that would not end on
// its own - SQL Server's unbounded lock wait is the production case - on a handoff and on Shutdown, and that the pool
// is usable after it.
func TestJanitor_AJobBlockedInAStatementIsCancelled(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	in := make(chan struct{}, 1)
	out := make(chan error, 1)
	e.janitorIdle = 10 * time.Millisecond
	e.janitorManual = false // the jobs run in the background, as in production
	e.janitorTestJobs = func(shard int, db *sequel.DB) []janitor.Job {
		return []janitor.Job{{Name: "test", Round: func(ctx context.Context, batch janitor.Batch) bool {
			err := batch(ctx, func(ctx context.Context, h janitor.Handle) error {
				in <- struct{}{}
				_, err := h.ExecContext(ctx, blockingSQL(h.DriverName()))
				return err
			})
			out <- err
			<-ctx.Done()
			return false
		}}}
	}
	assert.Must(assert.NoError(e.Startup(t.Context())))
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))

	// The handoff.
	<-in
	time.Sleep(200 * time.Millisecond) // into the statement
	start := time.Now()
	e.dispatchFlag(1).Store(false)
	select {
	case err := <-out:
		assert.Error(err, "the statement was cancelled")
		assert.True(time.Since(start) < 5*time.Second, "cancelled after %v", time.Since(start))
	case <-time.After(30 * time.Second):
		t.Fatal("a handoff did not cancel the job's statement")
	}
	var one int
	assert.NoError(db.QueryRowContext(t.Context(), "SELECT 1").Scan(&one), "the pool is usable after a cancellation")

	// Shutdown.
	e.dispatchFlag(1).Store(true)
	<-in
	time.Sleep(200 * time.Millisecond)
	start = time.Now()
	done := make(chan struct{})
	go func() {
		_ = e.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
		assert.Error(<-out, "the statement was cancelled")
		assert.True(time.Since(start) < 10*time.Second, "Shutdown took %v", time.Since(start))
	case <-time.After(60 * time.Second):
		t.Fatal("Shutdown did not cancel the job's statement")
	}
}

// TestJanitor_ShutdownCancelsTheJobsBeforeTheWorkersDrain pins the drain order: a job holding every connection of the
// pool - as a batch with no deadline may - is cancelled before Shutdown waits on the workers, so a worker in flight
// that needs a connection to record its step gets one and the drain ends. Cancelled after the workers' drain, the job
// would hold the connections for as long as the drain waited on the worker, which is forever.
func TestJanitor_ShutdownCancelsTheJobsBeforeTheWorkersDrain(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())

	entered := make(chan struct{})
	release := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("Hold")
	g.SetURL("Hold", "janitor/hold")
	g.AddTransition("Hold", workflow.End)
	mux.HandleGraph("janitor/graph", g)
	mux.HandleTask("janitor/hold", func(ctx context.Context, f *workflow.Flow) error {
		close(entered)
		<-release
		return f.Set("held", true)
	})
	assert.NoError(e.SetHost(mux))

	var grab atomic.Bool
	held := make(chan int, 1)
	giveUp := make(chan struct{})
	e.janitorIdle = 10 * time.Millisecond
	e.janitorManual = false // the jobs run in the background, as in production
	e.janitorTestJobs = func(shard int, db *sequel.DB) []janitor.Job {
		return []janitor.Job{{Name: "test", Round: func(ctx context.Context, _ janitor.Batch) bool {
			if !grab.CompareAndSwap(true, false) {
				return false
			}
			var conns []*sql.Conn
			for range db.Stats().MaxOpenConnections {
				c, err := db.DB.Conn(ctx)
				if err != nil {
					break
				}
				conns = append(conns, c)
			}
			held <- len(conns)
			select {
			case <-ctx.Done():
			case <-giveUp:
			}
			for _, c := range conns {
				_ = c.Close()
			}
			return false
		}}}
	}
	assert.Must(assert.NoError(e.Startup(t.Context())))
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))

	_, err = e.Create(t.Context(), "janitor/graph", nil, nil)
	assert.Must(assert.NoError(err))
	<-entered
	grab.Store(true)
	assert.Equal(db.Stats().MaxOpenConnections, <-held, "the job holds every connection")
	close(release)
	time.Sleep(200 * time.Millisecond) // the worker is waiting for a connection

	done := make(chan struct{})
	go func() {
		_ = e.Shutdown(context.Background())
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		close(giveUp)
		<-done
		t.Fatal("Shutdown waited on a worker the jobs held a connection from")
	}
}

// TestJanitor_ReapDueFlowsDrainsTheJobs pins the deterministic test mode, test-mode engines' default: a Manual janitor
// runs no goroutine, and reapDueFlows runs its jobs until a round makes no progress. Each batch takes a turn at dispatch's band, on a claim
// of its own.
func TestJanitor_ReapDueFlowsDrainsTheJobs(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	var rounds atomic.Int64
	var claims []turnstile.Claim
	e.janitorTestJobs = func(shard int, db *sequel.DB) []janitor.Job {
		return []janitor.Job{{Name: "test", Round: func(ctx context.Context, batch janitor.Batch) bool {
			n := rounds.Add(1)
			if n == 1 {
				for range 2 {
					assert.NoError(batch(ctx, func(ctx context.Context, h janitor.Handle) error {
						c, _ := turnstile.ClaimFrom(ctx)
						claims = append(claims, c)
						_, err := h.ExecContext(ctx, "SELECT 1")
						return err
					}))
				}
			}
			return n <= 3
		}}}
	}
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitJanitor(t, e, 1, true)
	time.Sleep(100 * time.Millisecond)
	assert.Equal(int64(0), rounds.Load(), "a Manual janitor runs no job by itself")

	e.reapDueFlows(t.Context())
	assert.Equal(int64(5), rounds.Load(), "three rounds of progress, then one without, then a pass without")
	assert.Must(assert.Len(claims, 2))
	assert.Equal(priorityCommon, claims[0].Priority)
	assert.NotEqual(claims[0].Seq, claims[1].Seq, "each batch is a job of its own at the turnstile")
}

// TestJanitor_TheProgressGauge pins dwarf_janitor_progress_age_seconds: one reading per shard and job while the
// replica dispatches the shard, none once it stops.
func TestJanitor_TheProgressGauge(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	reader := sdkmetric.NewManualReader()
	assert.NoError(e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	e.janitorTestJobs = func(shard int, db *sequel.DB) []janitor.Job {
		return []janitor.Job{{Name: "test", Round: func(context.Context, janitor.Batch) bool { return false }}}
	}
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitJanitor(t, e, 1, true)

	age := func() (float64, bool) {
		for _, sm := range collect(t, reader).ScopeMetrics {
			for _, m := range sm.Metrics {
				g, ok := m.Data.(metricdata.Gauge[float64])
				if m.Name != "dwarf_janitor_progress_age_seconds" || !ok {
					continue
				}
				for _, dp := range g.DataPoints {
					shard, _ := dp.Attributes.Value("shard")
					job, _ := dp.Attributes.Value("job")
					if shard.AsString() == "1" && job.AsString() == "test" {
						return dp.Value, true
					}
				}
			}
		}
		return 0, false
	}
	time.Sleep(100 * time.Millisecond)
	v, ok := age()
	assert.True(ok, "a dispatched shard's job is reported")
	assert.True(v >= 0.1 && v < 10, "age %v", v)

	e.dispatchFlag(1).Store(false)
	awaitJanitor(t, e, 1, false)
	_, ok = age()
	assert.False(ok, "a shard no longer dispatched is not reported")
}

// TestJanitor_SharesTheReapersFaultNames pins that the janitor consults the reaper's fault and checkpoint names, so a
// test arming one through engineseams arms both while both run.
func TestJanitor_SharesTheReapersFaultNames(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.Equal(reaper.FaultMidTree, janitor.FaultMidTree)
	assert.Equal(reaper.FaultSizeErr, janitor.FaultSizeErr)
	assert.Equal(reaper.FaultSelectErr, janitor.FaultSelectErr)
	assert.Equal(reaper.FaultContention, janitor.FaultContention)
	assert.Equal(reaper.FaultArchiveErr, janitor.FaultArchiveErr)
	assert.Equal(reaper.CheckpointBatchLocked, janitor.CheckpointBatchLocked)
	assert.Equal(reaper.ReasonNoRetention, janitor.ReasonNoRetention)
}

// TestJanitor_TheGraceOverrideReachesTheJobs pins the grace a test's ArchiveNow passes: the janitors' rounds under it
// archive a root closed just now, which the archive grace keeps them from otherwise.
func TestJanitor_TheGraceOverrideReachesTheJobs(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	e.archiveGrace = time.Hour
	root := closedRoots(t, e, 1)[0]
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	live := func() int {
		var n int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", root).Scan(&n))
		return n
	}
	e.drainJanitors(ctx)
	assert.Equal(1, live(), "within the archive grace")
	e.drainJanitors(context.WithValue(ctx, janitorGraceKey{}, time.Duration(0)))
	assert.Zero(live(), "archived by the janitor under a grace of 0")
}
