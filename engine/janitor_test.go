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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/internal/reaper"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// runningJanitorEngine starts an engine whose janitors run their jobs in the background, waiting idle a short while,
// archiving closed roots at once, with the reaper kept out of the way.
func runningJanitorEngine(t *testing.T, mux *hosts.Mux) *Engine {
	t.Helper()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(mux)
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	e.janitorManual = false
	e.janitorIdle = 10 * time.Millisecond
	if err := e.SetRetention(30); err != nil {
		t.Fatal(err)
	}
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	enginetest.AwaitVisitCount(t, e.diags.Seams(), 1, 10*time.Second, diagnostics.CheckpointArchivePassDone)
	return e
}

// setRole makes this replica dispatch shard, or stop, as the reconcile loop does.
func setRole(e *Engine, shard int, dispatches bool) {
	e.poolsLock.Lock()
	defer e.poolsLock.Unlock()
	e.applyRole(shard, dispatches)
}

// live reports whether flow is in the live tables.
func live(t *testing.T, e *Engine, flow int) bool {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	var n int
	if err := db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", flow).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n == 1
}

// awaitGone waits until flow has left the live tables.
func awaitGone(t *testing.T, e *Engine, flow int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for live(t, e, flow) {
		if time.Now().After(deadline) {
			t.Fatalf("flow %d is still live", flow)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestJanitor_FollowsTheDispatchRole pins the janitor's activity: it archives on a shard this replica dispatches,
// nothing once it stops dispatching it, and again once it dispatches it again.
func TestJanitor_FollowsTheDispatchRole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := runningJanitorEngine(t, treeMux())
	awaitGone(t, e, closedRoots(t, e, 1)[0])

	setRole(e, 1, false)
	time.Sleep(100 * time.Millisecond) // a round already under way finishes
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	root := seedTrees(t, db, 1, callShape)[0] // seeded closed: this replica no longer runs flows on the shard
	time.Sleep(500 * time.Millisecond)
	assert.True(live(t, e, root), "a paused janitor archives nothing")

	setRole(e, 1, true)
	awaitGone(t, e, root)
}

// TestJanitor_APauseOrShutdownCancelsABatchInFlight pins cancellation of a batch held inside its work - as SQL
// Server's unbounded lock wait would hold it - on a pause, after which a resume runs the jobs again, and on Shutdown,
// which does not wait it out.
func TestJanitor_APauseOrShutdownCancelsABatchInFlight(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	assert.NoError(e.SetHost(noopHost{}))
	e.janitorManual = false
	e.janitorIdle = 10 * time.Millisecond
	e.diags.Seams().Break(diagnostics.CheckpointJanitorBatchConn)
	defer e.diags.Seams().Resume(diagnostics.CheckpointJanitorBatchConn)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.True(e.diags.Seams().WaitTimeout(t.Context(), diagnostics.CheckpointJanitorBatchConn, 10*time.Second*enginetest.TimeoutScale()))

	setRole(e, 1, false)
	held := e.diags.Seams().Visits(diagnostics.CheckpointJanitorBatchConn)
	start := time.Now()
	setRole(e, 1, true) // waits for the paused jobs to be gone
	assert.True(time.Since(start) < 10*time.Second*enginetest.TimeoutScale(), "resumed after %v", time.Since(start))
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for e.diags.Seams().Visits(diagnostics.CheckpointJanitorBatchConn) <= held && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.True(e.diags.Seams().Visits(diagnostics.CheckpointJanitorBatchConn) > held, "the resumed jobs run again")

	start = time.Now()
	assert.NoError(e.Shutdown(context.Background()))
	assert.True(time.Since(start) < 10*time.Second*enginetest.TimeoutScale(), "Shutdown took %v", time.Since(start))
}

// TestJanitor_ShutdownCancelsTheJobsBeforeTheWorkersDrain pins the drain order: batches holding connections - as a
// batch with no deadline may, here held at a breakpoint while the test holds the rest of the pool - are cancelled
// before Shutdown waits on the workers, so a worker in flight that needs a connection to record its step gets one and
// the drain ends. Cancelled after the workers' drain, the batches would hold the connections for as long as the drain
// waited on the worker, which is forever.
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
	e.janitorManual = false
	// A connection for every job held at the breakpoint, and more for the worker's claim: held, the jobs would take
	// every connection of the test-mode cap, and the task would never start.
	e.testConnCap = 2 * defaultTestConnCap
	e.diags.Seams().Break(diagnostics.CheckpointJanitorBatchConn)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	jobs := len(e.janitorOn(1).JobAges(time.Now()))
	assert.Must(assert.True(jobs > 0, "the janitor has jobs"))
	assert.Must(assert.True(db.Stats().MaxOpenConnections > jobs, "a pool of %d holds the %d jobs and the worker",
		db.Stats().MaxOpenConnections, jobs))

	_, err = e.Create(t.Context(), "janitor/graph", nil, nil)
	assert.Must(assert.NoError(err))
	<-entered
	// Every job holds its batch's connection at the breakpoint; the test takes the rest of the pool.
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for e.diags.Seams().Visits(diagnostics.CheckpointJanitorBatchConn) < jobs && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Must(assert.True(e.diags.Seams().Visits(diagnostics.CheckpointJanitorBatchConn) >= jobs, "every job holds a connection"))
	var conns []*sql.Conn
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	// Bounded, so a count that does not match the jobs holding connections fails rather than waits for good.
	taking, cancel := context.WithTimeout(t.Context(), 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	for len(conns) < db.Stats().MaxOpenConnections-jobs {
		c, err := db.DB.Conn(taking)
		assert.Must(assert.NoError(err))
		conns = append(conns, c)
	}
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
		e.diags.Seams().Resume(diagnostics.CheckpointJanitorBatchConn)
		for _, c := range conns {
			c.Close()
		}
		conns = nil
		<-done
		t.Fatal("Shutdown waited on a worker the jobs held a connection from")
	}
}

// TestJanitor_ReapDueFlowsDrainsTheJobs pins the deterministic test mode, test-mode engines' default: a Manual janitor
// runs no job by itself, and reapDueFlows runs its jobs until a round makes no progress.
func TestJanitor_ReapDueFlowsDrainsTheJobs(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := archiveEngine(t, treeMux(), 30)
	root := closedRoots(t, e, 1)[0]
	time.Sleep(100 * time.Millisecond)
	assert.True(live(t, e, root), "a Manual janitor runs no job by itself")
	e.drainJanitors(t.Context(), e.archiveGrace)
	assert.False(live(t, e, root), "drained, the janitor archives it")
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
	assert.Must(assert.NoError(e.Startup(t.Context())))

	ages := func() map[string]float64 {
		out := map[string]float64{}
		for _, sm := range collect(t, reader).ScopeMetrics {
			for _, m := range sm.Metrics {
				g, ok := m.Data.(metricdata.Gauge[float64])
				if m.Name != "dwarf_janitor_progress_age_seconds" || !ok {
					continue
				}
				for _, dp := range g.DataPoints {
					shard, _ := dp.Attributes.Value("shard")
					job, _ := dp.Attributes.Value("job")
					if shard.AsString() == "1" {
						out[job.AsString()] = dp.Value
					}
				}
			}
		}
		return out
	}
	time.Sleep(100 * time.Millisecond)
	got := ages()
	assert.Len(got, 4, "fast archiving, sweep archiving, large-tree archiving and fast deletion are reported")
	assert.True(got[janitor.JobFastArchiving] >= 0.1 && got[janitor.JobFastArchiving] < 10, "age %v", got)

	setRole(e, 1, false)
	assert.Len(ages(), 0, "a shard no longer dispatched is not reported")
}

// TestJanitor_SharesTheReapersFaultNames pins the reaper's own copies of its seam names to the catalogue's, which the
// janitor consults: a test arming a name through internal/diagnostics then arms the reaper's site as well, and the
// janitor's beside it while both run.
func TestJanitor_SharesTheReapersFaultNames(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.Equal(reaper.FaultMidTree, diagnostics.FaultReapMidTree)
	assert.Equal(reaper.FaultSizeErr, diagnostics.FaultReapSizeErr)
	assert.Equal(reaper.FaultSelectErr, diagnostics.FaultReapSelectErr)
	assert.Equal(reaper.FaultContention, diagnostics.FaultReapContention)
	assert.Equal(reaper.FaultArchiveErr, diagnostics.FaultReapArchiveErr)
	assert.Equal(reaper.FaultArchiveDelete, diagnostics.FaultArchiveDelete)
	assert.Equal(reaper.FaultStageChunk, diagnostics.FaultReapStageChunk)
	assert.Equal(reaper.FaultStageLost, diagnostics.FaultReapStageLost)
	assert.Equal(reaper.CheckpointBatchLocked, diagnostics.CheckpointReapBatchLocked)
	assert.Equal(reaper.CheckpointBeforeRootDelete, diagnostics.CheckpointReapBeforeRootDelete)
	assert.Equal(reaper.CheckpointChunkDone, diagnostics.CheckpointReapChunkDone)
	assert.Equal(reaper.CheckpointStageChunkDone, diagnostics.CheckpointReapStageChunkDone)
	assert.Equal(reaper.CheckpointArchiveDeleted, diagnostics.CheckpointReapArchiveDeleted)
	assert.Equal(reaper.ReasonNoRetention, janitor.ReasonNoRetention)
}

// TestJanitor_TheGraceOverrideReachesTheJobs pins the grace a test's ArchiveNow passes: the janitors' rounds under it
// archive a root closed just now, which the archive grace keeps them from otherwise; and the archive grace is theirs
// again after it.
func TestJanitor_TheGraceOverrideReachesTheJobs(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	e.archiveGrace = time.Hour
	roots := closedRoots(t, e, 2)
	e.drainJanitors(ctx, e.archiveGrace)
	assert.True(live(t, e, roots[0]), "within the archive grace")
	e.drainJanitors(ctx, 0)
	assert.False(live(t, e, roots[0]), "archived by the janitor under a grace of 0")
	closedRoots(t, e, 1)
	e.janitorOn(1).Drain(ctx)
	assert.Len(closedLive(t, e), 1, "the archive grace is the janitor's again")
}

// closedLive returns the closed roots in the live tables.
func closedLive(t *testing.T, e *Engine) []int {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	rows, err := db.QueryContext(t.Context(), "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id=0 AND status IN ('"+
		workflow.StatusCompleted+"','"+workflow.StatusFailed+"','"+workflow.StatusTerminated+"','"+workflow.StatusCancelled+"')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestJanitor_TheSweepStartsWhatFastArchivingLeftBehind pins the handoff of a large tree fast archiving did not start -
// its root held when fast archiving came to it, or closed behind fast archiving's position: once it is overdue by the
// sweep's window, sweep archiving starts its archive attempt, and large-tree archiving copies and publishes it.
func TestJanitor_TheSweepStartsWhatFastArchivingLeftBehind(t *testing.T) {
	t.Parallel()
	for _, how := range []string{"held", "behind"} {
		t.Run(how, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			e := archiveEngine(t, treeMux(), 30)
			e.reapStageRows = 1 // every tree of more than one row is staged
			sh, err := e.db.Shard(1)
			assert.Must(assert.NoError(err))
			db := sh.DB()
			if d := db.DriverName(); how == "held" && d != "pgx" && d != "mysql" && d != "mariadb" {
				t.Skip("a held root is skipped, not waited for, on PostgreSQL, MySQL and MariaDB")
			}
			attempted := func(root int) bool {
				cond, args := reaper.AttemptedSQL(db.DriverName())
				var n int
				assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=? AND "+cond,
					append([]any{root}, args...)...).Scan(&n)))
				return n == 1
			}
			today, err := e.archiveOn(1).Today(ctx)
			assert.Must(assert.NoError(err))
			archived := func(root int) int {
				var n int
				assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+migrations.FlowsTable(today)+
					" WHERE root_flow_id=?", root).Scan(&n)))
				return n
			}
			roots := closedRoots(t, e, 2)
			root, other := roots[0], roots[1]
			switch how {
			case "held":
				locked, release, finished := make(chan error, 1), make(chan struct{}), make(chan struct{})
				go func() {
					defer close(finished)
					_ = db.Transact(ctx, func(tx *sequel.Tx) error {
						rows, err := tx.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE flow_id=? FOR UPDATE", root)
						if err == nil {
							rows.Close()
						}
						locked <- err
						<-release
						return err
					})
				}()
				assert.Must(assert.NoError(<-locked))
				e.drainJanitors(ctx, 0)
				close(release)
				<-finished
			case "behind":
				backdate(t, e, root, -time.Hour) // closed in the future: not due when fast archiving passes it
				e.drainJanitors(ctx, 0)
				backdate(t, e, root, 10*time.Second) // due now, closed before fast archiving's position, within the window
			}
			e.drainJanitors(ctx, 0)
			if now, _ := e.archiveOn(1).Today(ctx); now != today {
				t.Skip("midnight passed during the test")
			}
			assert.False(live(t, e, other), "the other tree archived")
			assert.False(attempted(root), "neither fast archiving nor, within its window, the sweep started an attempt")
			assert.True(live(t, e, root))

			backdate(t, e, root, janitor.DefaultSweepOverdue+time.Minute) // overdue by the sweep's window
			e.drainJanitors(ctx, 0)
			assert.Equal(2, archived(root), "the sweep started its attempt, and large-tree archiving copied and published it")
			// Its stamp is behind fast deletion's position, which passed the other tree's higher flow_id: a sweep's.
			e.reapPass(ctx, 0, false)
			assert.False(live(t, e, root), "and its live copy is removed")
		})
	}
}
