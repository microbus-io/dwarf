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
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// reaperKinds are the kinds the reaper's gauges are reported for.
var reaperKinds = []string{"closed", "deleted", "retention"}

// holdARoot leaves the reaper holding one closed root on shard 1: its archive move fails, so repair counts it held and
// its sweep stays open, and the pass reads an archive lag.
func holdARoot(t *testing.T, e *Engine, db *sequel.DB) {
	t.Helper()
	root := seedTrees(t, db, 1, callShape)[0]
	closeTreesAgo(t, db, 2*time.Minute, root)
	e.reapSlice = 0
	e.reapBatch = 1
	e.seams.InjectN(engineseams.FaultReapArchiveErr, 10000)
	defer e.seams.Withdraw(engineseams.FaultReapArchiveErr)
	e.reapDueFlows(t.Context())
}

// assertNoReaperReadings asserts that the reaper's gauges on shard 1 report no reading of a previous run: every kind's
// series at zero, and no archive lag.
func assertNoReaperReadings(t *testing.T, rm metricdata.ResourceMetrics) {
	t.Helper()
	assert := testarossa.For(t)
	for _, kind := range reaperKinds {
		attrs := map[string]string{"shard": "1", "kind": kind}
		for _, name := range []string{"dwarf_archive_held_roots", "dwarf_archive_oldest_held_seconds", "dwarf_archive_sweep_seconds"} {
			v, ok := gaugeAt(rm, name, attrs)
			assert.True(ok, "%s{kind=%s} is reported", name, kind)
			assert.Zero(v, "%s{kind=%s}", name, kind)
		}
	}
	failing, _ := gaugeAt(rm, "dwarf_archive_failing_seconds", map[string]string{"shard": "1"})
	assert.Zero(failing)
	_, ok := gaugeAt(rm, "dwarf_archive_lag_seconds", map[string]string{"shard": "1"})
	assert.False(ok, "no lag until a pass scans")
}

// assertHeldReadings asserts that holdARoot's readings are reported, so a test that then expects them gone starts from
// readings that were there.
func assertHeldReadings(t *testing.T, rm metricdata.ResourceMetrics) {
	t.Helper()
	assert := testarossa.For(t)
	held, _ := gaugeAt(rm, "dwarf_archive_held_roots", map[string]string{"shard": "1", "kind": "closed"})
	assert.Equal(float64(1), held)
	_, ok := gaugeAt(rm, "dwarf_archive_lag_seconds", map[string]string{"shard": "1"})
	assert.True(ok, "a lag was read")
}

// TestReaperContract_ARestartForgetsTheWalksAndTheLag pins that the reaper's readings live as long as the engine's run:
// after a held root, a Shutdown and a Startup report no held root, no sweep in progress and no archive lag.
func TestReaperContract_ARestartForgetsTheWalksAndTheLag(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, hosts.NewMux(), 30)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	holdARoot(t, e, db)
	assertHeldReadings(t, collect(t, reader))

	assert.NoError(e.Shutdown(ctx))
	assert.Must(assert.NoError(e.Startup(ctx)))
	assertNoReaperReadings(t, collect(t, reader))
}

// TestReaperContract_AFailedStartupLeavesNoReadings pins that a Startup that fails, and the one that then succeeds,
// report none of the previous run's readings.
func TestReaperContract_AFailedStartupLeavesNoReadings(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, hosts.NewMux(), 30)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	holdARoot(t, e, db)
	assertHeldReadings(t, collect(t, reader))

	assert.NoError(e.Shutdown(ctx))
	e.seams.InjectN(engineseams.FaultInitRuntime, 1)
	assert.Error(e.Startup(ctx))
	assert.Must(assert.NoError(e.Startup(ctx)))
	assertNoReaperReadings(t, collect(t, reader))
}

// TestReaperContract_EveryKindIsReportedFromTheStart pins that a dispatching engine reports the reaper's gauges for all
// three kinds, at zero, before its first pass.
func TestReaperContract_EveryKindIsReportedFromTheStart(t *testing.T) {
	t.Parallel()
	_, reader := meteredArchiveEngine(t, hosts.NewMux(), 30)
	assertNoReaperReadings(t, collect(t, reader))
}

// TestReaperContract_RemovalsRunOnTheCallersContext pins that Purge and Delete of archived trees keep to the caller's
// context: neither the reaper's attempt deadline nor its faults apply to them. A Purge across two transactions and a
// Delete succeed with the attempt at a nanosecond and every sizing fault armed.
func TestReaperContract_RemovalsRunOnTheCallersContext(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, hosts.NewMux(), 30)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, db, 4, callShape)
	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}
	var token string
	var hour int
	assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT flow_token, created_hour FROM "+migrations.FlowsTable(today)+
		" WHERE flow_id=?", roots[3]).Scan(&token, &hour)))

	e.reapAttempt = time.Nanosecond
	e.purgeChunk = 2
	e.seams.InjectN(engineseams.FaultReapSizeErr, 10000)
	defer e.seams.Withdraw(engineseams.FaultReapSizeErr)
	for _, root := range roots {
		fault := engineseams.Join(engineseams.FaultReapSizeErr, strconv.Itoa(root))
		e.seams.InjectN(fault, 10000)
		defer e.seams.Withdraw(fault)
	}
	assert.NoError(e.Delete(ctx, keys.NewFlowKey(1, hour, roots[3], token)))
	removed, _, err := e.Purge(ctx, enginetest.Recent(workflow.Query{Limit: 10}))
	assert.NoError(err)
	assert.Equal(3, removed, "the other three, two to a transaction")
	for _, root := range roots {
		flows, steps := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), root)
		assert.Zero(flows+steps, "tree %d", root)
	}
}

// TestReaperContract_PurgeTakesTheReapersRowBudget pins Purge's transactions to the reaper's row budget: trees of two
// rows each, at a budget of four, go two to a transaction, and at a budget of two, one.
func TestReaperContract_PurgeTakesTheReapersRowBudget(t *testing.T) {
	t.Parallel()
	for _, c := range []struct{ rows, transactions int }{{4, 2}, {2, 4}} {
		t.Run(strconv.Itoa(c.rows), func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			log := &queryLog{}
			e := NewEngineUnderTest(t.Name())
			t.Cleanup(func() { e.Shutdown(context.Background()) })
			e.SetHost(hosts.NewMux())
			assert.NoError(e.SetLogger(slog.New(log)))
			e.archiveGrace = 0
			e.reapInterval = time.Hour
			assert.NoError(e.SetRetention(30))
			assert.Must(assert.NoError(e.Startup(ctx)))
			db, err := e.db.Shard(1)
			assert.Must(assert.NoError(err))
			today, err := e.archives.Load().byShard[1].Today(ctx)
			assert.NoError(err)
			seedTrees(t, db, 4, callShape)
			e.reapDueFlows(ctx)
			if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
				t.Skip("midnight passed during the pass")
			}

			e.reapBatchRows = c.rows
			stmt := "DELETE FROM " + migrations.FlowsTable(today) + " "
			before := log.count(stmt)
			removed, _, err := e.Purge(ctx, enginetest.Recent(workflow.Query{Limit: 10}))
			assert.NoError(err)
			assert.Equal(4, removed)
			assert.Equal(c.transactions, log.count(stmt)-before, "one flows delete per transaction")
		})
	}
}

// count returns how many logged statements start with prefix.
func (l *queryLog) count(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, q := range l.queries {
		if strings.HasPrefix(q, prefix) {
			n++
		}
	}
	return n
}

// TestReaperContract_APartialPurgeCountsWhatItRemoved pins a Purge that fails part-way: it reports the roots its
// committed transactions removed, and dwarf_flows_removed counts exactly those. The second transaction's deletion is
// faulted.
func TestReaperContract_APartialPurgeCountsWhatItRemoved(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader := meteredArchiveEngine(t, hosts.NewMux(), 30)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, db, 4, callShape)
	e.reapDueFlows(ctx)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the pass")
	}

	e.reapBatchRows = 4 // two trees to a transaction: roots[2] is in the second
	fault := engineseams.Join(engineseams.FaultArchiveDelete, strconv.Itoa(roots[2]))
	e.seams.InjectN(fault, 1000)
	defer e.seams.Withdraw(fault)
	removed, done, err := e.Purge(ctx, enginetest.Recent(workflow.Query{Limit: 10}))
	assert.Error(err)
	assert.False(done)
	assert.Equal(2, removed, "the first transaction's roots")
	assert.Equal(int64(2), sumAt(collect(t, reader), "dwarf_flows_removed", map[string]string{"shard": "1", "op": "purge"}))
}

// TestReaperContract_ANonDispatcherTakesNoTurn pins that a pass on a shard this replica does not dispatch takes no
// turn there: with no turn to be had, the pass returns at once, while a dispatching replica's pass waits for one.
func TestReaperContract_ANonDispatcherTakesNoTurn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, hosts.NewMux(), 30)
	// A turnstile sized to nothing admits nobody, a pass returned by a holder included.
	ts := e.turnstiles.Turnstile(1)
	assert.Must(assert.NotNil(ts))
	size := ts.Size()
	e.turnstiles.Resize(1, 0)
	defer e.turnstiles.Resize(1, size)

	e.dispatchFlag(1).Store(false)
	start := time.Now()
	e.reapDueFlows(ctx)
	assert.True(time.Since(start) < time.Second, "a non-dispatcher's pass took %v", time.Since(start))

	e.dispatchFlag(1).Store(true)
	wait := 300 * time.Millisecond
	bounded, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	start = time.Now()
	e.reapDueFlows(bounded)
	assert.True(time.Since(start) >= wait, "a dispatcher's pass waits for a turn: %v", time.Since(start))
}

// TestReaperContract_ThePassTimeExcludesExpiration pins dwarf_reap_pass_seconds to the reap alone: an expiration stage
// held for half a second after it is not counted in it.
func TestReaperContract_ThePassTimeExcludesExpiration(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader, _ := expiryEngine(t, time.Hour)
	expiring := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, expiring, 2*time.Hour)

	e.seams.Break(engineseams.CheckpointTerminateAfterScan)
	done := make(chan struct{})
	go func() {
		defer close(done)
		expireNow(ctx, e)
	}()
	assert.Must(assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointTerminateAfterScan, 10*time.Second*enginetest.TimeoutScale())))
	const held = 500 * time.Millisecond
	time.Sleep(held)
	e.seams.Resume(engineseams.CheckpointTerminateAfterScan)
	<-done
	status, _ := statusOf(t, e, expiring)
	assert.Equal(workflow.StatusTerminated, status, "the expiration ran in the pass")

	shard1 := map[string]string{"shard": "1"}
	rm := collect(t, reader)
	n, ok := histCount(rm, "dwarf_reap_pass_seconds", shard1)
	assert.True(ok && n == 1, "one pass: %d", n)
	sum, _ := histSum(rm, "dwarf_reap_pass_seconds", shard1)
	assert.True(sum < held.Seconds(), "the pass took %vs, the expiration was held %v", sum, held)
}
