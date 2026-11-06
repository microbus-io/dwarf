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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// recoveredCount reads dwarf_steps_recovered from the reader the engine's meter provider writes to.
func recoveredCount(t *testing.T, reader *sdkmetric.ManualReader) int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	n, _ := sumCounter(rm, "dwarf_steps_recovered", "", "")
	return n
}

// TestLeaseRecovery_StepCompletedBetweenSelectAndResetIsLeftAlone pins that lease recovery's by-id reset repeats
// the expired predicate. A step it selected can complete before the reset runs; without the predicate the reset
// turns that completed step back to pending, so the flow advances twice and the task runs again.
func TestLeaseRecovery_StepCompletedBetweenSelectAndResetIsLeftAlone(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls, bCalls atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("LeaseGap")
	g.SetURL("A", "leasegap/a")
	g.SetURL("B", "leasegap/b")
	g.AddTransition("A", "B")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph("leasegap/g", g)
	mux.HandleTask("leasegap/a", func(ctx context.Context, f *workflow.Flow) error {
		aCalls.Add(1)
		started <- struct{}{}
		<-release
		return nil
	})
	mux.HandleTask("leasegap/b", func(ctx context.Context, f *workflow.Flow) error { bCalls.Add(1); return nil })
	reader := sdkmetric.NewManualReader()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	e.SetHost(mux)
	assert.NoError(e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitStartupRecoverySweep(t, e) // the Break below must catch only the sweep this test drives

	fk, err := e.Create(ctx, "leasegap/g", nil, nil)
	assert.NoError(err)
	select {
	case <-started:
	case <-time.After(15 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("A never started")
	}
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	var aStepID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStepID))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE step_id=?", aStepID)
	assert.NoError(err)

	// Hold the sweep after it has selected A, then let A complete and the flow finish in the gap.
	e.seams.Break(engineseams.CheckpointLeaseRecoverySelected)
	swept := make(chan struct{})
	go func() {
		defer close(swept)
		e.recoverExpiredLeases(ctx)
	}()
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointLeaseRecoverySelected, 15*time.Second), "the sweep never selected A")
	unblock()
	out, err := enginetest.BoundedAwait(t, e, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	e.seams.Resume(engineseams.CheckpointLeaseRecoverySelected)
	<-swept

	var aStatus string
	assert.NoError(db.QueryRowContext(ctx, "SELECT status FROM dwarf_steps WHERE step_id=?", aStepID).Scan(&aStatus))
	assert.Equal(workflow.StatusCompleted, aStatus, "the reset rewound a step that completed after the sweep selected it")
	assert.Equal(int64(0), recoveredCount(t, reader))
	assert.Equal(int32(1), aCalls.Load())
	assert.Equal(int32(1), bCalls.Load())
}

// TestLeaseRecovery_RecoversAStepNeitherWriteCouldRecord pins the path the persist classifier leaves to lease
// recovery: the outcome write fails, and so does the clean failure write, as when the database is unreachable.
// The step must stay running under its lease, recording nothing, until the lease lapses and the sweep resets it;
// then the task runs again and the flow completes.
func TestLeaseRecovery_RecoversAStepNeitherWriteCouldRecord(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var aCalls atomic.Int32
	mux := hosts.NewMux()
	g := workflow.NewGraph("LeaseUnwritable")
	g.SetURL("A", "leaseunwritable/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("leaseunwritable/g", g)
	mux.HandleTask("leaseunwritable/a", func(ctx context.Context, f *workflow.Flow) error { aCalls.Add(1); return nil })
	reader := sdkmetric.NewManualReader()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	assert.NoError(e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitStartupRecoverySweep(t, e)
	e.seams.InjectN(engineseams.Join(engineseams.FaultPersistErr, "A"), math.MaxInt)
	e.seams.Inject(engineseams.Join(engineseams.FaultFailStepErr, "A"))

	fk, err := e.Create(ctx, "leaseunwritable/g", nil, nil)
	assert.NoError(err)
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	var aStepID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&aStepID))
	enginetest.AwaitVisitCount(t, e.seams, 1, 15*time.Second,
		engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(aStepID)))

	held := enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE step_id=? AND status='"+
		workflow.StatusRunning+"' AND lease_expires>NOW_UTC()", aStepID)
	assert.Equal(1, held, "with neither write landing, the step must stay running under its lease")
	assert.Equal(int32(1), aCalls.Load())

	// The database is back: let the lease lapse and sweep.
	e.seams.Withdraw(engineseams.Join(engineseams.FaultPersistErr, "A"))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE step_id=?", aStepID)
	assert.NoError(err)
	e.recoverExpiredLeases(ctx)
	assert.Equal(int64(1), recoveredCount(t, reader))

	out, err := enginetest.BoundedAwait(t, e, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int32(2), aCalls.Load(), "from the database's view the first run never happened, so the task runs again")
}

// TestLeaseRecovery_ResetsPastOneBatch pins that lease recovery resets every expired step when they span more
// than one reset batch, and counts each one.
func TestLeaseRecovery_ResetsPastOneBatch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const flows = 5
	started := make(chan struct{}, 2*flows)
	release := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("LeaseBatch")
	g.SetURL("A", "leasebatch/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("leasebatch/g", g)
	mux.HandleTask("leasebatch/a", func(ctx context.Context, f *workflow.Flow) error {
		started <- struct{}{}
		<-release
		return nil
	})
	reader := sdkmetric.NewManualReader()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	e.SetHost(mux)
	e.leaseRecoveryBatch = 2 // three batches for five steps
	assert.NoError(e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitStartupRecoverySweep(t, e)

	var fks []string
	for range flows {
		fk, err := e.Create(ctx, "leasebatch/g", nil, nil)
		assert.NoError(err)
		fks = append(fks, fk)
	}
	for range flows {
		select {
		case <-started:
		case <-time.After(15 * time.Second * enginetest.TimeoutScale()):
			t.Fatal("a step never started")
		}
	}
	shard, _, _, _, err := keys.ParseFlowKey(fks[0])
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	res, err := db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE status='"+workflow.StatusRunning+"'")
	assert.NoError(err)
	if n, _ := res.RowsAffected(); !assert.Equal(int64(flows), n) {
		return
	}

	e.recoverExpiredLeases(ctx)
	assert.Equal(int64(flows), recoveredCount(t, reader), "every expired step is reset and counted, across batches")

	unblock()
	for _, fk := range fks {
		out, err := enginetest.BoundedAwait(t, e, fk)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, out.Status)
		}
	}
}
