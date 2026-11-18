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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestRecoveryJobs_Defaults pins the cadence the crash-recovery bound is published against - lease recovery and
// orphan repair every minute, the wedge sweep every five, matching its detectors' 5-minute age guards - and that a
// dispatched shard's janitor holds the three jobs.
func TestRecoveryJobs_Defaults(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.Equal(time.Minute, e.recoveryInterval)
	assert.Equal(5*time.Minute, wedgeSweepEvery*e.recoveryInterval)
	assert.Equal(e.parkWedgeThreshold, wedgeSweepEvery*e.recoveryInterval)

	assert.NoError(e.SetHost(noopHost{}))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	ages := e.janitorOn(1).JobAges(time.Now())
	for _, job := range []string{janitor.JobLeaseRecovery, janitor.JobOrphanRepair, janitor.JobWedgeSweep} {
		_, ok := ages[job]
		assert.True(ok, "the janitor holds %s", job)
	}
}

// heldTask returns a host whose task at taskURL holds its first run until release is closed and returns at once on
// every later one, entered closed when the first run began, and the count of its runs.
func heldTask(graphURL, taskURL string) (mux *hosts.Mux, entered, release chan struct{}, runs *atomic.Int32) {
	mux = hosts.NewMux()
	g := workflow.NewGraph("One")
	g.SetURL("A", taskURL)
	g.AddTransition("A", workflow.End)
	mux.HandleGraph(graphURL, g)
	entered, release = make(chan struct{}), make(chan struct{})
	runs = &atomic.Int32{}
	mux.HandleTask(taskURL, func(ctx context.Context, f *workflow.Flow) error {
		if runs.Add(1) == 1 {
			close(entered)
			<-release // the crashed worker's task, never to write back
		}
		return nil
	})
	return mux, entered, release, runs
}

// expireRunningLease creates a flow at graphURL on e, waits for its held task to be entered, and lapses the running
// step's lease, as a dead worker's lapses. It returns the flow's key.
func expireRunningLease(t *testing.T, e *Engine, graphURL string, entered chan struct{}) (flowKey string) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()
	fk, err := e.Create(ctx, graphURL, nil, nil)
	assert.Must(assert.NoError(err))
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.Must(assert.NoError(err))
	select {
	case <-entered:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("A was never dispatched")
	}
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	_, err = sh.DB().ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", flowID)
	assert.NoError(err)
	return fk
}

// TestRecoveryJobs_ADrainRecoversLeases pins that lease recovery is a job the test mode's drain runs: a step whose
// lease lapsed is reset by the drain, and runs again on a live worker.
func TestRecoveryJobs_ADrainRecoversLeases(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux, entered, release, runs := heldTask("recoverydrain/g", "recoverydrain/a")

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.setClaimsTrackerEnabled(false)
	defer close(release) // before Shutdown, which waits for the held worker
	assert.NoError(e.SetHost(mux))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	fk := expireRunningLease(t, e, "recoverydrain/g", entered)

	e.reapDueFlows(ctx)
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int32(2), runs.Load(), "the recovered step runs again on a live worker")
}

// TestRecoveryJobs_ExpiredLeaseRecoveredInTheBackground pins the job end to end: a claimed step whose lease lapses is
// re-dispatched by the background lease recovery, with no call from the test. Lease recovery is the only repair that
// resets an expired running lease, so the step completing at all shows the job ran; its pace, a pass an interval after
// the last one ended, is pinned against an injected clock by internal/janitor's TestRecovery_ThePace. So the wait is
// the scaled bound of the other tests here, not a wall-clock bound on the pace, which a loaded machine misses.
func TestRecoveryJobs_ExpiredLeaseRecoveredInTheBackground(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	mux, entered, release, runs := heldTask("recoverybg/g", "recoverybg/a")

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer close(release)
	e.janitorManual = false
	e.recoveryInterval = 2 * time.Second
	assert.NoError(e.SetHost(mux))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	fk := expireRunningLease(t, e, "recoverybg/g", entered)

	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int32(2), runs.Load())
}

// TestRecoveryJobs_AHeldOrphanRepairDelaysNoLeaseReset pins why lease recovery and orphan repair are jobs apart: with
// orphan repair's pass held, as a slow scan of every open flow would hold it, lease recovery still resets an expired
// lease in the background.
func TestRecoveryJobs_AHeldOrphanRepairDelaysNoLeaseReset(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	mux, entered, release, runs := heldTask("recoveryapart/g", "recoveryapart/a")

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.setClaimsTrackerEnabled(false)
	defer close(release)
	e.janitorManual = false
	e.recoveryInterval = 500 * time.Millisecond
	orphanPass := diagnostics.Join(diagnostics.CheckpointRecoveryPassDone, janitor.JobOrphanRepair, strconv.Itoa(1))
	e.diags.Seams().Break(orphanPass)
	defer e.diags.Seams().Resume(orphanPass)
	assert.NoError(e.SetHost(mux))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	assert.Must(assert.True(e.diags.Seams().WaitTimeout(ctx, orphanPass, 10*time.Second*enginetest.TimeoutScale()),
		"orphan repair never ran its pass"))
	fk := expireRunningLease(t, e, "recoveryapart/g", entered)

	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int32(2), runs.Load())
	assert.Equal(1, e.diags.Seams().Visits(orphanPass), "orphan repair stayed held throughout")
}
