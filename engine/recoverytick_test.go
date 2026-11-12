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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestRecoveryTick_Defaults pins the cadence the crash-recovery bound is published against: lease recovery and the
// orphan-flow repair every minute, the park detectors every five, matching their 5-minute age guards.
func TestRecoveryTick_Defaults(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngine()
	assert.Equal(time.Minute, e.recoveryInterval)
	assert.Equal(5*time.Minute, heavyRepairEvery*e.recoveryInterval)
	assert.Equal(e.parkWedgeThreshold, heavyRepairEvery*e.recoveryInterval)
}

// TestRecoveryTick_LightPassRecoversLeases pins that a pass without the park detectors still resets expired leases.
// The light pass is what runs on four ticks of five, so without this a crashed worker's step would wait for the
// heavy cadence again.
func TestRecoveryTick_LightPassRecoversLeases(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("One")
	g.SetURL("A", "ticklight/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("ticklight/g", g)
	entered := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	mux.HandleTask("ticklight/a", func(ctx context.Context, f *workflow.Flow) error {
		if runs.Add(1) == 1 {
			close(entered)
			<-release // the crashed worker's task, never to write back
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer close(release) // before Shutdown, which waits for the held worker
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	awaitStartupRecoverySweep(t, e) // the next background pass is a minute away

	fk, err := e.Create(ctx, "ticklight/g", nil, nil)
	assert.NoError(err)
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	select {
	case <-entered:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("A was never dispatched")
	}
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", flowID)
	assert.NoError(err)

	e.runRecoverySweep(ctx, false)
	assert.Equal(0, enginetest.CountRows(t, e, shard,
		"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusRunning+"' AND lease_expires<=NOW_UTC()", flowID),
		"a light pass left the expired lease in place")

	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int32(2), runs.Load(), "the recovered step runs again on a live worker")
}

// TestRecoveryTick_ExpiredLeaseRecoveredOnTheTick pins the loop end to end: a claimed step whose lease lapses is
// re-dispatched on the next recovery tick, with no call from the test. The tick is shortened to 2s, and the wait
// for recovery is 6s: long enough for one tick under load, too short for five, so a loop that recovered leases
// only on the heavy cadence fails here.
func TestRecoveryTick_ExpiredLeaseRecoveredOnTheTick(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("One")
	g.SetURL("A", "tickloop/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("tickloop/g", g)
	entered := make(chan struct{})
	release := make(chan struct{})
	var runs atomic.Int32
	mux.HandleTask("tickloop/a", func(ctx context.Context, f *workflow.Flow) error {
		if runs.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer close(release)
	e.recoveryInterval = 2 * time.Second
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk, err := e.Create(ctx, "tickloop/g", nil, nil)
	assert.NoError(err)
	shard, _, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	select {
	case <-entered:
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("A was never dispatched")
	}
	sh, err := e.db.Shard(shard)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", flowID)
	assert.NoError(err)

	awaitCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int32(2), runs.Load())
}
