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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// waitQueued waits until the stripe has n waiters queued.
func waitQueued(t *testing.T, s *cohortStripe, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		got := len(s.queue)
		s.mu.Unlock()
		if got == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("never saw %d waiters queued", n)
}

// TestCohortStripe_FIFOAcrossTicks pins the property the stripe exists for: a waiter that wakes to tick keeps its
// place in line. Waiters queue in a known order, all tick many times while the stripe is held, and they are served
// in arrival order.
func TestCohortStripe_FIFOAcrossTicks(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	var s cohortStripe
	assert.True(s.lock(time.Hour, nil)) // uncontended: no tick, no wait

	const n = 5
	var ticks atomic.Int32
	var order []int
	var orderMu sync.Mutex
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.True(s.lock(2*time.Millisecond, func() bool { ticks.Add(1); return true }))
			orderMu.Lock()
			order = append(order, i)
			orderMu.Unlock()
			s.unlock()
		}()
		waitQueued(t, &s, i+1) // fix the arrival order
	}
	time.Sleep(50 * time.Millisecond) // every waiter ticks many times while held
	assert.True(ticks.Load() >= 2*n, "waiters did not tick while queued (%d ticks)", ticks.Load())
	s.unlock()
	wg.Wait()
	assert.Equal([]int{0, 1, 2, 3, 4}, order, "a ticking waiter lost its place")

	assert.True(s.lock(time.Hour, nil), "the stripe was not free once the queue drained")
	s.unlock()
}

// TestCohortStripe_Abandon pins both abandon paths: a queued waiter whose tick fails leaves the queue without the
// stripe, and the ones behind it are unaffected; a waiter handed the stripe while its tick was failing passes it on.
func TestCohortStripe_Abandon(t *testing.T) {
	t.Parallel()

	t.Run("queued", func(t *testing.T) {
		assert := testarossa.For(t)
		var s cohortStripe
		assert.True(s.lock(time.Hour, nil))
		quitter := make(chan bool, 1)
		go func() { quitter <- s.lock(time.Millisecond, func() bool { return false }) }()
		assert.False(<-quitter, "an abandoning waiter acquired the stripe")
		follower := make(chan bool, 1)
		go func() { follower <- s.lock(time.Hour, nil) }()
		waitQueued(t, &s, 1)
		s.unlock()
		assert.True(<-follower, "the waiter behind an abandoned one was not served")
		s.unlock()
	})

	t.Run("handed_over_mid_tick", func(t *testing.T) {
		assert := testarossa.For(t)
		var s cohortStripe
		assert.True(s.lock(time.Hour, nil))
		inTick := make(chan struct{})
		release := make(chan struct{})
		quitter := make(chan bool, 1)
		go func() {
			quitter <- s.lock(time.Millisecond, func() bool { close(inTick); <-release; return false })
		}()
		<-inTick
		s.unlock() // hands the stripe to the quitter, which is out ticking
		follower := make(chan bool, 1)
		go func() { follower <- s.lock(time.Hour, nil) }()
		waitQueued(t, &s, 1)
		close(release) // the tick fails: the quitter now holds the stripe and must pass it on
		assert.False(<-quitter)
		assert.True(<-follower, "a stripe handed to an abandoning waiter was not passed on")
		s.unlock()
	})
}

// TestCohortStripe_TickPanic pins that a panic in a waiter's tick withdraws the waiter before it propagates. Left
// queued, the waiter's channel would be handed the stripe on the next unlock with nobody to take it, and every later
// arrival would wait forever.
func TestCohortStripe_TickPanic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	var s cohortStripe
	assert.True(s.lock(time.Hour, nil))
	recovered := make(chan any, 1)
	go func() {
		defer func() { recovered <- recover() }()
		s.lock(time.Millisecond, func() bool { panic("tick") })
	}()
	assert.Equal("tick", <-recovered, "the panic did not propagate")
	follower := make(chan bool, 1)
	go func() { follower <- s.lock(time.Hour, nil) }()
	waitQueued(t, &s, 1)
	s.unlock()
	select {
	case got := <-follower:
		assert.True(got)
	case <-time.After(5 * time.Second):
		t.Fatal("the stripe was handed to the panicked waiter")
	}
	s.unlock()
}

// TestCohortStripe_QueuedArrivalsKeepTheirLeases pins that a cohort member queued on its stripe keeps its lease alive,
// so the orphan sweep leaves it to its worker. The test holds the cohort's stripe itself, so both branches complete
// and queue behind it with their steps `completed` and flagged - the stranded shape. Their leases are lapsed, as a
// long queue would lapse them; the waiters' ticks (every half leaseMargin, shortened here) must extend them again
// before the sweep runs, and the sweep must take nothing back. With the extension removed, both are rewound.
func TestCohortStripe_QueuedArrivalsKeepTheirLeases(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Queue")
	g.SetEndpoint("A", "queue/a")
	g.SetEndpoint("B1", "queue/b1")
	g.SetEndpoint("B2", "queue/b2")
	g.SetEndpoint("J", "queue/j")
	g.AddTransitionFanOut("A", "B1", "B2")
	g.AddTransition("B1", "J")
	g.AddTransition("B2", "J")
	g.SetFanIn("J")
	g.AddTransition("J", workflow.End)
	proxy.HandleGraph("queue/g", g)
	release := make(chan struct{})
	var started, runsB, runsJ atomic.Int32
	branch := func(ctx context.Context, f *workflow.Flow) error { started.Add(1); <-release; runsB.Add(1); return nil }
	proxy.HandleTask("queue/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("queue/b1", branch)
	proxy.HandleTask("queue/b2", branch)
	proxy.HandleTask("queue/j", func(ctx context.Context, f *workflow.Flow) error { runsJ.Add(1); return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	e.leaseMargin = 200 * time.Millisecond // waiters tick every 100ms
	assert.NoError(e.Startup(t.Context()))
	awaitStartupRecoverySweep(t, e) // it reads orphanFlowThreshold, which is written below

	fk, err := e.Create(ctx, "queue/g", nil, nil)
	assert.NoError(err)
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return started.Load() == 2 }), "branches never started")
	shard, flowID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(shard)
	assert.NoError(err)
	var spawnID int
	assert.NoError(db.QueryRowContext(ctx, "SELECT step_id FROM dwarf_steps WHERE flow_id=? AND task_name='A'", flowID).Scan(&spawnID))

	stripe := &e.cohortLocks[cohortLockStripe(shard, spawnID)]
	assert.True(stripe.lock(time.Hour, nil))
	close(release)
	queued := "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name IN ('B1', 'B2') AND status='" + workflow.StatusCompleted + "' AND advancing=1"
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool { return enginetest.CountRows(t, e, shard, queued, flowID) == 2 }),
		"the branches never queued on the stripe")
	waitQueued(t, stripe, 2)
	var seqBefore int
	assert.NoError(db.QueryRowContext(ctx, "SELECT SUM(lease_seq) FROM dwarf_steps WHERE flow_id=? AND task_name IN ('B1', 'B2')", flowID).Scan(&seqBefore))

	e.orphanFlowThreshold = 0
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", flowID)
	assert.NoError(err)
	// The waiters' ticks must push both leases back into the future before the sweep looks.
	assert.True(enginetest.WaitUntil(t, 10*time.Second, func() bool {
		return enginetest.CountRows(t, e, shard, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND task_name IN ('B1', 'B2') AND lease_expires > NOW_UTC()", flowID) == 2
	}), "the queued waiters never extended their leases")
	e.recoverOrphanedFlows(ctx, db, shard)
	var seqAfter int
	assert.NoError(db.QueryRowContext(ctx, "SELECT SUM(lease_seq) FROM dwarf_steps WHERE flow_id=? AND task_name IN ('B1', 'B2')", flowID).Scan(&seqAfter))
	assert.Equal(seqBefore, seqAfter, "the sweep took back a waiter that was keeping its lease")
	assert.Equal(2, enginetest.CountRows(t, e, shard, queued, flowID))

	stripe.unlock()
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
	assert.Equal(int32(2), runsB.Load())
	assert.Equal(int32(1), runsJ.Load())
	enginetest.AssertInvariants(t, e)
}
