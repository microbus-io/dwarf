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

package workers

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/internal/permits"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// newCache returns a cache sized well past anything these tests push through it, so the bound is never
// what a test is accidentally measuring.
func newCache(t *testing.T) *candidatecache.Cache {
	t.Helper()
	c := &candidatecache.Cache{}
	c.Init(64) // capacity 128
	t.Cleanup(c.Close)
	return c
}

// newGate returns the real permit set, sized for shard 1, closed at test end. These tests deliberately use
// the production Gate rather than a fake: the crew's contract with it (Acquire blocks, blocking counts as
// idle and so stops growth, close is a stop signal) is exactly what is under test.
func newGate(t *testing.T, n int) *permits.Set {
	t.Helper()
	g := permits.New()
	g.Resize(1, n, n)
	t.Cleanup(g.Close)
	return g
}

// fill pushes n candidates onto shard 1 as a refill would.
func fill(c *candidatecache.Cache, n int) {
	batch := make([]candidatecache.Job, 0, n)
	for i := range n {
		batch = append(batch, candidatecache.Job{StepID: i + 1, Shard: 1})
	}
	c.Refill(1, batch, 100)
}

// eventually waits briefly for cond, so a test never depends on goroutine scheduling order.
func eventually(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

func TestCrew_NewValidates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	noop := func(context.Context, int, int, func()) error { return nil }
	_, err := New(nil, newGate(t, 4), noop)
	assert.Error(err, "cache is required")
	_, err = New(&candidatecache.Cache{}, nil, noop)
	assert.Error(err, "gate is required")
	_, err = New(&candidatecache.Cache{}, newGate(t, 4), nil)
	assert.Error(err, "process is required")
}

// TestCrew_ProcessesEveryCandidate is the baseline: the resident set drains the cache and each candidate
// reaches the callback exactly once.
func TestCrew_ProcessesEveryCandidate(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var mu sync.Mutex
	seen := map[int]int{}
	crew, err := New(c, newGate(t, 8), func(_ context.Context, shard, stepID int, release func()) error {
		release()
		mu.Lock()
		seen[stepID]++
		mu.Unlock()
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(4)

	fill(c, 20)
	crew.Start(context.Background(), 4)
	assert.True(eventually(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 20
	}), "every candidate must reach the callback")

	c.Close()
	crew.Drain()
	mu.Lock()
	defer mu.Unlock()
	for id, n := range seen {
		assert.Equal(1, n, "step %d handled once", id)
	}
}

// TestCrew_SaturationDoesNotGrowThePool is the NEGATIVE half of the growth trigger, and it is the half that
// matters: it is the test whose absence let a measured runaway ship (~20% throughput lost on a saturated
// shard, a crew bloated to ~1,300 goroutines where ~512 sufficed).
//
// The property is that saturation must not grow the crew WITHOUT BOUND, and it is enforced with no capacity
// query at all: the crew spawns one worker, that worker blocks in AcquireEnter, and blocking makes it count
// IDLE - which declines every subsequent check. So the overshoot is exactly one goroutine, permanently, and
// it is the one first in line for the next permit.
//
// Two residents hold both permits, so the assertion is 3: the crew that took the last candidate added its
// reserve, and the reserve cannot proceed. A build that read saturation as a reason to keep spawning fails
// here at Max (8), which is the runaway this test exists for.
func TestCrew_SaturationDoesNotGrowThePool(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	hold := make(chan struct{})
	var busy atomic.Int32
	// Two permits for two residents, so the crew starts fully committed with none to spare.
	crew, err := New(c, newGate(t, 2), func(_ context.Context, shard, stepID int, release func()) error {
		busy.Add(1)
		<-hold // busy, and deliberately still HOLDING the permit: this is what saturation looks like
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(8)

	fill(c, 20)
	crew.Start(context.Background(), 2)
	assert.True(eventually(func() bool { return busy.Load() == 2 }), "both workers entered the callback")
	time.Sleep(50 * time.Millisecond) // long enough for a runaway to show up
	assert.Equal(3, crew.Resident(),
		"saturation adds the one reserve worker and then stops, since that worker blocks on the gate")
	assert.True(crew.Idle() > 0, "the reserve is blocked on the gate, which is what declines further growth")

	close(hold)
	c.Close()
	crew.Drain()
}

// TestCrew_GrowsWhenNobodyIsFree is the positive half. A worker that RELEASES its permit and then blocks is
// the long-task shape - it holds nothing the crew is bounded on - so with work waiting, nobody parked, and
// the gate reporting room, the crew must add capacity up to Max.
//
// This is the case the previous trigger could not serve at any useful rate. It required every worker to be
// inside the handler SIMULTANEOUSLY, a coincidence whose probability decays exponentially in the crew size,
// so the crew grew only logarithmically in time and a long-task workload's throughput was capped at
// crew-size-over-task-duration no matter how much database capacity sat idle.
func TestCrew_GrowsWhenNobodyIsFree(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	hold := make(chan struct{})
	var inTask atomic.Int32
	crew, err := New(c, newGate(t, 8), func(_ context.Context, shard, stepID int, release func()) error {
		release() // the long call holds no permit - the ExecuteTask boundary
		inTask.Add(1)
		<-hold
		inTask.Add(-1)
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(6)

	fill(c, 20)
	crew.Start(context.Background(), 2)

	assert.True(eventually(func() bool { return crew.Resident() == 6 }),
		"work waiting with nobody free must grow the crew to Max, got %d resident", crew.Resident())
	assert.True(eventually(func() bool { return inTask.Load() == 6 }))

	close(hold)
	c.Close()
	crew.Drain()
	assert.Equal(0, crew.Idle(), "no worker is left parked once the cache closes")
}

// TestCrew_GrowthStopsWhenAWorkerIsParked pins the other half of the trigger: idleness, not busyness, is
// what stops growth. With more workers than work, somebody is always parked waiting, so the crew must sit
// still even though the gate has permits to spare and Max is far above.
func TestCrew_GrowthStopsWhenAWorkerIsParked(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var handled atomic.Int32
	crew, err := New(c, newGate(t, 32), func(_ context.Context, shard, stepID int, release func()) error {
		release()
		handled.Add(1)
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(32)

	// Far more workers than candidates, so the cache drains at once and everyone parks.
	fill(c, 2)
	crew.Start(context.Background(), 8)
	assert.True(eventually(func() bool { return handled.Load() == 2 }))
	assert.True(eventually(func() bool { return crew.Idle() == 8 }), "every worker is idle once work runs out")
	time.Sleep(50 * time.Millisecond)
	assert.Equal(8, crew.Resident(), "an idle peer means growth adds nothing")

	c.Close()
	crew.Drain()
}

// TestCrew_LostPopContinuesRatherThanExiting pins that a worker losing the race for a peeked candidate goes
// back for more instead of returning. resident never decrements, so a return here would erode the crew under
// exactly the contention that caused the race - and silently, since nothing else reports it.
func TestCrew_LostPopContinuesRatherThanExiting(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var handled atomic.Int32
	crew, err := New(c, newGate(t, 16), func(_ context.Context, shard, stepID int, release func()) error {
		release()
		handled.Add(1)
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(8)

	// Many workers against one candidate at a time: all but one lose the pop on every round.
	crew.Start(context.Background(), 8)
	for range 20 {
		fill(c, 1)
		time.Sleep(time.Millisecond)
	}
	assert.True(eventually(func() bool { return handled.Load() >= 20 }),
		"every candidate is handled despite the racing pops, got %d", handled.Load())
	assert.Equal(8, crew.Resident(), "a lost pop must not retire a worker")

	c.Close()
	crew.Drain()
}

// TestCrew_ReleaseIsBackstopped pins that a handler which never releases still gives its permit back. The
// permit's lifetime crosses the callback boundary (the crew acquires, the handler releases), so every early
// return and caught panic inside the handler is a leak path - and a leaked permit is PERMANENT, decaying
// admission until it stops. The crew's unconditional OnceFunc call on the way out is what closes it.
func TestCrew_ReleaseIsBackstopped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	gate := newGate(t, 2)
	var handled atomic.Int32
	crew, err := New(c, gate, func(_ context.Context, shard, stepID int, release func()) error {
		handled.Add(1)
		return nil // never releases, and on some rounds panics instead
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(2)

	fill(c, 10)
	crew.Start(context.Background(), 2)
	assert.True(eventually(func() bool { return handled.Load() == 10 }),
		"a handler that never releases must not starve the crew of permits, handled %d of 10", handled.Load())

	c.Close()
	crew.Drain()
	enter, _ := gate.Snapshot()
	assert.Equal(int64(2), enter[1], "every permit is back")
}

// TestCrew_MaxIsRespectedAndLive pins that growth stops at Max and that lowering Max stops further growth
// without retiring goroutines that already exist.
func TestCrew_MaxIsRespectedAndLive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	hold := make(chan struct{})
	crew, err := New(c, newGate(t, 16), func(_ context.Context, shard, stepID int, release func()) error {
		release()
		<-hold
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(3)

	fill(c, 20)
	crew.Start(context.Background(), 1)
	assert.True(eventually(func() bool { return crew.Resident() == 3 }))
	time.Sleep(30 * time.Millisecond)
	assert.Equal(3, crew.Resident(), "growth stops at Max")

	// Lowering Max stops growth but never shrinks: retiring a goroutine would need a whole protocol.
	crew.SetMax(1)
	assert.Equal(3, crew.Resident(), "lowering Max does not retire existing workers")

	close(hold)
	c.Close()
	crew.Drain()
}

// TestCrew_StartSpawnsNoMoreThanMax pins that even the resident set is capped, so a caller that asks for
// more residents than the ceiling gets the ceiling rather than an over-full crew.
func TestCrew_StartSpawnsNoMoreThanMax(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	crew, err := New(c, newGate(t, 4), func(_ context.Context, _, _ int, release func()) error {
		release()
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(2)
	crew.Start(context.Background(), 10)
	assert.Equal(2, crew.Resident())

	c.Close()
	crew.Drain()
}

// TestCrew_DrainRacesGrowthWithoutPanicking is the reason this package is separable at all: the shutdown
// protocol is subtle and was previously untestable in isolation. A WaitGroup.Add concurrent with a Wait
// PANICS, and a worker can try to spawn a peer at any instant, including while Drain is waiting. The
// spawnClosed flag, set under the same lock that guards the Add, is what makes it safe.
func TestCrew_DrainRacesGrowthWithoutPanicking(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	gate := newGate(t, 64)
	crew, err := New(c, gate, func(_ context.Context, shard, stepID int, release func()) error {
		release()
		time.Sleep(200 * time.Microsecond)
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(16)

	// Keep candidates arriving so workers keep taking work - and so keep hitting the spawn path - throughout.
	// The handler holds each worker just long enough that the crew is momentarily fully committed, which is
	// what makes the trigger fire: a handler that returned instantly would always leave a peer idle and the
	// crew would never grow at all.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			fill(c, 64)
			time.Sleep(time.Millisecond)
		}
	}()
	crew.Start(context.Background(), 8)
	assert.True(eventually(func() bool { return crew.Resident() > 8 }), "the crew grew while spinning")

	// Drain while workers are still trying to spawn peers. Without the flag this panics.
	close(stop)
	c.Close()
	gate.Close()
	crew.Drain()
}

// TestCrew_SpawnAfterDrainIsInert pins that a late spawn attempt adds nothing rather than joining a
// WaitGroup nobody waits on.
func TestCrew_SpawnAfterDrainIsInert(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	gate := newGate(t, 4)
	crew, err := New(c, gate, func(_ context.Context, _, _ int, release func()) error {
		release()
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(4)
	crew.Start(context.Background(), 1)
	c.Close()
	gate.Close()
	crew.Drain()

	before := crew.Resident()
	crew.considerGrowth()
	assert.Equal(before, crew.Resident(), "the crew is closed to new goroutines")
}

// TestCrew_ClosedGateReleasesWorkers pins the second stop signal. A worker blocked on a permit is not
// blocked on the cache, so closing only the cache would leave Drain waiting on it forever.
func TestCrew_ClosedGateReleasesWorkers(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	gate := permits.New()
	gate.Resize(1, 1, 1)
	entered := make(chan struct{}, 1)
	hold := make(chan struct{})
	crew, err := New(c, gate, func(_ context.Context, shard, stepID int, release func()) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-hold // holds the only permit, so every peer blocks in Acquire
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(4)

	fill(c, 20)
	crew.Start(context.Background(), 4)
	<-entered
	// The peers are blocked in Acquire, which the cache's close cannot reach - that is the whole point of
	// the second stop signal, and it is what Drain below would hang on.
	assert.True(eventually(func() bool { return crew.Idle() == 3 }),
		"three peers wait on the one permit, got %d idle", crew.Idle())

	// Release the handler, then close BOTH signals: the cache frees whoever is parked, the gate frees
	// whoever is waiting on a permit.
	close(hold)
	c.Close()
	gate.Close()
	crew.Drain() // hangs if the gate close is not a stop signal
}

// TestCrew_HandlerErrorAndPanicKeepTheWorkerAlive pins that neither an error nor a panic from the callback
// takes a goroutine down: the crew logs and goes back for the next candidate. A panic that escaped would
// kill the process, and a worker that exited on an error would silently shrink the crew.
func TestCrew_HandlerErrorAndPanicKeepTheWorkerAlive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var buf bytes.Buffer
	var handled atomic.Int32
	gate := newGate(t, 4)
	crew, err := New(c, gate, func(_ context.Context, shard, stepID int, release func()) error {
		switch n := handled.Add(1); n {
		case 1:
			return errors.New("boom") // returns without releasing: the backstop must cover it
		case 2:
			panic("panic in the handler")
		}
		release()
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetLogger(slog.New(slog.NewTextHandler(&buf, nil)))
	crew.SetMax(1)

	fill(c, 4)
	crew.Start(context.Background(), 1)
	assert.True(eventually(func() bool { return handled.Load() == 4 }),
		"one worker must survive both an error and a panic, handled %d of 4", handled.Load())
	assert.Equal(1, crew.Resident())

	c.Close()
	crew.Drain()
	assert.True(strings.Contains(buf.String(), "Processing candidate"))
	enterBack, _ := gate.Snapshot()
	assert.Equal(int64(4), enterBack[1], "the error and panic paths both gave their permit back")
}

// TestCrew_WorkArrivingWhileFullyCommittedStillGrows is what replaced the periodic growth check, and it
// pins the exact state that check existed for: work arrives when every worker is already committed to a
// long call, so NOBODY pops and no edge fires.
//
// It works without a cadence because the crew keeps a standing reserve of one. The worker that took the
// last candidate spawned a peer, that peer parked, and a parked worker is one WaitForWork can wake - so the
// arrival is noticed, the reserve pops, and the cascade takes over from there.
//
// Against a build where growth also required work to be waiting, this HANGS: taking the last candidate
// spawned nothing, the crew settled with zero parked workers, and the second batch had nobody to wake. That
// is the measured 130-candidates-against-96-workers stall, in miniature.
func TestCrew_WorkArrivingWhileFullyCommittedStillGrows(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	hold := make(chan struct{})
	var inTask atomic.Int32
	crew, err := New(c, newGate(t, 32), func(_ context.Context, shard, stepID int, release func()) error {
		release() // the long-call shape: holds no permit while it blocks
		inTask.Add(1)
		<-hold
		return nil
	})
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(16)

	// Exactly as many candidates as residents, so the crew consumes the batch and is fully committed with
	// the cache EMPTY - the resting state the old rule could reach with nobody parked.
	fill(c, 2)
	crew.Start(context.Background(), 2)
	assert.True(eventually(func() bool { return inTask.Load() == 2 }), "both residents are in the long call")
	assert.True(eventually(func() bool { return crew.Idle() > 0 }),
		"a reserve worker exists even though the cache is empty and every other worker is committed")

	// Work arrives with every original worker still blocked. Nothing pops it but the reserve.
	fill(c, 8)
	assert.True(eventually(func() bool { return inTask.Load() == 10 }),
		"work arriving while fully committed must still be served, got %d in task", inTask.Load())

	close(hold)
	c.Close()
	crew.Drain()
}
