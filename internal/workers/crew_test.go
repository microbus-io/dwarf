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

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// newCache returns a cache sized well past anything these tests push through it, so the bound is never
// what a test is accidentally measuring.
func newCache(t *testing.T) *candidates.Cache {
	t.Helper()
	c := &candidates.Cache{}
	c.Init(64) // capacity 128
	t.Cleanup(c.Close)
	return c
}

// newGate returns a real gate over a turnstile sized for shard 1, closed at test end. These tests
// deliberately use the production Gate rather than a fake: the crew's contract with it (Acquire blocks,
// blocking counts as idle and so stops growth, close is a stop signal) is exactly what is under test.
func newGate(t *testing.T, n int) *turnstile.Gate {
	t.Helper()
	s := turnstile.NewSet()
	s.Resize(1, n)
	t.Cleanup(s.Close)
	return s.Gate(1)
}

// newGateSet is newGate when the test needs to close the set itself, mid-run.
func newGateSet(t *testing.T, n int) (*turnstile.Set, *turnstile.Gate) {
	t.Helper()
	s := turnstile.NewSet()
	s.Resize(1, n)
	t.Cleanup(s.Close)
	return s, s.Gate(1)
}

// fill pushes n candidates onto shard 1 as a refill would.
func fill(c *candidates.Cache, n int) {
	batch := make([]candidates.Job, 0, n)
	for i := range n {
		batch = append(batch, candidates.Job{StepID: i + 1, Shard: 1})
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
	_, err = New(&candidates.Cache{}, nil, noop)
	assert.Error(err, "gate is required")
	_, err = New(&candidates.Cache{}, newGate(t, 4), nil)
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
// query at all: the crew spawns one worker, that worker blocks in Acquire, and blocking makes it count
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

	set, gate := newGateSet(t, 2)
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
	avail, _ := set.Snapshot()
	assert.Equal(2, avail[1], "every permit is back")
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

	// Lowering Max stops growth; it does not force a shrink. A crew above a lowered Max comes down only as
	// its own workers measure themselves surplus - here they are all inside the handler, so none does.
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

	set, gate := newGateSet(t, 64)
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
	set.Close()
	crew.Drain()
}

// TestCrew_SpawnAfterDrainIsInert pins that a late spawn attempt adds nothing rather than joining a
// WaitGroup nobody waits on.
func TestCrew_SpawnAfterDrainIsInert(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	set, gate := newGateSet(t, 4)
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
	set.Close()
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

	set, gate := newGateSet(t, 1)
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
	set.Close()
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
	set, gate := newGateSet(t, 4)
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
	enterBack, _ := set.Snapshot()
	assert.Equal(4, enterBack[1], "the error and panic paths both gave their permit back")
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

// feed keeps shard 1 supplied until the returned stop is called. It is not scaffolding for its own sake:
// retirement is evaluated when a worker comes round the top of its loop, so a surplus worker decides only
// because SOMETHING wakes it - in the engine, a piston's cycle refilling the partition. A test that let the
// cache go quiet would be measuring a crew that never gets asked, which is the one state the rule cannot
// cover and does not claim to.
//
// ONE candidate per refill is the setting the shrink tests want. Refill broadcasts, so every worker wakes
// and comes round to its check, but only one can pop - so the rest reach the check having taken nothing,
// and no pop ever leaves idle at zero. That is what keeps GROWTH out of a measurement about shrinking.
func feed(c *candidates.Cache, n int) (stop func()) {
	done, stopped := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(stopped)
		for {
			select {
			case <-done:
				return
			default:
			}
			fill(c, n)
			time.Sleep(time.Millisecond)
		}
	}()
	// stop WAITS for the goroutine, so a caller that has stopped the supply can rely on no further candidate
	// arriving. round below depends on exactly that.
	return sync.OnceFunc(func() { close(done); <-stopped })
}

// fakeClock is the crew's injected time source under test. Advancing it is what turns retirement from
// something a test waits out into something it drives: with the clock still, a worker's window can only
// elapse when the test says so, so each Advance past the window is exactly ONE verdict per worker (the
// verdict resets that worker's window to the same still instant, so the next pass reads zero elapsed).
//
// It is an atomic because every worker reads it and the test writes it.
type fakeClock struct{ nanos atomic.Int64 }

func newFakeClock() *fakeClock {
	f := &fakeClock{}
	f.nanos.Store(time.Now().UnixNano())
	return f
}

func (f *fakeClock) Now() time.Time          { return time.Unix(0, f.nanos.Load()) }
func (f *fakeClock) Advance(d time.Duration) { f.nanos.Add(int64(d)) }

// retireOn wires a crew to a still clock with a 1-minute window, and returns it. Nothing retires until a
// test advances past the window, so growth phases are unperturbed by the rule under test.
func retireOn(crew *Crew) *fakeClock {
	clk := newFakeClock()
	crew.now = clk.Now
	crew.retireWindow = time.Minute
	return clk
}

// round drives exactly ONE retirement verdict per worker, and returns the replacement supply stop.
//
// The quiesce is not tidiness, it is the correctness of the round. A worker caught inside the busy bracket
// when the clock jumps banks the WHOLE jump as busy time, reads a fraction of 1, and silently skips the
// verdict this round was meant to give it - which shows up much later as a count short by one. So: stop the
// supply (feed's stop waits, so nothing more can arrive), let the crew drain and park, and only then move
// time. Supply resumes afterwards because a parked worker reaches its check only when something wakes it.
func round(t *testing.T, crew *Crew, c *candidates.Cache, clk *fakeClock, stop func()) func() {
	t.Helper()
	stop()
	settled := 0
	if !eventually(func() bool {
		if crew.Idle() != crew.Resident() {
			settled = 0
			return false
		}
		// Held across consecutive samples, so a worker merely between the cache and the bracket cannot read
		// as parked.
		settled++
		return settled >= 3
	}) {
		t.Fatalf("crew never quiesced: %d idle of %d resident", crew.Idle(), crew.Resident())
	}
	clk.Advance(2 * time.Minute)
	return feed(c, 1)
}

// settles waits for the crew to stop changing size and returns where it came to REST.
//
// Every assertion about a shrink must be made on the resting size, never on a value the crew merely passes
// through. A build with a broken floor descends past the right answer on its way to zero, and a build that
// never resets a worker's window lets every pass be a fresh verdict - both visit the correct count in
// transit, so an assertion that only has to observe it once passes against either.
func settles(crew *Crew) int {
	last, stable := -1, 0
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n := crew.Resident(); n != last {
			last, stable = n, 0
		} else if stable++; stable >= 25 {
			return n
		}
		time.Sleep(2 * time.Millisecond)
	}
	return crew.Resident()
}

// blocker is a process func that parks in the handler while armed, so a test can hold the whole crew inside
// the busy region and advance the clock underneath it. Disarmed, the handler returns at once and accrues
// exactly zero busy time against a still clock - which is what a surplus worker looks like, exactly.
type blocker struct {
	ch     atomic.Pointer[chan struct{}]
	inTask atomic.Int32
	peak   atomic.Int32
}

func (b *blocker) arm() { ch := make(chan struct{}); b.ch.Store(&ch) }
func (b *blocker) release() {
	if ch := b.ch.Swap(nil); ch != nil {
		close(*ch)
	}
}

func (b *blocker) process(_ context.Context, _, _ int, release func()) error {
	release()
	ch := b.ch.Load()
	if ch == nil {
		return nil
	}
	n := b.inTask.Add(1)
	for {
		p := b.peak.Load()
		if n <= p || b.peak.CompareAndSwap(p, n) {
			break
		}
	}
	<-*ch
	b.inTask.Add(-1)
	return nil
}

// TestCrew_SurplusRetiresAndTheCrewGrowsAgain is the whole retirement cycle, and its third phase is the one
// that matters most. resident gates spawning, so a crew that retired goroutines without decrementing it
// would believe it still held them and WOULD NEVER GROW AGAIN - silently, and only once load returned. Max
// is deliberately set to what phase 1 grows to, so that bug is what phase 3 measures: without the decrement
// the crew sits pinned at Min with resident reading Max, and the peak never leaves 2.
//
// Growth is asserted on the peak count inside the handler rather than on Resident for the same reason:
// Resident is the very bookkeeping the failure corrupts, so an assertion on it passes against the build
// this test exists to catch.
func TestCrew_SurplusRetiresAndTheCrewGrowsAgain(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var b blocker
	b.arm()
	crew, err := New(c, newGate(t, 64), b.process)
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(16)
	clk := retireOn(crew)
	crew.retireChance = 1 // the coin is not what this test is about

	stop := feed(c, 64)
	defer stop()
	crew.Start(context.Background(), 2)

	// Phase 1 - workers park in the handler still holding their candidate, so nobody is idle and the crew
	// grows to its ceiling. The clock is still, so nothing can retire while it does.
	assert.True(eventually(func() bool { return crew.Resident() == 16 }),
		"long tasks must grow the crew to Max, got %d", crew.Resident())
	assert.True(eventually(func() bool { return b.inTask.Load() == 16 }),
		"those must be real goroutines in the handler, got %d", b.inTask.Load())
	b.release()

	// Phase 2 - the same offered work, now instant, so every worker accrues zero busy against a still clock.
	// One round is one verdict each, and with a certain coin that is the whole surplus.
	stop = round(t, crew, c, clk, stop)
	assert.Equal(2, settles(crew), "an idle surplus must come to rest at Min")

	// Phase 3 - load returns, with Max still binding at 16 and the clock still (so no further verdicts).
	b.peak.Store(0)
	b.arm()
	stop()
	stop = feed(c, 64)
	assert.True(eventually(func() bool { return b.peak.Load() >= 8 }),
		"a crew that has retired must still be able to grow, peaked at %d", b.peak.Load())
	b.release()

	stop()
	c.Close()
	crew.Drain()
	assert.Equal(0, crew.Resident(), "every exit decrements resident, drain included")
}

// TestCrew_LongTasksDoNotRetire is why the rule measures TIME rather than items, and it is the negative half
// without which the whole design is unsound. These workers handle a handful of candidates per window against
// thousands for a no-op worker, so any "items handled below X" rule retires precisely the crew that
// grow-on-demand deliberately created for long tasks. A busy FRACTION is duration-independent.
//
// The whole crew is held inside the handler while the clock moves under it, so every worker's busy time IS
// its elapsed window - a fraction of exactly 1, with no sleeping and nothing to race.
func TestCrew_LongTasksDoNotRetire(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var b blocker
	b.arm()
	crew, err := New(c, newGate(t, 64), b.process)
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(8)
	clk := retireOn(crew)
	crew.retireChance = 1
	crew.retireThreshold = 1 // only a worker busy for its ENTIRE window survives, which is the claim

	fill(c, 8)
	crew.Start(context.Background(), 2)
	assert.True(eventually(func() bool { return b.inTask.Load() == 8 }),
		"every worker is inside a long task, got %d", b.inTask.Load())

	// A whole window passes with every worker holding its candidate. This is the one place the clock moves
	// WITHOUT quiescing first, and deliberately: banking the jump as busy time is precisely the reading
	// under test.
	clk.Advance(2 * time.Minute)
	b.release()

	assert.True(eventually(func() bool { return crew.Idle() == 8 }), "every worker came back round")
	assert.Equal(8, settles(crew), "a crew busy for its whole window must not retire")

	c.Close()
	crew.Drain()
}

// TestCrew_RetirementNeverGoesBelowMin pins the guard that keeps a quiet engine from shrinking to nothing
// and then stalling on the first arrival. Every worker here reads surplus and the coin always says go, so
// Min is the only thing holding the crew up.
func TestCrew_RetirementNeverGoesBelowMin(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var b blocker
	b.arm()
	crew, err := New(c, newGate(t, 64), b.process)
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(32)
	clk := retireOn(crew)
	crew.retireChance = 1
	crew.retireThreshold = 1 // every worker reads surplus

	stop := feed(c, 64)
	defer stop()
	crew.Start(context.Background(), 6)
	assert.Equal(6, crew.Min(), "Start's resident set is Min")
	assert.True(eventually(func() bool { return crew.Resident() == 32 }), "grow well above Min first")
	b.release()

	// Several rounds, each a verdict for every survivor. The first takes the whole surplus; the rest find
	// the crew already at Min and must take nobody. The assertion is on where it RESTS, because a build
	// without the guard descends straight past 6 on its way to zero.
	for range 3 {
		stop = round(t, crew, c, clk, stop)
		assert.Equal(6, settles(crew), "retirement must come to rest at Min and never cross it")
	}

	stop()
	c.Close()
	crew.Drain()
}

// TestCrew_TheCoinFlipCanDeclineRetirement pins that the probability is genuinely consulted rather than
// decorative. A build that ignored it would pass every other test here.
func TestCrew_TheCoinFlipCanDeclineRetirement(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var b blocker
	b.arm()
	crew, err := New(c, newGate(t, 64), b.process)
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(8)
	clk := retireOn(crew)
	crew.retireThreshold = 1 // every worker reads surplus...
	crew.retireChance = 0    // ...and the coin never lets one go.

	stop := feed(c, 64)
	defer stop()
	// Start SMALL and grow, so the crew sits well above Min - otherwise Min would be what held it up and
	// this would pass against a build that ignored the coin entirely.
	crew.Start(context.Background(), 2)
	assert.True(eventually(func() bool { return crew.Resident() == 8 }),
		"the crew must grow above Min first, got %d", crew.Resident())
	b.release()

	var rounds atomic.Int32
	crew.roll = func() float64 { rounds.Add(1); return 0 } // 0 is the LOWEST roll there is, and 0 >= 0
	for range 3 {
		stop = round(t, crew, c, clk, stop)
	}
	assert.True(eventually(func() bool { return rounds.Load() >= 8 }),
		"workers must have reached the coin at all, got %d rolls", rounds.Load())
	assert.Equal(8, settles(crew), "a zero chance must retire nobody")

	stop()
	c.Close()
	crew.Drain()
}

// TestCrew_TheCoinFlipShrinksOnlyPartOfTheSurplus is what the coin is FOR, and it is unobservable without
// both injections. Every worker measures the same load and reaches the same verdict at the same instant, so
// a deterministic rule takes the entire surplus in one go and the next arrival finds nobody but Min. The
// flip turns that one verdict into a partial shrink, and the window then makes the survivors re-measure a
// changed world before anyone decides again.
//
// The clock makes a round discrete - exactly one verdict per worker per Advance - and the injected coin
// makes the proportion exact rather than statistical, so this asserts counts and not tendencies. Growth is
// kept out by feeding one candidate at a time: no pop can leave idle at zero.
func TestCrew_TheCoinFlipShrinksOnlyPartOfTheSurplus(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	c := newCache(t)

	var b blocker
	b.arm()
	crew, err := New(c, newGate(t, 64), b.process)
	if !assert.NoError(err) {
		return
	}
	crew.SetMax(16)
	clk := retireOn(crew)
	crew.retireThreshold = 1 // every worker reads surplus, every round
	crew.retireChance = 0.25

	// Exactly one roll in four comes up under the chance. Concurrency-safe and order-independent: what the
	// assertions below turn on is how many rolls were drawn, never which worker drew which.
	var rolls atomic.Int64
	crew.roll = func() float64 {
		if rolls.Add(1)%4 == 0 {
			return 0.1 // under 0.25 - this worker goes
		}
		return 0.9
	}

	stop := feed(c, 64)
	defer stop()
	crew.Start(context.Background(), 4)
	assert.True(eventually(func() bool { return crew.Resident() == 16 }), "grow to Max first")
	b.release()

	// Round one: 16 verdicts, a quarter of them fatal. The crew must come down - and must NOT collapse.
	stop = round(t, crew, c, clk, stop)
	assert.True(eventually(func() bool { return rolls.Load() >= 16 }),
		"every worker must reach the coin, got %d rolls", rolls.Load())
	assert.Equal(12, settles(crew), "one verdict must take a QUARTER of the surplus and then STOP")
	assert.True(crew.Resident() > crew.Min(),
		"and must leave the crew well above Min - a cliff is what the coin exists to prevent")

	// Round two: the survivors re-measure and a quarter of THOSE go. That is the hysteresis - a shrink is
	// spread across rounds, each priced on a fresh measurement, rather than settled in one.
	stop = round(t, crew, c, clk, stop)
	assert.True(eventually(func() bool { return rolls.Load() >= 28 }),
		"every survivor must reach the coin again, got %d rolls", rolls.Load())
	assert.Equal(9, settles(crew), "the next round takes a quarter of the survivors")

	stop()
	c.Close()
	crew.Drain()
}
