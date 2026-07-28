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

package permits

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

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

// TestPermits_EnterBoundsConcurrency is the baseline: at most n holders at a time, and a release admits
// exactly one waiter.
func TestPermits_EnterBoundsConcurrency(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 2, 2)

	r1, ok := s.AcquireEnter(1)
	assert.True(ok)
	_, ok = s.AcquireEnter(1)
	assert.True(ok)
	enter, _ := s.Snapshot()
	assert.Equal(int64(0), enter[1])

	var admitted atomic.Int32
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, ok := s.AcquireEnter(1); ok {
			admitted.Add(1)
		}
	})
	time.Sleep(20 * time.Millisecond)
	assert.Equal(int32(0), admitted.Load(), "a third acquire must wait")

	r1()
	assert.True(eventually(func() bool { return admitted.Load() == 1 }), "a release admits the waiter")
	wg.Wait()
	s.Close()
}

// TestPermits_UnsizedShardAdmitsNothing pins that a shard nobody sized bounds at zero rather than at
// infinity. The engine sizes every shard before its workers start, so this is the safe direction for a
// configuration mistake: a stalled shard is visible, an unbounded one is the collapse the gate prevents.
func TestPermits_UnsizedShardAdmitsNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	var admitted atomic.Bool
	go func() {
		if _, ok := s.AcquireEnter(7); ok {
			admitted.Store(true)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	assert.False(admitted.Load(), "an unsized shard admits nothing")

	s.Resize(7, 1, 1)
	assert.True(eventually(admitted.Load), "sizing the shard releases the waiter")
	s.Close()
}

// TestPermits_ReservationsAreIndependent is THE property the split exists for, and it is pinned in BOTH
// directions because each failure was measured on a shared pool.
//
// Exits starving entries: giving exits precedence in one pool collapsed short-task throughput 3x on a
// saturating rig (4,416 vs 7,964 steps/s), because when work is instant the exit queue never empties and
// entry - which IS dispatch - never ran. Entries starving exits: served evenly instead, exits lost at
// random and queued behind entry, 286 of them waiting out a full second.
//
// Dedicated reservations make both impossible, which is what lets both sides simply block.
func TestPermits_ReservationsAreIndependent(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 1, 1)

	// Exhaust the EXIT side and hold it.
	exitRel, ok := s.AcquireExit(1)
	assert.True(ok)
	_, exit := s.Snapshot()
	assert.Equal(int64(0), exit[1])

	// Entry must be untouched by that: it has its own reservation.
	entered := make(chan struct{})
	go func() {
		if _, ok := s.AcquireEnter(1); ok {
			close(entered)
		}
	}()
	got := false
	select {
	case <-entered:
		got = true
	case <-time.After(2 * time.Second):
	}
	assert.True(got, "an exhausted exit side must not block entry - that is the 3x collapse this prevents")
	exitRel()

	// And the reverse: with entry exhausted and held, an exit is served immediately.
	s2 := New()
	s2.Resize(1, 1, 1)
	_, ok = s2.AcquireEnter(1)
	assert.True(ok)
	served := make(chan struct{})
	go func() {
		if _, ok := s2.AcquireExit(1); ok {
			close(served)
		}
	}()
	got = false
	select {
	case <-served:
		got = true
	case <-time.After(2 * time.Second):
	}
	assert.True(got, "an exhausted entry side must not block exits - that is the 286-waiter failure")

	s.Close()
	s2.Close()
}

// TestPermits_ExitWaitsForItsOwnReservation pins that an exit DOES queue when its own side is full, and is
// handed the permit the moment one frees. Blocking is safe only because it waits behind other exits, which
// are themselves finishing - so this is the wait that must work, not one to avoid.
func TestPermits_ExitWaitsForItsOwnReservation(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 4, 1)
	rel, ok := s.AcquireExit(1) // the only exit permit
	assert.True(ok)

	var served atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, ok := s.AcquireExit(1); ok {
			served.Store(true)
		}
	})
	time.Sleep(20 * time.Millisecond)
	assert.False(served.Load(), "with its reservation exhausted, an exit waits")
	_, exit := s.Snapshot()
	assert.Equal(int64(0), exit[1], "and nothing is handed out past the bound")

	rel()
	assert.True(eventually(served.Load), "a release is handed straight to the waiting exit")
	wg.Wait()
	s.Close()
}

// TestPermits_ExitIsImmediateWhenFreeOrClosed pins the two paths that must not wait: a permit already free,
// and the drain. On close the caller gets !ok and MUST still record its work - the outcome exists nowhere
// else - so the release has to be a safe no-op rather than a missing permit.
func TestPermits_ExitIsImmediateWhenFreeOrClosed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 2, 2)
	start := time.Now()
	rel, ok := s.AcquireExit(1)
	assert.True(ok)
	assert.True(time.Since(start) < time.Second, "a free permit is taken without waiting")
	rel()

	s.Close()
	start = time.Now()
	rel, ok = s.AcquireExit(1)
	assert.False(ok, "a closed Set reports the drain rather than blocking the caller in it")
	assert.True(time.Since(start) < time.Second)
	rel() // must be a safe no-op
}

// TestPermits_ResizeMovesByDelta pins that a live resize never hands out a permit an in-flight holder is
// still using. Assigning the ceiling outright - the obvious spelling - would do exactly that.
func TestPermits_ResizeMovesByDelta(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 4, 4)
	_, ok := s.AcquireEnter(1)
	assert.True(ok)
	_, ok = s.AcquireEnter(1)
	assert.True(ok)
	enter, _ := s.Snapshot()
	assert.Equal(int64(2), enter[1])

	// Grow: +4 available, NOT reset to 8 (which would double-issue the two held).
	s.Resize(1, 8, 4)
	enter, _ = s.Snapshot()
	assert.Equal(int64(6), enter[1], "a grow adds the delta, leaving held permits held")
	e, x := s.Size(1)
	assert.Equal(8, e)
	assert.Equal(4, x, "the other reservation is untouched by its neighbour's resize")

	// Shrink below what is held: the count simply goes negative and blocks until holders release.
	s.Resize(1, 1, 4)
	enter, _ = s.Snapshot()
	assert.Equal(int64(-1), enter[1], "a shrink past the held count blocks rather than over-issuing")
	s.Close()
}

// TestPermits_ReleaseWakesTheRIGHTShardsWaiter pins the per-shard waiting queue, which is the one place a
// plausible simplification is silently wrong.
//
// With ONE condition variable shared across shards, a release on shard 1 can Signal a goroutine waiting on
// shard 2: it wakes, re-checks its own count, finds nothing, and sleeps again - while shard 1's waiter is
// never woken and its free permit sits unused. Nothing detects that; admission on the shard with capacity
// simply stops.
func TestPermits_ReleaseWakesTheRIGHTShardsWaiter(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	const shards = 8
	for shard := 1; shard <= shards; shard++ {
		s.Resize(shard, 1, 1)
	}
	var rel1 func()
	for shard := 1; shard <= shards; shard++ {
		r, ok := s.AcquireEnter(shard)
		assert.True(ok)
		if shard == 1 {
			rel1 = r
		}
	}

	admitted := make([]atomic.Bool, shards+1)
	var wg sync.WaitGroup
	for shard := 1; shard <= shards; shard++ {
		wg.Go(func() {
			if _, ok := s.AcquireEnter(shard); ok {
				admitted[shard].Store(true)
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	for shard := 1; shard <= shards; shard++ {
		assert.False(admitted[shard].Load(), "shard %d must still be waiting", shard)
	}

	rel1()
	assert.True(eventually(admitted[1].Load), "the release must wake the waiter on ITS OWN shard")
	for shard := 2; shard <= shards; shard++ {
		assert.False(admitted[shard].Load(), "shard %d has no free permit and must not be admitted", shard)
	}

	s.Close()
	wg.Wait()
}

// TestPermits_CloseReleasesEveryWaiter pins the stop signal on BOTH reservations. The crew's Drain waits on
// workers that may be blocked in either, and closing the cache alone cannot reach them - so a close that
// failed to wake one queue would hang shutdown.
func TestPermits_CloseReleasesEveryWaiter(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 1, 1)
	_, ok := s.AcquireEnter(1)
	assert.True(ok)
	_, _ = s.AcquireExit(1)

	var woke atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if _, ok := s.AcquireEnter(1); !ok {
				woke.Add(1)
			}
		})
	}
	for range 2 {
		wg.Go(func() {
			s.AcquireExit(1) // must not outlive the close
			woke.Add(1)
		})
	}
	time.Sleep(20 * time.Millisecond)
	s.Close()
	wg.Wait()
	assert.Equal(int32(6), woke.Load(), "every waiter on both reservations is released")

	_, ok = s.AcquireEnter(1)
	assert.False(ok, "a closed set admits nobody, forever")
	s.Close() // idempotent
}

// TestPermits_ConcurrentHoldersNeverExceedTheBound is the stress form of the bound: many goroutines cycling
// acquire/release, with the peak concurrent holder count asserted against the ceiling. Run under -race this
// also exercises the cond's predicate re-check.
func TestPermits_ConcurrentHoldersNeverExceedTheBound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	const bound = 5
	s := New()
	s.Resize(1, bound, bound)

	var held, peak atomic.Int32
	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			for range 50 {
				rel, ok := s.AcquireEnter(1)
				if !ok {
					return
				}
				n := held.Add(1)
				for {
					p := peak.Load()
					if n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				held.Add(-1)
				rel()
			}
		})
	}
	wg.Wait()
	assert.True(peak.Load() <= bound, "concurrent holders never exceeded the bound, peaked at %d", peak.Load())
	enter, _ := s.Snapshot()
	assert.Equal(int64(bound), enter[1], "every permit came back")
	s.Close()
}
