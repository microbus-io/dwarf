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

// TestPermits_AcquireBoundsConcurrency is the baseline: at most n holders at a time, and a release admits
// exactly one waiter.
func TestPermits_AcquireBoundsConcurrency(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 2)

	r1, ok := s.Acquire(1)
	assert.True(ok)
	_, ok = s.Acquire(1)
	assert.True(ok)
	assert.Equal(int64(0), s.Snapshot()[1])

	var admitted atomic.Int32
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, ok := s.Acquire(1); ok {
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
// configuration mistake: a stalled shard is visible, an unbounded one is the collapse the gate exists to
// prevent.
func TestPermits_UnsizedShardAdmitsNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	var admitted atomic.Bool
	go func() {
		if _, ok := s.Acquire(7); ok {
			admitted.Store(true)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	assert.False(admitted.Load(), "an unsized shard admits nothing")

	s.Resize(7, 1)
	assert.True(eventually(admitted.Load), "sizing the shard releases the waiter")
	s.Close()
}

// TestPermits_DebitGoesNegativeAndSuppressesAdmission is THE property this type exists for, and the reason
// it is a mutex+cond over an int64 rather than a buffered channel or x/sync/semaphore - neither can express
// a take past zero.
//
// The completion path must never queue: its side effects have already fired and its outcome exists nowhere
// but in that goroutine, so recording it outranks throughput. But it must not be invisible either, or a
// storm of completions would swamp the resource while new admission carried on at full rate. Debiting
// resolves both: the completion proceeds immediately, and while the count is negative NOTHING new is
// admitted.
func TestPermits_DebitGoesNegativeAndSuppressesAdmission(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 1)

	// One holder takes the only permit; three completions debit past zero without waiting.
	rel, ok := s.Acquire(1)
	assert.True(ok)
	for range 3 {
		s.Debit(1) // must not block
	}
	assert.Equal(int64(-3), s.Snapshot()[1], "the count goes negative rather than making a completion wait")

	// The holder releasing is not enough to admit anyone: the debt comes first.
	rel()
	var admitted atomic.Bool
	go func() {
		if _, ok := s.Acquire(1); ok {
			admitted.Store(true)
		}
	}()
	time.Sleep(20 * time.Millisecond)
	assert.False(admitted.Load(), "admission stays suppressed while the count is negative")
	assert.Equal(int64(-2), s.Snapshot()[1])

	// Each Restore pays down the debt; only once the count goes positive is anyone admitted.
	s.Restore(1)
	s.Restore(1)
	time.Sleep(20 * time.Millisecond)
	assert.False(admitted.Load(), "still nothing free at zero")
	s.Restore(1)
	assert.True(eventually(admitted.Load), "admission resumes once the storm has drained")
	s.Close()
}

// TestPermits_ResizeMovesByDelta pins that a live resize never hands out a permit an in-flight holder is
// still using. Assigning the ceiling outright - the obvious spelling - would do exactly that.
func TestPermits_ResizeMovesByDelta(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 4)
	_, ok := s.Acquire(1)
	assert.True(ok)
	_, ok = s.Acquire(1)
	assert.True(ok)
	assert.Equal(int64(2), s.Snapshot()[1])

	// Grow: +4 available, NOT reset to 8 (which would double-issue the two held).
	s.Resize(1, 8)
	assert.Equal(int64(6), s.Snapshot()[1], "a grow adds the delta, leaving held permits held")
	assert.Equal(8, s.Size(1))

	// Shrink below what is held: the count simply goes negative and blocks admission until holders release.
	s.Resize(1, 1)
	assert.Equal(int64(-1), s.Snapshot()[1], "a shrink past the held count blocks rather than over-issuing")
	s.Close()
}

// TestPermits_ReleaseWakesTheRIGHTShardsWaiter pins the per-shard waiting queue, which is the one place a
// plausible simplification is silently wrong.
//
// With ONE condition variable shared across shards, a release on shard 1 can Signal a goroutine waiting on
// shard 2: it wakes, re-checks its own count, finds nothing, and sleeps again - while shard 1's waiter is
// never woken and its free permit sits unused. Nothing detects that; admission on the shard with capacity
// simply stops.
//
// The test drives exactly that interleaving: every shard exhausted, one waiter parked on each, then a single
// release on shard 1. Only shard 1's waiter may proceed, and it MUST.
func TestPermits_ReleaseWakesTheRIGHTShardsWaiter(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	const shards = 8
	for shard := 1; shard <= shards; shard++ {
		s.Resize(shard, 1)
	}
	// Exhaust every shard, keeping shard 1's release for the assertion below.
	var rel1 func()
	for shard := 1; shard <= shards; shard++ {
		r, ok := s.Acquire(shard)
		assert.True(ok)
		if shard == 1 {
			rel1 = r
		}
	}

	// One waiter per shard, each blocked on its own exhausted count.
	admitted := make([]atomic.Bool, shards+1)
	var wg sync.WaitGroup
	for shard := 1; shard <= shards; shard++ {
		wg.Go(func() {
			if _, ok := s.Acquire(shard); ok {
				admitted[shard].Store(true)
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	for shard := 1; shard <= shards; shard++ {
		assert.False(admitted[shard].Load(), "shard %d must still be waiting", shard)
	}

	// One release on shard 1. A shared cond would let this wake any of the eight, and seven of those wake to
	// no effect - leaving shard 1's waiter asleep on a permit that is free.
	rel1()
	assert.True(eventually(admitted[1].Load), "the release must wake the waiter on ITS OWN shard")
	for shard := 2; shard <= shards; shard++ {
		assert.False(admitted[shard].Load(), "shard %d has no free permit and must not be admitted", shard)
	}

	s.Close()
	wg.Wait()
}

// TestPermits_CloseReleasesEveryWaiter pins the stop signal. The crew's Drain waits on workers that may be
// blocked here, and closing the cache alone cannot reach them - so a close that failed to wake a waiter
// would hang shutdown.
func TestPermits_CloseReleasesEveryWaiter(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	s := New()
	s.Resize(1, 1)
	_, ok := s.Acquire(1)
	assert.True(ok)

	var woke atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			if _, ok := s.Acquire(1); !ok {
				woke.Add(1)
			}
		})
	}
	time.Sleep(20 * time.Millisecond)
	s.Close()
	wg.Wait()
	assert.Equal(int32(4), woke.Load(), "every waiter is released with ok=false")

	_, ok = s.Acquire(1)
	assert.False(ok, "a closed set admits nobody, forever")
	assert.False(s.Available(1))
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
	s.Resize(1, bound)

	var held, peak atomic.Int32
	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			for range 50 {
				rel, ok := s.Acquire(1)
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
	assert.Equal(int64(bound), s.Snapshot()[1], "every permit came back")
	s.Close()
}
