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

package turnstile

import (
	"container/heap"
	"context"
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

// queued reports whether n callers are parked in the turnstile. Every ordering test needs this: a waiter
// that has not reached the queue yet cannot be ordered against one that has, so releasing before they are
// all in line would measure the race instead of the ordering.
func queued(t *Turnstile, n int) bool {
	return eventually(func() bool {
		_, waiting := t.Snapshot()
		return waiting == n
	})
}

// TestTurnstile_BoundsConcurrency is the baseline: at most n passes are out at a time, and a return admits
// exactly one waiter.
func TestTurnstile_BoundsConcurrency(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(2)
	p1, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)
	_, ok = ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)
	avail, _ := ts.Snapshot()
	assert.Equal(0, avail)

	var admitted atomic.Int32
	var wg sync.WaitGroup
	wg.Go(func() {
		if _, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()}); ok {
			admitted.Add(1)
		}
	})
	time.Sleep(20 * time.Millisecond)
	assert.Equal(int32(0), admitted.Load(), "a third turn must wait")

	p1.Return()
	assert.True(eventually(func() bool { return admitted.Load() == 1 }), "a return admits the waiter")
	wg.Wait()
	ts.Close()
}

// TestTurnstile_BandBeatsAge pins strict priority: a better band arriving LAST is served FIRST, ahead of an
// older claim. This is the property that makes a band unusable for anything that can arrive faster than it
// is served - whatever sits in the better band starves everything below it.
func TestTurnstile_BandBeatsAge(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(1)
	held, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)

	old := time.Now().Add(-time.Hour)
	served := make(chan string, 2)
	var wg sync.WaitGroup
	wg.Go(func() {
		p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: old})
		if ok {
			served <- "old-band-1"
			p.Return()
		}
	})
	assert.True(queued(ts, 1), "the older claim must be in line before the better band arrives")

	wg.Go(func() {
		p, ok := ts.WaitTurn(ctx, Claim{Priority: 0, Since: time.Now()})
		if ok {
			served <- "new-band-0"
			p.Return()
		}
	})
	assert.True(queued(ts, 2))

	held.Return()
	wg.Wait()
	assert.Equal("new-band-0", <-served, "the better band is served first however late it arrived")
	assert.Equal("old-band-1", <-served)
	ts.Close()
}

// TestTurnstile_OldestFirstWithinABand pins the ordering the design exists for: inside one band, the caller
// whose WORK started earliest wins, regardless of when it got in line. That is what makes the order
// first-in-first-out over jobs - a caller returning for its next turn keeps its original age and beats work
// that started later, so jobs in progress finish before new ones begin.
func TestTurnstile_OldestFirstWithinABand(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(1)
	held, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)

	now := time.Now()
	served := make(chan int, 3)
	var wg sync.WaitGroup
	// Queued youngest first, so arrival order is the exact reverse of the order they must be served in.
	for _, age := range []int{1, 2, 3} {
		wg.Go(func() {
			p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: now.Add(-time.Duration(age) * time.Minute)})
			if ok {
				served <- age
				p.Return()
			}
		})
		assert.True(queued(ts, age), "each waiter must be in line before the next arrives")
	}

	held.Return()
	wg.Wait()
	assert.Equal(3, <-served, "the oldest work is served first")
	assert.Equal(2, <-served)
	assert.Equal(1, <-served)
	ts.Close()
}

// TestTurnstile_EqualClaimsServedInArrivalOrder pins the tiebreak. Two callers can hold the same band and
// the same timestamp - a coarse clock, or two turns of one job - and without a third term the heap orders
// them arbitrarily, which makes the whole ordering untestable and lets one of them be passed over
// repeatedly.
func TestTurnstile_EqualClaimsServedInArrivalOrder(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(1)
	held, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)

	same := time.Now()
	served := make(chan int, 4)
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: same})
			if ok {
				served <- i
				p.Return()
			}
		})
		assert.True(queued(ts, i+1))
	}

	held.Return()
	wg.Wait()
	for i := range 4 {
		assert.Equal(i, <-served, "identical claims are served in the order they arrived")
	}
	ts.Close()
}

// TestTurnstile_ContextDeadlineReleasesTheCallerAndItsPlace pins both halves of giving up: the caller stops
// waiting, AND it leaves the queue. Leaving it in place would be the worse bug of the two - the pass would
// be handed to a caller that is no longer there to take it, stalling every waiter behind a claim that can
// never be satisfied.
func TestTurnstile_ContextDeadlineReleasesTheCallerAndItsPlace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	ts := New(1)
	held, ok := ts.WaitTurn(context.Background(), Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)

	// The better claim by age, but on a deadline it will not outlive.
	var gaveUp atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now().Add(-time.Hour)})
		assert.False(ok, "an expired deadline issues no pass")
		p.Return() // the zero pass must be safe to return unconditionally
		gaveUp.Store(true)
	})
	assert.True(queued(ts, 1))

	// The worse claim by age, which therefore only wins if the first one really left.
	served := make(chan struct{})
	wg.Go(func() {
		p, ok := ts.WaitTurn(context.Background(), Claim{Priority: 1, Since: time.Now()})
		if ok {
			close(served)
			p.Return()
		}
	})
	assert.True(queued(ts, 2))

	assert.True(eventually(gaveUp.Load), "the deadline releases its caller")
	assert.True(queued(ts, 1), "and takes its place in line with it")

	held.Return()
	select {
	case <-served:
	case <-time.After(2 * time.Second):
		assert.True(false, "the remaining waiter must be served once the abandoned claim is gone")
	}
	wg.Wait()
	ts.Close()
}

// TestTurnstile_AbandonedGrantIsNotLost drives the one window with no ordering to lean on: a pass handed
// over in the instant between the ctx firing and the caller noticing. The caller then holds a pass nobody
// else knows about, so simply walking away retires it - the ceiling shrinks by one, permanently and
// silently, and does so again on every recurrence until nothing is admitted at all.
//
// The window is nanoseconds wide, so it is FORCED rather than raced for: the test takes the turnstile's own
// lock, cancels the ctx, and grants the pass while the abandoning caller is stuck outside that lock. The
// sleep is what makes it deterministic rather than hopeful - throughout it, the caller's select has only
// ctx.Done() ready, so it cannot take the other branch, and it can get no further than the lock the test is
// holding. Racing for this instead measures nothing: an earlier stochastic version of this test passed
// against an implementation that dropped the pass outright.
func TestTurnstile_AbandonedGrantIsNotLost(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	ts := New(1)
	// The only pass, held. It is never returned through Pass.Return: the releaseLocked below stands in for
	// that return, landing at the one instant that makes the grant unobservable to the caller receiving it.
	_, ok := ts.WaitTurn(context.Background(), Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)

	ctx, cancel := context.WithCancel(context.Background())
	var gaveUp atomic.Bool
	var wg sync.WaitGroup
	wg.Go(func() {
		p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
		assert.False(ok, "the caller that gave up is issued nothing")
		p.Return()
		gaveUp.Store(true)
	})
	assert.True(queued(ts, 1))

	ts.mu.Lock()
	cancel()
	time.Sleep(50 * time.Millisecond) // the caller is now inside the ctx branch, blocked on this lock
	ts.releaseLocked()                // the holder returns at exactly the wrong instant: the pass is granted
	assert.True(ts.q.Len() == 0 && ts.avail == 0, "the pass was handed to the caller that is walking away")
	ts.mu.Unlock()

	assert.True(eventually(gaveUp.Load), "the caller still reports that it got nothing")
	wg.Wait()
	avail, waiting := ts.Snapshot()
	assert.Equal(1, avail, "and the pass it was given is passed on rather than retired")
	assert.Equal(0, waiting)
	assert.Equal(ts.Size(), avail, "the turnstile is whole again, at its full ceiling")
	ts.Close()
}

// TestTurnstile_DoubleReturnIsANoOp pins that a second return does not issue a pass that was never taken.
// The failure it prevents is quiet rather than loud: the ceiling drifts above the resource it is sized
// against, so the excess callers simply queue somewhere with no ordering at all, and the turnstile reports
// itself healthy the whole time.
func TestTurnstile_DoubleReturnIsANoOp(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(2)
	p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)
	avail, _ := ts.Snapshot()
	assert.Equal(1, avail)

	p.Return()
	avail, _ = ts.Snapshot()
	assert.Equal(2, avail)

	p.Return()
	avail, _ = ts.Snapshot()
	assert.Equal(2, avail, "a second return issues nothing")

	var zero Pass
	zero.Return() // a pass that was never issued is safe to return
	avail, _ = ts.Snapshot()
	assert.Equal(2, avail)
	ts.Close()
}

// TestTurnstile_ResizeMovesByDelta pins that a live resize never issues a pass an in-flight holder is still
// using. Assigning the ceiling outright - the obvious spelling - would do exactly that.
func TestTurnstile_ResizeMovesByDelta(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(4)
	_, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)
	held, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)
	avail, _ := ts.Snapshot()
	assert.Equal(2, avail)

	// Grow: +4 available, NOT reset to 8, which would issue the two held a second time.
	ts.Resize(8)
	avail, _ = ts.Snapshot()
	assert.Equal(6, avail, "a grow adds the delta, leaving held passes held")
	assert.Equal(8, ts.Size())

	// Shrink below what is held: the count goes negative and admits nobody until holders return.
	ts.Resize(1)
	avail, _ = ts.Snapshot()
	assert.Equal(-1, avail, "a shrink past the held count blocks rather than over-issuing")

	held.Return()
	avail, _ = ts.Snapshot()
	assert.Equal(0, avail, "a return climbs out of the hole rather than admitting anyone")
	ts.Close()
}

// TestTurnstile_ResizeGrowAdmitsWaitersInOrder pins that new capacity goes to the head of the queue rather
// than to whoever happens to call next. A grow that only raised the count would leave the waiters it was
// meant for asleep on passes that are already free.
func TestTurnstile_ResizeGrowAdmitsWaitersInOrder(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(0) // admits nobody until sized
	now := time.Now()
	served := make(chan int, 3)
	var wg sync.WaitGroup
	for _, age := range []int{1, 2, 3} {
		wg.Go(func() {
			p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: now.Add(-time.Duration(age) * time.Minute)})
			if ok {
				served <- age
				p.Return()
			}
		})
		assert.True(queued(ts, age))
	}

	ts.Resize(1)
	wg.Wait()
	assert.Equal(3, <-served, "the grow goes to the best waiting claim")
	assert.Equal(2, <-served)
	assert.Equal(1, <-served)
	ts.Close()
}

// TestTurnstile_CloseReleasesEveryWaiter pins the stop signal. A caller parked here is reachable by nothing
// else, so a close that failed to wake the queue would hang shutdown outright.
func TestTurnstile_CloseReleasesEveryWaiter(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	ts := New(1)
	_, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)

	var woke atomic.Int32
	var wg sync.WaitGroup
	for range 6 {
		wg.Go(func() {
			p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
			if !ok {
				woke.Add(1)
			}
			p.Return() // must be a safe no-op on the drain path
		})
	}
	assert.True(queued(ts, 6))

	ts.Close()
	wg.Wait()
	assert.Equal(int32(6), woke.Load(), "every waiter is released")

	p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
	assert.False(ok, "a closed turnstile admits nobody, forever")
	p.Return()
	ts.Close() // idempotent
}

// TestTurnstile_ConcurrentHoldersNeverExceedTheBound is the stress form of the bound, with the peak
// concurrent holder count asserted against the ceiling. Under -race it also exercises the handoff's
// ordering of the grant against the waiter's read of it.
func TestTurnstile_ConcurrentHoldersNeverExceedTheBound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const bound = 5
	ts := New(bound)
	var held, peak atomic.Int32
	var wg sync.WaitGroup
	for range 40 {
		wg.Go(func() {
			for range 50 {
				p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
				if !ok {
					return
				}
				n := held.Add(1)
				for {
					if p := peak.Load(); n <= p || peak.CompareAndSwap(p, n) {
						break
					}
				}
				held.Add(-1)
				p.Return()
			}
		})
	}
	wg.Wait()
	assert.True(peak.Load() <= bound, "concurrent holders never exceeded the bound, peaked at %d", peak.Load())
	avail, waiting := ts.Snapshot()
	assert.Equal(bound, avail, "every pass came back")
	assert.Equal(0, waiting)
	ts.Close()
}

// BenchmarkTurnstile_Uncontended is the cost on the path every caller pays even when nothing is waiting.
func BenchmarkTurnstile_Uncontended(b *testing.B) {
	ctx := context.Background()
	ts := New(1)
	now := time.Now()
	for b.Loop() {
		p, _ := ts.WaitTurn(ctx, Claim{Priority: 1, Since: now})
		p.Return()
	}
	ts.Close()
}

// BenchmarkTurnstile_Contended is the cost with every caller queueing, which is where the heap and the
// handoff are actually exercised - and where a single mutex would convoy if it were going to.
func BenchmarkTurnstile_Contended(b *testing.B) {
	ctx := context.Background()
	ts := New(8)
	now := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			p, _ := ts.WaitTurn(ctx, Claim{Priority: 1, Since: now})
			p.Return()
		}
	})
	ts.Close()
}

// TestTurnstile_CloseRacingAnExpiredContextDoesNotPanic pins the OTHER abandoned-waiter race, the mirror of
// TestTurnstile_AbandonedGrantIsNotLost. Close pops every waiter off the heap, which sets its index to -1
// while leaving it UNGRANTED; a waiter whose ctx expired in the same instant then takes the give-up branch
// and, seeing no grant, tries to remove itself from a heap it is no longer in. heap.Remove(-1) indexes the
// slice at -1 and takes the process down - on the CALLER's goroutine, which nothing here wraps.
//
// Forced rather than raced for, the same way the grant race is: the test holds the lock, expires the ctx,
// and closes underneath the waiter while it is stuck outside.
func TestTurnstile_CloseRacingAnExpiredContextDoesNotPanic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	ts := New(1)
	_, ok := ts.WaitTurn(context.Background(), Claim{Priority: 1, Since: time.Now()})
	assert.True(ok)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan bool, 1)
	go func() {
		p, ok := ts.WaitTurn(ctx, Claim{Priority: 1, Since: time.Now()})
		p.Return()
		done <- ok
	}()
	assert.True(queued(ts, 1))

	ts.mu.Lock()
	cancel()
	time.Sleep(50 * time.Millisecond) // the waiter is now in the ctx branch, blocked on this lock
	ts.closed = true                  // Close's effect, performed under the lock the waiter is waiting for
	for ts.q.Len() > 0 {
		close(heap.Pop(&ts.q).(*entry).ch) // pops set index = -1 and leave granted false
	}
	ts.mu.Unlock()

	select {
	case got := <-done:
		assert.False(got, "a waiter released by Close is issued nothing")
	case <-time.After(5 * time.Second):
		assert.True(false, "the waiter never returned")
	}
	ts.Close() // idempotent
}
