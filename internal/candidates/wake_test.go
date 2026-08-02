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

package candidates

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// settle waits briefly for cond, so a test never turns on goroutine scheduling order.
func settle(cond func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// parked reports how many callers are in cond.Wait right now. Reading the field directly is the point of
// keeping these tests in-package: the wake bound is computed from it, so a test that could not see it would
// be asserting on timing instead.
func parked(c *Cache) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiting
}

// TestCandidateCache_OneRefillStrandsNoCandidate pins the safety of waking FEWER waiters than there are
// parked: a single refill, with nothing after it to rescue a straggler, must still serve every candidate.
//
// What makes that safe is that a waiter parks only on an EMPTY cache and both wait sites re-check totalLen
// in a loop, so a woken worker that loses its race goes round again rather than parking on top of work: the
// floor on progress is ONE worker, not one per candidate. What this test catches is a refill that wakes
// NOBODY. It does not isolate the re-check loop - verified, by turning both of them into a single if, which
// the whole suite survives - because that same greedy drain covers for it here.
//
// IT DELIBERATELY ASSERTS NOTHING ABOUT PARALLELISM, and the reason is worth keeping because the obvious
// test does not work. A woken worker with a fast handler comes round, finds the cache still non-empty and
// takes another WITHOUT re-parking, so a batch of 64 is routinely drained by a handful of workers - measured
// at 7 - however many were signalled. The count of distinct workers that got one is therefore not evidence
// of anything.
//
// Nor is any other count: the losers of a broadcast do not return from WaitForWork either (they re-park
// inside its loop once the batch has drained), so a herd and a bounded wake are INDISTINGUISHABLE from
// outside. The wake protocol shows up on the clock alone - see BenchmarkCache_RefillWake, which is where
// that evidence lives.
func TestCandidateCache_OneRefillStrandsNoCandidate(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	const crew, batch = 256, 64
	c := &Cache{}
	c.Init(batch)
	var total atomic.Int64
	var wg sync.WaitGroup
	for range crew {
		wg.Go(func() {
			for {
				shard, ok := c.WaitForWork()
				if !ok {
					return
				}
				if _, ok, _ := c.TryPopFrom(shard); ok {
					total.Add(1)
				}
			}
		})
	}
	// The whole crew parked, so it is four times the batch: most of it must be left alone, and whoever is
	// woken has to finish the job unaided.
	assert.True(settle(func() bool { return parked(c) == crew }), "the whole crew parks first")

	jobs := make([]Job, batch)
	for i := range jobs {
		jobs[i] = Job{StepID: i + 1, Shard: 1}
	}
	c.Refill(1, jobs, 0)

	assert.True(settle(func() bool { return total.Load() == batch }),
		"one refill must serve every candidate it admitted, got %d of %d", total.Load(), batch)

	c.Close()
	wg.Wait()
}

// TestCandidateCache_WakesGoInArrivalOrder pins that waiters are woken FIFO.
//
// THIS DEPENDS ON AN IMPLEMENTATION DETAIL, WHICH IS WHY IT IS PINNED HERE. sync.Cond's documented contract
// promises only that Signal "wakes one goroutine waiting on c, if there is any" - nothing about WHICH one,
// and it explicitly disclaims scheduling order against goroutines contending for c.L. FIFO comes from the
// runtime's ticket-based notifyList (runtime/sema.go): a waiter takes a monotonically increasing ticket on
// arrival, and notifyListNotifyOne wakes the holder of the next one in sequence. That is a strong property
// and it has been stable for many releases, but it is not something the API owes us, so this test is the
// canary: a Go release that relaxed it would fail HERE rather than silently starving the crew's retirement
// check months later.
//
// Note the scope of the guarantee, which is narrower than it first looks. FIFO governs which waiter is
// SELECTED and readied; it says nothing about which then wins c.L, and Signal's own doc warns that a
// goroutine merely locking c.L may barge ahead. Selection is what the no-starvation argument needs, so
// waking one at a time - a single candidate per round, with the crew fully re-parked before the next - is
// what makes the order observable at all.
func TestCandidateCache_WakesGoInArrivalOrder(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	const crew = 16
	c := &Cache{}
	c.Init(crew)
	var order []int
	var mu sync.Mutex
	var total atomic.Int64
	var wg sync.WaitGroup
	// Spawned one at a time, each parked before the next starts, so arrival order IS spawn order and the
	// expected wake sequence is known rather than inferred.
	for w := range crew {
		wg.Go(func() {
			for {
				shard, ok := c.WaitForWork()
				if !ok {
					return
				}
				if _, ok, _ := c.TryPopFrom(shard); ok {
					mu.Lock()
					order = append(order, w)
					mu.Unlock()
					total.Add(1)
				}
			}
		})
		if !assert.True(settle(func() bool { return parked(c) == w+1 }), "worker %d parks before the next", w) {
			break
		}
	}

	for i := range crew {
		c.Refill(1, []Job{{StepID: i + 1, Shard: 1}}, 0)
		// Back to a FULLY parked crew, not one short: the worker that took the candidate loops round and
		// re-parks, taking a fresh ticket at the BACK of the queue - which is the mechanism that makes the
		// expected order a plain rotation rather than a single worker serving everything.
		if !assert.True(settle(func() bool { return total.Load() == int64(i+1) && parked(c) == crew }),
			"round %d must be served and the crew fully re-parked", i) {
			break
		}
	}

	mu.Lock()
	got := append([]int(nil), order...)
	mu.Unlock()
	want := make([]int, crew)
	for i := range want {
		want[i] = i
	}
	assert.Equal(want, got, "waiters must be woken in arrival order")

	c.Close()
	wg.Wait()
}

// TestCandidateCache_WakesRotateAcrossTheParkedCrew pins that waiters are served FIFO, which is not a detail
// of sync.Cond a caller may take or leave: internal/workers retires a surplus worker on a check it can only
// reach by WAKING, so a wake protocol that kept re-waking the same few would starve the rest of that check
// and the crew would never shrink. Work distribution depends on it equally.
//
// One candidate per round, with the crew fully re-parked before the next, so exactly one worker wakes per
// round and rotation is the only way to reach them all. A LIFO handoff would keep re-waking the most
// recently parked worker, and this would fail with almost none of the crew touched.
func TestCandidateCache_WakesRotateAcrossTheParkedCrew(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	const crew = 32
	c := &Cache{}
	c.Init(crew)
	woken := make([]atomic.Int32, crew)
	var total atomic.Int64
	var wg sync.WaitGroup
	for w := range crew {
		wg.Go(func() {
			for {
				shard, ok := c.WaitForWork()
				if !ok {
					return
				}
				if _, ok, _ := c.TryPopFrom(shard); ok {
					woken[w].Add(1)
					total.Add(1)
				}
			}
		})
	}
	assert.True(settle(func() bool { return parked(c) == crew }), "the whole crew parks first")

	for i := range crew * 2 {
		c.Refill(1, []Job{{StepID: i + 1, Shard: 1}}, 0)
		// Wait for the candidate to be taken AND its taker to re-park, so the next round starts from a
		// fully parked crew and cannot be served by a worker that simply never went back to sleep.
		if !settle(func() bool { return total.Load() == int64(i+1) && parked(c) == crew }) {
			break
		}
	}

	distinct := 0
	for w := range crew {
		if woken[w].Load() > 0 {
			distinct++
		}
	}
	assert.Equal(crew, distinct, "wakes must rotate through every parked worker, not re-wake the same few")

	c.Close()
	wg.Wait()
}

// BenchmarkCache_RefillWake measures WAKE AMPLIFICATION: how much a parked crew is disturbed to hand out one
// refill's worth of candidates. The crew is varied against a FIXED batch, because that is the whole question
// - useful work per refill is bounded by the batch, so anything that scales with the crew is herd.
//
// takes/candidate is the CONTROL, not the finding. It sits at 1.05-1.10 under either wake protocol, which is
// what makes the wall-clock gap attributable: both arms do identical work, so the difference is overhead and
// nothing else. It does NOT expose the herd, and expecting it to is the wrong model - the losers of a
// broadcast do not get far enough to spin. They wake, queue on the one mutex, and by the time each acquires
// it the batch is long since drained, so they re-park inside WaitForWork's loop without ever returning. The
// cost is the wake-and-requeue convoy itself, and only the clock sees it.
//
// Measured on this benchmark, medians of 5 at 200 refills of 64 candidates (ns/op):
//
//	crew    64     256     808     2048     8192
//	bcast   47.8k  61.7k   182k    534k     1,306k
//	signal  48.5k  60.2k   59.5k   61.8k    60.6k
//
// Signal-N is FLAT across a 128x crew range; broadcast is linear in the crew above ~256. The two are a wash
// at 64-256, so "a win at any crew size" would be an overstatement - the win begins where the crew exceeds
// the batch, which is exactly when a wake stops being able to do useful work.
func BenchmarkCache_RefillWake(b *testing.B) {
	const batch = 64
	for _, crew := range []int{64, 256, 808, 2048, 8192} {
		b.Run(fmt.Sprintf("crew=%d", crew), func(b *testing.B) {
			c := &Cache{}
			c.Init(batch) // capacity 2*batch, so a whole batch always fits
			var served, takes atomic.Int64
			var wg sync.WaitGroup
			for range crew {
				wg.Go(func() {
					for {
						shard, ok := c.WaitForWork()
						if !ok {
							return
						}
						takes.Add(1) // one trip round the loop, whether or not it won anything
						if _, ok, _ := c.TryPopFrom(shard); ok {
							served.Add(1)
						}
					}
				})
			}
			jobs := make([]Job, batch)
			for i := range jobs {
				jobs[i] = Job{StepID: i + 1, Shard: 1}
			}
			b.ResetTimer()
			for i := range b.N {
				batchCopy := append([]Job(nil), jobs...)
				c.Refill(1, batchCopy, 0)
				target := int64((i + 1) * batch)
				for served.Load() < target {
					runtime.Gosched()
				}
			}
			b.StopTimer()
			c.Close()
			wg.Wait()
			b.ReportMetric(float64(takes.Load())/float64(served.Load()), "takes/candidate")
		})
	}
}
