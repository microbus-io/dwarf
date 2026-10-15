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
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// jobs returns n candidates for one shard, step ids from first.
func jobs(shard, first, n int) []Job {
	out := make([]Job, n)
	for i := range out {
		out[i] = Job{StepID: first + i, Shard: shard}
	}
	return out
}

// routeNow calls WaitForWork and reports the shard, or 0 if it is still blocked. Every caller expects it to
// return at once, so the bound only turns a regression that blocks forever into a failure.
func routeNow(c *Cache) int {
	got := make(chan int, 1)
	go func() {
		shard, ok := c.WaitForWork()
		if !ok {
			shard = -1
		}
		got <- shard
	}()
	select {
	case shard := <-got:
		return shard
	case <-time.After(5 * time.Second):
		return 0
	}
}

// TestCandidateCache_RoutingSpreadsByUnreservedDepth pins the point of the reservation: workers routed
// before any of them pops spread across same-band partitions in proportion to the work left, rather than all
// choosing the deepest. Without the reservation all five go to shard 1.
func TestCandidateCache_RoutingSpreadsByUnreservedDepth(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)
	c.Refill(1, jobs(1, 1, 3), 10)
	c.Refill(2, jobs(2, 100, 2), 10)

	var got []int
	for range 5 {
		shard, ok := c.WaitForWork()
		assert.True(ok)
		got = append(got, shard)
	}
	// 3:2 -> shard 1; 2:2 tie -> lower shard; 1:2 -> shard 2; 1:1 -> shard 1; 0:1 -> shard 2.
	assert.Equal([]int{1, 1, 2, 1, 2}, got)
}

// TestCandidateCache_FullyReservedParksTheNextWorker pins that a worker finding every candidate spoken for
// parks rather than being routed to a partition it could only queue at, and that a new candidate wakes it.
func TestCandidateCache_FullyReservedParksTheNextWorker(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)
	c.Refill(1, jobs(1, 1, 1), 10)
	shard, _ := c.WaitForWork()
	assert.Equal(1, shard)

	got := make(chan int, 1)
	go func() {
		shard, _ := c.WaitForWork()
		got <- shard
	}()
	assert.True(settle(func() bool { return parked(c) == 1 }), "the second worker must park")
	assert.True(c.Offer(Job{StepID: 50, Shard: 2}, 10))
	select {
	case shard := <-got:
		assert.Equal(2, shard)
	case <-time.After(5 * time.Second):
		t.Fatal("an offered candidate must wake the parked worker")
	}
	c.Close()
}

// TestCandidateCache_PopReleasesTheReservation pins the release on both outcomes of TryPopFrom. A pop that
// finds the partition empty must still release, or the shard reads spoken for and a later candidate there is
// never routed.
func TestCandidateCache_PopReleasesTheReservation(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)

	// Taken: the candidate and the reservation leave together.
	c.Refill(1, jobs(1, 1, 2), 10)
	shard, _ := c.WaitForWork()
	_, ok := c.TryPopFrom(shard)
	assert.True(ok)
	assert.Equal(1, routeNow(c), "the remaining candidate is still routable")
	_, ok = c.TryPopFrom(1)
	assert.True(ok)

	// Lost: the partition emptied while the worker waited; the reservation must go anyway.
	c.Refill(1, jobs(1, 10, 1), 10)
	shard = routeNow(c)
	assert.Equal(1, shard, "the taken candidate's reservation must have been released")
	c.Refill(1, nil, 10)
	_, ok = c.TryPopFrom(shard)
	assert.False(ok)
	c.Refill(1, jobs(1, 20, 1), 10)
	assert.Equal(1, routeNow(c), "a failed pop must release its reservation")
	c.Close()
}

// TestCandidateCache_ReservationsSurviveARefill pins that a refill keeps the count: the workers it counts are
// still on their way and will pop from the new batch, so forgetting them would route more on top of them.
func TestCandidateCache_ReservationsSurviveARefill(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)
	c.Refill(1, jobs(1, 1, 2), 10)
	c.Refill(2, jobs(2, 100, 1), 10)
	for range 2 {
		shard, _ := c.WaitForWork()
		assert.Equal(1, shard)
	}
	c.Refill(1, jobs(1, 10, 2), 10) // two candidates, two workers already on their way
	assert.Equal(2, routeNow(c), "a refill must not make shard 1's reserved candidates routable again")
	c.Close()
}

// TestCandidateCache_FullyReservedBetterBandIsPassedOver pins that a better-banded partition whose every
// candidate already has a worker routed to it does not hold back an extra worker that could serve another
// shard.
func TestCandidateCache_FullyReservedBetterBandIsPassedOver(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)
	c.Refill(1, jobs(1, 1, 1), 1)
	c.Refill(2, jobs(2, 100, 1), 5)
	shard, _ := c.WaitForWork()
	assert.Equal(1, shard, "the better band first")
	shard, _ = c.WaitForWork()
	assert.Equal(2, shard, "then the worse band elsewhere, not a second worker for shard 1's one candidate")
}

// TestCandidateCache_ShrunkPartitionRecoversThroughFailedPops pins the recovery from a refill that shrinks a
// partition below its reservations: it routes nobody while oversubscribed, and the queued workers' failed pops
// bring it back.
func TestCandidateCache_ShrunkPartitionRecoversThroughFailedPops(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)
	c.Refill(1, jobs(1, 1, 3), 10)
	for range 3 {
		c.WaitForWork()
	}
	c.Refill(1, jobs(1, 10, 1), 10) // one candidate, three workers on their way
	c.Refill(2, jobs(2, 100, 1), 10)
	assert.Equal(2, routeNow(c), "the oversubscribed shard routes nobody")

	for range 3 {
		c.TryPopFrom(1) // the first takes the candidate, the other two find it empty
	}
	c.Refill(1, jobs(1, 20, 1), 10)
	assert.Equal(1, routeNow(c), "the failed pops released their reservations")
	c.Close()
}

// TestCandidateCache_RerouteMovesTheReservation pins Reroute: the reservation leaves the shard the worker could
// not start on and lands on the best other routable one, skipping shards already tried; with nowhere else to
// go it stays put.
func TestCandidateCache_RerouteMovesTheReservation(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)
	c.Refill(1, jobs(1, 1, 1), 10)
	c.Refill(2, jobs(2, 100, 1), 10)
	c.Refill(3, jobs(3, 200, 1), 10)

	shard, _ := c.WaitForWork()
	assert.Equal(1, shard)
	next, ok := c.Reroute(1, nil)
	assert.True(ok)
	assert.Equal(2, next, "the best other routable shard")
	next, ok = c.Reroute(2, []int{1})
	assert.True(ok)
	assert.Equal(3, next, "a shard already tried is skipped")
	_, ok = c.Reroute(3, []int{1, 2})
	assert.False(ok, "nowhere left to go")

	// The reservation sits on shard 3 alone, so shards 1 and 2 are routable again and 3 is not.
	assert.Equal(1, routeNow(c))
	assert.Equal(2, routeNow(c))
	c.Close()
}

// TestCandidateCache_RerouteLeavesTheCallersSliceAlone pins that Reroute does not write into spare capacity of
// the slice it is handed - the crew passes a slice of a fixed array it keeps filling.
func TestCandidateCache_RerouteLeavesTheCallersSliceAlone(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)
	c.Refill(1, jobs(1, 1, 1), 10)
	c.Refill(2, jobs(2, 100, 1), 10)
	shard, _ := c.WaitForWork()
	var tried [4]int
	c.Reroute(shard, tried[:0])
	assert.Equal([4]int{}, tried)
	c.Close()
}

// TestCandidateCache_MoveReservation pins the move back: the reservation leaves one shard and lands on the
// other, so the routing of later workers sees it where it now is.
func TestCandidateCache_MoveReservation(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	c := &Cache{}
	c.Init(64)
	c.Refill(1, jobs(1, 1, 1), 10)
	c.Refill(2, jobs(2, 100, 1), 10)
	shard, _ := c.WaitForWork()
	assert.Equal(1, shard)
	next, _ := c.Reroute(1, nil)
	assert.Equal(2, next)
	c.MoveReservation(2, 1)
	assert.Equal(2, routeNow(c), "shard 1's one candidate is reserved again, shard 2's is free")
	c.Close()
}
