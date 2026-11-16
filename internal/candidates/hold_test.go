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

// drain pops every candidate the cache will hand out without blocking, in order.
func drain(c *Cache, shard int) []int {
	var ids []int
	for {
		j, ok := c.TryPopFrom(shard)
		if !ok {
			return ids
		}
		ids = append(ids, j.StepID)
	}
}

// TestCandidateCache_HeldLineageIsNotHandedOut pins the hold's one rule: a held lineage's candidates are never
// taken, those already cached and those a later Refill brings alike, while every other candidate still is.
func TestCandidateCache_HeldLineageIsNotHandedOut(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.Refill(1, at(5, []Job{{StepID: 1, Shard: 1, LineageID: 7}, {StepID: 2, Shard: 1}, {StepID: 3, Shard: 1, LineageID: 7}}), 5)
	c.HoldLineage(1, 7)
	assert.Expect(drain(&c, 1), []int{2})
	assert.Equal(2, c.Len(), "held candidates are still cached")

	c.Refill(1, at(5, []Job{{StepID: 4, Shard: 1, LineageID: 7}, {StepID: 5, Shard: 1, LineageID: 8}}), 5)
	assert.Equal([]int{5}, drain(&c, 1), "a Refill files the held lineage's candidates away")

	_, routed := c.PeekShard()
	assert.False(routed, "a held candidate routes nobody")
}

// TestCandidateCache_ReleaseMergesInBandOrder pins where released candidates go: back in band order, each behind
// what is already cached at its band, so the release reorders nothing the plan chose.
func TestCandidateCache_ReleaseMergesInBandOrder(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.Refill(1, []Job{
		{StepID: 1, Shard: 1, Priority: 1, LineageID: 7},
		{StepID: 2, Shard: 1, Priority: 3},
		{StepID: 3, Shard: 1, Priority: 3, LineageID: 7},
		{StepID: 4, Shard: 1, Priority: 5},
	}, 5)
	c.HoldLineage(1, 7)
	c.ReleaseLineage(1, 7)
	assert.Expect(drain(&c, 1), []int{1, 2, 3, 4})

	c.Refill(1, []Job{
		{StepID: 1, Shard: 1, Priority: 3, LineageID: 7},
		{StepID: 2, Shard: 1, Priority: 3},
	}, 5)
	c.HoldLineage(1, 7)
	c.ReleaseLineage(1, 7)
	assert.Equal([]int{2, 1}, drain(&c, 1), "a released candidate goes behind its band")
}

// TestCandidateCache_OfferDeclinesAHeldLineage pins the doorbell's side: an Offer of a held lineage's step is
// declined, so it waits for a Refill after the release rather than sitting cached where no worker may take it.
func TestCandidateCache_OfferDeclinesAHeldLineage(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.HoldLineage(1, 7)
	assert.False(c.Offer(Job{StepID: 1, Shard: 1, LineageID: 7}, 5))
	assert.True(c.Offer(Job{StepID: 2, Shard: 1, LineageID: 8}, 5))
	c.ReleaseLineage(1, 7)
	assert.True(c.Offer(Job{StepID: 3, Shard: 1, LineageID: 7}, 5))
	assert.Expect(drain(&c, 1), []int{2, 3})
}

// TestCandidateCache_LineageZeroIsNeverHeld pins that a step outside every cohort cannot be held back by asking.
func TestCandidateCache_LineageZeroIsNeverHeld(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.Refill(1, at(5, []Job{{StepID: 1, Shard: 1}}), 5)
	c.HoldLineage(1, 0)
	assert.Expect(drain(&c, 1), []int{1})
}

// TestCandidateCache_ReleaseWakesAWaiter pins the wake: a worker parked because everything cached was held is
// woken by the release, as it would be by a Refill. Without it the worker sleeps on work it could take until the
// next Refill or Offer.
func TestCandidateCache_ReleaseWakesAWaiter(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.Refill(1, at(5, []Job{{StepID: 1, Shard: 1, LineageID: 7}}), 5)
	c.HoldLineage(1, 7)
	routed := make(chan int, 1)
	go func() {
		shard, ok := c.WaitForWork()
		if ok {
			routed <- shard
		}
	}()
	select {
	case <-routed:
		assert.True(false, "routed to a held candidate")
		return
	case <-time.After(100 * time.Millisecond):
	}
	c.ReleaseLineage(1, 7)
	select {
	case shard := <-routed:
		assert.Expect(shard, 1)
		j, ok := c.TryPopFrom(shard)
		assert.True(ok)
		assert.Expect(j.StepID, 1)
	case <-time.After(2 * time.Second):
		assert.True(false, "the release woke nobody")
	}
	c.Close()
}

// TestCandidateCache_HeldCandidatesCountAsDiscarded pins the refiller's waste signal: a held candidate the next
// Refill replaces was selected and never taken, so it is counted like any other.
func TestCandidateCache_HeldCandidatesCountAsDiscarded(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.Refill(1, at(5, []Job{{StepID: 1, Shard: 1, LineageID: 7}, {StepID: 2, Shard: 1, LineageID: 7}, {StepID: 3, Shard: 1}}), 5)
	c.HoldLineage(1, 7)
	assert.Expect(c.Refill(1, nil, 5), 3)
}
