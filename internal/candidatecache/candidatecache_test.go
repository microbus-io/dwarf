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

package candidatecache

import (
	"math"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

func TestCandidateCache_InitLowWater(t *testing.T) {
	assert := testarossa.For(t)

	var c1 Cache
	c1.Init(1)
	assert.Expect(c1.size, 2)
	assert.Expect(c1.Capacity(), 2)
	assert.Expect(c1.lowWater, 1)
	assert.Expect(c1.floor, math.MaxInt)

	var c8 Cache
	c8.Init(8)
	assert.Expect(c8.size, 16)
	assert.Expect(c8.lowWater, 8)

	var c0 Cache
	c0.Init(0)
	assert.Expect(c0.size, 1)
	assert.Expect(c0.lowWater, 1)
}

func TestCandidateCache_RefillPopFIFOAndFloorReset(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)
	c.Refill([]Job{{StepID: 1, Shard: 0}, {StepID: 2, Shard: 1}, {StepID: 3, Shard: 0}}, 5)
	assert.Expect(c.Len(), 3)
	assert.Expect(c.floor, 5)

	j, ok, _ := c.Pop()
	assert.True(ok)
	assert.Expect(j, Job{StepID: 1, Shard: 0})
	j, ok, _ = c.Pop()
	assert.True(ok)
	assert.Expect(j, Job{StepID: 2, Shard: 1})
	j, ok, _ = c.Pop()
	assert.True(ok)
	assert.Expect(j, Job{StepID: 3, Shard: 0})

	assert.Expect(c.Len(), 0)
	assert.Expect(c.floor, math.MaxInt)
}

func TestCandidateCache_NeedRefillAtLowWater(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4) // size 8, lowWater 4
	c.Refill([]Job{
		{StepID: 1}, {StepID: 2}, {StepID: 3}, {StepID: 4},
		{StepID: 5}, {StepID: 6}, {StepID: 7}, {StepID: 8},
	}, 5)

	_, _, need := c.Pop() // 7 remain, 7 > 4
	assert.False(need)
	_, _, need = c.Pop() // 6 remain
	assert.False(need)
	_, _, need = c.Pop() // 5 remain
	assert.False(need)
	_, _, need = c.Pop() // 4 remain, 4 <= 4
	assert.True(need)
	_, _, need = c.Pop() // 3 remain
	assert.True(need)
}

func TestCandidateCache_PopBlocksThenRefillWakes(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(2)
	done := make(chan Job, 1)
	go func() {
		j, _, _ := c.Pop()
		done <- j
	}()

	select {
	case <-done:
		t.Fatal("pop returned before any refill")
	case <-time.After(100 * time.Millisecond):
	}

	c.Refill([]Job{{StepID: 99, Shard: 2}}, 7)
	select {
	case j := <-done:
		assert.Expect(j, Job{StepID: 99, Shard: 2})
	case <-time.After(2 * time.Second):
		t.Fatal("pop did not wake after refill")
	}
}

func TestCandidateCache_CloseUnblocksBlockedPop(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(2)
	done := make(chan bool, 1)
	go func() {
		_, ok, _ := c.Pop()
		done <- ok
	}()
	select {
	case <-done:
		t.Fatal("pop returned before close")
	case <-time.After(100 * time.Millisecond):
	}

	c.Close()
	select {
	case ok := <-done:
		assert.False(ok)
	case <-time.After(2 * time.Second):
		t.Fatal("close did not unblock pop")
	}
}

func TestCandidateCache_OfferEmptyWakesIdleNoInsert(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)
	assert.True(c.Offer(Job{StepID: 7, Shard: 1}, 5))
	assert.Expect(c.Len(), 0)
	assert.Expect(c.floor, math.MaxInt)
}

func TestCandidateCache_OfferPriorityJumpNoFlush(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)
	c.Refill([]Job{{StepID: 1}, {StepID: 2}}, 5)

	assert.False(c.Offer(Job{StepID: 8}, 7))
	assert.False(c.Offer(Job{StepID: 9}, 5))
	assert.Expect(c.Len(), 2)

	assert.True(c.Offer(Job{StepID: 99, Shard: 3}, 3))
	assert.Expect(c.Len(), 3)
	assert.Expect(c.floor, 3)
	j, _, _ := c.Pop()
	assert.Expect(j, Job{StepID: 99, Shard: 3})
}

func TestCandidateCache_OfferBoundsToSize(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(1) // size 2
	c.Refill([]Job{{StepID: 1}, {StepID: 2}}, 5)
	assert.True(c.Offer(Job{StepID: 99}, 1))
	assert.Expect(c.Len(), 2)
	j, _, _ := c.Pop()
	assert.Expect(j, Job{StepID: 99})
	j, _, _ = c.Pop()
	assert.Expect(j, Job{StepID: 1})
}

func TestCandidateCache_OfferClosedIsNoop(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(2)
	c.Close()
	assert.False(c.Offer(Job{StepID: 1}, 0))
}

// TestCandidateCache_RefillEmptyIsWholesale pins that an EMPTY batch is a real refill, not a no-op: it
// empties the cache and resets the floor. An empty batch is the scan's statement that nothing is due, so
// every still-cached candidate is a step that is no longer pending - a dead hint a worker would pop and
// burn a claim-CAS round-trip on. Skipping the empty case (what Refill used to do) kept the whole dead
// batch and left the floor advertising a band the cache no longer held.
func TestCandidateCache_RefillEmptyIsWholesale(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)
	c.Refill([]Job{{StepID: 1}, {StepID: 2}}, 5)
	assert.Expect(c.Len(), 2)
	assert.Expect(c.floor, 5)

	c.Refill(nil, math.MaxInt) // the scan came back empty: nothing is due
	assert.Expect(c.Len(), 0)
	assert.Expect(c.floor, math.MaxInt)

	// An arrival on an empty cache asks for a refill rather than head-inserting (the refiller picks the
	// strictly-best step); the point here is that it is not weighed against a floor the cache no longer has.
	assert.True(c.Offer(Job{StepID: 9}, 7))
	assert.Expect(c.Len(), 0)
}

// TestCandidateCache_RefillReportsDiscarded pins the count Refill returns: the candidates a wholesale
// replace threw away un-popped. It is the refiller's WASTE signal - the refiller is triggered after every
// processStep and turns far faster than the workers drain, so under load it routinely replaces a batch it
// just paid a round-trip to fetch. The discarded steps stay `pending` and are re-selected, so this is cost,
// never loss; without the count there is no way to see how much of the refiller's work is thrown away.
func TestCandidateCache_RefillReportsDiscarded(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)

	// Nothing cached yet: nothing to discard.
	assert.Equal(0, c.Refill([]Job{{StepID: 1}, {StepID: 2}, {StepID: 3}}, 5))

	// One popped, so the replace discards the two that were left.
	_, ok, _ := c.Pop()
	assert.True(ok)
	assert.Equal(2, c.Refill([]Job{{StepID: 4}}, 5))

	// An empty batch is a real replace, so it discards too.
	assert.Equal(1, c.Refill(nil, math.MaxInt))
	assert.Equal(0, c.Refill(nil, math.MaxInt))

	// A closed cache discards nothing - the replace is a no-op, so reporting a discard would double-count
	// candidates the drain already abandoned.
	c.Refill([]Job{{StepID: 5}}, 5)
	c.Close()
	assert.Equal(0, c.Refill([]Job{{StepID: 6}}, 5))
}

func TestCandidateCache_RefillClosedIsNoop(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(2)
	c.Close()
	c.Refill([]Job{{StepID: 1}}, 1)
	assert.Expect(c.Len(), 0)
}

func TestCandidateCache_CloseZeroValueDoesNotPanic(t *testing.T) {
	var c Cache
	c.Close()
}

// TestCandidateCache_ResizeTrimsAndRebounds pins the live re-bound the engine performs when the observed replica
// count changes and each shard's connection budget is re-divided. The cache must follow that split: it is sized
// from what the replica can actually CLAIM, and one left at its startup (R=1) size is handed far more candidates
// than it can claim.
func TestCandidateCache_ResizeTrimsAndRebounds(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8) // capacity 16, lowWater 8
	assert.Equal(16, c.Capacity())

	batch := make([]Job, 16)
	for i := range batch {
		batch[i] = Job{StepID: i + 1, Shard: 1}
	}
	c.Refill(batch, 5)
	assert.Equal(16, c.Len())

	// Shrink to a 2-worker replica: capacity 4, and the overflowing tail is trimmed. The trimmed candidates stay
	// `pending` in the database and are simply re-selected - the same thing an Offer's head-insert already does
	// when it pushes one past the bound.
	c.Resize(2)
	assert.Equal(4, c.Capacity())
	assert.Equal(4, c.Len())
	// The head is kept: it is the strictly-best work the refiller selected.
	j, ok, _ := c.Pop()
	assert.True(ok)
	assert.Equal(1, j.StepID)

	// Growing back re-bounds without discarding anything.
	c.Resize(8)
	assert.Equal(16, c.Capacity())
	assert.Equal(3, c.Len())

	// Resize after Close is inert, like Offer/Refill.
	c.Close()
	c.Resize(64)
	assert.Equal(16, c.Capacity())
}
