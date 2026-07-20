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

func TestCandidateCache_InitSizing(t *testing.T) {
	assert := testarossa.For(t)

	var c1 Cache
	c1.Init(1)
	assert.Expect(c1.size, 2)
	assert.Expect(c1.Capacity(), 2)

	var c8 Cache
	c8.Init(8)
	assert.Expect(c8.size, 16)

	var c0 Cache
	c0.Init(0)
	assert.Expect(c0.size, 1)
}

func TestCandidateCache_RefillPopFIFOAndFloorReset(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)
	c.Refill(1, []Job{{StepID: 1, Shard: 1}, {StepID: 2, Shard: 1}, {StepID: 3, Shard: 1}}, 5)
	assert.Expect(c.Len(), 3)
	assert.Expect(c.parts[1].floor(), 5)

	j, ok, _ := c.Pop()
	assert.True(ok)
	assert.Expect(j, Job{StepID: 1, Shard: 1, Priority: 5})
	j, ok, _ = c.Pop()
	assert.True(ok)
	assert.Expect(j, Job{StepID: 2, Shard: 1, Priority: 5})
	j, ok, _ = c.Pop()
	assert.True(ok)
	assert.Expect(j, Job{StepID: 3, Shard: 1, Priority: 5})

	assert.Expect(c.Len(), 0)
	assert.Expect(c.parts[1].floor(), math.MaxInt)
}

func TestCandidateCache_NeedRefillAtLowWater(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4) // capacity 8
	batch := make([]Job, 8)
	for i := range batch {
		batch[i] = Job{StepID: i + 1, Shard: 1}
	}
	c.Refill(1, batch, 5) // lastFill 8, low water 4

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

// TestCandidateCache_LowWaterIsPerPartition pins that the low-water mark follows each partition's own
// last-refill depth, not a global constant: partition sizes are dynamic (each refiller's slice of the
// global plan), so a global threshold would over-trigger small partitions and under-trigger large ones.
func TestCandidateCache_LowWaterIsPerPartition(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8) // capacity 16
	big := make([]Job, 8)
	for i := range big {
		big[i] = Job{StepID: 100 + i, Shard: 1}
	}
	c.Refill(1, big, 5) // low water 4
	small := []Job{{StepID: 201, Shard: 2}, {StepID: 202, Shard: 2}, {StepID: 203, Shard: 2}, {StepID: 204, Shard: 2}}
	c.Refill(2, small, 3) // low water 2

	// Shard 2 has the better floor, so its candidates pop first; its refill request fires when IT
	// drains to its own low water (4 -> 2), regardless of shard 1's depth.
	j, _, need := c.Pop()
	assert.Expect(j.Shard, 2)
	assert.False(need, "3 remain > low water 2")
	j, _, need = c.Pop()
	assert.Expect(j.Shard, 2)
	assert.True(need, "2 remain <= low water 2")
	_ = j
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

	c.Refill(2, []Job{{StepID: 99, Shard: 2}}, 7)
	select {
	case j := <-done:
		assert.Expect(j, Job{StepID: 99, Shard: 2, Priority: 7})
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

// TestCandidateCache_PopPicksLowestFloorPartition pins partition selection: lowest floor wins - that
// is the entire dispatch rule - with ties broken by depth (deepest first, tracking where the work is)
// and then by shard index for determinism.
func TestCandidateCache_PopPicksLowestFloorPartition(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.Refill(1, []Job{{StepID: 11, Shard: 1}, {StepID: 12, Shard: 1}}, 5)
	c.Refill(2, []Job{{StepID: 21, Shard: 2}}, 3)

	// Shard 2's floor (3) beats shard 1's (5) despite shard 1 being deeper.
	j, _, _ := c.Pop()
	assert.Expect(j, Job{StepID: 21, Shard: 2, Priority: 3})

	// Equal floors: the deeper partition wins.
	c.Refill(2, []Job{{StepID: 22, Shard: 2}}, 5)
	j, _, _ = c.Pop()
	assert.Expect(j, Job{StepID: 11, Shard: 1, Priority: 5})

	// Equal floors, equal depth: the lower shard wins (determinism).
	j, _, _ = c.Pop()
	assert.Expect(j.Shard, 1)
	j, _, _ = c.Pop()
	assert.Expect(j.Shard, 2)
}

// TestCandidateCache_DerivedFloorAfterOfferPop pins the floor-is-derived fix. With a stored floor, an
// Offer head-insert of one band-1 item over a band-5 body left the partition advertising floor=1 after
// the band-1 item popped - so lowest-floor partition selection preferred it and drained its entire
// band-5 body ahead of another partition's band-3 body: up to a full partition of inversion, for up to
// a full cycle. Deriving the floor from the head item retires that: the moment the pioneer pops, the
// partition's floor reverts to its body's real band.
func TestCandidateCache_DerivedFloorAfterOfferPop(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.Refill(1, []Job{{StepID: 11, Shard: 1}, {StepID: 12, Shard: 1}}, 5)
	c.Refill(2, []Job{{StepID: 21, Shard: 2}, {StepID: 22, Shard: 2}}, 3)

	// A band-1 pioneer head-inserts onto shard 1, making it the best partition.
	assert.True(c.Offer(Job{StepID: 99, Shard: 1}, 1))
	j, _, _ := c.Pop()
	assert.Expect(j, Job{StepID: 99, Shard: 1, Priority: 1})

	// The pioneer is gone: shard 1's floor must read 5 again (derived from its head), so shard 2's
	// band-3 body drains BEFORE shard 1's band-5 body.
	assert.Expect(c.parts[1].floor(), 5)
	j, _, _ = c.Pop()
	assert.Expect(j.Shard, 2)
	j, _, _ = c.Pop()
	assert.Expect(j.Shard, 2)
	j, _, _ = c.Pop()
	assert.Expect(j.Shard, 1)
}

func TestCandidateCache_OfferEmptyWakesIdleNoInsert(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)
	assert.True(c.Offer(Job{StepID: 7, Shard: 1}, 5))
	assert.Expect(c.Len(), 0)

	// An empty PARTITION declines the same way even when another partition holds work.
	c.Refill(2, []Job{{StepID: 21, Shard: 2}}, 3)
	assert.True(c.Offer(Job{StepID: 8, Shard: 1}, 5))
	assert.Expect(c.Len(), 1)
}

func TestCandidateCache_OfferPriorityJumpNoFlush(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)
	c.Refill(1, []Job{{StepID: 1, Shard: 1}, {StepID: 2, Shard: 1}}, 5)

	assert.False(c.Offer(Job{StepID: 8, Shard: 1}, 7))
	assert.False(c.Offer(Job{StepID: 9, Shard: 1}, 5))
	assert.Expect(c.Len(), 2)

	assert.True(c.Offer(Job{StepID: 99, Shard: 1}, 3))
	assert.Expect(c.Len(), 3)
	assert.Expect(c.parts[1].floor(), 3)
	j, _, _ := c.Pop()
	assert.Expect(j, Job{StepID: 99, Shard: 1, Priority: 3})
}

// TestCandidateCache_OfferRoutesByShard pins that an Offer lands on ITS OWN shard's partition: the
// admission check runs against that partition's floor, not another's, so a doorbell for shard 2
// neither preempts nor pollutes shard 1's slice.
func TestCandidateCache_OfferRoutesByShard(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8)
	c.Refill(1, []Job{{StepID: 11, Shard: 1}}, 2)
	c.Refill(2, []Job{{StepID: 21, Shard: 2}}, 5)

	// Priority 3 is worse than shard 1's floor (2) but better than shard 2's (5): routed to shard 2,
	// it inserts there.
	assert.True(c.Offer(Job{StepID: 99, Shard: 2}, 3))
	assert.Expect(len(c.parts[2].items), 2)
	assert.Expect(len(c.parts[1].items), 1)
	assert.Expect(c.parts[2].floor(), 3)
}

func TestCandidateCache_OfferBoundsToSize(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(1) // size 2
	c.Refill(1, []Job{{StepID: 1, Shard: 1}, {StepID: 2, Shard: 1}}, 5)
	assert.True(c.Offer(Job{StepID: 99, Shard: 1}, 1))
	assert.Expect(c.Len(), 2)
	j, _, _ := c.Pop()
	assert.Expect(j, Job{StepID: 99, Shard: 1, Priority: 1})
	j, _, _ = c.Pop()
	assert.Expect(j, Job{StepID: 1, Shard: 1, Priority: 5})
}

func TestCandidateCache_OfferClosedIsNoop(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(2)
	c.Close()
	assert.False(c.Offer(Job{StepID: 1, Shard: 1}, 0))
}

// TestCandidateCache_RefillEmptyIsWholesale pins that an EMPTY batch is a real refill, not a no-op: it
// empties the shard's partition. An empty batch is the scan's statement that nothing is due on that
// shard, so every still-cached candidate there is a step that is no longer pending - a dead hint a
// worker would pop and burn a claim-CAS round-trip on. Other shards' partitions are untouched.
func TestCandidateCache_RefillEmptyIsWholesale(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)
	c.Refill(1, []Job{{StepID: 1, Shard: 1}, {StepID: 2, Shard: 1}}, 5)
	c.Refill(2, []Job{{StepID: 21, Shard: 2}}, 5)
	assert.Expect(c.Len(), 3)

	c.Refill(1, nil, math.MaxInt) // shard 1's scan came back empty: nothing is due there
	assert.Expect(c.Len(), 1)
	assert.Expect(c.parts[1].floor(), math.MaxInt)
	assert.Expect(c.parts[2].floor(), 5)

	// An arrival on the emptied partition asks for a refill rather than head-inserting (the refiller
	// picks the strictly-best step); the point here is that it is not weighed against a floor the
	// partition no longer has.
	assert.True(c.Offer(Job{StepID: 9, Shard: 1}, 7))
	assert.Expect(len(c.parts[1].items), 0)
}

// TestCandidateCache_RefillReportsDiscarded pins the count Refill returns: the candidates the
// wholesale replace of ONE partition threw away un-popped. It is the refiller's WASTE signal. The
// discarded steps stay `pending` and are re-selected, so this is cost, never loss; without the count
// there is no way to see how much of the refiller's work is thrown away.
func TestCandidateCache_RefillReportsDiscarded(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(4)

	// Nothing cached yet: nothing to discard.
	assert.Equal(0, c.Refill(1, []Job{{StepID: 1, Shard: 1}, {StepID: 2, Shard: 1}, {StepID: 3, Shard: 1}}, 5))

	// One popped, so the replace discards the two that were left.
	_, ok, _ := c.Pop()
	assert.True(ok)
	assert.Equal(2, c.Refill(1, []Job{{StepID: 4, Shard: 1}}, 5))

	// A replace of a DIFFERENT partition discards nothing of shard 1's.
	assert.Equal(0, c.Refill(2, []Job{{StepID: 21, Shard: 2}}, 5))

	// An empty batch is a real replace, so it discards too.
	assert.Equal(1, c.Refill(1, nil, math.MaxInt))
	assert.Equal(0, c.Refill(1, nil, math.MaxInt))

	// A closed cache discards nothing - the replace is a no-op, so reporting a discard would
	// double-count candidates the drain already abandoned.
	c.Refill(1, []Job{{StepID: 5, Shard: 1}}, 5)
	c.Close()
	assert.Equal(0, c.Refill(1, []Job{{StepID: 6, Shard: 1}}, 5))
}

func TestCandidateCache_RefillClosedIsNoop(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(2)
	c.Close()
	c.Refill(1, []Job{{StepID: 1, Shard: 1}}, 1)
	assert.Expect(c.Len(), 0)
}

func TestCandidateCache_CloseZeroValueDoesNotPanic(t *testing.T) {
	var c Cache
	c.Close()
}

// TestCandidateCache_ResizeTrimsAndRebounds pins the live re-bound the engine performs when the observed
// replica count changes and each shard's connection budget is re-divided. The cache must follow that
// split: it is sized from what the replica can actually CLAIM, and one left at its startup (R=1) size is
// handed far more candidates than it can claim. The trim applies across ALL partitions, proportionally
// to their depth.
func TestCandidateCache_ResizeTrimsAndRebounds(t *testing.T) {
	assert := testarossa.For(t)

	var c Cache
	c.Init(8) // capacity 16
	assert.Equal(16, c.Capacity())

	batch1 := make([]Job, 12)
	for i := range batch1 {
		batch1[i] = Job{StepID: 100 + i, Shard: 1}
	}
	c.Refill(1, batch1, 5)
	batch2 := make([]Job, 4)
	for i := range batch2 {
		batch2[i] = Job{StepID: 200 + i, Shard: 2}
	}
	c.Refill(2, batch2, 5)
	assert.Equal(16, c.Len())

	// Shrink to a 2-worker replica: capacity 4, trimmed proportionally (12:4 -> 3:1). The trimmed
	// candidates stay `pending` in the database and are simply re-selected - the same thing an Offer's
	// head-insert already does when it pushes one past the bound.
	c.Resize(2)
	assert.Equal(4, c.Capacity())
	assert.Equal(4, c.Len())
	assert.Equal(3, len(c.parts[1].items))
	assert.Equal(1, len(c.parts[2].items))
	// Each partition's head is kept: it is the strictly-best work its refiller selected.
	assert.Equal(100, c.parts[1].items[0].StepID)
	assert.Equal(200, c.parts[2].items[0].StepID)

	// Growing back re-bounds without discarding anything.
	c.Resize(8)
	assert.Equal(16, c.Capacity())
	assert.Equal(4, c.Len())

	// Resize after Close is inert, like Offer/Refill.
	c.Close()
	c.Resize(64)
	assert.Equal(16, c.Capacity())
}
