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

package engine

import (
	"math"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

func TestCandidateCache_InitLowWater(t *testing.T) {
	assert := testarossa.For(t)

	var c1 candidateCache
	c1.init(1)
	assert.Expect(c1.size, 2)
	assert.Expect(c1.capacity(), 2)
	assert.Expect(c1.lowWater, 1)
	assert.Expect(c1.floor, math.MaxInt)

	var c8 candidateCache
	c8.init(8)
	assert.Expect(c8.size, 16)
	assert.Expect(c8.lowWater, 8)

	var c0 candidateCache
	c0.init(0)
	assert.Expect(c0.size, 1)
	assert.Expect(c0.lowWater, 1)
}

func TestCandidateCache_RefillPopFIFOAndFloorReset(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(4)
	c.refill([]job{{stepID: 1, shard: 0}, {stepID: 2, shard: 1}, {stepID: 3, shard: 0}}, 5)
	assert.Expect(c.len(), 3)
	assert.Expect(c.floor, 5)

	j, ok, _ := c.pop()
	assert.True(ok)
	assert.Expect(j, job{stepID: 1, shard: 0})
	j, ok, _ = c.pop()
	assert.True(ok)
	assert.Expect(j, job{stepID: 2, shard: 1})
	j, ok, _ = c.pop()
	assert.True(ok)
	assert.Expect(j, job{stepID: 3, shard: 0})

	assert.Expect(c.len(), 0)
	assert.Expect(c.floor, math.MaxInt)
}

func TestCandidateCache_NeedRefillAtLowWater(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(4) // size 8, lowWater 4
	c.refill([]job{
		{stepID: 1}, {stepID: 2}, {stepID: 3}, {stepID: 4},
		{stepID: 5}, {stepID: 6}, {stepID: 7}, {stepID: 8},
	}, 5)

	_, _, need := c.pop() // 7 remain, 7 > 4
	assert.False(need)
	_, _, need = c.pop() // 6 remain
	assert.False(need)
	_, _, need = c.pop() // 5 remain
	assert.False(need)
	_, _, need = c.pop() // 4 remain, 4 <= 4
	assert.True(need)
	_, _, need = c.pop() // 3 remain
	assert.True(need)
}

func TestCandidateCache_PopBlocksThenRefillWakes(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(2)
	done := make(chan job, 1)
	go func() {
		j, _, _ := c.pop()
		done <- j
	}()

	select {
	case <-done:
		t.Fatal("pop returned before any refill")
	case <-time.After(100 * time.Millisecond):
	}

	c.refill([]job{{stepID: 99, shard: 2}}, 7)
	select {
	case j := <-done:
		assert.Expect(j, job{stepID: 99, shard: 2})
	case <-time.After(2 * time.Second):
		t.Fatal("pop did not wake after refill")
	}
}

func TestCandidateCache_CloseUnblocksBlockedPop(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(2)
	done := make(chan bool, 1)
	go func() {
		_, ok, _ := c.pop()
		done <- ok
	}()
	select {
	case <-done:
		t.Fatal("pop returned before close")
	case <-time.After(100 * time.Millisecond):
	}

	c.close()
	select {
	case ok := <-done:
		assert.False(ok)
	case <-time.After(2 * time.Second):
		t.Fatal("close did not unblock pop")
	}
}

func TestCandidateCache_OfferEmptyWakesIdleNoInsert(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(4)
	assert.True(c.offer(job{stepID: 7, shard: 1}, 5))
	assert.Expect(c.len(), 0)
	assert.Expect(c.floor, math.MaxInt)
}

func TestCandidateCache_OfferPriorityJumpNoFlush(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(4)
	c.refill([]job{{stepID: 1}, {stepID: 2}}, 5)

	assert.False(c.offer(job{stepID: 8}, 7))
	assert.False(c.offer(job{stepID: 9}, 5))
	assert.Expect(c.len(), 2)

	assert.True(c.offer(job{stepID: 99, shard: 3}, 3))
	assert.Expect(c.len(), 3)
	assert.Expect(c.floor, 3)
	j, _, _ := c.pop()
	assert.Expect(j, job{stepID: 99, shard: 3})
}

func TestCandidateCache_OfferBoundsToSize(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(1) // size 2
	c.refill([]job{{stepID: 1}, {stepID: 2}}, 5)
	assert.True(c.offer(job{stepID: 99}, 1))
	assert.Expect(c.len(), 2)
	j, _, _ := c.pop()
	assert.Expect(j, job{stepID: 99})
	j, _, _ = c.pop()
	assert.Expect(j, job{stepID: 1})
}

func TestCandidateCache_OfferClosedIsNoop(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(2)
	c.close()
	assert.False(c.offer(job{stepID: 1}, 0))
}

func TestCandidateCache_RefillEmptyOrClosedIsNoop(t *testing.T) {
	assert := testarossa.For(t)

	var c candidateCache
	c.init(2)

	c.refill(nil, 5)
	assert.Expect(c.len(), 0)
	assert.Expect(c.floor, math.MaxInt)

	c.close()
	c.refill([]job{{stepID: 1}}, 1)
	assert.Expect(c.len(), 0)
}

func TestCandidateCache_CloseZeroValueDoesNotPanic(t *testing.T) {
	var c candidateCache
	c.close()
}
