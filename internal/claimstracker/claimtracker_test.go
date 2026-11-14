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

package claimstracker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/testarossa"
)

// clockAt builds a tracker whose clock the test drives by hand, anchored at the start of a generation.
func clockAt(base time.Time) (*Tracker, *atomic.Int64) {
	var offset atomic.Int64
	t := &Tracker{
		curr: map[key]bool{},
		prev: map[key]bool{},
		now:  func() time.Time { return base.Add(time.Duration(offset.Load())) },
	}
	t.stamp = generationOf(t.now())
	return t, &offset
}

func TestTryClaim_SecondCallerIsTurnedAway(t *testing.T) {
	assert := testarossa.For(t)
	tr := New()
	assert.True(tr.TryClaim(1, 42), "first reservation must be granted")
	assert.False(tr.TryClaim(1, 42), "a sibling must be turned away while it is held")
}

func TestTryClaim_ShardIsPartOfTheKey(t *testing.T) {
	assert := testarossa.For(t)
	tr := New()
	assert.True(tr.TryClaim(1, 42))
	assert.True(tr.TryClaim(2, 42), "same step id on another shard is a different step")
	assert.False(tr.TryClaim(2, 42))
}

func TestRelinquish_FreesBothGenerations(t *testing.T) {
	assert := testarossa.For(t)
	tr, off := clockAt(time.Unix(1000, 0))

	assert.True(tr.TryClaim(1, 7))
	off.Store(int64(generation)) // roll once: the entry is now in prev
	assert.False(tr.TryClaim(1, 7), "still held from the previous generation")

	tr.RelinquishClaim(1, 7)
	assert.True(tr.TryClaim(1, 7), "relinquish must clear it from prev too")
}

// TestRollWindow pins the lifetime: an entry survives exactly one roll (still seen a generation later) and is
// gone after two. This is the whole safety argument - a reservation can only ever delay a re-claim by a
// bounded window, never strand a step.
func TestRollWindow(t *testing.T) {
	assert := testarossa.For(t)
	tr, off := clockAt(time.Unix(1000, 0))
	assert.True(tr.TryClaim(1, 7))

	off.Store(int64(generation))
	assert.False(tr.TryClaim(1, 7), "still covered a generation later (lives in prev)")

	off.Store(int64(2 * generation))
	assert.True(tr.TryClaim(1, 8), "an unrelated claim rolls the maps again")
	// 7 was inserted in g0; in g1 it moved to prev; the g2 roll dropped that prev.
	assert.True(tr.TryClaim(1, 7), "gone two generations later")
}

// TestRollWindow_TwoToFourSeconds pins the window in seconds rather than generations: a reservation must
// outlast a slow supply cycle, and a refiller fetch measured at ~0.5 s with longer tails let duplicates outlive
// a one-to-two-second window and lose their claim. An entry taken as a generation ends still covers two seconds;
// one taken as it starts is gone at four.
func TestRollWindow_TwoToFourSeconds(t *testing.T) {
	assert := testarossa.For(t)
	tr, off := clockAt(time.Unix(1000, 0))

	off.Store(int64(1900 * time.Millisecond))
	assert.True(tr.TryClaim(1, 7), "taken late in its generation")
	off.Store(int64(3800 * time.Millisecond))
	assert.False(tr.TryClaim(1, 7), "still covered 1.9 s later")

	tr, off = clockAt(time.Unix(1000, 0))
	assert.True(tr.TryClaim(1, 8), "taken as its generation starts")
	off.Store(int64(3900 * time.Millisecond))
	assert.False(tr.TryClaim(1, 8), "still covered 3.9 s later")
	off.Store(int64(4 * time.Second))
	assert.True(tr.TryClaim(1, 8), "gone at 4 s")
}

func TestRoll_WithinSameGenerationDoesNotExpire(t *testing.T) {
	assert := testarossa.For(t)
	tr, off := clockAt(time.Unix(1000, 0))
	assert.True(tr.TryClaim(1, 7))
	off.Store(int64(generation - 100*time.Millisecond)) // same generation
	assert.False(tr.TryClaim(1, 7), "an advance within the generation must not expire the entry")
}

// TestRoll_LargeJumpClearsBoth covers a gap of more than one generation and a backwards clock - both must
// leave the maps clean rather than rotate a stale generation into coverage.
func TestRoll_LargeJumpClearsBoth(t *testing.T) {
	assert := testarossa.For(t)
	tr, off := clockAt(time.Unix(1000, 0))
	assert.True(tr.TryClaim(1, 7))

	off.Store(int64(5 * generation / 2))
	assert.True(tr.TryClaim(1, 7), "a gap past the next generation clears both")

	// Backwards clock: also treated as stale, cleared, still functional.
	assert.True(tr.TryClaim(1, 8))
	off.Store(int64(-3 * time.Second))
	assert.True(tr.TryClaim(1, 8), "a backwards clock clears rather than wedging")
}

// TestConcurrent is a race-detector exercise: many goroutines claim/relinquish overlapping keys while the
// clock advances, asserting only that it never panics and stays internally consistent.
func TestConcurrent(t *testing.T) {
	tr, off := clockAt(time.Unix(1000, 0))
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 2000 {
				k := (g*7 + i) % 64
				if tr.TryClaim(1, k) {
					tr.RelinquishClaim(1, k)
				}
				if i%128 == 0 {
					off.Add(int64(generation))
				}
			}
		}()
	}
	wg.Wait()
}
