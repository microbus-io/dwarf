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

// clockAt builds a tracker whose clock the test drives by hand, anchored at an arbitrary whole second.
func clockAt(base time.Time) (*Tracker, *atomic.Int64) {
	var offset atomic.Int64
	t := &Tracker{
		curr: map[key]bool{},
		prev: map[key]bool{},
		now:  func() time.Time { return base.Add(time.Duration(offset.Load())) },
	}
	t.stamp = t.now().Unix()
	return t, &offset
}

func TestTryClaim_SecondCallerIsTurnedAway(t *testing.T) {
	tr := New()
	testarossa.True(t, tr.TryClaim(1, 42), "first reservation must be granted")
	testarossa.False(t, tr.TryClaim(1, 42), "a sibling must be turned away while it is held")
}

func TestTryClaim_ShardIsPartOfTheKey(t *testing.T) {
	tr := New()
	testarossa.True(t, tr.TryClaim(1, 42))
	testarossa.True(t, tr.TryClaim(2, 42), "same step id on another shard is a different step")
	testarossa.False(t, tr.TryClaim(2, 42))
}

func TestRelinquish_FreesBothGenerations(t *testing.T) {
	tr, off := clockAt(time.Unix(1000, 0))

	testarossa.True(t, tr.TryClaim(1, 7))
	off.Store(int64(time.Second)) // roll once: the entry is now in prev
	testarossa.False(t, tr.TryClaim(1, 7), "still held from the previous generation")

	tr.RelinquishClaim(1, 7)
	testarossa.True(t, tr.TryClaim(1, 7), "relinquish must clear it from prev too")
}

// TestRollWindow pins the lifetime: an entry survives exactly one roll (still seen after +1s) and is gone
// after two (dropped at +2s). This is the whole safety argument - a reservation can only ever delay a
// re-claim by a bounded window, never strand a step.
func TestRollWindow(t *testing.T) {
	tr, off := clockAt(time.Unix(1000, 0))
	testarossa.True(t, tr.TryClaim(1, 7))

	off.Store(int64(time.Second))
	testarossa.False(t, tr.TryClaim(1, 7), "still covered one second later (lives in prev)")

	off.Store(int64(2 * time.Second))
	testarossa.True(t, tr.TryClaim(1, 8), "an unrelated claim rolls the maps again")
	// 7 was inserted at t0; at t0+1 it moved to prev; the t0+2 roll dropped that prev.
	testarossa.True(t, tr.TryClaim(1, 7), "gone two seconds later")
}

func TestRoll_WithinSameSecondDoesNotExpire(t *testing.T) {
	tr, off := clockAt(time.Unix(1000, 0))
	testarossa.True(t, tr.TryClaim(1, 7))
	off.Store(int64(900 * time.Millisecond)) // same whole second
	testarossa.False(t, tr.TryClaim(1, 7), "a sub-second advance must not expire the entry")
}

// TestRoll_LargeJumpClearsBoth covers a gap of more than one second and a backwards clock - both must
// leave the maps clean rather than rotate a stale generation into coverage.
func TestRoll_LargeJumpClearsBoth(t *testing.T) {
	tr, off := clockAt(time.Unix(1000, 0))
	testarossa.True(t, tr.TryClaim(1, 7))

	off.Store(int64(5 * time.Second))
	testarossa.True(t, tr.TryClaim(1, 7), "a multi-second gap clears both generations")

	// Backwards clock: also treated as stale, cleared, still functional.
	testarossa.True(t, tr.TryClaim(1, 8))
	off.Store(int64(-3 * time.Second))
	testarossa.True(t, tr.TryClaim(1, 8), "a backwards clock clears rather than wedging")
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
					off.Add(int64(time.Second))
				}
			}
		}()
	}
	wg.Wait()
}
