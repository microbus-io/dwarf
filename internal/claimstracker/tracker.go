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

// Package claimstracker records which steps a replica has a claim CAS in flight on, so a sibling worker
// that pops the same candidate can skip it instead of paying a round trip to lose the CAS.
//
//	if !tracker.TryClaim(shard, stepID) {
//		continue // a sibling worker in this replica is already claiming it
//	}
//
// It is strictly ADVISORY: the claim CAS remains the only thing that grants a step, so a missed or stale
// entry costs a wasted round trip or a deferred dispatch, never correctness. It is safe for concurrent
// use.
//
// A reservation EXPIRES ON ITS OWN, between one and two seconds after it was taken - long enough to
// outlast the interval between candidate scans, far short of the lease margin. Nothing has to release
// one, and RelinquishClaim exists only for a caller that knows a step is settled sooner than that and
// wants the entry gone now. Any path that re-offers a step to THIS replica must call it, or the replica
// skips its own re-dispatch until the entry ages out.
package claimstracker

import (
	"sync"
	"time"
)

// key identifies one step within a replica. The shard is part of the key, not decoration: step_id is a
// per-shard auto-increment, so every shard has a step 42 and a step-id-only key would report shard 2's
// step 42 as in-flight because shard 1's is. A struct key is hashable and collision-free, unlike packing
// the two ints into one.
type key struct {
	shard  int
	stepID int
}

// Tracker is the two-generation in-flight set. Safe for concurrent use.
type Tracker struct {
	mu    sync.Mutex
	curr  map[key]bool // this second's claims
	prev  map[key]bool // last second's, still checked until the next roll drops them
	stamp int64        // the whole-second the maps currently represent (Unix seconds)
	now   func() time.Time
}

// New returns an empty Tracker anchored at the current second.
func New() *Tracker {
	t := &Tracker{
		curr: map[key]bool{},
		prev: map[key]bool{},
		now:  time.Now,
	}
	t.stamp = t.now().Unix()
	return t
}

// TryClaim reserves the right to run the claim CAS for one step, reporting false when a sibling worker in
// this replica already holds a reservation for it (in either generation). On true the caller owns the
// reservation until it expires with the maps or is explicitly released via RelinquishClaim.
func (t *Tracker) TryClaim(shard, stepID int) bool {
	k := key{shard: shard, stepID: stepID}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.rollMaps()
	if t.curr[k] || t.prev[k] {
		return false
	}
	t.curr[k] = true
	return true
}

// RelinquishClaim drops a reservation from both generations. Nothing on the dispatch path needs to call
// it - reservations expire on their own roll - it exists for a caller that knows a step is settled sooner
// than the window and wants the entry gone now. It does not roll the maps: deleting from a stale
// generation is harmless.
func (t *Tracker) RelinquishClaim(shard, stepID int) {
	k := key{shard: shard, stepID: stepID}
	t.mu.Lock()
	delete(t.curr, k)
	delete(t.prev, k)
	t.mu.Unlock()
}

// rollMaps advances the two generations to the current whole second. Caller holds t.mu.
//
//   - same second as the stamp: nothing to do.
//   - exactly one second later: rotate - current becomes previous (its entries still get one more second
//     of coverage), a fresh current is allocated, the previous is dropped. O(1).
//   - more than one second later, OR the clock stepped backwards (neither same nor +1): both generations
//     are stale, so clear both. A backwards wall clock lands here and clears, which is the safe advisory
//     direction (it only forgets in-flight steps, costing at most a wasted round trip).
func (t *Tracker) rollMaps() {
	nowSec := t.now().Unix()
	switch {
	case nowSec == t.stamp:
		return
	case nowSec == t.stamp+1:
		t.prev = t.curr
		t.curr = map[key]bool{}
	default:
		t.prev = map[key]bool{}
		t.curr = map[key]bool{}
	}
	t.stamp = nowSec
}
