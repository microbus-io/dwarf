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

package planner

import (
	"math"
	"testing"

	"github.com/microbus-io/testarossa"
)

// mkEntry fabricates one shard's tally for the merge/slice unit tests, which exercise those functions
// directly rather than through a Planner.
func mkEntry(shard, band int, tallies ...Tally) entry {
	st := &shardTally{band: band, tallies: tallies}
	if len(tallies) > 0 {
		st.byKey = make(map[string]int, len(tallies))
		for i, t := range tallies {
			st.byKey[t.Key] = i
		}
	}
	return entry{shard: shard, t: st}
}

// TestPlanner_MergeAcrossShards pins the merge: the global minimum band; per-key counts summed across
// shards; the globally-oldest step's weight winning a key's weight (so a tenant cannot self-promote
// by queueing newer high-weight work); and worse-band shards contributing nothing.
func TestPlanner_MergeAcrossShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	entries := []entry{
		mkEntry(1, 3,
			Tally{Key: "a", Weight: 2, AgeMs: 500, Count: 3},
			Tally{Key: "b", Weight: 1, AgeMs: 100, Count: 1},
		),
		mkEntry(2, 3,
			Tally{Key: "a", Weight: 7, AgeMs: 900, Count: 6}, // older: its weight wins
		),
		mkEntry(3, 9, Tally{Key: "z", Weight: 1, AgeMs: 1, Count: 1}), // worse band
		mkEntry(4, math.MaxInt), // nothing due
	}
	band, keys := merge(entries)
	assert.Equal(3, band)
	assert.Equal(2, len(keys))
	assert.Equal("a", keys[0].key)
	assert.Equal(9, keys[0].count, "counts sum across shards")
	assert.Equal(7.0, keys[0].weight, "the globally-oldest step's weight wins the merge")
	assert.Equal("b", keys[1].key)

	// An all-empty fleet has no band.
	band, keys = merge([]entry{mkEntry(1, math.MaxInt)})
	assert.Equal(math.MaxInt, band)
	assert.Equal(0, len(keys))
}

// TestPlanner_SliceFirstSlotToOldestThenProportional pins the slice rule: a key's first slot goes to
// the shard holding its OLDEST step, the rest split proportional to per-shard counts with
// deterministic largest-remainder rounding, and shards at a worse band win nothing regardless of how
// much backlog they hold.
func TestPlanner_SliceFirstSlotToOldestThenProportional(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	entries := []entry{
		mkEntry(1, 3, Tally{Key: "t", Weight: 1, AgeMs: 500, Count: 3}),
		mkEntry(2, 3, Tally{Key: "t", Weight: 1, AgeMs: 900, Count: 6}),
		mkEntry(3, 5, Tally{Key: "t", Weight: 1, AgeMs: 9999, Count: 50}), // worse band: excluded
	}
	order := []string{"t", "t", "t", "t"}

	// Shard 2 holds the oldest step at the band, so it takes the first slot; the remaining 3 split
	// over avail (shard 1: 3, shard 2: 5) by largest remainder -> shard 1: 1, shard 2: 2 more.
	s1, k1, cap1 := slice(order, entries, 3, 1)
	s2, _, cap2 := slice(order, entries, 3, 2)
	s3, k3, cap3 := slice(order, entries, 3, 3)

	assert.Equal(1, len(s1))
	assert.Equal(3, len(s2))
	assert.Equal(0, len(s3), "a shard above the band gets no slots regardless of its backlog")
	assert.Equal(1, cap1)
	assert.Equal(3, cap2)
	assert.Equal(0, cap3)
	assert.Equal([]string{"t"}, k1)
	assert.Equal(0, len(k3))

	// Every slot is accounted for exactly once across the shards holding the key.
	assert.Equal(len(order), len(s1)+len(s2))

	// Deterministic: two shards replaying the same plan against the same snapshot must agree, and
	// repeated evaluation must be identical (map iteration must not leak into the result).
	for range 10 {
		r1, rk1, rc1 := slice(order, entries, 3, 1)
		assert.Equal(s1, r1)
		assert.Equal(k1, rk1)
		assert.Equal(cap1, rc1)
	}
}

// TestPlanner_SliceStarvationGuard pins the exact case the first-slot rule exists for: a key with ONE
// old step on a quiet shard and a deep, constantly-replenished backlog on a busy one. A purely
// proportional split of a small demand rounds the quiet shard to zero, cycle after cycle.
func TestPlanner_SliceStarvationGuard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	entries := []entry{
		mkEntry(1, 3, Tally{Key: "t", Weight: 1, AgeMs: 60000, Count: 1}), // one OLD step
		mkEntry(2, 3, Tally{Key: "t", Weight: 1, AgeMs: 800, Count: 1000}),
	}
	s1, _, _ := slice([]string{"t", "t"}, entries, 3, 1)
	assert.Equal(1, len(s1), "the quiet shard's old step must take the first slot, not be rounded away")
}

// TestPlanner_SliceKeepsFairnessInterleave pins that a shard's Slots preserve the global plan's
// ordering among the occurrences it owns. Filtering must not regroup by key - that would hand one
// tenant a contiguous run and undo the interleave the lottery produced.
func TestPlanner_SliceKeepsFairnessInterleave(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Both keys live only on shard 1, so every slot lands there and Slots must equal the plan.
	entries := []entry{
		mkEntry(1, 3,
			Tally{Key: "a", Weight: 1, AgeMs: 100, Count: 3},
			Tally{Key: "b", Weight: 1, AgeMs: 100, Count: 3},
		),
	}
	order := []string{"a", "b", "a", "b", "a", "b"}
	slots, keys, perKey := slice(order, entries, 3, 1)
	assert.Equal(order, slots, "the interleave must survive the filter")
	assert.Equal([]string{"a", "b"}, keys, "distinct keys in first-appearance order")
	assert.Equal(3, perKey)
}

// TestPlanner_ClearReleasesTheBand pins the wedge this whole mechanism exists to prevent. A shard whose
// scan fails still holds the best band, so its stale claim would make every peer compute that global
// minimum, find none of its own keys there, and dispatch nothing - forever, waiting on a shard that will
// never report again. Clear releases the band on the failing cycle, not after some cutoff elapses.
func TestPlanner_ClearReleasesTheBand(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p := New()
	p.Tally(1, 5, []Tally{{Key: "live", Weight: 1, AgeMs: 100, Count: 2}})
	p.Tally(2, 1, []Tally{{Key: "gone", Weight: 1, AgeMs: 100, Count: 2}})

	// Both reporting: shard 2's better band wins, and shard 1 correctly dispatches nothing.
	plan := p.Plan(1, 8)
	assert.Equal(1, plan.GlobalBand)
	assert.Equal(0, len(plan.Slots), "shard 1 is above the band and dispatches nothing")

	p.Clear(2)

	plan = p.Plan(1, 8)
	assert.Equal(5, plan.GlobalBand, "a cleared shard's band claim must not hold the fleet")
	assert.Equal(2, len(plan.Slots), "shard 1 now holds the band and takes both its steps")
	assert.Equal([]string{"live"}, plan.Keys)

	// Recovery costs one cycle and no more: the next Tally restores the band with no cooldown.
	p.Tally(2, 1, []Tally{{Key: "gone", Weight: 1, AgeMs: 100, Count: 2}})
	assert.Equal(1, p.Plan(1, 8).GlobalBand, "a recovered shard is counted immediately")
}

// TestPlanner_SlowShardIsNotDropped pins the case a timeout could never get right, and the reason there
// is none: a shard that is merely slow to report is ALIVE, its last tally is still the best information
// anyone has, and it keeps its band until it says otherwise. Only an explicit Clear removes it.
func TestPlanner_SlowShardIsNotDropped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p := New()
	p.Tally(1, 9, []Tally{{Key: "fast", Weight: 1, AgeMs: 1, Count: 1}})
	p.Tally(2, 2, []Tally{{Key: "slow", Weight: 1, AgeMs: 1, Count: 1}})

	// Shard 1 cycles many times while shard 2 says nothing at all - a deep scan in progress, not a
	// failure. Shard 2's better band must survive every one of those cycles.
	for range 100 {
		p.Tally(1, 9, []Tally{{Key: "fast", Weight: 1, AgeMs: 1, Count: 1}})
		assert.Equal(2, p.Plan(1, 8).GlobalBand, "a quiet shard is not a dead shard")
	}
	assert.Equal(0, len(p.Plan(1, 8).Slots), "and the fast shard stays correctly above the band")
}

// TestPlanner_PickIsProportionalAndBacklogIndependent pins the lottery's defining property: a key's
// share tracks its WEIGHT and is independent of how deep its backlog is. Rolling per slot rather than
// per key is what buys that; a deep-backlog tenant must not crowd out a shallow one at equal weight.
func TestPlanner_PickIsProportionalAndBacklogIndependent(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	keys := []keyAgg{
		{key: "heavy", weight: 4, count: 100},
		{key: "light", weight: 1, count: 100},
		{key: "shallow", weight: 4, count: 4000}, // same weight as heavy, far deeper backlog
	}
	counts := map[string]int{}
	for range 200 {
		for _, k := range pick(keys, 12, defaultRand) {
			counts[k]++
		}
	}
	assert.True(counts["heavy"] > counts["light"]*2,
		"weight 4 must beat weight 1 by a wide margin (heavy=%d light=%d)", counts["heavy"], counts["light"])
	ratio := float64(counts["shallow"]) / float64(counts["heavy"])
	assert.True(ratio > 0.7 && ratio < 1.4,
		"equal weights must get comparable shares regardless of backlog depth (ratio %.2f)", ratio)
}

// TestPlanner_PickNeverExceedsCount pins that a key cannot be drawn more times than it has steps: the
// batch would name work that does not exist, and the caller's fetch would come up short every cycle.
func TestPlanner_PickNeverExceedsCount(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	keys := []keyAgg{{key: "a", weight: 1, count: 2}, {key: "b", weight: 1, count: 1}}
	order := pick(keys, 100, defaultRand)
	assert.Equal(3, len(order), "the whole band fits in the batch: exactly count(a)+count(b) slots")
	counts := map[string]int{}
	for _, k := range order {
		counts[k]++
	}
	assert.Equal(2, counts["a"])
	assert.Equal(1, counts["b"])
}

// TestPlanner_TallyNormalizesWeight pins the guard on a non-positive weight, which would make the
// lottery's 1/weight exponent infinite and the key unpickable forever - silent starvation.
func TestPlanner_TallyNormalizesWeight(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p := New()
	p.Tally(1, 5, []Tally{{Key: "zero", Weight: 0, AgeMs: 10, Count: 1}})
	plan := p.Plan(1, 8)
	assert.Equal(1, len(plan.Slots), "a zero-weight key must still be dispatchable")
	assert.Equal([]string{"zero"}, plan.Keys)
}

// TestPlanner_EmptyAndReset pins the two trivial states: a planner nobody has reported to plans
// nothing, and Reset returns it there (a tally surviving a restart would claim a band nobody serves).
func TestPlanner_EmptyAndReset(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p := New()
	plan := p.Plan(1, 8)
	assert.Equal(math.MaxInt, plan.GlobalBand, "an idle fleet has no band")
	assert.Equal(0, len(plan.Slots))
	band, keys := p.LastBand()
	assert.Equal(-1, band, "an idle fleet reports nothing to observe, never MaxInt as a metric label")
	assert.Equal(0, keys)

	p.Tally(1, 5, []Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1}})
	assert.Equal(1, len(p.Plan(1, 8).Slots))
	band, keys = p.LastBand()
	assert.Equal(5, band)
	assert.Equal(1, keys)

	p.Reset()
	assert.Equal(0, len(p.Plan(1, 8).Slots))
	band, _ = p.LastBand()
	assert.Equal(-1, band, "Reset restores the nothing-planned-yet sentinel")
}

// TestPlanner_CapacityBounds pins that the plan never exceeds capacity and that a non-positive
// capacity yields nothing rather than looping.
func TestPlanner_CapacityBounds(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p := New()
	p.Tally(1, 5, []Tally{{Key: "a", Weight: 1, AgeMs: 1, Count: 1000}})
	assert.Equal(7, len(p.Plan(1, 7).Slots))
	assert.Equal(0, len(p.Plan(1, 0).Slots))
	assert.Equal(0, len(p.Plan(1, -1).Slots))
}
