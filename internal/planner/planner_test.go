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
	"sync"
	"testing"

	"github.com/microbus-io/testarossa"
)

// mkEntry fabricates one shard's tally at one band for the merge/slice unit tests, which exercise those
// functions directly rather than through a Planner.
func mkEntry(shard, band int, tallies ...Tally) entry {
	for i := range tallies {
		tallies[i].Band = band
	}
	return mkBands(shard, tallies...)
}

// mkBands is mkEntry for tallies that carry their own bands.
func mkBands(shard int, tallies ...Tally) entry {
	st := &shardTally{tallies: tallies}
	if len(tallies) > 0 {
		st.byKey = make(map[Slot]int, len(tallies))
		for i, t := range tallies {
			st.byKey[Slot{Band: t.Band, Key: t.Key}] = i
		}
	}
	return entry{shard: shard, t: st}
}

// slots repeats one slot n times, for building a plan order by hand.
func slots(band int, key string, n int) []Slot {
	out := make([]Slot, n)
	for i := range out {
		out[i] = Slot{Band: band, Key: key}
	}
	return out
}

// TestPlanner_MergeAcrossShards pins the merge: bands best first; per-key counts summed across shards
// within a band; the lowest weight any shard reports winning a key's weight (so a tenant cannot self-promote
// by queueing heavier work on another shard); and a worse band kept apart rather than mixed in.
func TestPlanner_MergeAcrossShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	entries := []entry{
		mkEntry(1, 3,
			Tally{Key: "a", Weight: 2, AgeMs: 500, Count: 3},
			Tally{Key: "b", Weight: 1, AgeMs: 100, Count: 1},
		),
		mkEntry(2, 3,
			Tally{Key: "a", Weight: 7, AgeMs: 900, Count: 6}, // older and heavier: its age wins, not its weight
		),
		mkEntry(3, 9, Tally{Key: "z", Weight: 1, AgeMs: 1, Count: 1}), // worse band
		mkEntry(4, math.MaxInt), // nothing due
	}
	bands := merge(entries)
	assert.Equal(2, len(bands))
	assert.Equal(3, bands[0].band)
	keys := bands[0].keys
	assert.Equal(2, len(keys))
	assert.Equal("a", keys[0].key)
	assert.Equal(9, keys[0].count, "counts sum across shards")
	assert.Equal(2.0, keys[0].weight, "the lowest weight wins the merge")
	assert.Equal(900.0, keys[0].ageMs, "the globally-oldest step's age wins the merge")
	assert.Equal("b", keys[1].key)
	assert.Equal(9, bands[1].band, "a worse band follows the better one")
	assert.Equal("z", bands[1].keys[0].key)

	// A key at two bands is two entries: fairness is decided within a band, never across.
	bands = merge([]entry{mkBands(1,
		Tally{Band: 5, Key: "t", Weight: 1, AgeMs: 1, Count: 2},
		Tally{Band: 1, Key: "t", Weight: 1, AgeMs: 1, Count: 3},
	)})
	assert.Equal(2, len(bands))
	assert.Equal(1, bands[0].band, "best band first, whatever order the shard reported in")
	assert.Equal(3, bands[0].keys[0].count)
	assert.Equal(2, bands[1].keys[0].count)

	// An all-empty fleet has no band.
	assert.Equal(0, len(merge([]entry{mkEntry(1, math.MaxInt)})))
}

// TestPlanner_SliceFirstSlotToOldestThenProportional pins the slice rule: a key's first slot goes to
// the shard holding its OLDEST step, the rest split proportional to per-shard counts with
// deterministic largest-remainder rounding, and a shard holding the key only at another band wins nothing
// regardless of how much backlog it holds there.
func TestPlanner_SliceFirstSlotToOldestThenProportional(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	entries := []entry{
		mkEntry(1, 3, Tally{Key: "t", Weight: 1, AgeMs: 500, Count: 3}),
		mkEntry(2, 3, Tally{Key: "t", Weight: 1, AgeMs: 900, Count: 6}),
		mkEntry(3, 5, Tally{Key: "t", Weight: 1, AgeMs: 9999, Count: 50}), // another band: excluded
	}
	order := slots(3, "t", 4)

	// Shard 2 holds the oldest step at the band, so it takes the first slot; the remaining 3 split
	// over avail (shard 1: 3, shard 2: 5) by largest remainder -> shard 1: 1, shard 2: 2 more.
	s1, f1 := slice(order, entries, 1)
	s2, f2 := slice(order, entries, 2)
	s3, f3 := slice(order, entries, 3)

	assert.Equal(1, len(s1))
	assert.Equal(3, len(s2))
	assert.Equal(0, len(s3), "a shard holding the key only at another band gets no slots regardless of its backlog")
	assert.Equal([]Fetch{{Band: 3, Keys: []string{"t"}, PerKeyCap: 1}}, f1)
	assert.Equal(3, f2[0].PerKeyCap)
	assert.Equal(0, len(f3))

	// Every slot is accounted for exactly once across the shards holding the key.
	assert.Equal(len(order), len(s1)+len(s2))

	// Deterministic: two shards replaying the same plan against the same snapshot must agree, and
	// repeated evaluation must be identical (map iteration must not leak into the result).
	for range 10 {
		r1, rf1 := slice(order, entries, 1)
		assert.Equal(s1, r1)
		assert.Equal(f1, rf1)
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
	s1, _ := slice(slots(3, "t", 2), entries, 1)
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
	a, b := Slot{Band: 3, Key: "a"}, Slot{Band: 3, Key: "b"}
	order := []Slot{a, b, a, b, a, b}
	got, fetches := slice(order, entries, 1)
	assert.Equal(order, got, "the interleave must survive the filter")
	assert.Equal([]Fetch{{Band: 3, Keys: []string{"a", "b"}, PerKeyCap: 3}}, fetches, "distinct keys in first-appearance order")
}

// TestPlanner_ClearReleasesTheBand pins the wedge this whole mechanism exists to prevent. A shard whose
// scan fails still holds its last tally, so a stale claim on a band that fills the batch would leave every
// peer no capacity to spill into, and they would dispatch nothing - forever, waiting on a shard that will
// never report again. Clear releases the band on the failing cycle, not after some cutoff elapses.
func TestPlanner_ClearReleasesTheBand(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p := New()
	p.Tally(1, []Tally{{Band: 5, Key: "live", Weight: 1, AgeMs: 100, Count: 2}})
	p.Tally(2, []Tally{{Band: 1, Key: "gone", Weight: 1, AgeMs: 100, Count: 8}})

	// Both reporting: shard 2's better band fills the batch, and shard 1 correctly dispatches nothing.
	plan := p.Plan(1, 8)
	assert.Equal(1, plan.GlobalBand)
	assert.Equal(1, plan.Reach)
	assert.Equal(0, len(plan.Slots), "shard 1 is past the reach and dispatches nothing")

	p.Clear(2)

	plan = p.Plan(1, 8)
	assert.Equal(5, plan.GlobalBand, "a cleared shard's band claim must not hold the fleet")
	assert.Equal(2, len(plan.Slots), "shard 1 now holds the band and takes both its steps")
	assert.Equal([]Fetch{{Band: 5, Keys: []string{"live"}, PerKeyCap: 2}}, plan.Fetches)

	// Recovery costs one cycle and no more: the next Tally restores the band with no cooldown.
	p.Tally(2, []Tally{{Band: 1, Key: "gone", Weight: 1, AgeMs: 100, Count: 8}})
	assert.Equal(1, p.Plan(1, 8).GlobalBand, "a recovered shard is counted immediately")
}

// TestPlanner_SlowShardIsNotDropped pins the case a timeout could never get right, and the reason there
// is none: a shard that is merely slow to report is ALIVE, its last tally is still the best information
// anyone has, and it keeps its band until it says otherwise. Only an explicit Clear removes it.
func TestPlanner_SlowShardIsNotDropped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	p := New()
	p.Tally(1, []Tally{{Band: 9, Key: "fast", Weight: 1, AgeMs: 1, Count: 1}})
	p.Tally(2, []Tally{{Band: 2, Key: "slow", Weight: 1, AgeMs: 1, Count: 8}})

	// Shard 1 cycles many times while shard 2 says nothing at all - a deep scan in progress, not a
	// failure. Shard 2's better band must survive every one of those cycles.
	for range 100 {
		p.Tally(1, []Tally{{Band: 9, Key: "fast", Weight: 1, AgeMs: 1, Count: 1}})
		assert.Equal(2, p.Plan(1, 8).GlobalBand, "a quiet shard is not a dead shard")
	}
	assert.Equal(0, len(p.Plan(1, 8).Slots), "and the fast shard stays correctly past the reach")
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
	p.Tally(1, []Tally{{Band: 5, Key: "zero", Weight: 0, AgeMs: 10, Count: 1}})
	plan := p.Plan(1, 8)
	assert.Equal(1, len(plan.Slots), "a zero-weight key must still be dispatchable")
	assert.Equal([]string{"zero"}, plan.Fetches[0].Keys)
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

	p.Tally(1, []Tally{{Band: 5, Key: "a", Weight: 1, AgeMs: 1, Count: 1}})
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
	p.Tally(1, []Tally{{Band: 5, Key: "a", Weight: 1, AgeMs: 1, Count: 1000}})
	assert.Equal(7, len(p.Plan(1, 7).Slots))
	assert.Equal(0, len(p.Plan(1, 0).Slots))
	assert.Equal(0, len(p.Plan(1, -1).Slots))
}

// TestPlanner_TalliedAgreesWithSlotsUnderConcurrency pins that Plan.Tallied and Plan.Slots describe the SAME
// snapshot, which is what lets a caller act on the pair.
//
// They are read by different goroutines than the ones writing them - the caller's tally loop and its supply
// loop run on independent cadences - so a Tallied read from a FRESH map lookup rather than from the snapshot
// can disagree with the Slots computed beside it. The damaging direction is quiet: a shard absent at the
// snapshot but re-tallied a moment later reports Tallied=true with no Slots, which reads as the positive
// statement "nothing here is dispatchable" and gets acted on by clearing a healthy cache partition.
//
// The direction assertable from outside is its mirror - winning slots is only possible for a shard that was
// in the snapshot - and it fails against a build that re-reads the map, because Clear can land in between.
func TestPlanner_TalliedAgreesWithSlotsUnderConcurrency(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	p := New()

	// A peer holds the same band throughout, so shard 1 always has keys to win slots at.
	p.Tally(2, []Tally{{Band: 5, Key: "k", Weight: 1, AgeMs: 100, Count: 8}})

	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			p.Tally(1, []Tally{{Band: 5, Key: "k", Weight: 1, AgeMs: 100, Count: 8}})
			p.Clear(1)
		}
	}()

	for range 20000 {
		plan := p.Plan(1, 8)
		if len(plan.Slots) > 0 && !plan.Tallied {
			assert.True(false, "a shard that won %d slots must have been in the snapshot that granted them", len(plan.Slots))
			break
		}
	}
	close(stop)
	wg.Wait()
}

// TestPlanner_ReachReadsTheStandingTallies pins Reach: the band at which a fill at the given capacity runs
// out, MaxInt when everything fits, moved by a Tally or a Clear without anything planning, and -1 when nothing
// is due anywhere.
func TestPlanner_ReachReadsTheStandingTallies(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	p := New()
	assert.Equal(-1, p.Reach(8), "nothing reported")
	p.Tally(1, []Tally{{Band: 5, Key: "a", Weight: 1, Count: 1}})
	p.Tally(2, []Tally{{Band: 3, Key: "b", Weight: 1, Count: 1}})
	p.Tally(3, nil) // a shard with nothing due holds no band
	assert.Equal(math.MaxInt, p.Reach(8), "everything due fits, so nothing is held back")
	assert.Equal(3, p.Reach(1))
	assert.Equal(5, p.Reach(2))
	p.Clear(2)
	assert.Equal(5, p.Reach(1), "a drained band is gone as soon as its shard reports, with no plan in between")
}

// TestPlanner_FillSpillsToWorseBands pins the priority rule: a worse band gets exactly the capacity the better
// ones leave unused. A sparse but continuous better band must not idle the batch - a better band that cannot
// fill it starves nothing - while one that does fill it holds every worse band back.
func TestPlanner_FillSpillsToWorseBands(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	p := New()

	// Three steps due at the better band, on shard 1; a deep backlog at the worse one, on shard 2.
	p.Tally(1, []Tally{{Band: 50, Key: "hi", Weight: 1, AgeMs: 10, Count: 3}})
	p.Tally(2, []Tally{{Band: 100, Key: "lo", Weight: 1, AgeMs: 60000, Count: 8}})
	p1, p2 := p.Plan(1, 8), p.Plan(2, 8)
	assert.Equal(50, p1.GlobalBand)
	assert.Equal(100, p1.Reach, "the fill ran out of capacity at the worse band")
	assert.Equal(slots(50, "hi", 3), p1.Slots, "the better band takes all it has")
	assert.Equal(slots(100, "lo", 5), p2.Slots, "and the worse band takes exactly what is left")

	// A better band that fills the batch holds the worse one back entirely.
	p.Tally(1, []Tally{{Band: 50, Key: "hi", Weight: 1, AgeMs: 10, Count: 8}})
	assert.Equal(8, len(p.Plan(1, 8).Slots))
	assert.Equal(0, len(p.Plan(2, 8).Slots), "no capacity is left to spill into")
	assert.Equal(50, p.Plan(2, 8).Reach)
}

// TestPlanner_SlotsAreBestBandFirst pins the order a shard holding several bands gets: the better band's slots
// lead, and each band gets its own fetch, best first.
func TestPlanner_SlotsAreBestBandFirst(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	p := New()

	// Reported worst band first, and the same key at both bands.
	p.Tally(1, []Tally{
		{Band: 9, Key: "t", Weight: 1, AgeMs: 900, Count: 10},
		{Band: 2, Key: "t", Weight: 1, AgeMs: 10, Count: 2},
	})
	plan := p.Plan(1, 6)
	assert.Equal(append(slots(2, "t", 2), slots(9, "t", 4)...), plan.Slots)
	assert.Equal([]Fetch{
		{Band: 2, Keys: []string{"t"}, PerKeyCap: 2},
		{Band: 9, Keys: []string{"t"}, PerKeyCap: 4},
	}, plan.Fetches)
	assert.Equal(9, plan.Reach)
}

// TestPlanner_BetterBandsThatFillTheBatchHoldTheRestBack pins the fill's stopping point with four bands due
// across shards: the best two fill the batch, so the worse two win no slot anywhere and the reach is the
// second band.
func TestPlanner_BetterBandsThatFillTheBatchHoldTheRestBack(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	p := New()

	p.Tally(1, []Tally{
		{Band: 10, Key: "a", Weight: 1, AgeMs: 10, Count: 3},
		{Band: 30, Key: "e", Weight: 1, AgeMs: 90000, Count: 8},
	})
	p.Tally(2, []Tally{
		{Band: 20, Key: "c", Weight: 1, AgeMs: 10, Count: 4},
		{Band: 20, Key: "d", Weight: 1, AgeMs: 10, Count: 4},
		{Band: 40, Key: "f", Weight: 9, AgeMs: 90000, Count: 8},
	})
	for _, shard := range []int{1, 2} {
		plan := p.Plan(shard, 8)
		assert.Equal(20, plan.Reach)
		for _, slot := range plan.Slots {
			assert.True(slot.Band <= 20, "shard %d planned band %d past the reach", shard, slot.Band)
		}
		for _, f := range plan.Fetches {
			assert.True(f.Band <= 20, "shard %d fetches band %d past the reach", shard, f.Band)
		}
	}
	assert.Equal(slots(10, "a", 3), p.Plan(1, 8).Slots, "shard 1 serves all of the best band and none of its band 30")
	assert.Equal(5, len(p.Plan(2, 8).Slots), "shard 2 fills the rest from band 20, none from band 40")
}
