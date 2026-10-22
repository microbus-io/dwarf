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
	"sort"
)

// keyAgg is one fairness key's aggregate ACROSS shards at one band - the merge of every shard's Tally for
// that key there.
type keyAgg struct {
	key    string
	weight float64
	ageMs  float64
	count  int
}

// bandAgg is one band's keys, merged across shards.
type bandAgg struct {
	band int
	keys []keyAgg
}

// merge groups every shard's tallies by band, best band first, and merges each key within its band.
//
// A key's steps can span shards, so counts sum, its age is the globally-oldest step's, and its weight is the
// lowest any shard reports - the same anti-self-promotion rule a single shard's Tally applies locally. A key
// at two bands is two independent entries: fairness is decided within a band, never across bands.
//
// Iteration is in shard order and keys keep insertion order within their band, so the result is stable -
// the lottery downstream depends on that for reproducibility.
func merge(entries []entry) []bandAgg {
	var bands []bandAgg
	at := map[int]int{}   // band -> index into bands
	idx := map[Slot]int{} // (band, key) -> index into that band's keys
	for _, e := range entries {
		for _, t := range e.t.tallies {
			b, ok := at[t.Band]
			if !ok {
				b = len(bands)
				at[t.Band] = b
				bands = append(bands, bandAgg{band: t.Band})
			}
			s := Slot{Band: t.Band, Key: t.Key}
			i, ok := idx[s]
			if !ok {
				idx[s] = len(bands[b].keys)
				bands[b].keys = append(bands[b].keys, keyAgg{key: t.Key, weight: t.Weight, ageMs: t.AgeMs, count: t.Count})
				continue
			}
			agg := &bands[b].keys[i]
			agg.count += t.Count
			agg.ageMs = max(agg.ageMs, t.AgeMs)
			agg.weight = min(agg.weight, t.Weight)
		}
	}
	sort.Slice(bands, func(i, j int) bool { return bands[i].band < bands[j].band })
	return bands
}

// fill draws the global plan: the best band's lottery first, then the next band's for whatever capacity is
// left, and so on until capacity is reached or every band is exhausted. That is the whole priority rule - a
// worse band gets exactly the capacity the better ones leave unused, so a better band that fills the batch
// starves the worse ones, and one that cannot fill it starves nothing.
func fill(bands []bandAgg, capacity int, randFloat func() float64) []Slot {
	order := make([]Slot, 0, capacity)
	for _, b := range bands {
		if len(order) >= capacity {
			break
		}
		for _, k := range pick(b.keys, capacity-len(order), randFloat) {
			order = append(order, Slot{Band: b.band, Key: k})
		}
	}
	return order
}

// reach is the band at which a fill at capacity runs out of capacity, or math.MaxInt when every due step fits.
// It needs no lottery: pick draws min(capacity left, the band's total count) slots whatever it rolls.
func reach(bands []bandAgg, capacity int) int {
	n := 0
	for _, b := range bands {
		for _, k := range b.keys {
			n += k.count
		}
		if n >= capacity {
			return b.band
		}
	}
	return math.MaxInt
}

// pick runs the weighted fairness lottery over one band's keys and returns the ordered sequence of
// keys to dispatch - one entry per step, at most capacity.
//
// Each slot is an independent Efraimidis-Spirakis draw over the KEYS, re-rolled every slot. Rolling
// per slot rather than per key is what makes a key's expected share proportional to its weight and
// INDEPENDENT of how deep its backlog is - a tenant with a million queued steps and a tenant with ten
// get the same share at the same weight. A key can never be drawn more times than it has steps.
func pick(keys []keyAgg, capacity int, randFloat func() float64) []string {
	remaining := make([]int, len(keys))
	for i := range keys {
		remaining[i] = keys[i].count
	}
	order := make([]string, 0, capacity)
	for len(order) < capacity {
		best, bestScore := -1, -1.0
		for i := range keys {
			if remaining[i] <= 0 {
				continue
			}
			score := math.Pow(randFloat(), 1/keys[i].weight)
			if score > bestScore {
				bestScore = score
				best = i
			}
		}
		if best < 0 {
			break // every key exhausted: the whole band fits in the batch
		}
		order = append(order, keys[best].key)
		remaining[best]--
	}
	return order
}

// slice routes the global plan to shards and returns the part that lands on one of them.
//
// Per slot - one key at one band - the FIRST occurrence goes to the shard holding that key's oldest step
// at that band. That shard fetches
// oldest-first, so the key leads with its globally-oldest step - and, more importantly, it is what
// stops unbounded intra-tenant starvation: a key with one old step on a quiet shard and a
// constantly-replenished backlog on a busy one would see a purely proportional split round the quiet
// shard to zero, pass after pass, forever.
//
// The REMAINING slots split proportional to per-shard counts, largest-remainder, shard order breaking
// ties. Every step of this is deterministic, which is the load-bearing part: each shard runs this
// independently, and they must agree on who owns which slot without exchanging a word. Same plan plus
// same snapshot must give the same assignment on every caller.
//
// Deliberately NOT: per-shard lotteries, weights scaled by how many shards hold a key, or capacity
// allocated by shard counts. The global pick already priced every key's share; this only routes it.
func slice(order []Slot, entries []entry, shard int) (slots []Slot, fetches []Fetch) {
	needed := map[Slot]int{}
	for _, k := range order {
		needed[k]++
	}
	assign := make(map[Slot][]int, len(needed))
	perKey := make(map[Slot]int, len(needed))
	mine := 0
	for k, n := range needed {
		type holder struct {
			shard int
			count int
			age   float64
		}
		var holders []holder
		for _, e := range entries {
			if i, ok := e.t.byKey[k]; ok {
				t := e.t.tallies[i]
				holders = append(holders, holder{shard: e.shard, count: t.Count, age: t.AgeMs})
			}
		}
		if len(holders) == 0 {
			continue
		}
		// First slot: the oldest holder. Entries arrive in shard order, so a strict > keeps the lower
		// shard on an age tie.
		oldest := 0
		for i := 1; i < len(holders); i++ {
			if holders[i].age > holders[oldest].age {
				oldest = i
			}
		}
		quota := make([]int, len(holders))
		quota[oldest] = 1
		avail := make([]int, len(holders))
		totalAvail := 0
		for i := range holders {
			avail[i] = max(0, holders[i].count-quota[i])
			totalAvail += avail[i]
		}
		rem := min(n-1, totalAvail) // counts can be stale; a shortfall self-corrects next cycle
		if rem > 0 {
			assigned := 0
			base := make([]int, len(holders))
			for i := range holders {
				base[i] = rem * avail[i] / totalAvail
				assigned += base[i]
			}
			idx := make([]int, len(holders))
			for i := range idx {
				idx[i] = i
			}
			sort.SliceStable(idx, func(a, b int) bool {
				ra := rem * avail[idx[a]] % totalAvail
				rb := rem * avail[idx[b]] % totalAvail
				if ra != rb {
					return ra > rb
				}
				return idx[a] < idx[b]
			})
			for _, i := range idx {
				if assigned >= rem {
					break
				}
				if base[i] < avail[i] {
					base[i]++
					assigned++
				}
			}
			for i := range holders {
				quota[i] += base[i]
			}
		}
		// Per-occurrence assignment: oldest holder first, then holders in shard order. Below the head
		// the interleave is approximate by design - each shard's own fetch keeps its steps oldest-first,
		// and the head slot carries the globally-oldest.
		seq := make([]int, 0, n)
		seq = append(seq, holders[oldest].shard)
		for i := range holders {
			extra := quota[i]
			if i == oldest {
				extra--
			}
			for range extra {
				seq = append(seq, holders[i].shard)
			}
		}
		assign[k] = seq
		c := 0
		for _, s := range seq {
			if s == shard {
				c++
			}
		}
		perKey[k] = c
		mine += c
	}
	if mine == 0 {
		return nil, nil
	}
	// Replay the global order keeping only this shard's occurrences. Filtering preserves relative
	// order, so the fairness interleave survives intact within each band, and the bands stay best first.
	occ := map[Slot]int{}
	slots = make([]Slot, 0, mine)
	seen := make(map[Slot]bool, len(needed))
	fetchAt := map[int]int{} // band -> index into fetches
	for _, k := range order {
		seq := assign[k]
		o := occ[k]
		occ[k]++
		if o >= len(seq) || seq[o] != shard {
			continue
		}
		slots = append(slots, k)
		if seen[k] {
			continue
		}
		seen[k] = true
		f, ok := fetchAt[k.Band]
		if !ok {
			f = len(fetches)
			fetchAt[k.Band] = f
			fetches = append(fetches, Fetch{Band: k.Band})
		}
		fetches[f].Keys = append(fetches[f].Keys, k.Key)
		fetches[f].PerKeyCap = max(fetches[f].PerKeyCap, perKey[k])
	}
	return slots, fetches
}
