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

// keyAgg is one fairness key's aggregate ACROSS shards at the global band - the merge of every
// shard's Tally for that key.
type keyAgg struct {
	key    string
	weight float64
	ageMs  float64
	count  int
}

// merge computes the globally-minimum due band and the per-key aggregates at that band.
//
// A key's steps can span shards, so counts sum. Its weight comes from the globally-oldest step (max
// age wins), which is the same anti-self-promotion rule a single shard's Tally applies locally.
// Shards at a worse band contribute nothing: a worse band materializes only once the better one
// drains, which is what "strict" means here.
//
// Iteration is in shard order and keys keep insertion order, so the returned slice is stable - the
// lottery downstream depends on that for reproducibility.
func merge(entries []entry) (band int, keys []keyAgg) {
	band = math.MaxInt
	for _, e := range entries {
		if len(e.t.tallies) > 0 && e.t.band < band {
			band = e.t.band
		}
	}
	if band == math.MaxInt {
		return band, nil
	}
	byKey := map[string]*keyAgg{}
	var order []string
	for _, e := range entries {
		if e.t.band != band {
			continue
		}
		for _, t := range e.t.tallies {
			agg := byKey[t.Key]
			if agg == nil {
				byKey[t.Key] = &keyAgg{key: t.Key, weight: t.Weight, ageMs: t.AgeMs, count: t.Count}
				order = append(order, t.Key)
				continue
			}
			agg.count += t.Count
			if t.AgeMs > agg.ageMs {
				agg.ageMs = t.AgeMs
				agg.weight = t.Weight
			}
		}
	}
	keys = make([]keyAgg, 0, len(order))
	for _, k := range order {
		keys = append(keys, *byKey[k])
	}
	return band, keys
}

// pick runs the weighted fairness lottery over the band's keys and returns the ordered sequence of
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
// Per key, the FIRST slot goes to the shard holding that key's oldest step. That shard fetches
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
func slice(order []string, entries []entry, band, shard int) (slots, keys []string, perKeyCap int) {
	needed := map[string]int{}
	for _, k := range order {
		needed[k]++
	}
	assign := make(map[string][]int, len(needed))
	mine := 0
	for k, n := range needed {
		type holder struct {
			shard int
			count int
			age   float64
		}
		var holders []holder
		for _, e := range entries {
			if e.t.band != band {
				continue
			}
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
		if c > perKeyCap {
			perKeyCap = c
		}
		mine += c
	}
	if mine == 0 {
		return nil, nil, 0
	}
	// Replay the global order keeping only this shard's occurrences. Filtering preserves relative
	// order, so the fairness interleave survives intact within the slice.
	occ := map[string]int{}
	slots = make([]string, 0, mine)
	seen := make(map[string]bool, len(needed))
	for _, k := range order {
		seq := assign[k]
		o := occ[k]
		occ[k]++
		if o >= len(seq) || seq[o] != shard {
			continue
		}
		slots = append(slots, k)
		if !seen[k] {
			seen[k] = true
			keys = append(keys, k)
		}
	}
	return slots, keys, perKeyCap
}
