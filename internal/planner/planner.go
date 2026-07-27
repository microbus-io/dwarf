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

// Package planner decides which work a shard may dispatch, given what every shard last reported.
//
// Two scheduling rules are global while the thing that enforces them is per-shard: strict priority says
// no worse band is served while a better one has due work anywhere, and weighted fairness says the
// distinct keys at that band split the batch by weight. So each shard reports what it sees and plans
// against the merged picture, on its own clock - no shard ever waits for another.
//
//	plan := p.Plan(shard, capacity) // after this shard's own Tally or Clear
//
// A shard's participation is DECLARED, never inferred. Every cycle a shard either Tally's what it saw or
// Clear's because it could not look; there is no timeout, and a merely slow shard keeps its last tally
// and stays counted. A shard that fails to scan MUST Clear, or the band it last claimed goes on
// outranking every peer that could actually serve work.
//
// The planner holds no steps, touches no database, and does no I/O. It deals in fairness keys and
// counts; resolving a key to actual steps is the caller's job. Every method is safe for concurrent use -
// one planner is shared by every shard on a replica.
package planner

import (
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

// Tally is one fairness key's aggregate on one shard, at that shard's minimum due band: how many due
// steps the key has there, and the age and weight of its OLDEST one.
//
// It is not a step. A Tally with Count 40 stands for 40 steps, and a shard reports one Tally per
// distinct key - so a tally set is O(keys), never O(backlog). That is the property the whole
// three-phase shape exists to preserve, and a caller that reports one Tally per step defeats it.
//
// Count is expected to be capped at the planning capacity by whoever produced it. The cap is lossless
// here: no key is ever assigned more than the whole batch, so a count above capacity is
// indistinguishable from capacity.
//
// Weight is the fairness weight of the key's oldest due step, deliberately not of its newest - keying
// weight off the oldest is what stops a tenant self-promoting by queueing newer high-weight work.
type Tally struct {
	Key    string
	Weight float64
	AgeMs  float64
	Count  int
}

// Plan is one shard's answer for one cycle.
//
// An empty Slots means this shard dispatches nothing, and it deliberately does not say why - nothing
// is due anywhere, nothing is due here, this shard's work is above the global band, or the band's
// slots all went to shards holding more of the planned keys. The caller's action is the same in every
// case (hold no candidates), so distinguishing them would be surface with no consumer. For a log line,
// compare GlobalBand against the band just tallied.
type Plan struct {
	// GlobalBand is the best (lowest) band any live shard has due work at, or math.MaxInt when the
	// fleet has nothing due. Slots is drawn entirely from this band - there is no spill.
	GlobalBand int
	// Slots is the ordered sequence of fairness keys this shard should dispatch, one entry per step.
	// A key appearing three times means three of its steps. The order is the global plan's fairness
	// interleave, filtered to this shard's occurrences, so replaying it preserves the interleave.
	Slots []string
	// Keys is the distinct keys in Slots, in first-appearance order - the caller's fetch predicate.
	Keys []string
	// PerKeyCap is the largest number of slots any single key won on this shard. A caller fetching
	// with a uniform per-key cap uses this: it over-fetches for the lighter keys, but keeps the fetch
	// a single query, and both factors are bounded by capacity.
	PerKeyCap int
}

// Planner holds the most recent Tally from each shard and turns them into per-shard Plans.
// Safe for concurrent use: one goroutine per shard is the expected caller shape.
type Planner struct {
	mu     sync.Mutex
	shards map[int]*shardTally
	// Most recent plan's band and distinct-key count, for observability only. Whichever shard planned
	// last wins; nothing reads them to make a decision.
	lastBand int
	lastKeys int
	// randFloat is the fairness lottery's source, a field so a test can make the pick deterministic.
	randFloat func() float64
}

// shardTally is one shard's last report. Entries are REPLACED, never mutated, so a reader that took a
// pointer under the lock can use it after releasing - which is what lets planning run unlocked.
type shardTally struct {
	band    int
	tallies []Tally
	byKey   map[string]int // key -> index into tallies, for the slice rule's lookups
	// at is when this report was made, for TallyAge. Observability only - nothing plans on it, and the
	// planner deliberately infers NOTHING from a tally's age: participation is declared (Clear), never
	// timed out, because a clock cannot tell a dead shard from a slow one.
	at time.Time
}

// entry pairs a tally with its shard for ordered iteration. Map iteration order is not stable, and
// both the merge and the slice rule depend on a stable shard order to stay deterministic.
type entry struct {
	shard int
	t     *shardTally
}

// defaultRand is the lottery's production source, named so a test can drive pick directly.
func defaultRand() float64 { return rand.Float64() }

// New returns an empty Planner.
func New() *Planner {
	p := &Planner{randFloat: defaultRand}
	p.Reset()
	return p
}

// Reset drops every tally. Call it when the owning process restarts its shard set; a stale tally from
// a previous run would otherwise claim a band nobody is serving.
func (p *Planner) Reset() {
	p.mu.Lock()
	p.shards = map[int]*shardTally{}
	p.lastBand = -1
	p.lastKeys = 0
	p.mu.Unlock()
}

// Tally records what one shard currently sees: its minimum due band, and one Tally per fairness key at
// that band. It replaces that shard's previous report.
//
// A shard with nothing due must still report (band math.MaxInt, no tallies), and one that tried to look
// and FAILED must call Clear. Between them those cover every outcome, which is what lets the planner
// know a shard's state outright instead of inferring it from silence. Staying quiet is not an option a
// caller has: the previous tally stands until something replaces or clears it.
//
// The tallies slice is HANDED OVER, not copied: it is retained, and normalized in place (a non-positive
// weight is rewritten to 1 - see below). So a caller must neither mutate nor reuse a slice it has passed,
// and in particular must not pass the same backing array twice: the second call's normalization would
// write into an array a snapshot of the first is still handing out to an unlocked Plan. Every producer
// today allocates a fresh slice per cycle, which is what the contract asks for.
func (p *Planner) Tally(shard, band int, tallies []Tally) {
	st := &shardTally{band: band, tallies: tallies, at: time.Now()}
	if len(tallies) > 0 {
		st.byKey = make(map[string]int, len(tallies))
		for i := range tallies {
			// A non-positive weight would make the lottery's 1/weight exponent infinite and the key
			// unpickable forever - silent starvation. Normalize here, where the invariant belongs,
			// rather than trusting every producer.
			if tallies[i].Weight <= 0 {
				tallies[i].Weight = 1
			}
			st.byKey[tallies[i].Key] = i
		}
	}
	p.mu.Lock()
	p.shards[shard] = st
	p.mu.Unlock()
}

// TallyAge is how long ago the STALEST shard still in the planner reported, and zero when none is - the
// one number that says how mixed the freshness of a plan's inputs is.
//
// It measures a real coupling with no other readout. Every shard plans from a merged view of every
// shard's LAST report, so a piston cycling slowly holds its peers' plans on a picture that old: the
// global band and the slice rule are both computed from those mixed-freshness tallies, and a shard
// whose cycle stretched to 400ms while its peers spin at 67ms has them planning against a view six of
// their own cycles stale. Nothing detects or corrects that today, and nothing here should start to -
// see Clear on why a timeout would be the wrong fix. This exists so the question "are the inputs
// diverging" can be ASKED, since a throughput number alone cannot distinguish it from a slow database.
//
// Expect roughly one cycle interval in a healthy fleet. Sustained multiples of that name a shard whose
// piston has fallen behind its peers.
func (p *Planner) TallyAge() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	var oldest time.Time
	for _, st := range p.shards {
		if oldest.IsZero() || st.at.Before(oldest) {
			oldest = st.at
		}
	}
	if oldest.IsZero() {
		return 0
	}
	return time.Since(oldest)
}

// Clear drops a shard's tally, excluding it from planning until it reports again. A caller whose scan
// FAILED calls this: it holds that fact directly, so the planner never has to infer a shard's absence
// from silence and a clock.
//
// Excluding a shard that could not look is the accurate model, not a safety hack. It cannot dispatch
// this cycle regardless, and the global band means the best band with due work that someone can actually
// serve. Nothing is lost either: if the shard recovers still holding that band, its next Tally restores
// it within a cycle.
//
// Leaving a failed shard's last tally in place is what makes this necessary rather than optional. That
// tally claims a band only it holds, so every peer computes the same global minimum, finds none of its
// own keys there, and dispatches nothing - forever, waiting on a shard that will never report again. A
// shard that is merely SLOW is a different case and must not be cleared: it is alive, its last tally is
// still the best information anyone has, and it will replace it when its scan returns.
func (p *Planner) Clear(shard int) {
	p.mu.Lock()
	delete(p.shards, shard)
	p.mu.Unlock()
}

// Plan returns what one shard should dispatch this cycle, drawn from the global band and capped at
// capacity slots across the whole fleet.
//
// Call it after this shard's own Tally for the cycle: planning first means planning against your own
// previous report, which at best wastes a cycle and at worst claims a band you no longer hold.
//
// Every shard rolls its own plan from its own snapshot. The lottery is independent per caller, which
// changes nothing in expectation and needs no coordination.
func (p *Planner) Plan(shard, capacity int) Plan {
	entries := p.snapshot()
	globalBand, keys := merge(entries)

	// An idle fleet reports -1, not math.MaxInt: LastBand's consumer labels a metric with the band, and
	// MaxInt would publish a series named for a priority no caller can ever set.
	observed := globalBand
	if len(keys) == 0 {
		observed = -1
	}
	p.mu.Lock()
	p.lastBand, p.lastKeys = observed, len(keys)
	p.mu.Unlock()

	out := Plan{GlobalBand: globalBand}
	if len(keys) == 0 || capacity <= 0 {
		return out
	}
	// The global pick prices every key's share of the batch; the slice rule only routes it. A shard
	// above the band, or with nothing due, holds none of the planned keys and falls out with no slots -
	// no special case needed for either.
	out.Slots, out.Keys, out.PerKeyCap = slice(pick(keys, capacity, p.randFloat), entries, globalBand, shard)
	return out
}

// LastBand reports the band and distinct-key count of the most recent plan, for metrics. A NEGATIVE
// band means there is nothing to report - either nothing has been planned yet, or the last plan found
// the fleet idle - and the caller should emit no sample rather than label one with the value.
func (p *Planner) LastBand() (band, keyCount int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastBand, p.lastKeys
}

// snapshot returns the current tallies in shard order. The lock covers the map copy only, never the
// planning that follows - holding it across the computation would re-couple the very cycles this design
// decouples.
func (p *Planner) snapshot() []entry {
	p.mu.Lock()
	live := make([]entry, 0, len(p.shards))
	for shard, st := range p.shards {
		live = append(live, entry{shard: shard, t: st})
	}
	p.mu.Unlock()
	sort.Slice(live, func(a, b int) bool { return live[a].shard < live[b].shard })
	return live
}
