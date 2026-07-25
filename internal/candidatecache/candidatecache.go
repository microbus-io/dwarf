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

// Package candidatecache holds the per-replica bounded set of step candidates produced by the
// engine's refillers. It is a hint cache, not a work queue: entries confer no ownership, so a stale
// or duplicated candidate is harmless. The engine claims the underlying step via CAS before running it.
package candidatecache

import (
	"math"
	"sort"
	"sync"
)

// Job holds a step ID, its shard index, and its priority band for the worker pool. Priority is
// assigned by the cache itself (Refill stamps its floor onto every batch job; Offer stamps the
// offered priority), so a partition's floor is always derivable from its head.
type Job struct {
	StepID   int
	Shard    int
	Priority int
}

// Cache is a bounded per-replica set of step candidates - hints, not ownership - partitioned by
// shard. Each shard's refiller wholesale-replaces its own partition; workers Pop from whichever
// partition advertises the best (lowest) floor. One mutex and one condition variable span all
// partitions, so a worker blocked on an empty cache wakes when ANY partition fills - N separate
// cond vars would let a worker sleep through another shard's refill.
type Cache struct {
	mu     sync.Mutex
	cond   *sync.Cond
	parts  map[int]*partition
	size   int // global capacity, twice the worker count (see Init/Resize); the refillers' plan bound
	closed bool
}

// partition is one shard's slice of the cache. Its floor is DERIVED - the head item's priority, or
// math.MaxInt when empty - never stored. A stored floor lies after Offer head-inserts one better
// item over a body at a worse band: the head pops, the stored floor keeps advertising the better
// band, and floor-based partition selection then drains the whole worse-band body ahead of another
// partition's better work. Deriving from items[0] is exact: Offer inserts only strictly-better
// heads and Refill batches are uniform at their band, so the head is always the best item present.
type partition struct {
	items []Job
	// lastFill is the depth of the last wholesale Refill; the partition's low-water mark is half of
	// it. Partition sizes are dynamic (each refiller's slice of the global plan), so a global
	// constant would over-trigger small partitions and under-trigger large ones.
	lastFill int
}

// floor returns the partition's best (lowest) represented priority: the head item's, since items
// are best-first (see the partition doc).
func (p *partition) floor() int {
	if len(p.items) == 0 {
		return math.MaxInt
	}
	return p.items[0].Priority
}

// lowWater is the pop-below-this-requests-a-refill threshold, half the last wholesale refill.
func (p *partition) lowWater() int {
	return max(1, p.lastFill/2)
}

// Init sizes the cache to twice the worker count and resets it for reuse.
func (c *Cache) Init(workers int) {
	c.cond = sync.NewCond(&c.mu)
	c.parts = map[int]*partition{}
	c.size = max(1, 2*workers)
	c.closed = false
}

// Capacity returns the cache's global bound (twice the worker count). It bounds the SUM of the
// partitions, and it is the capacity each refiller draws its global plan at.
func (c *Cache) Capacity() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

// totalLen is the current number of buffered candidates across all partitions. Callers hold c.mu.
func (c *Cache) totalLen() int {
	n := 0
	for _, p := range c.parts {
		n += len(p.items)
	}
	return n
}

// Resize re-bounds the cache to a new worker count, live, and applies to ALL partitions: when the
// sum overflows the new bound, each partition's tail is trimmed proportionally to its depth
// (largest-remainder rounding, lower shard first on ties, so the trim is deterministic). A trimmed
// candidate stays `pending` in the database and is simply re-selected - exactly what already happens
// when an Offer's head-insert pushes one past the bound - so this adds no new state, only a smaller
// bound.
//
// The engine calls this when the observed replica count changes and each shard's connection budget is
// re-divided. The cache MUST follow that split: it is sized from what the replica can actually CLAIM,
// and one still holding a cache sized for the WHOLE fleet's budget is handed far more candidates than
// it can claim - stale hints whose claim CAS loses to a peer, and wasted round-trips, precisely when
// the fleet is busiest.
func (c *Cache) Resize(workers int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.size = max(1, 2*workers)
	total := c.totalLen()
	if total <= c.size {
		return
	}
	// Proportional targets: floor(len*size/total) each, then hand the leftover slots out by largest
	// remainder (tie: lower shard), never past a partition's current depth.
	shards := make([]int, 0, len(c.parts))
	for s := range c.parts {
		shards = append(shards, s)
	}
	sort.Ints(shards)
	targets := make(map[int]int, len(shards))
	assigned := 0
	for _, s := range shards {
		t := len(c.parts[s].items) * c.size / total
		targets[s] = t
		assigned += t
	}
	order := append([]int(nil), shards...)
	sort.SliceStable(order, func(a, b int) bool {
		ra := len(c.parts[order[a]].items) * c.size % total
		rb := len(c.parts[order[b]].items) * c.size % total
		if ra != rb {
			return ra > rb
		}
		return order[a] < order[b]
	})
	for _, s := range order {
		if assigned >= c.size {
			break
		}
		if targets[s] < len(c.parts[s].items) {
			targets[s]++
			assigned++
		}
	}
	for _, s := range shards {
		p := c.parts[s]
		if len(p.items) > targets[s] {
			p.items = p.items[:targets[s]]
			if len(p.items) == 0 {
				p.items = nil
			}
		}
	}
}

// Pop removes and returns the front candidate of the best partition, blocking until one is available
// or the cache closes. Partition selection is lowest floor; ties break by depth (deepest first -
// round-robin would hand every shard an equal share of workers regardless of backlog), then by lower
// shard index for determinism.
//
// needRefill signals that the POPPED partition has drained to its low-water mark, so the caller
// should nudge that shard's refiller (the popped job's Shard names it) and draining overlaps refill.
func (c *Cache) Pop() (j Job, ok bool, needRefill bool) {
	c.mu.Lock()
	for c.totalLen() == 0 && !c.closed {
		c.cond.Wait()
	}
	var best *partition
	for _, p := range c.parts {
		if len(p.items) == 0 {
			continue
		}
		if best == nil {
			best = p
			continue
		}
		bf, pf := best.floor(), p.floor()
		switch {
		case pf < bf:
			best = p
		case pf == bf && len(p.items) > len(best.items):
			best = p
		case pf == bf && len(p.items) == len(best.items) && p.items[0].Shard < best.items[0].Shard:
			best = p
		}
	}
	if best == nil {
		c.mu.Unlock()
		return Job{}, false, false
	}
	j = best.items[0]
	best.items = best.items[1:]
	if len(best.items) == 0 {
		best.items = nil
	}
	needRefill = len(best.items) <= best.lowWater()
	c.mu.Unlock()
	return j, true, needRefill
}

// Refill replaces one shard's partition with batch at the given priority floor and wakes all
// waiters. The floor is stamped onto every batch job (a refill batch is uniform at its band), which
// is what keeps the partition's derived floor exact. The replacement is wholesale: an EMPTY batch
// empties the partition, exactly as draining the last item through Pop does.
//
// The empty case must not be skipped. An empty batch is the scan's statement that NOTHING IS DUE on
// that shard, so every candidate still cached there is a step that is no longer pending - a dead
// hint. An early return on len(batch)==0 (what this once did) keeps that whole dead batch, and each
// worker then pops one and burns a claim-CAS round-trip discovering it is gone, up to a partition's
// worth of them. Neither breaks liveness - a CAS loser re-requests a refill, so the cache
// self-corrects within a cycle - which is exactly why the bug was invisible; it is wasted work, not
// a wedge.
//
// The caller must NOT route a *failed* scan here: a scan error means "unknown", not "nothing is
// due", and wholesale-replacing a healthy partition with nothing on a transient DB blip would idle
// its workers in Pop. runShardRefill returns early instead (see the error path in
// engine/scheduling.go).
//
// discarded is the number of candidates the replace threw away un-popped. It is the refiller's WASTE
// signal: a refiller is triggered after every processStep on its shard and turns far faster than the
// workers drain, so a replace routinely discards candidates the previous scan selected and paid to
// fetch. Those steps stay `pending` and are simply re-selected, so this is cost, never loss - but the
// ratio against the batch size is what says whether the refiller is oversupplying, which no other
// instrument can see.
func (c *Cache) Refill(shard int, batch []Job, floor int) (discarded int) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return 0
	}
	p := c.parts[shard]
	if p == nil {
		p = &partition{}
		c.parts[shard] = p
	}
	discarded = len(p.items)
	if len(batch) == 0 {
		batch = nil // Pop's empty representation, so Len/append behave identically either way
	}
	for i := range batch {
		batch[i].Priority = floor
	}
	// Deliberately NOT trimmed to c.size. The bound is a sizing target, not an invariant: Offer enforces it on a
	// head-insert and Resize enforces it when the bound moves, but each refiller slices its batch from its own
	// roll of the global plan, so the sum of independently-rolled slices can transiently overshoot the bound and
	// simply drains within a cycle. Trimming here would make the bound strict for the first time, and nothing
	// needs it to be.
	p.items = batch
	p.lastFill = len(batch)
	c.mu.Unlock()
	c.cond.Broadcast()
	return discarded
}

// Offer front-loads a single higher-priority candidate onto its shard's partition (routing is j.Shard).
//
// It reports needRefill: this partition needs the refiller. True when it is empty (a scan must supply
// it) or when a head-insert happened (only one pioneer is admitted per band-opening, so a scan must top
// up the rest of the band). A candidate whose priority is no better than the partition's floor is
// dropped and needs nothing.
//
// A head-insert serves the offered step itself immediately - it is popped next - so the refill it asks
// for is for the step's SIBLINGS, and can wait out the caller's scan floor like any other nudge.
func (c *Cache) Offer(j Job, priority int) (needRefill bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	p := c.parts[j.Shard]
	if p == nil || len(p.items) == 0 {
		return true
	}
	if priority >= p.floor() {
		return false
	}
	j.Priority = priority
	p.items = append([]Job{j}, p.items...)
	if c.totalLen() > c.size {
		p.items = p.items[:len(p.items)-1]
	}
	c.cond.Signal()
	return true
}

// Len returns the number of buffered candidates across all partitions.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.totalLen()
}

// Close permanently unblocks all waiters. Offer/Refill become no-ops after close.
func (c *Cache) Close() {
	if c.cond == nil {
		return
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.cond.Broadcast()
}
