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

// Package candidates holds the per-replica bounded set of step candidates produced by the
// engine's refillers. It is a hint cache, not a work queue: entries confer no ownership, so a stale
// or duplicated candidate is harmless. The engine claims the underlying step via CAS before running it.
package candidates

import (
	"math"
	"sort"
	"sync"
)

// Job holds a step ID, its shard index, and its priority band for the worker pool. Priority is
// assigned by the cache itself (Refill stamps its floor onto every batch job; Offer stamps the
// offered priority onto the single job it admits), so a partition's floor is always derivable from
// its head.
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
// math.MaxInt when empty - never stored, because a stored one is a second source of truth that can
// drift from the items while costing nothing to avoid.
//
// Its floor is the band it is serving for the current window - see floor for why that is frozen rather
// than read off the head.
type partition struct {
	items []Job
	// lastFill is the depth of the last wholesale Refill; the partition's low-water mark is half of
	// it. Partition sizes are dynamic (each refiller's slice of the global plan), so a global
	// constant would over-trigger small partitions and under-trigger large ones.
	lastFill int
	// offered is how many of the CURRENTLY resident items arrived by Offer rather than by Refill. Refill
	// subtracts it from the discard count so the refiller's waste signal is not charged for candidates it
	// never selected. Exact because of the layout: a Refill replaces items wholesale and offers only ever
	// append, so the refill block is always the head and the offered ones the tail - which makes "was the
	// item Pop is about to remove an offered one?" the test len(items) <= offered, with no per-job flag.
	offered int
	// band is the priority band this partition is serving for the current window - set by the Refill that
	// planned it, or by an Offer into an empty partition. FROZEN until the next Refill: see floor.
	band int
}

// floor is the band this partition is serving for the current window, or math.MaxInt when it is empty.
// It is Pop's key for choosing which partition to drain, and Offer's bar for what it will admit.
//
// FROZEN for the window rather than read off the head, and the difference is visible: Offer appends a
// better-banded step behind the planned batch, so a head-derived floor would DIP as that item reached the
// front and rise again after it popped. A partition planned at band 6 holding `6 6 5 6` would advertise
// 6,6,5,6 as it drained, oscillating Pop's partition choice - and worse, Offer's own admission test would
// swing with it, so an offered band-6 step would be declined or accepted purely on pop timing.
//
// Storing it is safe HERE in a way it was not before, and the direction is the whole argument. Offer
// declines anything worse than this band, so every item present is at least as good as it: a frozen floor
// can only ever UNDERSTATE the partition's urgency, which delays a buried better-band step by at most a
// cycle. The old stored floor failed the other way - back when Offer could head-insert, it kept
// advertising band 1 after the band-1 head had popped, OVERSTATING urgency and draining a whole band-5
// body ahead of another partition's band-3 work.
func (p *partition) floor() int {
	if len(p.items) == 0 {
		return math.MaxInt
	}
	return p.band
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
	best, _ := c.bestPartition()
	if best == nil {
		c.mu.Unlock()
		return Job{}, false, false
	}
	j = best.items[0]
	if len(best.items) <= best.offered {
		best.offered--
	}
	best.items = best.items[1:]
	if len(best.items) == 0 {
		best.items = nil
	}
	needRefill = len(best.items) <= best.lowWater()
	c.mu.Unlock()
	return j, true, needRefill
}

// bestPartition returns the partition Pop would drain and its shard, or nil when every partition is empty.
// Callers hold c.mu. Selection is lowest floor; ties break by depth (deepest first - round-robin would hand
// every shard an equal share of workers regardless of backlog), then by lower shard index for determinism.
func (c *Cache) bestPartition() (*partition, int) {
	var best *partition
	bestShard := 0
	for shard, p := range c.parts {
		if len(p.items) == 0 {
			continue
		}
		if best == nil {
			best, bestShard = p, shard
			continue
		}
		bf, pf := best.floor(), p.floor()
		switch {
		case pf < bf:
			best, bestShard = p, shard
		case pf == bf && len(p.items) > len(best.items):
			best, bestShard = p, shard
		case pf == bf && len(p.items) == len(best.items) && shard < bestShard:
			best, bestShard = p, shard
		}
	}
	return best, bestShard
}

// WaitForWork blocks until some partition holds a candidate, and reports which shard Pop would drain. ok is
// false ONLY when the cache CLOSES, so a worker parked here distinguishes "drained" from "nothing yet"
// without a second signal - the same contract Pop has always had.
//
// It exists so a worker can park holding NOTHING. A worker that took a permit and then blocked waiting for
// work would hoard admission capacity it is not using, so the order must be park, then acquire, then take
// work non-blockingly - which is what splits the old Pop into this plus TryPopFrom.
//
// The shard is a HINT and nothing more. By the time the caller acts on it another worker may have taken the
// peeked entry and a better-banded arrival may have landed, which is why the pop that follows must tolerate
// finding the partition empty.
func (c *Cache) WaitForWork() (shard int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for c.totalLen() == 0 && !c.closed {
		c.cond.Wait()
	}
	if c.closed {
		return 0, false
	}
	_, shard = c.bestPartition()
	return shard, true
}

// TryPopFrom removes and returns the front candidate of ONE shard's partition without blocking. ok is false
// when that partition is empty (lost the race, or it drained) or the cache is closed.
//
// THE TWO CASES ARE DELIBERATELY NOT DISTINGUISHED HERE, because the caller must treat them identically:
// retry the park. It is WaitForWork that reports a close, and a worker that returned on an empty partition
// instead of looping would erode the crew under exactly the contention that caused the race - on a signal
// that says nothing about whether that worker was surplus.
//
// needRefill signals that this partition has drained to its low-water mark, exactly as Pop's does.
func (c *Cache) TryPopFrom(shard int) (j Job, ok bool, needRefill bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return Job{}, false, false
	}
	p := c.parts[shard]
	if p == nil || len(p.items) == 0 {
		return Job{}, false, false
	}
	j = p.items[0]
	if len(p.items) <= p.offered {
		p.offered--
	}
	p.items = p.items[1:]
	if len(p.items) == 0 {
		p.items = nil
	}
	return j, true, len(p.items) <= p.lowWater()
}

// PeekShard reports which shard Pop would drain right now, without blocking and without removing anything.
// ok is false when every partition is empty or the cache is closed.
//
// It is the non-blocking twin of WaitForWork, and exists for the demand side's growth decision: "is there
// work waiting, and on which shard" is one question, and answering it with two calls would let the two
// halves disagree. A HINT either way - the answer can be stale before the caller acts on it, which there
// costs one goroutine that parks harmlessly.
func (c *Cache) PeekShard() (shard int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, false
	}
	p, shard := c.bestPartition()
	return shard, p != nil
}

// Refill replaces one shard's partition with batch at the given priority floor and wakes all waiters. The
// floor is stamped onto every batch job, which is what keeps the partition's floor exact.
//
// The replacement is WHOLESALE, and the empty case must not be short-circuited. An empty batch is the
// scan's statement that nothing is due on that shard, so every candidate still cached there is a dead hint
// a worker would pop and burn a claim-CAS round trip on. An early return on len(batch)==0 - what this once
// did - keeps the whole dead batch, and nothing breaks loudly: the cache self-corrects within a cycle, so
// it reads as wasted work rather than a wedge, which is why the bug was invisible.
//
// The caller must NOT route a FAILED scan here. An error means "unknown", not "nothing is due", and
// replacing a healthy partition with nothing on a transient blip idles its workers in Pop.
//
// discarded counts the candidates thrown away un-popped, MINUS those the doorbell admitted - it is the
// refiller's oversupply signal, and charging it for work it never selected would read worst exactly when
// the doorbell is working best.
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
	// Only the refiller's OWN un-popped selections are waste it can act on; candidates the doorbell
	// admitted were never its choice, and charging them here would make the oversupply ratio read high
	// exactly when the doorbell is working well.
	discarded = len(p.items) - p.offered
	p.offered = 0
	if len(batch) == 0 {
		batch = nil // Pop's empty representation, so Len/append behave identically either way
	}
	p.band = floor
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

// Offer admits a candidate onto its shard's partition (routing is j.Shard) without consulting the plan.
// Callers are step-origination sites, most often the successor of a step that just completed, so what it
// answers is whether this replica can run the step now or must wait for a cycle to select it.
//
// Three rules, and each is load-bearing:
//
//   - An EMPTY partition admits REGARDLESS of the global bound. That bound is a sum over all partitions, so
//     gating this on it lets one busy shard silence an idle one's doorbell - and a caller reads a decline as
//     "this partition needs nothing", so the step then waits for a scan nobody asked for.
//   - A WORSE band than the partition's is DECLINED. Admitting it would run band-900 work while band-100
//     work sits cached right here, a strict-priority inversion rather than the soft staleness the design
//     accepts. Note the direction: a BETTER band is harmless and simply appends.
//   - Otherwise admit while a slot has been freed (totalLen < size), at the TAIL so nothing the plan chose
//     is reordered. A better band is deliberately not head-inserted; preempting was built, measured as
//     nothing, and removed.
//
// Admissions are counted (partition.offered) so Refill's discard count stays the refiller's own waste.
func (c *Cache) Offer(j Job, priority int) (admitted bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	p := c.parts[j.Shard]
	if p == nil {
		p = &partition{}
		c.parts[j.Shard] = p
	}
	j.Priority = priority
	switch {
	case len(p.items) == 0:
		// Nothing planned here, so nothing to be worse than - and this arrival sets the band the partition
		// serves until the next Refill.
		p.items = []Job{j}
		p.band = priority
	case priority > p.floor():
		return false // worse than this partition's planned band - see above
	case c.totalLen() < c.size:
		p.items = append(p.items, j)
	default:
		return false // no vacated slot to take
	}
	p.offered++
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
