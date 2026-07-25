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
// Its caller is a step ORIGINATION site - most often the successor of a step that just completed - so the
// question it answers is whether this replica can run the step now or must wait for a cycle to select it.
//
// THIS IS NOT A FAIRNESS EXCEPTION, and the framing is what makes the rule fall out. A plan grants a
// fairness key a share of the batch for the CYCLE, not a single dispatch. A step whose predecessor just
// completed is taking the slot that predecessor vacated, inside the window its key already won. So the
// bound is not arbitrary: admit where a slot has actually been freed, which is what `totalLen < size`
// tests. It cannot amplify a tenant's share either - the successor exists only because its predecessor
// freed the worker that will run it - and fairness still fully governs ADMISSION, which flows get started.
//
// An EMPTY partition admits regardless of the global bound. That bound is a sum over ALL partitions, so
// letting it gate this would have one busy shard silence an idle one's doorbell, and the caller reads a
// decline as "this partition needs nothing": measured as 159 of 500 flows stranded in
// fixtures/completionraceflow_test.go. The overshoot is at most one candidate per shard.
//
// THE ONE PRIORITY TEST IS AGAINST A WORSE BAND, and it points the opposite way from the obvious guess.
// A BETTER band is harmless: the step is at least as important as everything this partition was planned
// to run, so appending it violates nothing - it simply dispatches in arrival order rather than ahead of
// the queue. A WORSE one is the real hazard, and is declined: admitting it would have a worker run
// band-900 work while band-100 work sits cached right there, which is a strict-priority inversion rather
// than the soft cross-shard staleness the design already accepts. It waits for a cycle in which its band
// is the global minimum, which is exactly when it is allowed to run.
//
// A better band is NOT head-inserted either, and that is the part that used to be here. Preempting the
// queue with it measured as nothing (see the CLAUDE.md) and cost a fairness bypass to argue about. So it
// waits its turn in arrival order, and the partition goes on advertising the band it was planned at
// (floor is frozen for the window). The residual softness is that the items ahead of it run first, for at
// most a cycle, until the next Refill re-plans from the true global minimum - inside what
// docs/scheduling-and-reliability.md promises: priority is never preemptive, and a new band is served
// within a snapshot cycle or two.
//
// Worth knowing which callers can even bring a better band, because it is not the successor case: priority
// is frozen at Create and inherited by every step of a flow, so a successor never arrives at a better band
// than its own predecessor ran at. Only a NEW FLOW can (Create/Continue/Fork offering its entry step), or
// the narrow straggler where a better band drained, the partition was refilled at a worse one, and that
// band's last successor turns up afterwards.
//
// Admitted candidates are counted (see partition.offered) so Refill's discard count stays the refiller's
// own waste signal rather than being charged for work it never selected.
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
