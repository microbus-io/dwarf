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
	"slices"
	"sort"
	"sync"
)

// Job holds a step ID, its shard index, and its priority band for the worker pool. Priority is
// assigned by the cache itself: Refill stamps its floor onto every batch job, and Offer stamps the
// offered priority onto the single job it admits.
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
	mu    sync.Mutex
	cond  *sync.Cond
	parts map[int]*partition
	size  int // global capacity, twice the worker count (see Init/Resize); the refillers' plan bound
	// waiting is how many callers are parked in cond.Wait right now, maintained around both Wait sites.
	// Refill wakes at most this many, because a wake beyond it is a call that finds nobody - see wake.
	waiting int
	closed  bool
}

// partition is one shard's slice of the cache. Its floor is the band it is serving for the current window,
// stored rather than read off the head - see floor for why that is frozen.
type partition struct {
	items []Job
	// offered is how many of the CURRENTLY resident items arrived by Offer rather than by Refill. Refill
	// subtracts it from the discard count so the refiller's waste signal is not charged for candidates it
	// never selected. Exact because of the layout: a Refill replaces items wholesale and offers only ever
	// append, so the refill block is always the head and the offered ones the tail - which makes "was the
	// item Pop is about to remove an offered one?" the test len(items) <= offered, with no per-job flag.
	offered int
	// band is the priority band this partition is serving for the current window - set by the Refill that
	// planned it, or by an Offer into an empty partition. FROZEN until the next Refill: see floor.
	band int
	// reserved is how many workers WaitForWork has routed here that have not yet reached their TryPopFrom -
	// in the engine, the workers queued at this shard's gate. Routing ranks by what is left once they are
	// served (unreserved), so workers peeking at the same instant spread across shards instead of all
	// choosing the deepest. A Refill leaves it alone: those workers are still on their way and will pop
	// from the new batch.
	reserved int
}

// unreserved is how many of this partition's candidates no routed worker is already on its way to take.
// Negative when a Refill shrank the partition below its reservations.
func (p *partition) unreserved() int {
	return len(p.items) - p.reserved
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
// Storing it is safe because of its direction. Offer declines anything worse than this band, so every item
// present is at least as good as it: a frozen floor can only ever UNDERSTATE the partition's urgency, which
// delays a buried better-band step by at most a cycle. That rests on Offer never head-inserting a better
// band: with a head-insert, a stored floor would keep advertising the better band after that head had popped,
// OVERSTATING urgency and draining a whole worse-band body ahead of another partition's better work.
func (p *partition) floor() int {
	if len(p.items) == 0 {
		return math.MaxInt
	}
	return p.band
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

// anyUnreserved reports whether some partition holds a candidate no routed worker is on its way to take.
// Callers hold c.mu.
func (c *Cache) anyUnreserved() bool {
	for _, p := range c.parts {
		if p.unreserved() > 0 {
			return true
		}
	}
	return false
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
// candidate stays `pending` in the database and is simply re-selected, so this adds no new state, only a
// smaller bound.
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
// shard index for determinism. It neither makes nor releases reservations: it is the single-call form,
// and routing a worker ahead of its pop is what reservations are for.
func (c *Cache) Pop() (j Job, ok bool) {
	c.mu.Lock()
	for c.totalLen() == 0 && !c.closed {
		c.waiting++
		c.cond.Wait()
		c.waiting--
	}
	best, _ := c.bestPartition(false, nil)
	if best == nil {
		c.mu.Unlock()
		return Job{}, false
	}
	j = best.items[0]
	if len(best.items) <= best.offered {
		best.offered--
	}
	best.items = best.items[1:]
	if len(best.items) == 0 {
		best.items = nil
	}
	c.mu.Unlock()
	return j, true
}

// bestPartition returns the partition to drain and its shard, or nil when none qualifies. Callers hold c.mu.
// Selection is lowest floor; ties break by unreserved depth (deepest first - round-robin would hand every
// shard an equal share of workers regardless of backlog), then by lower shard index for determinism.
//
// routing restricts the choice to partitions with an unreserved candidate, which is how WaitForWork routes a
// worker, so a better-banded partition whose every candidate already has a worker on its way is passed over
// for a worse band on another shard. skip, when set, names shards not to choose.
func (c *Cache) bestPartition(routing bool, skip func(shard int) bool) (*partition, int) {
	var best *partition
	bestShard := 0
	for shard, p := range c.parts {
		if len(p.items) == 0 || (routing && p.unreserved() <= 0) || (skip != nil && skip(shard)) {
			continue
		}
		if best == nil {
			best, bestShard = p, shard
			continue
		}
		bf, pf := best.floor(), p.floor()
		bd, pd := best.unreserved(), p.unreserved()
		switch {
		case pf < bf:
			best, bestShard = p, shard
		case pf == bf && pd > bd:
			best, bestShard = p, shard
		case pf == bf && pd == bd && shard < bestShard:
			best, bestShard = p, shard
		}
	}
	return best, bestShard
}

// WaitForWork blocks until some partition holds a candidate no routed worker is already on its way to take,
// routes the caller to the best such partition, and reserves one of its candidates for it. ok is false ONLY
// when the cache CLOSES, so a worker parked here distinguishes "drained" from "nothing yet" without a second
// signal - the same contract Pop has.
//
// The reservation is released by the caller's TryPopFrom on that shard, whether it takes a candidate or
// finds the partition empty, so every WaitForWork must be followed by exactly one TryPopFrom. A caller that
// stops in between leaves its reservation behind; the crew does so only when its gate closes, which happens
// with this cache already closed.
//
// It exists so a worker can park holding NOTHING. A worker that took a permit and then blocked waiting for
// work would hoard admission capacity it is not using, so the order must be park, then acquire, then take
// work non-blockingly - which is why this is separate from TryPopFrom.
//
// The shard is a HINT and nothing more. By the time the caller acts on it another worker may have taken the
// peeked entry and a better-banded arrival may have landed, which is why the pop that follows must tolerate
// finding the partition empty.
func (c *Cache) WaitForWork() (shard int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for !c.anyUnreserved() && !c.closed {
		c.waiting++
		c.cond.Wait()
		c.waiting--
	}
	if c.closed {
		return 0, false
	}
	p, shard := c.bestPartition(true, nil)
	p.reserved++
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
// It releases the reservation WaitForWork made, and does so on BOTH outcomes: a worker that took its candidate
// leaves the unreserved depth others route by unchanged, and one that found the partition empty must not
// keep it looking spoken for. Floored at zero for a caller that never reserved.
func (c *Cache) TryPopFrom(shard int) (j Job, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return Job{}, false
	}
	p := c.parts[shard]
	if p != nil && p.reserved > 0 {
		p.reserved--
	}
	if p == nil || len(p.items) == 0 {
		return Job{}, false
	}
	j = p.items[0]
	if len(p.items) <= p.offered {
		p.offered--
	}
	p.items = p.items[1:]
	if len(p.items) == 0 {
		p.items = nil
	}
	return j, true
}

// Reroute moves the reservation a WaitForWork made on from to the best other partition with an unreserved
// candidate, skipping the shards in tried, and reports that shard. It is for a caller that could not get
// started on from - in the engine, a shard with no free turn - and has somewhere else to go. ok is false
// when there is nowhere else, or the cache closed; the reservation then stays on from.
//
// A better band left behind is not an inversion: it is on another shard, whose resource had nothing free
// for this caller to use.
func (c *Cache) Reroute(from int, tried []int) (shard int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return from, false
	}
	best, shard := c.bestPartition(true, func(s int) bool { return s == from || slices.Contains(tried, s) })
	if best == nil {
		return from, false
	}
	c.moveReservationLocked(from, best)
	return shard, true
}

// MoveReservation moves a reservation from one shard to another, for a caller that rerouted and then found
// nowhere better to start and goes back to wait where it was first routed.
func (c *Cache) MoveReservation(from, to int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || from == to {
		return
	}
	p := c.parts[to]
	if p == nil {
		p = &partition{}
		c.parts[to] = p
	}
	c.moveReservationLocked(from, p)
}

// moveReservationLocked releases a reservation on from and takes one on to. Callers hold c.mu.
func (c *Cache) moveReservationLocked(from int, to *partition) {
	if p := c.parts[from]; p != nil && p.reserved > 0 {
		p.reserved--
	}
	to.reserved++
}

// PeekShard reports which shard WaitForWork would route to right now, without blocking, reserving or
// removing anything. ok is false when no partition holds an unreserved candidate or the cache is closed.
//
// A HINT - the answer can be stale before the caller acts on it.
func (c *Cache) PeekShard() (shard int, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return 0, false
	}
	p, shard := c.bestPartition(true, nil)
	return shard, p != nil
}

// Refill replaces one shard's partition with batch at the given priority floor and wakes up to ONE WAITER
// PER UNRESERVED CANDIDATE - never all of them, since a wake beyond those cannot be given anything to do.
// The floor is stamped onto every batch job, which is what keeps the partition's floor exact.
//
// The replacement is WHOLESALE, and the empty case must not be short-circuited. An empty batch is the
// scan's statement that nothing is due on that shard, so every candidate still cached there is a dead hint
// a worker would pop and burn a claim-CAS round trip on. Do not return early on len(batch)==0: that keeps
// the whole dead batch, and nothing breaks loudly - the cache self-corrects within a cycle, so it reads as
// wasted work rather than a wedge.
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
	// Deliberately NOT trimmed to c.size. The bound is a sizing target, not an invariant: Offer admits into a
	// non-empty partition only below it and Resize enforces it when the bound moves, but each refiller slices its batch from its own
	// roll of the global plan, so the sum of independently-rolled slices can transiently overshoot the bound and
	// simply drains within a cycle. Trimming here would make the bound strict for the first time, and nothing
	// needs it to be.
	p.items = batch
	// Read under the lock; a waiter Signal has already picked cannot leave Wait until we release, so this
	// cannot undercount the ones about to go. Only unreserved candidates can be routed to a woken waiter.
	wake := min(max(0, p.unreserved()), c.waiting)
	c.mu.Unlock()
	// ONE WAKE PER CANDIDATE, not a broadcast. A refill admits at most the cache's capacity, so a broadcast
	// wakes the WHOLE parked crew to hand out that many jobs and the rest contend on the single mutex and
	// re-park: useful work per wake is fixed while the herd grows with the crew. Go has no wake-N, and a
	// Signal loop IS wake-N.
	//
	// Under-waking cannot strand a candidate, which is what makes the bound safe: a waiter only parks when it
	// finds no unreserved candidate, so a woken worker that loses its race re-checks and keeps going rather
	// than parking on top of work it could take. The floor on parallelism is therefore whoever is already
	// awake, and this adds one worker per unreserved candidate on top of that.
	for range wake {
		c.cond.Signal()
	}
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
//     is reordered. A better band is deliberately not head-inserted: preempting measured no latency gain
//     and costs a fairness bypass.
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
