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
// engine's refiller. It is a hint cache, not a work queue: entries confer no ownership, so a stale
// or duplicated candidate is harmless. The engine claims the underlying step via CAS before running it.
package candidatecache

import (
	"math"
	"sync"
)

// Job holds a step ID and its shard index for the worker pool.
type Job struct {
	StepID int
	Shard  int
}

// Cache is a bounded per-replica set of step candidates - hints, not ownership.
type Cache struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    []Job
	floor    int // best (lowest) priority represented; math.MaxInt when empty
	size     int // capacity, twice the worker count (see Init/Resize); the per-fairness-key row cut in the refiller band scan
	lowWater int // pop below this requests a refill so draining overlaps refill
	closed   bool
}

// Init sizes the cache to twice the worker count and resets it for reuse.
func (c *Cache) Init(workers int) {
	c.cond = sync.NewCond(&c.mu)
	c.items = nil
	c.floor = math.MaxInt
	c.size = max(1, 2*workers)
	c.lowWater = max(1, c.size/2)
	c.closed = false
}

// Capacity returns the cache's bound (twice the worker count).
func (c *Cache) Capacity() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.size
}

// Resize re-bounds the cache to a new worker count, live, and trims the tail if it now overflows. A trimmed
// candidate stays `pending` in the database and is simply re-selected - exactly what already happens when an
// Offer's head-insert pushes one past the bound - so this adds no new state, only a smaller bound.
//
// The engine calls this when the observed replica count changes and each shard's connection budget is re-divided.
// The cache MUST follow that split: it is sized from what the replica can actually CLAIM, and one still holding a
// cache sized for the WHOLE fleet's budget is handed far more candidates than it can claim - stale hints whose
// claim CAS loses to a peer, and wasted round-trips, precisely when the fleet is busiest.
func (c *Cache) Resize(workers int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.size = max(1, 2*workers)
	c.lowWater = max(1, c.size/2)
	if len(c.items) > c.size {
		c.items = c.items[:c.size]
	}
}

// Pop removes and returns the front candidate, blocking until one is available or the cache closes.
// needRefill signals that the cache has drained to its low-water mark so draining overlaps refill.
func (c *Cache) Pop() (j Job, ok bool, needRefill bool) {
	c.mu.Lock()
	for len(c.items) == 0 && !c.closed {
		c.cond.Wait()
	}
	if len(c.items) == 0 {
		c.mu.Unlock()
		return Job{}, false, false
	}
	j = c.items[0]
	c.items = c.items[1:]
	if len(c.items) == 0 {
		c.items = nil
		c.floor = math.MaxInt
	}
	needRefill = len(c.items) <= c.lowWater
	c.mu.Unlock()
	return j, true, needRefill
}

// Refill replaces the cache contents with batch at the given priority floor and wakes all waiters. The
// replacement is wholesale: an EMPTY batch empties the cache and resets the floor, exactly as draining the
// last item through Pop does.
//
// The empty case must not be skipped. An empty batch is the scan's statement that NOTHING IS DUE, so every
// candidate still cached is a step that is no longer pending - a dead hint. An early return on
// len(batch)==0 (what this did) keeps that whole dead batch, and each worker then pops one and burns a
// claim-CAS round-trip discovering it is gone, up to a cacheful of them. The floor rides along stale,
// advertising a band the cache no longer holds. Neither breaks liveness - a CAS loser re-requests a refill,
// so the cache self-corrects within a cycle - which is exactly why the bug was invisible; it is wasted work
// and a lying floor, not a wedge. The caller already computes the right floor for this case (MaxInt).
//
// The caller must NOT route a *failed* scan here: a scan error means "unknown", not "nothing is due", and
// wholesale-replacing a healthy cache with nothing on a transient DB blip would idle every worker in Pop.
// runRefill returns early instead (see the error path in engine/scheduling.go).
func (c *Cache) Refill(batch []Job, floor int) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if len(batch) == 0 {
		batch = nil // Pop's empty representation, so Len/append behave identically either way
	}
	// Deliberately NOT trimmed to c.size. The bound is a sizing target, not an invariant: Offer enforces it on a
	// head-insert and Resize enforces it when the bound moves, but a refill batch that was sized from a capacity
	// a live Resize has since shrunk simply overshoots for one cycle and drains. Trimming here would make the
	// bound strict for the first time, and nothing needs it to be.
	c.items = batch
	c.floor = floor
	c.mu.Unlock()
	c.cond.Broadcast()
}

// Offer front-loads a single higher-priority candidate. It returns needRefill=true when the cache is
// empty (the caller should refill); a candidate whose priority is no better than the current floor is
// dropped.
func (c *Cache) Offer(j Job, priority int) (needRefill bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	if len(c.items) == 0 {
		return true
	}
	if priority >= c.floor {
		return false
	}
	c.items = append([]Job{j}, c.items...)
	if len(c.items) > c.size {
		c.items = c.items[:c.size]
	}
	c.floor = priority
	c.cond.Signal()
	return true
}

// Len returns the number of buffered candidates.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
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
