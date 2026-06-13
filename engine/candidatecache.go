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

package engine

import (
	"math"
	"sync"
)

// job holds a step ID and its shard index for the worker pool.
type job struct {
	stepID int
	shard  int
}

// candidateCache is a small, per-replica bounded set of step candidates produced
// by the refiller's two-level priority+fairness selection. It is NOT a work
// queue: it holds hints, not ownership. Workers pop a candidate and atomically
// CAS-acquire the step before executing, so a stale or duplicated candidate is
// harmless (the loser of the CAS simply pops the next one).
type candidateCache struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    []job
	floor    int // best (lowest) priority represented; math.MaxInt when empty
	size     int // capacity, equal to the worker count
	lowWater int // pop below this requests a refill so draining overlaps refill
	closed   bool
}

func (c *candidateCache) init(workers int) {
	c.cond = sync.NewCond(&c.mu)
	c.items = nil
	c.floor = math.MaxInt
	c.size = max(1, 2*workers)
	c.lowWater = max(1, c.size/2)
	c.closed = false
}

func (c *candidateCache) capacity() int {
	return c.size
}

func (c *candidateCache) pop() (j job, ok bool, needRefill bool) {
	c.mu.Lock()
	for len(c.items) == 0 && !c.closed {
		c.cond.Wait()
	}
	if len(c.items) == 0 {
		c.mu.Unlock()
		return job{}, false, false
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

func (c *candidateCache) refill(batch []job, floor int) {
	c.mu.Lock()
	if c.closed || len(batch) == 0 {
		c.mu.Unlock()
		return
	}
	c.items = batch
	c.floor = floor
	c.mu.Unlock()
	c.cond.Broadcast()
}

func (c *candidateCache) offer(j job, priority int) (needRefill bool) {
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
	c.items = append([]job{j}, c.items...)
	if len(c.items) > c.size {
		c.items = c.items[:c.size]
	}
	c.floor = priority
	c.cond.Signal()
	return true
}

func (c *candidateCache) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func (c *candidateCache) close() {
	if c.cond == nil {
		return
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.cond.Broadcast()
}
