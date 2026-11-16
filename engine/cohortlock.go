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
	"context"
	"slices"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/diagnostics"
)

// cohortStripe is one stripe of e.cohortLocks: a FIFO lock whose waiters can run a callback periodically while they
// wait without losing their place in line, and whose unlock hands the stripe to the longest waiter. The zero value
// is an unlocked stripe. The uncontended path allocates nothing; a waiter allocates a channel and a timer.
type cohortStripe struct {
	mu    sync.Mutex
	held  bool
	queue []chan struct{} // waiters in arrival order; unlock hands the stripe to the head by closing its channel
	// batches are the open arrival batches queued on this stripe, one per cohort (see arrivalBatch).
	batches map[arrivalKey]*arrivalBatch
	// atFanIn counts this replica's workers at each of the stripe's cohorts' fan-ins, and whether the cohort's
	// lineage is held in the cache (enterFanIn). A cohort with none there has no entry.
	atFanIn map[arrivalKey]fanInCount
}

// fanInCount is one cohort's workers at its fan-in on this replica, and whether its lineage is held.
type fanInCount struct {
	workers int
	held    bool
}

// join adds a pure arrival to its cohort's open batch on this stripe, or opens one it leads (idx 0). It returns a nil
// batch when the stripe was free, and then the caller holds it.
func (s *cohortStripe) join(key arrivalKey, d *dispatch) (b *arrivalBatch, idx int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.held {
		s.held = true
		return nil, 0
	}
	if b = s.batches[key]; b != nil {
		b.members = append(b.members, d)
		return b, len(b.members) - 1
	}
	if s.batches == nil {
		s.batches = map[arrivalKey]*arrivalBatch{}
	}
	b = &arrivalBatch{key: key, members: []*dispatch{d}, done: make(chan struct{})}
	s.batches[key] = b
	return b, 0
}

// seal closes a batch to new members and returns them; an arrival joining after it opens the next batch.
func (s *cohortStripe) seal(b *arrivalBatch) []*dispatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.batches[b.key] == b {
		delete(s.batches, b.key)
	}
	return b.members
}

// lock acquires the stripe. While it waits it calls tick, first after the delay first and then after whatever delay
// the previous tick returned, keeping its place in line; tick returning false abandons the wait, and lock then
// returns false without the stripe. A delay at or below zero ticks at once. A panic in tick withdraws the waiter
// before it propagates, so the stripe is never handed to a goroutine that is gone.
func (s *cohortStripe) lock(first time.Duration, tick func() (next time.Duration, ok bool)) bool {
	s.mu.Lock()
	if !s.held {
		s.held = true
		s.mu.Unlock()
		return true
	}
	turn := make(chan struct{})
	s.queue = append(s.queue, turn)
	s.mu.Unlock()

	timer := time.NewTimer(first)
	defer timer.Stop()
	for {
		select {
		case <-turn:
			return true
		case <-timer.C:
			next, ok := s.tick(turn, tick)
			if !ok {
				s.abandon(turn)
				return false
			}
			timer.Reset(next)
		}
	}
}

// tick runs one tick for the waiter holding turn, withdrawing it if the callback panics.
func (s *cohortStripe) tick(turn chan struct{}, tick func() (time.Duration, bool)) (time.Duration, bool) {
	defer func() {
		if r := recover(); r != nil {
			s.abandon(turn)
			panic(r)
		}
	}()
	return tick()
}

// abandon withdraws a waiter. If the stripe was handed to it while it was out ticking, it now holds the stripe, and
// passes it on.
func (s *cohortStripe) abandon(turn chan struct{}) {
	s.mu.Lock()
	if i := slices.Index(s.queue, turn); i >= 0 {
		s.queue = slices.Delete(s.queue, i, i+1)
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.unlock()
}

// unlock releases the stripe, handing it to the longest waiter if there is one.
func (s *cohortStripe) unlock() {
	s.mu.Lock()
	if len(s.queue) == 0 {
		s.held = false
		s.queue = nil
	} else {
		next := s.queue[0]
		s.queue[0] = nil
		s.queue = s.queue[1:]
		close(next)
	}
	s.mu.Unlock()
}

// enterFanIn counts d's worker among this replica's workers at a cohort's fan-in, until leaveFanIn, and holds the
// cohort's lineage in the cache while they number fanInHoldAt or more - so no worker takes another of the cohort's
// candidates until enough of its arrivals have landed. It reports whether it counted the worker; one already counted
// (an arrival that fails on its way through, reaching failStep's stripe from the transition's) is not counted twice.
//
// The count is what paces a wide cohort's claims to its fan-in. A worker waiting here holds its candidate, so the
// crew counts it busy, and holds no turn, so the gate does not stop the next claim: without the hold, a cohort whose
// branches finish faster than its arrivals land grows the crew by a worker per branch claimed, up to the crew's
// maximum. Only workers HERE are counted, never those in a task, so a cohort of long tasks still grows the crew as
// wide as it needs.
//
// The cache is called under the stripe's mutex, so two crossings of the depth cannot reach it out of order; the cache
// never calls out, so the order stripe-then-cache cannot invert.
func (e *Engine) enterFanIn(stripe *cohortStripe, shard, spawn int, d *dispatch) bool {
	if d.atFanIn {
		return false
	}
	d.atFanIn = true
	key := arrivalKey{shard: shard, spawn: spawn}
	stripe.mu.Lock()
	defer stripe.mu.Unlock()
	if stripe.atFanIn == nil {
		stripe.atFanIn = map[arrivalKey]fanInCount{}
	}
	c := stripe.atFanIn[key]
	c.workers++
	e.settleFanInHold(key, &c)
	stripe.atFanIn[key] = c
	return true
}

// leaveFanIn undoes an enterFanIn that reported true.
func (e *Engine) leaveFanIn(stripe *cohortStripe, shard, spawn int, d *dispatch) {
	d.atFanIn = false
	key := arrivalKey{shard: shard, spawn: spawn}
	stripe.mu.Lock()
	defer stripe.mu.Unlock()
	c := stripe.atFanIn[key]
	c.workers--
	e.settleFanInHold(key, &c)
	if c.workers <= 0 {
		delete(stripe.atFanIn, key)
		return
	}
	stripe.atFanIn[key] = c
}

// settleFanInHold holds or releases a cohort's lineage to match its count. The depth is read on every call rather
// than kept, so a cohort whose depth moved is settled by its next arrival or departure, and the last to leave always
// releases. Callers hold the stripe's mutex.
func (e *Engine) settleFanInHold(key arrivalKey, c *fanInCount) {
	at := e.fanInHoldAt.Load()
	hold := at > 0 && int64(c.workers) >= at
	if hold == c.held {
		return
	}
	if hold {
		e.cache.HoldLineage(key.shard, key.spawn)
	} else {
		e.cache.ReleaseLineage(key.shard, key.spawn)
	}
	c.held = hold
}

// lockCohortStripe parks a step's worker on its cohort's stripe, keeping the step's lease alive while it waits. It
// ticks when the lease has `lead` left - half of leaseMargin or of the extension, whichever is shorter - and each tick
// extends it to a full extension on a turn of its own. The first tick is timed from the lease the claim set
// (leaseDeadline), so a waiter served before then extends nothing; after a tick the next is one extension minus the
// lead later, measured from when the tick began, which the extension cannot predate. It returns false if an extension
// found the step taken back - re-claimed, rewound or terminalized - in which case the worker no longer owns it and must
// abandon. A database error on an extension keeps waiting on the lease already held and tries again a lead later; the
// fence still protects the step if that lapses.
//
// The caller parks holding NO turn: the stripe exists so a losing sibling waits without occupying a connection, and
// a turn is that occupancy one level up.
func (e *Engine) lockCohortStripe(ctx context.Context, stripe *cohortStripe, d *dispatch) bool {
	lead := e.leaseLead()
	return stripe.lock(time.Until(e.leaseDeadline(d))-lead, func() (time.Duration, bool) {
		return e.extendWaitingLease(ctx, d, lead)
	})
}

// leaseLead is how much lease a waiter keeps ahead of it: it extends when this much is left.
func (e *Engine) leaseLead() time.Duration {
	return min(e.leaseMargin, e.persistLeaseExtension) / 2
}

// extendWaitingLease extends a waiting step's lease to a full extension, on a turn of its own, and returns how long
// until the next extension is due - measured from before the turn, which the extension cannot predate - and whether
// the step is still this worker's. A database error keeps the lease it has and is due again a lead later.
func (e *Engine) extendWaitingLease(ctx context.Context, d *dispatch, lead time.Duration) (next time.Duration, held bool) {
	if e.diags.Seams().Enabled() && e.diags.Seams().IsFault(diagnostics.FaultPanicLeaseTick) {
		panic("injected fault: " + diagnostics.FaultPanicLeaseTick)
	}
	start := time.Now()
	tctx, doneTurn := e.dbTurn(ctx, d.shard)
	defer doneTurn()
	held, err := e.extendLease(tctx, d.db, d.stepID, d.row.leaseSeq, e.persistLeaseExtension)
	if err != nil {
		return lead, true
	}
	return time.Until(start.Add(e.persistLeaseExtension)) - lead, held
}

// leaseDeadline is a lower bound on when the lease the dispatch's claim set runs out: the claim's own NOW plus the
// step's budget plus the margin, with the local time before the claim was sent standing in for that NOW.
func (e *Engine) leaseDeadline(d *dispatch) time.Time {
	return d.leaseFrom.Add(time.Duration(d.row.timeBudgetMs)*time.Millisecond + e.leaseMargin)
}
