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

// Package permits bounds how many goroutines may be doing DATABASE work on one shard at a time.
//
// Work about to START and work that has already HAPPENED are counted SEPARATELY, against reservations
// sized independently:
//
//   - AcquireEnter blocks until one of the shard's ENTER permits is free.
//   - AcquireExit blocks until one of the shard's EXIT permits is free.
//
// The split is what keeps either side from starving the other, and it is why BOTH can simply block. A
// single pool forces a choice about who wins a contested permit, and both answers were measured failing:
// served evenly, exits lose at random and queue behind entry; served with exits given precedence, entry -
// which IS dispatch - never runs, and short-task throughput collapsed 3x. Separate counts remove the
// choice rather than answering it, so neither side needs a priority rule or an escape hatch.
//
// An exit blocking cannot deadlock: it waits only behind OTHER exits, and nothing an entering caller does
// can hold an exit permit. The wait is therefore bounded by how fast exits drain, which is the resource's
// own service rate.
//
//	rel, ok := set.AcquireEnter(shard)   // blocks; ok=false only when CLOSED
//	if !ok { return }
//	... work that holds the resource ...
//	rel()
//
// Close is the stop signal, mirroring the candidate cache: it permanently unblocks every waiter, and
// AcquireEnter reports !ok forever after. Sizes are live (Resize), because the pools they derive from are.
package permits

import "sync"

// Set is a per-shard pair of signed permit counts, safe for concurrent use by any number of goroutines.
// The zero value is not usable; call New.
type Set struct {
	mu sync.Mutex
	// enter and exit are the two reservations, each with its own waiting queue per shard. Separate conds
	// are what make the split work: one queue would wake a caller whose own count is empty and put it
	// straight back to sleep. Per SHARD as well, because a release on shard 2 must not wake a waiter on
	// shard 5 - that is a lost wakeup, and on a multi-shard engine it strands the free permit.
	enter, exit shardCounts
	closed      bool
}

// shardCounts is one reservation across all shards: what is left, the configured ceiling, and who waits.
// Maps rather than slices because shard indices are caller-chosen and 1-based; created on demand and never
// removed, since the shard set is fixed for a run and there is nothing to garbage-collect.
type shardCounts struct {
	avail map[int]int64
	size  map[int]int64
	conds map[int]*sync.Cond
}

func newShardCounts() shardCounts {
	return shardCounts{avail: map[int]int64{}, size: map[int]int64{}, conds: map[int]*sync.Cond{}}
}

// New returns an empty Set. A shard with no configured size admits nothing until Resize gives it one, so
// every shard the engine intends to dispatch on must be sized before its workers start.
func New() *Set {
	return &Set{enter: newShardCounts(), exit: newShardCounts()}
}

// condFor returns this reservation's waiting queue for a shard, creating it on first use. Callers hold
// the Set's mutex, which every cond here is built over.
func (c *shardCounts) condFor(mu *sync.Mutex, shard int) *sync.Cond {
	cond := c.conds[shard]
	if cond == nil {
		cond = sync.NewCond(mu)
		c.conds[shard] = cond
	}
	return cond
}

// Resize sets a shard's two ceilings, live. Each available count moves by ITS OWN DELTA, never to the new
// value, so permits held right now stay held - assigning the ceiling outright would hand those out a second
// time. Shrinking below what is held drives a count negative, which blocks that side until enough holders
// release, which is self-correcting and needs no special case.
func (s *Set) Resize(shard int, enter, exit int) {
	if enter < 0 {
		enter = 0
	}
	if exit < 0 {
		exit = 0
	}
	s.mu.Lock()
	for _, r := range []struct {
		c *shardCounts
		n int
	}{{&s.enter, enter}, {&s.exit, exit}} {
		delta := int64(r.n) - r.c.size[shard]
		r.c.size[shard] = int64(r.n)
		r.c.avail[shard] += delta
		// Broadcast, not Signal: a grow of n frees up to n waiters at once, so waking one would leave the
		// rest asleep on permits that are already free. A shrink wakes them to no effect, so neither case
		// needs telling apart.
		r.c.condFor(&s.mu, shard).Broadcast()
	}
	s.mu.Unlock()
}

// Size reports a shard's two configured ceilings.
func (s *Set) Size(shard int) (enter, exit int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.enter.size[shard]), int(s.exit.size[shard])
}

// AcquireEnter takes one of the shard's ENTER permits, blocking until one is free or the Set closes. ok is
// false ONLY on close, so a caller distinguishes "drained" from "waited" without a second signal.
//
// It can never be blocked by exiting work: the two draw on separate reservations. That is the property
// that keeps dispatch alive under saturation, and it is not an optimisation - a shared pool with exits
// given precedence measured a 3x short-task collapse because entry never ran.
//
// The returned release is NOT idempotent: calling it twice hands out a permit that was never taken, and
// never calling it costs one permanently. Where the acquire and the release sit in different functions,
// wrap it in sync.OnceFunc at the acquiring site and call it unconditionally on the way out - that covers
// both. On !ok the release is a no-op, so an unconditional call is always safe.
func (s *Set) AcquireEnter(shard int) (release func(), ok bool) {
	s.mu.Lock()
	for s.enter.avail[shard] <= 0 && !s.closed {
		s.enter.condFor(&s.mu, shard).Wait()
	}
	if s.closed {
		s.mu.Unlock()
		return func() {}, false
	}
	s.enter.avail[shard]--
	s.mu.Unlock()
	return func() { s.release(&s.enter, shard) }, true
}

// AcquireExit takes one of the shard's EXIT permits for work that has ALREADY HAPPENED and only needs
// recording, blocking until one is free or the Set closes. ok is false ONLY on close.
//
// **A caller that gets !ok must still do its work.** Close means the owner is draining, not that the work
// may be dropped - the outcome exists nowhere but in that goroutine. The returned release is a no-op then,
// so calling it unconditionally is always safe.
//
// Blocking here is safe because the reservation is dedicated: an exit waits only behind other exits, never
// behind entering work, so it cannot be starved by a busy intake and cannot deadlock. What bounds the wait
// in the worst case - every in-flight caller finishing at once - is the owner's own concurrency ceiling,
// not this type.
//
// How long it blocked is not reported here - this package holds no metrics - but it is worth timing at the
// call site: the counts a Snapshot exposes are instantaneous, so a reservation saturated for a whole window
// without ever being sampled empty looks identical to an idle one. The wait is the only durable readout of
// the exit side becoming the binding constraint.
func (s *Set) AcquireExit(shard int) (release func(), ok bool) {
	s.mu.Lock()
	for s.exit.avail[shard] <= 0 && !s.closed {
		s.exit.condFor(&s.mu, shard).Wait()
	}
	if s.closed {
		s.mu.Unlock()
		return func() {}, false
	}
	s.exit.avail[shard]--
	s.mu.Unlock()
	return func() { s.release(&s.exit, shard) }, true
}

// release returns one permit to its own reservation and wakes a single waiter ON THAT SHARD. Signal rather
// than Broadcast is safe because the queues are per shard and per reservation: one permit satisfies at most
// one waiter, and every waiter in that queue is waiting on exactly this count, so the one woken can always
// proceed. Broadcasting would wake every waiter to re-sleep - a herd paid on every step.
func (s *Set) release(c *shardCounts, shard int) {
	s.mu.Lock()
	c.avail[shard]++
	c.condFor(&s.mu, shard).Signal()
	s.mu.Unlock()
}

// Snapshot is the available count per shard for each reservation, for metrics. Signed only because a live
// Resize can shrink a ceiling below what is currently held; neither acquire path can drive a count past
// zero.
func (s *Set) Snapshot() (enter, exit map[int]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	enter = make(map[int]int64, len(s.enter.avail))
	for shard, n := range s.enter.avail {
		enter[shard] = n
	}
	exit = make(map[int]int64, len(s.exit.avail))
	for shard, n := range s.exit.avail {
		exit[shard] = n
	}
	return enter, exit
}

// Close permanently unblocks every waiter on both reservations; both acquires report !ok forever after.
// Idempotent.
func (s *Set) Close() {
	s.mu.Lock()
	s.closed = true
	for _, c := range s.enter.conds {
		c.Broadcast()
	}
	for _, c := range s.exit.conds {
		c.Broadcast()
	}
	s.mu.Unlock()
}
