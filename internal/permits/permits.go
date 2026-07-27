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
// It is a per-shard counting semaphore with one unusual property: the count may go NEGATIVE, because it
// is taken two different ways by two callers with opposite priorities.
//
//   - ADMISSION waits. A goroutine about to start new work calls Acquire, which blocks until a permit is
//     free. New work can and should queue.
//   - COMPLETION never waits. A goroutine that has already run its side effects and only needs to RECORD
//     them calls Debit, which takes a permit without waiting and drives the count negative when none is
//     free, then Restore when it is done.
//
// So a burst of completions suppresses new admission automatically - while the count is negative nothing
// new is admitted at all - without ever holding a finished job hostage to a throughput optimisation.
//
//	rel, ok := set.Acquire(shard)   // blocks; ok=false only when CLOSED
//	if !ok { return }
//	... work that holds a connection ...
//	rel()
//
// Close is the stop signal, mirroring the candidate cache: it permanently unblocks every waiter, and
// Acquire reports !ok forever after. Sizes are live (Resize), because the pools they are derived from are.
package permits

import "sync"

// Set is a per-shard signed permit count, safe for concurrent use by any number of goroutines. The zero
// value is not usable; call New.
type Set struct {
	mu sync.Mutex
	// conds is one waiting queue per shard - shared conds lose wakeups across shards, and a Broadcast on
	// every release is a herd on the hottest path. Created on demand and never removed: the shard set is
	// fixed for a run, so there is nothing to garbage-collect.
	conds map[int]*sync.Cond
	// avail is what is left to hand out, per shard. SIGNED: Debit may take it below zero, and a negative
	// value is the storm signature - the persist path has fully suppressed new admission.
	avail map[int]int64
	// size is the configured ceiling per shard, held so Resize can move avail by the DELTA rather than
	// resetting it, which would silently hand out permits that in-flight workers are still holding.
	size   map[int]int64
	closed bool
}

// New returns an empty Set. A shard with no configured size admits nothing until Resize gives it one, so
// every shard the engine intends to dispatch on must be sized before its workers start.
func New() *Set {
	return &Set{conds: map[int]*sync.Cond{}, avail: map[int]int64{}, size: map[int]int64{}}
}

// condFor returns the shard's waiting queue, creating it on first use. Callers hold s.mu.
func (s *Set) condFor(shard int) *sync.Cond {
	c := s.conds[shard]
	if c == nil {
		c = sync.NewCond(&s.mu)
		s.conds[shard] = c
	}
	return c
}

// Resize sets a shard's permit ceiling, live. The available count moves by the DELTA, never to n, so
// permits held right now stay held. Shrinking below what is held drives the count negative, which blocks
// admission until enough holders release.
func (s *Set) Resize(shard int, n int) {
	if n < 0 {
		n = 0
	}
	s.mu.Lock()
	delta := int64(n) - s.size[shard]
	s.size[shard] = int64(n)
	s.avail[shard] += delta
	// Broadcast, not Signal: a grow of n frees up to n waiters at once, so waking one would leave the rest
	// asleep on permits that are already free. A shrink wakes them to no effect, so neither case needs
	// telling apart.
	s.condFor(shard).Broadcast()
	s.mu.Unlock()
}

// Size is a shard's configured ceiling.
func (s *Set) Size(shard int) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.size[shard])
}

// Acquire takes one of the shard's permits, blocking until one is free or the Set closes. ok is false ONLY
// on close, so a caller distinguishes "drained" from "waited" without a second signal.
//
// The returned release is NOT idempotent: calling it twice hands out a permit that was never taken, and
// never calling it costs one permanently. Where the acquire and the release sit in different functions,
// wrap it in sync.OnceFunc at the acquiring site and call it unconditionally on the way out - that covers
// both. On !ok the release is a no-op, so an unconditional call is always safe.
func (s *Set) Acquire(shard int) (release func(), ok bool) {
	s.mu.Lock()
	for s.avail[shard] <= 0 && !s.closed {
		s.condFor(shard).Wait()
	}
	if s.closed {
		s.mu.Unlock()
		return func() {}, false
	}
	s.avail[shard]--
	s.mu.Unlock()
	return func() { s.release(shard) }, true
}

// Available reports whether the shard has a permit free right now. It is a HINT - the answer can be stale
// before the caller acts on it - and is meant for the growth decision, where being wrong costs one
// goroutine that parks harmlessly, never correctness.
func (s *Set) Available(shard int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed && s.avail[shard] > 0
}

// Debit takes a permit WITHOUT waiting, driving the count negative when none is free. It is the completion
// path's half of the contract: work whose side effects have already fired is never made to queue, but it
// still shows up in the count, so a storm of completions suppresses new admission.
//
// Every Debit MUST be paired with a Restore, and unlike a leaked Acquire release there is no backstop for
// it: pair them with a defer on the next line.
func (s *Set) Debit(shard int) {
	s.mu.Lock()
	s.avail[shard]--
	s.mu.Unlock()
}

// Restore returns a debited permit and wakes whoever is waiting on it.
func (s *Set) Restore(shard int) {
	s.release(shard)
}

// release returns one permit and wakes a single waiter ON THAT SHARD. Signal is safe only because the queue
// is per shard: one permit satisfies at most one Acquire, and every waiter here is waiting on exactly this
// count, so the one woken can always proceed.
func (s *Set) release(shard int) {
	s.mu.Lock()
	s.avail[shard]++
	s.condFor(shard).Signal()
	s.mu.Unlock()
}

// Snapshot is the available count per shard, for metrics. Signed: a negative value means the persist path
// has fully suppressed admission on that shard, which has no other readout.
func (s *Set) Snapshot() map[int]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int]int64, len(s.avail))
	for shard, n := range s.avail {
		out[shard] = n
	}
	return out
}

// Close permanently unblocks every waiter; Acquire reports !ok forever after. Idempotent.
func (s *Set) Close() {
	s.mu.Lock()
	s.closed = true
	for _, c := range s.conds {
		c.Broadcast()
	}
	s.mu.Unlock()
}
