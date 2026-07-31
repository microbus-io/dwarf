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

package turnstile

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// seqCeiling bounds the job counter. It only ever tells apart two jobs that share a timestamp, so it has
// to outlast one clock tick's worth of arrivals rather than a process - a million is many orders beyond
// what any clock coarse enough to collide can accumulate in a tick. It wraps rather than saturating,
// because a saturated counter would make every job past the ceiling compare equal.
const seqCeiling = 1_000_000

// jobSeq numbers jobs, and is deliberately process-wide rather than per Set: it identifies a job, and a
// job is not a property of the shard it happens to be talking to.
var jobSeq atomic.Uint32

// stamp is what a prioritized context carries: which turnstile governs the resource this context is bound
// for, and the claim to present to it.
type stamp struct {
	ts    *Turnstile
	claim Claim
}

// ctxKey is the private context key. Private so nothing outside this package can forge a claim - the
// ordering trusts Since completely, so a caller able to write its own would be able to choose its place
// in line.
type ctxKey struct{}

// Set holds one Turnstile per shard. It is a lookup table and nothing more: the turnstiles are wholly
// independent, each with its own lock and queue, so one shard's contention is invisible to another and a
// release on one can never wake a waiter on another.
type Set struct {
	mu     sync.RWMutex
	ts     map[int]*Turnstile
	closed bool
}

// NewSet returns an empty Set. A shard admits nothing through this package until Resize gives it a
// turnstile; see ContextWithPriority for what happens to a context bound for a shard that has none.
func NewSet() *Set {
	return &Set{ts: map[int]*Turnstile{}}
}

// Resize sets a shard's ceiling, creating its turnstile on first use. Size it against the resource being
// ordered - AT MOST as many passes as there are units of it, or the surplus callers simply queue inside
// the resource again, where there is no ordering, while this reports itself healthy.
//
// Every path that changes that resource's size must call this, for the same reason the ceiling exists.
func (s *Set) Resize(shard int, concurrency int) {
	s.mu.Lock()
	// A closed set stays closed. Without this a resize racing a shutdown - the owner's pool recompute
	// against its own drain - would mint a fresh, OPEN turnstile inside it, and callers would park in
	// something nothing will ever close.
	if s.closed {
		s.mu.Unlock()
		return
	}
	t := s.ts[shard]
	if t == nil {
		t = New(concurrency)
		s.ts[shard] = t
	} else {
		t.Resize(concurrency)
	}
	s.mu.Unlock()
}

// Turnstile returns a shard's turnstile, or nil if it has none.
func (s *Set) Turnstile(shard int) *Turnstile {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ts[shard]
}

// ContextWithPriority binds a context to one shard's turnstile at the given band, so that WaitTurn can be
// called anywhere downstream without the turnstile being threaded through every signature.
//
// It stamps the job's age and identity ONCE. Calling it again on an already-stamped context - which is
// what a cross-shard operation does, re-pointing the same job at each shard in turn - keeps the original
// age and job number and changes only the shard and the band. Re-stamping instead would make a job look
// newly arrived at every hop, which is precisely the ordering this exists to provide: a job that has been
// running for a while would keep losing to work that just started.
//
// A shard with no turnstile yields a context that WaitTurn passes straight through. That is the fail-open
// direction and it is the right one here, because this orders access to a resource that already bounds
// itself - so the cost of an unsized shard is that its callers go unordered, not that they are blocked. A
// gate that IS the bound would have to fail the other way.
func (s *Set) ContextWithPriority(ctx context.Context, shard int, priority int) context.Context {
	next := &stamp{
		ts:    s.Turnstile(shard),
		claim: Claim{Priority: priority, Since: time.Now(), Seq: jobSeq.Add(1) % seqCeiling},
	}
	if prev, ok := ctx.Value(ctxKey{}).(*stamp); ok {
		next.claim.Since = prev.claim.Since
		next.claim.Seq = prev.claim.Seq
	}
	return context.WithValue(ctx, ctxKey{}, next)
}

// Close closes every turnstile in the set, releasing all their waiters. Idempotent.
func (s *Set) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, t := range s.ts {
		t.Close()
	}
}

// Closed reports whether the set has been closed. WaitTurn deliberately cannot answer this - it lets every
// caller through so that work already under way still finishes - so a caller whose job is to STOP on a
// drain, rather than to proceed unordered, asks here.
func (s *Set) Closed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.closed
}

// Snapshot reports each shard's free passes and queued callers, for metrics.
func (s *Set) Snapshot() (available, waiting map[int]int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	available = make(map[int]int, len(s.ts))
	waiting = make(map[int]int, len(s.ts))
	for shard, t := range s.ts {
		available[shard], waiting[shard] = t.Snapshot()
	}
	return available, waiting
}

// ClaimFrom reports the claim a context carries, if it was prioritized.
func ClaimFrom(ctx context.Context) (Claim, bool) {
	st, ok := ctx.Value(ctxKey{}).(*stamp)
	if !ok {
		return Claim{}, false
	}
	return st.claim, true
}

// WaitTurn takes a turn for the claim on ctx, blocking until it is the best one waiting. Wrap it around
// the work that holds the resource:
//
//	pass := turnstile.WaitTurn(ctx)
//	defer pass.Return()
//	... the call that holds a connection, INCLUDING reading the rows it returns ...
//
// The pass must enclose the resource for the whole time it is held, which for a query means until its rows
// are closed and for a transaction means until it commits - a query hands back a connection-holding cursor,
// so returning the pass when the call returns leaves the connection held by a caller that no longer has a
// turn. That is the one arrangement this cannot survive: with a pass for every unit of the resource,
// callers holding units without passes and callers holding passes without units deadlock each other.
//
// It never refuses to let a caller proceed. A context that was never prioritized, a shard with no
// turnstile, an expired ctx and a closed turnstile all yield a zero Pass, which is safe to Return and
// simply means this caller went unordered - so a call site can be converted before or after the paths that
// stamp its context, and a drain never strands work whose outcome exists nowhere else.
func WaitTurn(ctx context.Context) Pass {
	st, ok := ctx.Value(ctxKey{}).(*stamp)
	if !ok || st.ts == nil {
		return Pass{}
	}
	pass, _ := st.ts.WaitTurn(ctx, st.claim)
	return pass
}
