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

// Package turnstile bounds how many callers may hold a resource at once AND decides which waiter is
// admitted next: the lowest priority band, and within a band the job that started earliest.
//
// A job declares itself once, and every call it goes on to make takes its turns from the context:
//
//	ctx = set.ContextWithPriority(ctx, shard, priority) // once, when the JOB begins
//	...
//	pass := turnstile.WaitTurn(ctx)                     // at each call that holds the resource
//	defer pass.Return()
//
// Nothing is wrapped, so each call site chooses where its own pass begins and ends. That is deliberate:
// only the call site knows when the resource is really released. A query hands back a cursor that keeps
// holding a connection until it is closed, so a wrapper returning that cursor could not know when to hand
// the pass back, while the call site simply holds it until it has finished reading.
//
// A PASS MUST STRICTLY ENCLOSE THE RESOURCE IT ORDERS ACCESS TO - for the whole time it is held, which for
// a query means until its rows are closed and for a transaction means until it commits. Never take a pass
// while already holding the resource: with a pass for every unit of the resource, callers holding units
// without passes and callers holding passes without units deadlock each other, and neither side can break
// the cycle. Where a unit is held across several operations, one pass wraps them all rather than one per
// operation.
//
// THE CLAIM IS THE JOB'S, NOT THE CALL'S. Claim.Since is when the job began and Claim.Seq identifies it,
// so both stay fixed for every turn it takes - which is what makes the ordering first-in-first-out over
// JOBS rather than over calls: a caller coming back for its next turn keeps its original age and is served
// ahead of work that started later, so a job in progress finishes before a new one begins. The claim is
// trusted entirely, so it must not come from anywhere a caller could pick its own place in line - which is
// why a context's claim can only be set through this package.
//
// Ordering is by priority band first, so a band is served to exhaustion before the next one is looked at.
// A band therefore cannot be given to any caller that can arrive faster than it is served, or the bands
// below it never run at all.
//
// Close is the stop signal: it releases every waiter and the turnstile admits nobody forever after.
// Concurrency is live (Resize), because the resource it is sized against moves at runtime.
//
// One Turnstile governs one resource, and a Set holds one per shard. A single turnstile spanning several
// resources would let a caller queued for a busy one hold up a caller bound for an idle one.
package turnstile

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

// Claim is what a caller is asking with, and the whole of what orders it against the other waiters: the
// band it sits in, when its WORK began, and a tiebreak for the two of those being equal.
//
// Since is the age of the JOB, not of the call. Seq identifies the job when two of them share a Since,
// which a clock coarser than the arrival rate makes common; it must therefore be constant for every turn
// one job takes, or two tied jobs interleave instead of one finishing first.
type Claim struct {
	Priority int
	Since    time.Time
	Seq      uint32
}

// Turnstile admits up to a configured number of concurrent holders, choosing among waiters by priority and
// then by age. Safe for concurrent use by any number of goroutines. The zero value is not usable; call New.
type Turnstile struct {
	mu sync.Mutex
	// avail is how many passes are free right now. It goes NEGATIVE only when a live Resize shrinks the
	// ceiling below what is currently held; no acquire path can drive it past zero.
	avail int
	size  int
	// arrival counts callers in, and is the LAST tiebreak: two identical claims - same band, same job age,
	// same job - would otherwise be ordered arbitrarily by the heap, which makes the order untestable and
	// lets one of them be passed over repeatedly.
	arrival uint64
	q       waiters
	closed  bool
}

// token is what an issued pass names, and all a returned pass needs to be recognised as already returned.
// It is separate from entry so the path that never queues allocates these few bytes instead of a whole
// place in line, which is most acquisitions on an unsaturated turnstile.
type token struct {
	returned bool
}

// entry is one blocked caller's place in line. Its token outlives the queue, because Pass keeps a
// reference to it.
type entry struct {
	tok     token
	claim   Claim
	arrival uint64
	// ch is closed to hand the pass over, or to report a close. Nil on the fast path, where the caller
	// never queued and so was never woken.
	ch    chan struct{}
	index int
	// granted says whether the wakeup carried a pass. It is written under mu and read either under mu or
	// after receiving on ch, whose close supplies the ordering.
	granted bool
}

// waiters is the priority queue, ordered by band, then by job age, then by job, then by arrival.
type waiters []*entry

func (w waiters) Len() int { return len(w) }

func (w waiters) Less(i, j int) bool {
	a, b := w[i].claim, w[j].claim
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	if !a.Since.Equal(b.Since) {
		return a.Since.Before(b.Since)
	}
	if a.Seq != b.Seq {
		return a.Seq < b.Seq
	}
	return w[i].arrival < w[j].arrival
}

func (w waiters) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
	w[i].index = i
	w[j].index = j
}

func (w *waiters) Push(x any) {
	e := x.(*entry)
	e.index = len(*w)
	*w = append(*w, e)
}

func (w *waiters) Pop() any {
	old := *w
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	e.index = -1
	*w = old[:n-1]
	return e
}

// New returns a Turnstile issuing at most concurrency passes at a time. A concurrency of zero admits
// nobody until Resize gives it one.
func New(concurrency int) *Turnstile {
	if concurrency < 0 {
		concurrency = 0
	}
	return &Turnstile{avail: concurrency, size: concurrency}
}

// WaitTurn blocks until this claim is the best one waiting on a free pass, the ctx is done, or the
// turnstile closes.
//
// ok is false when no pass was issued, which is either the ctx expiring or a close; the caller's own
// ctx.Err() tells the two apart. The returned Pass is safe to Return either way.
//
// The returned Pass is not reusable and not safe to share: it names one issued pass, and returning it
// twice is a no-op rather than a second release.
func (t *Turnstile) WaitTurn(ctx context.Context, claim Claim) (Pass, bool) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return Pass{}, false
	}
	// A free pass with nobody ahead is taken without queueing. The queue is non-empty only while avail is
	// at or below zero - a grant always drains what it can - so this cannot barge past a waiter.
	if t.avail > 0 && t.q.Len() == 0 {
		t.avail--
		t.mu.Unlock()
		return Pass{t: t, tok: &token{}}, true
	}
	e := &entry{claim: claim, arrival: t.arrival, ch: make(chan struct{})}
	t.arrival++
	heap.Push(&t.q, e)
	t.mu.Unlock()

	// Timed from here rather than from entry, so the path that never queues pays no clock read at all. What
	// is left out is one uncontended lock and a heap push, which is noise against a wait worth reporting.
	start := time.Now()
	select {
	case <-e.ch:
		// granted is read without the mutex on purpose: it is written before the close, and receiving on a
		// closed channel is ordered after that close, so the value is visible. This is the hot path, and
		// the mutex it would otherwise take is the one every caller contends for.
		if !e.granted {
			return Pass{}, false
		}
		return Pass{t: t, tok: &e.tok, waited: time.Since(start)}, true
	case <-ctx.Done():
		t.mu.Lock()
		if e.granted {
			// The pass was handed over between the ctx firing and this lock. It is ours and nobody else
			// knows that, so it has to be passed on: an abandoned pass is one the turnstile never issues
			// again, shrinking the bound permanently and silently.
			e.tok.returned = true
			t.releaseLocked()
		} else if e.index >= 0 {
			heap.Remove(&t.q, e.index)
		}
		// The index test is the mirror of the grant test above, for the OTHER way a waiter can be taken off
		// the queue while it is on its way to the lock: Close pops every entry, which leaves the index at -1
		// with granted still false. Removing again then indexes the heap's slice at -1 and takes the process
		// down, on the caller's goroutine. Nothing else can produce that pair - a grant pops and sets
		// granted, so index<0 without a grant means exactly "Close got here first", and there is nothing
		// left to remove.
		t.mu.Unlock()
		return Pass{}, false
	}
}

// grantLocked hands free passes to the best waiters. A grant TRANSFERS the pass - the count falls and the
// waiter proceeds holding it - rather than waking a caller to go and look for one. That is what makes the
// ordering exact: the queue decides who proceeds, so a caller arriving at the same moment cannot take the
// pass a woken waiter was on its way to collect.
func (t *Turnstile) grantLocked() {
	for t.avail > 0 && t.q.Len() > 0 {
		e := heap.Pop(&t.q).(*entry)
		e.granted = true
		t.avail--
		close(e.ch)
	}
}

// releaseLocked puts one pass back and immediately hands it on if anyone is waiting.
func (t *Turnstile) releaseLocked() {
	t.avail++
	t.grantLocked()
}

// Resize changes the ceiling, live. The available count moves by its DELTA, never to the new value, so
// passes held right now stay held - assigning the ceiling outright would issue those a second time.
// Shrinking below what is held drives the count negative, which simply admits nobody until enough holders
// return, and needs no special case. A grow hands the new passes straight to the waiters at the head.
func (t *Turnstile) Resize(concurrency int) {
	if concurrency < 0 {
		concurrency = 0
	}
	t.mu.Lock()
	t.avail += concurrency - t.size
	t.size = concurrency
	t.grantLocked()
	t.mu.Unlock()
}

// Size reports the configured ceiling.
func (t *Turnstile) Size() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.size
}

// Snapshot reports the passes currently free and the callers currently queued, for metrics. available is
// signed only because a live Resize can shrink the ceiling below what is held.
//
// Both numbers are instantaneous, so a turnstile that was saturated for a whole window without ever being
// sampled empty reads the same as an idle one. The durable measure is how long callers actually waited,
// which Pass.Waited reports per acquisition.
func (t *Turnstile) Snapshot() (available, waiting int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.avail, t.q.Len()
}

// Close permanently releases every waiter; WaitTurn reports !ok forever after. Idempotent. It is the only
// stop signal, so a caller parked here is unreachable until it is called - closing whatever else the
// callers park on is not enough to drain them.
func (t *Turnstile) Close() {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		for t.q.Len() > 0 {
			// granted stays false, so each waiter learns it was released empty-handed rather than issued
			// a pass nobody will ever take back.
			close(heap.Pop(&t.q).(*entry).ch)
		}
	}
	t.mu.Unlock()
}

// Pass is one issued turn. Return it when the work that holds the resource is done.
type Pass struct {
	t      *Turnstile
	tok    *token
	waited time.Duration
}

// Return hands the pass back, admitting the best waiting caller. Calling it twice is a no-op: a second
// release would issue a pass that was never taken, inflating the ceiling past the resource it is sized
// against - which does not fail loudly, it quietly moves the waiting somewhere that has no ordering.
// Returning a pass that was never issued (the !ok case, or the zero value) is also a no-op.
//
// Not calling it at all is the failure this cannot absorb: admission decays with every leak until it
// stops. Where the acquire and the return sit in different functions, call it unconditionally on the way
// out - the no-op cases are what make that safe.
func (p Pass) Return() {
	if p.t == nil {
		return
	}
	p.t.mu.Lock()
	if !p.tok.returned {
		p.tok.returned = true
		p.t.releaseLocked()
	}
	p.t.mu.Unlock()
}

// Waited reports how long WaitTurn blocked before issuing this pass. It is zero for a pass taken without
// queueing, and it is the signal that says whether the turnstile is the binding constraint - the free-pass
// count cannot, being a single instant.
func (p Pass) Waited() time.Duration {
	return p.waited
}
