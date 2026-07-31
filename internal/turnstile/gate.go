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

import "context"

// Gate takes a job's FIRST turn, at a fixed band, before the job has been picked up.
//
// It exists for a caller that hands work to a pool of goroutines and decides how big that pool should be by
// watching whether they are blocked - the pattern internal/workers implements. Such a caller must take its
// turn BEFORE it takes the work, for two reasons that have nothing to do with bounding the database:
//
//   - A goroutine blocked here is holding no work, so it reads as available, so the pool does not grow to
//     add capacity that could only queue behind the same turnstile. Blocking IS the signal; nothing has to
//     ask this package how full it is.
//   - Work not yet taken is still visible to every other goroutine. Taking it first and blocking afterwards
//     strands it inside one that cannot proceed with it.
//
// The turn it takes is the job's first, not a separate reservation: the context it returns carries the
// claim, stamped at the moment the job was picked up, so every later turn the job takes is ordered by that
// same moment. The caller releases it when the work stops holding the resource - typically right after the
// first database call - and takes further turns on the returned context from there on.
type Gate struct {
	set      *Set
	priority int
}

// Gate returns a gate over this set at the given band. The band is the caller's policy, which is why it is
// bound here rather than assumed.
func (s *Set) Gate(priority int) *Gate {
	return &Gate{set: s, priority: priority}
}

// Acquire stamps a claim for a job about to be picked up on a shard and blocks until its turn comes,
// returning the context every later turn of that job must be taken on.
//
// ok is false only when the set has CLOSED, which is the caller's stop signal. It is checked on both sides
// of the wait: before, so a drained set admits nobody, and after, because a close is what releases a
// blocked caller and it must be told to stop rather than left to carry on into a shutting-down system.
// This is the one place a closed turnstile means "stop" instead of "proceed unordered" - which is why it
// asks the set rather than reading the pass.
func (g *Gate) Acquire(ctx context.Context, shard int) (context.Context, func(), bool) {
	if g.set.Closed() {
		return ctx, func() {}, false
	}
	gated := g.set.ContextWithPriority(ctx, shard, g.priority)
	pass := WaitTurn(gated)
	if g.set.Closed() {
		pass.Return()
		return ctx, func() {}, false
	}
	return gated, pass.Return, true
}
