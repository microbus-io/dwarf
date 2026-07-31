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

	"github.com/microbus-io/dwarf/internal/turnstile"
)

// dbTurn takes this caller's turn at a shard's turnstile and returns the context every database call it
// then makes must run on, plus the release.
//
//	ctx, done := e.dbTurn(ctx, shardNum)
//	defer done()
//
// TAKE IT AT THE OUTERMOST DATABASE OPERATION OF A CALL PATH, NEVER INSIDE A SHARED HELPER. A turn taken
// while one is already held is the nesting the enclosure rule forbids: holders waiting on a second turn
// deadlock against each other, and the helpers here are shared between paths that hold a turn (a worker
// mid-step) and paths that do not (a sweep, an operation). The engine's rule is therefore positional -
// operations take a turn at their entry point, background loops take one per shard per pass, and the
// worker path takes one per database call between the phases it has already released.
//
// The claim's age comes from the first stamp on the context and is never re-stamped, so a caller that
// visits several shards presents the same age at each - which is what keeps a long cross-shard operation
// from looking newly arrived at every hop it makes.
//
// DO NOT HOLD ONE ACROSS A WAIT FOR SOMETHING THAT IS NOT THE DATABASE - a host call, a park on the latch
// board, a sleep. The turn is only for the stretch that holds or awaits a connection; everything else must
// give it back first, or a caller parked on something unbounded bounds every other caller with it.
//
// NOTHING THAT TAKES A CONNECTION IS EXEMPT, and the two callers that look like they should be are the
// reason to say so out loud. A mechanism that reports on saturation - the observable-gauge callback, the
// peer Sonars - must not be throttled by it: a Sonar that cannot beat reads as BLIND, which makes its
// replica stop partitioning, so throttling the registry would let load reshape the fleet.
//
// The answer is the REFILLER'S BAND (dbTurnAt with priorityRefill), not an exemption. Exempting a caller
// does not put it outside the contest, it ranks it ABOVE every band - a bypasser takes a connection
// without queueing for one - and drops it from the population the turn count is there to bound. Band 0
// gives the same precedence back while keeping both properties, and costs those callers nothing: strict
// priority means a band-0 caller waits only for the NEXT turn to free, not for the queue to drain, which
// at hundreds of turns each held for one round trip is no wait at all.
//
// Startup's RTT probe is the one genuine exemption, trivially: it runs before any worker exists, so there
// is nothing for it to be ordered against.
func (e *Engine) dbTurn(ctx context.Context, shardNum int) (context.Context, func()) {
	return e.dbTurnAt(ctx, shardNum, priorityCommon)
}

// heldTurnKey carries the turn its holder is currently on, so a CALLEE that has to park on something other
// than the database can hand it back without the pass being threaded through every signature between them.
// The claim already travels this way; this is the same trick for the pass itself.
type heldTurnKey struct{}

// withHeldTurn publishes the pass this caller holds to everything it calls.
func withHeldTurn(ctx context.Context, pass *turnstile.Pass) context.Context {
	return context.WithValue(ctx, heldTurnKey{}, pass)
}

// yieldHeldTurn hands back whatever turn the context says its caller is holding for the duration of fn, and
// takes a fresh one after - yieldTurn for a callee that cannot see the pass. A context carrying no turn
// simply runs fn, which is what makes it safe to use on a path reached both with and without one.
//
// It exists because the same lock is taken from both: the success path parks on the cohort stripe holding
// a turn it can yield directly, while failStep parks on the SAME stripe from callers that hold a turn it
// has no reference to. Left unyielded there, a cohort whose branches fail together - the exact shape that
// convoys on a stripe - parks its losers inside the turnstile, which is the occupancy the stripe exists to
// keep out of the database's queue in the first place.
func yieldHeldTurn(ctx context.Context, fn func()) {
	if pass, ok := ctx.Value(heldTurnKey{}).(*turnstile.Pass); ok && pass != nil {
		yieldTurn(ctx, pass, fn)
		return
	}
	fn()
}

// dbTurnAt is dbTurn in a caller-chosen band. Reserve it for the cadenced, bounded-by-construction callers
// that belong above the common band - see the band definitions in poolsize.go for what qualifies.
func (e *Engine) dbTurnAt(ctx context.Context, shardNum, priority int) (context.Context, func()) {
	if e.turnstiles == nil { // before Startup, and in the operations' own not-started guard paths
		return ctx, func() {}
	}
	ctx = e.turnstiles.ContextWithPriority(ctx, shardNum, priority)
	pass := turnstile.WaitTurn(ctx)
	// The release is a CLOSURE over the variable, not a bound method value: yieldHeldTurn replaces the pass
	// in place, and a bound pass.Return would hand back the one that existed when this returned.
	return withHeldTurn(ctx, &pass), func() { pass.Return() }
}

// yieldTurn hands the turn back for the duration of fn and takes a fresh one afterwards. fn is a wait that
// is NOT the database - a host call, a lock, a sleep - and holding a turn across one bounds every other
// caller on something that is not the resource being metered.
//
// The pass is passed by POINTER and replaced in place, so a caller's deferred return must be written as a
// closure (`defer func() { pass.Return() }()`); a plain `defer pass.Return()` binds the receiver at defer
// time and would hand back a pass that is no longer the one being held.
//
// It re-takes unconditionally, including when the turnstile has closed - a closed one yields a no-op pass,
// so the work still finishes, which is the same drain contract every other site has.
//
// The re-take is DEFERRED so it also runs when fn panics. One of these wraps a host call inside
// errors.CatchPanic, so a panicking host would otherwise unwind past the re-take and leave the caller
// holding a spent pass: no leak and no double release (both are no-ops), but every database call on the
// failure disposition would then run unordered, which is the one thing this whole path is for.
func yieldTurn(ctx context.Context, pass *turnstile.Pass, fn func()) {
	pass.Return()
	defer func() { *pass = turnstile.WaitTurn(ctx) }()
	fn()
}
