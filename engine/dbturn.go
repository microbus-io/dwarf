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
	"time"

	"github.com/microbus-io/dwarf/internal/turnstile"
)

// dbTurn takes this caller's turn at a shard's turnstile and returns the context every database call it
// then makes must run on, plus the release.
//
//	ctx, done := e.dbTurn(ctx, shardNum)
//	defer done()
//
// Take it at the OUTERMOST database operation of a call path, never inside a shared helper - except one that has
// first handed back the turn its caller held, as lockCohortStripe's lease tick does: a turn taken while one is held
// deadlocks against the other holders. Do not hold it across a wait that is not the database (a host call, the latch
// board, a sleep); hand it back first. The claim's age is the context's first stamp and is never re-stamped, so a
// cross-shard caller presents one age at every shard. Nothing that takes a connection is exempt: the peer Sonars
// take the refiller's band (dbTurnAt with priorityRefill), since an exemption ranks a caller above every band and
// drops it from the population the turns bound. Startup's RTT probe is the one exemption, as no worker exists yet.
func (e *Engine) dbTurn(ctx context.Context, shardNum int) (context.Context, func()) {
	return e.dbTurnAt(ctx, shardNum, priorityCommon)
}

// opTurn is dbTurn for an API operation's entry point. Its release also records how long the operation held
// the shard - the wait for the turn plus the database work under it - as dwarf_operation_seconds. An operation
// takes one per shard it works on, so it records one sample per shard; any further turn it needs is a plain
// dbTurn.
func (e *Engine) opTurn(ctx context.Context, shardNum int, op string) (context.Context, func()) {
	start := time.Now()
	turnCtx, doneTurn := e.dbTurn(ctx, shardNum)
	return turnCtx, func() {
		doneTurn()
		e.metricOperation(ctx, op, shardNum, time.Since(start))
	}
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
// is NOT the database - a lock, a sleep - and holding a turn across one bounds every other caller on
// something that is not the resource being metered. The claim is re-taken unchanged; a host call takes
// yieldTurnAway instead.
//
// The pass is passed by POINTER and replaced in place, so a caller's deferred return must be written as a
// closure (`defer func() { pass.Return() }()`); a plain `defer pass.Return()` binds the receiver at defer
// time and would hand back a pass that is no longer the one being held.
//
// It re-takes unconditionally, including when the turnstile has closed - a closed one yields a no-op pass,
// so the work still finishes, which is the same drain contract every other site has.
//
// The re-take is DEFERRED so it also runs when fn panics. A panic caught further up would otherwise unwind
// past the re-take and leave the caller holding a spent pass: no leak and no double release (both are
// no-ops), but every database call on the failure disposition would then run unordered, which is the one
// thing this whole path is for.
func yieldTurn(ctx context.Context, pass *turnstile.Pass, fn func()) {
	pass.Return()
	defer func() { *pass = turnstile.WaitTurn(ctx) }()
	fn()
}

// yieldTurnAway is yieldTurn for a host call: the claim on *ctx is moved forward by the call's duration
// (turnstile.Away) before the turn is re-taken, so the job ranks by its time at the database, exactly as it
// does after ExecuteTask. *ctx is replaced in place so the caller's later turns take the moved claim too, and
// the move is deferred with the re-take so a panicking fn gets both.
func yieldTurnAway(ctx *context.Context, pass *turnstile.Pass, fn func()) {
	pass.Return()
	start := time.Now()
	defer func() {
		*ctx = turnstile.Away(*ctx, time.Since(start))
		*pass = turnstile.WaitTurn(*ctx)
	}()
	fn()
}
