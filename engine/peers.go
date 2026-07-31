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
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/peers"
)

// --- Peer discovery (per-shard, DB-backed) ---
//
// Every fact the engine derives about the fleet comes from the shared dwarf_peers registry, NOT from the
// host transport, and it comes PER SHARD: one internal/peers.Sonar per open shard owns this replica's row
// in that shard's registry and publishes what reading it implies. The rationale for the mechanism lives in
// internal/peers; what matters here is the shape of what it publishes and what the engine does with it.
//
// Two numbers, from one reading, with opposite risk profiles:
//
//   - Replicas divides that shard's connection POOL, because the budget belongs to the shard's database and
//     N replicas each holding the whole budget would overshoot it N times over.
//   - Partition divides that shard's WORK - the residue class of step_id each replica selects - across the
//     replicas that demonstrably serve it.
//
// PER SHARD is the whole point, and it is what a fleet-global count cannot express: a peer whose piston
// wedges on shard 3 drops out of shard 3's work divisor and stays in every other shard's, and a peer whose
// beats to shard 3 fail mis-sizes only shard 3's pool. It also retires cross-shard comparison entirely -
// every timestamp in the registry is stamped by the shard holding it, so ages are comparable within a shard
// and timestamps are comparable nowhere.
//
// This is deliberately a LOOKUP, not a control loop: the counts are exact and discrete and independent of
// the actuation (a shrunk pool still heartbeats). They are TUNING numbers - a wrong count mis-sizes pools
// and degrades performance but corrupts nothing.

// peerCadence is the cadence this engine's Sonars read at - the one place that decides it, so the
// reconcile tick below can be derived from the same number rather than assuming the default.
func (e *Engine) peerCadence() time.Duration {
	if testing.Testing() {
		return testPeerCadence
	}
	return peers.Cadence
}

// testPeerCadence is the registry read cadence under test - see buildSonars.
//
// Short enough that Join's two cadences do not dominate a suite standing an engine up per test, and no
// shorter: the blindness grace is TWO cadences, and a Sonar that reads as blind declines to partition. At
// 10ms that grace is 20ms, which ordinary goroutine scheduling under a parallel suite misses often enough
// to disable partitioning at random - the same cliff a slow pass produces in production, reached through
// the test knob. 50ms leaves a 100ms grace, which jitter does not reach.
//
// A var so a test can restore the production cadence.
var testPeerCadence = 50 * time.Millisecond

// reconcileTicksPerCadence is how many times runReconcileLoop ticks per Sonar read cadence - see the
// margin argument there. Eight rather than two, and rather than something larger still: the apply is
// already negligible against the wait at eight, while every further tick is pure loss on a loop that
// early-returns.
const reconcileTicksPerCadence = 8

// sonarFor returns the Sonar watching one shard, or nil before Startup has built them.
func (e *Engine) sonarFor(shard int) *peers.Sonar {
	if e.sonars == nil {
		return nil
	}
	return e.sonars[shard]
}

// replicasOn is how many replicas hold connections to one shard - the divisor for that shard's pool. One
// before the Sonars exist, which is the solo sizing every derivation falls back to.
func (e *Engine) replicasOn(shard int) int {
	s := e.sonarFor(shard)
	if s == nil {
		return 1
	}
	return s.Replicas()
}

// partitionOn is the (dispatchers, ordinal) pair that splits candidate selection on one shard, or ok=false
// when selection there must not be partitioned. Handed to that shard's piston as its PartitionFunc.
//
// Everything about it fails OPEN - a shard with no Sonar, a solo dispatcher, an unknown ordinal, a Sonar
// that has gone blind - because partitioning EXCLUDES rows: a wrong pair strands a residue class, while
// declining to partition only restores overlapping selection, which the claim CAS arbitrates at the cost of
// a lost round trip.
func (e *Engine) partitionOn(shard int) (replicas, ordinal int, ok bool) {
	s := e.sonarFor(shard)
	if s == nil {
		return 0, 0, false
	}
	return s.Partition()
}

// buildSonars creates one Sonar per open shard, before anything is sized from a Sonar. Errors are logged
// rather than returned: a shard without one falls back to solo sizing on that shard (pools sized for one
// replica) and to unpartitioned selection, which is the safe direction on both axes.
func (e *Engine) buildSonars() {
	e.sonars = make(map[int]*peers.Sonar, e.db.NumShards())
	for _, idx := range e.db.Indices() {
		db, err := e.db.Shard(idx)
		if err != nil {
			e.logger.Error("Resolving shard for its peer sonar", "shard", idx, "error", err)
			continue
		}
		s, err := peers.New(e.engineID, idx, db)
		if err != nil {
			e.logger.Error("Building peer sonar", "shard", idx, "error", err)
			continue
		}
		s.SetLogger(e.logger)
		s.SetSeams(e.seams)
		if testing.Testing() {
			// A test fleet is ephemeral: every engine pays Join's two cadences on the way up, and a suite that
			// stands one up per test would spend most of its time there. Shortening the cadence shortens Join
			// and the blindness grace with it, and nothing else - the windows are unchanged, so what the tests
			// exercise is the same policy against the same rows.
			s.SetCadence(e.peerCadence())
		}
		e.sonars[idx] = s
	}
}

// joinFleet announces this replica on every shard and returns once each Sonar has read the fleet back, so
// every getter is seeded before anything is sized from it.
//
// Each Join blocks for two read cadences - that wait is what keeps a join from exceeding a shard's
// connection budget even momentarily, since peers go on holding pools sized for the fleet WITHOUT this
// replica until they read the registry again. Announce first, let them shrink, then grow. Run in parallel
// across shards, so the whole fleet costs one wait rather than one per shard.
func (e *Engine) joinFleet(ctx context.Context) {
	var wg sync.WaitGroup
	for idx, s := range e.sonars {
		wg.Go(func() {
			if err := s.Join(ctx); err != nil {
				// Not fatal: the pools simply size for whatever this shard's reading found, and the Sonar's own
				// loop repairs a missing row and re-reads within a cadence.
				e.logger.ErrorContext(ctx, "Joining the fleet", "shard", idx, "error", err)
			}
		})
	}
	wg.Wait()
	// AND THEN WAIT FOR PEERS TO HAVE APPLIED, not merely to have read. Join's own wait covers DETECTION -
	// two read cadences, so the reading after this replica's row landed must have seen it - but a peer
	// applies what it read on its reconcile tick, which can be a whole tick later. Sizing this replica's
	// pools in that gap is the one thing the announce-before-consume ordering exists to prevent, just one
	// step further along: every peer knows the new count and none has shrunk to it yet, so the shard's
	// server briefly sees the joiner's full share on top of shares nobody has given back.
	//
	// IT SHRINKS THE WINDOW; IT CANNOT CLOSE IT, and the distinction belongs here rather than in a comment
	// that claims more. A peer's apply is local to its process, so no wait can prove it happened - which
	// leaves one reachable worst case whatever the grace: every survivor still on the pre-join split (which
	// sums to the whole budget) while this replica has grown into its post-join share. Measured over 25
	// rollouts by TestPeerRollingRestart_FleetNeverExceedsTheShardBudget: peaks of 52 (joiner still on its
	// bootstrap pool) and 60 = 48 + 12 (joiner grown, nobody shrunk), against a 48 budget. That is the
	// engine's guarantee - budget plus one post-join share - and it is what that test asserts.
	//
	// Without the grace the same rollout failed a budget+bootstrap bound 10 times out of 10 under -race.
	//
	// Two ticks rather than one, because the apply is not instantaneous: it pushes a new ceiling to every
	// open shard, and under a loaded host (or -race) that runs long. A peer cannot be OBSERVED to have
	// applied - the pool size is local to its process - so a wait is the only instrument available.
	e.sleepPeerGrace(ctx, 2*e.peerCadence()/reconcileTicksPerCadence)
}

// sleepPeerGrace waits d, or until ctx ends. Startup's own context governs it, so a cancelled startup does
// not sit out the grace.
func (e *Engine) sleepPeerGrace(ctx context.Context, d time.Duration) {
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-ctx.Done():
	}
}

// leaveFleet deletes this replica's row from every shard's registry, so peers recount without waiting out
// the freshness window.
//
// Called at shutdown with a LIVE context, after the Sonars' own loops have stopped: a Sonar's beat only ever
// UPDATEs, so once its loop has returned nothing can resurrect the row this deletes. Best effort - a missed
// delete just ages out of peers' counts on its own.
func (e *Engine) leaveFleet(ctx context.Context) {
	for idx, s := range e.sonars {
		if err := s.Leave(ctx); err != nil {
			e.logger.ErrorContext(ctx, "Leaving the fleet", "shard", idx, "error", err)
		}
	}
}

// runReconcileLoop keeps what the engine DERIVES from the fleet in step with what the Sonars observe.
//
// A pull rather than a notification, and the reason is not taste. Its invariant is "the applied pool sizes
// match the currently observed fleet", which is a job that exists whether or not anything announces a
// change: it also absorbs drift no notification could express - a SetMaxOpenConns override landing, a push
// that errored, a count moving while a previous push was in flight. An edge would additionally have to be
// fired from every path that can move a published value (a confirmed fall, a recovery from blindness, a
// registration repair, a prune), and missing one would leave the derived sizes stale forever with no
// backstop; it would also run the pool policy on a Sonar's goroutine, coupling one shard's beat to another
// shard's slow push.
//
// IT MUST TICK A SMALL FRACTION OF THE CADENCE THE SONARS READ AT, because the apply spends part of a
// budget the join is already spending. A joining replica waits two cadences before opening a connection,
// which is what keeps a join from exceeding a shard's budget - but that wait is sized against the READ, and
// a peer has not shrunk anything until it has also APPLIED. Detection already costs up to one cadence plus
// a pass (a peer's read may have begun just before the joiner's row landed), so everything the apply spends
// comes out of the one remaining cadence.
//
// Half the cadence is NOT enough of a fraction, which was measured rather than reasoned:
// TestPeerRollingRestart_FleetNeverExceedsTheShardBudget priced a four-replica rollout at 56 connections
// against a 52 bound (two survivors still holding the pre-join 16 while the joiner had grown to 12) on a
// loaded -race run. At an eighth the apply is a rounding error against the wait, so what remains is jitter
// in the peer's own read, which nothing on this side can shorten.
//
// The divisor is derived from the cadence the Sonars actually run at, not from the default, so the
// relationship holds under test too. Each tick is atomic loads plus a recompute that early-returns when
// nothing moved - no I/O at all, so a faster tick costs nothing measurable.
func (e *Engine) runReconcileLoop() {
	ticker := time.NewTicker(max(time.Millisecond, e.peerCadence()/reconcileTicksPerCadence))
	defer ticker.Stop()
	// The fleet size this loop last SAW, per shard. Deliberately not recomputePools' lastAppliedR: that one
	// records what the pools were last DERIVED with and is never updated under a SetMaxOpenConns override
	// (pinned pools derive nothing), so churn counted from it would report a settled fleet whenever an
	// operator pinned the pools - which is exactly what a benchmark does. Owned by this goroutine alone, so
	// it needs no lock.
	seen := map[int]int{}
	for _, idx := range e.db.Indices() {
		seen[idx] = e.replicasOn(idx)
	}
	for {
		select {
		case <-e.reconcileStop:
			return
		case <-ticker.C:
		}
		// Report churn before applying it: a fleet that is settled must count ZERO here for a whole run, so
		// every move is worth a line and a tick of the counter whether or not any pool size follows from it.
		for _, idx := range e.db.Indices() {
			if now := e.replicasOn(idx); now != seen[idx] {
				e.metricPeerCountChanged(e.lifetimeCtx, idx, seen[idx], now)
				seen[idx] = now
			}
		}
		e.recomputePools()
	}
}
