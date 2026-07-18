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
	"encoding/json"
	"testing"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// signalOp identifies which internal handler an inbound peer signal targets. It is the op routing key
// passed across the host boundary as a plain string.
type signalOp string

const (
	signalOpEnqueue      signalOp = "enqueue"
	signalOpStatusChange signalOp = "statusChange"
	// signalOpPeersChanged is a best-effort nudge that the fleet changed (a replica joined or left):
	// receivers re-read the shared dwarf_peers registry and resize their pools. The registry is the
	// source of truth for the replica count R; this signal only accelerates convergence from the
	// heartbeat cadence (pingInterval) to sub-second. A host that no-ops SignalPeers therefore degrades
	// convergence SPEED, never correctness - every replica still re-reads R on its own heartbeat.
	signalOpPeersChanged signalOp = "peersChanged"
)

// Per-op payload bodies. The engine marshals these in emitSignal and unmarshals the received bytes in
// DeliverSignal. Origin carries the sending engine's random instanceID: SignalPeers' contract asks the
// host to deliver only to OTHER replicas, but a broadcast transport may echo the signal back to the
// sender - DeliverSignal discards a payload whose Origin matches its own instanceID rather than rely
// on the host. (An empty Origin - e.g. a signal from an older build - is never discarded.)
type (
	enqueuePayload struct {
		Origin        string
		Shard, StepID int
	}
	statusChangePayload struct {
		Origin          string
		FlowKey, Status string
	}
	peerPayload struct {
		Origin string
	}
)

// emitSignal serializes a signal body and hands (op, bytes) to the host for delivery to OTHER replicas.
func (e *Engine) emitSignal(ctx context.Context, op signalOp, payload any) {
	data, err := json.Marshal(payload)
	if err != nil {
		e.logger.ErrorContext(ctx, "Marshaling peer signal", "op", string(op), "error", err)
		return
	}
	err = errors.CatchPanic(func() error {
		// faultSignalPeersPanic simulates the host's SignalPeers panicking, so the test can prove the
		// boundary CatchPanic swallows it (logged below) and flow completion / Await are unaffected. Scoped
		// by op so a test targets the terminal statusChange wake without also tripping enqueue doorbells.
		if e.seams.IsFault(faultSignalPeersPanic, string(op)) {
			panic("injected fault: " + faultSignalPeersPanic + " " + string(op))
		}
		e.host.SignalPeers(ctx, string(op), data)
		return nil
	})
	if err != nil {
		e.logger.ErrorContext(ctx, "SignalPeers callback panicked", "op", string(op), "error", err)
	}
}

func (e *Engine) signalEnqueue(ctx context.Context, shard, stepID int) {
	e.emitSignal(ctx, signalOpEnqueue, enqueuePayload{Origin: e.instanceID, Shard: shard, StepID: stepID})
}

func (e *Engine) signalStatusChange(ctx context.Context, flowKey, status string) {
	e.emitSignal(ctx, signalOpStatusChange, statusChangePayload{Origin: e.instanceID, FlowKey: flowKey, Status: status})
}

// signalPeersChanged tells peers the fleet changed so they re-read the registry (join at Startup, leave
// at Shutdown). Fire-and-forget: a lost nudge only delays a peer's recount to its next heartbeat.
func (e *Engine) signalPeersChanged(ctx context.Context) {
	e.emitSignal(ctx, signalOpPeersChanged, peerPayload{Origin: e.instanceID})
}

// DeliverSignal processes an inbound peer signal. The host calls it with the op routing key and the
// payload bytes it received from a peer (the JSON encoding of what the engine handed that peer's
// SignalPeers). It delegates by op to the matching internal handler. op and payload are opaque to the
// host; only the engine interprets them.
//
// Trust boundary: the host MUST authenticate the peer channel; a signal admitted here is trusted.
//
// An engine that is not running (never started, or already shut down) discards every signal: there is
// no cache to ring a doorbell into, no waiter to wake, and no pool to resize. Returns nil, not an
// error: peer signals are fire-and-forget hints, and a dropped one is always recoverable by the
// receiver's own backstop (the poll for doorbells, the Await re-snapshot for status changes, the next
// heartbeat's registry re-read for the replica count).
func (e *Engine) DeliverSignal(ctx context.Context, op string, payload []byte) error {
	if !e.started.Load() {
		return nil
	}
	switch signalOp(op) {
	case signalOpEnqueue:
		var p enqueuePayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		if p.Origin == e.instanceID {
			return nil // the host echoed this engine's own broadcast back; nothing new to learn
		}
		e.handleEnqueue(ctx, p.Shard, p.StepID)
	case signalOpStatusChange:
		var p statusChangePayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		if p.Origin == e.instanceID {
			return nil
		}
		e.notifyStatusChange(p.FlowKey, p.Status)
	case signalOpPeersChanged:
		var p peerPayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		if p.Origin == e.instanceID {
			return nil // own echo: a recount would just re-read our own row, nothing to learn
		}
		e.refreshReplicaCount(ctx)
	default:
		return errors.New("unknown peer signal op: %q", op)
	}
	return nil
}

// --- Peer discovery (observed replica count R, DB-backed) ---
//
// The engine discovers how many replicas share its shards' databases from the shared dwarf_peers
// registry, NOT from the host transport: each replica heartbeats its own row (engine_id, seen_at) into
// every shard and reads R = COUNT of rows seen within peerFreshWindow. The host's peersChanged signal
// is a best-effort nudge that only accelerates a peer's recount; correctness rests on the registry and
// the heartbeat, so a host that no-ops SignalPeers degrades convergence SPEED, not the count itself.
//
// R divides the derived per-shard connection pools (see recomputePools): the budget is a property of
// the shard's DATABASE, and n replicas each holding the full budget would overshoot the measured knee
// n times over.
//
// This is deliberately a LOOKUP, not a control loop: the count is exact and discrete, independent of
// the actuation (a shrunk pool still heartbeats). R is a tuning number - a wrong count mis-sizes pools
// and degrades performance but corrupts nothing.
//
// Placement and fault-tolerance: the registry is written to and read from ALL shards via OnEach (the
// same fan-out every other cross-shard op uses), so a future shard-fault-tolerant OnEach lets the count
// read from the survivors with no change here. Today OnEach is all-or-nothing, which is correct: today
// a down shard fails everything, so the peer count degrades exactly in lockstep with the rest of the
// engine. The read takes MAX across shards - every shard holds the same population, so MAX is the
// most-complete view and errs toward under-counting (which only grows pools, the safe direction).

// observedReplicas returns the observed replica count R (self included), read from the registry by the
// heartbeat / Startup discovery and cached in observedR. Never below 1.
func (e *Engine) observedReplicas() int {
	return max(1, int(e.observedR.Load()))
}

// peerFreshWindow / peerStragglerAge are derived from the heartbeat cadence: a replica beating every
// pingInterval is COUNTED while its row is younger than 4x the cadence (so up to ~3 missed beats are
// tolerated before it drops out - a crashed peer's row, whose owner never sends goodbye, ages out of
// the count on its own), and its dead row is DELETED once older than 8x (pure table hygiene, well past
// the point it stopped being counted).
func (e *Engine) peerFreshWindow() time.Duration  { return 4 * e.pingInterval }
func (e *Engine) peerStragglerAge() time.Duration { return 8 * e.pingInterval }

// upsertSelf writes this engine's heartbeat row (seen_at=NOW_UTC()) to one shard: an UPDATE by
// engine_id, falling back to an INSERT when no row exists yet (the first beat, or after the row was
// pruned). Two statements rather than a per-dialect upsert, so it stays dialect-agnostic. Timestamps
// come from the database clock (NOW_UTC()), never a bound Go time, so every shard's freshness compare
// runs on one clock.
func (e *Engine) upsertSelf(ctx context.Context, db *sequel.DB) error {
	res, err := db.ExecContext(ctx, "UPDATE dwarf_peers SET seen_at=NOW_UTC() WHERE engine_id=?", e.engineID)
	if err != nil {
		return errors.Trace(err)
	}
	// RowsAffected is 0 only when no row matched (seen_at=NOW_UTC() always changes across the >=1
	// heartbeat interval between writes, so a matched row never reports "unchanged" on MySQL).
	n, err := res.RowsAffected()
	if err != nil {
		return errors.Trace(err)
	}
	if n > 0 {
		return nil
	}
	_, err = db.ExecContext(ctx, "INSERT INTO dwarf_peers (engine_id, seen_at) VALUES (?, NOW_UTC())", e.engineID)
	return errors.Trace(err)
}

// writePeerHeartbeat upserts this replica's row and prunes long-dead rows on every shard (parallel via
// OnEach). Called once at Startup and every pingInterval by runPeersLoop.
func (e *Engine) writePeerHeartbeat(ctx context.Context) error {
	straggleMs := e.peerStragglerAge().Milliseconds()
	return e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if err := e.upsertSelf(ctx, db); err != nil {
			return errors.Trace(err)
		}
		// Prune rows older than peerStragglerAge - hygiene, not counting (the read filter already excludes
		// them). A crashed peer's row is removed here; the survivors keep the table tiny (one row per live
		// replica), so the unindexed full scan the count relies on stays trivially fast.
		_, err := db.ExecContext(ctx,
			"DELETE FROM dwarf_peers WHERE seen_at < DATE_ADD_MILLIS(NOW_UTC(), ?)", -straggleMs)
		return errors.Trace(err)
	})
}

// readReplicaCount reads R from the registry: COUNT of rows seen within peerFreshWindow on each shard
// (parallel via OnEach), taking the MAX across shards. Never returns below 1 (this replica's own row is
// always fresh; a transient miss under-counts, which only grows pools).
func (e *Engine) readReplicaCount(ctx context.Context) (int, error) {
	freshMs := e.peerFreshWindow().Milliseconds()
	indices, pos := e.shardOrdinals()
	counts := make([]int, len(indices))
	err := e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		var n int
		err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM dwarf_peers WHERE seen_at > DATE_ADD_MILLIS(NOW_UTC(), ?)", -freshMs,
		).Scan(&n)
		if err != nil {
			return errors.Trace(err)
		}
		counts[pos[shard]] = n
		return nil
	})
	if err != nil {
		return 0, errors.Trace(err)
	}
	r := 1
	for _, n := range counts {
		r = max(r, n)
	}
	return r, nil
}

// refreshReplicaCount re-reads R and, when it changed, recomputes the pools. The peersChanged handler
// and the heartbeat loop share it. A read error is logged and dropped (R is a tuning number; the next
// heartbeat retries) - it never disturbs the last good count.
func (e *Engine) refreshReplicaCount(ctx context.Context) {
	r, err := e.readReplicaCount(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "Reading peer replica count", "error", err)
		return
	}
	e.observedR.Store(int32(r))
	// recomputePools dedupes on lastAppliedR, so an unchanged R is a cheap no-op; it also early-returns
	// under a SetMaxOpenConns override.
	e.recomputePools()
}

// discoverReplicasAtStartup registers this replica in the registry, nudges peers to re-read it, waits
// startupPeerSettle for a simultaneously-starting fleet's rows to land, then reads R. Synchronous and
// run BEFORE any worker dispatches, so R is known before the replica takes on work - there is no async
// grace window and no partial-count over-connect: a cold-starting fleet's rows settle in the registry,
// then one read yields the converged count. Returns R (>=1); any error falls back to 1 (never fatal to
// Startup - a mis-count is a tuning error the first heartbeat corrects).
//
// The settle exists for the simultaneous cold start: without it an early reader races the other
// starters' inserts and sees a partial fleet. It is skipped under an override (R does not size the
// pools then) and disabled in tests (startupPeerSettle=0).
func (e *Engine) discoverReplicasAtStartup(ctx context.Context, override int) int {
	if err := e.writePeerHeartbeat(ctx); err != nil {
		e.logger.ErrorContext(ctx, "Registering peer at startup", "error", err)
	}
	if override > 0 {
		// Pools are pinned to the override and never divided by R, so there is nothing to settle for or
		// size from. Still register (above) so peers count this replica against the server budget.
		if r, err := e.readReplicaCount(ctx); err == nil {
			return r
		}
		return 1
	}
	e.signalPeersChanged(ctx)
	// The settle covers the simultaneous cold start (let peers' rows land before reading). It is skipped
	// under test so the suite does not pay it on every RunInTest; the peer tests drive R by writing the
	// registry directly and reading it back, so they never need the wait.
	if startupPeerSettle > 0 && !testing.Testing() {
		select {
		case <-ctx.Done():
		case <-time.After(startupPeerSettle):
		}
	}
	r, err := e.readReplicaCount(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "Reading peer replica count at startup", "error", err)
		return 1
	}
	return r
}

// runPeersLoop is the heartbeat: every pingInterval, upsert this replica's row + prune dead rows, then
// re-read R and recompute pools if the fleet changed. This is what catches a vanished peer with no
// signal - a crashed replica sends no goodbye, so nothing nudges a recount; the periodic re-read drops
// it once its row ages past peerFreshWindow. Started by initRuntime; stopped via peersStop in
// drainRuntime.
func (e *Engine) runPeersLoop() {
	ticker := time.NewTicker(e.pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.peersStop:
			return
		case <-ticker.C:
		}
		if err := e.writePeerHeartbeat(e.lifetimeCtx); err != nil {
			e.logger.ErrorContext(e.lifetimeCtx, "Writing peer heartbeat", "error", err)
		}
		e.refreshReplicaCount(e.lifetimeCtx)
	}
}

// deregisterPeer deletes this replica's row from every shard on graceful shutdown, so peers recount
// without waiting out peerStragglerAge. Paired with a peersChanged nudge (see drainRuntime). Best
// effort: a failed delete just leaves a row that ages out of the count on its own.
func (e *Engine) deregisterPeer(ctx context.Context) error {
	return e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		_, err := db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=?", e.engineID)
		return errors.Trace(err)
	})
}
