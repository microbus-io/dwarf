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
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// signalOp identifies which internal handler an inbound peer signal targets. It is the op routing key
// passed across the host boundary as a plain string.
type signalOp string

// There is deliberately NO work-doorbell op. An `enqueue` signal existed and was removed: its volume
// scaled with steps/s (R-1 messages per step) while buying no dispatch latency the receiver's own
// refiller scan would not have covered, and it cost every receiver a PK lookup plus an unpartitioned
// head-insert that raced the residue class's owner for the claim CAS. Work discovery is now entirely
// pull-based (each shard's refiller, bounded by refillIdleInterval). Do not re-add a per-step op here.
const (
	signalOpStatusChange signalOp = "statusChange"
	// signalOpPeersChanged is a best-effort nudge that the fleet changed (a replica joined or left):
	// receivers re-read the shared dwarf_peers registry and resize their pools. The registry is the
	// source of truth for the replica count R; this signal only accelerates convergence from the
	// heartbeat cadence (pingInterval) to sub-second. A host that no-ops SignalPeers therefore degrades
	// convergence SPEED, never correctness - every replica still re-reads R on its own heartbeat.
	signalOpPeersChanged signalOp = "peersChanged"
)

// Per-op payload bodies. The engine marshals these in emitSignal and unmarshals the received bytes in
// DeliverSignal. Origin carries the sending engine's random engineIDBase36: SignalPeers' contract asks the
// host to deliver only to OTHER replicas, but a broadcast transport may echo the signal back to the
// sender - DeliverSignal discards a payload whose Origin matches its own engineIDBase36 rather than rely
// on the host. (An empty Origin - e.g. a signal from an older build - is never discarded.)
type (
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
		// FaultSignalPeersPanic simulates the host's SignalPeers panicking, so the test can prove the
		// boundary CatchPanic swallows it (logged below) and flow completion / Await are unaffected. Scoped
		// by op so a test targets one signal kind without tripping the others.
		if e.seams.IsFault(FaultSignalPeersPanic, string(op)) {
			panic("injected fault: " + FaultSignalPeersPanic + " " + string(op))
		}
		e.host.SignalPeers(ctx, string(op), data)
		return nil
	})
	if err != nil {
		e.logger.ErrorContext(ctx, "SignalPeers callback panicked", "op", string(op), "error", err)
	}
}

func (e *Engine) signalStatusChange(ctx context.Context, flowKey, status string) {
	e.emitSignal(ctx, signalOpStatusChange, statusChangePayload{Origin: e.engineIDBase36, FlowKey: flowKey, Status: status})
}

// signalPeersChanged tells peers the fleet changed so they re-read the registry (join at Startup, leave
// at Shutdown). Fire-and-forget: a lost nudge only delays a peer's recount to its next heartbeat.
func (e *Engine) signalPeersChanged(ctx context.Context) {
	e.emitSignal(ctx, signalOpPeersChanged, peerPayload{Origin: e.engineIDBase36})
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
// receiver's own backstop (the Await re-snapshot for status changes, the next heartbeat's registry
// re-read for the replica count).
func (e *Engine) DeliverSignal(ctx context.Context, op string, payload []byte) error {
	if !e.started.Load() {
		return nil
	}
	switch signalOp(op) {
	case signalOpStatusChange:
		var p statusChangePayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		if p.Origin == e.engineIDBase36 {
			return nil
		}
		e.notifyStatusChange(p.FlowKey, p.Status)
	case signalOpPeersChanged:
		var p peerPayload
		err := json.Unmarshal(payload, &p)
		if err != nil {
			return errors.Trace(err)
		}
		if p.Origin == e.engineIDBase36 {
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

// observedPartition returns the (R, ordinal) pair that partitions candidate selection across replicas,
// or ok=false when selection must NOT be partitioned - a solo replica (nothing to divide) or an unknown
// ordinal (self missing from the roster). Both halves come from ONE roster read: deriving R from one
// shard's count and the ordinal from another's list could place two replicas on the same ordinal, or
// leave a residue class owned by nobody.
func (e *Engine) observedPartition() (replicas, ordinal int, ok bool) {
	// The DISPATCHER count, not R: R divides connection pools (every peer holds connections), while this
	// divides work (only peers with workers ever claim it). Using R here would assign a residue class to
	// an await-only replica, and nothing would ever select it.
	d := int(e.observedDispatchers.Load())
	o := int(e.observedOrdinal.Load())
	if d < 2 || o < 0 || o >= d {
		return d, 0, false
	}
	return d, o, true
}

// rosterOrdinal finds engineID's 0-based position among engine_id-sorted ids, or -1 when absent. The
// sort is what makes the assignment agree across replicas without any coordination: every replica reads
// the same registry rows and orders them the same way, so each computes a distinct ordinal from the
// same list.
func rosterOrdinal(ids []int64, engineID int64) int {
	return slices.Index(ids, engineID)
}

// peerFreshWindow / peerStragglerAge are derived from the heartbeat cadence: a replica beating every
// pingInterval is COUNTED while its row is younger than 4x the cadence (so up to ~3 missed beats are
// tolerated before it drops out - a crashed peer's row, whose owner never sends goodbye, ages out of
// the count on its own), and its dead row becomes eligible for the DELETE once older than 8x (pure table
// hygiene, well past the point it stopped being counted; conditionally/statistically pruned - see
// heartbeatPeers).
func (e *Engine) peerFreshWindow() time.Duration  { return 4 * e.pingInterval }
func (e *Engine) peerStragglerAge() time.Duration { return 8 * e.pingInterval }

// peerDispatchWindow is how recently a replica must have advanced dispatched_at to be counted among the
// DISPATCHERS - the divisor the candidate partition splits across, distinct from R.
//
// It is deliberately far tighter than peerFreshWindow, because the two errors are asymmetric in OPPOSITE
// directions. Over-counting R over-sizes pools and can collapse a database, so R is generous. Over-counting
// dispatchers hands a residue class of step ids to a replica that never selects them, and work in that
// class is then run by nobody - while UNDER-counting only makes replicas select overlapping candidates,
// which the claim CAS arbitrates at the cost of a lost round trip. So this one errs toward dropping a
// replica, where R errs toward keeping one.
//
// Five seconds is five of the piston's one-second beats. That margin is only safe because dispatched_at
// also advances while a cycle is IN FLIGHT (see piston.beat): without it, a deep-backlog scan - still
// O(backlog) on any dialect without the run-condition early-stop, and measured in the tens of seconds -
// would let every healthy replica in a loaded fleet fall out of the divisor at once, disabling
// partitioning precisely when overlapping selection is most expensive.
const peerDispatchWindow = 5 * time.Second

// upsertSelf CREATES this engine's registry row on one shard: an UPDATE by engine_id, falling back to an
// INSERT when no row exists yet. Two statements rather than a per-dialect upsert, so it stays
// dialect-agnostic. Timestamps come from the database clock (NOW_UTC()), never a bound Go time, so every
// shard's freshness compare runs on one clock.
//
// This runs ONCE, from registerSelf at Startup - before any piston exists. The ongoing refresh is the
// pistons' (piston.beat), which only ever UPDATEs, so the row's lifetime is exactly "created here, deleted
// by deregisterPeer" with in-place refreshes in between.
//
// It deliberately does NOT stamp dispatched_at. That column is EVIDENCE that a piston turned, never a
// statement of intent, and registration is pure intent - so a replica leaves it at its far-past default
// and earns it on its first cycle instead. The cost is a brief window where the replica is not counted
// among the dispatchers, which is the fail-open direction (it partitions nothing and selects everything,
// overlapping peers at the price of a lost claim); the piston beats as soon as that first cycle lands, so
// the window is milliseconds rather than a heartbeat interval.
func (e *Engine) upsertSelf(ctx context.Context, db *sequel.DB) error {
	res, err := db.ExecContext(ctx,
		"UPDATE dwarf_peers SET seen_at=NOW_UTC() WHERE engine_id=?", e.engineID)
	if err != nil {
		return errors.Trace(err)
	}
	// RowsAffected is 0 only when no row matched: seen_at=NOW_UTC() always changes between two calls of
	// this (they are a whole engine lifetime apart), so a matched row never reports "unchanged" on MySQL.
	n, err := res.RowsAffected()
	if err != nil {
		return errors.Trace(err)
	}
	if n > 0 {
		return nil
	}
	_, err = db.ExecContext(ctx,
		"INSERT INTO dwarf_peers (engine_id, seen_at) VALUES (?, NOW_UTC())", e.engineID)
	return errors.Trace(err)
}

// registerSelf upserts this replica's row on every shard (parallel via OnEach). The Startup register
// (pre-settle); it does NOT prune, so the settle sees a clean write pass - pruning is the heartbeat's
// job (heartbeatPeers), and any stragglers from a prior crashed instance are cleaned there within a
// heartbeat (the count filter excludes them meanwhile).
func (e *Engine) registerSelf(ctx context.Context) error {
	return e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		return errors.Trace(e.upsertSelf(ctx, db))
	})
}

// shardRoster is one shard's view of the fleet: the engine_id-sorted fresh peers, and how stale that
// view is (the age of its most recent heartbeat).
type shardRoster struct {
	// ids is every fresh peer - what R (the pool divisor) counts, since an await-only replica still holds
	// connections. dispatchers is the subset with workers, which is what the candidate partition divides.
	ids         []int64
	dispatchers []int64
	// freshest is ms since this shard's newest heartbeat; math.MaxFloat64 when the shard reported none.
	// float64 because DATE_DIFF_MILLIS is fractional on SQLite (scanning it into an int64 fails outright),
	// the same type censusRow.ageMs carries for the identical expression.
	freshest float64
}

// freshestRoster picks the shard whose registry saw a heartbeat most recently, and returns that shard's
// roster whole. Picking ONE shard's view is the point: R is its length and the ordinal is a position
// within it, so assembling them from two shards could seat two replicas on one ordinal or leave a
// residue class owned by nobody.
//
// Ranked on AGE, never on raw seen_at. Every shard stamps its own NOW_UTC(), so timestamps are not
// comparable across shards - a shard whose clock runs fast would always look freshest and would win
// every pick regardless of its actual staleness. `NOW_UTC() - seen_at` puts both terms on one shard's
// clock, so the offset cancels exactly (the same cancellation the refiller's per-shard age relies on).
// Ties break on the lexicographically smaller roster so every replica resolves an exact tie identically.
func freshestRoster(rosters []shardRoster) shardRoster {
	best := -1
	for i, r := range rosters {
		if len(r.ids) == 0 {
			continue
		}
		if best < 0 || r.freshest < rosters[best].freshest ||
			(r.freshest == rosters[best].freshest && slices.Compare(r.ids, rosters[best].ids) < 0) {
			best = i
		}
	}
	if best < 0 {
		return shardRoster{}
	}
	return rosters[best]
}

// scanFreshPeers reads one shard's engine_id-sorted roster of peers inside peerFreshWindow, plus the age
// of its newest heartbeat. ORDER BY in SQL rather than in Go so the ordering is the database's, identical
// for every replica reading the same rows - which is what lets each replica compute a distinct ordinal
// from the same list with no coordination.
func (e *Engine) scanFreshPeers(ctx context.Context, db *sequel.DB, freshMs int64) (shardRoster, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT engine_id, DATE_DIFF_MILLIS(NOW_UTC(), seen_at) AS age_ms,"+
			" DATE_DIFF_MILLIS(NOW_UTC(), dispatched_at) AS dispatch_age_ms FROM dwarf_peers"+
			" WHERE seen_at > DATE_ADD_MILLIS(NOW_UTC(), ?) ORDER BY engine_id", -freshMs,
	)
	if err != nil {
		return shardRoster{}, errors.Trace(err)
	}
	defer rows.Close()
	dispatchMs := float64(peerDispatchWindow / time.Millisecond)
	roster := shardRoster{freshest: math.MaxFloat64}
	for rows.Next() {
		var id int64
		var ageMs, dispatchAgeMs float64
		if err := rows.Scan(&id, &ageMs, &dispatchAgeMs); err != nil {
			return shardRoster{}, errors.Trace(err)
		}
		roster.ids = append(roster.ids, id)
		// EVIDENCE, not a claim: dispatched_at is advanced only by a replica whose piston actually turned,
		// so a wedged one drops out of the partition divisor on its own while its seen_at keeps it in R.
		// This REPLACED a `dispatches` flag, which carried the same fact as a claim: a replica that
		// published "I dispatch" and then wedged kept its residue class forever, and a class nobody selects
		// is work nobody runs. The column was dropped once this became its only reader.
		if dispatchAgeMs <= dispatchMs {
			roster.dispatchers = append(roster.dispatchers, id)
		}
		roster.freshest = min(roster.freshest, ageMs)
	}
	if err := rows.Err(); err != nil {
		return shardRoster{}, errors.Trace(err)
	}
	return roster, nil
}

// readReplicaCount reads the peer roster from the registry: each shard's engine_id-sorted fresh peers
// (parallel via OnEach), returning the freshest shard's roster whole - R is its length, the ordinal a
// position within it, so both must come from one shard's view (see freshestRoster). Pure read - no write,
// no prune - so it is the path the peersChanged nudge and the Startup post-settle read use.
func (e *Engine) readReplicaCount(ctx context.Context) (shardRoster, error) {
	freshMs := e.peerFreshWindow().Milliseconds()
	indices, pos := e.shardOrdinals()
	rosters := make([]shardRoster, len(indices))
	err := e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		roster, err := e.scanFreshPeers(ctx, db, freshMs)
		if err != nil {
			return errors.Trace(err)
		}
		rosters[pos[shard]] = roster
		return nil
	})
	if err != nil {
		return shardRoster{}, errors.Trace(err)
	}
	return freshestRoster(rosters), nil
}

// heartbeatPeers is the heartbeat's single per-shard pass: read the fresh roster AND the stale count, and
// conditionally prune - then return the freshest shard's roster. It no longer writes this replica's own
// row; the pistons do that. The prune runs only
// when a shard has stale rows (`stale > 0`) AND this replica wins a 1/R dice roll, so:
//   - steady state issues ZERO range-DELETEs (only conflict-free per-PK upserts touch the table), which
//     removes the one op with any lock-contention potential (the MySQL gap-lock on `seen_at <`) from the
//     common path; and
//   - a crash's stragglers are cleaned by ~one replica per round rather than an N-way concurrent DELETE
//     burst exactly when a node died.
//
// Pruning is pure hygiene - the fresh filter already excludes stale rows from the roster - so a delayed
// or skipped prune is harmless; a solo replica (R=1) always wins the roll, so its own restart stragglers
// never linger. The stale count is a second aggregate query (it cannot ride the roster scan, which lists
// only fresh rows) - the one place the heartbeat costs a round trip the pure-read path does not.
func (e *Engine) heartbeatPeers(ctx context.Context) (shardRoster, error) {
	freshMs := e.peerFreshWindow().Milliseconds()
	straggleMs := e.peerStragglerAge().Milliseconds()
	indices, pos := e.shardOrdinals()
	rosters := make([]shardRoster, len(indices))
	err := e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		// No steady-state upsert here: the PISTONS own the heartbeat write, one per shard on their own
		// cadence, and this pass is a reader plus the prune. registerSelf writes once at Startup, before any
		// piston exists.
		//
		// The roster replaces the fresh COUNT: it carries the same number (its length) plus the identities
		// the ordinal needs, so this still costs one scan. The stale count keeps its own aggregate, which
		// cannot ride the roster query - stale rows are excluded from it by construction.
		roster, err := e.scanFreshPeers(ctx, db, freshMs)
		if err != nil {
			return errors.Trace(err)
		}

		// SELF-HEAL: the beat is UPDATE-only, so a row that has gone MISSING is refreshed by nobody and
		// matches zero rows forever, silently. It can go missing: this replica's beats to one shard failing
		// (or its process stalling) for peerStragglerAge lets a peer's prune delete the row, and then the
		// replica is permanently absent from R - peers UNDER-count, which over-sizes their pools, the
		// direction that collapses a database - while its own partitioning fails open and it overlaps every
		// peer. That used to be self-correcting within a pingInterval, back when this loop upserted.
		//
		// Re-registering HERE rather than restoring the beat's INSERT fallback keeps exactly one steady-state
		// writer per row, so the two can never race; this fires only on a state that is already broken. It is
		// checked per shard, against that shard's own roster, because a row can be pruned on one shard while
		// alive on another and freshestRoster reports only one shard's view. Ordered before the prune below,
		// so the row it writes is fresh and cannot be swept by this same pass.
		if len(roster.ids) > 0 && rosterOrdinal(roster.ids, e.engineID) < 0 {
			e.logger.WarnContext(ctx, "Peer registry row missing; re-registering", "shard", shard)
			if err := e.upsertSelf(ctx, db); err != nil {
				return errors.Trace(err)
			}
		}
		var stale int
		err = db.QueryRowContext(ctx,
			"SELECT COALESCE(SUM(CASE WHEN seen_at <= DATE_ADD_MILLIS(NOW_UTC(), ?) THEN 1 ELSE 0 END), 0)"+
				" FROM dwarf_peers",
			-straggleMs,
		).Scan(&stale)
		if err != nil {
			return errors.Trace(err)
		}
		rosters[pos[shard]] = roster
		if stale > 0 && rand.IntN(max(1, len(roster.ids))) == 0 {
			_, err := db.ExecContext(ctx,
				"DELETE FROM dwarf_peers WHERE seen_at < DATE_ADD_MILLIS(NOW_UTC(), ?)", -straggleMs)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return shardRoster{}, errors.Trace(err)
	}
	return freshestRoster(rosters), nil
}

// applyReplicaCount stores R and recomputes the pools. recomputePools dedupes on lastAppliedR, so an
// unchanged R is a cheap no-op; it also early-returns under a SetMaxOpenConns override.
func (e *Engine) applyReplicaCount(roster shardRoster) {
	// Store the ordinal BEFORE R. Both are read together by observedPartition, and the pair is only ever
	// used to EXCLUDE rows: publishing a stale-low ordinal against a fresh-high R is inert (it selects a
	// residue class this replica already owned), while a stale-high ordinal against a fresh-low R would
	// fail the o<r bound and disable partitioning - the safe direction either way.
	if len(roster.ids) == 0 {
		// A transient read miss: keep the last good triple rather than collapsing to R=1, which would
		// re-expand the pools on a blip.
		return
	}
	e.observedOrdinal.Store(int32(rosterOrdinal(roster.dispatchers, e.engineID)))
	e.observedDispatchers.Store(int32(len(roster.dispatchers)))
	e.observedR.Store(int32(max(1, len(roster.ids))))
	e.recomputePools()
}

// refreshReplicaCount re-reads R (pure read) and applies it - the peersChanged nudge's path. A read
// error is logged and dropped (R is a tuning number; the next heartbeat retries), never disturbing the
// last good count.
func (e *Engine) refreshReplicaCount(ctx context.Context) {
	roster, err := e.readReplicaCount(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "Reading peer replica count", "error", err)
		return
	}
	e.applyReplicaCount(roster)
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
	if err := e.registerSelf(ctx); err != nil {
		e.logger.ErrorContext(ctx, "Registering peer at startup", "error", err)
	}
	if override > 0 {
		// Pools are pinned to the override and never divided by R, so there is nothing to settle for or
		// size from. Still register (above) so peers count this replica against the server budget.
		if roster, err := e.readReplicaCount(ctx); err == nil {
			return max(1, len(roster.ids))
		}
		return 1
	}
	e.signalPeersChanged(ctx)
	// The settle covers the simultaneous cold start (let peers' rows land before reading). It is skipped
	// under test so the suite does not pay it on every engine startup; the peer tests drive R by writing the
	// registry directly and reading it back, so they never need the wait.
	if startupPeerSettle > 0 && !testing.Testing() {
		select {
		case <-ctx.Done():
		case <-time.After(startupPeerSettle):
		}
	}
	roster, err := e.readReplicaCount(ctx)
	if err != nil {
		e.logger.ErrorContext(ctx, "Reading peer replica count at startup", "error", err)
		return 1
	}
	// Startup seeds the ordinal too: the refiller may run before the first heartbeat, and an unset
	// ordinal would leave partitioning disabled for a whole ping interval.
	e.observedOrdinal.Store(int32(rosterOrdinal(roster.dispatchers, e.engineID)))
	e.observedDispatchers.Store(int32(len(roster.dispatchers)))
	return max(1, len(roster.ids))
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
		// One pass: read R + conditional prune (the pistons write our own row). Apply the count directly.
		roster, err := e.heartbeatPeers(e.lifetimeCtx)
		if err != nil {
			e.logger.ErrorContext(e.lifetimeCtx, "Peer heartbeat", "error", err)
			continue
		}
		e.applyReplicaCount(roster)
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
