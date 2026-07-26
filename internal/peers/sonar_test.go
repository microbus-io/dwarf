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

package peers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// selfID is the replica identity every rig is built with; the registry is keyed on it.
const selfID = 4242

// staleAge is any age old enough that no live beat could have produced it. The dispatch column's default
// sits decades back, so this separates "never" from "just now" unambiguously.
const staleAge = 24 * 60 * 60 * 1000.0

// testWindows are the classification thresholds the pure-policy tests apply, spelled out rather than taken
// from the constants so a change in policy shows up as a changed expectation and not a changed test.
var testWindows = windows{fresh: 40000, dispatch: 5000, straggler: 80000}

// fakeClock is the Sonar's clock under test. Every time-dependent decision - blindness, the healthy run
// behind the prune, the beat cadence - is driven by advancing it rather than by sleeping, so the tests stay
// fast and parallel.
//
// step makes every reading of the clock cost time, which is how a pass takes a measurable interval against a
// clock the test drives. Zero for every test but the pacing one, where the whole question is what a pass
// costs.
type fakeClock struct {
	ns   atomic.Int64
	step atomic.Int64
}

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC).UnixNano())
	return c
}

func (c *fakeClock) now() time.Time {
	// One load, so the value advanced by and the value subtracted back are the same one.
	step := c.step.Load()
	return time.Unix(0, c.ns.Add(step)-step).UTC()
}
func (c *fakeClock) advance(d time.Duration) { c.ns.Add(int64(d)) }

// rig is one Sonar over its own isolated, migrated database.
type rig struct {
	s   *Sonar
	db  *sequel.DB
	clk *fakeClock
}

// newRig stands up an isolated database for the test and wires a Sonar over it on a fake clock.
// internal/database is a test-only dependency here - the Sonar itself never opens or closes a handle.
//
// The cadences and windows are plain fields, immutable once anything is driven, so a test wanting different
// ones assigns them here rather than through an API no owner would call.
func newRig(t *testing.T) *rig {
	t.Helper()
	sum := sha256.Sum256([]byte(t.Name()))
	var set database.ShardSet
	assert := testarossa.For(t)
	assert.NoError(set.Open(context.Background(), database.Config{
		Shards:      map[int]database.ShardConfig{1: {MaxIdleConns: 2, MaxOpenConns: 4}},
		TestID:      hex.EncodeToString(sum[:8]),
		TestConnCap: 4,
	}))
	t.Cleanup(set.Close)
	db, err := set.Shard(1)
	assert.NoError(err)
	s, err := New(selfID, 1, db)
	assert.NoError(err)
	clk := newFakeClock()
	// Both, together: New seeded lastGood from the real clock, which against the fake one would read as
	// either decades of blindness or none at all.
	s.now = clk.now
	s.lastGood.Store(clk.now().UnixNano())
	return &rig{s: s, db: db, clk: clk}
}

// addPeer inserts another replica's row with both timestamps placed at a chosen age. Ages are computed in
// SQL against the database clock - never round-tripped through Go - so "stale" means the same thing here as
// it does to the Sonar.
func (r *rig) addPeer(t *testing.T, id int64, seenAge, dispatchAge time.Duration) {
	t.Helper()
	assert := testarossa.For(t)
	_, err := r.db.ExecContext(context.Background(),
		"INSERT INTO dwarf_peers (engine_id, seen_at, dispatched_at) VALUES (?,"+
			" DATE_ADD_MILLIS(NOW_UTC(), ?), DATE_ADD_MILLIS(NOW_UTC(), ?))",
		id, -seenAge.Milliseconds(), -dispatchAge.Milliseconds())
	assert.NoError(err)
}

// ids returns the registry's engine ids, so a test can see what a delete did or did not remove.
func (r *rig) ids(t *testing.T) []int64 {
	t.Helper()
	assert := testarossa.For(t)
	rows, err := r.db.QueryContext(context.Background(),
		"SELECT engine_id FROM dwarf_peers ORDER BY engine_id")
	if !assert.NoError(err) {
		return nil
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		assert.NoError(rows.Scan(&id))
		out = append(out, id)
	}
	return out
}

// row returns one registry row as the Sonar would read it.
func (r *rig) row(t *testing.T, id int64) peer {
	t.Helper()
	assert := testarossa.For(t)
	var found peer
	for _, p := range r.read(t) {
		if p.engineID == id {
			found = p
		}
	}
	// Reported here rather than left to the caller: an absent row reads as a zero age, which would pass a
	// "this timestamp is fresh" assertion for the wrong reason.
	assert.NotZero(found.engineID, "no registry row for engine %d", id)
	return found
}

func (r *rig) read(t *testing.T) []peer {
	t.Helper()
	assert := testarossa.For(t)
	rows, err := r.s.read(context.Background())
	assert.NoError(err)
	return rows
}

// evidence returns an EvidenceFunc a test can move, standing in for the owner's dispatcher.
func evidence(turns *atomic.Uint64, busy, idle *atomic.Bool) EvidenceFunc {
	return func() (uint64, bool, bool) { return turns.Load(), busy.Load(), idle.Load() }
}

// TestPeers_ClassifyCountsAndOrdinal pins the two counts against one snapshot. Every fresh row divides the
// connection pool, only a row that also proved it dispatches divides the work, and the ordinal is a position
// among the latter - in the engine_id order the read imposes, which is what lets each replica derive a
// distinct one with no coordination.
func TestPeers_ClassifyCountsAndOrdinal(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	v := classify([]peer{
		{engineID: 10, seenAgeMs: 100, dispatchAgeMs: 100},    // fresh, dispatching
		{engineID: 20, seenAgeMs: 100, dispatchAgeMs: 30000},  // fresh, NOT dispatching (await-only/wedged)
		{engineID: selfID, seenAgeMs: 50, dispatchAgeMs: 200}, // us, dispatching
		{engineID: 90, seenAgeMs: 60000, dispatchAgeMs: 100},  // stale: counts for nothing
	}, selfID, testWindows)

	assert.Equal(3, v.replicas, "three fresh rows hold connections")
	assert.Equal(2, v.dispatchers, "only two of them proved they serve the shard")
	assert.Equal(1, v.ordinal, "we are second among the dispatchers, by engine id")
	assert.True(v.selfSeen)
	assert.Len(v.dead, 0, "60s is stale but not yet a corpse")

	// A row past the straggler age becomes a delete candidate, and only then.
	v = classify([]peer{
		{engineID: selfID, seenAgeMs: 50, dispatchAgeMs: 50},
		{engineID: 90, seenAgeMs: 90000, dispatchAgeMs: 90000},
	}, selfID, testWindows)
	assert.Equal([]int64{90}, v.dead)
}

// TestPeers_ClassifySelfAbsenceCutsBothWays pins the asymmetry that keeps a replica missing from its own
// registry from doing damage in the dangerous direction. It still counts ITSELF toward the pool divisor - it
// demonstrably exists, and under-counting there over-sizes every pool - but it must not appoint itself a
// dispatcher, because that divisor has to agree with what peers compute from the same table.
func TestPeers_ClassifySelfAbsenceCutsBothWays(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	v := classify([]peer{
		{engineID: 10, seenAgeMs: 100, dispatchAgeMs: 100},
		{engineID: 20, seenAgeMs: 100, dispatchAgeMs: 100},
	}, selfID, testWindows)

	assert.False(v.selfSeen)
	assert.Equal(3, v.replicas, "two peers plus this process, which exists whether or not its row does")
	assert.Equal(2, v.dispatchers, "self is never fudged into the work divisor")
	assert.Equal(-1, v.ordinal, "no ordinal means do not partition")

	// A row that EXISTS but has aged out counts the same way, and that case is the one worth stating: a stale
	// own row is exactly what a heartbeat starved of a connection produces, which is when over-sizing pools
	// would be most harmful.
	v = classify([]peer{
		{engineID: 10, seenAgeMs: 100, dispatchAgeMs: 100},
		{engineID: selfID, seenAgeMs: 60000, dispatchAgeMs: 60000},
	}, selfID, testWindows)
	assert.True(v.selfSeen, "the row is there, so nothing needs re-registering")
	assert.Equal(2, v.replicas, "but it is not fresh, so we still count ourselves")
	assert.Equal(-1, v.ordinal)
}

// TestPeers_ClassifyNeverCondemnsSelf pins the guard that makes a fleet-wide wipe unreachable. A replica
// whose own row has gone stale - its beats failing, or its process stalled - must never put itself on the
// delete list: nothing refreshes a deleted row, because the beat only ever UPDATEs.
func TestPeers_ClassifyNeverCondemnsSelf(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	v := classify([]peer{
		{engineID: selfID, seenAgeMs: 999999, dispatchAgeMs: 999999},
		{engineID: 90, seenAgeMs: 999999, dispatchAgeMs: 999999},
	}, selfID, testWindows)

	assert.Equal([]int64{90}, v.dead, "every row is a corpse except ours")
	assert.Equal(1, v.replicas, "we still count ourselves")
}

// TestPeers_ReplicaFallIsWithheldAcrossAGap pins the direction that matters for the pool divisor. A rise
// publishes at once - it shrinks pools, the safe direction - while a fall is not believed on the strength of
// the one reading that ended a blind spell.
//
// The case is not one peer dying but a CORRELATED stall: a spell of database trouble stalls every replica's
// beat and every replica's read at once. It clears, and the first reading afterward shows a registry in which
// every row is stale, so every replica would compute a tiny count and grow to the full per-database budget
// simultaneously, against a database that is already sick.
func TestPeers_ReplicaFallIsWithheldAcrossAGap(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	fleet := []peer{
		{engineID: 10, seenAgeMs: 100, dispatchAgeMs: 100},
		{engineID: 20, seenAgeMs: 100, dispatchAgeMs: 100},
		{engineID: selfID, seenAgeMs: 100, dispatchAgeMs: 100},
	}
	alone := fleet[2:]

	r.s.observe(ctx, fleet, nil)
	assert.Equal(3, r.s.Replicas(), "a rise is published immediately")

	// A gap, then a reading in which the fleet appears to have vanished.
	r.clk.advance(3 * r.s.scan)
	r.s.observe(ctx, alone, nil)
	assert.Equal(3, r.s.Replicas(), "the reading that ends a blind spell cannot shrink the fleet")

	// One more healthy reading is all it takes - a beat is far shorter than the read cadence, so by now every
	// live peer has refreshed its row and the count is real.
	r.clk.advance(r.s.scan)
	r.s.observe(ctx, alone, nil)
	assert.Equal(1, r.s.Replicas(), "confirmed on the next reading")

	// A RISE is never withheld, gap or no gap: it shrinks pools.
	r.clk.advance(3 * r.s.scan)
	r.s.observe(ctx, fleet, nil)
	assert.Equal(3, r.s.Replicas(), "growth in the fleet is always safe to believe")
}

// TestPeers_FailedReadHoldsTheLastGoodFleet pins the rule the rest of the design rests on: a read that did
// not happen is not an observation that anybody left. Collapsing the count on a blip would GROW every pool
// derived from it, against a database that is evidently already unwell.
func TestPeers_FailedReadHoldsTheLastGoodFleet(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.s.observe(ctx, []peer{
		{engineID: 10, seenAgeMs: 100, dispatchAgeMs: 100},
		{engineID: 20, seenAgeMs: 100, dispatchAgeMs: 100},
		{engineID: selfID, seenAgeMs: 100, dispatchAgeMs: 100},
	}, nil)
	assert.Equal(3, r.s.Replicas())

	for range 5 {
		r.clk.advance(r.s.scan)
		r.s.observe(ctx, nil, errors.New("registry unreachable"))
	}
	assert.Equal(3, r.s.Replicas(), "the fleet is held, not collapsed to one")
	assert.True(r.s.BlindFor() > 0, "and the owner can see that it is stale")
}

// TestPeers_PartitionFailsOpenWhenBlind pins the posture for the work divisor. Every reason to doubt the pair
// fails OPEN: not partitioning makes replicas select overlapping candidates, which costs a lost claim round
// trip, while partitioning on a stale pair leaves a class of steps that nobody selects.
func TestPeers_PartitionFailsOpenWhenBlind(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	r.s.observe(ctx, []peer{
		{engineID: 10, seenAgeMs: 100, dispatchAgeMs: 100},
		{engineID: selfID, seenAgeMs: 100, dispatchAgeMs: 100},
	}, nil)
	replicas, ordinal, ok := r.s.Partition()
	assert.True(ok, "two dispatchers and a known ordinal")
	assert.Equal(2, replicas)
	assert.Equal(1, ordinal)

	// One missed reading is not blindness - a healthy Sonar meets a transient error on any pass.
	r.clk.advance(2 * r.s.scan)
	_, _, ok = r.s.Partition()
	assert.True(ok, "still inside the grace")

	r.clk.advance(r.s.scan)
	_, _, ok = r.s.Partition()
	assert.False(ok, "the readings have stopped, so the pair can no longer be justified")

	// A solo dispatcher has nothing to divide, and a replica absent from the roster must not claim a class its
	// peers have handed to someone else.
	r.clk.advance(-3 * r.s.scan)
	r.s.observe(ctx, []peer{{engineID: selfID, seenAgeMs: 100, dispatchAgeMs: 100}}, nil)
	_, _, ok = r.s.Partition()
	assert.False(ok, "a solo dispatcher partitions nothing")

	r.s.observe(ctx, []peer{
		{engineID: 10, seenAgeMs: 100, dispatchAgeMs: 100},
		{engineID: 20, seenAgeMs: 100, dispatchAgeMs: 100},
	}, nil)
	_, _, ok = r.s.Partition()
	assert.False(ok, "absent from the roster: decline rather than guess")
}

// TestPeers_RegisterInsertsThenUpdatesAndBeatNeverCreates pins the row's lifecycle. Registration creates it
// once and refreshes it if it is already there; the beat only ever refreshes. That is what lets Leave be
// final - a straggler beat matches nothing instead of resurrecting a replica that has gone.
func TestPeers_RegisterInsertsThenUpdatesAndBeatNeverCreates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	assert.NoError(r.s.register(ctx))
	assert.Equal([]int64{selfID}, r.ids(t))
	assert.NoError(r.s.register(ctx), "registering again updates in place")
	assert.Equal([]int64{selfID}, r.ids(t))

	// Registration is INTENT, and the dispatch column is EVIDENCE: a replica earns it on its first turn, not
	// by announcing itself.
	assert.True(r.row(t, selfID).dispatchAgeMs > staleAge,
		"registration must not stamp the dispatch column")

	assert.NoError(r.s.Leave(ctx))
	assert.Len(r.ids(t), 0)
	r.s.publishBeat(ctx, true)
	assert.Len(r.ids(t), 0, "a beat must never re-create a deleted row")
}

// TestPeers_BeatStampsDispatchOnlyOnEvidence pins the distinction between the two timestamps. One says the
// replica is alive and holding connections, which is what the pool divisor counts; the other says it is
// genuinely serving this shard, which is what earns it a residue class of step ids. A replica handed a class
// it never selects strands the work in it, so the second must never be a claim.
func TestPeers_BeatStampsDispatchOnlyOnEvidence(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	assert.NoError(r.s.register(ctx))
	var turns atomic.Uint64
	var busy, idle atomic.Bool
	r.s.SetEvidence(evidence(&turns, &busy, &idle))

	// No turn behind it: alive, but no evidence of service.
	r.s.pass(ctx)
	assert.True(r.row(t, selfID).seenAgeMs < staleAge, "the beat proves this replica is alive")
	assert.True(r.row(t, selfID).dispatchAgeMs > staleAge, "and claims nothing more")

	// A turn is the evidence, and the next beat publishes it.
	turns.Store(1)
	r.clk.advance(r.s.beat)
	r.s.pass(ctx)
	assert.True(r.row(t, selfID).dispatchAgeMs < staleAge, "a completed turn earns the dispatch stamp")

	// An IN-FLIGHT turn is evidence too. A scan can legitimately outlast the dispatch window on a deep
	// backlog, and without this every healthy replica in a loaded fleet would drop out of the divisor at
	// once - exactly when overlapping selection costs the most.
	busy.Store(true)
	_, dispatched := r.s.dispatchEvidence()
	assert.True(dispatched, "a turn in flight is service, even with the counter unmoved")

	// Idling suppresses it outright, and the term stays explicit for the case a turn completed just before the
	// dispatcher went idle: over-counting dispatchers is the direction that strands work.
	busy.Store(false)
	turns.Store(2)
	idle.Store(true)
	_, dispatched = r.s.dispatchEvidence()
	assert.False(dispatched, "an idling dispatcher claims nothing, whatever the counter says")

	// With no evidence wired at all, a replica keeps its row alive and claims nothing - the await-only shape.
	// Absence of evidence must never read as evidence.
	r.s.SetEvidence(nil)
	_, dispatched = r.s.dispatchEvidence()
	assert.False(dispatched)
}

// TestPeers_EvidenceIsNotConsumedByLooking pins why the evidence is a plain counter rather than a flag the
// reader clears. A pass that only looks - because no beat was due - must leave the turn for the next beat to
// publish, or the one fact the owner cannot observe for itself would be silently swallowed.
func TestPeers_EvidenceIsNotConsumedByLooking(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)
	var turns atomic.Uint64
	var busy, idle atomic.Bool
	r.s.SetEvidence(evidence(&turns, &busy, &idle))

	turns.Store(1)
	for range 3 {
		_, dispatched := r.s.dispatchEvidence()
		assert.True(dispatched, "looking twice reports the same turn twice")
	}
}

// TestPeers_BeatRidesTheReadCadenceWhenEvidenceFlips pins the early publish. A starting replica's first turn
// lands milliseconds after it starts, long before its next beat would be due, and until that turn is
// published the replica is absent from its own fleet's dispatcher count. Beating on the flip bounds that to a
// scan interval instead of a beat interval - and does the same for a replica that STOPS serving, which is the
// direction that strands work.
func TestPeers_BeatRidesTheReadCadenceWhenEvidenceFlips(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	assert.NoError(r.s.register(ctx))
	var turns atomic.Uint64
	var busy, idle atomic.Bool
	r.s.SetEvidence(evidence(&turns, &busy, &idle))

	r.s.pass(ctx)
	due := r.s.nextBeat
	assert.Equal(r.clk.now().Add(r.s.beat), due, "the first pass always beats")

	// Not due, nothing changed: no beat, so the schedule is untouched.
	r.clk.advance(r.s.scan)
	r.s.pass(ctx)
	assert.Equal(due, r.s.nextBeat, "an unremarkable pass does not beat")

	// The evidence bit flips, so the beat rides this pass rather than waiting out the cadence.
	turns.Store(1)
	r.clk.advance(r.s.scan)
	r.s.pass(ctx)
	assert.True(r.s.nextBeat.After(due), "a flip in the evidence beats off-cadence")
	assert.True(r.row(t, selfID).dispatchAgeMs < staleAge)
}

// TestPeers_ReadIsUnfiltered pins that the read returns the whole table, corpses included. The freshness
// decisions are this package's, not SQL's: that is what lets every window change without touching a
// statement, gives the hygiene delete its candidates from the same reading everything else is derived from,
// and keeps a row that is ABSENT distinguishable from one that is merely stale.
func TestPeers_ReadIsUnfiltered(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	assert.NoError(r.s.register(ctx))
	r.addPeer(t, 10, time.Second, time.Second)
	r.addPeer(t, 90, 10*time.Minute, 10*time.Minute)

	rows := r.read(t)
	assert.Equal(3, len(rows), "a ten-minute-old row is still returned")
	assert.Equal(int64(10), rows[0].engineID, "engine_id ascending, ordered by the database")
	assert.Equal(int64(90), rows[1].engineID)
	assert.Equal(int64(selfID), rows[2].engineID)
	assert.True(rows[1].seenAgeMs > 9*60*1000, "and its age is the database's arithmetic")
}

// TestPeers_JoinAnnouncesWaitsAndSeedsEveryGetter pins the startup sequence as one call. The wait is the
// point rather than an implementation detail: a joining replica sizes its pool for the fleet it is joining
// while its peers still hold pools sized for the fleet without it, so announcing first is what keeps the
// shard's connection budget from being exceeded even momentarily.
func TestPeers_JoinAnnouncesWaitsAndSeedsEveryGetter(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.s.now = time.Now // the wait is real time, so this one runs on the real clock
	r.s.lastGood.Store(time.Now().UnixNano())
	r.s.scan = 20 * time.Millisecond
	r.addPeer(t, 10, time.Second, time.Second)
	r.addPeer(t, 20, time.Second, time.Second)
	var turns atomic.Uint64
	var busy, idle atomic.Bool
	turns.Store(1) // a turn behind us, so this replica counts as a dispatcher
	r.s.SetEvidence(evidence(&turns, &busy, &idle))

	started := time.Now()
	assert.NoError(r.s.Join(ctx))
	assert.True(time.Since(started) >= 2*r.s.scan,
		"the announcement must precede consumption by two read cadences")

	assert.Equal([]int64{10, 20, selfID}, r.ids(t), "announced")
	assert.Equal(3, r.s.Replicas(), "and every getter is seeded before the owner sizes anything")
	replicas, ordinal, ok := r.s.Partition()
	assert.True(ok)
	assert.Equal(3, replicas)
	assert.Equal(2, ordinal, "third by engine id among the dispatchers")
	assert.True(r.s.BlindFor() < r.s.scan, "and the reading behind them is current")
}

// TestPeers_ReRegistersWhenItsRowIsMissing pins the one repair path. Nothing else can fix a missing row - the
// beat only UPDATEs, so a row that has gone is refreshed by nobody. Left alone, peers under-count the fleet
// and over-size their pools, the direction that collapses a database.
func TestPeers_ReRegistersWhenItsRowIsMissing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.addPeer(t, 10, time.Second, time.Second)

	// Never registered: the very first pass notices and fixes it.
	r.s.pass(ctx)
	assert.NoError(r.s.lastErr)
	assert.Equal([]int64{10, selfID}, r.ids(t))

	// And again if the row is lost mid-life.
	_, err := r.db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=?", selfID)
	assert.NoError(err)
	r.clk.advance(r.s.scan)
	r.s.pass(ctx)
	assert.Equal([]int64{10, selfID}, r.ids(t))
}

// TestPeers_EmptyRegistryStillRepairsItself pins the case an emptiness test would miss. A registry with no
// rows at all - truncated, or swept by a delete that found every row stale at once - is ALWAYS wrong, since
// this process is in it by definition. Reading it as "no peers" and stopping there would leave a whole fleet
// unregistered until a restart.
func TestPeers_EmptyRegistryStillRepairsItself(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	assert.Len(r.ids(t), 0)
	r.s.pass(ctx)
	assert.NoError(r.s.lastErr)
	assert.Equal([]int64{selfID}, r.ids(t), "an empty registry is a broken one, not a quiet one")
	assert.Equal(1, r.s.Replicas())
}

// TestPeers_PruneWaitsForAHealthyRun pins the most conservative of the three thresholds, and why it is the
// most conservative: every other decision here is reversible - a wrongly dropped peer returns on the next
// reading - while a deleted row is refreshed by nobody afterward. Waiting costs nothing, because the
// freshness windows already exclude these rows from every count.
func TestPeers_PruneWaitsForAHealthyRun(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.s.straggler = time.Second
	r.s.pruneAfter = 400 * time.Millisecond
	assert.NoError(r.s.register(ctx))
	r.addPeer(t, 90, time.Minute, time.Minute) // a corpse by any measure

	r.s.pass(ctx)
	assert.Equal([]int64{90, selfID}, r.ids(t), "the first reading has no healthy run behind it")

	r.clk.advance(200 * time.Millisecond)
	r.s.pass(ctx)
	assert.Equal([]int64{90, selfID}, r.ids(t), "still short of the patience")

	r.clk.advance(200 * time.Millisecond)
	r.s.pass(ctx)
	assert.Equal([]int64{selfID}, r.ids(t), "readable long enough: the corpse goes")
}

// TestPeers_PruneStandsDownAfterAGap pins the anchor that makes a recovery storm unreachable. The case is a
// stall longer than the straggler age: it clears, every row in the table is stale at once - including this
// replica's own - and a delete anchored on the clock rather than on what was OBSERVED would empty the
// registry for the entire fleet.
func TestPeers_PruneStandsDownAfterAGap(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	r.s.straggler = time.Second
	r.s.pruneAfter = 400 * time.Millisecond
	assert.NoError(r.s.register(ctx))
	r.addPeer(t, 90, time.Minute, time.Minute)

	// Earn most of the patience...
	r.s.pass(ctx)
	r.clk.advance(300 * time.Millisecond)
	r.s.pass(ctx)

	// ...then go blind for longer than the grace. The healthy run starts over, so the reading that ends the
	// gap deletes nothing, however stale the table looks.
	r.clk.advance(10 * time.Second)
	r.s.pass(ctx)
	assert.Equal([]int64{90, selfID}, r.ids(t), "blind time must not count toward the patience")

	r.clk.advance(200 * time.Millisecond)
	r.s.pass(ctx)
	assert.Equal([]int64{90, selfID}, r.ids(t), "the run restarted, so it is short again")

	r.clk.advance(200 * time.Millisecond)
	r.s.pass(ctx)
	assert.Equal([]int64{selfID}, r.ids(t), "and resolves once the registry has stayed readable")
}

// TestPeers_RunReadsUntilTheContextEnds pins the loop itself against a real database, on the real clock:
// cancelling is the only stop signal it needs, since the read is a pure read and the beat is one idempotent
// UPDATE.
//
// The two assertions past the fleet count are a smoke check on the loop being paced at all: asserting only
// that the fleet was published would pass against a loop with no cadence whatsoever, since a rise publishes
// whether or not the reading looked like it ended a gap. They do NOT discriminate start-to-start pacing from
// end-to-start - a pass here costs a fraction of a millisecond against a 20ms interval, so both spellings
// stay far inside the grace. The interval arithmetic itself is pinned by
// TestPeers_PacingMeasuresFromThePassStart, which drives it on a clock instead of a database.
func TestPeers_RunReadsUntilTheContextEnds(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := newRig(t)
	r.s.now = time.Now
	r.s.lastGood.Store(time.Now().UnixNano())
	// A CADENCE this test drives on the real clock has to sit well clear of scheduler jitter, because every
	// assertion below is a multiple of it: the blindness grace is two of these, and a gap that wide also
	// resets the healthy run. At 20ms the grace was 40ms, which a co-running suite or a bench process on the
	// same box can eat outright - the loop then genuinely gaps and the test correctly reports a cadence that
	// the machine, not the code, failed to keep.
	r.s.scan = 100 * time.Millisecond
	r.s.beat = 100 * time.Millisecond
	assert.NoError(r.s.register(ctx))
	r.addPeer(t, 10, time.Second, time.Second)

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.s.Run(ctx)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for r.s.Replicas() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	assert.Equal(2, r.s.Replicas(), "the loop published the fleet it read")
	// The pass that published this is the one that opened the healthy run, and it is over: observe sets
	// healthySince before it stores the count. So this instant is an upper bound on healthySince that costs
	// no clock arithmetic and loosens itself on a slow machine.
	firstPass := time.Now()

	// Let it turn several more times, then check it is keeping up with its own cadence. Sampled over a
	// window rather than at one instant: the instant is a coin toss on a busy machine while the property is
	// not, since a loop that has stopped reading has BlindFor growing without bound and never once dips
	// under the grace, however long the window.
	time.Sleep(5 * r.s.scan)
	kept := false
	for deadline := time.Now().Add(2 * time.Second); !kept && time.Now().Before(deadline); {
		if kept = r.s.BlindFor() < 2*r.s.scan; !kept {
			time.Sleep(2 * time.Millisecond)
		}
	}
	assert.True(kept, "a loop keeping its cadence is never blind (blind for %s)", r.s.BlindFor())

	cancel()
	returned := false
	select {
	case <-done:
		returned = true
	case <-time.After(5 * time.Second):
	}
	assert.True(returned, "Run did not return on a cancelled context")

	// Safe to read the driving goroutine's own state only now that it has returned. If every reading had
	// looked like it ended a gap, this would have been reset on the last pass rather than set on the first,
	// and the prune's patience would never accumulate. Compared against the FIRST PASS rather than against
	// the start plus a few cadences: the first pass is an exact upper bound (it is the pass that set this),
	// so the two cases are "at or before it" and "after it" with no margin to size.
	assert.True(!r.s.healthySince.After(firstPass),
		"the healthy run was set on the first pass and never reset (reset %s into the run)",
		r.s.healthySince.Sub(firstPass))

	// Leave is the owner's, with a context that still works - Run's is cancelled by the time it returns, and
	// Run having returned is what makes the delete final.
	assert.NoError(r.s.Leave(context.Background()))
	assert.Equal([]int64{10}, r.ids(t))
}

// TestPeers_PacingMeasuresFromThePassStart pins the loop's period against the one thing that silently
// invalidates every interval derived from it. Sleeping the whole interval AFTER each pass makes the real
// period `scan + pass`, which leaves the blindness grace of two intervals with only one pass of margin - so
// under pool contention every reading starts to look like it ended a gap, and the replica count stops
// falling, the prune's patience stops accumulating, and partitioning switches off, all at once and all
// silently.
func TestPeers_PacingMeasuresFromThePassStart(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	assert.Equal(time.Duration(0), r.s.untilNextPass(), "no pass yet: read at once")

	// Make every clock reading cost a quarter of the interval, so a real pass - which reads the clock at its
	// start and again when it publishes - takes half of one. The stamp then comes from the code under test
	// rather than from the test, and it has to be taken at the pass's START for this to hold.
	r.clk.step.Store(int64(r.s.scan / 4))
	r.s.pass(context.Background())
	assert.Equal(r.s.scan/2, r.s.untilNextPass(),
		"what a pass costs comes out of the interval, not on top of it")

	// A pass slower than the whole interval must not turn the loop into back-to-back readings against a
	// database that is evidently already struggling.
	r.clk.step.Store(0)
	r.clk.advance(r.s.scan)
	assert.Equal(r.s.scan/4, r.s.untilNextPass(), "and never below the floor")
}

// TestPeers_NewRejectsAnUnusableIdentity pins that the identity is a constructor argument rather than a
// setter: engine_id is the registry's PRIMARY KEY, so an unset one is not a harmless default - every
// unconfigured replica in a fleet would collide on it and fight over a single row.
func TestPeers_NewRejectsAnUnusableIdentity(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t)

	_, err := New(0, 1, r.db)
	assert.Error(err)
	_, err = New(-1, 1, r.db)
	assert.Error(err)
	_, err = New(selfID, 0, r.db)
	assert.Error(err)
	_, err = New(selfID, 1, nil)
	assert.Error(err)
}

// TestPeers_ConstantsHoldTheirRelationships pins the couplings between the cadences and the windows, which
// are what make each of them safe. They are separate constants precisely so they can be tuned independently,
// and these are the bounds that must survive any such tuning.
func TestPeers_ConstantsHoldTheirRelationships(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// The beat has to sit well inside the tightest window a reader applies, or a healthy replica flickers in
	// and out of its own fleet.
	assert.True(beatInterval*5 <= dispatchWindow, "five beats must fit in the dispatch window")
	// The pool divisor's window is the generous one and the work divisor's the tight one, because the two
	// errors point in opposite directions.
	assert.True(dispatchWindow < freshWindow, "the two windows must not converge")
	// Nothing is deleted until well past the point it stopped counting, and then only after a long healthy run.
	assert.True(stragglerAge > freshWindow)
	assert.True(pruneHealthyFor > stragglerAge)
	// Detection is the read's job, and reading more often than beating is the whole point of splitting them.
	assert.True(scanInterval < beatInterval)
}

// TestPeers_SeamsDefaultInert pins that a Sonar built and never told otherwise consults nothing, so a
// production one pays a bool read per site and neither fault can fire.
func TestPeers_SeamsDefaultInert(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)

	assert.False(r.s.faulted(FaultReadErr))
	assert.False(r.s.faulted(FaultBeatErr))
	r.s.SetSeams(nil) // nil restores the inert default rather than nil-panicking
	assert.False(r.s.faulted(FaultReadErr))

	r.s.pass(ctx)
	assert.NoError(r.s.lastErr)
	assert.Equal(1, len(r.ids(t)), "an unwired Sonar reads and writes normally")
}

// TestPeers_ReadFaultDrivesTheBlindPolicy pins the seam against the policy it exists to reach: a reading
// that fails publishes nothing, and once the readings have stopped for longer than the grace the partition
// switches off. Every one of those is this package's decision, and none is reachable from outside without
// a reading that fails.
func TestPeers_ReadFaultDrivesTheBlindPolicy(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	sm := seamster.New(true)
	r.s.SetSeams(sm)

	// A healthy fleet of three, two of them dispatching alongside us.
	r.addPeer(t, 10, time.Second, time.Second)
	r.addPeer(t, 20, time.Second, 10*time.Minute) // alive, but claims no work
	assert.NoError(r.s.register(ctx))
	var turns atomic.Uint64
	var busy, idle atomic.Bool
	turns.Store(1)
	r.s.SetEvidence(evidence(&turns, &busy, &idle))
	r.s.pass(ctx)
	assert.Equal(3, r.s.Replicas())
	_, _, ok := r.s.Partition()
	assert.True(ok)

	// Now the readings fail. Nothing is published, so the fleet is held rather than collapsing - which is
	// the direction that matters, since under-counting over-sizes every pool derived from it.
	sm.InjectN(1000, FaultReadErr)
	for range 5 {
		r.clk.advance(r.s.scan)
		r.s.pass(ctx)
	}
	assert.Error(r.s.lastErr, "the reading failed")
	assert.Equal(3, r.s.Replicas(), "a read that did not happen is not an observation that anybody left")
	_, _, ok = r.s.Partition()
	assert.False(ok, "but the pair can no longer be justified, so selection stops being partitioned")

	// Reading resumes while the fleet has genuinely shrunk. The first reading back must NOT be believed:
	// this is the correlated stall, where every row looks stale at once and every replica would otherwise
	// size for a fleet of one against a database that is already sick.
	sm.Withdraw(FaultReadErr)
	assert.NoError(r.s.register(ctx))
	_, err := r.db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id IN (10, 20)")
	assert.NoError(err)
	r.clk.advance(r.s.scan)
	r.s.pass(ctx)
	assert.NoError(r.s.lastErr)
	assert.Equal(3, r.s.Replicas(), "the reading that ended the blind spell cannot shrink the fleet")

	// The next one can, and does.
	r.clk.advance(r.s.scan)
	r.s.pass(ctx)
	assert.Equal(1, r.s.Replicas(), "confirmed on the next reading")
}

// TestPeers_BeatFaultStopsProvingLiveness pins the other seam. The process goes on running and reading
// perfectly well; it simply stops refreshing its own row, which is exactly how a replica that has lost the
// ability to prove it is alive appears to its peers - and the only way a test can occupy that view without
// killing a process.
func TestPeers_BeatFaultStopsProvingLiveness(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	sm := seamster.New(true)
	r.s.SetSeams(sm)
	assert.NoError(r.s.register(ctx))

	var turns atomic.Uint64
	var busy, idle atomic.Bool
	turns.Store(1)
	r.s.SetEvidence(evidence(&turns, &busy, &idle))
	r.s.pass(ctx)
	assert.True(r.row(t, selfID).dispatchAgeMs < staleAge, "a healthy beat proves both facts")

	// Age the row, then beat with the fault armed: the row must stay exactly as stale as we left it.
	sm.InjectN(1000, FaultBeatErr)
	_, err := r.db.ExecContext(ctx,
		"UPDATE dwarf_peers SET seen_at=DATE_ADD_MILLIS(NOW_UTC(), ?), dispatched_at=DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" WHERE engine_id=?", -60000, -60000, selfID)
	assert.NoError(err)
	turns.Store(2)
	r.clk.advance(r.s.beat)
	r.s.pass(ctx)
	assert.True(r.row(t, selfID).seenAgeMs > 50000, "the beat wrote nothing, so the row went on ageing")

	// The reading still works throughout - this replica is blind to nothing, it is simply invisible.
	assert.NoError(r.s.lastErr)

	// Its own count still includes itself, because it demonstrably exists whatever its row says. That is the
	// asymmetry: a replica never argues itself out of the pool divisor.
	assert.Equal(1, r.s.Replicas())

	// And it must not RESURRECT itself either. A peer eventually prunes the row this replica stopped
	// refreshing; the repair path would then re-create it with a fresh timestamp, so a replica that cannot
	// prove its liveness would prove it anyway through the other write. The fault covers both writes for
	// exactly this reason.
	_, err = r.db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=?", selfID)
	assert.NoError(err)
	r.clk.advance(r.s.scan)
	r.s.pass(ctx) // observes itself absent and tries to repair
	assert.Len(r.ids(t), 0, "a replica that cannot prove its liveness must not re-register either")

	// Lift the fault and the repair works again, which is what proves the gate was the fault and not a
	// broken repair path.
	sm.Withdraw(FaultBeatErr)
	r.clk.advance(r.s.scan)
	r.s.pass(ctx)
	assert.Equal([]int64{selfID}, r.ids(t), "and repairs itself the moment it can write again")
}

// TestPeers_FaultsScopeByShard pins the per-shard consult. Blinding one shard must leave every other shard
// reading, which is the property this package's whole per-shard shape exists to provide - and it would be
// untestable if the seam were fleet-wide only.
func TestPeers_FaultsScopeByShard(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t) // shard 1
	sm := seamster.New(true)
	r.s.SetSeams(sm)

	sm.InjectN(1000, FaultReadErr, "2")
	assert.False(r.s.faulted(FaultReadErr), "a fault scoped to another shard leaves this one reading")

	sm.Withdraw(FaultReadErr, "2")
	sm.InjectN(1000, FaultReadErr, "1")
	assert.True(r.s.faulted(FaultReadErr), "scoped to this shard, it fires")

	sm.Withdraw(FaultReadErr, "1")
	sm.InjectN(1000, FaultReadErr)
	assert.True(r.s.faulted(FaultReadErr), "and unscoped means every shard")
}

// TestPeers_LongTurnKeepsProvingService pins the case the busy term exists for, and it is the mirror of
// TestPeers_BeatFaultStopsProvingLiveness: a replica part-way through ONE very long turn must go on proving
// it serves the shard, beat after beat, for as long as the turn lasts.
//
// Without it a scan that outruns the dispatch window would drop a perfectly healthy replica out of the
// divisor - and on any dialect without the run-condition early-stop a deep-backlog scan runs for tens of
// seconds, so in a loaded fleet it would drop EVERY replica at once, disabling partitioning exactly when
// overlapping selection costs the most.
//
// The turn count never moves here, which is the point: for the whole span, the busy term is the only thing
// carrying the evidence.
func TestPeers_LongTurnKeepsProvingService(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	r := newRig(t)
	assert.NoError(r.s.register(ctx))

	var turns atomic.Uint64
	var busy, idle atomic.Bool
	turns.Store(7) // one turn completed long ago, and none since
	r.s.SetEvidence(evidence(&turns, &busy, &idle))
	r.s.pass(ctx)

	// The turn now in flight has outrun its period, so the dispatcher reports busy for its whole duration.
	// With the turn count parked where the last beat published it, that term is the ONLY thing left saying
	// this replica serves the shard - assert it directly, so this test cannot pass on the counter instead.
	assert.Equal(uint64(7), r.s.lastTurns, "the last beat published this count, so it is no longer news")
	_, dispatched := r.s.dispatchEvidence()
	assert.False(dispatched, "a parked counter alone proves nothing")
	busy.Store(true)
	_, dispatched = r.s.dispatchEvidence()
	assert.True(dispatched, "a turn that has outrun its period does")
	const rounds = 10
	stamps := 0
	for range rounds {
		r.clk.advance(r.s.beat) // a beat comes due
		// The row's age is measured by the DATABASE clock, which the fake one does not move, so let real time
		// pass before reading it. Without this the before/after ages tie at millisecond precision and a stamp
		// is invisible - the row is being refreshed either way, but the test cannot see it.
		time.Sleep(15 * time.Millisecond)
		before := r.row(t, selfID).dispatchAgeMs
		r.s.pass(ctx)
		if r.row(t, selfID).dispatchAgeMs < before {
			stamps++ // the age went DOWN, so this beat re-stamped it
		}
		assert.Equal(uint64(7), turns.Load(), "no turn ever completes during the span")
	}
	assert.Equal(rounds, stamps, "every beat across a long turn re-stamps the dispatch timestamp")
	assert.True(r.row(t, selfID).dispatchAgeMs < 100,
		"so the row never ages toward a window, however long the turn runs (age %.0fms)",
		r.row(t, selfID).dispatchAgeMs)
}
