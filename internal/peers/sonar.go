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

// Package peers reports the fleet sharing one shard's database, from that shard's replica registry.
//
// One Sonar works one shard: it owns this replica's registry row there - creating it, refreshing it,
// deleting it - reads the whole registry on its own clock, and publishes what the reading implies. An owner
// with N shards runs N of them, and nothing here spans shards: every timestamp in the registry belongs to
// the shard that stamped it, so a fleet view assembled from two shards would be comparing two clocks.
//
// A Sonar is a CONSUMER of its database. The handle arrives already open and is closed by whoever opened
// it, so there is no Open, no Close, and no say over pool sizes.
//
// # Driving one
//
//	s, err := peers.New(engineID, shard, db)
//	s.SetEvidence(dispatcher.Liveness) // optional; without it this replica never claims to dispatch
//	s.Join(ctx)                        // announce, wait for peers to notice, then read
//	go s.Run(ctx)                      // ... and keep reading until ctx ends
//	<-runReturned
//	s.Leave(liveCtx)                   // with a context that still works
//
// Join and Run must be driven by a SINGLE goroutine and must not overlap. Every getter is safe to call from
// any goroutine at any time.
//
// Join BLOCKS for a fraction of a second by design - see its own doc. It returns with every getter seeded,
// so an owner that sizes anything from this Sonar can do so immediately afterward and never against an
// unknown fleet.
//
// Leave needs a live context, so the owner calls it after Run has returned rather than relying on Run to
// clean up on the way out. Run having returned is also what makes the delete stick: no beat can follow it,
// and a beat never creates a row.
//
// # What it publishes
//
// Replicas is how many replicas hold connections to this shard - the divisor for the shard's connection
// pool. Partition is the (replicas, ordinal) pair that splits candidate selection across the replicas that
// actually serve this shard, in the shape a dispatcher consumes: ok=false means "select everything".
// BlindFor is how long it has been since the registry was last read successfully, which an owner should
// surface: a Sonar that cannot read holds every one of its published values frozen.
//
// A failed read publishes NOTHING - every getter keeps reporting the last good reading - because a read
// that did not happen is not evidence that anybody left.
package peers

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
)

// The two faults an owner may arm - see SetSeams. Exported so the owning application's catalogue aliases
// them rather than re-spelling the strings.
const (
	// FaultReadErr makes the registry read fail without touching the database. It is how a test reaches
	// every consequence of being BLIND: the counts hold, a fall is withheld across the gap that follows,
	// the partition switches off, and the prune's healthy run starts over.
	FaultReadErr = "peerReadErr"
	// FaultBeatErr makes every write that would PROVE this replica's liveness fail without touching the
	// database - the beat, and the registration repair that would otherwise re-create a row a peer has
	// pruned. Its row therefore stops being refreshed while the process goes on running, which is what a
	// replica that has lost the ability to prove itself looks like to its PEERS, and the only way to reach
	// that view without killing a process.
	//
	// It deliberately does NOT gate Leave: a test still has to be able to tear its fleet down.
	FaultBeatErr = "peerBeatErr"
)

// The cadences and windows. Only the read cadence is settable (SetCadence), because only it prices
// something the owner can see; the rest are this package's policy, derived from the asymmetry below rather
// than from anything an owner knows better. Tests in this package assign the fields directly.
//
// The two windows are deliberately far apart, and it is not a cost decision. Over-counting the replicas
// over-sizes pools and can collapse a database, so the fresh window is generous. Over-counting the
// DISPATCHERS hands a residue class of step ids to a replica that never selects them, and work in that
// class is then run by nobody, so the dispatch window is tight. The two errors point in opposite
// directions, so THE TWO WINDOWS MUST NOT BE DERIVED FROM ONE NUMBER.
// Cadence is how often a Sonar reads the registry, and so how often every value it publishes can change.
//
// An owner that polls those values should tick at this rate: slower discards detection it has already paid
// for, faster reads the same answer twice. It is also what Join blocks for, twice over.
const Cadence = 250 * time.Millisecond

const (
	// scanInterval is the read cadence a Sonar is built with. It is Cadence, named separately only because
	// this is the field's default and Cadence is the owner-facing name for the same number.
	//
	// It is the more frequent of the two round trips, so it carries the cost argument the beat is held to,
	// four times over. THREE loads outvote that, and lengthening it trades against all three at once:
	//
	//   - Join. Every replica in the fleet waits TWICE this on the way up, before it may open a connection,
	//     so a second here is two seconds added to every start and to every step of a rolling deploy.
	//   - Convergence. There is no nudge entry point - deliberately, since polling this often converges
	//     faster than a fleet-membership broadcast would, which is what lets an owner do without one. Slow
	//     this down and that stops being true.
	//   - Detection, which is the load it is least sized by: both consumers respond to deployment events.
	//
	// Read the two constants together: this one is sized by startup latency and convergence, the beat by the
	// windows.
	scanInterval = Cadence
	// beatInterval is how often this replica's own row is refreshed. It only has to sit well inside the
	// tightest window a reader applies (five beats fit in the dispatch window); beating faster buys no
	// detection at all and costs a round trip on the pool the owner's dispatcher is competing for.
	beatInterval = time.Second
	// freshWindow is how long a row counts toward Replicas - some 40 beats, so a live replica has to go badly
	// wrong to be declared gone. The beat is itself a pooled write, so under heavy load it can starve for a
	// connection, which is exactly when growing pools would be most harmful.
	freshWindow = 40 * time.Second
	// dispatchWindow is how recently a replica must have proved it serves this shard to own a residue class -
	// five beats. Safe to keep this tight only because the evidence keeps arriving while a turn is still in
	// flight; without that, one slow scan would drop every healthy replica in a loaded fleet out of the
	// divisor at once, disabling partitioning precisely when overlapping selection costs the most.
	dispatchWindow = 5 * time.Second
	// stragglerAge is when a row becomes eligible for the hygiene delete - well past the point it stopped
	// counting toward anything.
	stragglerAge = 80 * time.Second
	// pruneHealthyFor is how long the registry must have been continuously readable before anything is
	// deleted from it. See prune: the most conservative of the three thresholds, because deleting is the only
	// irreversible thing here.
	pruneHealthyFor = 5 * time.Minute
	// maxPruneBatch bounds one hygiene delete. A crash-looping replica that mints a fresh identity per
	// restart leaves one corpse per restart, so the list is bounded by the crash rate rather than by anything
	// here; deleting a bounded slice per pass keeps the statement one shape and its parameter count trivially
	// inside every driver's ceiling.
	maxPruneBatch = 64
)

// EvidenceFunc reports whether the owner's dispatcher for this shard is turning - see SetEvidence.
type EvidenceFunc func() (turns uint64, busy, idle bool)

// partition is the pair Partition reports: how many replicas serve this shard, and which of them this one
// is. Immutable once published, so a reader always holds a pair that was true together.
type partition struct {
	dispatchers int
	// ordinal is this replica's 0-based position among the dispatchers, or -1 when it is not among them.
	ordinal int
}

// Sonar owns this replica's row in one shard's peer registry and everything derived from reading it.
//
// Join and Run must be driven by a single goroutine. Every getter is safe to call from another one, at any
// time.
type Sonar struct {
	// engineID identifies this replica in the registry. Immutable: it is the registry's primary key, and a
	// replica that changed it mid-life would leave a ghost row behind.
	engineID int64
	shard    int
	db       *sequel.DB

	// Cadences and windows, seeded from the constants above. Immutable once anything is driven, which is what
	// lets the getters read them without synchronizing; a test assigns them between New and its first call.
	scan       time.Duration
	beat       time.Duration
	fresh      time.Duration
	dispatch   time.Duration
	straggler  time.Duration
	pruneAfter time.Duration

	evidence atomic.Pointer[EvidenceFunc]
	// passTurn admits each pass to the owner's database metering - see SetTurnFunc.
	passTurn atomic.Pointer[TurnFunc]
	logger   atomic.Pointer[slog.Logger]
	seams    atomic.Pointer[seamster.Seamster]

	// Published state: written only by the driving goroutine, read by the owner from anywhere.
	replicas atomic.Int32
	// part is the dispatcher count and this replica's ordinal in it, published as ONE value. They are two
	// halves of a single decision - an ordinal only means anything against the count it was derived from -
	// so a reader that caught one half of a fleet change would partition on a pair that never existed. As
	// two atomics, no store order avoids that: on a shrink (3,2)->(2,1) an ordinal-first reader sees (3,1),
	// a class this replica never owned, and on a grow a count-first reader sees the mirror of it. Both are
	// advisory - the claim CAS arbitrates and the window is one getter call wide - but the fix is cheaper
	// than the argument, and a pointer swap once per reading costs nothing.
	part atomic.Pointer[partition]
	// lastGood is the wall time of the last successful read, in nanoseconds. Seeded at construction so a
	// fresh Sonar reports no blindness and then accrues it until its first read lands.
	lastGood atomic.Int64

	// Driving-goroutine state, deliberately unsynchronized - nothing outside Join and Run touches it, and
	// those two are contractually one goroutine.
	//
	// lastTurns is the dispatcher's turn count as of the last beat that PUBLISHED it. Only a beat advances
	// it, so a pass that merely looks at the evidence cannot swallow a turn.
	lastTurns      uint64
	beatDispatched bool
	nextBeat       time.Time
	// healthySince is when the current unbroken run of successful reads began; zero before the first one.
	// Reset whenever a sustained gap interrupts the run, which is what stops blind time from counting toward
	// the prune's patience.
	healthySince time.Time
	blindLogged  bool
	// lastPass is when the last pass STARTED, which is what the next one is paced from - see untilNextPass.
	lastPass time.Time
	// lastErr is the most recent read's error, kept so Join can report synchronously what the loop only logs.
	lastErr error

	// now is the clock, indirected for tests only.
	now func() time.Time
}

// New returns a Sonar for one shard over an already-open database handle.
//
// engineID leads because it is required rather than optional: it is the registry's PRIMARY KEY, so an unset
// one is not a harmless default - every unconfigured replica in the fleet would collide on id 0 and fight
// over a single row. A setter would make that state reachable; a constructor argument does not.
func New(engineID int64, shard int, db *sequel.DB) (*Sonar, error) {
	if engineID <= 0 {
		return nil, errors.New("engine id must be positive, got %d", engineID)
	}
	if shard < 1 {
		return nil, errors.New("shard must be positive, got %d", shard)
	}
	if db == nil {
		return nil, errors.New("db is required")
	}
	s := &Sonar{
		engineID: engineID, shard: shard, db: db, now: time.Now,
		scan: scanInterval, beat: beatInterval, fresh: freshWindow,
		dispatch: dispatchWindow, straggler: stragglerAge, pruneAfter: pruneHealthyFor,
	}
	s.SetLogger(nil)
	s.SetSeams(nil)
	s.lastGood.Store(s.now().UnixNano())
	s.replicas.Store(1)
	s.part.Store(&partition{ordinal: -1})
	return s, nil
}

// SetEvidence supplies the one fact a Sonar cannot observe for itself: whether the owner's dispatcher for
// this shard is turning. It decides whether a beat also stamps this replica as SERVING the shard, which is
// what earns it a residue class of step ids.
//
// The three returns are read together and interpreted as: turns advanced since the last beat, or a turn has
// been running long enough to count on its own, and the dispatcher is not idling. The second term is for a
// scan that legitimately runs far longer than the dispatch window on a deep backlog - but it must mean a
// LONG turn rather than merely one in flight, because a turn that fails instantly is briefly in flight too,
// and a reader sampling on a cadence catches that often enough to keep a dispatcher that serves nothing
// looking alive for good.
//
// It is a PURE READ and may be called any number of times - the "since the last beat" part is this
// package's business, held against the turn count it last published. A consuming getter would make any
// second caller (a metric, a test) silently clear the evidence and leave a healthy dispatcher looking
// stalled.
//
// Without it a Sonar keeps its row alive and never claims to dispatch, which is exactly right for a replica
// that holds connections but claims no work: it counts toward Replicas and toward nobody's partition.
// Absence of evidence must never read as evidence.
// TurnFunc admits a pass to whatever the owner meters its database with, returning the context the pass's
// calls run on and the release to call when they are done - see SetTurnFunc.
type TurnFunc func(context.Context) (context.Context, func())

// SetTurnFunc supplies an admission step taken at the START of each pass and released at its end, so an
// owner that orders access to its database can order this too, without this package knowing what the
// ordering is. Nil (the default) admits every pass immediately.
//
// It returns a CONTEXT AND A RELEASE rather than just a context, because a derivation alone would be
// inert: the owner has to be able to hold something for the duration of the pass and hand it back, and a
// hook that only decorated the context would look wired while metering nothing.
//
// Whatever the owner puts there, IT MUST NOT BE ABLE TO STALL A PASS. A Sonar that cannot beat reads as
// blind, and a blind Sonar makes its replica stop partitioning and hold its last fleet count - so anything
// that throttled this in proportion to load would let load reshape the fleet. Ordering a pass ahead of
// ordinary work is fine; making it queue behind ordinary work is not.
//
// Once per PASS rather than once per statement, because a pass is one unit of work: the beat and the read
// that follows it describe the same moment, and re-deriving between them would present them as two
// arrivals.
func (s *Sonar) SetTurnFunc(fn TurnFunc) {
	if fn == nil {
		s.passTurn.Store(nil)
		return
	}
	s.passTurn.Store(&fn)
}

func (s *Sonar) SetEvidence(fn EvidenceFunc) {
	if fn == nil {
		s.evidence.Store(nil)
		return
	}
	s.evidence.Store(&fn)
}

// SetCadence overrides how often this Sonar reads the registry, for an owner whose whole fleet is
// ephemeral - a test suite standing engines up and tearing them down, where the default's two-cadence Join
// would dominate every startup. Zero or negative restores Cadence.
//
// It is the ONE knob here, and every other number is deliberately not one: the windows and the beat are
// this package's policy, derived from which errors are safe in which direction rather than from anything
// an owner knows better. This one is different because it prices the owner's startup latency, which only
// the owner can see.
//
// Shortening it shortens what derives from it - Join's wait and the blindness grace - and nothing else, so
// a short-cadence Sonar still applies the same windows to the same rows. Not safe to call once anything is
// being driven; set it before Join.
func (s *Sonar) SetCadence(d time.Duration) {
	if d <= 0 {
		d = scanInterval
	}
	s.scan = d
}

// SetSeams supplies the OWNER's fault-injection seams, so a test driving a whole engine can break this
// Sonar's database calls without breaking its database. Nil restores an inert one, which is also the
// default - a Sonar built and never told otherwise consults nothing, and a Seamster built disabled makes
// every consult a bool read.
//
// The seams are the owner's, not this package's, for the same reason the logger is: one catalogue of fault
// names per application, armed in one place, however many modules consult it.
//
// EXACTLY TWO FAULTS, both at an I/O boundary, and that is the whole rule for adding a third. A seam inside
// pure logic here would be a signal that a dependency should have been injected instead - and every
// time-dependent decision already has one, the clock - so it would buy nothing. These two reach what
// injection cannot: a database that answers, but wrongly for this replica. FaultReadErr makes the reading
// fail (blindness, and everything downstream of it); FaultBeatErr stops this replica proving its own
// liveness while it goes on running, which is the only way for a test to occupy a PEER's point of view of a
// replica that has effectively died.
//
// Both are consulted unscoped OR scoped by shard, so a test can blind one shard and leave the rest reading
// - which is the per-shard property this package exists to provide, and it would be untestable otherwise.
func (s *Sonar) SetSeams(sm *seamster.Seamster) {
	if sm == nil {
		sm = seamster.New(false)
	}
	s.seams.Store(sm)
}

// seamsJoin builds a targeted seam name: a base name, then the entity it targets, joined with ":". A consult
// site and the test that arms it both call it, so neither can spell the join the other does not. A targeted
// name and the bare one are DIFFERENT seams, so a site wanting both fires both.
func seamsJoin(parts ...string) string {
	return strings.Join(parts, ":")
}

// faulted reports whether a fault is armed for this Sonar, either fleet-wide or for this shard alone.
func (s *Sonar) faulted(name string) bool {
	sm := s.seams.Load()
	if !sm.Enabled() { // gates the assembled per-shard name in production
		return false
	}
	return sm.IsFault(name) || sm.IsFault(seamsJoin(name, strconv.Itoa(s.shard)))
}

// SetLogger sets the logger. Nil restores the discarding default.
func (s *Sonar) SetLogger(l *slog.Logger) {
	if l == nil {
		l = slog.New(slog.DiscardHandler)
	}
	s.logger.Store(l)
}

// Replicas is how many replicas hold connections to this shard, self included, never below one. It is the
// divisor for the shard's connection pool: the budget belongs to the shard's DATABASE, so N replicas each
// holding the whole budget would overshoot it N times over.
func (s *Sonar) Replicas() int { return max(1, int(s.replicas.Load())) }

// Partition is the (replicas, ordinal) pair that splits candidate selection across the replicas serving
// this shard, or ok=false when selection must not be partitioned at all.
//
// It reports the DISPATCHER count, not Replicas: the pool divisor counts every replica holding connections,
// while this divides work, and handing a residue class to a replica that claims none would leave those steps
// selected by nobody.
//
// ok=false whenever the answer cannot be justified - a solo dispatcher, this replica absent from the roster,
// an ordinal outside the divisor, or a Sonar gone blind. Every one of those fails OPEN: not partitioning
// means replicas select overlapping candidates, which costs a lost claim round trip, while partitioning on a
// stale pair means a class of steps nobody selects.
//
// Blindness is evaluated HERE rather than published by the loop, so a shard that stops answering stops being
// partitioned within a round trip rather than within a cadence.
func (s *Sonar) Partition() (replicas, ordinal int, ok bool) {
	p := s.part.Load()
	if s.blind(s.now()) || p.dispatchers < 2 || p.ordinal < 0 || p.ordinal >= p.dispatchers {
		return p.dispatchers, 0, false
	}
	return p.dispatchers, p.ordinal, true
}

// BlindFor is how long it has been since the registry was last read successfully. Zero on a healthy Sonar.
func (s *Sonar) BlindFor() time.Duration {
	return max(0, s.now().Sub(time.Unix(0, s.lastGood.Load())))
}

// Join announces this replica and returns with every getter seeded. It BLOCKS for two scan intervals, and
// that wait is the point rather than an implementation detail: it is what keeps a join from exceeding the
// shard's connection budget even momentarily.
//
// A joining replica sizes its own pool for the fleet it is joining, but its peers go on holding pools sized
// for the fleet WITHOUT it until they read the registry again, so consuming immediately puts the shard's
// server over budget by roughly one replica's share until they catch up. Announcing first inverts that:
// peers shrink, then the newcomer grows. What it buys is peers having STOPPED ACQUIRING beyond their new cap
// - lowering a pool's limit closes nothing, so any surplus drains as connections are returned.
//
// TWO intervals, not one: a peer's read may have begun just before this replica's row was committed, so
// that read proves nothing and only the one after it must see the row. It also gives a simultaneously
// starting fleet's rows time to land, so the reading that follows is of a settled roster rather than a
// partial one - and a partial one would UNDER-count, which over-sizes pools.
//
// The whole sequence runs even if part of it fails, so a transient error still leaves the getters as well
// seeded as they can be; the first error is returned for the owner to log.
func (s *Sonar) Join(ctx context.Context) error {
	err := s.register(ctx)
	if err != nil {
		s.logger.Load().ErrorContext(ctx, "Registering peer", "shard", s.shard, "error", err)
	}
	s.sleep(ctx, 2*s.scan)
	s.pass(ctx)
	if err == nil {
		err = s.lastErr
	}
	return errors.Trace(err)
}

// Run beats, reads and publishes until ctx ends. It blocks; the owner puts it in a goroutine.
//
// A cancelled context is the only stop signal it needs, and no second one is wanted: the read is a pure read
// and the beat is one idempotent UPDATE, so abandoning either mid-flight commits nothing and strands
// nothing.
func (s *Sonar) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if !s.sleep(ctx, s.untilNextPass()) {
			return
		}
		s.pass(ctx)
	}
}

// untilNextPass is what remains of the scan interval since the last pass STARTED.
//
// Measuring start to start is what makes the period actually be the scan interval, and everything derived
// from that interval depends on it. Sleeping the interval AFTER each pass instead makes the real period
// `scan + pass`, which leaves the blindness grace - two intervals - with only one pass of margin. A pass is
// one or two round trips on the pool the owner's dispatcher is competing for, so under contention it can
// approach the interval, and at that point EVERY reading looks like it ended a gap: the replica count can
// never fall, the prune's healthy run never accumulates, and partitioning switches off. All three fail in
// the safe direction, but all three fail silently and at once. A slow pass now pays for itself instead of
// stacking on top of the interval.
//
// The floor keeps a pass slower than the whole interval from turning the loop into back-to-back readings
// against a database that is evidently already struggling.
//
// Zero before the first pass, so a Sonar that was never joined reads immediately - and, since Join ends
// with a pass, so that Run does not repeat it microseconds later.
func (s *Sonar) untilNextPass() time.Duration {
	if s.lastPass.IsZero() {
		return 0
	}
	return max(s.scan-s.now().Sub(s.lastPass), s.scan/4)
}

// Leave deletes this replica's row, so peers recount without waiting out the freshness window.
//
// Call it with a LIVE context after Run has returned. Run cannot do it on the way out - its context is
// already cancelled by then - and Run having returned is what makes the delete stick, since no beat can
// follow it and a beat never creates a row.
func (s *Sonar) Leave(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM dwarf_peers WHERE engine_id=?", s.engineID)
	return errors.Trace(err)
}

// register creates this replica's row: an UPDATE by id, falling back to an INSERT when no row matched. Two
// statements rather than a per-dialect upsert, so it stays dialect-agnostic.
//
// It deliberately does NOT stamp the dispatch timestamp. That column is evidence that this replica served
// the shard, and registering is pure intent, so a replica starts out counted toward the pool divisor and
// toward nobody's partition, and earns the rest on its first turn.
//
// The RowsAffected test is safe on MySQL, which counts CHANGED rather than matched rows, because both
// callers are clear of the one case that would trip it - an existing row whose seen_at already holds this
// millisecond. Join runs before anything has beaten, and the repair path has just OBSERVED the row absent.
// This replica is the only writer of its own row, so that observation cannot be raced.
func (s *Sonar) register(ctx context.Context) error {
	// FaultBeatErr covers this as well as the beat, and the reason is the repair below: a Sonar that observes
	// itself absent re-registers, so gating only the beat would let a replica which cannot prove its liveness
	// resurrect its own row - with a FRESH seen_at - the moment a peer pruned it. The fault's premise is that
	// no write of this replica's proves anything, and there are two such writes.
	if s.faulted(FaultBeatErr) {
		return nil
	}
	res, err := s.db.ExecContext(ctx,
		"UPDATE dwarf_peers SET seen_at=NOW_UTC() WHERE engine_id=?", s.engineID)
	if err != nil {
		return errors.Trace(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return errors.Trace(err)
	}
	if n > 0 {
		return nil
	}
	_, err = s.db.ExecContext(ctx,
		"INSERT INTO dwarf_peers (engine_id, seen_at) VALUES (?, NOW_UTC())", s.engineID)
	return errors.Trace(err)
}

// pass is one cycle: beat if a beat is due or the dispatch evidence has changed, then read and publish.
//
// The beat rides the READ's cadence when the evidence bit flips, which is what keeps a starting replica out
// of its own fleet's dispatcher count for at most a scan interval rather than a whole beat interval - its
// first turn lands milliseconds after Run starts, long before the next beat would. The same early publish
// applies when a replica STOPS serving, which is the direction that strands work.
func (s *Sonar) pass(ctx context.Context) {
	// Held for the whole pass: the beat and the read that follows it are one unit of work, and the read
	// hands back rows that keep a connection until they are scanned.
	if fn := s.passTurn.Load(); fn != nil {
		var release func()
		ctx, release = (*fn)(ctx)
		defer release()
	}
	now := s.now()
	s.lastPass = now
	turns, dispatched := s.dispatchEvidence()
	if !now.Before(s.nextBeat) || dispatched != s.beatDispatched {
		s.publishBeat(ctx, dispatched)
		// Only a beat may advance the published turn count: a pass that looked at the evidence without
		// publishing it must leave it for the next beat to carry.
		s.lastTurns = turns
		s.beatDispatched = dispatched
		s.nextBeat = now.Add(s.beat)
	}
	rows, err := s.read(ctx)
	s.observe(ctx, rows, err)
}

// dispatchEvidence reads the injected evidence and reduces it to "this replica is serving the shard".
// Non-consuming: the turn count is returned for a beat to publish, never cleared here.
//
// The idle term stays explicit even though an idling dispatcher stops advancing its turn count on its own.
// Without it, a turn that completed just before the dispatcher went idle would make the next beat claim
// service for a whole window - and over-counting dispatchers is the direction that strands work.
func (s *Sonar) dispatchEvidence() (turns uint64, dispatched bool) {
	fn := s.evidence.Load()
	if fn == nil {
		return 0, false
	}
	turns, busy, idle := (*fn)()
	return turns, !idle && (turns != s.lastTurns || busy)
}

// publishBeat refreshes this replica's row. Timestamps come from the database clock (NOW_UTC()), never a
// bound Go time, so every freshness comparison on this shard runs on one clock.
//
// It is an UPDATE and NOTHING ELSE - it never creates the row. That is what lets Leave be final: a straggler
// beat matches nothing instead of resurrecting a replica that has gone.
//
// A failed write is not retried. Retrying a broken registry write turns a database blip into a write storm,
// and the next beat is one interval away regardless.
func (s *Sonar) publishBeat(ctx context.Context, dispatched bool) {
	// FaultBeatErr drops the write, so this replica stops refreshing its own row while everything else about
	// it goes on working. That is what a replica which can no longer prove its liveness looks like from a
	// PEER's side, and a test can occupy that side no other way short of killing a process.
	if s.faulted(FaultBeatErr) {
		return
	}
	// The dispatch assignment is composed into the statement rather than bound through a CASE: a conditional
	// assignment is two plain statement shapes here, where CASE WHEN ? would lean on how each driver binds a
	// boolean into an expression.
	set := "seen_at=NOW_UTC()"
	if dispatched {
		set += ", dispatched_at=NOW_UTC()"
	}
	_, err := s.db.ExecContext(ctx,
		"UPDATE dwarf_peers SET "+set+" WHERE engine_id=?", s.engineID)
	if err != nil && ctx.Err() == nil {
		s.logger.Load().ErrorContext(ctx, "Peer heartbeat", "shard", s.shard, "error", err)
	}
}

// read returns every row in the shard's registry, engine_id-ascending, with both ages computed by the
// database.
//
// It is deliberately UNFILTERED. A freshness predicate in SQL would save nothing - the table holds one row
// per live replica plus a few corpses, has no secondary index by design, and is scanned whole either way -
// while costing three things worth having: every window becomes a value this package can change without
// touching SQL, the hygiene delete gets its candidate list from the same reading everything else is derived
// from instead of a second query, and a row that is ABSENT becomes distinguishable from one that is merely
// stale.
//
// ORDER BY in SQL rather than in Go so the ordering is the database's, identical for every replica reading
// the same rows - which is what lets each derive a distinct ordinal with no coordination.
//
// The ages are float64 because DATE_DIFF_MILLIS is fractional on SQLite, where scanning it into an int64
// fails outright.
func (s *Sonar) read(ctx context.Context) ([]peer, error) {
	// FaultReadErr fails the reading without touching the database, which is the only way to reach what
	// being blind implies - the counts held, a fall withheld across the gap, the partition switched off,
	// the prune's patience restarted. All four are policy this package owns and none is reachable from
	// outside without a reading that fails.
	if s.faulted(FaultReadErr) {
		return nil, errors.New("injected fault: " + FaultReadErr)
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT engine_id, DATE_DIFF_MILLIS(NOW_UTC(), seen_at) AS seen_age_ms,"+
			" DATE_DIFF_MILLIS(NOW_UTC(), dispatched_at) AS dispatch_age_ms"+
			" FROM dwarf_peers ORDER BY engine_id")
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	var out []peer
	for rows.Next() {
		var p peer
		if err := rows.Scan(&p.engineID, &p.seenAgeMs, &p.dispatchAgeMs); err != nil {
			return nil, errors.Trace(err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	return out, nil
}

// observe turns one reading into published state, or holds everything if the reading failed.
//
// A FAILED READ PUBLISHES NOTHING, and that is the one rule the rest depends on: a read that did not happen
// is not an observation that anybody left. Holding the last good values keeps a database blip from
// collapsing the pool divisor - which would grow every pool, against a database that is evidently already
// unwell.
func (s *Sonar) observe(ctx context.Context, rows []peer, err error) {
	s.lastErr = err
	now := s.now()
	if err != nil {
		if !s.blindLogged && s.blind(now) {
			s.blindLogged = true
			s.logger.Load().ErrorContext(ctx, "Peer registry unreadable; holding last known fleet",
				"shard", s.shard, "blindFor", s.BlindFor().String(), "error", err)
		}
		return
	}
	// Whether this reading ENDS a gap has to be decided before it is recorded as the newest one.
	gapped := s.blind(now)
	if s.blindLogged {
		s.blindLogged = false
		s.logger.Load().InfoContext(ctx, "Peer registry readable again", "shard", s.shard)
	}
	s.lastGood.Store(now.UnixNano())
	if gapped || s.healthySince.IsZero() {
		s.healthySince = now
	}

	v := classify(rows, s.engineID, s.windows())
	// The dispatch pair is published as observed, with no hysteresis: under-counting only makes replicas
	// select overlapping candidates, so there is nothing here worth debouncing, and delaying a REMOVAL would
	// keep a residue class assigned to a replica that has stopped serving it.
	s.part.Store(&partition{dispatchers: v.dispatchers, ordinal: v.ordinal})

	// A FALL is not believed on the strength of the reading that ended a blind spell, while a RISE always is.
	// The asymmetry is the whole point: a rise shrinks pools, which is safe, whereas a fall grows every pool
	// derived from the count. The case that matters is not one peer dying but a CORRELATED stall - a spell of
	// database trouble stalls every replica's beat and every replica's read at once; it clears, and the first
	// reading afterward shows a registry in which every row is stale, so every replica would compute a tiny
	// count and grow to the full per-database budget simultaneously, against a database that is already sick.
	// Skipping one reading is enough, because a beat is far shorter than the read cadence: by the next one
	// every live peer has refreshed its row and the count is real again.
	if !gapped || v.replicas >= s.Replicas() {
		s.replicas.Store(int32(max(1, v.replicas)))
	}

	if !v.selfSeen {
		// Nothing else can fix this: the beat only UPDATEs, so a row that has gone missing is refreshed by
		// nobody and stays missing. Left alone, peers under-count the fleet and over-size their pools - the
		// direction that collapses a database - while this replica declines to partition and overlaps every
		// peer. Repaired promptly rather than on the hygiene cadence for exactly that reason.
		//
		// An EMPTY registry lands here too, and must: it is always wrong, since this process is in it by
		// definition, so reading it as "no peers" and stopping there would leave a whole fleet unregistered.
		s.logger.Load().WarnContext(ctx, "Peer registry row missing; re-registering", "shard", s.shard)
		if err := s.register(ctx); err != nil && ctx.Err() == nil {
			s.logger.Load().ErrorContext(ctx, "Re-registering peer", "shard", s.shard, "error", err)
		}
	}
	s.prune(ctx, v.dead)
}

// prune deletes rows that have been stale long enough to be corpses, by PRIMARY KEY, from the list the same
// reading produced.
//
// It waits for the registry to have been continuously readable for pruneHealthyFor, and that patience is the
// most conservative of the three thresholds on purpose. Every other decision here is reversible - a wrongly
// dropped peer comes back on the next reading - while a deleted row is refreshed by nobody afterward. The
// case it guards is a stall longer than the straggler age: it clears, every row in the table is stale at
// once including the pruner's own, and a now-anchored delete would empty the registry for the whole fleet.
// Waiting costs nothing at all, because the freshness windows already exclude these rows from every count;
// hygiene deferred is hygiene.
//
// Deleting by an explicit id list, rather than by a range predicate on the timestamp, is what keeps the
// decision in the hands of the reading it came from: the server never re-evaluates a clock this process did
// not observe, there are no gap locks to contend over, and a fleet-wide wipe is not expressible.
func (s *Sonar) prune(ctx context.Context, dead []int64) {
	if len(dead) == 0 || s.healthySince.IsZero() {
		return
	}
	if s.now().Sub(s.healthySince) < s.pruneAfter {
		return
	}
	if len(dead) > maxPruneBatch {
		dead = dead[:maxPruneBatch]
	}
	args := make([]any, len(dead))
	for i, id := range dead {
		args[i] = id
	}
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM dwarf_peers WHERE engine_id IN ("+strings.Repeat("?,", len(dead)-1)+"?)", args...)
	if err != nil && ctx.Err() == nil {
		s.logger.Load().ErrorContext(ctx, "Pruning peer registry", "shard", s.shard, "error", err)
	}
}

// windows snapshots the three thresholds in the milliseconds the database's ages are measured in.
func (s *Sonar) windows() windows {
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return windows{fresh: ms(s.fresh), dispatch: ms(s.dispatch), straggler: ms(s.straggler)}
}

// blind reports whether the last successful read is old enough that this Sonar's published view can no
// longer be justified.
//
// The grace is two scan intervals, derived rather than configured: one interval is a single missed reading,
// which a healthy Sonar meets on any transient error, while two means the readings have actually stopped. It
// is the same threshold that interrupts the prune's healthy run and that withholds a fall in the replica
// count, so a gap either counts against all three or against none.
func (s *Sonar) blind(now time.Time) bool {
	return now.Sub(time.Unix(0, s.lastGood.Load())) > 2*s.scan
}

// sleep waits d or until ctx is done, reporting false if the context ended.
func (s *Sonar) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
