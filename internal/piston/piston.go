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

// Package piston supplies step candidates from one shard. One piston works one shard, firing the same two
// cycles over and over against its own database on its own clock, with no barrier against its peers; an
// engine with N shards runs N of them.
//
// A piston is a CONSUMER of its database, never the owner: the handle is passed in already open and is
// closed by whoever opened it, so there is no Open, no Close, and no say over pool sizes. It owns the two
// supply loops, the two queries behind them, and its instruments; it borrows the planner and the candidate
// cache, both shared with every other piston on the replica.
//
// Run blocks and drives both loops until its context ends, each on its own goroutine and its own cadence:
//
//	tally  (paced by the pipeline) -> record -> repeat
//	supply (paced by the pipeline) -> record -> repeat
//
// Liveness reports whether the piston is turning, for an owner that publishes this replica's liveness
// somewhere the fleet can see it.
//
// SetIdle(true) skips both loops entirely, which is the await-only replica: it keeps holding connections,
// but claims no work and reports itself idle. Going idle withdraws the shard from the shared planner and
// empties its cache partition, so nothing is left claiming a band this piston no longer reports on.
//
// Every Set* is live: the owner may re-derive any of them while Run is in flight. SetIdle is the one that
// takes both loops to do properly - see its own doc.
package piston

import (
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/pipeline"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// refillBuckets are the explicit bucket boundaries for the refill-duration histogram, in SECONDS. The OTEL defaults are
// tuned for millisecond-valued instruments and would file every one of these samples in the first bucket.
//
// The LOW end is the load-bearing part, and it must not be raised. A warm same-zone band scan measures
// ~0.29ms, while the same query on the same data measures ~100ms once its Postgres statistics go stale
// and the plan flips to a sequential scan - the flip the phase label exists to expose. Boundaries that
// start at 0.0005 put the healthy case in the first bucket and hide exactly that. The tail stays open past
// 1s to separate a genuinely sick shard from a merely slow one.
var refillBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
}

// idlePoll is how often an idle piston re-checks whether it is still idle. It paces nothing else: an idle
// piston runs no cycle, so this is only the latency of a live SetIdle(false) resuming dispatch.
const idlePoll = time.Second

// defaultStealAfter is how many cycle periods a step outside this replica's residue class must have been
// DUE before an otherwise-idle replica selects it anyway - see stealGrace and SetStealAfter.
//
// Four is not a delicate number, and the measurements say why: a healthy fleet's oldest due step sits at
// 0-1s while a stalled owner's class ages to 23-41s, against a ~67ms derived period. Anything from ~2 to
// ~10 periods separates those cleanly. It is a small multiple because it only has to outlast the time a
// HEALTHY owner takes to reach its own work, which is a cycle or two by construction - the FILL ORDER, not
// this, is what keeps a replica with work of its own from taking anyone else's.
const defaultStealAfter = 4

// instruments is this piston's metric set, swapped atomically as a group so a recording cycle never sees
// half of one meter and half of another.
type instruments struct {
	queryDuration metric.Float64Histogram
	selected      metric.Int64Counter
	discarded     metric.Int64Counter
	stolen        metric.Int64Counter
}

// Piston runs one shard's two supply loops and its heartbeat.
//
// Run spawns a goroutine per loop and blocks until both return. Every Set* is safe to call from another
// one, at any time.
type Piston struct {
	shard int
	// shardAttr labels every instrument this piston records, built once because the shard is immutable
	// after New and the alternative is rebuilding it up to seven times per round trip.
	//
	// A STRING, not an Int, and the two must never be mixed. `shard` is emitted by the engine as well, and
	// an OTLP-native backend distinguishes attribute TYPES - so the same key at two types is two different
	// attributes there, which silently breaks any query grouping the piston's instruments against the
	// engine's (the refiller's cost against the turnstile's queue is exactly that join). Prometheus renders
	// both as the label string and hides the split, which is why it survived unnoticed.
	shardAttr attribute.KeyValue

	sh       *database.Shard
	db       *sequel.DB
	planner  *planner.Planner
	cache    *candidates.Cache
	tallier  *pipeline.Tallier
	supplier *pipeline.Supplier

	// Live configuration, each independently atomic. There is no grouped snapshot because nothing here is
	// coupled - reading idle and the partition a microsecond apart cannot produce an inconsistent pair.
	idle atomic.Bool
	// diags is what the piston reports through, and inst the instruments it made on diags' meter.
	diags *diagnostics.Diags
	inst  *instruments

	// stealAfter is how many cycle periods a foreign step must have been due before this replica takes it.
	// Zero disables stealing outright, which is what an owner sets when it wants the residue class enforced
	// strictly.
	stealAfter atomic.Int32
	// maxPriority is the worst band the scan counts - priorities run 1..maxPriority. See SetMaxPriority.
	maxPriority atomic.Int32
	// tallyTurns and supplyTurns count each loop's successful cycles, monotonically and independently -
	// neither loop ever writes the other's. Liveness reports neither: it reports turns, a generation that
	// advances only once BOTH have moved since its last advance (advanceTurn), because a turn has to mean this
	// piston can both LOOK at its shard and SERVE it, and the two failures are symmetric:
	//
	//   - every SCAN failing clears this shard from planning and selects nothing, while the supply loop
	//     turns happily on an empty plan;
	//   - every FETCH failing leaves the tally honest and the band claimed, while the partition takes zero
	//     candidates - the quiet shape the bind-count overflow produces.
	//
	// Either alone therefore reads a piston that serves nothing as fully alive, and it then holds a residue
	// class nobody else will select. The generation stalls when either loop does, which is the whole point.
	//
	// Not the MINIMUM of the two counts: the loops are paced independently, so one banks a lead over the other
	// while both are healthy, and once the leader fails every turn of the other moves the minimum again until the
	// lead runs out - a window that grows with uptime. Measured: 1 -> 11 over ten cycles of failing fetches.
	//
	// COUNTERS rather than a flag the reader clears: a consuming getter would make any second caller - a
	// metric, a test - silently swallow the evidence and leave a healthy piston reading as stalled. Holding
	// "since I last looked" is the reader's business.
	tallyTurns  atomic.Uint64
	supplyTurns atomic.Uint64
	// turns is the generation Liveness reports, and turnMark the two counts it last advanced at; turnMu makes the
	// check and the advance one step, since both loops call advanceTurn.
	turnMu   sync.Mutex
	turnMark [2]uint64
	turns    atomic.Uint64
	// lag is what the last completed band scan read of its oldest due step - see DispatchLag. Nil until a
	// scan completes, and again once the piston has gone idle.
	lag atomic.Pointer[lagReading]
}

// lagReading is one band scan's view of how long its most overdue step had been due, per band it tallied.
// Empty when the scan found nothing due.
type lagReading struct {
	bands []bandLag
}

// bandLag is one band's reading within a scan.
type bandLag struct {
	band  int
	dueMs float64
	at    time.Time // when that band's statement was sent, the instant its NOW_UTC() approximates
}

// New returns a piston for one already-open shard: its queries run on the shard's database, take the shard's
// priority turns, and keep to the residue class the shard reports. Each loop stamps its turns once per CYCLE rather
// than once per query, which for the supply loop means the plan and the fetch share one stamp: re-stamping between
// them would make the fetch look like a newer arrival than the plan it is resolving. The two LOOPS are stamped
// separately, and that is correct - they are separate units of work on separate clocks. It reports through d: its
// logger, its seams, and its meter, on which it creates its instruments (one it cannot create is logged and
// records nothing). The planner and cache are shared with this replica's other pistons and are not owned here.
func New(sh *database.Shard, d *diagnostics.Diags, plan *planner.Planner, cache *candidates.Cache) (*Piston, error) {
	if sh == nil {
		return nil, errors.New("shard is required")
	}
	shard, db := sh.Index(), sh.DB()
	if shard < 1 {
		return nil, errors.New("shard must be positive, got %d", shard)
	}
	if db == nil {
		return nil, errors.New("db is required")
	}
	if plan == nil {
		return nil, errors.New("planner is required")
	}
	if cache == nil {
		return nil, errors.New("cache is required")
	}
	p := &Piston{shard: shard, sh: sh, db: db, planner: plan, cache: cache,
		shardAttr: attribute.String("shard", strconv.Itoa(shard))}
	p.stealAfter.Store(defaultStealAfter)
	p.maxPriority.Store(DefaultMaxPriority)
	tallier, err := pipeline.NewTallier(shard, p, plan)
	if err != nil {
		return nil, errors.Trace(err)
	}
	supplier, err := pipeline.NewSupplier(shard, p, plan, cache)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.tallier, p.supplier = tallier, supplier
	p.diags = d
	inst, err := newInstruments(d.Meter())
	if err != nil {
		d.Logger().Error("Building piston instruments", "shard", shard, "error", err)
	}
	p.inst = inst
	return p, nil
}

// Shard is the shard this piston works.
func (p *Piston) Shard() int { return p.shard }

// SetIdle puts the piston in or out of idle. An idling piston runs no cycle and claims no work, so
// Liveness reports it idle and its owner can keep this replica counted for the connections it holds while
// excluding it from anything that divides work.
//
// The default is NOT idle: a fresh piston dispatches, which is the common case, and a zero value that
// silently did nothing would be the worse default.
//
// "Idle" here is a configured MODE, distinct from the refill sense of the word (nothing is due), which
// is a circumstance a dispatching piston meets all the time.
//
// GOING IDLE WITHDRAWS THIS SHARD, and it must: the planner's contract is that every shard either tallies
// or clears each cycle, and an idle piston does neither, so its last tally would stand forever. The
// planner is shared with this replica's other pistons, so a stale claim on a better band that fills the
// batch is the documented wedge - every live piston finds no capacity left to spill into and dispatches
// nothing, indefinitely. Benign only if every piston is idle, and this setter is per-piston, so the API permits the
// bad case. The cache partition goes for the same reason an empty plan clears it: nothing here is
// dispatchable, so every cached candidate is a dead hint a worker would pop and burn a claim round-trip on.
//
// Both are the same positive statement an empty plan makes, so both use the same two calls. They are the
// PROMPT half only: a cycle already in flight can re-tally or re-push after them, so each loop withdraws
// again on its way into idle - see runTallier. This setter alone is not sufficient, and is not the thing to
// reach for if that withdrawal ever needs strengthening.
func (p *Piston) SetIdle(idle bool) {
	p.idle.Store(idle)
	if idle {
		p.planner.Clear(p.shard)
		p.cache.Refill(p.shard, nil, pipeline.NoBand)
		p.lag.Store(nil)
	}
}

// Idle reports whether the piston is idling.
func (p *Piston) Idle() bool { return p.idle.Load() }

// SetStealAfter sets how many cycle periods a step outside this replica's residue class must have been DUE
// before this replica will select it at all - and even then only for the slots its own class could not
// fill, since the fetch ranks admitted steps by residue distance. Zero or negative disables stealing,
// restoring strict residue partitioning.
//
// WHAT IT IS FOR. Partitioning hands each replica a disjoint class of step ids, which is what keeps peers
// from racing for the same rows - but it also means a replica that is slow rather than DEAD keeps its class
// while being unable to serve it, and nobody else will look at those steps. Measured on a three-replica
// fleet with one replica crippled: throughput collapsed to a third of what the same fleet did with that
// replica REMOVED from the divisor entirely, with its class aging past 30s while its healthy peers sat at a
// third of a core. Two independent cripplings - a one-worker capacity cap and 10ms of injected latency -
// produced the same cap, so it is a property of the partitioning rather than of how a replica goes slow.
//
// It fails open on every axis: stealing only ever ADMITS rows, the claim CAS still grants every step, and a
// replica whose own class can fill its batch ranks every foreign row behind its own and keeps none of them.
func (p *Piston) SetStealAfter(periods int) {
	if periods < 0 {
		periods = 0
	}
	p.stealAfter.Store(int32(periods))
}

// StealAfter is the current steal grace in cycle periods; zero means stealing is off.
func (p *Piston) StealAfter() int { return int(p.stealAfter.Load()) }

// DefaultMaxPriority is the worst band a fresh piston counts.
const DefaultMaxPriority = 5

// SetMaxPriority sets the worst band the scan counts on its own: priorities run 1..maxBand. Work at a priority
// beyond it is counted together as one overflow band and served after every band in the range. Every piston on
// every replica sharing the shard should use the same value. A value below 1 is taken as 1. Read once per scan,
// so it is safe to call on a running piston.
func (p *Piston) SetMaxPriority(maxBand int) {
	p.maxPriority.Store(int32(min(max(1, maxBand), math.MaxInt32)))
}

// MaxPriority is the worst band the scan counts.
func (p *Piston) MaxPriority() int { return int(p.maxPriority.Load()) }

// Liveness reports whether this piston is turning, for an owner that publishes the fact somewhere the
// fleet can see it: the count of tally cycles completed so far, whether either loop is working right now,
// and whether the piston is idling.
//
// A COUNTER rather than a "since you last asked" flag, and that is the load-bearing part. A consuming
// getter would create a contract - call it exactly once per publication, from exactly one caller - and any
// second caller, a metric or a test, would silently clear the evidence and leave a healthy piston reading
// as stalled. Holding the previous count is the reader's business, so this is a pure read that may be
// called any number of times.
//
// The count advances only once BOTH loops have completed a cycle since it last advanced, because a turn has to
// mean this piston can both LOOK at its shard and SERVE it. Taking it from either loop alone, or from the minimum
// of their lifetime counts, reads a piston that serves nothing as alive - see tallyTurns.
//
// A loop inside its QUERIES counts, because a scan can legitimately run for tens of seconds on a deep
// backlog where the executor cannot early-stop, and a piston in the middle of one is plainly still serving.
// It is deliberately the queries and not the whole cycle: a cycle spends most of a healthy wall clock
// asleep in its pace, so counting that would make busy permanently true and turn this into "the loop is
// alive".
func (p *Piston) Liveness() (turns uint64, busy, idle bool) {
	// Busy means a loop has been in its queries LONGER THAN ONE CYCLE PERIOD, not merely that one is. A
	// cycle that fails instantly is also briefly in its queries, and a reader sampling on its own clock
	// catches that often enough to keep a broken piston looking alive for good - the exact stranding this
	// evidence exists to prevent. A scan that outruns the period is the case busy is for, and a scan that
	// does not will have completed and advanced the turn count long before any reader looks.
	//
	// EITHER loop counts: both hold a connection while they work, so a piston stuck in either is one nobody
	// should be redistributing work away from yet.
	//
	// EACH LOOP IS COMPARED AGAINST ITS OWN PERIOD, never against a shared one. The two are paced
	// independently, so a single threshold is right for at most one of them: measure the supply loop against
	// a longer tally period and a genuinely stuck fetch reads idle, measure it the other way and a healthy
	// scan reads busy. Period() floors at the DefaultMinGap constant, which is what keeps this a duration
	// predicate rather than the bool it exists to replace when a caller pins the cadence to zero.
	busy = p.tallier.WorkingFor() > p.tallier.Period() || p.supplier.WorkingFor() > p.supplier.Period()
	return p.turns.Load(), busy, p.idle.Load()
}

// DispatchLag is how long the most overdue step this piston can see, at a band the plan reaches, has been
// due, as of now: what the last completed band scan read, plus the time since it read it. ok is false until a
// scan has completed, and while the piston is idle.
//
// It covers what the scan covers - the shard's due bands up to where they fill the cache, this replica's
// residue class and foreign steps due past the steal grace - so the maximum across a shard's dispatchers is
// the shard's figure. It reads zero when nothing is due, and leaves out every band worse than the planner's
// reach: that work is held back by strict priority, not waiting on dispatch.
func (p *Piston) DispatchLag() (lag time.Duration, ok bool) {
	r := p.lag.Load()
	if r == nil || p.idle.Load() {
		return 0, false
	}
	reach := p.planner.Reach(p.cache.Capacity())
	for _, b := range r.bands {
		if reach >= 0 && b.band > reach {
			break // bands are best first, so every one after this is held back too
		}
		lag = max(lag, time.Duration(b.dueMs*float64(time.Millisecond))+time.Since(b.at))
	}
	return lag, true
}

// SetTallyCadence and SetSupplyCadence pace the two loops SEPARATELY, and there is deliberately no setter
// for both at once.
//
// The loops are independent by design - the whole reason the piston has two - so "the" cycle period is not a
// quantity that exists here. A single knob over both would let an owner express a cadence it does not mean,
// and would tempt anything inside this package into keying a threshold off whichever loop the facade
// happened to report; both are mistakes this API is shaped to make unavailable. Anything reasoning about how
// often a loop comes round asks that loop, through Period().
//
// Both are live: the values are read once per cycle rather than captured, so an owner re-deriving them takes
// effect on the very next cycle of each loop without a restart.
func (p *Piston) SetTallyCadence(interval, minGap time.Duration) {
	p.tallier.SetInterval(interval)
	p.tallier.SetMinGap(minGap)
}

// TallyCadence is the tally loop's configured pacing.
func (p *Piston) TallyCadence() (interval, minGap time.Duration) {
	return p.tallier.Interval(), p.tallier.MinGap()
}

// SetSupplyCadence paces the supply loop; see SetTallyCadence.
func (p *Piston) SetSupplyCadence(interval, minGap time.Duration) {
	p.supplier.SetInterval(interval)
	p.supplier.SetMinGap(minGap)
}

// SupplyCadence is the supply loop's configured pacing.
func (p *Piston) SupplyCadence() (interval, minGap time.Duration) {
	return p.supplier.Interval(), p.supplier.MinGap()
}

// newInstruments creates the piston's instruments on an already-created meter, keeping every one it could create
// and returning the error of the first it could not.
//
// A Meter rather than a MeterProvider so the OWNER picks the instrumentation scope once, for every
// module it assembles. Each package deriving its own scope from a provider would split one engine's
// metrics across several scopes in the export whenever a name drifted, and nothing here needs
// provider-level capability anyway.
//
// Instrument names are a public surface that dashboards bind to - do not rename them.
func newInstruments(m metric.Meter) (*instruments, error) {
	var errs []error
	hist := func(name, desc string) metric.Float64Histogram {
		h, err := m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(refillBuckets...))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return h
	}
	ctr := func(name, desc string) metric.Int64Counter {
		c, err := m.Int64Counter(name, metric.WithDescription(desc))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return c
	}
	inst := &instruments{
		queryDuration: hist("dwarf_refill_query_duration_seconds",
			"Duration of one phase of one shard's refill cycle, labelled by shard and by phase: the two queries "+
				"(band_keys, fetch_steps) and the two in-memory phases (planning, pushing)."),
		selected: ctr("dwarf_refill_candidates_selected",
			"Counts step candidates the refiller selected into the cache. Compare against dwarf_refill_candidates_discarded for the oversupply ratio."),
		discarded: ctr("dwarf_refill_candidates_discarded",
			"Counts cached step candidates thrown away un-popped by a wholesale refill - the refiller's waste signal. The steps stay pending and are re-selected, so this is cost, not loss."),
		stolen: ctr("dwarf_steps_stolen",
			"Counts candidates this replica selected from OUTSIDE its own residue class, because its own class could not fill the batch and the step had been due for several cycle periods - i.e. its owner was not taking it. Zero in a healthy fleet by construction: a replica holding its own work never steals. A sustained nonzero rate names a peer that is alive in the registry but not serving its share, which nothing else reports - and it is the quantity to read dwarf_steps_claim_lost against, since stealing trades exclusivity for coverage."),
	}
	if len(errs) > 0 {
		return inst, errors.Trace(errs[0])
	}
	return inst, nil
}

// Run drives both of the piston's loops until ctx is cancelled - one goroutine each, on independent
// cadences - and blocks until both have returned. A caller runs it in a goroutine and waits on its own
// WaitGroup.
//
// TWO LOOPS RATHER THAN ONE, because the band scan is O(due rows at the band) on every dialect (see
// ScanBand) and nothing in its tally early-stops. A single loop would let a deep backlog's scan set the
// whole cycle's period and therefore the rate candidates reach the workers - supplying least exactly when
// the backlog is deepest. Kept apart, the supply loop turns at its own cadence on whatever the planner
// already holds while a long scan runs beside it, and what a slow scan costs is the FRESHNESS of the
// fairness tally, which is the cheap thing to be stale about.
//
// It publishes nothing about this replica's liveness itself - Liveness is a pure read an owner samples on
// its own cadence, which is what keeps how often that is published independent of how long a cycle takes.
// The independence is a correctness requirement rather than tidiness: a liveness signal gated on a cycle
// RETURNING would let one deep-backlog scan drop a perfectly healthy replica out of its own fleet - which
// should mean "the process is stuck", nothing less.
func (p *Piston) Run(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); p.runTallier(ctx) }()
	go func() { defer wg.Done(); p.runSupplier(ctx) }()
	wg.Wait()
}

// runTallier reports what this shard has due, over and over, on the tally loop's own cadence.
func (p *Piston) runTallier(ctx context.Context) {
	// TRUE to start: this exists to undo what a COMPLETED cycle re-inserted, and a loop that has not run one
	// has nothing to undo - SetIdle already withdrew on its way to making this loop idle. Starting false
	// instead makes an already-idle piston wipe state on its first iteration, racing whatever its owner set
	// up in the meantime.
	withdrawn := true
	for {
		if ctx.Err() != nil {
			return
		}
		// An idle piston runs no cycle on either loop, so its turn count never moves and Liveness reports it
		// idle - which is how a reader tells the two populations apart without trusting anything the replica
		// says about itself. Re-checked on the idle poll so a live SetIdle(false) resumes within one poll.
		//
		// THE LOOP WITHDRAWS, not just SetIdle, and that is what makes going idle safe against a cycle
		// already in flight. SetIdle can only clear what is there when it runs; a scan that started before it
		// - and the band scan runs for seconds on a deep backlog - publishes its tally AFTER, re-entering the
		// shard nothing will ever report on again. Withdrawing here, past the point the cycle has returned,
		// undoes that whatever the interleaving was. Once per transition, since Clear on an absent shard is a
		// no-op but there is no reason to do it every poll.
		if p.idle.Load() {
			if !withdrawn {
				p.planner.Clear(p.shard)
				p.lag.Store(nil)
				withdrawn = true
			}
			if !p.sleep(ctx, idlePoll) {
				return
			}
			continue
		}
		withdrawn = false
		r := p.TallyCycle(ctx)
		if r.Err != nil && ctx.Err() == nil {
			p.diags.Logger().ErrorContext(ctx, "Refill tally", "shard", p.shard, "error", r.Err)
		}
		if r.Err == nil {
			// A scan completed and its tally is in the planner - see diagnostics.CheckpointRefillTallyDone.
			p.fireCheckpoint(ctx, diagnostics.CheckpointRefillTallyDone)
		}
	}
}

// runSupplier fills this shard's cache partition from the plan, over and over, on the supply loop's own
// cadence.
func (p *Piston) runSupplier(ctx context.Context) {
	withdrawn := true // see runTallier
	for {
		if ctx.Err() != nil {
			return
		}
		// Empties the partition on the way into idle, for the same reason the tally loop clears the shard:
		// a fetch already in flight when SetIdle ran pushes its batch AFTER the emptying, and the hints it
		// leaves are for work this piston will never claim. Each loop withdraws what IT owns - the Tallier
		// this shard's participation in planning, this its cache partition - the same seam the error policy
		// splits on.
		if p.idle.Load() {
			if !withdrawn {
				p.cache.Refill(p.shard, nil, pipeline.NoBand)
				withdrawn = true
			}
			if !p.sleep(ctx, idlePoll) {
				return
			}
			continue
		}
		withdrawn = false
		r := p.SupplyCycle(ctx)
		if r.Err != nil && ctx.Err() == nil {
			p.diags.Logger().ErrorContext(ctx, "Refill supply", "shard", p.shard, "error", r.Err)
		}
		if r.Reconciled {
			// This shard's partition now reflects the plan - see diagnostics.CheckpointRefillCycleDone. Gated on the push
			// rather than on the error, so a visit cannot mean "looked and gave up": both hold-everything
			// paths leave the partition alone and only one of them is an error.
			p.fireCheckpoint(ctx, diagnostics.CheckpointRefillCycleDone)
		}
	}
}

// fireCheckpoint fires name both unscoped (fleet-wide) and shard-scoped, the shape every checkpoint this
// package fires uses, so a caller can wait on either "any shard" or this shard specifically. A no-op when
// seams are disabled, which the caller need not check itself.
func (p *Piston) fireCheckpoint(ctx context.Context, name string) {
	p.diags.CheckpointBy(ctx, name, p.shard)
}

// Cycle runs exactly one tally cycle and then one supply cycle - each paced as always - and returns both
// results. It is the HAND-DRIVEN shape, for a caller that wants a full round trip at a moment of its own
// choosing; Run does not do this, it turns the two loops independently.
//
// NOT safe to call concurrently with Run, or with itself: each loop's cadence timestamps are
// single-goroutine state. A caller that drives cycles by hand should idle the piston first.
func (p *Piston) Cycle(ctx context.Context) (pipeline.TallyResult, pipeline.SupplyResult) {
	tr := p.TallyCycle(ctx)
	sr := p.SupplyCycle(ctx)
	return tr, sr
}

// SupplyCycle runs exactly ONE supply cycle - paced as always - records it, and returns what happened.
// runSupplier is this in a loop.
//
// NOT safe to call concurrently with Run or with itself; see Cycle.
func (p *Piston) SupplyCycle(ctx context.Context) pipeline.SupplyResult {
	p.fireCheckpoint(ctx, diagnostics.CheckpointRefillSupplyBegin)
	ctx = p.sh.ContextWithPriority(ctx)
	r := p.supplier.Cycle(ctx)
	p.recordSupply(ctx, r)
	// A cycle that pushed nothing still counts: an untallied shard and an outranked one are both honest
	// outcomes, and only a FAILED fetch means this piston could not serve what it was asked to.
	if r.Err == nil {
		p.supplyTurns.Add(1)
		p.advanceTurn()
	}
	return r
}

// TallyCycle runs exactly ONE tally cycle - paced as always - records it, and returns what happened.
// runTallier is this in a loop.
//
// NOT safe to call concurrently with Run or with itself; see Cycle.
func (p *Piston) TallyCycle(ctx context.Context) pipeline.TallyResult {
	p.fireCheckpoint(ctx, diagnostics.CheckpointRefillTallyBegin)
	ctx = p.sh.ContextWithPriority(ctx)
	r := p.tallier.Cycle(ctx)
	p.recordTally(ctx, r)
	// A cycle that found nothing due still counts: it proves this piston looked and could have served.
	// Gating on candidates instead would make a quiet fleet look like it had no dispatchers at all.
	if r.Err == nil {
		p.tallyTurns.Add(1)
		p.advanceTurn()
	}
	return r
}

// advanceTurn advances the generation Liveness reports when both loops have completed a cycle since it last
// advanced. Called by each loop after its own successful cycle, so whichever loop completes the pair advances it.
func (p *Piston) advanceTurn() {
	p.turnMu.Lock()
	defer p.turnMu.Unlock()
	tally, supply := p.tallyTurns.Load(), p.supplyTurns.Load()
	if tally > p.turnMark[0] && supply > p.turnMark[1] {
		p.turnMark = [2]uint64{tally, supply}
		p.turns.Add(1)
	}
}

// recordTally translates a tally cycle's result into this piston's instruments.
//
// There is deliberately no end-to-end cycle histogram. One existed, and its job was to expose the MERGED
// pass's straggler tax as the gap over the per-shard query max - a quantity the per-shard decoupling
// deleted along with the barrier that produced it. What remained was a coarse duplicate of the four phases
// this and recordSupply emit, which sum to the same work and say WHICH part was slow. Do not add it back
// without a question it answers that the phase split does not.
func (p *Piston) recordTally(ctx context.Context, r pipeline.TallyResult) {
	if r.Tallying > 0 {
		p.inst.queryDuration.Record(ctx, r.Tallying.Seconds(),
			metric.WithAttributes(p.shardAttr, attribute.String("phase", "band_keys")))
	}
}

// recordSupply translates a supply cycle's result into this piston's instruments.
func (p *Piston) recordSupply(ctx context.Context, r pipeline.SupplyResult) {
	in := p.inst
	if r.Fetching > 0 {
		in.queryDuration.Record(ctx, r.Fetching.Seconds(),
			metric.WithAttributes(p.shardAttr, attribute.String("phase", "fetch_steps")))
	}
	// The two non-query phases are recorded on the same histogram. The instrument's name says "query" for
	// historical reasons - it predates them - and renaming it would break the dashboards it is a public
	// surface for, so the phase label carries the distinction instead. Recording them matters because
	// planning is the one cost in the design that scales with fairness-key CARDINALITY (the lottery re-rolls
	// per slot over every key), and because the phases together are what reconstructs a cycle now that there
	// is no end-to-end histogram of one. Planning sits on the hot loop - once per supply cycle, an order of
	// magnitude more often than a scan at the reference cadence - so it is the phase to watch as fairness-key
	// cardinality grows.
	if r.Planning > 0 {
		in.queryDuration.Record(ctx, r.Planning.Seconds(),
			metric.WithAttributes(p.shardAttr, attribute.String("phase", "planning")))
	}
	if r.Pushing > 0 {
		in.queryDuration.Record(ctx, r.Pushing.Seconds(),
			metric.WithAttributes(p.shardAttr, attribute.String("phase", "pushing")))
	}
	if r.Selected > 0 {
		in.selected.Add(ctx, int64(r.Selected), metric.WithAttributes(p.shardAttr))
	}
	if r.Discarded > 0 {
		in.discarded.Add(ctx, int64(r.Discarded), metric.WithAttributes(p.shardAttr))
	}
}

// partitionPredicate restricts a selection scan to this replica's residue class of step_id. It returns
// ("", nil) - selecting everything - whenever partitioning must not apply.
//
// `step_id % R` is not sargable, so the scan still walks the band and filters: this reduces claim
// collisions, not scan cost. That is the intended trade - the scan was never the contended resource. And
// the class is a RESIDENCY, not a lock: the claim CAS remains the only thing that grants a step, so a
// stale pair costs a lost claim, never correctness.
// The pair is VALIDATED, not trusted, because both bad shapes are worse than not partitioning. replicas=0
// emits `step_id % 0`, which errors on every query - so the scan fails, the pipeline clears this shard, and
// it stays out of planning for as long as the shard keeps reporting it. An ordinal at or past replicas is
// quieter and worse: the predicate matches nothing, so the piston reports NoBand while genuinely holding
// work, with no error anywhere to notice. replicas=1 is a solo replica, where the predicate matches
// everything and is pure overhead. Today's caller happens to guard all three, but the fail-open posture is
// this package's promise, so it is enforced here.
// STEALING relaxes the class - see stealGrace - and it is a RELAXATION, never a restriction: the clause
// can only ever admit rows, so no residue class can be stranded by it and the claim CAS still arbitrates
// every step it admits.
//
// The PAIR and the GRACE are both passed IN rather than read here, and for the same reason: everything that
// selects rows and everything that then RANKS them must work from one reading. A caller that loads the pair
// again would be free to rank by a pair the query did not filter by - see resolvePartition.
func (p *Piston) partitionPredicate(replicas, ordinal int, grace time.Duration) (string, []any) {
	if replicas < 2 {
		return "", nil
	}
	if grace > 0 {
		// TWO TIERS, by distance: the NEIGHBOUR's class after one grace, ANYONE's after two.
		//
		// A single tier - anyone's class after one grace - works, and phase-9 measured it curing both
		// cripplings, but every idle replica becomes eligible for the same steps at the same instant, so
		// they race each other for rows their owner has abandoned. Giving each class exactly one designated
		// stealer for the first grace period makes that window contention-FREE: no two replicas are ever
		// eligible for the same step while it is in tier one.
		//
		// The second tier is not a fallback for tidiness - it is what keeps the scheme from stranding work
		// outright. With neighbour-only, two consecutive degraded replicas leave the far one's class with no
		// working stealer at all (its designated one is itself broken), so that class gets ZERO service
		// rather than slow service. Indefinite stranding is the exact failure stealing exists to prevent, so
		// coverage wins over efficiency once the work has demonstrably been ignored by BOTH its owner and its
		// neighbour. With two classes the neighbour is everyone else and the tiers coincide; the second tier
		// matters from three classes up. Pinned by TestPiston_StealReachesPastTheNeighbourAfterTwoGraces.
		//
		// The age is measured from not_before, NOT created_at. not_before is stamped NOW_UTC() at creation
		// and pushed forward by flow.Sleep and every retry backoff, so this reads as "has been DUE for at
		// least the grace" - which is the quantity meant. Against created_at, an hour-long sleep would be
		// stolen the instant it came due, on a fleet with nothing wrong with it.
		ms := grace.Milliseconds()
		return " AND (step_id % ? = ?" +
				" OR (step_id % ? = ? AND not_before <= DATE_ADD_MILLIS(NOW_UTC(), ?))" +
				" OR not_before <= DATE_ADD_MILLIS(NOW_UTC(), ?))",
			[]any{replicas, ordinal, replicas, (ordinal + 1) % replicas, -ms, -2 * ms}
	}
	return " AND step_id % ? = ?", []any{replicas, ordinal}
}

// resolvePartition loads this replica's (replicas, ordinal) pair and validates it, returning a zero pair
// whenever partitioning must not apply. EVERY consumer takes it from here, and takes it ONCE per query: the
// predicate that selects rows and the ranking that then orders them are only consistent if they work from
// the same reading, and the pair changes as the fleet does.
//
// The pair is VALIDATED, not trusted, because both bad shapes are worse than not partitioning. replicas=0
// emits `step_id % 0`, which errors on every query - so the scan fails, the Tallier clears this shard, and
// it stays out of planning for as long as the shard keeps reporting it. An ordinal at or past replicas is
// quieter and worse: the predicate matches nothing, so the piston reports NoBand while genuinely holding
// work, with no error anywhere to notice. replicas=1 is a solo replica, where the predicate matches
// everything and is pure overhead. Today's caller happens to guard all three, but the fail-open posture is
// this package's promise, so it is enforced here.
//
// A HALF-VALIDATED read is worse than no validation, which is why there is exactly one of these. An
// out-of-range ordinal correctly disables the SQL predicate, so the query selects everything - but a ranking
// built from that same bad pair puts nothing in tier 0 and counts every kept step as taken from a peer,
// flooding dwarf_steps_stolen and firing diagnostics.CheckpointRefillStole for steals that never happened.
func (p *Piston) resolvePartition() (replicas, ordinal int) {
	r, o, ok := p.sh.Residency()
	if !ok || r < 2 || o < 0 || o >= r {
		return 0, 0
	}
	return r, o
}

// stealGrace is how long a step outside this replica's residue class must have been DUE before this
// replica will select it anyway, or zero when stealing is off. BOTH queries call it, and they must: the
// plan is built from the tally, so a fetch selecting from a different population than the scan counted asks
// for rows it cannot see. Nothing gates it, so the two cannot disagree.
//
// A GRACE AND A FILL ORDER, and each covers the other's blind spot - neither alone is sufficient:
//
//   - THE GRACE (here): a foreign step must have been due for several cycle periods before it is even
//     ADMITTED. A healthy owner dispatches its own class within a cycle or two, so its work is never
//     eligible; a stalled owner's class ages without bound (measured: 23-41s against a ~67ms cycle, while a
//     healthy fleet's oldest due step sits at 0-1s). The two regimes are three orders of magnitude apart,
//     which is why the exact multiple is not delicate. Without it, every replica in an under-saturated
//     fleet would fill its spare slots from healthy peers, because a batch is sized to cache capacity
//     rather than to what is due.
//   - THE FILL ORDER (FetchSteps): among ADMITTED rows, this replica's own class is taken first, then its
//     neighbour's, then anyone's. So a replica reaches outside its class only for the slots its own class
//     could not fill - exact, current, and per slot.
//
// An ABSOLUTE age threshold was rejected for the grace: normal queueing delay under load is seconds, so any
// constant either never fires or admits everything under exactly the load it exists for. Deriving it from
// the cycle period is what keeps it proportionate to how fast a healthy owner reaches its own work.
func (p *Piston) stealGrace() time.Duration {
	n := p.stealAfter.Load()
	if n <= 0 {
		return 0
	}
	// The LONGER of the two loops' periods, because reaching a step of one's own takes a full round trip -
	// a scan to tally it, then a supply cycle to fetch it - so the loop that comes round least often is what
	// bounds how quickly a HEALTHY owner gets to its own work. Keying off either loop alone under-states that
	// the moment the two are paced differently, and an under-stated grace admits a healthy peer's work.
	// Period() carries the DefaultMinGap floor, without which a caller pinning the cadence to zero would get
	// a zero grace and take every foreign step the instant it came due, with no fuse at all.
	return time.Duration(n) * max(p.tallier.Period(), p.supplier.Period())
}

// residueTier ranks an admitted step by how far it is from this replica: 0 its own class, 1 its designated
// neighbour's, 2 anyone else's. It mirrors the three terms of partitionPredicate's relaxed clause, and
// FetchSteps sorts on it so a replica takes foreign work only for slots its own class could not fill.
//
// Everything is tier 0 when partitioning does not apply, which makes the sort a no-op for a solo replica.
func residueTier(stepID, replicas, ordinal int) int {
	if replicas < 2 {
		return 0
	}
	switch stepID % replicas {
	case ordinal:
		return 0
	case (ordinal + 1) % replicas:
		return 1
	}
	return 2
}

// residueSort sorts list by its parallel, precomputed tiers - the decorate half of rankByResidue's
// decorate-sort-undecorate, so a step's tier is computed once rather than twice per comparison.
type residueSort struct {
	list  []int
	tiers []int
}

func (s *residueSort) Len() int { return len(s.list) }
func (s *residueSort) Swap(i, j int) {
	s.list[i], s.list[j] = s.list[j], s.list[i]
	s.tiers[i], s.tiers[j] = s.tiers[j], s.tiers[i]
}
func (s *residueSort) Less(i, j int) bool { return s.tiers[i] < s.tiers[j] }

// ScanBand implements pipeline.BandSource. It returns this shard's best due priority band and one aggregate
// row per fairness key per due band, for every band up to the one where the counts reach the cache capacity - no
// fill reaches past it - in TWO statements whatever the number of bands:
//
//  1. COUNTS: every band 1..MaxPriority, each counted up to the cache capacity (bandCountsQuery). From them the
//     scan knows exactly which bands a fill can reach.
//  2. TALLY: one aggregate row per key at each of those bands, and nothing beyond them (bandTallyQuery).
//
// Nothing due anywhere costs the first statement only.
//
// It RETURNS O(distinct keys) rows per reached band but COSTS O(due rows) at the reached bands, on every dialect:
// the GROUP BY reads every due row of a band to count it. Sharding divides this term; nothing in the query does.
// The counts statement costs at most the capacity per band, and an empty band one index probe.
//
// The per-key count is CAPPED at the cache capacity here rather than in SQL. Capping is what the planner needs -
// the count becomes the key's remaining demand, and no key can be assigned more than the whole batch - so the cap
// is lossless.
//
// The partition filters both statements, so this replica tallies the bands ITS OWN class holds (plus foreign
// steps past the steal grace). A band that only a peer's class holds is that peer's to serve.
//
// The shard argument is the pipeline's own and equals Shard(); it is accepted for symmetry with the
// planner and cache calls, which are keyed the same way.
func (p *Piston) ScanBand(ctx context.Context, shard int) (band int, tallies []planner.Tally, err error) {
	// diagnostics.FaultRefillScanErr fails the scan without touching the database, so a test can drive the pipeline's
	// scan-error policy - clear this shard from planning, leave its cache partition intact - which is
	// otherwise reachable only by breaking a real database mid-run.
	if p.diags.Seams().IsFault(diagnostics.FaultRefillScanErr) {
		return pipeline.NoBand, nil, errors.New("injected fault: " + diagnostics.FaultRefillScanErr)
	}
	replicas, ordinal := p.resolvePartition()
	part, partArgs := p.partitionPredicate(replicas, ordinal, p.stealGrace())
	capacity := p.cache.Capacity()
	reached, err := p.countBands(ctx, part, partArgs, capacity)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	band = pipeline.NoBand
	reading := lagReading{}
	if len(reached) > 0 {
		var dueMs map[int]float64
		var sent time.Time
		tallies, dueMs, sent, err = p.tallyBands(ctx, part, partArgs, reached, capacity)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		// Best band first. The overflow branch reports the actual priorities beyond the range, so the bands
		// here are read off the tallies, not off the reached list.
		bands := make([]int, 0, len(dueMs))
		for b := range dueMs {
			bands = append(bands, b)
		}
		sort.Ints(bands)
		for _, b := range bands {
			band = min(band, b)
			reading.bands = append(reading.bands, bandLag{band: b, dueMs: dueMs[b], at: sent})
		}
	}
	p.lag.Store(&reading)
	return band, tallies, nil
}

// countBands runs the counts statement and returns every band from 1 up to, and including, the one where the
// counts reach capacity - or the worst band with due work, when they never do - best first, or none when nothing
// is due. MaxPriority+1 stands for the overflow - every priority beyond the range, counted together and served
// after every band in it.
//
// EVERY band up to the reach, empty ones included, so the tally's statement text depends on the reach alone: an
// empty band costs its branch one index probe, while a text per subset of non-empty bands is a fresh prepare on
// pgx and a fresh compile on SQL Server each time a sparse band comes or goes.
func (p *Piston) countBands(ctx context.Context, part string, partArgs []any, capacity int) ([]int, error) {
	maxBand := p.MaxPriority()
	args := make([]any, 0, (maxBand+1)*len(partArgs))
	for range maxBand + 1 { // the bands and the overflow
		args = append(args, partArgs...)
	}
	// The turn must outlive the ROWS, not just the call: they hold the connection until Close. Deferring it
	// FIRST is what gets that ordering - defers run last-in-first-out, so rows.Close below runs before this.
	// A context nothing stamped yields a no-op pass, which is how a caller that does not order its database
	// access pays nothing here.
	pass := database.WaitTurn(ctx)
	defer pass.Return()
	rows, err := p.db.QueryContext(ctx, bandCountsQuery(p.db.DriverName(), part, maxBand, capacity), args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	counts := make(map[int]int, maxBand)
	for rows.Next() {
		var b, n int
		if err := rows.Scan(&b, &n); err != nil {
			return nil, errors.Trace(err)
		}
		counts[b] = n
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	last, counted := 0, 0
	for b := 1; b <= maxBand+1 && counted < capacity; b++ {
		if counts[b] > 0 {
			last = b
			counted += counts[b]
		}
	}
	reached := make([]int, last)
	for i := range reached {
		reached[i] = i + 1
	}
	return reached, nil
}

// tallyBands runs the tally statement over the given bands and returns its tallies, each band's most overdue
// step's due time, and when the statement was sent.
func (p *Piston) tallyBands(ctx context.Context, part string, partArgs []any, bands []int, capacity int) (tallies []planner.Tally, dueMs map[int]float64, sent time.Time, err error) {
	args := make([]any, 0, len(bands)*len(partArgs))
	for range bands {
		args = append(args, partArgs...)
	}
	// The same turn-outlives-the-rows ordering as countBands.
	pass := database.WaitTurn(ctx)
	defer pass.Return()
	// due_ms rides the rows the count already reads, so the dispatch lag costs this scan one aggregate and
	// no query of its own. It is measured from not_before, not created_at: a step that slept for an hour has
	// been WAITING only since it came due.
	sent = time.Now()
	rows, err := p.db.QueryContext(ctx, bandTallyQuery(part, bands, p.MaxPriority()), args...)
	if err != nil {
		return nil, nil, sent, errors.Trace(err)
	}
	defer rows.Close()
	dueMs = map[int]float64{}
	for rows.Next() {
		var t planner.Tally
		var rowDueMs float64
		if err := rows.Scan(&t.Key, &t.Count, &t.AgeMs, &t.Weight, &t.Band, &rowDueMs); err != nil {
			return nil, nil, sent, errors.Trace(err)
		}
		t.Count = min(t.Count, capacity)
		tallies = append(tallies, t)
		dueMs[t.Band] = max(dueMs[t.Band], rowDueMs)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, sent, errors.Trace(err)
	}
	return tallies, dueMs, sent, nil
}

// bandCountsQuery is the scan's first statement: one row per band 1..maxBand, its due steps counted up to
// capacity, with the partition predicate part spliced into every band - plus one OVERFLOW row, maxBand+1, counting
// every priority beyond the range together.
//
// The overflow is what keeps work outside the range from stranding. Create refuses a priority beyond the maximum,
// but a step can still carry one: a flow created before the maximum existed, or by a replica configured with a
// larger one, and anything Continue, Fork or a subgraph inherits from either. Such work is served after every band
// in the range, in its own priority order. In normal operation the overflow is empty and costs one index probe. Each band is its own branch, an equality
// on priority with a LIMIT (TOP on mssql) inside, so each is a seek on idx_dwarf_steps_due - (parked, priority,
// not_before) - that stops after capacity entries, and a band whose steps are all asleep is an empty range: the
// seek's upper bound on not_before excludes them before any is read. The bands and the cap are inlined as integer
// literals, never user input, so the statement's binds are only the partition's.
//
// Do not count every band with one GROUP BY priority instead. Without an equality on priority, a range on
// not_before is no longer a seek, so the count reads every pending row, sleepers included - and it has no early
// stop, so a deep band below one that fills the cache is read in full on every scan.
func bandCountsQuery(driverName, part string, maxBand, capacity int) string {
	limit, top := " LIMIT "+strconv.Itoa(capacity), ""
	if driverName == "mssql" {
		limit, top = "", "TOP ("+strconv.Itoa(capacity)+") "
	}
	var b strings.Builder
	for band := 1; band <= maxBand+1; band++ {
		if band > 1 {
			b.WriteString(" UNION ALL ")
		}
		n := strconv.Itoa(band)
		match := " AND priority=" + n
		if band > maxBand {
			match = " AND priority>" + strconv.Itoa(maxBand)
		}
		b.WriteString("SELECT " + n + " AS priority, COUNT(*) AS cnt FROM (SELECT " + top + "1 AS x FROM dwarf_steps" +
			" WHERE " + dueSteps + part + match + limit + ") t" + n)
	}
	return b.String()
}

// bandTallyQuery is the scan's second statement: one aggregate row per fairness key at each of the given bands,
// with the partition predicate part spliced into every band. A band beyond maxBand is the overflow (see
// bandCountsQuery). Each band is its own branch with its value inlined,
// so each is a seek on idx_dwarf_steps_due's equality and not_before range, and a planner sizes it from that
// band's own statistics rather than an average.
//
// A plain GROUP BY, and that is load-bearing: nothing in it needs the rows in any order, so whichever index the
// planner reads them from, it never sorts them. Do not rank them with a window function (ROW_NUMBER() OVER the
// key, to cap the count or to pick the oldest step's columns): a ranking needs the band's rows in key order, and
// on PostgreSQL 18 an under-estimated deep band was read off the due index and then sorted whole, spilling to disk.
// A key's weight is therefore its LOWEST due step's, which an aggregate reads directly; the oldest step's would
// need a lookup per key, and a fan-out puts thousands of a key's steps in the same millisecond, so the lookup
// matches thousands of rows.
func bandTallyQuery(part string, bands []int, maxBand int) string {
	var b strings.Builder
	for i, band := range bands {
		if i > 0 {
			b.WriteString(" UNION ALL ")
		}
		// The overflow groups by the actual priority too, so each priority beyond the range is its own band to
		// the planner and the fetch matches it exactly.
		column, match, group := strconv.Itoa(band), " AND priority="+strconv.Itoa(band), ""
		if band > maxBand {
			column, match, group = "priority", " AND priority>"+strconv.Itoa(maxBand), ", priority"
		}
		b.WriteString("SELECT fairness_key, COUNT(*) AS cnt," +
			" DATE_DIFF_MILLIS(NOW_UTC(), MIN(created_at)) AS age_ms," +
			" MIN(fairness_weight) AS weight," +
			" " + column + " AS priority," +
			" DATE_DIFF_MILLIS(NOW_UTC(), MIN(not_before)) AS due_ms" +
			" FROM dwarf_steps" +
			" WHERE " + dueSteps + part + match +
			" GROUP BY fairness_key" + group)
	}
	return b.String()
}

// FetchSteps implements pipeline.StepSource. It loads, per (band, key) pair the plan chose, up to that band's
// per-key cap of this shard's oldest due steps, keyed and ordered oldest-first WITHIN each pair - which is all the
// plan replay reads. One statement serves every band the plan reached.
//
// The statement's per-key cap is UNIFORM, the largest of the plan's per-band caps, and each pair is trimmed to its
// own band's cap afterwards. On pgx, mssql and sqlite the pairs travel as one or two parameters (arrays on pgx,
// JSON on the others) and are joined back to rows in SQL, so the bind count does not grow with the number of keys;
// mysql binds two per pair - see fetchQuery for why.
//
// Cost is O(pairs x perKey) rows examined on pgx and mssql, where the per-key cap is a LIMIT/TOP inside a lateral
// join over an ordered index scan and therefore stops it. mysql and sqlite rank every due row of every chosen pair
// and cut afterwards - O(due rows at the planned keys), the same shape ScanBand has everywhere.
//
// The bands are bound, not re-mined: the plan committed to them, and re-mining could pick a better one that
// arrived between phases and mismatch the chosen keys. A bound priority does not defeat the selection index - only
// a bound status would, which is why status stays inlined.
func (p *Piston) FetchSteps(ctx context.Context, shard int, fetches []planner.Fetch) (map[planner.Slot][]int, error) {
	var pairs []planner.Slot
	capOf := make(map[int]int, len(fetches))
	perKey := 0
	for _, f := range fetches {
		if f.PerKeyCap <= 0 {
			continue
		}
		for _, k := range f.Keys {
			pairs = append(pairs, planner.Slot{Band: f.Band, Key: k})
		}
		capOf[f.Band] = f.PerKeyCap
		perKey = max(perKey, f.PerKeyCap)
	}
	if len(pairs) == 0 {
		return nil, nil
	}
	// diagnostics.FaultRefillFetchErr fails the fetch without touching the database, so a test can drive the one failure that
	// is invisible from outside: a piston that tallies honestly and serves nothing - see diagnostics.FaultRefillFetchErr.
	if p.diags.Seams().IsFault(diagnostics.FaultRefillFetchErr) {
		return nil, errors.New("injected fault: " + diagnostics.FaultRefillFetchErr)
	}
	// ONE reading of the pair and ONE of the grace, feeding the predicate that selects, the multiple that
	// over-fetches, and the ranking that orders - so none of the three can work from a different fleet than
	// the others. The grace is the same one the scan ran on; nothing gates it, so the two cannot disagree.
	replicas, ordinal := p.resolvePartition()
	grace := p.stealGrace()
	part, partArgs := p.partitionPredicate(replicas, ordinal, grace)
	// Over-fetch, so the ranking below has own-class rows to prefer: the query returns oldest first and the
	// oldest admitted rows are the stalled peer's. Twice R, not R: when every class is admitted, the own rows
	// in the oldest perKey*R are only perKey ON AVERAGE, so a multiple of R leaves the ranking short about
	// half the time. ONLY when foreign rows can be admitted at all - with the class strict, every admitted row
	// is already this replica's own, and a multiple would fetch four times the plan's demand purely to
	// discard three quarters of it.
	overFetch := 1
	if grace > 0 {
		overFetch = max(1, min(2*replicas, 4))
	}
	stmt, args, err := fetchQuery(p.db.DriverName(), pairs, perKey*overFetch, part, partArgs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// The turn must outlive the ROWS, not just the call, so this is deferred before the query - same
	// ordering argument as in ScanBand.
	pass := database.WaitTurn(ctx)
	defer pass.Return()
	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	out := map[planner.Slot][]int{}
	for rows.Next() {
		var stepID int
		var slot planner.Slot
		// Every dialect's query projects (priority, fairness_key, step_id) in that order - see fetchQuery,
		// which keeps the projection uniform precisely so this loop is shared.
		if err := rows.Scan(&slot.Band, &slot.Key, &stepID); err != nil {
			return nil, errors.Trace(err)
		}
		out[slot] = append(out[slot], stepID)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	stolen := p.rankByResidue(out, capOf, replicas, ordinal)
	if stolen > 0 {
		p.inst.stolen.Add(ctx, int64(stolen), metric.WithAttributes(p.shardAttr))
		p.fireCheckpoint(ctx, diagnostics.CheckpointRefillStole)
	}
	return out, nil
}

// rankByResidue applies the fill order to a fetch: within each (band, key) pair it sorts the admitted steps by how
// far they are from this replica, then trims to the per-key cap the plan asked for at that band. It returns how many of the
// KEPT steps came from outside this replica's class, which is what dwarf_steps_stolen counts.
//
// The sort is STABLE, so oldest-first survives inside each tier - the fetch returns rows in
// (created_at, step_id) order and only the tier boundaries move. Ordering across tiers is the one place
// this design trades away strict oldest-first, and it is bounded: a row only reaches a foreign tier after
// its owner has demonstrably ignored it for a grace or two.
//
// Counting the stolen steps AFTER the trim, not before, is what keeps the metric honest. The fetch
// deliberately over-fetches, so a healthy replica pulls foreign rows it then ranks last and discards -
// counting those would report stealing that never happened.
func (p *Piston) rankByResidue(out map[planner.Slot][]int, capOf map[int]int, replicas, ordinal int) int {
	stolen := 0
	for slot, list := range out {
		perKey := capOf[slot.Band]
		if replicas > 1 {
			// Tier is computed once per element (decorate-sort-undecorate) rather than twice per
			// comparison: sort.SliceStable's comparator would otherwise recompute residueTier on both
			// operands every comparison, O(n log n) redundant evaluations instead of O(n).
			tiers := make([]int, len(list))
			for i, stepID := range list {
				tiers[i] = residueTier(stepID, replicas, ordinal)
			}
			sort.Stable(&residueSort{list: list, tiers: tiers})
		}
		if len(list) > perKey {
			list = list[:perKey]
		}
		if replicas > 1 {
			for _, stepID := range list {
				if stepID%replicas != ordinal {
					stolen++
				}
			}
		}
		out[slot] = list
	}
	return stolen
}

// sleep waits d or until ctx is done, reporting false if the context ended. A method for containment
// rather than need - nothing here reads the receiver.
func (p *Piston) sleep(ctx context.Context, d time.Duration) bool {
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
