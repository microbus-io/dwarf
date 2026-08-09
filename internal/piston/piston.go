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
	"log/slog"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/pipeline"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// refillBuckets are the explicit bucket boundaries for both histograms, in SECONDS. The OTEL defaults are
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

// PartitionFunc reports the replica partition - see SetPartitionFunc.
type PartitionFunc func() (replicas, ordinal int, ok bool)

// ContextFunc derives the context a cycle's queries run on - see SetContextFunc.
type ContextFunc func(context.Context) context.Context

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

	db       *sequel.DB
	planner  *planner.Planner
	cache    *candidates.Cache
	tallier  *pipeline.Tallier
	supplier *pipeline.Supplier

	// Live configuration, each independently atomic. There is no grouped snapshot because nothing here is
	// coupled - reading idle and the partition a microsecond apart cannot produce an inconsistent pair.
	idle      atomic.Bool
	partition atomic.Pointer[PartitionFunc]
	// cycleCtx derives the context each cycle's queries run on - see SetContextFunc.
	cycleCtx atomic.Pointer[ContextFunc]
	logger   atomic.Pointer[slog.Logger]
	inst     atomic.Pointer[instruments]
	seams    atomic.Pointer[seamster.Seamster]

	// stealAfter is how many cycle periods a foreign step must have been due before this replica takes it.
	// Zero disables stealing outright, which is what an owner sets when it wants the residue class enforced
	// strictly.
	stealAfter atomic.Int32
	// tallyTurns and supplyTurns count each loop's successful cycles, monotonically and independently -
	// neither loop ever writes the other's. Liveness reports their MINIMUM, because a turn has to mean this
	// piston can both LOOK at its shard and SERVE it, and the two failures are symmetric:
	//
	//   - every SCAN failing clears this shard from planning and selects nothing, while the supply loop
	//     turns happily on an empty plan;
	//   - every FETCH failing leaves the tally honest and the band claimed, while the partition takes zero
	//     candidates - the quiet shape the bind-count overflow produces.
	//
	// Either alone therefore reads a piston that serves nothing as fully alive, and it then holds a residue
	// class nobody else will select. The minimum stalls when either loop does, which is the whole point.
	//
	// COUNTERS rather than a flag the reader clears: a consuming getter would make any second caller - a
	// metric, a test - silently swallow the evidence and leave a healthy piston reading as stalled. Holding
	// "since I last looked" is the reader's business.
	tallyTurns  atomic.Uint64
	supplyTurns atomic.Uint64
}

// New returns a piston for one shard over an already-open database handle. The planner and cache are
// shared with this replica's other pistons and are not owned here.
func New(shard int, db *sequel.DB, plan *planner.Planner, cache *candidates.Cache) (*Piston, error) {
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
	p := &Piston{shard: shard, db: db, planner: plan, cache: cache,
		shardAttr: attribute.String("shard", strconv.Itoa(shard))}
	p.stealAfter.Store(defaultStealAfter)
	tallier, err := pipeline.NewTallier(shard, p, plan)
	if err != nil {
		return nil, errors.Trace(err)
	}
	supplier, err := pipeline.NewSupplier(shard, p, plan, cache)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.tallier, p.supplier = tallier, supplier
	p.SetLogger(nil)
	p.SetMeter(nil)
	p.SetSeams(nil)
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
// planner is shared with this replica's other pistons, so that stale claim on the best band is the
// documented wedge - every live piston finds none of its own keys at that band and dispatches nothing,
// indefinitely. Benign only if every piston is idle, and this setter is per-piston, so the API permits the
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
// The count is the MINIMUM of the two loops' own counts, because a turn has to mean this piston can both
// LOOK at its shard and SERVE it. Taking it from either loop alone reads a piston that serves nothing as
// fully alive - see tallyTurns.
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
	turns = min(p.tallyTurns.Load(), p.supplyTurns.Load())
	return turns, busy, p.idle.Load()
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

// SetPartitionFunc supplies the replica partition: the (replicas, ordinal) pair that restricts this
// replica's selection to its own residue class of step ids, so replicas sharing a database select
// disjoint candidates instead of racing for the same rows. ok=false selects everything, which is correct
// for a solo replica or an unknown ordinal.
//
// A function rather than a value because the pair changes as the fleet does, and a captured one would
// leave this piston selecting a class that no longer exists.
func (p *Piston) SetPartitionFunc(fn PartitionFunc) {
	if fn == nil {
		p.partition.Store(nil)
		return
	}
	p.partition.Store(&fn)
}

// SetContextFunc supplies a derivation applied to the context at the START of each cycle, on both loops, so
// whatever the caller needs its queries to carry - a priority, an admission time, a deadline - is on both
// of them without this package knowing what any of it means. Nil (the default) leaves the context alone.
//
// Once per CYCLE rather than once per query, which for the supply loop means the plan and the fetch share
// one stamp: re-deriving between them would make the fetch look like a newer arrival than the plan it is
// resolving. The two LOOPS are stamped separately, and that is correct - they are separate units of work
// on separate clocks, and stamping them together would be claiming a coupling the split removed.
func (p *Piston) SetContextFunc(fn ContextFunc) {
	if fn == nil {
		p.cycleCtx.Store(nil)
		return
	}
	p.cycleCtx.Store(&fn)
}

// SetLogger sets the logger. Nil restores the discarding default.
func (p *Piston) SetLogger(l *slog.Logger) {
	if l == nil {
		l = slog.New(slog.DiscardHandler)
	}
	p.logger.Store(l)
}

// SetSeams supplies the owner's fault-injection seams, so a test that drives a whole engine can make this
// piston's scan fail (FaultScanErr) without reaching for the database. Nil restores an inert one, which is
// also the default - a piston built and never told otherwise consults nothing.
//
// The seams are the OWNER's, not this package's, for the same reason the meter is: one catalogue of fault
// names per application, armed in one place, however many modules consult it. A Seamster built disabled
// makes every consult a bool read, so a production piston pays nothing for the call site.
//
// This package deliberately has no seam of its OWN, and the distinction matters. A seam inside pure logic
// would be a signal that a dependency should have been injected instead; this one perturbs the DATABASE
// query, which is exactly the boundary a test cannot otherwise reach - the pipeline's error policy (clear
// the shard from planning, leave the cache alone) is only reachable by making a real scan fail. The
// pipeline itself gets none: its faults are all reachable through the Source, which is this type.
func (p *Piston) SetSeams(s *seamster.Seamster) {
	if s == nil {
		s = seamster.New(false)
	}
	p.seams.Store(s)
}

// SetMeter resolves this piston's instruments from an already-created meter. Nil restores no-ops.
//
// A Meter rather than a MeterProvider so the OWNER picks the instrumentation scope once, for every
// module it assembles. Each package deriving its own scope from a provider would split one engine's
// metrics across several scopes in the export whenever a name drifted, and nothing here needs
// provider-level capability anyway.
//
// Instrument names are a public surface that dashboards bind to - do not rename them.
func (p *Piston) SetMeter(m metric.Meter) error {
	if m == nil {
		m = noop.NewMeterProvider().Meter("")
	}
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
	p.inst.Store(&instruments{
		queryDuration: hist("dwarf_refill_query_duration_seconds",
			"Duration of one phase of one shard's refill cycle, labelled by shard and by phase: the two queries "+
				"(band_keys, fetch_steps) and the two in-memory phases (planning, pushing)."),
		selected: ctr("dwarf_refill_candidates_selected",
			"Counts step candidates the refiller selected into the cache. Compare against dwarf_refill_candidates_discarded for the oversupply ratio."),
		discarded: ctr("dwarf_refill_candidates_discarded",
			"Counts cached step candidates thrown away un-popped by a wholesale refill - the refiller's waste signal. The steps stay pending and are re-selected, so this is cost, not loss."),
		stolen: ctr("dwarf_steps_stolen",
			"Counts candidates this replica selected from OUTSIDE its own residue class, because its own class could not fill the batch and the step had been due for several cycle periods - i.e. its owner was not taking it. Zero in a healthy fleet by construction: a replica holding its own work never steals. A sustained nonzero rate names a peer that is alive in the registry but not serving its share, which nothing else reports - and it is the quantity to read dwarf_steps_claim_lost against, since stealing trades exclusivity for coverage."),
	})
	if len(errs) > 0 {
		return errors.Trace(errs[0])
	}
	return nil
}

// Run drives both of the piston's loops until ctx is cancelled - one goroutine each, on independent
// cadences - and blocks until both have returned. A caller runs it in a goroutine and waits on its own
// WaitGroup.
//
// TWO LOOPS RATHER THAN ONE, because the band scan is O(due rows at the band) on every dialect (see
// ScanBand) and nothing in the query early-stops. A single loop would let a deep backlog's scan set the
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
			p.logger.Load().ErrorContext(ctx, "Refill tally", "shard", p.shard, "error", r.Err)
		}
		if r.Err == nil {
			// A scan completed and its tally is in the planner - see CheckpointTallyDone.
			if seams := p.seams.Load(); seams.Enabled() { // Enabled gates the assembled name in production
				seams.Checkpoint(ctx, CheckpointTallyDone)
				seams.Checkpoint(ctx, seamsJoin(CheckpointTallyDone, strconv.Itoa(p.shard)))
			}
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
			p.logger.Load().ErrorContext(ctx, "Refill supply", "shard", p.shard, "error", r.Err)
		}
		if r.Reconciled {
			// This shard's partition now reflects the plan - see CheckpointCycleDone. Gated on the push
			// rather than on the error, so a visit cannot mean "looked and gave up": both hold-everything
			// paths leave the partition alone and only one of them is an error.
			if seams := p.seams.Load(); seams.Enabled() { // Enabled gates the assembled name in production
				seams.Checkpoint(ctx, CheckpointCycleDone)
				seams.Checkpoint(ctx, seamsJoin(CheckpointCycleDone, strconv.Itoa(p.shard)))
			}
		}
	}
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
	if fn := p.cycleCtx.Load(); fn != nil {
		ctx = (*fn)(ctx)
	}
	r := p.supplier.Cycle(ctx)
	p.recordSupply(ctx, r)
	// A cycle that pushed nothing still counts: an untallied shard and an outranked one are both honest
	// outcomes, and only a FAILED fetch means this piston could not serve what it was asked to.
	if r.Err == nil {
		p.supplyTurns.Add(1)
	}
	return r
}

// TallyCycle runs exactly ONE tally cycle - paced as always - records it, and returns what happened.
// runTallier is this in a loop.
//
// NOT safe to call concurrently with Run or with itself; see Cycle.
func (p *Piston) TallyCycle(ctx context.Context) pipeline.TallyResult {
	if fn := p.cycleCtx.Load(); fn != nil {
		ctx = (*fn)(ctx)
	}
	r := p.tallier.Cycle(ctx)
	p.recordTally(ctx, r)
	// A cycle that found nothing due still counts: it proves this piston looked and could have served.
	// Gating on candidates instead would make a quiet fleet look like it had no dispatchers at all.
	if r.Err == nil {
		p.tallyTurns.Add(1)
	}
	return r
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
		p.inst.Load().queryDuration.Record(ctx, r.Tallying.Seconds(),
			metric.WithAttributes(p.shardAttr, attribute.String("phase", "band_keys")))
	}
}

// recordSupply translates a supply cycle's result into this piston's instruments.
func (p *Piston) recordSupply(ctx context.Context, r pipeline.SupplyResult) {
	in := p.inst.Load()
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
// it stays out of planning for as long as the func keeps saying so. An ordinal at or past replicas is
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
		// neighbour. Pinned by TestStealTwoBadApplesflow.
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
// it stays out of planning for as long as the func keeps saying so. An ordinal at or past replicas is
// quieter and worse: the predicate matches nothing, so the piston reports NoBand while genuinely holding
// work, with no error anywhere to notice. replicas=1 is a solo replica, where the predicate matches
// everything and is pure overhead. Today's caller happens to guard all three, but the fail-open posture is
// this package's promise, so it is enforced here.
//
// A HALF-VALIDATED read is worse than no validation, which is why there is exactly one of these. An
// out-of-range ordinal correctly disables the SQL predicate, so the query selects everything - but a ranking
// built from that same bad pair puts nothing in tier 0 and counts every kept step as taken from a peer,
// flooding dwarf_steps_stolen and firing CheckpointStole for steals that never happened.
func (p *Piston) resolvePartition() (replicas, ordinal int) {
	fn := p.partition.Load()
	if fn == nil {
		return 0, 0
	}
	r, o, ok := (*fn)()
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

// ScanBand implements pipeline.BandSource. It returns this shard's minimum due priority band and one
// aggregate row per fairness key at that band.
//
// It RETURNS O(distinct keys) rows but COSTS O(due rows at the band), on every dialect. The window
// function ranks every matching row before the outer `rn <= capacity` cut discards any, so the cap bounds
// the reported COUNT and nothing else - not the scan, and not the wire either, since the GROUP BY emits
// one row per key whether or not the cut fires. Measured on PostgreSQL at ~0.004-0.005ms per due row over
// a fixed floor, and 4x the backlog on one shard made the scan 2.85x slower - super-linear, because a
// slower scan deepens the backlog it is scanning. Sharding divides this term; nothing in the query does.
//
// The per-key count is CAPPED at the cache capacity rather than exact: MAX(rn) under the `rn <= capacity`
// cut, not COUNT(*) OVER. Capping is what the planner needs - the count becomes the key's remaining
// demand, and no key can be assigned more than the whole batch - so the cap is lossless there even though
// it buys nothing on the scan.
//
// The partition filters the ROWS this replica tallies but deliberately NOT the MIN(priority) subquery:
// the band is a cluster-wide fact, so mining it from one replica's slice would let replicas disagree on
// which band is open. A replica holding nothing at the global band therefore tallies zero rows - correct,
// since its own work is at a worse band that must not be served until the better one drains.
//
// The shard argument is the pipeline's own and equals Shard(); it is accepted for symmetry with the
// planner and cache calls, which are keyed the same way.
func (p *Piston) ScanBand(ctx context.Context, shard int) (band int, tallies []planner.Tally, err error) {
	// FaultScanErr fails the scan without touching the database, so a test can drive the pipeline's
	// scan-error policy - clear this shard from planning, leave its cache partition intact - which is
	// otherwise reachable only by breaking a real database mid-run.
	if p.seams.Load().IsFault(FaultScanErr) {
		return pipeline.NoBand, nil, errors.New("injected fault: " + FaultScanErr)
	}
	replicas, ordinal := p.resolvePartition()
	part, partArgs := p.partitionPredicate(replicas, ordinal, p.stealGrace())
	args := make([]any, 0, len(partArgs)+1)
	args = append(args, partArgs...)
	args = append(args, p.cache.Capacity())
	// The turn must outlive the ROWS, not just the call: they hold the connection until Close. Deferring it
	// FIRST is what gets that ordering - defers run last-in-first-out, so rows.Close below runs before this.
	// A context nothing stamped yields a no-op pass, which is how a caller that does not order its database
	// access pays nothing here.
	pass := turnstile.WaitTurn(ctx)
	defer pass.Return()
	rows, err := p.db.QueryContext(ctx,
		"SELECT fairness_key, MAX(rn) AS cnt,"+
			" MAX(CASE WHEN rn=1 THEN age_ms ELSE NULL END) AS age_ms,"+
			" MAX(CASE WHEN rn=1 THEN weight ELSE NULL END) AS weight,"+
			" MAX(priority) AS priority FROM ("+
			"SELECT fairness_key, priority,"+
			" DATE_DIFF_MILLIS(NOW_UTC(), created_at) AS age_ms,"+
			" fairness_weight AS weight,"+
			" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn"+
			" FROM dwarf_steps"+
			" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
			part+
			" AND priority=(SELECT MIN(priority) FROM dwarf_steps"+
			" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC())"+
			") t WHERE rn<=? GROUP BY fairness_key",
		args...,
	)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	defer rows.Close()
	band = pipeline.NoBand
	for rows.Next() {
		var t planner.Tally
		var priority int
		if err := rows.Scan(&t.Key, &t.Count, &t.AgeMs, &t.Weight, &priority); err != nil {
			return 0, nil, errors.Trace(err)
		}
		band = priority
		tallies = append(tallies, t)
	}
	if err := rows.Err(); err != nil {
		return 0, nil, errors.Trace(err)
	}
	return band, tallies, nil
}

// FetchSteps implements pipeline.StepSource. It loads, per chosen fairness key, up to perKey of this shard's
// oldest due steps at the given band, keyed and ordered oldest-first WITHIN each key - which is all the
// plan replay reads. Rows are grouped by key rather than sorted across keys.
//
// perKey is a UNIFORM cap, the max per-key demand across this shard's slice rather than each key's exact
// demand. The chosen keys travel as ONE json-encoded parameter and are joined back to rows in SQL, so the
// bind count is FIXED at four or five however many keys the plan chose - see fetchQuery.
//
// Cost is O(len(keys) x perKey) rows examined on pgx/mysql/mssql, where the per-key cap is a LIMIT on an
// ordered index scan and therefore stops it. SQLite has no lateral join, so it ranks every due row of every
// chosen key and cuts afterwards - O(due rows at the band), the same shape ScanBand has everywhere.
//
// The band is bound (priority=?), not re-mined from a MIN subquery: the plan committed to this band, and
// re-mining could pick a lower one that arrived between phases and mismatch the chosen keys. A bound
// priority does not defeat the selection index - only a bound status would, which is why status stays
// inlined.
func (p *Piston) FetchSteps(ctx context.Context, shard, band int, keys []string, perKey int) (map[string][]int, error) {
	if len(keys) == 0 || perKey <= 0 {
		return nil, nil
	}
	// FaultFetchErr fails the fetch without touching the database, so a test can drive the one failure that
	// is invisible from outside: a piston that tallies honestly and serves nothing - see FaultFetchErr.
	if p.seams.Load().IsFault(FaultFetchErr) {
		return nil, errors.New("injected fault: " + FaultFetchErr)
	}
	// ONE reading of the pair and ONE of the grace, feeding the predicate that selects, the multiple that
	// over-fetches, and the ranking that orders - so none of the three can work from a different fleet than
	// the others. The grace is the same one the scan ran on; nothing gates it, so the two cannot disagree.
	replicas, ordinal := p.resolvePartition()
	grace := p.stealGrace()
	part, partArgs := p.partitionPredicate(replicas, ordinal, grace)
	// Over-fetch, so the ranking below has own-class rows to prefer: the query returns oldest first and the
	// oldest admitted rows are the stalled peer's. ONLY when foreign rows can be admitted at all - with the
	// class strict, every admitted row is already this replica's own, and a multiple would fetch four times
	// the plan's demand purely to discard three quarters of it.
	overFetch := 1
	if grace > 0 {
		overFetch = max(1, min(replicas, 4))
	}
	stmt, args, err := fetchQuery(p.db.DriverName(), band, keys, perKey*overFetch, part, partArgs)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// The turn must outlive the ROWS, not just the call, so this is deferred before the query - same
	// ordering argument as in ScanBand.
	pass := turnstile.WaitTurn(ctx)
	defer pass.Return()
	rows, err := p.db.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	out := map[string][]int{}
	for rows.Next() {
		var stepID int
		var key string
		// Every dialect's query projects (fairness_key, step_id) in that order - see fetchQuery, which
		// keeps the projection uniform precisely so this loop is shared.
		if err := rows.Scan(&key, &stepID); err != nil {
			return nil, errors.Trace(err)
		}
		out[key] = append(out[key], stepID)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	stolen := p.rankByResidue(out, perKey, replicas, ordinal)
	if stolen > 0 {
		p.inst.Load().stolen.Add(ctx, int64(stolen), metric.WithAttributes(p.shardAttr))
		if seams := p.seams.Load(); seams.Enabled() {
			seams.Checkpoint(ctx, CheckpointStole)
			seams.Checkpoint(ctx, seamsJoin(CheckpointStole, strconv.Itoa(p.shard)))
		}
	}
	return out, nil
}

// rankByResidue applies the fill order to a fetch: within each key it sorts the admitted steps by how far
// they are from this replica, then trims to the per-key cap the plan asked for. It returns how many of the
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
func (p *Piston) rankByResidue(out map[string][]int, perKey, replicas, ordinal int) int {
	stolen := 0
	for key, list := range out {
		if replicas > 1 {
			sort.SliceStable(list, func(i, j int) bool {
				return residueTier(list[i], replicas, ordinal) < residueTier(list[j], replicas, ordinal)
			})
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
		out[key] = list
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
