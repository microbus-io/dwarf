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

// Package piston supplies step candidates from one shard. One piston works one shard, firing the same
// cycle over and over against its own database on its own clock, with no barrier against its peers; an
// engine with N shards runs N of them.
//
// A piston is a CONSUMER of its database, never the owner: the handle is passed in already open and is
// closed by whoever opened it, so there is no Open, no Close, and no say over pool sizes. It owns the
// supply cycle, the two queries behind it, and its instruments; it borrows the planner and the candidate
// cache, both shared with every other piston on the replica.
//
// Run blocks and drives the cycle until its context ends:
//
//	cycle (paced by the pipeline) -> record -> repeat
//
// Liveness reports whether that loop is turning, for an owner that publishes this replica's liveness
// somewhere the fleet can see it.
//
// SetIdle(true) skips the cycle entirely, which is the await-only replica: it keeps holding connections,
// but claims no work and reports itself idle. Going idle withdraws the shard from the shared planner and
// empties its cache partition, so nothing is left claiming a band this piston no longer reports on.
//
// Every Set* is live: the owner may re-derive any of them while Run is in flight.
package piston

import (
	"context"
	"log/slog"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/internal/pipeline"
	"github.com/microbus-io/dwarf/internal/planner"
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
// ~10 periods separates those cleanly. It is a small multiple because the gate - not this - is what keeps
// a busy fleet from stealing at all; this only has to outlast the time a HEALTHY owner takes to reach its
// own work, which is a cycle or two by construction.
const defaultStealAfter = 4

// FaultScanErr makes ScanBand fail without touching the database - see SetSeams. The name is exported so
// the owning application's fault catalogue can alias it rather than re-spell the string.
const FaultScanErr = "refillScanErr"

// CheckpointCycleDone fires once per cycle that PUSHED - see SetSeams. Exported for the same catalogue
// reason as FaultScanErr.
//
// It is fired only when the cycle reached its push, which is exactly when this shard's cache partition has
// been reconciled against the plan: the two error paths (a failed tally, a failed fetch) return before
// pushing and deliberately leave the partition alone, while an empty plan pushes nothing and CLEARS it. So
// a visit means "this shard's partition now reflects the plan", which is the thing a test can neither
// observe from outside nor wait out on a clock - each piston turns on its own cadence, and a shard whose
// goroutine is starved or blocked on a slow round trip can hold an unreconciled partition arbitrarily long
// while its peers turn normally.
//
// Fired BOTH unscoped and scoped by shard (a scoped fire does not wake an unscoped waiter, so a waiter for
// "any shard cycled" and one for "shard 3 cycled" need separate fires). Counting scoped visits is the way
// to wait for a SPECIFIC shard, since with several shards the unscoped count says nothing about which.
const CheckpointCycleDone = "refillCycleDone"

// CheckpointStole fires once per fetch that took at least one step from OUTSIDE this replica's residue
// class - see SetSeams. Exported for the same catalogue reason as FaultScanErr.
//
// It earns its place on the same boundary rule as CheckpointCycleDone: it reports an effect on state the
// package borrows, at the moment the effect happens, and no clock substitutes for it. A test proving that a
// slow peer's work is picked up cannot wait out a duration - the steal fires on the first cycle after the
// gate arms and the grace elapses, which is a function of the pipeline's cadence, the peer's degradation
// and the backlog, none of which the test controls. Without it the only assertion available is "the flows
// eventually finished", which passes just as well against a build where stealing does nothing and the
// dispatch-window eviction did the work several seconds later - i.e. it cannot tell the mechanism under
// test from the mechanism it replaces.
//
// Fired BOTH unscoped and scoped by shard, for the same reason CheckpointCycleDone is.
const CheckpointStole = "refillStole"

// seamsJoin builds a targeted seam name: a base name, then the entity it targets, joined with ":". A consult
// site and the test that arms it both call it, so neither can spell the join the other does not. A targeted
// name and the bare one are DIFFERENT seams, so a site wanting both fires both.
func seamsJoin(parts ...string) string {
	return strings.Join(parts, ":")
}

// PartitionFunc reports the replica partition - see SetPartitionFunc.
type PartitionFunc func() (replicas, ordinal int, ok bool)

// instruments is this piston's metric set, swapped atomically as a group so a recording cycle never sees
// half of one meter and half of another.
type instruments struct {
	cycleDuration metric.Float64Histogram
	queryDuration metric.Float64Histogram
	selected      metric.Int64Counter
	discarded     metric.Int64Counter
	stolen        metric.Int64Counter
}

// Piston runs one shard's supply cycle and heartbeat.
//
// Run must be driven by a single goroutine. Every Set* is safe to call from another one, at any time.
type Piston struct {
	shard   int
	db      *sequel.DB
	planner *planner.Planner
	cache   *candidatecache.Cache
	pipe    *pipeline.Pipeline

	// Live configuration, each independently atomic. There is no grouped snapshot because nothing here is
	// coupled - reading idle and the partition a microsecond apart cannot produce an inconsistent pair.
	idle      atomic.Bool
	partition atomic.Pointer[PartitionFunc]
	logger    atomic.Pointer[slog.Logger]
	inst      atomic.Pointer[instruments]
	seams     atomic.Pointer[seamster.Seamster]

	// stealing is armed by a cycle that found nothing due in this replica's own residue class, and read by
	// the NEXT cycle's predicate. Atomic because SetStealAfter and a test may touch the pair from another
	// goroutine, not because the cycle path is concurrent - it is not.
	stealing atomic.Bool
	// lastTally is the total due-step count this replica's own residue class reported on the last scan,
	// against which the steal gate measures spare capacity.
	lastTally atomic.Int64
	// stealAfter is how many cycle periods a foreign step must have been due before this replica takes it.
	// Zero disables stealing outright, which is what an owner sets when it wants the residue class enforced
	// strictly.
	stealAfter atomic.Int32
	// turns counts successful cycles, monotonically. A COUNTER rather than a flag the reader clears: a
	// consuming getter would make any second caller - a metric, a test - silently swallow the evidence and
	// leave a healthy piston reading as stalled. Holding "since I last looked" is the reader's business.
	turns atomic.Uint64
}

// New returns a piston for one shard over an already-open database handle. The planner and cache are
// shared with this replica's other pistons and are not owned here.
func New(shard int, db *sequel.DB, plan *planner.Planner, cache *candidatecache.Cache) (*Piston, error) {
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
	p := &Piston{shard: shard, db: db, planner: plan, cache: cache}
	p.stealAfter.Store(defaultStealAfter)
	pipe, err := pipeline.New(shard, p, plan, cache)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.pipe = pipe
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
// Both are the same positive statement an empty plan makes, so both use the same two calls.
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
// before this replica selects it anyway - and only while its own class is empty. Zero or negative disables
// stealing, restoring strict residue partitioning.
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
// replica holding work of its own never steals at all.
func (p *Piston) SetStealAfter(periods int) {
	if periods < 0 {
		periods = 0
	}
	p.stealAfter.Store(int32(periods))
}

// StealAfter is the current steal grace in cycle periods; zero means stealing is off.
func (p *Piston) StealAfter() int { return int(p.stealAfter.Load()) }

// Liveness reports whether this piston is turning, for an owner that publishes the fact somewhere the
// fleet can see it: the count of cycles completed so far, whether one is running right now, and whether
// the piston is idling.
//
// A COUNTER rather than a "since you last asked" flag, and that is the load-bearing part. A consuming
// getter would create a contract - call it exactly once per publication, from exactly one caller - and any
// second caller, a metric or a test, would silently clear the evidence and leave a healthy piston reading
// as stalled. Holding the previous count is the reader's business, so this is a pure read that may be
// called any number of times.
//
// A cycle inside its QUERIES counts, because a scan can legitimately run for tens of seconds on a deep
// backlog where the executor cannot early-stop, and a piston in the middle of one is plainly still serving.
// It is deliberately the queries and not the whole cycle: a cycle spends most of a healthy wall clock
// asleep in its pace, so counting that would make busy permanently true and turn this into "the loop is
// alive" - which a piston whose every scan FAILS would satisfy just as well, keeping a residue class of
// steps it never selects. That is the exact stranding the evidence exists to prevent.
func (p *Piston) Liveness() (turns uint64, busy, idle bool) {
	// Busy means a cycle has been in its queries LONGER THAN ONE CYCLE PERIOD, not merely that one is. A
	// cycle that fails instantly is also briefly in its queries, and a reader sampling on its own clock
	// catches that often enough to keep a broken piston looking alive for good - the exact stranding this
	// evidence exists to prevent. A scan that outruns the period is the case busy is for, and a scan that
	// does not will have completed and advanced the turn count long before any reader looks.
	//
	// FLOORED AT MinGap, because the period can legitimately be zero - a bench sweep measuring the unlimited
	// arm pins it there, and so does any caller driving cycles by hand - and a zero threshold is the bool
	// this predicate exists to replace. MinGap is already this package's fuse for that same degenerate
	// regime, so it is the floor that already means "shorter than this is not a cycle worth pacing".
	return p.turns.Load(), p.pipe.WorkingFor() > max(p.Interval(), pipeline.DefaultMinGap), p.idle.Load()
}

// SetInterval sets the cycle period, start of one cycle's scan to the next. See pipeline.SetInterval.
func (p *Piston) SetInterval(d time.Duration) { p.pipe.SetInterval(d) }

// Interval is the current cycle period.
func (p *Piston) Interval() time.Duration { return p.pipe.Interval() }

// SetMinGap sets the minimum quiet time between cycles. See pipeline.SetMinGap.
func (p *Piston) SetMinGap(d time.Duration) { p.pipe.SetMinGap(d) }

// MinGap is the current minimum quiet time between cycles.
func (p *Piston) MinGap() time.Duration { return p.pipe.MinGap() }

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
		cycleDuration: hist("dwarf_refill_duration_seconds",
			"Wall-clock duration of one shard's complete refill cycle, excluding the pace it slept beforehand."),
		queryDuration: hist("dwarf_refill_query_duration_seconds",
			"Duration of one phase of one shard's refill cycle, labelled by shard and by phase: the two queries "+
				"(band_keys, fetch_steps) and the two in-memory phases (planning, pushing)."),
		selected: ctr("dwarf_refill_candidates_selected",
			"Counts step candidates the refiller selected into the cache. Compare against dwarf_refill_candidates_discarded for the oversupply ratio."),
		discarded: ctr("dwarf_refill_candidates_discarded",
			"Counts cached step candidates thrown away un-popped by a wholesale refill - the refiller's waste signal. The steps stay pending and are re-selected, so this is cost, not loss."),
		stolen: ctr("dwarf_steps_stolen",
			"Counts candidates this replica selected from OUTSIDE its own residue class, because its own class was empty and the step had been due for several cycle periods - i.e. its owner was not taking it. Zero in a healthy fleet by construction: a replica holding its own work never steals. A sustained nonzero rate names a peer that is alive in the registry but not serving its share, which nothing else reports - and it is the quantity to read dwarf_steps_claim_lost against, since stealing trades exclusivity for coverage."),
	})
	if len(errs) > 0 {
		return errors.Trace(errs[0])
	}
	return nil
}

// Run drives the piston until ctx is cancelled. It blocks; a caller runs it in a goroutine and waits on
// its own WaitGroup.
//
// It publishes nothing about this replica's liveness itself - Liveness is a pure read an owner samples on
// its own cadence, which is what keeps how often that is published independent of how long a cycle takes.
// The independence is a correctness requirement rather than tidiness: phase one's `rn <= capacity` cut
// early-stops only on Postgres 15+, so on MySQL/SQL Server/SQLite a deep backlog is still O(backlog),
// measured in the tens of seconds at a few million due rows. A liveness signal gated on a cycle RETURNING
// would let one such scan drop a perfectly healthy replica out of its own fleet - which should mean "the
// process is stuck", nothing less.
func (p *Piston) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if p.idle.Load() {
			// An idle piston runs no cycle, so its turn count never moves and Liveness reports it idle -
			// which is how a reader tells the two populations apart without trusting anything the replica
			// says about itself. Re-checked on the idle poll so a live SetIdle(false) resumes dispatching
			// within one interval.
			if !p.sleep(ctx, idlePoll) {
				return
			}
			continue
		}
		r := p.Cycle(ctx)
		if r.Err != nil && ctx.Err() == nil {
			p.logger.Load().ErrorContext(ctx, "Refill cycle", "shard", p.shard, "error", r.Err)
		}
		if r.Err == nil {
			// This shard's partition now reflects the plan - see CheckpointCycleDone. Gated on the error so
			// a visit cannot mean "looked and gave up": neither error path pushed, so neither reconciled.
			if seams := p.seams.Load(); seams.Enabled() { // Enabled gates the assembled name in production
				seams.Checkpoint(ctx, CheckpointCycleDone)
				seams.Checkpoint(ctx, seamsJoin(CheckpointCycleDone, strconv.Itoa(p.shard)))
			}
		}
	}
}

// Cycle runs exactly ONE supply cycle - paced as always - records it, and returns what happened. Run is
// this in a loop; a caller drives it directly when it wants a cycle at a moment of its own choosing rather
// than on the piston's cadence.
//
// NOT safe to call concurrently with Run, or with itself: the pipeline's cadence timestamps are
// single-goroutine state. A caller that drives cycles by hand should idle the piston first.
func (p *Piston) Cycle(ctx context.Context) pipeline.Result {
	r := p.pipe.Cycle(ctx)
	p.record(ctx, r)
	// A cycle that found nothing due still counts: it proves this piston looked and could have served.
	// Gating on candidates instead would make a quiet fleet look like it had no dispatchers at all.
	if r.Err == nil {
		p.turns.Add(1)
	}
	// Arm or disarm the steal from what this cycle SAW, for the next one to act on - see stealGrace.
	//
	// SHORTFALL, not emptiness. An earlier cut armed on Band == NoBand - nothing due in this replica's own
	// class at all - and that is too strict for any workload with CONTINUOUS arrivals: a healthy replica
	// keeping up with its share still finds a step or two due on almost every scan, so the gate never armed
	// while its peer's class backed up unboundedly beside it. Measured against a 50 flows/s open-loop bench
	// with one crippled peer: zero steals, and the fleet stayed at the same ~177 steps/s the unfixed build
	// managed. Emptiness only looked sufficient because a BURST workload drains a class to nothing.
	//
	// What matters is whether this replica could serve more than its class is offering, which is exactly the
	// tally against the batch it was about to fill. It is a property of the database backlog rather than of
	// anything this piston did, so it stays true while a peer is stalled and clears itself the moment this
	// replica's own class can fill its own batch again - which is also why the one-cycle lag is harmless
	// and, being hysteresis, mildly useful.
	//
	// A FAILED cycle leaves the flag alone. An error means "unknown", not "nothing is due" - the same
	// distinction the pipeline draws when it clears the shard from planning but spares its cache partition.
	//
	// While stealing, the scan the gate reads is itself the RELAXED one, so the tally counts what this
	// replica can now see rather than what its own class holds - and a replica filling its batch by stealing
	// therefore disarms, scans strictly next cycle, finds the shortfall again and re-arms. That alternation
	// is deliberate rather than tolerated: it steals on every other cycle while re-checking its own class in
	// between, so recovery is noticed within one period and no separate re-probe is needed. Reading the strict
	// tally instead would need a second query per cycle to learn a fact that costs nothing to rediscover.
	if r.Err == nil {
		p.stealing.Store(p.lastTally.Load() < int64(p.cache.Capacity()))
	}
	return r
}

// record translates a cycle's result into this piston's instruments.
func (p *Piston) record(ctx context.Context, r pipeline.Result) {
	in := p.inst.Load()
	shardAttr := attribute.Int("shard", p.shard)
	// A FAILED cycle is deliberately excluded from the end-to-end histogram, while the per-phase durations
	// below still record. A cycle that returned early on a scan or fetch error is a truncated one with no
	// meaningful total, and folding it in would drag the percentiles toward an error path that is already
	// logged; the same goes for the ~0 sample a cancellation during the pace produces, one per shard per
	// shutdown. The phase timings are worth keeping either way - they show WHICH phase was slow on the way
	// to failing.
	if r.Err == nil {
		in.cycleDuration.Record(ctx, r.Total.Seconds(), metric.WithAttributes(shardAttr))
	}
	if r.Tallying > 0 {
		in.queryDuration.Record(ctx, r.Tallying.Seconds(),
			metric.WithAttributes(shardAttr, attribute.String("phase", "band_keys")))
	}
	if r.Fetching > 0 {
		in.queryDuration.Record(ctx, r.Fetching.Seconds(),
			metric.WithAttributes(shardAttr, attribute.String("phase", "fetch_steps")))
	}
	// The two non-query phases are recorded on the same histogram. The instrument's name says "query" for
	// historical reasons - it predates them - and renaming it would break the dashboards it is a public
	// surface for, so the phase label carries the distinction instead. Recording them matters because
	// planning is the one cost in the design that scales with fairness-key CARDINALITY (the lottery re-rolls
	// per slot over every key), and left unrecorded it was visible only as cycleDuration minus the two query
	// phases - which is to say, not measurable at all.
	if r.Planning > 0 {
		in.queryDuration.Record(ctx, r.Planning.Seconds(),
			metric.WithAttributes(shardAttr, attribute.String("phase", "planning")))
	}
	if r.Pushing > 0 {
		in.queryDuration.Record(ctx, r.Pushing.Seconds(),
			metric.WithAttributes(shardAttr, attribute.String("phase", "pushing")))
	}
	if r.Selected > 0 {
		in.selected.Add(ctx, int64(r.Selected), metric.WithAttributes(shardAttr))
	}
	if r.Discarded > 0 {
		in.discarded.Add(ctx, int64(r.Discarded), metric.WithAttributes(shardAttr))
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
func (p *Piston) partitionPredicate() (string, []any) {
	fn := p.partition.Load()
	if fn == nil {
		return "", nil
	}
	replicas, ordinal, ok := (*fn)()
	if !ok || replicas < 2 || ordinal < 0 || ordinal >= replicas {
		return "", nil
	}
	if grace := p.stealGrace(); grace > 0 {
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

// stealGrace is how long a step outside this replica's residue class must have been DUE before this
// replica will select it anyway, or zero when stealing is off.
//
// TWO conditions, and each covers the other's blind spot - neither alone is sufficient:
//
//   - THE GATE (armed here): the last cycle found nothing due in this replica's OWN class. A replica with
//     its own work never steals, so a uniformly loaded fleet - every class full - adds no overlap at all,
//     whatever the grace is. This is the fuse, and it is self-limiting by construction: a replica that
//     steals is one that had nothing else to do, so the worst case is a lost claim round trip it was not
//     going to spend anyway.
//   - THE GRACE: at MODERATE load every replica can have spare capacity while the fleet is perfectly
//     healthy, and there the gate alone would re-enable overlapping selection fleet-wide. A healthy owner
//     dispatches its own class within a cycle or two, so requiring several cycles of due-age leaves its
//     work alone; a stalled owner's class ages without bound (measured: 23-41s against a ~67ms cycle,
//     while a healthy fleet's oldest due step sits at 0-1s). The two regimes are three orders of magnitude
//     apart, which is why the exact multiple is not delicate.
//
// An ABSOLUTE age threshold was rejected for this: normal queueing delay under load is seconds, so any
// constant either never fires or disables partitioning under exactly the load it exists for. The gate is
// what makes an age term usable at all, by restricting it to replicas that are already idle.
func (p *Piston) stealGrace() time.Duration {
	if !p.stealing.Load() {
		return 0
	}
	n := p.stealAfter.Load()
	if n <= 0 {
		return 0
	}
	// Floored at the CONSTANT DefaultMinGap, not at the configured MinGap, and that distinction is the point:
	// a caller may legitimately pin both the interval and the gap to zero (a bench sweep, a hand-driven
	// cycle), and deriving the floor from a configurable that can itself be zeroed yields a zero grace -
	// which would steal every foreign step the instant it came due, with no fuse at all. Same degenerate
	// regime MinGap exists to fuse, reached through the one door MinGap cannot cover.
	period := max(p.pipe.Interval(), p.pipe.MinGap(), pipeline.DefaultMinGap)
	return time.Duration(n) * period
}

// ScanBand implements pipeline.Source. It returns this shard's minimum due priority band and one
// aggregate row per fairness key at that band - O(distinct keys), never O(backlog).
//
// The per-key count is CAPPED at the cache capacity rather than exact: it is MAX(rn) under an
// `rn <= capacity` cut, not COUNT(*) OVER. The cap is lossless, since no key can be assigned more than
// the whole batch, and it is what lets the scan stop touching a key's rows past capacity instead of
// counting the whole partition - the O(backlog) cost that made a single-key flood scan millions of rows
// every cycle.
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
	part, partArgs := p.partitionPredicate()
	args := make([]any, 0, len(partArgs)+1)
	args = append(args, partArgs...)
	args = append(args, p.cache.Capacity())
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
	// Recorded for the steal gate, which asks whether this replica's own class can fill the batch it is
	// about to plan. Set here rather than derived by the caller because this is the only place the tallies
	// and the scan that produced them are both in hand.
	total := int64(0)
	for _, t := range tallies {
		total += int64(t.Count)
	}
	p.lastTally.Store(total)
	return band, tallies, nil
}

// FetchSteps implements pipeline.Source. It loads, per chosen fairness key, up to perKey of this shard's
// oldest due steps at the given band, keyed and ordered oldest-first - the order the plan replay expects.
//
// perKey is a UNIFORM cap, the max per-key demand across this shard's slice rather than each key's exact
// demand, which keeps the fetch one IN-list query (an exact per-key cap would need a per-key
// VALUES/LATERAL join, non-trivial across four dialects). The cost is at most len(keys)*perKey rows, and
// both factors are bounded by the cache capacity - so the fetch is bounded by capacity^2 regardless of
// how many fairness keys exist. That independence from key cardinality is the whole point: at high
// cardinality perKey is ~1, so the fetch is ~capacity.
//
// The band is bound (priority=?), not re-mined from a MIN subquery: the plan committed to this band, and
// re-mining could pick a lower one that arrived between phases and mismatch the chosen keys. A bound
// priority does not defeat the selection index - only a bound status would, which is why status stays
// inlined.
func (p *Piston) FetchSteps(ctx context.Context, shard, band int, keys []string, perKey int) (map[string][]int, error) {
	if len(keys) == 0 || perKey <= 0 {
		return nil, nil
	}
	part, partArgs := p.partitionPredicate()
	args := make([]any, 0, len(keys)+len(partArgs)+2)
	args = append(args, band)
	for _, k := range keys {
		args = append(args, k)
	}
	args = append(args, partArgs...)
	args = append(args, perKey)
	placeholders := strings.Repeat("?,", len(keys)-1) + "?"
	// rn IS the oldest-first ordinal - the window already ranks each key by (created_at, step_id) - so
	// ordering by it makes the result exactly right BY CONSTRUCTION. This used to select
	// DATE_DIFF_MILLIS(NOW_UTC(), created_at) instead and re-sort each key in Go by age descending with a
	// step_id tiebreak, which agrees only incidentally: age is the same ordering read backwards through
	// millisecond-truncated arithmetic, so it lands on the step_id tiebreak for anything created inside one
	// millisecond. Ordering on rn drops the per-row date arithmetic, the nullable scan, the intermediate
	// struct and the sort. The age was a leftover from the engine's version, where it fed a cross-shard
	// merge that does not exist here - the planner has already assigned the slots.
	rows, err := p.db.QueryContext(ctx,
		"SELECT step_id, fairness_key FROM ("+
			"SELECT step_id, fairness_key,"+
			" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn"+
			" FROM dwarf_steps"+
			" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
			" AND priority=? AND fairness_key IN ("+placeholders+")"+
			part+
			") t WHERE rn<=? ORDER BY fairness_key, rn",
		args...,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	out := map[string][]int{}
	// Count what came from OUTSIDE this replica's class. The pair is re-read rather than threaded down from
	// partitionPredicate because it is a lock-free load either way, and reading it here keeps the count
	// honest if the fleet changed mid-cycle. Zero unless stealing armed and actually took something, so a
	// healthy fleet records nothing at all.
	replicas, ordinal := 0, 0
	if fn := p.partition.Load(); fn != nil {
		if r, o, ok := (*fn)(); ok && r > 1 {
			replicas, ordinal = r, o
		}
	}
	stolen := 0
	for rows.Next() {
		var stepID int
		var key string
		if err := rows.Scan(&stepID, &key); err != nil {
			return nil, errors.Trace(err)
		}
		if replicas > 1 && stepID%replicas != ordinal {
			stolen++
		}
		out[key] = append(out[key], stepID)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	if stolen > 0 {
		p.inst.Load().stolen.Add(ctx, int64(stolen), metric.WithAttributes(attribute.Int("shard", p.shard)))
		if seams := p.seams.Load(); seams.Enabled() {
			seams.Checkpoint(ctx, CheckpointStole)
			seams.Checkpoint(ctx, seamsJoin(CheckpointStole, strconv.Itoa(p.shard)))
		}
	}
	return out, nil
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
