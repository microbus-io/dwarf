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

// Package piston drives one shard: it is the engine's per-shard cylinder, firing the same supply cycle
// over and over against its own database, on its own clock, with no barrier against its peers. A fleet
// of shards is a fleet of pistons.
//
// It is a CONSUMER of its database, never the owner. The handle is passed in already open and is closed
// by whoever opened it, so a piston has no Open, no Close, and no say over pool sizes. That is also why
// it is not called a shard: the shard is the database partition, and this is the thing that works it.
//
// What it owns is the supply cycle - the pipeline, the two queries behind it, its own instruments - plus
// this replica's heartbeat into the shard's peer registry. The heartbeat REFRESHES that row and never
// creates it: the owner registers once at startup and deletes once at shutdown, so a beat can never
// resurrect a row the owner just removed. What it borrows is the planner and the candidate cache, both
// shared with every other piston on this replica.
//
// Run blocks and drives two independent loops - the supply cycle, and this replica's heartbeat on its own
// fixed cadence:
//
//	cycle (paced by the pipeline) -> record -> repeat
//	beat -> sleep -> repeat
//
// They are separate goroutines because a beat behind a cycle is a beat gated on that cycle RETURNING, and
// a deep-backlog scan can run for tens of seconds on a dialect without the early-stop - long enough to
// drop a healthy replica out of the fleet's roster.
//
// An IDLE piston skips the cycle entirely and only heartbeats. That is the await-only replica: it holds
// connections, so it must stay in the registry and keep dividing the pools, but it claims no work, so it
// must not be counted among the dispatchers - a replica handed a residue class of candidates it never
// selects strands them. Going idle also withdraws the shard from the shared planner and empties its cache
// partition, since a piston that has stopped reporting must not leave a stale band claim behind.
package piston

import (
	"context"
	"log/slog"
	"strings"
	"sync"
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

// heartbeatInterval is the DEFAULT cadence at which a piston refreshes this replica's row in its shard's
// peer registry; SetHeartbeatInterval overrides it per piston. A var rather than a const only so a test
// can shorten the default.
//
// One second is simply cheap enough against a freshness window measured in tens of seconds. It used to be
// load-bearing for a second reason - the beat was an UPDATE with an INSERT fallback, which read
// RowsAffected to decide whether a row existed, and MySQL counts CHANGED rather than matched rows, so two
// beats inside one NOW_UTC() millisecond would report zero and fire a spurious INSERT. The beat no longer
// inserts (see beat), so that constraint is gone and this is a cost knob again.
var heartbeatInterval = time.Second

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

// FaultScanErr makes ScanBand fail without touching the database - see SetSeams. The name is exported so
// the owning application's fault catalogue can alias it rather than re-spell the string.
const FaultScanErr = "refillScanErr"

// PartitionFunc reports the replica partition - see SetPartitionFunc.
type PartitionFunc func() (replicas, ordinal int, ok bool)

// instruments is this piston's metric set, swapped atomically as a group so a recording cycle never sees
// half of one meter and half of another.
type instruments struct {
	cycleDuration metric.Float64Histogram
	queryDuration metric.Float64Histogram
	selected      metric.Int64Counter
	discarded     metric.Int64Counter
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

	// engineID identifies this replica in the peer registry. Immutable: it is the registry's primary key,
	// and a replica that changed it mid-life would leave a ghost row behind.
	engineID int64

	// Live configuration, each independently atomic. There is no grouped snapshot because nothing here is
	// coupled - reading idle and the partition a microsecond apart cannot produce an inconsistent pair.
	idle      atomic.Bool
	beatEvery atomic.Int64 // nanoseconds; see SetHeartbeatInterval
	partition atomic.Pointer[PartitionFunc]
	logger    atomic.Pointer[slog.Logger]
	inst      atomic.Pointer[instruments]
	seams     atomic.Pointer[seamster.Seamster]

	// dispatchedSinceBeat is sticky: any successful cycle sets it and the beat that publishes it clears it.
	// Sticky rather than "did the last cycle succeed" because beats are ~20x rarer than cycles, so sampling
	// the single cycle that happens to precede one would let a healthy piston look stalled whenever a
	// transient error landed in that gap. Atomic because the cycle loop sets it and the beat loop - a
	// separate goroutine, see Run - consumes it.
	dispatchedSinceBeat atomic.Bool
	// cycleInFlight is true while the cycle loop is inside pipe.Cycle. A cycle that has not RETURNED yet is
	// still evidence this piston is serving - and on a dialect without the run-condition early-stop a deep
	// backlog scan can run for tens of seconds, far longer than a reader's dispatch window. Without this a
	// loaded fleet's healthy replicas would all stop advancing dispatched_at at once and fall out of the
	// partition divisor exactly when overlapping selection costs the most.
	cycleInFlight atomic.Bool
}

// New returns a piston for one shard over an already-open database handle. The planner and cache are
// shared with this replica's other pistons and are not owned here.
//
// engineID leads because it is required rather than optional: it is the peer registry's PRIMARY KEY, so
// an unset one is not a harmless default - every unconfigured replica in the fleet would collide on id 0
// and fight over a single row. A setter would make that state reachable; a constructor argument does not.
func New(engineID int64, shard int, db *sequel.DB, plan *planner.Planner, cache *candidatecache.Cache) (*Piston, error) {
	// Rejected rather than defaulted, for the reason the argument exists at all: engine_id is the registry's
	// PRIMARY KEY, so zero is not a harmless placeholder but a value every unconfigured replica in the fleet
	// would collide on, fighting over one row.
	if engineID <= 0 {
		return nil, errors.New("engine id must be positive, got %d", engineID)
	}
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
	p := &Piston{engineID: engineID, shard: shard, db: db, planner: plan, cache: cache}
	p.beatEvery.Store(int64(heartbeatInterval))
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

// EngineID is this replica's identity in the peer registry.
func (p *Piston) EngineID() int64 { return p.engineID }

// Shard is the shard this piston works.
func (p *Piston) Shard() int { return p.shard }

// SetIdle puts the piston in or out of idle. Idling still turns over - it heartbeats, so the replica
// keeps its registry row and keeps dividing the connection pools - but it runs no cycle and claims no
// work - so it never advances dispatched_at, which is how peers exclude it from the candidate partition.
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

// SetHeartbeatInterval sets how often this piston refreshes the registry row. Zero or negative restores
// the default.
//
// THE OWNER MUST KEEP THIS WELL UNDER THE FRESHNESS WINDOW ITS READERS APPLY, and the two are set in
// different places, so the coupling is easy to break silently: readers decide a peer is gone when its
// seen_at ages past their window, and the only thing refreshing that column is this beat. Beat slower than
// the window and a perfectly healthy replica ages out of its own fleet - including out of R, which
// re-expands every peer's connection pools.
func (p *Piston) SetHeartbeatInterval(d time.Duration) {
	if d <= 0 {
		d = heartbeatInterval
	}
	p.beatEvery.Store(int64(d))
}

// HeartbeatInterval is the current registry-refresh cadence.
func (p *Piston) HeartbeatInterval() time.Duration { return time.Duration(p.beatEvery.Load()) }

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
	})
	if len(errs) > 0 {
		return errors.Trace(errs[0])
	}
	return nil
}

// Run drives the piston until ctx is cancelled. It blocks; a caller runs it in a goroutine and waits on
// its own WaitGroup.
//
// The heartbeat runs on its OWN goroutine, and that is a correctness requirement rather than tidiness.
// Beating from the cycle loop gates the beat not merely on the cycle *succeeding* - which it must not be,
// see beat - but on the cycle *returning*, so an expensive scan postpones this replica's liveness signal
// by its whole duration. That is not hypothetical here: phase one's `rn <= capacity` cut early-stops only
// on Postgres 15+, so on MySQL/SQL Server/SQLite a deep backlog is still O(backlog), measured in the tens
// of seconds at a few million due rows. A scan that outruns the peer freshness window would drop a
// perfectly healthy replica out of R - shrinking the divisor fleet-wide - and out of the roster, which
// reshuffles every peer's ordinal and hands the residue classes around. Exactly the outcome that should
// mean "the process is stuck, nothing less."
//
// The two loops share only dispatchedSinceBeat, which is atomic, so nothing else needs synchronizing.
func (p *Piston) Run(ctx context.Context) {
	var beats sync.WaitGroup
	beats.Go(func() {
		p.beatLoop(ctx)
	})
	defer beats.Wait()
	// published tracks whether any beat has carried dispatch evidence yet. The beat loop fires once on
	// entry, BEFORE the first cycle can have completed, so that beat correctly reports none - leaving the
	// replica out of its own fleet's dispatcher count until the next one. Beating again the instant the
	// first cycle lands closes that to milliseconds instead of a whole heartbeat interval, which matters
	// because a replica absent from the divisor partitions nothing and selects everything, overlapping
	// every peer. Only the FIRST one is special-cased; after that the loop's cadence is the whole policy.
	published := false
	for {
		if ctx.Err() != nil {
			return
		}
		if p.idle.Load() {
			// An idle piston runs no cycle, so it never advances dispatched_at - which is precisely how a
			// reader tells the two populations apart without trusting anything the replica claims about
			// itself. The beat loop keeps its registry row alive meanwhile. Re-checked on the beat cadence
			// so a live SetIdle(false) resumes dispatching within one interval.
			if !p.sleep(ctx, p.HeartbeatInterval()) {
				return
			}
			continue
		}
		r := p.Cycle(ctx)
		if r.Err != nil && ctx.Err() == nil {
			p.logger.Load().ErrorContext(ctx, "Refill cycle", "shard", p.shard, "error", r.Err)
		}
		if !published && r.Err == nil {
			published = true
			p.beat(ctx)
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
	p.cycleInFlight.Store(true)
	r := p.pipe.Cycle(ctx)
	p.cycleInFlight.Store(false)
	p.record(ctx, r)
	// A cycle that found nothing due still counts: it proves this piston looked and could have served.
	// Gating on candidates instead would make a quiet fleet look like it had no dispatchers at all.
	if r.Err == nil {
		p.dispatchedSinceBeat.Store(true)
	}
	return r
}

// beatLoop refreshes this replica's registry row on a fixed cadence, independent of whatever the cycle
// loop is doing. It beats immediately on entry so a starting piston registers without waiting an interval.
func (p *Piston) beatLoop(ctx context.Context) {
	for {
		p.beat(ctx)
		if !p.sleep(ctx, p.HeartbeatInterval()) {
			return
		}
	}
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

// beat refreshes this replica's row in the shard's peer registry. Timestamps come from the database clock
// (NOW_UTC()), never a bound Go time, so every freshness comparison on this shard runs on one clock.
//
// It is an UPDATE and NOTHING ELSE - it never creates the row. The owner creates it at startup and deletes
// it at shutdown, so a straggler beat after that delete matches nothing instead of RESURRECTING the row.
// An INSERT fallback here (what this used to do) would make the shutdown delete stick only under an
// ordering constraint spanning two packages.
//
// TWO timestamps, and the distinction is evidence versus claim. seen_at moves on every beat: this replica
// is alive and holding connections, which is what the pool divisor R counts. dispatched_at moves only with
// a cycle behind it: this replica is genuinely serving, which is what the candidate partition divides
// across. A replica handed a residue class it never selects strands those steps, so that divisor cannot
// trust a flag - a piston that claims to dispatch and then wedges simply stops advancing dispatched_at.
//
// A failed write is not retried: that would turn a database blip into a write storm, and the next beat is
// an interval away regardless.
//
// Safe to call from either loop - everything it reads is atomic and the write is idempotent - which is
// what lets Run publish its first evidence early.
func (p *Piston) beat(ctx context.Context) {
	idle := p.idle.Load()
	// Either a cycle COMPLETED since the last beat, or one is running right now. Both are evidence; only a
	// piston that is neither finishing nor attempting cycles stops advancing dispatched_at, which is the
	// wedged case the column exists to catch.
	dispatched := (p.dispatchedSinceBeat.Swap(false) || p.cycleInFlight.Load()) && !idle
	// The dispatched_at assignment is composed into the statement rather than bound through a CASE: a
	// conditional assignment is two plain statement shapes here, where CASE WHEN ? would lean on how each
	// driver binds a boolean into an expression.
	set := "seen_at=NOW_UTC()"
	if dispatched {
		set += ", dispatched_at=NOW_UTC()"
	}
	_, err := p.db.ExecContext(ctx,
		"UPDATE dwarf_peers SET "+set+" WHERE engine_id=?", p.engineID)
	if err != nil && ctx.Err() == nil {
		p.logger.Load().ErrorContext(ctx, "Peer heartbeat", "shard", p.shard, "error", err)
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
func (p *Piston) partitionPredicate() (string, []any) {
	fn := p.partition.Load()
	if fn == nil {
		return "", nil
	}
	replicas, ordinal, ok := (*fn)()
	if !ok || replicas < 2 || ordinal < 0 || ordinal >= replicas {
		return "", nil
	}
	return " AND step_id % ? = ?", []any{replicas, ordinal}
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
	for rows.Next() {
		var stepID int
		var key string
		if err := rows.Scan(&stepID, &key); err != nil {
			return nil, errors.Trace(err)
		}
		out[key] = append(out[key], stepID)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
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
