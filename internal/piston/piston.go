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
// this replica's heartbeat into the shard's peer registry. What it borrows is the planner and the
// candidate cache, both shared with every other piston on this replica.
//
// Run blocks and does the whole job:
//
//	cycle (paced by the pipeline) -> record -> heartbeat if due -> repeat
//
// An IDLE piston skips the cycle entirely and only heartbeats. That is the await-only replica: it holds
// connections, so it must stay in the registry and keep dividing the pools, but it claims no work, so it
// must not be counted among the dispatchers - a replica handed a residue class of candidates it never
// selects strands them. One setter drives both halves so the two can never disagree.
package piston

import (
	"context"
	"database/sql"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/internal/pipeline"
	"github.com/microbus-io/dwarf/internal/planner"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

// heartbeatInterval is how often a piston refreshes this replica's row in its shard's peer registry. A
// var rather than a const only so a test can shorten it.
//
// One second is not merely cheaper than beating every cycle - it keeps the write inside the premise its
// UPDATE-then-INSERT fallback rests on. The fallback reads RowsAffected to decide whether a row existed,
// and MySQL counts CHANGED rows rather than matched ones, so two beats landing inside the same
// NOW_UTC() tick (millisecond precision) would report zero and fire a spurious INSERT. At a second's
// spacing that cannot happen; at a per-cycle cadence the margin is one millisecond.
var heartbeatInterval = time.Second

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
	partition atomic.Pointer[PartitionFunc]
	logger    atomic.Pointer[slog.Logger]
	inst      atomic.Pointer[instruments]

	// Beat state, touched only by Run. dispatchedSinceBeat is sticky: any successful cycle sets it and the
	// beat that publishes it clears it. Sticky rather than "did the last cycle succeed" because beats are
	// ~20x rarer than cycles, so sampling the single cycle that happens to precede one would let a healthy
	// piston look stalled whenever a transient error landed in that gap.
	lastBeat            time.Time
	dispatchedSinceBeat bool
}

// New returns a piston for one shard over an already-open database handle. The planner and cache are
// shared with this replica's other pistons and are not owned here.
//
// engineID leads because it is required rather than optional: it is the peer registry's PRIMARY KEY, so
// an unset one is not a harmless default - every unconfigured replica in the fleet would collide on id 0
// and fight over a single row. A setter would make that state reachable; a constructor argument does not.
func New(engineID int64, shard int, db *sequel.DB, plan *planner.Planner, cache *candidatecache.Cache) (*Piston, error) {
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
	pipe, err := pipeline.New(shard, p, plan, cache)
	if err != nil {
		return nil, errors.Trace(err)
	}
	p.pipe = pipe
	p.SetLogger(nil)
	p.SetMeter(nil)
	return p, nil
}

// EngineID is this replica's identity in the peer registry.
func (p *Piston) EngineID() int64 { return p.engineID }

// Shard is the shard this piston works.
func (p *Piston) Shard() int { return p.shard }

// SetIdle puts the piston in or out of idle. Idling still turns over - it heartbeats, so the replica
// keeps its registry row and keeps dividing the connection pools - but it runs no cycle and claims no
// work, and its registry row says so (dispatches=0) so peers exclude it from the candidate partition.
//
// The default is NOT idle: a fresh piston dispatches, which is the common case, and a zero value that
// silently did nothing would be the worse default.
//
// "Idle" here is a configured MODE, distinct from the refill sense of the word (nothing is due), which
// is a circumstance a dispatching piston meets all the time.
func (p *Piston) SetIdle(idle bool) { p.idle.Store(idle) }

// Idle reports whether the piston is idling.
func (p *Piston) Idle() bool { return p.idle.Load() }

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
		// Sub-millisecond through several seconds: a warm index probe at one end, a deep-backlog scan at
		// the other.
		h, err := m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(
				0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5))
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
			"Duration of one shard's refill query, labelled by shard and by phase (band_keys, fetch_steps)."),
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
// The heartbeat is deliberately NOT gated on the cycle succeeding. A shard whose scans are failing is
// still a live replica holding connections, and dropping out of its registry over a database blip would
// have peers recount R, resize pools, and reshuffle ordinals for no reason. "This piston stopped
// beating" should mean the process is stuck, nothing less.
func (p *Piston) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if p.idle.Load() {
			// Nothing to pace against, so the heartbeat sets the cadence itself. An idle piston runs no
			// cycle, so it never advances dispatched_at - which is precisely how a reader tells the two
			// populations apart without trusting anything the replica claims about itself.
			p.beat(ctx)
			if !p.sleep(ctx, heartbeatInterval) {
				return
			}
			continue
		}
		r := p.pipe.Cycle(ctx)
		p.record(ctx, r)
		if r.Err != nil && ctx.Err() == nil {
			p.logger.Load().ErrorContext(ctx, "Refill cycle", "shard", p.shard, "error", r.Err)
		}
		// A cycle that found nothing due still counts: it proves this piston looked and could have served.
		// Gating on candidates instead would make a quiet fleet look like it had no dispatchers at all.
		if r.Err == nil {
			p.dispatchedSinceBeat = true
		}
		if time.Since(p.lastBeat) >= heartbeatInterval {
			p.beat(ctx)
		}
	}
}

// record translates a cycle's result into this piston's instruments.
func (p *Piston) record(ctx context.Context, r pipeline.Result) {
	in := p.inst.Load()
	shardAttr := attribute.Int("shard", p.shard)
	in.cycleDuration.Record(ctx, r.Total.Seconds(), metric.WithAttributes(shardAttr))
	if r.Tallying > 0 {
		in.queryDuration.Record(ctx, r.Tallying.Seconds(),
			metric.WithAttributes(shardAttr, attribute.String("phase", "band_keys")))
	}
	if r.Fetching > 0 {
		in.queryDuration.Record(ctx, r.Fetching.Seconds(),
			metric.WithAttributes(shardAttr, attribute.String("phase", "fetch_steps")))
	}
	if r.Selected > 0 {
		in.selected.Add(ctx, int64(r.Selected), metric.WithAttributes(shardAttr))
	}
	if r.Discarded > 0 {
		in.discarded.Add(ctx, int64(r.Discarded), metric.WithAttributes(shardAttr))
	}
}

// beat refreshes this replica's row in the shard's peer registry: an UPDATE by engine_id, falling back
// to an INSERT when no row matched (the first beat, or after the row was pruned). Two statements rather
// than a per-dialect upsert, so it stays dialect-agnostic. Timestamps come from the database clock
// (NOW_UTC()), never a bound Go time, so every freshness comparison on this shard runs on one clock.
//
// TWO timestamps, and the distinction is the point. seen_at moves on every beat and means "this replica
// is alive and holding connections" - what the pool divisor R counts. dispatched_at moves only when a
// cycle has actually completed since the last beat, and means "this replica is genuinely serving work" -
// what the candidate partition must divide across. A replica handed a residue class of step ids it never
// selects strands them, so that divisor needs EVIDENCE rather than a claim: a piston that says it
// dispatches and then wedges stops advancing dispatched_at and falls out of the divisor on its own,
// while its seen_at keeps it in R where it belongs. dispatches carries the same fact as a flag for the
// benefit of readers that have not moved to the timestamp yet.
//
// lastBeat advances even on failure. Retrying a broken registry write every cycle would turn a database
// blip into a write storm, and the next beat is a second away regardless.
func (p *Piston) beat(ctx context.Context) {
	p.lastBeat = time.Now()
	idle := p.idle.Load()
	dispatched := p.dispatchedSinceBeat && !idle
	p.dispatchedSinceBeat = false
	dispatches := 1
	if idle {
		dispatches = 0
	}
	// The dispatched_at assignment is composed into the statement rather than bound through a CASE: a
	// conditional assignment is two plain statement shapes here, where CASE WHEN ? would lean on how each
	// driver binds a boolean into an expression.
	set := "seen_at=NOW_UTC(), dispatches=?"
	if dispatched {
		set += ", dispatched_at=NOW_UTC()"
	}
	res, err := p.db.ExecContext(ctx,
		"UPDATE dwarf_peers SET "+set+" WHERE engine_id=?", dispatches, p.engineID)
	if err == nil {
		var n int64
		if n, err = res.RowsAffected(); err == nil {
			if n > 0 {
				return
			}
			// On the insert path dispatched_at is left to its default - a timestamp far enough in the past
			// that a brand-new row reads as "never dispatched" until it earns otherwise.
			cols, vals := "engine_id, seen_at, dispatches", "?, NOW_UTC(), ?"
			if dispatched {
				cols, vals = cols+", dispatched_at", vals+", NOW_UTC()"
			}
			_, err = p.db.ExecContext(ctx,
				"INSERT INTO dwarf_peers ("+cols+") VALUES ("+vals+")", p.engineID, dispatches)
		}
	}
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
func (p *Piston) partitionPredicate() (string, []any) {
	fn := p.partition.Load()
	if fn == nil {
		return "", nil
	}
	replicas, ordinal, ok := (*fn)()
	if !ok {
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
	rows, err := p.db.QueryContext(ctx,
		"SELECT step_id, fairness_key, age_ms FROM ("+
			"SELECT step_id, fairness_key,"+
			" DATE_DIFF_MILLIS(NOW_UTC(), created_at) AS age_ms,"+
			" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn"+
			" FROM dwarf_steps"+
			" WHERE status='"+workflow.StatusPending+"' AND parked=0 AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
			" AND priority=? AND fairness_key IN ("+placeholders+")"+
			part+
			") t WHERE rn<=? ORDER BY step_id",
		args...,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	type fetched struct {
		stepID int
		ageMs  float64
	}
	byKey := map[string][]fetched{}
	for rows.Next() {
		var f fetched
		var key string
		var age sql.NullFloat64
		if err := rows.Scan(&f.stepID, &key, &age); err != nil {
			return nil, errors.Trace(err)
		}
		f.ageMs = age.Float64
		byKey[key] = append(byKey[key], f)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	out := make(map[string][]int, len(byKey))
	for key, list := range byKey {
		// Oldest first: age desc, then step_id so equal ages order deterministically.
		sort.Slice(list, func(a, b int) bool {
			if list[a].ageMs != list[b].ageMs {
				return list[a].ageMs > list[b].ageMs
			}
			return list[a].stepID < list[b].stepID
		})
		ids := make([]int, 0, len(list))
		for _, f := range list {
			ids = append(ids, f.stepID)
		}
		out[key] = ids
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
