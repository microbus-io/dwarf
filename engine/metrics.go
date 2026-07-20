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
	"database/sql"
	"strconv"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metricScope is the OpenTelemetry instrumentation scope for the engine's instruments. Its Resource
// (service identity) comes from the injected MeterProvider, not from here - the engine never stamps
// service.name/instance attributes (those are resource-level, owned by the host's OTEL pipeline).
const metricScope = "github.com/microbus-io/dwarf"

// Attribute cardinality and PII are the host's to bound via a metric View.

// engineMetrics holds the engine's OpenTelemetry instruments. The counters are incremented inline at
// their event sites; the gauges are observable (async) and pulled by observeGauges at collection time.
type engineMetrics struct {
	flowsStarted      metric.Int64Counter
	flowsTerminated   metric.Int64Counter
	stepsExecuted     metric.Int64Counter
	stepsRecovered    metric.Int64Counter
	stepsUnwedged     metric.Int64Counter
	stepsWriteRetried metric.Int64Counter
	stepsWriteFailed  metric.Int64Counter
	flowsOrphaned     metric.Int64Counter
	stateWriteBytes   metric.Int64Counter
	stateReadBytes    metric.Int64Counter
	refillSelected    metric.Int64Counter
	refillDiscarded   metric.Int64Counter

	refillDuration      metric.Float64Histogram
	refillQueryDuration metric.Float64Histogram

	reg metric.Registration // the observable-gauge callback registration, unregistered at Shutdown
}

// refillBuckets are the explicit bucket boundaries for the refill histograms, in SECONDS. The OTEL default
// boundaries are tuned for millisecond-valued instruments and would put every one of these samples in the
// first bucket. The span that matters runs from a warm same-zone index scan (~0.3ms) to a band scan whose
// plan flipped to a sequential scan (~100ms measured on identical data minutes apart, as statistics went
// stale) - so the boundaries must resolve both ends, and the tail must stay open past 1s to catch a shard
// that is genuinely sick rather than merely slow.
var refillBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
}

// initMetrics creates the engine's instruments from the resolved MeterProvider and registers the
// observable-gauge callback. Called from initRuntime. With no provider injected it falls back to the
// global otel.GetMeterProvider(), which is the no-op provider unless the host configures the SDK - so
// in unit tests (and unconfigured standalone use) the instruments are no-ops and the callback is never
// invoked, incurring no per-collection DB queries.
func (e *Engine) initMetrics() error {
	mp := e.meterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(metricScope)
	m := &engineMetrics{}

	var errs []error
	ctr := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return c
	}
	// Counter instrument names carry no _total suffix; the Prometheus exporter appends it at scrape time.
	// Unit-denominated counters end with the unit (…_bytes), per the Prometheus naming convention.
	m.flowsStarted = ctr("dwarf_flows_started", "Counts flows that have been started.")
	m.flowsTerminated = ctr("dwarf_flows_terminated", "Counts flows that have reached a terminal status.")
	m.stepsExecuted = ctr("dwarf_steps_executed", "Counts steps that have been executed.")
	m.stepsRecovered = ctr("dwarf_steps_recovered", "Counts steps recovered by pollPendingSteps after lease expiry.")
	m.stepsUnwedged = ctr("dwarf_steps_unwedged", "Counts parked steps recovered by the wedge sweep, labelled by park type. A nonzero value signals a latent bug whose effect the sweep papered over.")
	m.stepsWriteRetried = ctr("dwarf_steps_write_retried", "Counts in-place retries of a step's persistence write after a non-contention database error. The task is NOT re-executed; only the write is retried. A rising count tracks database flakiness, not workflow failure.")
	m.stepsWriteFailed = ctr("dwarf_steps_write_failed", "Counts steps terminalized because their outcome could not be persisted while the database was reachable - i.e. the payload, not the database, was the problem. A nonzero value signals a latent bug (an unstorable value, a column/packet limit, a constraint violation), like dwarf_steps_unwedged.")
	m.flowsOrphaned = ctr("dwarf_flows_orphaned", "Counts running flows detected as stranded by the orphan sweep - every step terminal, none touched within the threshold, no successor. A nonzero value signals a latent bug the processStep recovery defer could not cover, like dwarf_steps_unwedged; detection-only, the sweep does not re-drive the flow.")
	bytesCtr := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit("By"))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return c
	}
	// State byte throughput on the dispatch hot path only: rates here track against the database's
	// byte-throughput ceiling (disk/WAL), which binds separately from its steps ceiling. Reads/writes by
	// the introspection APIs (List/History/Snapshot) and flow-row writes (final_state) are not counted.
	m.stateWriteBytes = bytesCtr("dwarf_state_write_bytes", "Counts payload bytes written to step rows on the execution path, labelled by workflow and by column (state, changes, interrupt_payload).")
	m.stateReadBytes = bytesCtr("dwarf_state_read_bytes", "Counts payload bytes read from step rows on the execution path, labelled by workflow and by column (state, changes, resume_data, subgraph_result).")

	// Refiller instruments. The refiller is triggered after every processStep and fans its two scan phases
	// across all shards with OnEach, which returns only when the SLOWEST shard does - so a single slow shard
	// gates dispatch for the whole replica. These four make that visible: the per-shard query histogram
	// isolates the straggler (and the index-scan/seq-scan plan flip), the whole-pass histogram against the
	// per-shard max prices the straggler wait, and selected-vs-discarded prices the oversupply.
	m.refillSelected = ctr("dwarf_refill_candidates_selected", "Counts step candidates the refiller selected into the cache. Compare against dwarf_refill_candidates_discarded for the refiller's oversupply ratio.")
	m.refillDiscarded = ctr("dwarf_refill_candidates_discarded", "Counts cached step candidates thrown away un-popped by a wholesale refill - the refiller's waste signal. The steps stay pending and are re-selected, so this is cost, not loss; a ratio near 1 against dwarf_refill_candidates_selected means the refiller is turning far faster than the workers drain.")

	secHist := func(name, desc string) metric.Float64Histogram {
		h, err := meter.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(refillBuckets...))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return h
	}
	m.refillDuration = secHist("dwarf_refill_duration_seconds", "Wall-clock duration of a complete refiller pass, both scan phases across every shard. Exceeds the per-shard max of dwarf_refill_query_duration_seconds by the cross-shard straggler wait.")
	m.refillQueryDuration = secHist("dwarf_refill_query_duration_seconds", "Duration of one shard's refiller scan query, labelled by shard and by phase (band_keys, fetch_steps).")

	gauge := func(name, desc, unit string) metric.Int64ObservableGauge {
		g, err := meter.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return g
	}
	// The five gauges split into two kinds, and the difference is NOT cosmetic - it decides how a dashboard
	// must aggregate them across replicas:
	//   - PER-REPLICA (read from this replica's memory): queueDepth, fairnessKeys. Sum across replicas.
	//   - CLUSTER-WIDE (read by querying the SHARED shard databases): stepsPending, oldestAge, concurrency.
	//     Every replica observes the SAME number, so summing them multiplies by the replica count - a
	//     1,000-step backlog reads as 3,000 on three replicas. Aggregate with max (or avg), never sum.
	// Each description says so, because that is where an operator building a panel actually reads it.
	queueDepth := gauge("dwarf_steps_queue_depth", "Per-replica: steps waiting in this replica's local worker cache. Sum across replicas.", "")
	stepsPending := gauge("dwarf_steps_pending", "Cluster-wide: due pending steps in each priority band, read from the shared database. Every replica reports the same value - aggregate with max, not sum.", "")
	oldestAge := gauge("dwarf_steps_oldest_pending_age_seconds", "Cluster-wide: age of the oldest due pending step in each priority band, read from the shared database. Every replica reports the same value - aggregate with max, not sum.", "s")
	fairnessKeys := gauge("dwarf_steps_fairness_keys", "Per-replica: distinct fairness keys in this replica's most recent refill selection at the given priority band.", "")
	concurrency := gauge("dwarf_task_concurrency_running", "Cluster-wide: running steps per task, read from the shared database. Every replica reports the same value - aggregate with max, not sum.", "")

	reg, err := meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			return e.observeGauges(ctx, o, observableGauges{
				queueDepth:   queueDepth,
				stepsPending: stepsPending,
				oldestAge:    oldestAge,
				fairnessKeys: fairnessKeys,
				concurrency:  concurrency,
			})
		},
		queueDepth, stepsPending, oldestAge, fairnessKeys, concurrency,
	)
	if err != nil {
		errs = append(errs, errors.Trace(err))
	}
	m.reg = reg

	e.metrics = m
	return errors.Join(errs...)
}

// closeMetrics unregisters the observable-gauge callback so it is not invoked after the databases are
// closed. Safe to call when metrics were never initialized.
func (e *Engine) closeMetrics() {
	if e.metrics != nil && e.metrics.reg != nil {
		_ = e.metrics.reg.Unregister()
	}
}

// observableGauges bundles the gauge instruments handed to the collection callback.
type observableGauges struct {
	queueDepth   metric.Int64ObservableGauge
	stepsPending metric.Int64ObservableGauge
	oldestAge    metric.Int64ObservableGauge
	fairnessKeys metric.Int64ObservableGauge
	concurrency  metric.Int64ObservableGauge
}

// observeGauges is the observable-gauge callback. It reads in-memory engine state and queries the
// shards for the current-state gauges at collection time. Per-replica: cluster-wide aggregates (e.g.
// concurrency_running) are summed at the metrics backend across replicas.
func (e *Engine) observeGauges(ctx context.Context, o metric.Observer, g observableGauges) error {
	// Local in-memory gauges - no DB.
	o.ObserveInt64(g.queueDepth, int64(e.cache.Len()))

	// Fairness keys: the most recent refill's distinct-key count for the band it selected.
	e.lastRefillLock.Lock()
	band, keys := e.lastRefillBand, e.lastRefillKeys
	e.lastRefillLock.Unlock()
	if band >= 0 {
		o.ObserveInt64(g.fairnessKeys, int64(keys), metric.WithAttributes(attribute.String("priority", strconv.Itoa(band))))
	}

	// Shard-querying gauges: pending count + oldest age per priority band, and running count per task.
	pending, oldest, err := e.observePendingByBand(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for priority, count := range pending {
		o.ObserveInt64(g.stepsPending, int64(count), metric.WithAttributes(attribute.String("priority", strconv.Itoa(priority))))
	}
	for priority, sec := range oldest {
		o.ObserveInt64(g.oldestAge, int64(sec), metric.WithAttributes(attribute.String("priority", strconv.Itoa(priority))))
	}

	running, err := e.countRunningByTask(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for task, count := range running {
		o.ObserveInt64(g.concurrency, int64(count), metric.WithAttributes(attribute.String("task_url", task)))
	}
	return nil
}

// observePendingByBand returns, across all shards, the count of due pending steps per priority band
// and the age in seconds of the oldest due pending step per band (max across shards).
func (e *Engine) observePendingByBand(ctx context.Context) (countByBand, oldestSecByBand map[int]int, err error) {
	indices, pos := e.shardOrdinals()
	pendingPerShard := make([]map[int]int, len(indices))
	agePerShard := make([]map[int]int, len(indices))
	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		// parked=0 is what this gauge MEANS: a parked step is excluded from selection by construction, so
		// counting one would report a backlog no worker will ever pick up. Every other due-pending query in
		// the engine carries the predicate; this one was the lone exception. (No row is miscounted today -
		// `parkedSubgraph` is only ever written alongside status='running' - but that is an accident of the
		// current park kinds, not an invariant, and the next park kind that can coexist with `pending`
		// would silently inflate the gauge.)
		//
		// The reason is DEFINITIONAL, not performance - do not re-justify it as an index fix. Measured on
		// Postgres (300k terminal rows, 2k pending, the real partial index): the planner uses
		// idx_dwarf_steps_selection either way, seeking on the leading `status` column alone, and both plans
		// read every pending row (73 vs 76 buffers). They must: this is a GROUP BY over the whole due band,
		// so every due row is touched by definition and no index prefix can reduce that. Adding `parked`
		// only moves it into the Index Cond.
		rows, err := db.QueryContext(ctx,
			"SELECT priority, COUNT(*), DATE_DIFF_MILLIS(NOW_UTC(), MIN(created_at)) FROM dwarf_steps"+
				" WHERE status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC() GROUP BY priority",
			parkedNone,
		)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		counts := map[int]int{}
		ages := map[int]int{}
		for rows.Next() {
			var priority, count int
			var ageMs sql.NullFloat64
			err := rows.Scan(&priority, &count, &ageMs)
			if err != nil {
				return errors.Trace(err)
			}
			counts[priority] = count
			if ageMs.Valid {
				ages[priority] = int(ageMs.Float64 / 1000)
			}
		}
		err = rows.Err()
		if err != nil {
			return errors.Trace(err)
		}
		pendingPerShard[pos[shard]] = counts
		agePerShard[pos[shard]] = ages
		return nil
	})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	countByBand = map[int]int{}
	oldestSecByBand = map[int]int{}
	for i := range indices {
		for priority, count := range pendingPerShard[i] {
			countByBand[priority] += count
		}
		for priority, sec := range agePerShard[i] {
			if sec > oldestSecByBand[priority] {
				oldestSecByBand[priority] = sec
			}
		}
	}
	return countByBand, oldestSecByBand, nil
}

// countRunningByTask returns the cluster-wide (this replica's shards) count of running steps per task
// URL (the downstream identity the saturation/concurrency view keys on).
func (e *Engine) countRunningByTask(ctx context.Context) (map[string]int, error) {
	indices, pos := e.shardOrdinals()
	perShard := make([]map[string]int, len(indices))
	err := e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		rows, err := db.QueryContext(ctx,
			"SELECT task_url, COUNT(*) FROM dwarf_steps WHERE status='"+workflow.StatusRunning+"' AND parked=? GROUP BY task_url",
			parkedNone,
		)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		m := map[string]int{}
		for rows.Next() {
			var task string
			var count int
			err := rows.Scan(&task, &count)
			if err != nil {
				return errors.Trace(err)
			}
			m[task] = count
		}
		err = rows.Err()
		if err != nil {
			return errors.Trace(err)
		}
		perShard[pos[shard]] = m
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	total := map[string]int{}
	for i := range indices {
		for task, count := range perShard[i] {
			total[task] += count
		}
	}
	return total, nil
}

// --- Inline counter helpers (no-op until initMetrics has run). ---

func (e *Engine) metricFlowStarted(ctx context.Context, workflowURL string) {
	if e.metrics == nil {
		return
	}
	e.metrics.flowsStarted.Add(ctx, 1, metric.WithAttributes(attribute.String("workflow", workflowURL)))
}

func (e *Engine) metricFlowTerminated(ctx context.Context, workflowURL, status string) {
	if e.metrics == nil {
		return
	}
	e.metrics.flowsTerminated.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", workflowURL), attribute.String("status", status)))
}

// The `column` attribute on the byte counters is the dwarf_steps column the bytes moved through -
// "state" (snapshots), "changes" (task output deltas), "resume_data", "interrupt_payload",
// "subgraph_result" - bounded cardinality, and lets a chart split task data from engine snapshots from
// human-in-the-loop payloads. Sum across the attribute for the total.

func (e *Engine) metricStateWriteBytes(ctx context.Context, workflowURL, column string, n int) {
	if e.metrics == nil || n <= 0 {
		return
	}
	e.metrics.stateWriteBytes.Add(ctx, int64(n), metric.WithAttributes(
		attribute.String("workflow", workflowURL), attribute.String("column", column)))
}

func (e *Engine) metricStateReadBytes(ctx context.Context, workflowURL, column string, n int) {
	if e.metrics == nil || n <= 0 {
		return
	}
	e.metrics.stateReadBytes.Add(ctx, int64(n), metric.WithAttributes(
		attribute.String("workflow", workflowURL), attribute.String("column", column)))
}

// metricRefillPass records one complete refill pass of ONE shard's refiller: its wall clock, the batch
// it selected, and the candidates that batch discarded. Called once per pass, off the per-step hot
// path. All three instruments carry the shard attribute - the refillers are per-shard now, so a pass IS
// a shard's pass. (The old barrier made this instrument a merged-pass duration, and its divergence from
// the per-shard query maximum measured the fan-out straggler tax; that discriminator dissolved with the
// barrier.)
//
// A pass that returns early on a scan/fetch error is deliberately NOT recorded here, while the per-shard
// query histogram DOES record it (its defer fires whatever the closure returns). The asymmetry is the
// point: a failed pass has no meaningful end-to-end duration - it is a truncated pass, and folding one
// into the distribution would drag the percentiles toward an error path that is already logged and
// counted elsewhere. The query instrument still shows which shard was slow on the way to failing,
// which is the part worth keeping.
func (e *Engine) metricRefillPass(ctx context.Context, shardNum int, d time.Duration, selected, discarded int) {
	if e.metrics == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.Int("shard", shardNum))
	e.metrics.refillDuration.Record(ctx, d.Seconds(), attrs)
	if selected > 0 {
		e.metrics.refillSelected.Add(ctx, int64(selected), attrs)
	}
	if discarded > 0 {
		e.metrics.refillDiscarded.Add(ctx, int64(discarded), attrs)
	}
}

// metricRefillQuery records one shard's scan query duration, split by phase (band_keys / fetch_steps).
// The phase split is what makes the band scan's backlog dependence visible - phase 1 is where the
// measured cost concentrates, and its plan can flip between an index scan and a sequential scan on
// statistics freshness, which is indistinguishable from a slow shard without the split.
func (e *Engine) metricRefillQuery(ctx context.Context, shardNum int, phase string, d time.Duration) {
	if e.metrics == nil {
		return
	}
	e.metrics.refillQueryDuration.Record(ctx, d.Seconds(), metric.WithAttributes(
		attribute.Int("shard", shardNum), attribute.String("phase", phase)))
}

func (e *Engine) metricStepExecuted(ctx context.Context, taskName, status string) {
	if e.metrics == nil {
		return
	}
	// Step disposition keys by node name (graph topology - "which node"), unlike the concurrency/saturation
	// metric which keys by task_url (the downstream endpoint).
	e.metrics.stepsExecuted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("task_name", taskName), attribute.String("status", status)))
}

func (e *Engine) metricStepsRecovered(ctx context.Context, n int) {
	if e.metrics == nil || n <= 0 {
		return
	}
	e.metrics.stepsRecovered.Add(ctx, int64(n))
}

// metricStepWriteRetried counts a retry of the WRITE, never of the task. It is the signal that the database is
// flaky; it does not imply anything went wrong with the workflow, and a run in which it rises but
// dwarf_steps_write_failed stays zero is one where every blip was absorbed with no re-execution.
func (e *Engine) metricStepWriteRetried(ctx context.Context, shardNum int) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsWriteRetried.Add(ctx, 1, metric.WithAttributes(attribute.Int("shard", shardNum)))
}

// metricStepWriteFailed is an alarm, not a statistic: the engine could reach the database and still could not
// store the step's outcome, so the payload is at fault and the failure is permanent.
func (e *Engine) metricStepWriteFailed(ctx context.Context, taskName string) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsWriteFailed.Add(ctx, 1, metric.WithAttributes(attribute.String("task_name", taskName)))
}

func (e *Engine) metricStepUnwedged(ctx context.Context, parkType string) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsUnwedged.Add(ctx, 1, metric.WithAttributes(attribute.String("park_type", parkType)))
}

// metricOrphanedFlow is a detection-only alarm, not a statistic: the orphan sweep deliberately never re-drives
// the flow (re-driving would duplicate transition-evaluation logic and a false positive could double-advance
// it), so a nonzero value is a residual bug the recovery defer's own reset could not cover, surfaced for an
// operator alongside the error log. Labelled by workflow, like the other flow counters.
func (e *Engine) metricOrphanedFlow(ctx context.Context, workflowURL string) {
	if e.metrics == nil {
		return
	}
	e.metrics.flowsOrphaned.Add(ctx, 1, metric.WithAttributes(attribute.String("workflow", workflowURL)))
}
