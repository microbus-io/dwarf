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

// engineMetrics holds the engine's OpenTelemetry instruments. The counters are incremented inline at
// their event sites; the gauges are observable (async) and pulled by observeGauges at collection time.
type engineMetrics struct {
	flowsStarted    metric.Int64Counter
	flowsTerminated metric.Int64Counter
	stepsExecuted   metric.Int64Counter
	stepsRecovered  metric.Int64Counter
	stepsUnwedged   metric.Int64Counter

	reg metric.Registration // the observable-gauge callback registration, unregistered at Shutdown
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
	m.flowsStarted = ctr("dwarf_flows_started_total", "Counts flows that have been started.")
	m.flowsTerminated = ctr("dwarf_flows_terminated_total", "Counts flows that have reached a terminal status.")
	m.stepsExecuted = ctr("dwarf_steps_executed_total", "Counts steps that have been executed.")
	m.stepsRecovered = ctr("dwarf_steps_recovered_total", "Counts steps recovered by pollPendingSteps after lease expiry.")
	m.stepsUnwedged = ctr("dwarf_steps_unwedged_total", "Counts parked steps recovered by the wedge sweep, labelled by park type. A nonzero value signals a latent bug whose effect the sweep papered over.")

	gauge := func(name, desc, unit string) metric.Int64ObservableGauge {
		g, err := meter.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return g
	}
	queueDepth := gauge("dwarf_steps_queue_depth", "Steps waiting in the local worker queue.", "")
	stepsPending := gauge("dwarf_steps_pending", "Due pending steps in each priority band.", "")
	oldestAge := gauge("dwarf_steps_oldest_pending_age_seconds", "Age of the oldest due pending step in each priority band.", "s")
	fairnessKeys := gauge("dwarf_steps_fairness_keys", "Distinct fairness keys in the most recent refill selection at the given priority band.", "")
	concurrency := gauge("dwarf_task_concurrency_running", "Cluster-wide running steps per task.", "")

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
	o.ObserveInt64(g.queueDepth, int64(e.cache.len()))

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
	numShards := e.numDBShards()
	pendingPerShard := make([]map[int]int, numShards+1)
	agePerShard := make([]map[int]int, numShards+1)
	err = e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		rows, err := db.QueryContext(ctx,
			"SELECT priority, COUNT(*), DATE_DIFF_MILLIS(NOW_UTC(), MIN(created_at)) FROM dwarf_steps"+
				" WHERE status=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC() GROUP BY priority",
			workflow.StatusPending,
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
		pendingPerShard[shard] = counts
		agePerShard[shard] = ages
		return nil
	})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	countByBand = map[int]int{}
	oldestSecByBand = map[int]int{}
	for i := 1; i <= numShards; i++ {
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
	numShards := e.numDBShards()
	perShard := make([]map[string]int, numShards+1)
	err := e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		rows, err := db.QueryContext(ctx,
			"SELECT task_url, COUNT(*) FROM dwarf_steps WHERE status=? GROUP BY task_url",
			workflow.StatusRunning,
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
		perShard[shard] = m
		return nil
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	total := map[string]int{}
	for i := 1; i <= numShards; i++ {
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

func (e *Engine) metricStepUnwedged(ctx context.Context, parkType string) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsUnwedged.Add(ctx, 1, metric.WithAttributes(attribute.String("park_type", parkType)))
}
