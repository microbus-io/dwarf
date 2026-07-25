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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// sumCounter returns the total of an int64 Sum metric's data points, optionally filtered to points
// carrying the given attribute key=value (empty key = no filter).
func sumCounter(rm metricdata.ResourceMetrics, name, attrKey, attrVal string) (int64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				return 0, false
			}
			var total int64
			matched := false
			for _, dp := range sum.DataPoints {
				if attrKey != "" {
					v, ok := dp.Attributes.Value(attribute.Key(attrKey))
					if !ok || v.AsString() != attrVal {
						continue
					}
				}
				total += dp.Value
				matched = true
			}
			return total, matched
		}
	}
	return 0, false
}

// histCount returns the total sample count of a float64 Histogram metric, optionally filtered to points
// carrying ALL of the given attributes (nil = no filter).
func histCount(rm metricdata.ResourceMetrics, name string, attrs map[string]string) (uint64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				return 0, false
			}
			var total uint64
			matched := false
		points:
			for _, dp := range hist.DataPoints {
				for k, want := range attrs {
					v, ok := dp.Attributes.Value(attribute.Key(k))
					if !ok || v.String() != want {
						continue points
					}
				}
				total += dp.Count
				matched = true
			}
			return total, matched
		}
	}
	return 0, false
}

// gaugePresent reports whether an int64 observable gauge with the given name emitted any data point.
func gaugePresent(rm metricdata.ResourceMetrics, name string) bool {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			return ok && len(g.DataPoints) > 0
		}
	}
	return false
}

// TestCountRunningByTask_ExcludesParked pins that the dwarf_task_concurrency_running gauge counts only
// active (parked=parkedNone) running steps, not parked subgraph callers. A parked caller is status=running
// but holds no executing slot, so counting it would inflate task concurrency and contradict the saturation
// index's documented purpose (its (status, parked, task_url) shape excludes parked rows).
func TestCountRunningByTask_ExcludesParked(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	eng := NewEngineUnderTest(t)
	eng.SetHost(noopHost{})
	assert.NoError(eng.Startup(t.Context()))

	db, err := eng.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	ins := func(taskURL, status string, parked int) {
		// lease_expires is set far in the future so pollPendingSteps' lease recovery (which resets a
		// running step whose lease has lapsed) cannot flip this forged running step back to pending
		// between the insert and the count - the source of an intermittent "actual '0'" flake. A real
		// running step likewise holds a live lease into the future.
		_, err := db.ExecContext(ctx,
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, status, time_budget_ms, parked, lease_expires) VALUES (?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?))",
			1, 1, "tok", "T", taskURL, status, 1000, parked, 3600000,
		)
		assert.NoError(err)
	}
	ins("svc/x", workflow.StatusRunning, parkedNone)     // active caller - counts
	ins("svc/y", workflow.StatusRunning, parkedSubgraph) // parked caller - must be excluded

	counts, err := eng.countRunningByTask(ctx)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(1, counts["svc/x"])
	_, present := counts["svc/y"]
	assert.False(present) // parked subgraph caller does not inflate concurrency
}

func TestMetrics_EmittedOnRun(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	proxy := NewTestProxy()
	g := workflow.NewGraph("G")
	g.SetEndpoint("taskA", "metricsflow.verify:428/a")
	g.SetEndpoint("taskB", "metricsflow.verify:428/b")
	g.AddTransition("taskA", "taskB")
	g.AddTransition("taskB", workflow.END)
	proxy.HandleGraph("metricsflow.verify:428/g", g)
	proxy.HandleTask("metricsflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("metricsflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t)
	eng.SetHost(proxy)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	_, outcome, err := eng.Run(ctx, "metricsflow.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}

	// Counters. Instrument names carry no _total suffix (the Prometheus exporter appends it); the manual
	// reader here observes the raw instrument names.
	started, ok := sumCounter(rm, "dwarf_flows_started", "", "")
	assert.True(ok, "dwarf_flows_started should be present")
	assert.Equal(int64(1), started)

	terminated, ok := sumCounter(rm, "dwarf_flows_terminated", "status", workflow.StatusCompleted)
	assert.True(ok, "dwarf_flows_terminated{status=completed} should be present")
	assert.Equal(int64(1), terminated)

	// Two steps complete (taskA, taskB), each counted under status=completed.
	executed, ok := sumCounter(rm, "dwarf_steps_executed", "status", workflow.StatusCompleted)
	assert.True(ok, "dwarf_steps_executed{status=completed} should be present")
	assert.Equal(int64(2), executed)

	// State byte throughput: both counters fire on the execution path, labelled by workflow and by the
	// column the bytes moved through. Even no-op tasks carry at least the empty-object JSON ("{}"), so the
	// workflow-level sums are strictly positive; per-column, the entry snapshot ("state") and the
	// completion delta ("changes") must both appear for this two-task linear flow.
	writeBytes, ok := sumCounter(rm, "dwarf_state_write_bytes", "workflow", "metricsflow.verify:428/g")
	assert.True(ok, "dwarf_state_write_bytes{workflow} should be present")
	assert.True(writeBytes > 0, "state write bytes should be positive, got %d", writeBytes)
	stateW, ok := sumCounter(rm, "dwarf_state_write_bytes", "column", "state")
	assert.True(ok, "dwarf_state_write_bytes{column=state} should be present")
	assert.True(stateW > 0)
	changesW, ok := sumCounter(rm, "dwarf_state_write_bytes", "column", "changes")
	assert.True(ok, "dwarf_state_write_bytes{column=changes} should be present")
	assert.True(changesW > 0)
	readBytes, ok := sumCounter(rm, "dwarf_state_read_bytes", "workflow", "metricsflow.verify:428/g")
	assert.True(ok, "dwarf_state_read_bytes{workflow} should be present")
	assert.True(readBytes > 0, "state read bytes should be positive, got %d", readBytes)

	// The queue-depth observable gauge always emits a point at collection time.
	assert.True(gaugePresent(rm, "dwarf_steps_queue_depth"), "dwarf_steps_queue_depth gauge should be present")
}

// TestMetrics_RefillInstrumented pins the refiller's four instruments, which exist to answer a question no
// other instrument can: what the refiller actually costs, and how much of that cost is wasted.
//
// The waste is structural rather than a bug. The refiller is triggered after EVERY processStep, coalesced
// into a single slot, and each pass wholesale-REPLACES the cache - so whenever it turns faster than the
// workers drain (the deep-backlog case it exists for), it discards candidates the previous pass selected
// and paid a round-trip to fetch. Those steps stay `pending` and are re-selected, so it is cost, not loss.
// The selected/discarded ratio is the only way to see it.
//
// SetWorkers(0) makes all of this deterministic: nothing dispatches, so the backlog is stable and the
// second pass necessarily discards exactly what the first one cached.
func TestMetrics_RefillInstrumented(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	proxy := NewTestProxy()
	g := workflow.NewGraph("R")
	g.SetEndpoint("A", "refillmetrics/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("refillmetrics/g", g)
	proxy.HandleTask("refillmetrics/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.SetMeterProvider(mp))
	assert.NoError(e.SetWorkers(0)) // nothing dispatches: the backlog stays put
	assert.NoError(e.Startup(t.Context()))

	for range 8 {
		_, err := e.Create(ctx, "refillmetrics/g", nil, &workflow.FlowOptions{FairnessKey: "tenant"})
		assert.NoError(err)
	}

	// Two cycles over the same stable backlog. The first fills the cache; the second replaces it, so
	// everything the first selected is discarded un-popped. Driven by hand through the engine's own piston,
	// which is also what proves the engine handed it the meter: the instruments are the PISTON's, resolved
	// from the scope the engine resolved once (initRuntime), not built a second time here.
	assert.True(e.cache.Capacity() > 0)
	e.pistons[1].SetIdle(false) // SetWorkers(0) idled it; this test drives the cycles itself
	e.pistons[1].SetInterval(0)
	e.pistons[1].SetMinGap(0)
	e.pistons[1].Cycle(ctx)
	e.pistons[1].Cycle(ctx)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}

	// The whole-cycle histogram records every cycle, including the ones that select nothing.
	passes, ok := histCount(rm, "dwarf_refill_duration_seconds", nil)
	assert.True(ok, "dwarf_refill_duration_seconds should be present")
	assert.True(passes >= 2, "expected at least the 2 forced cycles, got %d", passes)

	// The per-shard query histogram is the one that isolates a straggler, so BOTH scan phases must be
	// labelled and attributed to a shard - a pass that timed only the aggregate would be unable to tell a
	// slow band scan (the plan-instability case) from a slow targeted fetch.
	for _, phase := range []string{"band_keys", "fetch_steps"} {
		n, ok := histCount(rm, "dwarf_refill_query_duration_seconds", map[string]string{"phase": phase, "shard": "1"})
		assert.True(ok, "dwarf_refill_query_duration_seconds{phase=%s,shard=1} should be present", phase)
		assert.True(n > 0, "phase %s recorded no samples", phase)
	}

	// Selected and discarded: the first pass cached a batch, the second threw it away.
	selected, ok := sumCounter(rm, "dwarf_refill_candidates_selected", "", "")
	assert.True(ok, "dwarf_refill_candidates_selected should be present")
	assert.True(selected > 0, "expected candidates to be selected from an 8-step backlog, got %d", selected)

	discarded, ok := sumCounter(rm, "dwarf_refill_candidates_discarded", "", "")
	assert.True(ok, "dwarf_refill_candidates_discarded should be present")
	assert.True(discarded > 0, "the second pass must discard the first pass's un-popped batch, got %d", discarded)
}

// TestMetrics_ForkCountsAsStarted pins that a Fork increments dwarf_flows_started. A fork builds its new
// root flow through its own INSERT...SELECT clone path, which never called metricFlowStarted - but the
// fork's completion runs through the same completeFlow that increments dwarf_flows_terminated. So a fork
// was counted on the way out and never on the way in: the standard in-flight panel (started - terminated)
// drifts NEGATIVE by one per fork, or permanently understates. Both counters must move together.
func TestMetrics_ForkCountsAsStarted(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	proxy := NewTestProxy()
	g := workflow.NewGraph("Forked")
	g.SetEndpoint("A", "forkmetric.verify:428/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("forkmetric.verify:428/g", g)
	proxy.HandleTask("forkmetric.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t)
	eng.SetHost(proxy)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	// One flow, run to completion: started=1, terminated=1.
	flowKey, outcome, err := eng.Run(ctx, "forkmetric.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Fork it from its only step, and let the fork run to completion too.
	hist, err := eng.History(ctx, flowKey)
	if !assert.NoError(err) || !assert.True(len(hist) > 0, "the origin has a step to fork from") {
		return
	}
	forkKey, err := eng.Fork(ctx, hist[0].StepKey, nil)
	if !assert.NoError(err) {
		return
	}
	forkOut, err := eng.Await(ctx, forkKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, forkOut.Status)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	started, ok := sumCounter(rm, "dwarf_flows_started", "", "")
	assert.True(ok, "dwarf_flows_started should be present")
	terminated, ok := sumCounter(rm, "dwarf_flows_terminated", "status", workflow.StatusCompleted)
	assert.True(ok, "dwarf_flows_terminated{status=completed} should be present")

	// Two flows started (the original and the fork), two terminated. The in-flight gauge a dashboard builds
	// from these - started minus terminated - must be 0, not -1.
	assert.Equal(int64(2), started, "the fork counts as a started flow")
	assert.Equal(int64(2), terminated)
	assert.Equal(int64(0), started-terminated, "in-flight (started - terminated) must not go negative")
}

// TestOrphanDetection_EmitsMetric pins that the orphan sweep increments dwarf_flows_orphaned (labelled by
// workflow) when it flags a stranded flow - the operability alarm that lets an operator alert on the residual
// orphan the recovery defer could not cover without scraping the error log. It forges the same orphan shape the
// log-only test uses (a completed flow flipped back to running, its step backdated stale) and asserts the count.
func TestOrphanDetection_EmitsMetric(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	proxy := NewTestProxy()
	g := workflow.NewGraph("Orphan")
	g.SetEndpoint("A", "orphanmetric.verify:428/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("orphanmetric.verify:428/g", g)
	proxy.HandleTask("orphanmetric.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngineUnderTest(t)
	eng.SetHost(proxy)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	key, err := eng.Create(ctx, "orphanmetric.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	out, err := eng.Await(ctx, key)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, out.Status) {
		return
	}
	shard, flowID, _, err := keys.ParseFlowKey(key)
	if !assert.NoError(err) {
		return
	}
	db, err := eng.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}

	// Forge the orphan shape: running flow, every step terminal and stale past the threshold.
	pastMs := -(eng.orphanFlowThreshold + time.Minute).Milliseconds()
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET status=?, updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		workflow.StatusRunning, pastMs, flowID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_steps SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?", pastMs, flowID)
	assert.NoError(err)

	eng.detectOrphanedFlows(ctx, db, shard)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	orphaned, ok := sumCounter(rm, "dwarf_flows_orphaned", "workflow", "orphanmetric.verify:428/g")
	assert.True(ok, "dwarf_flows_orphaned{workflow} should be present")
	assert.Equal(int64(1), orphaned, "the forged orphan is counted exactly once")
}
