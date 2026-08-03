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
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
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

// gaugeValue returns the single data point of an int64 observable gauge. It takes the LAST point rather
// than summing, because an observable gauge reports a level, not an accumulation.
func gaugeValue(rm metricdata.ResourceMetrics, name string) (int64, bool) {
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != name {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok || len(g.DataPoints) == 0 {
				return 0, false
			}
			return g.DataPoints[len(g.DataPoints)-1].Value, true
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

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
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

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
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
	// The piston STAYS IDLE, which is what makes driving it by hand legal: Piston.Cycle is not safe to call
	// concurrently with Run, and an idle Run never enters a cycle at all (it parks on the idle poll), so the
	// test goroutine is the only one touching the pipeline's cadence timestamps. Cycle itself does not
	// consult the idle flag, so the forced cycles run in full. Clearing the flag here instead released the
	// engine's own Run goroutine to cycle the same pipeline alongside these calls - a real data race, and
	// one that fails whichever OTHER tests happen to be running when the detector trips.
	e.pistons[1].SetInterval(0)
	e.pistons[1].SetMinGap(0)
	e.pistons[1].Cycle(ctx)
	e.pistons[1].Cycle(ctx)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}

	// Every cycle is counted by its band_keys phase - there is no end-to-end cycle histogram, so the phases
	// are what says a cycle happened at all as well as which part of it was slow.
	passes, ok := histCount(rm, "dwarf_refill_query_duration_seconds", map[string]string{"phase": "band_keys", "shard": "1"})
	assert.True(ok, "dwarf_refill_query_duration_seconds{phase=band_keys} should be present")
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

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
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

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
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

	// The Startup sweep runs detectOrphanedFlows too; let it finish before forging, or it counts the orphan
	// a second time and this test's "exactly once" reads two.
	awaitStartupRecoverySweep(t, eng)

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

// TestMetrics_InFlightStateGauges pins the one property that makes the in-flight state pair a RESIDENCY
// gauge rather than another byte counter: it rises while a task holds its carrier and falls back to zero
// when the host call returns. dwarf_state_read_bytes already reports bytes-ever-read and cannot answer
// "how much is this replica holding right now", which is the question the crew's unbounded growth for long
// tasks makes load-bearing - a worker inside ExecuteTask holds no connection, so nothing else bounds how
// many carriers are live at once.
//
// The window asserted is the HOST CALL, not processStep: the task blocks, so the reading below is taken
// with the engine parked in exactly the phase the gauge exists to measure.
func TestMetrics_InFlightStateGauges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	// Large enough that the reading cannot be confused with the empty-state floor ("{}") every flow pays.
	payload := strings.Repeat("x", 8192)

	started := make(chan struct{})
	release := make(chan struct{})
	proxy := NewTestProxy()
	g := workflow.NewGraph("H")
	g.SetEndpoint("A", "inflightstate.verify:428/a")
	g.AddTransition("A", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("inflightstate.verify:428/g", g)
	proxy.HandleTask("inflightstate.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		close(started)
		<-release
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	done := make(chan *workflow.FlowOutcome, 1)
	go func() {
		_, outcome, err := eng.Run(ctx, "inflightstate.verify:428/g", map[string]any{"doc": payload}, nil)
		if err != nil {
			done <- nil
			return
		}
		done <- outcome
	}()

	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("task never started")
	}

	// DURING the host call: the carrier is held, so both gauges read the work in flight.
	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	steps, ok := gaugeValue(rm, "dwarf_state_in_flight_steps")
	assert.True(ok, "dwarf_state_in_flight_steps should be present")
	assert.Equal(int64(1), steps, "exactly one task is inside a host call")
	bytes, ok := gaugeValue(rm, "dwarf_state_in_flight_bytes")
	assert.True(ok, "dwarf_state_in_flight_bytes should be present")
	assert.True(bytes >= int64(len(payload)),
		"held bytes must cover the carried payload, got %d for an %d-byte field", bytes, len(payload))

	close(release)
	select {
	case outcome := <-done:
		if !assert.NotNil(outcome, "Run should have succeeded") {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	case <-time.After(30 * time.Second):
		t.Fatal("flow never completed")
	}

	// AFTER: the release is what distinguishes a gauge from a counter. Both must return to zero, or the
	// bracket leaks and every reading after the first is cumulative.
	rm = metricdata.ResourceMetrics{}
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}
	steps, _ = gaugeValue(rm, "dwarf_state_in_flight_steps")
	assert.Equal(int64(0), steps, "no task is in a host call once the flow completed")
	bytes, _ = gaugeValue(rm, "dwarf_state_in_flight_bytes")
	assert.Equal(int64(0), bytes, "held bytes must return to zero - a nonzero reading here is a leaked bracket")
}

// TestMetrics_TerminatedCountsEveryTerminalStatus pins that dwarf_flows_terminated counts FAILED and
// CANCELLED flows, not only completed ones.
//
// It regressed silently and cost nothing to miss: the instrument carried a `status` attribute and was
// described as counting "flows that have reached a terminal status", while the only call site passed
// StatusCompleted. Two things were wrong at once. `sum by (status)` invited a completed/failed/cancelled
// breakdown and answered with completions alone, and the in-flight panel the dwarf_flows_started
// description recommends - started minus terminated - drifted upward permanently by every flow that did
// not finish cleanly, never recovering.
//
// Both halves are asserted per status rather than in aggregate, because a single total would pass against
// a build that counted one of them twice.
func TestMetrics_TerminatedCountsEveryTerminalStatus(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	proxy := NewTestProxy()

	// A graph whose only task fails, with no onError, so the flow fails.
	failing := workflow.NewGraph("Failing")
	failing.SetEndpoint("boom", "termmetrics.verify:429/boom")
	failing.AddTransition("boom", workflow.END)
	proxy.HandleGraph("termmetrics.verify:429/failing", failing)
	proxy.HandleTask("termmetrics.verify:429/boom", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("intentional failure")
	})

	// A graph whose only task interrupts, parking the flow so Cancel has something live to cancel.
	parking := workflow.NewGraph("Parking")
	parking.SetEndpoint("wait", "termmetrics.verify:429/wait")
	parking.AddTransition("wait", workflow.END)
	proxy.HandleGraph("termmetrics.verify:429/parking", parking)
	proxy.HandleTask("termmetrics.verify:429/wait", func(ctx context.Context, f *workflow.Flow) error {
		_, _ = f.Interrupt(nil, nil)
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	_, outcome, err := eng.Run(ctx, "termmetrics.verify:429/failing", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, outcome.Status)

	cancelKey, err := eng.Create(ctx, "termmetrics.verify:429/parking", nil, nil)
	if !assert.NoError(err) {
		return
	}
	// Await returns on any stop, and `interrupted` is one - so this parks until the entry task has actually
	// interrupted, which is what gives Cancel a live flow to cancel rather than a racing one.
	parked, err := eng.Await(ctx, cancelKey)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusInterrupted, parked.Status)
	assert.NoError(eng.Cancel(ctx, cancelKey, "test"))

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}

	failed, ok := sumCounter(rm, "dwarf_flows_terminated", "status", workflow.StatusFailed)
	assert.True(ok, "dwarf_flows_terminated{status=failed} should be present")
	assert.Equal(int64(1), failed, "a failed flow must be counted as terminated")

	cancelled, ok := sumCounter(rm, "dwarf_flows_terminated", "status", workflow.StatusCancelled)
	assert.True(ok, "dwarf_flows_terminated{status=cancelled} should be present")
	assert.Equal(int64(1), cancelled, "a cancelled flow must be counted as terminated")

	// The property the panel actually depends on: over a workload where nothing completed, starts and
	// terminations still balance.
	started, _ := sumCounter(rm, "dwarf_flows_started", "", "")
	terminated, _ := sumCounter(rm, "dwarf_flows_terminated", "", "")
	assert.Equal(started, terminated, "started minus terminated must not drift when flows fail or cancel")
}
