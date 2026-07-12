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
	assert := testarossa.For(t)
	ctx := context.Background()

	eng := NewEngine()
	eng.SetHost(noopHost{})
	eng.RunInTest(t)

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

	eng := NewEngine()
	eng.SetHost(proxy)
	eng.SetMeterProvider(mp)
	eng.RunInTest(t)

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
