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

func TestMetrics_EmittedOnRun(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	proxy := NewTestProxy()
	g := workflow.NewGraph("G", "metricsflow.verify:428/g")
	g.AddTask("taskA", "metricsflow.verify:428/a")
	g.AddTask("taskB", "metricsflow.verify:428/b")
	g.AddTransition("taskA", "taskB")
	g.AddTransition("taskB", workflow.END)
	proxy.HandleGraph("metricsflow.verify:428/g", g)
	proxy.HandleTask("metricsflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("metricsflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := NewEngine().
		WithHost(proxy).
		WithMeterProvider(mp)
	eng.RunInTest(t)

	outcome, err := eng.Run(ctx, "metricsflow.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	var rm metricdata.ResourceMetrics
	if !assert.NoError(reader.Collect(ctx, &rm)) {
		return
	}

	// Counters.
	started, ok := sumCounter(rm, "dwarf_flows_started_total", "", "")
	assert.True(ok, "dwarf_flows_started_total should be present")
	assert.Equal(int64(1), started)

	terminated, ok := sumCounter(rm, "dwarf_flows_terminated_total", "status", workflow.StatusCompleted)
	assert.True(ok, "dwarf_flows_terminated_total{status=completed} should be present")
	assert.Equal(int64(1), terminated)

	// Two steps complete (taskA, taskB), each counted under status=completed.
	executed, ok := sumCounter(rm, "dwarf_steps_executed_total", "status", workflow.StatusCompleted)
	assert.True(ok, "dwarf_steps_executed_total{status=completed} should be present")
	assert.Equal(int64(2), executed)

	// The queue-depth observable gauge always emits a point at collection time.
	assert.True(gaugePresent(rm, "dwarf_steps_queue_depth"), "dwarf_steps_queue_depth gauge should be present")
}
