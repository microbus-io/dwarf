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

package main

import (
	"context"
	"slices"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectCounters snapshots the engine's dwarf_* counter instruments from the manual reader, summing every
// data point per instrument name (attributes collapsed). Deltas between two snapshots give the per-window
// counts: dwarf_steps_executed is the primary steps/s source, and dwarf_steps_recovered /
// dwarf_steps_unwedged must stay zero for a run to be valid.
func collectCounters(reader *sdkmetric.ManualReader) map[string]int64 {
	// Collect invokes the engine's async gauge callback, which queries the shard databases with THIS
	// ctx - unbounded, a hung database (e.g. a paused container in a fault run) wedges the sampler
	// forever. A production OTEL PeriodicReader passes a timeout ctx; do the same.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var rm metricdata.ResourceMetrics
	err := reader.Collect(ctx, &rm)
	if err != nil {
		return nil
	}
	out := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			var total int64
			for _, dp := range sum.DataPoints {
				total += dp.Value
			}
			out[m.Name] = total
		}
	}
	return out
}

// collectAllCounters sums collectCounters across every replica's reader, so a fleet total (e.g. total
// dwarf_steps_executed, or total unwedged) reads as one map regardless of replica count.
func collectAllCounters(readers []*sdkmetric.ManualReader) map[string]int64 {
	out := map[string]int64{}
	for _, r := range readers {
		for name, v := range collectCounters(r) {
			out[name] += v
		}
	}
	return out
}

// counterDeltas subtracts the before snapshot from the after snapshot.
func counterDeltas(before, after map[string]int64) map[string]int64 {
	out := map[string]int64{}
	for name, v := range after {
		if d := v - before[name]; d != 0 {
			out[name] = d
		}
	}
	return out
}

// percentileMs returns the given percentile of the samples in milliseconds (0 when empty). Sorts a copy.
func percentileMs(samples []time.Duration, p float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(samples))
	copy(sorted, samples)
	slices.Sort(sorted)
	idx := int(p * float64(len(sorted)-1))
	return float64(sorted[idx]) / float64(time.Millisecond)
}
