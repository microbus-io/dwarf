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
	"maps"
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

// histSample is one histogram instrument's data point, per attribute set, flattened for the result artifact.
// Bucket boundaries and counts are carried verbatim so a percentile can be interpolated after the fact
// without re-running - the whole point of recording the distribution rather than a mean.
//
// There is deliberately no min/max: the reader's temporality is CUMULATIVE, so a windowed sample is the
// difference of two snapshots, and while counts and sums subtract cleanly a min/max does not (the window's
// extreme is unrecoverable once an earlier segment set it). The buckets carry the tail instead - which is
// what the instrument is for - so nothing is lost but a misleading field.
type histSample struct {
	Name       string            `json:"name"`
	Attrs      map[string]string `json:"attrs,omitempty"`
	Count      uint64            `json:"count"`
	SumSeconds float64           `json:"sumSeconds"`
	Bounds     []float64         `json:"bounds"`
	Counts     []uint64          `json:"counts"`
}

// key identifies a sample across two snapshots: the instrument plus its attribute set.
func (h histSample) key() string {
	k := h.Name
	for _, name := range slices.Sorted(maps.Keys(h.Attrs)) {
		k += "|" + name + "=" + h.Attrs[name]
	}
	return k
}

// collectHistograms snapshots the engine's float64 histogram instruments, one entry per (instrument,
// attribute set) - so dwarf_refill_query_duration_seconds yields a row per shard per phase rather than
// collapsing them, which is the entire discriminating power of that instrument.
//
// This exists because collectCounters extracts only Sum[int64] and silently drops everything else, so the
// gauges and histograms never reached a results file. Any hypothesis about the refiller was therefore
// untestable from a recorded run, no matter how many times the run was repeated.
func collectHistograms(reader *sdkmetric.ManualReader) []histSample {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var rm metricdata.ResourceMetrics
	err := reader.Collect(ctx, &rm)
	if err != nil {
		return nil
	}
	var out []histSample
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			hist, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				continue
			}
			for _, dp := range hist.DataPoints {
				if dp.Count == 0 {
					continue
				}
				s := histSample{
					Name:   m.Name,
					Count:  dp.Count,
					Bounds: dp.Bounds,
					Counts: dp.BucketCounts,
				}
				s.SumSeconds = dp.Sum
				if n := dp.Attributes.Len(); n > 0 {
					s.Attrs = make(map[string]string, n)
					for _, kv := range dp.Attributes.ToSlice() {
						s.Attrs[string(kv.Key)] = kv.Value.String()
					}
				}
				out = append(out, s)
			}
		}
	}
	return out
}

// collectAllHistograms merges collectHistograms across every replica's reader, summing the samples that
// share an (instrument, attributes) key. A shard's scan latency is a property of the SHARD, and every
// replica scans all of them, so per-replica rows would just be repeated draws from one distribution;
// summing gives the fleet's view of that shard with more samples behind it.
func collectAllHistograms(readers []*sdkmetric.ManualReader) []histSample {
	var order []string
	byKey := map[string]histSample{}
	for _, r := range readers {
		for _, s := range collectHistograms(r) {
			k := s.key()
			prev, seen := byKey[k]
			if !seen {
				order = append(order, k)
				byKey[k] = s
				continue
			}
			byKey[k] = addHist(prev, s)
		}
	}
	out := make([]histSample, 0, len(order))
	for _, k := range order {
		out = append(out, byKey[k])
	}
	return out
}

// addHist sums two samples of the same instrument+attributes (identical bucket boundaries by construction -
// the boundaries are a compile-time constant of the instrument).
func addHist(a, b histSample) histSample {
	a.Count += b.Count
	a.SumSeconds += b.SumSeconds
	for i := range a.Counts {
		if i < len(b.Counts) {
			a.Counts[i] += b.Counts[i]
		}
	}
	return a
}

// histogramDeltas subtracts the before snapshot from the after snapshot, per (instrument, attributes), so
// the result covers only the measurement window. The reader is cumulative, so without this the samples
// would include the discarded warmup segment - and warmup is exactly where a cold buffer cache or stale
// planner statistics put their slowest scans, which would land in the tail buckets and be read as a
// steady-state straggler.
func histogramDeltas(before, after []histSample) []histSample {
	prior := map[string]histSample{}
	for _, s := range before {
		prior[s.key()] = s
	}
	var out []histSample
	for _, s := range after {
		p, seen := prior[s.key()]
		if !seen {
			out = append(out, s)
			continue
		}
		s.Count -= p.Count
		s.SumSeconds -= p.SumSeconds
		counts := make([]uint64, len(s.Counts))
		for i := range s.Counts {
			counts[i] = s.Counts[i]
			if i < len(p.Counts) {
				counts[i] -= p.Counts[i]
			}
		}
		s.Counts = counts
		if s.Count == 0 {
			continue
		}
		out = append(out, s)
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
