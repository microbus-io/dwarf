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
	"runtime"
	"sync"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// gaugeSampleInterval is how often the gauges are read during a measurement window. It is fast relative to
// the quantities being sampled (a step's dispatch, a permit hold) and slow relative to a Collect, which is
// an in-memory callback per reader.
const gaugeSampleInterval = 50 * time.Millisecond

// gaugeSampler time-averages the observable gauges across a measurement window, because the window-edge
// snapshots alone cannot see the quantities that matter most.
//
// An observable gauge is an instantaneous LEVEL, so two samples at the window edges answer a question
// nobody asked: what the engine happened to be doing in the two microseconds either side of the window.
// For anything that fluctuates at dispatch frequency that reads as noise, and for anything that is zero
// while idle it reads as ZERO - measured: dwarf_state_in_flight_bytes/_steps snapshot 0 and 0 on every
// edge of a run that was executing hundreds of steps a second throughout, because both edges land between
// dispatches. The engine's own notes flag the same hazard for dwarf_permits_available ("an instantaneous
// sample cannot tell a reservation that is saturated all window from one that is idle"), so this is not
// specific to one instrument - it is what an edge snapshot structurally cannot do.
//
// Mean and peak answer different questions and both are kept: the mean is what the engine held on average
// (the capacity number, and the denominator of any per-step figure), the peak is what it had to have room
// for (the provisioning number). A pair whose peak far exceeds its mean is bursty, which is itself the
// finding for anything sized against a ceiling.
//
// Heap is sampled on the same tick, because it is the numerator every held-state ratio needs and the
// existing single end-of-window reading has the same edge problem. Note it is process-wide: it covers every
// in-process replica AND the load generator, so it is a comparative instrument across arms of one harness,
// never an absolute per-replica figure.
type gaugeSampler struct {
	stop chan struct{}
	done chan struct{}

	// trace, when set, is called every traceEvery with the reading just taken. It rides the sampler that
	// already exists rather than collecting on its own: an independent ticker would pay a second Collect
	// per line, and worse, would report numbers the artifact's own means were not computed from.
	trace      func(elapsed time.Duration, gauges map[string]float64, heapMB float64)
	traceEvery time.Duration

	mu       sync.Mutex
	sums     map[string]float64
	peaks    map[string]float64
	n        int
	heapSum  float64
	heapPeak float64
}

// startGaugeSampler begins sampling until close is called. Sampling every gauge rather than a named subset
// is deliberate: the edge-snapshot hazard belongs to the instrument KIND, not to any one instrument, so a
// subset would have to be extended by whoever next discovers their gauge reads zero.
func startGaugeSampler(readers []*sdkmetric.ManualReader) *gaugeSampler {
	return startGaugeSamplerTracing(readers, 0, nil)
}

// startGaugeSamplerTracing is startGaugeSampler plus a periodic callback with the live reading. every<=0 or
// a nil trace is the untraced sampler, so a caller that wants no line pays nothing for the option.
func startGaugeSamplerTracing(readers []*sdkmetric.ManualReader, every time.Duration,
	trace func(elapsed time.Duration, gauges map[string]float64, heapMB float64)) *gaugeSampler {

	s := &gaugeSampler{
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
		sums:       map[string]float64{},
		peaks:      map[string]float64{},
		trace:      trace,
		traceEvery: every,
	}
	go s.run(readers)
	return s
}

func (s *gaugeSampler) run(readers []*sdkmetric.ManualReader) {
	defer close(s.done)
	ticker := time.NewTicker(gaugeSampleInterval)
	defer ticker.Stop()
	begun := time.Now()
	nextTrace := begun
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			// collectAllGauges already reconciles across replicas (summing per-replica readings, MAXing the
			// ones every replica reports identically), so this averages the fleet-level value rather than
			// one replica's.
			g := collectAllGauges(readers)
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			heap := float64(ms.HeapAlloc) / (1 << 20)

			s.mu.Lock()
			for k, v := range g {
				s.sums[k] += v
				if v > s.peaks[k] {
					s.peaks[k] = v
				}
			}
			s.heapSum += heap
			if heap > s.heapPeak {
				s.heapPeak = heap
			}
			s.n++
			s.mu.Unlock()

			if s.trace != nil && s.traceEvery > 0 {
				if now := time.Now(); !now.Before(nextTrace) {
					nextTrace = now.Add(s.traceEvery)
					s.trace(now.Sub(begun), g, heap)
				}
			}
		}
	}
}

// gaugeWindow is what one window's sampling produced.
type gaugeWindow struct {
	Mean       map[string]float64 `json:"gaugesMean,omitempty"`
	Peak       map[string]float64 `json:"gaugesPeak,omitempty"`
	Samples    int                `json:"gaugeSamples,omitzero"`
	HeapMeanMB float64            `json:"heapMeanMB,omitzero"`
	HeapPeakMB float64            `json:"heapPeakMB,omitzero"`
}

// close stops the sampler and returns the window's aggregates. A sampler that never ticked (a window
// shorter than the interval) returns zero samples and empty maps rather than dividing by zero; the
// Samples count is what tells a reader the means are trustworthy.
func (s *gaugeSampler) close() gaugeWindow {
	if s == nil {
		return gaugeWindow{}
	}
	close(s.stop)
	<-s.done

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.n == 0 {
		return gaugeWindow{}
	}
	mean := make(map[string]float64, len(s.sums))
	for k, sum := range s.sums {
		mean[k] = sum / float64(s.n)
	}
	peak := make(map[string]float64, len(s.peaks))
	for k, v := range s.peaks {
		peak[k] = v
	}
	return gaugeWindow{
		Mean:       mean,
		Peak:       peak,
		Samples:    s.n,
		HeapMeanMB: s.heapSum / float64(s.n),
		HeapPeakMB: s.heapPeak,
	}
}
