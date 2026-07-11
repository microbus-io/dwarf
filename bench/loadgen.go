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
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// stepResult is the measured outcome of one closed-loop concurrency step.
type stepResult struct {
	Concurrency    int              `json:"concurrency"`
	WindowSec      float64          `json:"windowSec"`
	Flows          int              `json:"flows"`
	Errors         int              `json:"errors"`
	FlowsPerSec    float64          `json:"flowsPerSec"`
	StepsPerSec    float64          `json:"stepsPerSec"` // from the dwarf_steps_executed delta
	MBPerSec       float64          `json:"mbPerSec"`    // state payload bytes written by tasks
	P50ms          float64          `json:"p50Ms"`       // end-to-end flow latency percentiles
	P95ms          float64          `json:"p95Ms"`
	P99ms          float64          `json:"p99Ms"`
	EngineCounters map[string]int64 `json:"engineCounters"` // dwarf_* deltas over the window
}

// runStep drives the engine closed-loop at concurrency K: K submitters each Run one flow and immediately
// submit the next. The warmup segment is discarded; only flows completing inside the measurement window
// count. Returns the measured result.
func runStep(ctx context.Context, eng *engine.Engine, host *benchHost, reader *sdkmetric.ManualReader,
	pick func() *workload, k int, warmup, window time.Duration) stepResult {

	var (
		measuring  atomic.Bool
		stop       atomic.Bool
		flows      atomic.Int64
		errCount   atomic.Int64
		latMu      sync.Mutex
		latencies  []time.Duration
		submitters sync.WaitGroup
	)

	for range k {
		submitters.Go(func() {
			for !stop.Load() {
				w := pick()
				start := time.Now()
				_, outcome, err := eng.Run(ctx, w.graphURL, w.initialState(), nil)
				elapsed := time.Since(start)
				if !measuring.Load() {
					continue
				}
				if err != nil || outcome.Status != workflow.StatusCompleted {
					errCount.Add(1)
					continue
				}
				flows.Add(1)
				latMu.Lock()
				latencies = append(latencies, elapsed)
				latMu.Unlock()
			}
		})
	}

	time.Sleep(warmup)
	before := collectCounters(reader)
	bytesBefore := host.bytesWritten.Load()
	windowStart := time.Now()
	measuring.Store(true)
	time.Sleep(window)
	measuring.Store(false)
	elapsed := time.Since(windowStart)
	after := collectCounters(reader)
	bytesAfter := host.bytesWritten.Load()
	stop.Store(true)
	submitters.Wait()

	deltas := counterDeltas(before, after)
	sec := elapsed.Seconds()
	return stepResult{
		Concurrency:    k,
		WindowSec:      sec,
		Flows:          int(flows.Load()),
		Errors:         int(errCount.Load()),
		FlowsPerSec:    float64(flows.Load()) / sec,
		StepsPerSec:    float64(deltas["dwarf_steps_executed"]) / sec,
		MBPerSec:       float64(bytesAfter-bytesBefore) / (1 << 20) / sec,
		P50ms:          percentileMs(latencies, 0.50),
		P95ms:          percentileMs(latencies, 0.95),
		P99ms:          percentileMs(latencies, 0.99),
		EngineCounters: deltas,
	}
}
