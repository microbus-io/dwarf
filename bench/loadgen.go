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
	"fmt"
	"math/rand/v2"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// stepResult is the measured outcome of one closed-loop concurrency step.
type stepResult struct {
	Concurrency int     `json:"concurrency"`
	WindowSec   float64 `json:"windowSec"`
	Flows       int     `json:"flows"`
	Errors      int     `json:"errors"`
	FlowsPerSec float64 `json:"flowsPerSec"`
	// StepsPerSec comes from the dwarf_steps_executed delta, which counts step DISPOSITIONS, not step rows:
	// completed / failed / interrupted / subgraph-park / retried / error_routed each increment it, and
	// collectCounters collapses the status attribute, so they all sum together. No workload here retries,
	// interrupts, or calls a subgraph, so one step row = one `completed` disposition and the two coincide - but a
	// workload that DID retry would inflate steps/s, and a retrying step contributes once per attempt.
	//
	// It is also drawn from a wider population than FlowsPerSec: an errored flow is excluded from Flows (below)
	// while its steps still count here. The two are therefore only comparable on a run with Errors == 0, which is
	// exactly the run main.go marks valid.
	StepsPerSec    float64          `json:"stepsPerSec"`
	MBPerSec       float64          `json:"mbPerSec"` // state payload bytes written by tasks
	P50ms          float64          `json:"p50Ms"`    // end-to-end flow latency percentiles
	P95ms          float64          `json:"p95Ms"`
	P99ms          float64          `json:"p99Ms"`
	Goroutines     int              `json:"goroutines"`     // at the end of the window: the engine's pool is most of this
	EngineCounters map[string]int64 `json:"engineCounters"` // dwarf_* deltas over the window
	// Host is what the throughput COST this engine host - CPU cores and network bandwidth over the
	// measurement window. StepsPerCore is the engine-sizing headline: steps/s driven per engine CPU core.
	Host hostUsage `json:"host"`
}

// runStep drives the fleet closed-loop at concurrency K: K submitters each Run one flow and immediately
// submit the next, round-robining across the replicas (a client behind a load balancer). Steps are then
// claimed cluster-wide by whichever replica wins the CAS, so execution distributes regardless of which
// replica a flow was submitted to. The warmup segment is discarded; only flows completing inside the
// measurement window count. Returns the measured result.
func runStep(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader,
	bytesWritten *atomic.Int64, pick func() *workload, k, fairnessKeys int, warmup, window time.Duration) stepResult {

	// The refiller bounds its scan PER fairness key (ROW_NUMBER partitioned by it). With every flow on
	// one default key that bound degenerates to a single partition, so the window must order the whole
	// due backlog before the rn<=n cut applies. Spreading flows across keys is what restores the
	// intended bound - and makes the degenerate case measurable against it.
	flowOpts := func() *workflow.FlowOptions {
		if fairnessKeys <= 1 {
			return nil
		}
		return &workflow.FlowOptions{FairnessKey: fmt.Sprintf("k%d", rand.IntN(fairnessKeys))}
	}

	var (
		measuring  atomic.Bool
		stop       atomic.Bool
		flows      atomic.Int64
		errCount   atomic.Int64
		latMu      sync.Mutex
		latencies  []time.Duration
		submitters sync.WaitGroup
	)

	for i := range k {
		eng := engines[i%len(engines)]
		submitters.Go(func() {
			for !stop.Load() {
				w := pick()
				start := time.Now()
				_, outcome, err := eng.Run(ctx, w.graphURL, w.initialState(), flowOpts())
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
	before := collectAllCounters(readers)
	bytesBefore := bytesWritten.Load()
	hostBefore := sampleHost()
	windowStart := time.Now()
	measuring.Store(true)
	time.Sleep(window)
	measuring.Store(false)
	elapsed := time.Since(windowStart)
	hostAfter := sampleHost()
	after := collectAllCounters(readers)
	bytesAfter := bytesWritten.Load()
	stop.Store(true)
	submitters.Wait()

	deltas := counterDeltas(before, after)
	sec := elapsed.Seconds()
	host := usageSince(hostBefore, hostAfter)
	if host.CPUCores > 0 {
		host.StepsPerCore = float64(deltas["dwarf_steps_executed"]) / sec / host.CPUCores
	}
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
		Goroutines:     runtime.NumGoroutine(),
		EngineCounters: deltas,
		Host:           host,
	}
}
