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
	StepsPerSec float64 `json:"stepsPerSec"`
	MBPerSec    float64 `json:"mbPerSec"` // state payload bytes written by tasks
	P50ms       float64 `json:"p50Ms"`    // end-to-end (create->complete) flow latency percentiles
	P95ms       float64 `json:"p95Ms"`
	P99ms       float64 `json:"p99Ms"`
	// Admission (Create-call) latency percentiles - open-loop only. In closed-loop these stay zero.
	// The pair with P50/P99 is the point of open-loop: admission latency is how fast the engine ACCEPTS
	// work (a DB insert), separable from end-to-end which is dominated by backlog wait. A rising
	// admission latency under a growing backlog is the engine's own backpressure showing through.
	CreateP50ms    float64          `json:"createP50Ms,omitzero"`
	CreateP99ms    float64          `json:"createP99Ms,omitzero"`
	MaxOutstanding int              `json:"maxOutstandingObserved,omitzero"` // peak in-flight flows seen during the window
	Goroutines     int              `json:"goroutines"`                      // at the end of the window: the engine's pool is most of this
	EngineCounters map[string]int64 `json:"engineCounters"`                  // dwarf_* deltas over the window
	// EngineHists carries the dwarf_* histogram distributions over the window, one row per instrument per
	// attribute set - notably dwarf_refill_query_duration_seconds per shard per phase, which is what
	// separates "one shard is slow" from "the cross-shard fan-out wait is expensive" from "the refiller is
	// not the constraint at all". Recorded as buckets, so percentiles are recoverable after the run.
	EngineHists []histSample `json:"engineHistograms,omitempty"`
	// GaugesBefore/GaugesAfter snapshot the observable gauges at the window edges. The pair worth the
	// field: sequel_pool_wait_count / sequel_pool_wait_duration_seconds are cumulative sql.DBStats
	// totals, so their window delta is the pool-wait the engine paid inside the window - the signal
	// that decomposes any client-observed query duration (which includes that wait) against the
	// server-side time in PgStatements (which excludes it).
	GaugesBefore map[string]float64 `json:"gaugesBefore,omitempty"`
	GaugesAfter  map[string]float64 `json:"gaugesAfter,omitempty"`
	// Gauges is the same instruments TIME-AVERAGED across the window, and for anything that fluctuates at
	// dispatch frequency it is the only usable reading - the edge snapshots above land between dispatches
	// and report zero. See gaugesampler.go for which question the mean answers and which the peak does.
	Gauges gaugeWindow `json:"gauges,omitzero"`
	// PgStatements is the window delta of pg_stat_statements (top statements by server-side exec time),
	// present only on Postgres with the extension preloaded. Server-side clocks: excludes pool wait,
	// wire time and client scheduling - the other half of the attribution above.
	PgStatements []pgssRow `json:"pgStatements,omitempty"`
	// WaitProfile is where the database's active backends spent the window, as a share of observations.
	// It is what makes a throughput plateau interpretable: PgStatements says which statement was slow,
	// this says what it was stopped on, and only the second distinguishes a pool short of its knee from
	// a commit path that is already the wall.
	WaitProfile []waitRow `json:"waitProfile,omitempty"`
	// Host is what the throughput COST this engine host - CPU cores and network bandwidth over the
	// measurement window. StepsPerCore is the engine-sizing headline: steps/s driven per engine CPU core.
	Host hostUsage `json:"host"`
}

// runStep drives the fleet closed-loop at concurrency K: K submitters each Run one flow and immediately
// submit the next, round-robining across the replicas (a client behind a load balancer). Steps are then
// claimed cluster-wide by whichever replica wins the CAS, so execution distributes regardless of which
// replica a flow was submitted to. The warmup segment is discarded; only flows completing inside the
// measurement window count. Returns the measured result.
func runStep(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader, pgss *pgssSampler, wait *waitSampler,
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
	histBefore := collectAllHistograms(readers)
	gaugesBefore := collectAllGauges(readers)
	gaugeS := startGaugeSamplerTracing(readers, tracing.every, statsTrace())
	pgssBefore := pgss.snapshot(ctx)
	waitBefore := wait.snapshot()
	bytesBefore := bytesWritten.Load()
	hostBefore := sampleHost()
	windowStart := time.Now()
	measuring.Store(true)
	time.Sleep(window)
	measuring.Store(false)
	elapsed := time.Since(windowStart)
	gauges := gaugeS.close()
	hostAfter := sampleHost()
	after := collectAllCounters(readers)
	histAfter := collectAllHistograms(readers)
	gaugesAfter := collectAllGauges(readers)
	pgssAfter := pgss.snapshot(ctx)
	waitAfter := wait.snapshot()
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
		EngineHists:    histogramDeltas(histBefore, histAfter),
		GaugesBefore:   gaugesBefore,
		Gauges:         gauges,
		GaugesAfter:    gaugesAfter,
		PgStatements:   pgssDelta(pgssBefore, pgssAfter),
		WaitProfile:    waitDelta(waitBefore, waitAfter),
		Host:           host,
	}
}

// runStepOpenLoop drives the fleet OPEN-loop: `creators` goroutines create flows as fast as the
// outstanding cap allows, WITHOUT waiting for completion, so the backlog grows past the goroutine count
// and can bury the refiller's per-shard cache many times over (the closed loop pins in-flight FLOWS to
// the goroutine count, which for a linear workload - ~1 pending step per flow - keeps the whole backlog
// inside the cache at higher shard counts, so the refiller is never stressed and its scan interval has
// nothing to bind on). Open loop is what puts the refiller under the deep backlog it exists for, and it
// separates two latencies the closed loop conflates: admission (the Create call) and end-to-end.
//
// Outstanding is bounded to maxOutstanding (created - Sum(FlowsTerminated) across engines, a cheap
// atomic read - no per-flow Await, which under a deep backlog would park tens of thousands of callers and
// put every one of their flow ids in the latch detector's per-shard lookup). Completion latency is SAMPLED
// by a small bounded pool of
// Await goroutines, not one per flow. arrivalPerSec>0 rate-limits creation (fixed offered load); 0 runs
// flat-out to saturation (find max throughput vs the scan interval).
func runStepOpenLoop(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader, pgss *pgssSampler, wait *waitSampler,
	bytesWritten *atomic.Int64, pick func() *workload, creators, fairnessKeys, maxOutstanding, arrivalPerSec int,
	warmup, window time.Duration) stepResult {

	flowOpts := func() *workflow.FlowOptions {
		if fairnessKeys <= 1 {
			return nil
		}
		return &workflow.FlowOptions{FairnessKey: fmt.Sprintf("k%d", rand.IntN(fairnessKeys))}
	}
	terminated := func() int64 {
		var n int64
		for _, e := range engines {
			n += e.FlowsTerminated()
		}
		return n
	}
	baseTerminated := terminated()

	var (
		measuring   atomic.Bool
		stop        atomic.Bool
		created     atomic.Int64 // flows successfully created this run
		measCreated atomic.Int64 // ... within the measurement window (for arrival accounting)
		errCount    atomic.Int64
		peakOut     atomic.Int64
		latMu       sync.Mutex
		createLat   []time.Duration // admission latency (every create, while measuring)
		doneLat     []time.Duration // end-to-end latency (sampled)
		creatorsWG  sync.WaitGroup
		awaitWG     sync.WaitGroup
	)
	// outstanding = created - (terminated - baseTerminated). Read cheaply from the engine atomics.
	outstanding := func() int64 { return created.Load() - (terminated() - baseTerminated) }

	// Sampled completion latency: a bounded pool, cancelled at stop so a still-pending sample never
	// blocks the drain. One in `sampleEvery` created flows is measured.
	awaitCtx, cancelAwaits := context.WithCancel(ctx)
	defer cancelAwaits()
	sampleSem := make(chan struct{}, 256)
	const sampleEvery = 64

	var arrival <-chan time.Time
	if arrivalPerSec > 0 {
		tk := time.NewTicker(time.Second / time.Duration(arrivalPerSec))
		defer tk.Stop()
		arrival = tk.C
	}

	for i := range creators {
		eng := engines[i%len(engines)]
		creatorsWG.Go(func() {
			var n int64
			for !stop.Load() {
				if outstanding() >= int64(maxOutstanding) {
					time.Sleep(time.Millisecond) // backpressure: let completions drain the backlog
					continue
				}
				if arrival != nil {
					select {
					case <-arrival:
					case <-ctx.Done():
						return
					}
				}
				w := pick()
				born := time.Now()
				key, err := eng.Create(ctx, w.graphURL, w.initialState(), flowOpts())
				createElapsed := time.Since(born)
				if err != nil {
					errCount.Add(1)
					continue
				}
				created.Add(1)
				if o := outstanding(); o > peakOut.Load() {
					peakOut.Store(o)
				}
				if measuring.Load() {
					measCreated.Add(1)
					latMu.Lock()
					createLat = append(createLat, createElapsed)
					latMu.Unlock()
				}
				// Sample end-to-end latency on 1-in-sampleEvery, bounded so it cannot storm the DB.
				n++
				if n%sampleEvery == 0 {
					select {
					case sampleSem <- struct{}{}:
						awaitWG.Go(func() {
							defer func() { <-sampleSem }()
							oc, aerr := eng.Await(awaitCtx, key)
							if measuring.Load() && aerr == nil && oc.Status == workflow.StatusCompleted {
								latMu.Lock()
								doneLat = append(doneLat, time.Since(born))
								latMu.Unlock()
							}
						})
					default: // pool full: skip this sample rather than block creation
					}
				}
			}
		})
	}

	time.Sleep(warmup)
	before := collectAllCounters(readers)
	histBefore := collectAllHistograms(readers)
	gaugesBefore := collectAllGauges(readers)
	gaugeS := startGaugeSamplerTracing(readers, tracing.every, statsTrace())
	pgssBefore := pgss.snapshot(ctx)
	waitBefore := wait.snapshot()
	bytesBefore := bytesWritten.Load()
	hostBefore := sampleHost()
	windowStart := time.Now()
	measuring.Store(true)
	time.Sleep(window)
	measuring.Store(false)
	elapsed := time.Since(windowStart)
	gauges := gaugeS.close()
	hostAfter := sampleHost()
	after := collectAllCounters(readers)
	histAfter := collectAllHistograms(readers)
	gaugesAfter := collectAllGauges(readers)
	pgssAfter := pgss.snapshot(ctx)
	waitAfter := wait.snapshot()
	bytesAfter := bytesWritten.Load()
	stop.Store(true)
	cancelAwaits()
	creatorsWG.Wait()
	awaitWG.Wait()

	deltas := counterDeltas(before, after)
	sec := elapsed.Seconds()
	host := usageSince(hostBefore, hostAfter)
	if host.CPUCores > 0 {
		host.StepsPerCore = float64(deltas["dwarf_steps_executed"]) / sec / host.CPUCores
	}
	return stepResult{
		Concurrency:    creators,
		WindowSec:      sec,
		Flows:          int(measCreated.Load()),
		Errors:         int(errCount.Load()),
		FlowsPerSec:    float64(measCreated.Load()) / sec, // admission rate (open-loop): flows accepted/sec
		StepsPerSec:    float64(deltas["dwarf_steps_executed"]) / sec,
		MBPerSec:       float64(bytesAfter-bytesBefore) / (1 << 20) / sec,
		P50ms:          percentileMs(doneLat, 0.50),
		P95ms:          percentileMs(doneLat, 0.95),
		P99ms:          percentileMs(doneLat, 0.99),
		CreateP50ms:    percentileMs(createLat, 0.50),
		CreateP99ms:    percentileMs(createLat, 0.99),
		MaxOutstanding: int(peakOut.Load()),
		Goroutines:     runtime.NumGoroutine(),
		EngineCounters: deltas,
		EngineHists:    histogramDeltas(histBefore, histAfter),
		GaugesBefore:   gaugesBefore,
		Gauges:         gauges,
		GaugesAfter:    gaugesAfter,
		PgStatements:   pgssDelta(pgssBefore, pgssAfter),
		WaitProfile:    waitDelta(waitBefore, waitAfter),
		Host:           host,
	}
}
