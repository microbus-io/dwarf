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
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/accessors"
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
	CreateP50ms    float64 `json:"createP50Ms,omitzero"`
	CreateP99ms    float64 `json:"createP99Ms,omitzero"`
	MaxOutstanding int     `json:"maxOutstandingObserved,omitzero"` // peak in-flight flows seen during the window
	// ThrottledMs is creator-milliseconds spent parked on the -max-outstanding cap during the window.
	//
	// THE CAP IS A RUNAWAY GUARD ON ROW COUNT AND RUN LENGTH, NOT AN OOM GUARD. Pending steps are rows,
	// which Postgres writes to disk - a deep backlog costs disk and scan time, never resident memory,
	// and no measured workload has taken database memory near saturation. (The one real memory risk, parking
	// a caller per flow, is avoided outright: completion latency is sampled by a bounded Await pool.)
	// So it earns its place only on a FLAT-OUT run (-arrival-rate 0), where nothing else bounds
	// creation.
	//
	// UNDER A COMMANDED RATE IT MUST NOT BIND, and an arm where it did is not a measurement of the rate
	// it was asked for: admission falls to whatever completions allow, and because parking skips the
	// arrival ticker the dropped ticks are never made up. main.go invalidates such an arm. Measured: a
	// 20,000 cap turned a commanded 7,000 steps/s into ~5,500 offered and 4,328 executed, with a wait
	// profile that reads exactly like the over-connection collapse (CPU:running 37-42%, WAL share down
	// to 45%) - the same instance on the same build served the full 7,000 at 11% CPU:running once the
	// cap was lifted. Nothing in the artifact named the cap as the cause; that is why this field exists.
	ThrottledMs    int              `json:"admissionThrottledMs,omitzero"`
	Goroutines     int              `json:"goroutines"`     // at the end of the window: the engine's pool is most of this
	EngineCounters map[string]int64 `json:"engineCounters"` // dwarf_* deltas over the window
	// EngineHists carries the dwarf_* histogram distributions over the window, one row per instrument per
	// attribute set - notably dwarf_refill_query_duration_seconds per shard per phase, which is what
	// separates "one shard is slow" from "the cross-shard fan-out wait is expensive" from "the refiller is
	// not the constraint at all". Recorded as buckets, so percentiles are recoverable after the run.
	EngineHists []histSample `json:"engineHistograms,omitempty"`
	// HistsMissing says a histogram collection at either window edge failed on some replica, so EngineHists
	// is not the fleet's. Histograms are collected separately from the counters and can fail alone.
	HistsMissing bool `json:"histogramsMissing,omitzero"`
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
	// DB is what the database stored (table + index + TOAST growth) and logged (WAL) over the window, the
	// numerator of the amplification against the task-declared bytes behind MBPerSec. PostgreSQL only.
	DB *dbDelta `json:"db,omitempty"`
	// WaitProfile is where the database's active backends spent the window, as a share of observations.
	// It is what makes a throughput plateau interpretable: PgStatements says which statement was slow,
	// this says what it was stopped on, and only the second distinguishes a pool short of its knee from
	// a commit path that is already the wall.
	WaitProfile []waitRow `json:"waitProfile,omitempty"`
	// Host is what the throughput COST this engine host - CPU cores and network bandwidth over the
	// measurement window. StepsPerCore is the engine-sizing headline: steps/s driven per engine CPU core.
	Host hostUsage `json:"host"`
	// Stalled says why the watchdog ended this step early (no task started and no flow ended for
	// -stall-timeout while flows were in flight), or is empty. A stalled step's numbers cover a cut-short
	// window and the artifact is invalid.
	Stalled string `json:"stalled,omitzero"`
	// Invalidity is why this window is not a measurement (windowInvalidity), recorded on the window itself for a
	// volume probe, whose artifact holds many windows and names only the first invalid one.
	Invalidity string `json:"invalidity,omitzero"`
	// Drain is the open-loop step's end-of-run accounting (-drain): every flow created in the step, warmup
	// included, followed to its end. Nil when the drain is off or the step stalled.
	Drain *drainResult `json:"drain,omitzero"`
}

// guard holds the run guards, set once at startup from -drain and -stall-timeout. Package-level for the reason
// tracing is: runStepOpenLoop's signature already carries fifteen parameters and none of these.
var guard struct {
	drain time.Duration
	stall time.Duration
}

// stalledRun is set by any watchdog that fires and by a drain that gives up, so the run stops at the first stall
// whatever mode it is in: every window after one starts on hung engines, and main exits without their shutdown,
// which would wait on whatever hung.
var stalledRun atomic.Bool

// progressWatch ends a phase when nothing moves: no task started and no flow ended for -stall-timeout while
// busy reports work in flight. Every mode runs one - sweep windows open- and closed-loop, the volume fill and
// soak - because a hung engine otherwise holds the run until something outside kills it (an OOM-bound
// scatter arm sat ~34 minutes). It cannot catch a process too starved of memory to schedule its own
// goroutines, which is why the rig scripts carry a watchdog of their own.
type progressWatch struct {
	stalled chan struct{}
	why     string // written before stalled is closed, read only after
	cancel  context.CancelFunc
}

// watchProgress starts a watchdog over the engines' closed flows and the hosts' started tasks. With
// -stall-timeout 0 it never fires.
func watchProgress(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader, busy func() int64) *progressWatch {
	w := &progressWatch{stalled: make(chan struct{})}
	wctx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	if guard.stall <= 0 {
		return w
	}
	ended := func() int64 {
		var n int64
		for _, e := range engines {
			n += accessors.FlowsClosed(e)
		}
		return n
	}
	go func() {
		lastTasks, lastEnded, lastMove := tasksStarted.Load(), ended(), time.Now()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-tick.C:
			}
			tasks, done := tasksStarted.Load(), ended()
			if tasks != lastTasks || done != lastEnded || busy() <= 0 {
				lastTasks, lastEnded, lastMove = tasks, done, time.Now()
				continue
			}
			if time.Since(lastMove) >= guard.stall {
				w.why = fmt.Sprintf("no task started and no flow ended for %s with %d flows in flight", guard.stall, busy()) +
					dispatchedNothingNote(readers)
				stalledRun.Store(true)
				close(w.stalled)
				return
			}
		}
	}()
	return w
}

// dispatchedNothingNote names the one stall the watchdog cannot tell from a hang: this process's engines
// dispatch no shard, so peers in other processes run its flows and nothing it counts can move. Empty when any
// engine here dispatches a shard, or when the gauges cannot be read.
func dispatchedNothingNote(readers []*sdkmetric.ManualReader) string {
	gauges := collectAllGauges(readers)
	seen := false
	for key, v := range gauges {
		if name, _, _ := strings.Cut(key, "|"); name == "dwarf_shard_dispatching" {
			seen = true
			if v > 0 {
				return ""
			}
		}
	}
	if !seen {
		return ""
	}
	return "; this process dispatched no shard, so its flows ran on peers and its own counts could not move"
}

// pause sleeps for d, or until the watchdog fires.
func (w *progressWatch) pause(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-w.stalled:
	}
}

// reason says why the watchdog fired, or "" while it has not.
func (w *progressWatch) reason() string {
	select {
	case <-w.stalled:
		return w.why
	default:
		return ""
	}
}

func (w *progressWatch) stop() { w.cancel() }

// await runs wait and reports whether it returned before the watchdog fired. A phase waits on its goroutines
// through it, with the watchdog still running: one flow wedged among healthy ones is masked while the others
// keep tasks starting, and shows only once they have exited - here, where an unwatched Wait would hang.
func (w *progressWatch) await(wait func()) bool {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-w.stalled:
		return false
	}
}

// drainResult is what became of every flow an open-loop step created. "Nothing lost" is otherwise an inference
// from window counters (no errors, executed = offered) that says nothing about the ~2,000 flows still in flight
// when the window closes; this follows them to their end.
type drainResult struct {
	OutstandingAtWindowEnd int64   `json:"outstandingAtWindowEnd"`
	Sec                    float64 `json:"sec"`         // how long the drain took, up to the point it gave up
	FlowsPerSec            float64 `json:"flowsPerSec"` // the rate it closed flows at, with creation stopped
	Created                int64   `json:"created"`     // flows created in the step, warmup included
	// Completed and EndedOther count flows that ended over the whole step, by terminal status. With the drain
	// on, a sweep's previous step drained to zero before this one began, so these are this step's flows.
	Completed  int64            `json:"completed"`
	EndedOther map[string]int64 `json:"endedOther,omitempty"` // failed / terminated / cancelled
	Unfinished int64            `json:"unfinished"`           // still outstanding when the drain gave up
	// StepsRecovered counts steps lease recovery reset over the whole step. Not a loss - execution is
	// at-least-once - but it means a worker died or overran its lease.
	StepsRecovered int64 `json:"stepsRecovered"`
}

// missing is the flows created in the step that neither ended nor are still outstanding: vanished. Always 0
// unless something is badly wrong; reported so the check is arithmetic, not trust.
func (d *drainResult) missing() int64 {
	other := int64(0)
	for _, n := range d.EndedOther {
		other += n
	}
	return d.Created - d.Completed - other - d.Unfinished
}

// endedByStatus splits a counter delta's dwarf_flows_closed series by their status attribute.
func endedByStatus(deltas map[string]int64) (completed int64, other map[string]int64) {
	other = map[string]int64{}
	for key, n := range deltas {
		name, attrs, ok := strings.Cut(key, "|")
		if !ok || name != "dwarf_flows_closed" {
			continue
		}
		for attr := range strings.SplitSeq(attrs, "|") {
			status, ok := strings.CutPrefix(attr, "status=")
			if !ok {
				continue
			}
			if status == workflow.StatusCompleted {
				completed += n
			} else {
				other[status] += n
			}
		}
	}
	return completed, other
}

// runStep drives the fleet closed-loop at concurrency K: K submitters each Run one flow and immediately
// submit the next, round-robining across the replicas (a client behind a load balancer). Steps are then
// claimed cluster-wide by whichever replica wins the CAS, so execution distributes regardless of which
// replica a flow was submitted to. The warmup segment is discarded; only flows completing inside the
// measurement window count. Returns the measured result.
func runStep(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader, pgss *pgssSampler, wait *waitSampler, dbs *dbStatsSampler,
	bytesWritten *atomic.Int64, pick func() *workload, k, fairnessKeys int, warmup, window time.Duration) stepResult {

	// The refiller tallies and fetches PER fairness key. With every flow on one default key that degenerates
	// to a single group over the whole due backlog. Spreading flows across keys is what restores the
	// intended shape - and makes the degenerate case measurable against it.
	flowOpts := func() *workflow.FlowOptions {
		if fairnessKeys <= 1 {
			return nil
		}
		return &workflow.FlowOptions{FairnessKey: fmt.Sprintf("k%d", rand.IntN(fairnessKeys))}
	}

	var (
		measuring  atomic.Bool
		stop       atomic.Bool
		inFlight   atomic.Int64
		flows      atomic.Int64
		errCount   atomic.Int64
		latMu      sync.Mutex
		latencies  []time.Duration
		submitters sync.WaitGroup
	)
	watch := watchProgress(ctx, engines, readers, inFlight.Load)
	defer watch.stop()

	for i := range k {
		eng := engines[i%len(engines)]
		submitters.Go(func() {
			for !stop.Load() {
				w := pick()
				start := time.Now()
				inFlight.Add(1)
				_, outcome, err := eng.Run(ctx, w.graphURL, w.initialState(), flowOpts())
				inFlight.Add(-1)
				elapsed := time.Since(start)
				if !measuring.Load() {
					continue
				}
				if err != nil || outcome.Status != workflow.StatusCompleted {
					errCount.Add(1)
					continue
				}
				if w.check != nil {
					if err := w.check(outcome); err != nil {
						fmt.Fprintf(os.Stderr, "flow failed its workload check: %v\n", err)
						errCount.Add(1)
						continue
					}
				}
				flows.Add(1)
				latMu.Lock()
				latencies = append(latencies, elapsed)
				latMu.Unlock()
			}
		})
	}

	watch.pause(warmup)
	before := collectAllCounters(readers)
	histBefore, histBeforeOK := collectAllHistograms(readers)
	gaugesBefore := collectAllGauges(readers)
	gaugeS := startGaugeSamplerTracing(readers, tracing.every, statsTrace(), newWALProbe(dbs, wait), nil)
	pgssBefore := pgss.snapshot(ctx)
	dbBefore := dbs.sample(ctx)
	waitBefore := wait.snapshot()
	bytesBefore := bytesWritten.Load()
	hostBefore := sampleHost()
	windowStart := time.Now()
	measuring.Store(true)
	watch.pause(window)
	measuring.Store(false)
	elapsed := time.Since(windowStart)
	gauges := gaugeS.close()
	hostAfter := sampleHost()
	after := collectAllCounters(readers)
	histAfter, histAfterOK := collectAllHistograms(readers)
	gaugesAfter := collectAllGauges(readers)
	pgssAfter := pgss.snapshot(ctx)
	dbAfter := dbs.sample(ctx)
	waitAfter := wait.snapshot()
	bytesAfter := bytesWritten.Load()
	stop.Store(true)
	// A submitter stuck in Run on a hung engine never returns, so the wait gives up when the watchdog fires.
	watch.await(submitters.Wait)
	watch.stop()
	stallWhy := watch.reason()
	if stallWhy != "" {
		fmt.Println("STALLED:", stallWhy)
	}

	deltas := counterDeltas(before, after)
	sec := elapsed.Seconds()
	host := usageSince(hostBefore, hostAfter)
	if host.CPUCores > 0 {
		host.StepsPerCore = float64(deltas["dwarf_steps_executed"]) / sec / host.CPUCores
	}
	host.perStep(deltas["dwarf_steps_executed"])
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
		HistsMissing:   !histBeforeOK || !histAfterOK,
		GaugesBefore:   gaugesBefore,
		Gauges:         gauges,
		GaugesAfter:    gaugesAfter,
		PgStatements:   pgssDelta(pgssBefore, pgssAfter),
		DB:             dbs.windowDelta(dbBefore, dbAfter).withFlush(sec),
		WaitProfile:    waitDelta(waitBefore, waitAfter),
		Host:           host,
		Stalled:        stallWhy,
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
// Outstanding is bounded to maxOutstanding (created - the sum of accessors.FlowsClosed across engines, a cheap
// atomic read - no per-flow Await, which under a deep backlog would park tens of thousands of callers and
// put every one of their flow ids in the latch detector's per-shard lookup). Completion latency is SAMPLED
// by a small bounded pool of
// Await goroutines, not one per flow. arrivalPerSec>0 rate-limits creation (fixed offered load); 0 runs
// flat-out to saturation (find max throughput vs the scan interval).
func runStepOpenLoop(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader, pgss *pgssSampler, wait *waitSampler, dbs *dbStatsSampler,
	bytesWritten *atomic.Int64, pick func() *workload, creators, fairnessKeys, maxOutstanding int, arrivalPerSec float64,
	warmup, window, arrivalStop time.Duration) stepResult {

	flowOpts := func() *workflow.FlowOptions {
		if fairnessKeys <= 1 {
			return nil
		}
		return &workflow.FlowOptions{FairnessKey: fmt.Sprintf("k%d", rand.IntN(fairnessKeys))}
	}
	ended := func() int64 {
		var n int64
		for _, e := range engines {
			n += accessors.FlowsClosed(e)
		}
		return n
	}
	baseEnded := ended()

	var (
		measuring atomic.Bool
		stop      atomic.Bool
		// halted is set partway through the window by -arrival-stop: creators stop admitting while the
		// engine keeps running, so the rest of the window measures a drain under a load that has ABATED
		// rather than one still being offered. Separate from `stop`, which tears the creators down.
		halted      atomic.Bool
		created     atomic.Int64 // flows successfully created this run
		measCreated atomic.Int64 // ... within the measurement window (for arrival accounting)
		errCount    atomic.Int64
		peakOut     atomic.Int64
		// Milliseconds a creator spent parked on the outstanding cap DURING the measurement window.
		// Nonzero means the fuse blew, and with a commanded arrival rate that makes the arm a
		// measurement of the cap rather than of the rate it was asked for - see ThrottledMs.
		throttledMs atomic.Int64
		latMu       sync.Mutex
		createLat   []time.Duration // admission latency (every create, while measuring)
		doneLat     []time.Duration // end-to-end latency (sampled)
		creatorsWG  sync.WaitGroup
		awaitWG     sync.WaitGroup
	)
	// outstanding = created - (ended - baseEnded). Read cheaply from the engine atomics.
	outstanding := func() int64 { return created.Load() - (ended() - baseEnded) }
	var stepStart map[string]int64
	if guard.drain > 0 {
		stepStart = collectAllCounters(readers)
	}

	watch := watchProgress(ctx, engines, readers, outstanding)
	defer watch.stop()

	// Sampled completion latency: a bounded pool, cancelled at stop so a still-pending sample never
	// blocks the drain. One in `sampleEvery` created flows is measured, counted across ALL creators. Do not
	// count per creator: with each creator making fewer than sampleEvery flows in the window - a slow,
	// payload-heavy arm, whose latency is the point - nothing is sampled at all. At a commanded rate the
	// stride targets ~16 samples/s, so a 10-minute window holds thousands at any rate; flat out keeps 1 in
	// 64. The pool bound is what keeps sampling from storming the database either way.
	awaitCtx, cancelAwaits := context.WithCancel(ctx)
	defer cancelAwaits()
	sampleSem := make(chan struct{}, 256)
	sampleEvery := int64(64)
	if arrivalPerSec > 0 {
		sampleEvery = max(1, int64(arrivalPerSec/16))
	}
	var sampled atomic.Int64

	var arrival <-chan time.Time
	if arrivalPerSec > 0 {
		tk := time.NewTicker(time.Duration(float64(time.Second) / arrivalPerSec))
		defer tk.Stop()
		arrival = tk.C
	}

	for i := range creators {
		eng := engines[i%len(engines)]
		creatorsWG.Go(func() {
			for !stop.Load() && !halted.Load() {
				if outstanding() >= int64(maxOutstanding) {
					time.Sleep(time.Millisecond) // backpressure: let completions drain the backlog
					// Parking here SKIPS the arrival ticker, whose channel holds one slot - so every
					// tick generated while parked is dropped and admission can never make the rate up
					// afterwards. That is intended for a FUSE (flat-out runs, where the cap is the
					// governor) and fatal for a commanded rate, so the time is recorded and main.go
					// invalidates the arm when a rate was commanded.
					if measuring.Load() {
						throttledMs.Add(1)
					}
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
				if sampled.Add(1)%sampleEvery == 0 {
					select {
					case sampleSem <- struct{}{}:
						awaitWG.Go(func() {
							defer func() { <-sampleSem }()
							oc, aerr := eng.Await(awaitCtx, key)
							if aerr == nil && oc.Status == workflow.StatusCompleted && w.check != nil {
								if err := w.check(oc); err != nil {
									fmt.Fprintf(os.Stderr, "flow failed its workload check: %v\n", err)
									errCount.Add(1)
									return
								}
							}
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

	watch.pause(warmup)
	before := collectAllCounters(readers)
	histBefore, histBeforeOK := collectAllHistograms(readers)
	gaugesBefore := collectAllGauges(readers)
	gaugeS := startGaugeSamplerTracing(readers, tracing.every, statsTrace(), newWALProbe(dbs, wait), outstanding)
	pgssBefore := pgss.snapshot(ctx)
	dbBefore := dbs.sample(ctx)
	waitBefore := wait.snapshot()
	bytesBefore := bytesWritten.Load()
	hostBefore := sampleHost()
	windowStart := time.Now()
	measuring.Store(true)
	if arrivalStop > 0 && arrivalStop < window {
		// Timed off the measurement window, not the warmup, so the offered-load phase is exactly
		// arrivalStop long however long the warmup took to settle.
		t := time.AfterFunc(arrivalStop, func() { halted.Store(true) })
		defer t.Stop()
	}
	watch.pause(window)
	measuring.Store(false)
	elapsed := time.Since(windowStart)
	gauges := gaugeS.close()
	hostAfter := sampleHost()
	after := collectAllCounters(readers)
	histAfter, histAfterOK := collectAllHistograms(readers)
	gaugesAfter := collectAllGauges(readers)
	pgssAfter := pgss.snapshot(ctx)
	dbAfter := dbs.sample(ctx)
	waitAfter := wait.snapshot()
	bytesAfter := bytesWritten.Load()
	stop.Store(true)
	cancelAwaits()
	// A creator stuck in Create on a hung engine never returns, so the wait gives up when the watchdog fires.
	// main.go exits without the engines' graceful shutdown for the same reason.
	watch.await(func() {
		creatorsWG.Wait()
		awaitWG.Wait()
	})
	watch.stop()
	stallWhy := watch.reason()
	var drain *drainResult
	if stallWhy != "" {
		fmt.Println("STALLED:", stallWhy)
	} else if guard.drain > 0 {
		drain = drainOutstanding(readers, stepStart, created.Load(), outstanding, tasksStarted.Load, guard.drain)
		if drain.Unfinished > 0 {
			// The drain gives up only when no task started and no flow ended for -drain: the watchdog's own
			// definition of a hang, met after the watchdog stopped. The next window would start on the hung
			// engines, so it ends the run as a stall does.
			fmt.Println("STALLED in the drain:", drain.Unfinished, "flows unfinished")
			stalledRun.Store(true)
		}
	}

	deltas := counterDeltas(before, after)
	sec := elapsed.Seconds()
	host := usageSince(hostBefore, hostAfter)
	if host.CPUCores > 0 {
		host.StepsPerCore = float64(deltas["dwarf_steps_executed"]) / sec / host.CPUCores
	}
	host.perStep(deltas["dwarf_steps_executed"])
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
		ThrottledMs:    int(throttledMs.Load()),
		Goroutines:     runtime.NumGoroutine(),
		EngineCounters: deltas,
		EngineHists:    histogramDeltas(histBefore, histAfter),
		HistsMissing:   !histBeforeOK || !histAfterOK,
		GaugesBefore:   gaugesBefore,
		Gauges:         gauges,
		GaugesAfter:    gaugesAfter,
		PgStatements:   pgssDelta(pgssBefore, pgssAfter),
		DB:             dbs.windowDelta(dbBefore, dbAfter).withFlush(sec),
		WaitProfile:    waitDelta(waitBefore, waitAfter),
		Host:           host,
		Stalled:        stallWhy,
		Drain:          drain,
	}
}

// drainOutstanding waits for the step's outstanding flows to end, giving up once no task has started and no flow
// has ended for quiet, then accounts for every flow the step created. It runs after the measurement window has
// been collected, so it never touches the window's numbers; with creation stopped, a healthy engine drains ~2,000
// flows in seconds at small payloads and in a minute or two at 1 MB. tasks is the hosts' started-task count.
func drainOutstanding(readers []*sdkmetric.ManualReader, stepStart map[string]int64, created int64,
	outstanding, tasks func() int64, quiet time.Duration) *drainResult {

	d := &drainResult{OutstandingAtWindowEnd: max(0, outstanding()), Created: created}
	// Give up on silence, never on a wall-clock bound: a healthy saturated backlog drains for as long as it
	// takes, and only one that stopped draining counts as unfinished. A bound invalidates every slow cell -
	// 20,000 outstanding 10-step flows need 300s even at ~667 steps/s, and a 1MB cell runs at ~270.
	//
	// A task starting is progress too, as it is to the watchdog. Flow ends alone go quiet for as long as a whole
	// flow's remaining length: 10-step flows of 60s tasks in lockstep end nothing for 10 minutes while their
	// tasks keep starting, and a -drain checked against one task would give up on them.
	start := time.Now()
	left, pulse, progressed := d.OutstandingAtWindowEnd, tasks(), start
	for left > 0 && time.Since(progressed) < quiet {
		time.Sleep(200 * time.Millisecond)
		n, p := max(0, outstanding()), tasks()
		if n < left || p != pulse {
			left, pulse, progressed = min(left, n), p, time.Now()
		}
	}
	d.Sec = time.Since(start).Seconds()
	d.Unfinished = left
	if d.Sec > 0 {
		d.FlowsPerSec = float64(d.OutstandingAtWindowEnd-d.Unfinished) / d.Sec
	}
	deltas := counterDeltas(stepStart, collectAllCounters(readers))
	d.Completed, d.EndedOther = endedByStatus(deltas)
	d.StepsRecovered = deltas["dwarf_steps_recovered"]
	fmt.Printf("drain: %d flows outstanding at window end, %.1fs; created %d, completed %d, other %v, unfinished %d, missing %d, steps recovered %d\n",
		d.OutstandingAtWindowEnd, d.Sec, d.Created, d.Completed, d.EndedOther, d.Unfinished, d.missing(), d.StepsRecovered)
	return d
}
