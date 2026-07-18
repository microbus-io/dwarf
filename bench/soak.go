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

// The soak drives a mixed, production-shaped workload against one engine for a fixed duration under a
// create-purge steady state, sampling drift periodically. Unlike the concurrency sweep (which measures
// throughput/latency over short windows), the soak asks a different question: does anything drift or wedge
// over time? It exercises the full operation surface - Create/Run/Await, Interrupt/Resume, Continue, and
// retention (DeleteOnCompletion + a periodic Purge) so the reaper churns - and watches goroutines, heap,
// row counts, and the engine's alarm counters (dwarf_steps_unwedged / dwarf_flows_orphaned /
// dwarf_steps_write_failed must stay at zero).

// soakSample is one periodic drift reading.
type soakSample struct {
	TSec         float64 `json:"tSec"`
	Goroutines   int     `json:"goroutines"`
	HeapAllocMB  float64 `json:"heapAllocMB"`
	HeapSysMB    float64 `json:"heapSysMB"`
	Steps        int     `json:"steps"`        // live dwarf_steps rows across shards (ShardInfo)
	Flows        int     `json:"flows"`        // live dwarf_flows rows across shards
	Unwedged     int64   `json:"unwedged"`     // alarm: must stay 0
	Orphaned     int64   `json:"orphaned"`     // alarm: must stay 0
	WriteFailed  int64   `json:"writeFailed"`  // alarm: must stay 0
	Recovered    int64   `json:"recovered"`    // lease recoveries since soak start (watch)
	WriteRetried int64   `json:"writeRetried"` // transient write retries absorbed (watch)
	// Database-side readings (Postgres only; zero elsewhere): server connection count and the on-disk
	// size of the two dwarf tables including indexes/TOAST - the bloat-under-churn signal row counts
	// cannot show.
	DBConnections int     `json:"dbConnections,omitzero"`
	StepsDiskMB   float64 `json:"stepsDiskMB,omitzero"`
	FlowsDiskMB   float64 `json:"flowsDiskMB,omitzero"`
}

// soakReport is the soak's artifact section.
type soakReport struct {
	DurationSec   float64          `json:"durationSec"`
	Concurrency   int              `json:"concurrency"`
	Ops           map[string]int64 `json:"ops"`      // per operation-type success count
	OpErrors      map[string]int64 `json:"opErrors"` // per operation-type error count
	Purged        int64            `json:"purged"`   // roots removed by the periodic retention sweep
	Samples       []soakSample     `json:"samples"`
	FinalCounters map[string]int64 `json:"finalCounters"` // all dwarf_* deltas over the soak
}

// runSoak runs the soak for dur with k concurrent submitters round-robined across the replicas, sampling
// every sampleEvery. k=0 is drain/observe mode: no submitters, just the engines recovering and reaping
// whatever the database already holds - the post-crash verification shape. The interrupt graph is
// registered by registerWorkloads, so nothing mutates the shared registry after the engines start.
func runSoak(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader,
	dbs *dbStatsSampler, pick func() *workload, k int, dur, sampleEvery time.Duration) *soakReport {

	var (
		stop       atomic.Bool
		opMu       sync.Mutex
		ops        = map[string]int64{}
		opErrs     = map[string]int64{}
		purged     atomic.Int64
		submitters sync.WaitGroup
	)
	record := func(name string, err error) {
		opMu.Lock()
		if err != nil {
			opErrs[name]++
		} else {
			ops[name]++
		}
		opMu.Unlock()
	}

	// Submitters: each is pinned to one replica (a client behind a load balancer) and loops picking a
	// weighted-random operation until stopped. Disposable flows dominate (the create-complete-reap steady
	// state); slices of interrupt/resume, continue-chain, and fork exercise the parked-step, thread, and
	// clone paths. Execution still distributes cluster-wide because any replica can claim any pending
	// step. k=0 spawns no submitters (drain/observe mode).
	for i := range k {
		eng := engines[i%len(engines)]
		submitters.Go(func() {
			for !stop.Load() {
				switch r := rand.IntN(100); {
				case r < 74:
					record("disposable", runDisposable(ctx, eng, pick))
				case r < 86:
					record("interruptResume", runInterruptResume(ctx, eng, "bench/interrupt"))
				case r < 96:
					record("continueChain", runContinueChain(ctx, eng))
				default:
					record("forkChain", runForkChain(ctx, eng))
				}
			}
		})
	}

	// Retention sweep: List a page of live flows (the cross-shard read path under load), then Purge
	// completed non-disposable flows (continue chains, fork sources) once they age past the grace
	// window, so their rows don't grow unbounded and the operator-driven Purge path is exercised
	// alongside the DeleteOnCompletion reaper.
	stopCh := make(chan struct{})
	var sweeper sync.WaitGroup
	sweeper.Go(func() {
		t := time.NewTicker(60 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-t.C:
				pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_, _, _ = engines[0].List(pctx, workflow.Query{Limit: 100})
				n, err := engines[0].Purge(pctx, workflow.Query{Status: workflow.StatusCompleted, OlderThan: 90 * time.Second})
				cancel()
				if err == nil {
					purged.Add(int64(n))
				}
			}
		}
	})

	report := &soakReport{Concurrency: k}
	base := collectAllCounters(readers)
	start := time.Now()
	takeSample := func() {
		s := soakSampleNow(ctx, engines[0], readers, dbs, base, start)
		report.Samples = append(report.Samples, s)
		fmt.Printf("%-6.0f %8d %9.1f %9.1f %8d %8d %7d %8.1f %8.1f %9d %9d %8d %8d\n",
			s.TSec, s.Goroutines, s.HeapAllocMB, s.HeapSysMB, s.Steps, s.Flows,
			s.DBConnections, s.StepsDiskMB, s.FlowsDiskMB,
			s.Unwedged, s.Orphaned, s.Recovered, s.WriteRetried)
	}

	fmt.Printf("%-6s %8s %9s %9s %8s %8s %7s %8s %8s %9s %9s %8s %8s\n",
		"t(s)", "gorout", "heapMB", "heapSysMB", "steps", "flows", "dbconn", "stepsMB", "flowsMB", "unwedged", "orphaned", "recov", "wretry")
	sampler := time.NewTicker(sampleEvery)
	deadline := time.After(dur)
loop:
	for {
		select {
		case <-sampler.C:
			takeSample()
		case <-deadline:
			break loop
		}
	}
	sampler.Stop()

	stop.Store(true)
	close(stopCh)
	submitters.Wait()
	sweeper.Wait()

	takeSample() // final reading after the load drains
	report.DurationSec = time.Since(start).Seconds()
	report.Purged = purged.Load()
	report.Ops = ops
	report.OpErrors = opErrs
	report.FinalCounters = counterDeltas(base, collectAllCounters(readers))
	return report
}

// runDisposable creates and awaits one mixed-shape flow marked DeleteOnCompletion (the reaper removes it
// after the grace window), occasionally reading its outcome back via Snapshot during that window. Each
// flow carries a fuzzed extra payload - random size (0..8KB) and content - so state rows vary realistically
// rather than repeating one fixed shape.
func runDisposable(ctx context.Context, eng *engine.Engine, pick func() *workload) error {
	w := pick()
	state := w.initialState()
	if state == nil {
		state = map[string]any{}
	}
	state["fuzz"] = randAlnum(rand.IntN(8 << 10))
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	key, out, err := eng.Run(cctx, w.graphURL, state, &workflow.FlowOptions{DeleteOnCompletion: true})
	if err != nil {
		return err
	}
	if out.Status != workflow.StatusCompleted {
		return fmt.Errorf("disposable ended %s", out.Status)
	}
	if rand.IntN(5) == 0 {
		if _, err := eng.Snapshot(cctx, key); err != nil {
			return err
		}
	}
	return nil
}

// runForkChain runs a linear flow to completion, then forks it from a mid step (via History) and awaits
// the fork - the clone path under load. Both flows are non-disposable; the retention sweep purges them.
func runForkChain(ctx context.Context, eng *engine.Engine) error {
	cctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	key, out, err := eng.Run(cctx, "bench/linear", nil, nil)
	if err != nil {
		return err
	}
	if out.Status != workflow.StatusCompleted {
		return fmt.Errorf("fork source ended %s", out.Status)
	}
	hist, err := eng.History(cctx, key)
	if err != nil {
		return err
	}
	if len(hist) == 0 {
		return fmt.Errorf("fork source has empty history")
	}
	fkey, err := eng.Fork(cctx, hist[len(hist)/2].StepKey, map[string]any{"forked": true})
	if err != nil {
		return err
	}
	fout, err := eng.Await(cctx, fkey)
	if err != nil {
		return err
	}
	if fout.Status != workflow.StatusCompleted {
		return fmt.Errorf("fork ended %s", fout.Status)
	}
	return nil
}

// randAlnum returns n random alphanumeric bytes (incompressible enough to keep byte counts honest, and
// JSON-escape-free so the stored size matches).
func randAlnum(n int) string {
	const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alnum[rand.IntN(len(alnum))]
	}
	return string(b)
}

// runInterruptResume creates a flow that interrupts at its entry gate, awaits the interrupted state,
// resumes it, and awaits completion - exercising the park/resume path under load.
func runInterruptResume(ctx context.Context, eng *engine.Engine, url string) error {
	cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	key, err := eng.Create(cctx, url, nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	if err != nil {
		return err
	}
	out, err := eng.Await(cctx, key)
	if err != nil {
		return err
	}
	if out.Status != workflow.StatusInterrupted {
		return fmt.Errorf("expected interrupted, got %s", out.Status)
	}
	if err := eng.Resume(cctx, key, nil); err != nil {
		return err
	}
	out, err = eng.Await(cctx, key)
	if err != nil {
		return err
	}
	if out.Status != workflow.StatusCompleted {
		return fmt.Errorf("post-resume ended %s", out.Status)
	}
	return nil
}

// runContinueChain runs a linear flow, reads its History, then continues the thread two more turns -
// exercising Continue/threads and the History reader. These flows are not disposable; the retention sweep
// purges them once they age.
func runContinueChain(ctx context.Context, eng *engine.Engine) error {
	cctx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	key, out, err := eng.Run(cctx, "bench/linear", nil, nil)
	if err != nil {
		return err
	}
	if out.Status != workflow.StatusCompleted {
		return fmt.Errorf("continue turn 0 ended %s", out.Status)
	}
	if _, err := eng.History(cctx, key); err != nil {
		return err
	}
	for turn := range 2 {
		nk, err := eng.Continue(cctx, key, map[string]any{"turn": turn})
		if err != nil {
			return err
		}
		out, err := eng.Await(cctx, nk)
		if err != nil {
			return err
		}
		if out.Status != workflow.StatusCompleted {
			return fmt.Errorf("continue turn %d ended %s", turn+1, out.Status)
		}
		key = nk
	}
	return nil
}

// soakSampleNow reads the current drift indicators: goroutines and heap from the Go runtime, live row
// counts from ShardInfo, and the alarm/watch counter deltas since the soak began.
func soakSampleNow(ctx context.Context, eng *engine.Engine, readers []*sdkmetric.ManualReader,
	dbs *dbStatsSampler, base map[string]int64, start time.Time) soakSample {

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	cur := collectAllCounters(readers)
	dbst := dbs.sample(ctx)

	var steps, flows int
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	if infos, err := eng.ShardInfo(sctx); err == nil {
		for _, s := range infos {
			steps += s.Steps
			flows += s.Flows
		}
	}
	cancel()

	return soakSample{
		TSec:          time.Since(start).Seconds(),
		Goroutines:    runtime.NumGoroutine(),
		HeapAllocMB:   float64(ms.HeapAlloc) / (1 << 20),
		HeapSysMB:     float64(ms.HeapSys) / (1 << 20),
		Steps:         steps,
		Flows:         flows,
		Unwedged:      cur["dwarf_steps_unwedged"] - base["dwarf_steps_unwedged"],
		Orphaned:      cur["dwarf_flows_orphaned"] - base["dwarf_flows_orphaned"],
		WriteFailed:   cur["dwarf_steps_write_failed"] - base["dwarf_steps_write_failed"],
		Recovered:     cur["dwarf_steps_recovered"] - base["dwarf_steps_recovered"],
		WriteRetried:  cur["dwarf_steps_write_retried"] - base["dwarf_steps_write_retried"],
		DBConnections: dbst.Connections,
		StepsDiskMB:   float64(dbst.StepsBytes) / (1 << 20),
		FlowsDiskMB:   float64(dbst.FlowsBytes) / (1 << 20),
	}
}
