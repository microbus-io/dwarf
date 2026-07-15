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

// Throughput / latency benchmarks for the engine, exercised through the public API against whatever dialect
// SEQUEL_TESTING_DSN points at (unset = in-memory SQLite). They answer the questions no amount of review can:
// flows/sec and steps/sec per dialect per shard count, and dispatch-latency percentiles. See docs/benchmark.md
// for captured results and interpretation. Run one dialect at a time, e.g.:
//
//	go test ./fixtures/ -run=X -bench=BenchmarkFlowThroughput -benchtime=300x   # SQLite (no env)
//	SEQUEL_TESTING_DSN='postgres://root:secret1234@127.0.0.1:5432/dwarfbench_%d?sslmode=disable' \
//	  go test ./fixtures/ -run=X -bench=BenchmarkFlowThroughput -benchtime=300x
//
// -run=X selects no unit tests (so only benchmarks run); -benchtime=NNNx fixes the flow count so each shard
// count does one measured pass (no b.N auto-scaling re-creating the isolated DB repeatedly). The reported
// custom metrics are the signal: flows/s, steps/s, and p50/p95/p99 end-to-end latency in ms. Go's own ns/op
// is elapsed/flow amortized across the concurrent submitters and is the less useful number here.
//
// On the shard dimension: these benchmarks run all shards as separate databases on ONE host (the local
// dev/CI topology), so higher shard counts do NOT boost throughput - they add connection/coordination overhead
// against the same server and typically cost a little. The shard sub-benchmarks are a distributed-routing
// *reliability* check (does a multi-DB engine still complete every flow correctly?) and a measure of the
// co-located overhead shape, NOT a scaling demo. The engine's real horizontal-scale story is shard-per-server,
// which this single-host harness cannot exhibit.
package fixtures

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
)

// benchSeq makes each benchEngine's isolation key unique. Go re-invokes a benchmark function several times
// (warmup then measure) with the SAME b.Name(); reusing it as the SetInTest key would collide with
// CreateTestingDatabase's create-once cache, whose entry points at a database the previous invocation's
// Shutdown already dropped ("database does not exist"). A per-call suffix gives each invocation its own fresh
// isolated database set.
var benchSeq atomic.Uint64

// benchConcurrency is the number of client goroutines submitting flows in parallel (offered load). Fixed
// rather than GOMAXPROCS-derived so a run is comparable across machines; matched to the worker pool so the
// pool stays saturated and the number measures engine throughput, not client starvation.
const benchConcurrency = 32

// benchWorkers is the engine worker-pool size, fixed for reproducibility across dialects/shard counts.
const benchWorkers = 32

// benchMaxOpenConns is the per-shard connection pool, pinned exactly via SetMaxOpenConns. The test default
// (8) is sized to keep many parallel CI test engines under a server's connection cap; a single benchmark
// engine wants far more or the pool, not the engine, is what's measured. 30 per shard keeps
// shards * pool under PostgreSQL's default max_connections=100 at 3 co-located shards (≈90). MySQL (151)
// and SQL Server (~32k) have more headroom.
const benchMaxOpenConns = 30

// benchChainLen is the number of task executions per flow (A -> B -> C -> ... -> END). steps/sec derives from
// flows/sec times this, so a graph change stays reflected in the reported step rate.
const benchChainLen = 3

// benchEngine stands up an engine with the given shard count against the ambient dialect (SEQUEL_TESTING_DSN,
// or in-memory SQLite when unset), wired to a linear chain graph of benchChainLen trivial tasks. It uses the
// default discard logger (NOT RunInTest, which forces stderr Info logging that would dominate the timing) and
// registers cleanup via b.Cleanup. Returns the engine and the workflow URL to Run.
// setShards registers numShards test-mode shards (empty DSNs resolve per shard at Startup).
func setShards(eng *engine.Engine, numShards int) error {
	for i := 1; i <= numShards; i++ {
		err := eng.SetShard(engine.ShardSpec{Index: i})
		if err != nil {
			return err
		}
	}
	return nil
}

func benchEngine(b *testing.B, numShards int) (*engine.Engine, string) {
	b.Helper()

	proxy := engine.NewTestProxy()
	const prefix = "bench.verify:428"
	g := workflow.NewGraph("Bench")
	prev := ""
	for i := 0; i < benchChainLen; i++ {
		node := fmt.Sprintf("T%d", i)
		url := fmt.Sprintf("%s/t%d", prefix, i)
		g.SetEndpoint(node, url)
		proxy.HandleTask(url, func(ctx context.Context, f *workflow.Flow) error { return nil })
		if prev != "" {
			g.AddTransition(prev, node)
		}
		prev = node
	}
	g.AddTransition(prev, workflow.END)
	proxy.HandleGraph(prefix+"/g", g)

	eng := engine.NewEngine()
	if err := eng.SetHost(proxy); err != nil {
		b.Fatal(err)
	}
	if err := eng.SetWorkers(benchWorkers); err != nil {
		b.Fatal(err)
	}
	if err := eng.SetMaxOpenConns(benchMaxOpenConns); err != nil {
		b.Fatal(err)
	}
	if err := setShards(eng, numShards); err != nil {
		b.Fatal(err)
	}
	// SetInTest (not RunInTest) so the engine keeps its silent discard logger and no *testing.T logging path.
	// The key carries a per-invocation sequence (see benchSeq) so each measured pass gets its own isolated,
	// auto-dropped database set rather than colliding with a prior (already-dropped) invocation's DB.
	if err := eng.SetInTest(fmt.Sprintf("%s-%d", b.Name(), benchSeq.Add(1))); err != nil {
		b.Fatal(err)
	}
	if err := eng.Startup(context.Background()); err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { eng.Shutdown(context.Background()) })
	return eng, prefix + "/g"
}

// BenchmarkFlowThroughput measures sustained flow-completion throughput and end-to-end latency across shard
// counts. benchConcurrency client goroutines each Run (Create+Await) a share of b.N flows through a
// benchChainLen-step linear chain; the engine's worker pool does the dispatching. Reports flows/s, steps/s,
// and p50/p95/p99 latency (ms).
func BenchmarkFlowThroughput(b *testing.B) {
	// 1..3 shards is enough to show the co-located overhead trend; more just piles connections onto one host.
	for _, numShards := range []int{1, 2, 3} {
		b.Run(fmt.Sprintf("shards=%d", numShards), func(b *testing.B) {
			eng, url := benchEngine(b, numShards)
			elapsed, latencies := driveFlows(b, eng, url, nil)
			reportFlowMetrics(b, elapsed, latencies)
		})
	}
}

// BenchmarkStatePayload measures how state size affects throughput: a JSON string of the given size seeds the
// flow's initial state and rides through every step's `state` column (each step persists merge(prevState,
// changes), and no-op tasks carry state forward unchanged), so the engine serializes + writes + re-merges the
// whole payload benchChainLen times per flow. Reports the usual flows/s + latency plus payloadMB/s = payload
// size x steps/s: the rate at which caller state bytes move through durable storage (persisted bytes are ~this
// x the chain length, since every step stores its own copy). Fixed at 1 shard to isolate the payload effect.
func BenchmarkStatePayload(b *testing.B) {
	for _, sz := range []struct {
		name  string
		bytes int
	}{
		{"0KB", 0}, {"4KB", 4 << 10}, {"64KB", 64 << 10}, {"256KB", 256 << 10}, {"1MB", 1 << 20},
	} {
		b.Run(sz.name, func(b *testing.B) {
			eng, url := benchEngine(b, 1)
			var initial any
			if sz.bytes > 0 {
				initial = map[string]any{"payload": strings.Repeat("x", sz.bytes)}
			}
			elapsed, latencies := driveFlows(b, eng, url, initial)
			reportFlowMetrics(b, elapsed, latencies)
			if sz.bytes > 0 && elapsed > 0 {
				stepsPerSec := float64(len(latencies)) * float64(benchChainLen) / elapsed.Seconds()
				b.ReportMetric(float64(sz.bytes)*stepsPerSec/(1<<20), "payloadMB/s")
			}
		})
	}
}

// driveFlows runs b.N flows through url (each via Run = create+await) with benchConcurrency client goroutines,
// timing only the concurrent submission. Returns the wall-clock elapsed and the per-flow latencies (one per
// job index, so no shared-slot race). Shared by the throughput and payload benchmarks.
func driveFlows(b *testing.B, eng *engine.Engine, url string, initialState any) (time.Duration, []time.Duration) {
	b.Helper()
	ctx := context.Background()
	latencies := make([]time.Duration, b.N)
	jobs := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		jobs <- i
	}
	close(jobs)

	b.ResetTimer()
	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < benchConcurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				t0 := time.Now()
				_, out, err := eng.Run(ctx, url, initialState, nil)
				latencies[i] = time.Since(t0) // unique i per job: no shared-index race
				if err != nil {
					b.Errorf("Run: %v", err)
					return
				}
				if out == nil || out.Status != workflow.StatusCompleted {
					b.Errorf("flow did not complete: %+v", out)
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	b.StopTimer()
	return elapsed, latencies
}

// reportFlowMetrics attaches the throughput and latency-percentile custom metrics to the benchmark result.
func reportFlowMetrics(b *testing.B, elapsed time.Duration, latencies []time.Duration) {
	b.Helper()
	flows := float64(len(latencies))
	secs := elapsed.Seconds()
	if secs > 0 {
		b.ReportMetric(flows/secs, "flows/s")
		b.ReportMetric(flows*float64(benchChainLen)/secs, "steps/s")
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	pctl := func(p float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		idx := int(p * float64(len(latencies)))
		if idx >= len(latencies) {
			idx = len(latencies) - 1
		}
		return float64(latencies[idx].Microseconds()) / 1000.0 // ms
	}
	b.ReportMetric(pctl(0.50), "p50_ms")
	b.ReportMetric(pctl(0.95), "p95_ms")
	b.ReportMetric(pctl(0.99), "p99_ms")
}
