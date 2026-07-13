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

// The bench command drives the dwarf engine through its public API against a real database and measures
// step/flow/MB throughput and latency at a sweep of closed-loop concurrencies. It is the harness of the
// cloud benchmark campaign: a minimal proxy host (no transport framework), an RTT sampler for
// the database round-trip term of the sizing model, and one self-contained JSON artifact per run.
//
// Example (local smoke, SQLite):
//
//	go run ./bench -window 10s -warmup 2s -concurrency 8,32
//
// Example (RDS PostgreSQL):
//
//	go run ./bench -dsn 'postgres://user:pass@db.xyz.us-east-1.rds.amazonaws.com:5432/dwarf' \
//	  -workload linear -workers 64 -max-open-conns 30 -concurrency 8,16,32,64,128,256 \
//	  -window 5m -warmup 60s -label baseline
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// artifact is the self-contained record of one run: full configuration + environment + results, so
// aggregation across a sweep is mechanical.
type artifact struct {
	Label       string         `json:"label,omitzero"`
	StartedAt   time.Time      `json:"startedAt"`
	EndedAt     time.Time      `json:"endedAt"`
	Config      map[string]any `json:"config"`
	Environment map[string]any `json:"environment"`
	RTT         rttStats       `json:"rtt"`
	Results     []stepResult   `json:"results"`
	Valid       bool           `json:"valid"` // false when recovery/unwedge counters fired
	Invalidity  string         `json:"invalidity,omitzero"`
}

func main() {
	err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		dsns         shardFlags
		workloadName = flag.String("workload", "linear", "workload: linear, fanout, state, llm, mixed")
		payload      = flag.Int("payload", 64*1024, "state-workload payload bytes written per step")
		taskDelay    = flag.Duration("task-delay", 0, "per-task sleep simulating remote executor latency (the exec term)")
		// 0 on either override means "let the engine derive it" - the self-tuned path a production host
		// takes, and the only way to measure the derivation itself (workers from the lease margin, the
		// pool from VirtualCPUs). A sweep pins them; a validation run must not.
		workers = flag.Int("workers", 0, "engine worker goroutines (0 = derived from the lease-margin ceiling)")
		vcpus   = flag.Int("vcpus", 0, "ShardSpec.VirtualCPUs for every shard (0 = undeclared; the engine assumes 2)")
		// SetMaxOpenConns is the expert override: it pins every shard's pool to exactly this size, which
		// is what a pool-size sweep needs.
		maxOpenConns = flag.Int("max-open-conns", 0, "engine per-shard pool size (0 = derived from -vcpus; else pinned exactly)")
		concurrency  = flag.String("concurrency", "8,16,32,64,128", "comma-separated closed-loop submitter counts to sweep")
		window       = flag.Duration("window", 60*time.Second, "measurement window per concurrency step")
		warmup       = flag.Duration("warmup", 15*time.Second, "warmup before each measurement window (discarded)")
		label        = flag.String("label", "", "free-form run label recorded in the artifact")
		out          = flag.String("out", "", "artifact path (default bench/results/run-<timestamp>.json)")
	)
	flag.Var(&dsns, "dsn", "shard DSN, repeatable; 'N=dsn' sets shard N, a bare dsn sets shard 1 (default: throwaway local SQLite)")
	flag.Parse()

	if len(dsns) == 0 {
		// Out-of-the-box smoke run: a throwaway on-disk SQLite database.
		dir, err := os.MkdirTemp("", "dwarf-bench-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		dsns = shardFlags{{index: 1, dsn: "file:" + filepath.Join(dir, "bench.sqlite") + "?_pragma=busy_timeout(5000)"}}
		fmt.Println("no -dsn given; using throwaway local SQLite (smoke mode)")
	}
	ks, err := parseConcurrency(*concurrency)
	if err != nil {
		return err
	}

	// Wire the engine through the public API only - the same path a production host takes.
	host := &benchHost{
		graphs:    map[string]*workflow.Graph{},
		tasks:     map[string]func(context.Context, *workflow.Flow) error{},
		taskDelay: *taskDelay,
	}
	workloads := registerWorkloads(host, *payload)
	pick, err := chooseWorkload(workloads, *workloadName)
	if err != nil {
		return err
	}
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	eng := engine.NewEngine()
	for _, sh := range dsns {
		err = eng.SetShard(engine.ShardSpec{Index: sh.index, DSN: sh.dsn, VirtualCPUs: *vcpus})
		if err != nil {
			return err
		}
	}
	setters := []error{
		eng.SetHost(host),
		eng.SetMeterProvider(mp),
	}
	if *workers > 0 {
		setters = append(setters, eng.SetWorkers(*workers))
	}
	if *maxOpenConns > 0 {
		setters = append(setters, eng.SetMaxOpenConns(*maxOpenConns))
	}
	for _, setter := range setters {
		if setter != nil {
			return setter
		}
	}
	ctx := context.Background()
	err = eng.Startup(ctx)
	if err != nil {
		return err
	}
	defer eng.Shutdown(ctx)

	rtt, err := startRTTSampler(dsns[0].dsn)
	if err != nil {
		return err
	}
	defer rtt.close()

	art := artifact{
		Label:     *label,
		StartedAt: time.Now().UTC(),
		Config: map[string]any{
			"workload":     *workloadName,
			"payloadBytes": *payload,
			"taskDelayMs":  taskDelay.Milliseconds(),
			"workers":      *workers,
			"virtualCPUs":  *vcpus,
			"maxOpenConns": *maxOpenConns,
			"shards":       dsns.redacted(),
			"windowSec":    window.Seconds(),
			"warmupSec":    warmup.Seconds(),
		},
		Environment: environment(),
		Valid:       true,
	}

	fmt.Printf("%-6s %10s %10s %10s %8s %8s %8s %8s %8s\n", "conc", "flows/s", "steps/s", "MB/s", "p50ms", "p95ms", "p99ms", "gorout", "errors")
	for _, k := range ks {
		res := runStep(ctx, eng, host, reader, pick, k, *warmup, *window)
		art.Results = append(art.Results, res)
		fmt.Printf("%-6d %10.1f %10.1f %10.2f %8.1f %8.1f %8.1f %8d %8d\n",
			k, res.FlowsPerSec, res.StepsPerSec, res.MBPerSec, res.P50ms, res.P95ms, res.P99ms, res.Goroutines, res.Errors)
		if res.EngineCounters["dwarf_steps_recovered"] != 0 || res.EngineCounters["dwarf_steps_unwedged"] != 0 || res.Errors != 0 {
			art.Valid = false
			art.Invalidity = fmt.Sprintf("at concurrency %d: errors=%d recovered=%d unwedged=%d",
				k, res.Errors, res.EngineCounters["dwarf_steps_recovered"], res.EngineCounters["dwarf_steps_unwedged"])
		}
	}
	art.EndedAt = time.Now().UTC()
	art.RTT = rtt.stats()
	fmt.Printf("rtt: p50 %.2fms p95 %.2fms (%d samples)  valid: %v\n", art.RTT.P50ms, art.RTT.P95ms, art.RTT.Samples, art.Valid)
	if !art.Valid {
		fmt.Println("INVALID RUN:", art.Invalidity)
	}

	path := *out
	if path == "" {
		err = os.MkdirAll("bench/results", 0o755)
		if err != nil {
			return err
		}
		path = filepath.Join("bench/results", "run-"+art.StartedAt.Format("20060102-150405")+".json")
	}
	data, err := json.MarshalIndent(art, "", "  ")
	if err != nil {
		return err
	}
	err = os.WriteFile(path, data, 0o644)
	if err != nil {
		return err
	}
	fmt.Println("artifact:", path)
	return nil
}

// environment records the host-side context a result depends on.
func environment() map[string]any {
	hostname, _ := os.Hostname()
	return map[string]any{
		"hostname":  hostname,
		"goVersion": runtime.Version(),
		"goos":      runtime.GOOS,
		"goarch":    runtime.GOARCH,
		"numCPU":    runtime.NumCPU(),
	}
}

// shardFlags collects repeatable -dsn flags: "N=dsn" registers shard N, a bare DSN registers shard 1.
type shardFlags []struct {
	index int
	dsn   string
}

func (s *shardFlags) String() string {
	parts := make([]string, len(*s))
	for i, sh := range *s {
		parts[i] = strconv.Itoa(sh.index)
	}
	return strings.Join(parts, ",")
}

func (s *shardFlags) Set(v string) error {
	index := 1
	dsn := v
	if n, rest, ok := strings.Cut(v, "="); ok {
		if i, err := strconv.Atoi(n); err == nil {
			index = i
			dsn = rest
		}
	}
	*s = append(*s, struct {
		index int
		dsn   string
	}{index, dsn})
	return nil
}

// redacted returns the shard indices and DSNs with any password stripped, for the artifact.
func (s shardFlags) redacted() map[string]string {
	out := map[string]string{}
	for _, sh := range s {
		out[strconv.Itoa(sh.index)] = redactDSN(sh.dsn)
	}
	return out
}

// redactDSN strips the password from URL-style DSNs so credentials never land in an artifact.
func redactDSN(dsn string) string {
	at := strings.Index(dsn, "@")
	scheme := strings.Index(dsn, "://")
	if at > 0 && scheme > 0 && at > scheme {
		userinfo := dsn[scheme+3 : at]
		if colon := strings.Index(userinfo, ":"); colon >= 0 {
			return dsn[:scheme+3] + userinfo[:colon] + ":REDACTED" + dsn[at:]
		}
	}
	return dsn
}

// parseConcurrency parses the comma-separated -concurrency list.
func parseConcurrency(s string) ([]int, error) {
	var ks []int
	for part := range strings.SplitSeq(s, ",") {
		k, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || k < 1 {
			return nil, fmt.Errorf("invalid -concurrency %q", part)
		}
		ks = append(ks, k)
	}
	return ks, nil
}
