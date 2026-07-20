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
	"sync/atomic"
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
	Soak        *soakReport    `json:"soak,omitzero"`   // set in -soak mode instead of Results
	Volume      *volumeReport  `json:"volume,omitzero"` // set in -volume mode instead of Results
	Valid       bool           `json:"valid"`           // false when recovery/unwedge counters fired
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
		taskJitter   = flag.Duration("task-jitter", 0, "additional uniform random [0,d) per-task sleep - de-synchronizes fan-out siblings so a cohort's branches do not all complete at once")
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
		soak         = flag.Duration("soak", 0, "soak mode: run a mixed create/interrupt/continue workload under create-purge churn for this long, sampling drift, instead of the concurrency sweep (uses the first -concurrency value as the submitter count)")
		soakSample   = flag.Duration("soak-sample", 30*time.Second, "soak drift sampling interval")
		replicas     = flag.Int("replicas", 1, "number of in-process engine replicas sharing the database(s), peered for signal relay (1 = single engine)")
		// Signal-fault injection on the peer transport (see benchHost). Jitter/drop simulate an imperfect
		// network; -drop-ops kills specific signal kinds outright (e.g. 'enqueue' for the poll-fallback
		// A/B); the mute pair simulates one replica crashing (its outbound signals stop) and recovering.
		signalJitter   = flag.Duration("signal-jitter", 0, "delay each peer signal by a uniform random [0,d) - simulated network latency")
		signalDrop     = flag.Float64("signal-drop", 0, "probability [0,1) of dropping each per-peer signal delivery - simulated loss")
		dropOps        = flag.String("drop-ops", "", "comma-separated signal ops to drop entirely (e.g. 'enqueue')")
		muteAfter      = flag.Duration("mute-replica-after", 0, "mute the LAST replica's outbound signals this long after startup (simulated peer crash; 0 = never)")
		muteDuration   = flag.Duration("mute-duration", 0, "unmute again after this long muted (0 = stay muted)")
		replicaWorkers = flag.String("replica-workers", "", "comma-separated per-replica worker counts overriding -workers (e.g. '0,4' = replica 1 awaits only)")
		volume         = flag.Int("volume", 0, "volume mode: fill to this many dwarf_steps rows with NO deletion, probing the read paths at checkpoints, then measure Purge/reaper throughput (0 = disabled)")
		volumeCheckpt  = flag.Int("volume-checkpoint", 0, "probe every this many step rows during -volume (0 = one fifth of the target)")
		fairnessKeys   = flag.Int("fairness-keys", 1, "spread flows across this many distinct fairness keys (1 = all on the single default key, the degenerate case for the refiller's per-key scan bound)")
		fanOutWidth    = flag.Int("fanout-width", 16, "forEach branch count for the fanout workload - the knob that decouples pending-step backlog from submitter concurrency (backlog ~= concurrency x width)")
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

	if *replicas < 1 {
		return fmt.Errorf("-replicas must be >= 1")
	}
	// Per-replica worker overrides: "-replica-workers 0,4" pins replica 1 to 0 workers (await-only) and
	// replica 2 to 4. Entries beyond the list fall back to -workers. -1 marks "use -workers".
	perReplicaWorkers := make([]int, *replicas)
	for i := range perReplicaWorkers {
		perReplicaWorkers[i] = -1
	}
	if *replicaWorkers != "" {
		parts := strings.Split(*replicaWorkers, ",")
		if len(parts) > *replicas {
			return fmt.Errorf("-replica-workers names %d replicas but -replicas is %d", len(parts), *replicas)
		}
		for i, part := range parts {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n < 0 {
				return fmt.Errorf("invalid -replica-workers entry %q", part)
			}
			perReplicaWorkers[i] = n
		}
	}
	dropOpsSet := map[string]bool{}
	for op := range strings.SplitSeq(*dropOps, ",") {
		if op = strings.TrimSpace(op); op != "" {
			dropOpsSet[op] = true
		}
	}

	// One graph/task registry and one byte counter, shared by every replica's host (registered through the
	// public API only - the path a production host takes).
	sharedBytes := &atomic.Int64{}
	regHost := &benchHost{
		graphs:       map[string]*workflow.Graph{},
		tasks:        map[string]func(context.Context, *workflow.Flow) error{},
		taskDelay:    *taskDelay,
		taskJitter:   *taskJitter,
		bytesWritten: sharedBytes,
	}
	workloads := registerWorkloads(regHost, *payload, *fanOutWidth)
	pick, err := chooseWorkload(workloads, *workloadName)
	if err != nil {
		return err
	}

	// Build the fleet of in-process engine replicas. Each shares the registry above and the same
	// database(s), so they contend for every pending step at the claim CAS - exercising peer discovery,
	// the connection pool-split (open/R per replica), and the cross-replica doorbell/wake. Each has its own
	// metrics reader (summed across the fleet) and its own view of the fleet for signal relay. NOTE: one
	// process, so goroutine/heap readings are a fleet total and there is no per-replica RSS attribution or
	// kill-9 isolation - those need separate processes (a follow-up).
	ctx := context.Background()
	engines := make([]*engine.Engine, *replicas)
	hosts := make([]*benchHost, *replicas)
	readers := make([]*sdkmetric.ManualReader, *replicas)
	for i := range *replicas {
		reader := sdkmetric.NewManualReader()
		readers[i] = reader
		host := &benchHost{
			graphs:       regHost.graphs,
			tasks:        regHost.tasks,
			taskDelay:    *taskDelay,
			taskJitter:   *taskJitter,
			bytesWritten: sharedBytes,
			signalJitter: *signalJitter,
			signalDrop:   *signalDrop,
			dropOps:      dropOpsSet,
		}
		hosts[i] = host
		eng := engine.NewEngine()
		for _, sh := range dsns {
			err = eng.SetShard(engine.ShardSpec{Index: sh.index, DSN: sh.dsn, VirtualCPUs: *vcpus})
			if err != nil {
				return err
			}
		}
		setters := []error{
			eng.SetHost(host),
			eng.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
		}
		replicaWorkerCount := perReplicaWorkers[i]
		if replicaWorkerCount < 0 {
			replicaWorkerCount = *workers
		}
		if replicaWorkerCount > 0 || perReplicaWorkers[i] == 0 {
			setters = append(setters, eng.SetWorkers(replicaWorkerCount))
		}
		if *maxOpenConns > 0 {
			setters = append(setters, eng.SetMaxOpenConns(*maxOpenConns))
		}
		for _, setter := range setters {
			if setter != nil {
				return setter
			}
		}
		engines[i] = eng
	}
	// Wire every host to the whole fleet (self-echo discarded by Origin), then start all replicas.
	for _, host := range hosts {
		host.peers = engines
	}
	for _, eng := range engines {
		err = eng.Startup(ctx)
		if err != nil {
			return err
		}
		defer eng.Shutdown(ctx)
	}

	// Simulated peer crash: mute the last replica's outbound signals at T (its pings stop; the rest of
	// the fleet evicts it after ~3x pingInterval and regrows pools), optionally unmuting later (it
	// "rejoins" on its next ping and pools re-split). The engine keeps running - only its voice goes.
	if *muteAfter > 0 && *replicas > 1 {
		mutee := hosts[len(hosts)-1]
		time.AfterFunc(*muteAfter, func() {
			fmt.Printf("--- muting replica %d (simulated crash)\n", len(hosts))
			mutee.muted.Store(true)
			if *muteDuration > 0 {
				time.AfterFunc(*muteDuration, func() {
					fmt.Printf("--- unmuting replica %d (rejoin)\n", len(hosts))
					mutee.muted.Store(false)
				})
			}
		})
	}

	rtt, err := startRTTSampler(dsns[0].dsn)
	if err != nil {
		return err
	}
	defer rtt.close()

	// Database-side drift sampler (Postgres only; nil elsewhere): server connection count + on-disk
	// table/index sizes, the two signals the process cannot observe from inside.
	dbs, err := startDBStatsSampler(dsns[0].dsn)
	if err != nil {
		return err
	}
	defer dbs.close()

	// Server-side statement timings (Postgres with pg_stat_statements preloaded; nil elsewhere). Like
	// the two samplers above it watches shard 1 only - the local-harness shape; multi-shard attribution
	// wants a sampler per shard.
	pgss := startPgssSampler(dsns[0].dsn)
	defer pgss.close()

	art := artifact{
		Label:     *label,
		StartedAt: time.Now().UTC(),
		Config: map[string]any{
			"workload":        *workloadName,
			"payloadBytes":    *payload,
			"taskDelayMs":     taskDelay.Milliseconds(),
			"workers":         *workers,
			"virtualCPUs":     *vcpus,
			"maxOpenConns":    *maxOpenConns,
			"replicas":        *replicas,
			"replicaWorkers":  *replicaWorkers,
			"signalJitterMs":  signalJitter.Milliseconds(),
			"signalDrop":      *signalDrop,
			"dropOps":         *dropOps,
			"muteAfterSec":    muteAfter.Seconds(),
			"muteDurationSec": muteDuration.Seconds(),
			"shards":          dsns.redacted(),
			"windowSec":       window.Seconds(),
			"warmupSec":       warmup.Seconds(),
		},
		Environment: environment(),
		Valid:       true,
	}

	if *volume > 0 {
		every := *volumeCheckpt
		if every <= 0 {
			every = max(1, *volume/5)
		}
		art.Config["volumeTargetSteps"] = *volume
		art.Config["volumeCheckpointRows"] = every
		fmt.Printf("volume: filling to %d step rows at concurrency %d across %d replica(s), probing every %d rows\n",
			*volume, ks[0], *replicas, every)
		report := runVolume(ctx, engines, dbs, pick, ks[0], *volume, every)
		art.Volume = report
		if report.Errors != 0 {
			art.Valid = false
			art.Invalidity = fmt.Sprintf("volume: %d submit errors", report.Errors)
		}
	} else if *soak > 0 {
		art.Config["soakSec"] = soak.Seconds()
		art.Config["soakSampleSec"] = soakSample.Seconds()
		fmt.Printf("soak: %s at concurrency %d across %d replica(s), sampling every %s\n", *soak, ks[0], *replicas, *soakSample)
		report := runSoak(ctx, engines, readers, dbs, pick, ks[0], *soak, *soakSample)
		art.Soak = report
		final := report.Samples[len(report.Samples)-1]
		totalErrs := int64(0)
		for _, n := range report.OpErrors {
			totalErrs += n
		}
		fmt.Printf("soak ops: %v  errors: %v  purged: %d\n", report.Ops, report.OpErrors, report.Purged)
		if final.Unwedged != 0 || final.Orphaned != 0 || final.WriteFailed != 0 || totalErrs != 0 {
			art.Valid = false
			art.Invalidity = fmt.Sprintf("soak: opErrors=%d unwedged=%d orphaned=%d writeFailed=%d",
				totalErrs, final.Unwedged, final.Orphaned, final.WriteFailed)
		}
	} else {
		fmt.Printf("%-6s %10s %10s %10s %8s %8s %8s %7s %7s %9s %8s %8s %7s\n",
			"conc", "flows/s", "steps/s", "MB/s", "p50ms", "p95ms", "p99ms", "cpuCore", "cpu%", "steps/core", "netRxMB", "netTxMB", "errors")
		for _, k := range ks {
			res := runStep(ctx, engines, readers, pgss, sharedBytes, pick, k, *fairnessKeys, *warmup, *window)
			art.Results = append(art.Results, res)
			fmt.Printf("%-6d %10.1f %10.1f %10.2f %8.1f %8.1f %8.1f %7.2f %7.1f %9.0f %8.1f %8.1f %7d\n",
				k, res.FlowsPerSec, res.StepsPerSec, res.MBPerSec, res.P50ms, res.P95ms, res.P99ms,
				res.Host.CPUCores, res.Host.CPUPct, res.Host.StepsPerCore,
				res.Host.NetRxMBps, res.Host.NetTxMBps, res.Errors)
			if res.EngineCounters["dwarf_steps_recovered"] != 0 || res.EngineCounters["dwarf_steps_unwedged"] != 0 || res.Errors != 0 {
				art.Valid = false
				art.Invalidity = fmt.Sprintf("at concurrency %d: errors=%d recovered=%d unwedged=%d",
					k, res.Errors, res.EngineCounters["dwarf_steps_recovered"], res.EngineCounters["dwarf_steps_unwedged"])
			}
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

// parseConcurrency parses the comma-separated -concurrency list. 0 is legal and meaningful only in soak
// mode (drain/observe: engines run, no submitters - the post-crash verification shape).
func parseConcurrency(s string) ([]int, error) {
	var ks []int
	for part := range strings.SplitSeq(s, ",") {
		k, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || k < 0 {
			return nil, fmt.Errorf("invalid -concurrency %q", part)
		}
		ks = append(ks, k)
	}
	return ks, nil
}
