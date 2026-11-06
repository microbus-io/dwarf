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
// cloud benchmark campaign: a minimal in-process host (no transport framework), an RTT sampler for
// the database round-trip term of the sizing model, and one self-contained JSON artifact per run.
//
// Example (local smoke, SQLite):
//
//	go run ./bench -window 10s -warmup 2s -concurrency 8,32
//
// Example (RDS PostgreSQL):
//
//	go run ./bench -dsn 'postgres://user:pass@db.xyz.us-east-1.rds.amazonaws.com:5432/dwarf' \
//	  -workload linear -workers 64 -max-connections 30 -concurrency 8,16,32,64,128,256 \
//	  -window 5m -warmup 60s -label baseline
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/engineseams"
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
	Real        *realReport    `json:"real,omitzero"`   // set in -real mode instead of Results
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
		workloadName = flag.String("workload", "linear", "workload: linear, fanout, scatter, state, carry, carryfanout, loop, loopreplace, llm, mixed")
		payload      = flag.Int("payload", 0, "payload size in bytes: fresh bytes written by every `linear`/`fanout` task, the size of each `scatter` element, rewritten every step by `state`, written once and then carried by `carry`/`carryfanout`, the message each `loop`/`loopreplace` step adds. 0 = no payload for linear/fanout/scatter, 64KB for state/carry, 1KB for loop")
		taskDelay    = flag.Duration("task-delay", 0, "per-task sleep simulating remote executor latency (the exec term)")
		taskBurst    = flag.Duration("task-burst", 0, "bursty profile: apply -task-delay for this long, then drop it to zero for -task-quiet, repeating. 0 (with -task-quiet) keeps -task-delay constant for the whole run. A constant delay cannot see a crew that only shrinks once load FALLS - see taskProfile")
		taskQuiet    = flag.Duration("task-quiet", 0, "bursty profile: the quiet half of the cycle, during which tasks do not sleep. Flows keep arriving at the same rate, so only the exec term moves")
		pprofDir     = flag.String("pprof", "", "write a CPU profile for the whole run and a heap profile at the end into this directory. Run ONE arm at a time when attributing, since the CPU profile spans every -concurrency step")
		statsEvery   = flag.Duration("stats-interval", 0, "print a stats line this often during each measurement window (0 = off). The crew column is what a burst arm is for")
		logLevel     = flag.String("log-level", "warn", "the engines' log level on stderr: debug, info, warn, error or off")
		taskJitter   = flag.Duration("task-jitter", 0, "additional uniform random [0,d) per-task sleep - de-synchronizes fan-out siblings so a cohort's branches do not all complete at once")
		taskDist     = flag.String("task-delay-dist", "", "per-task sleep drawn per invocation from weighted classes, e.g. '0.80:5ms,0.15:200ms,0.05:8s' (weights sum to 1): several task durations on one crew. Replaces -task-delay; -task-jitter still adds on top")
		// 0 on either override means "let the engine derive it" - the self-tuned path a production host
		// takes, and the only way to measure the derivation itself (workers from the lease margin, the
		// pool from the shard's budget). A sweep pins them; a validation run must not.
		workers  = flag.Int("workers", 0, "engine worker goroutines (0 = derived from the lease-margin ceiling)")
		maxConns = flag.Int("max-connections", 0, "ShardSpec.MaxConnections for every shard: the fleet-wide budget the dispatching replicas divide (0 = derived from the RTT and the server's max_connections)")
		// Placement is even by default, so one slow shard holds a multi-shard saturated run to N x its own rate:
		// its flows stay in flight longest and fill the shared -max-outstanding cap. Weighting it down, from the
		// per-shard rates the run summary prints, is the operator's lever and the bench's.
		weightsFlag      = flag.String("weights", "", "ShardSpec.Weight per shard as comma-separated N=w (e.g. '1=1,2=0.8'); unlisted shards weigh 1")
		refillIntervalMs = flag.Int("refill-interval-ms", 0, "pin each shard's refill cycle period in ms (0 = derived from the pool); the scan-rate sweep knob")
		stateCacheMB     = flag.Int64("state-cache-mb", 0, "Engine.SetStateCacheSize in MiB for every replica: the memory each keeps for large state fields (0 = derived: a tenth of GOMEMLIMIT, else of the cgroup limit or physical memory, else 1 GiB)")
		openLoop         = flag.Bool("open-loop", false, "open-loop load: creators fire flows without awaiting completion, so the backlog grows past the goroutine count (closed-loop caps in-flight flows at -concurrency, keeping a linear backlog inside the cache and never stressing the refiller)")
		maxOutstanding   = flag.Int("max-outstanding", 100000, "open-loop only: cap on in-flight (created minus ended) flows, the backpressure bound that keeps the backlog from OOMing the DB")
		arrivalRate      = flag.Float64("arrival-rate", 0, "open-loop only: cap flow creation at this many/sec, fractional allowed (0 = flat-out to saturation)")
		drain            = flag.Duration("drain", 5*time.Minute, "open-loop only: after each window stop creating and wait for the outstanding flows to end, giving up once no task has started and no flow has ended for this long, then account for every flow the step created - completed, ended otherwise, unfinished, missing - in the artifact. Runs after the window is collected, so it never touches the window's numbers; an unfinished, failed or missing flow invalidates the run, and a drain that gives up ends it as a stall. Must exceed the longest task (0 = off)")
		stallTimeout     = flag.Duration("stall-timeout", 5*time.Minute, "end the run, mark it invalid and exit 3 when no task starts and no flow ends for this long while flows are in flight - in a sweep window, the volume fill or a soak. Must exceed the longest task (0 = off)")
		arrivalStop      = flag.Duration("arrival-stop", 0, "open-loop only: stop admitting flows this far into the measurement window while the engine keeps running, so the remainder measures a DRAIN under abated load (0 = keep offering for the whole window). The window mean spans both phases, so read the shape from -stats-interval, not the mean")
		concurrency      = flag.String("concurrency", "8,16,32,64,128", "comma-separated closed-loop submitter counts to sweep")
		window           = flag.Duration("window", 60*time.Second, "measurement window per concurrency step")
		warmup           = flag.Duration("warmup", 15*time.Second, "warmup before each measurement window (discarded)")
		label            = flag.String("label", "", "free-form run label recorded in the artifact")
		out              = flag.String("out", "", "artifact path (default bench/results/run-<timestamp>.json)")
		soak             = flag.Duration("soak", 0, "soak mode: run a mixed create/interrupt/continue workload under create-purge churn for this long, sampling drift, instead of the concurrency sweep (uses the first -concurrency value as the submitter count)")
		soakSample       = flag.Duration("soak-sample", 30*time.Second, "soak drift sampling interval")
		// Replicas share the database(s) and coordinate through them - nothing is sent between them, so
		// there is no transport here to make imperfect. Simulating a crashed replica means stopping its
		// engine so its registry rows go stale, not silencing anything.
		replicas        = flag.Int("replicas", 1, "number of in-process engine replicas sharing the database(s) (1 = single engine)")
		replicaWorkers  = flag.String("replica-workers", "", "comma-separated per-replica worker counts overriding -workers (e.g. '0,4' = replica 1 awaits only)")
		volume          = flag.Int("volume", 0, "volume mode: fill to this many step rows with NO deletion, probing the read paths at checkpoints, then, on the archive side, measure one Purge (0 = disabled)")
		volumeSide      = flag.String("volume-side", volumeArchive, "-volume only: which tables the fill grows - archive (flows close and are archived; the rows counted are both sides') or live (every flow is held open under a root that interrupts, so the live tables grow)")
		volumeCheckpt   = flag.Int("volume-checkpoint", 0, "probe every this many step rows during -volume (0 = one fifth of the target)")
		volumeMB        = flag.String("volume-checkpoints-mb", "", "volume mode by stored size, PostgreSQL only: comma-separated MB of dwarf_steps on disk to probe at, ascending; the fill stops at the last. Takes the place of -volume, which then only caps the rows")
		volumeTwoThirds = flag.Bool("volume-probe-two-thirds", true, "-volume-probe only: follow each saturated probe with a window at 2/3 of its rate (doubles each checkpoint's rows and time)")
		volumeCheckpts  = flag.String("volume-checkpoints", "", "-volume only: comma-separated step-row counts to probe at instead of every -volume-checkpoint rows, e.g. a doubling ladder; the -volume target is probed too")
		printPool       = flag.Float64("print-pool", 0, "print the budget the engine derives for one PostgreSQL shard at this round-trip time in ms against -server-max-connections, and exit - what a driver script pins with -max-connections, so every arm runs the pool that ships")
		serverMaxConns  = flag.Int("server-max-connections", 0, "-print-pool only: the server's max_connections the budget is clamped to half of (0 = unknown, no clamp)")
		volumeProbeWL   = flag.String("volume-probe-workload", "linear", "-volume-probe only: the workload the probe windows run, which need not be the fill's - a bytes-axis fill is `state` while its probes measure `linear`")
		volumeProbe     = flag.Duration("volume-probe", 0, "-volume only: at each checkpoint (and on the empty database first), pause the fill for an open-loop saturated window of this length and then one at 2/3 of the flow rate it served, at the pinned pool (0 = no throughput probe)")
		fairnessKeys    = flag.Int("fairness-keys", 1, "spread flows across this many distinct fairness keys (1 = all on the single default key, the degenerate case for the refiller's per-key scan bound)")
		linearStepsFlag = flag.Int("linear-steps", linearSteps, "linear workload chain length, and the number of messages a `loop`/`loopreplace` flow ends with; sweeping it separates a slow replica's share of STEPS from its share of FLOWS (see workloads.go)")
		fanOutWidth     = flag.Int("fanout-width", 16, "forEach branch count for the fanout, scatter and carryfanout workloads - the knob that decouples pending-step backlog from submitter concurrency (backlog ~= concurrency x width)")
		branchDepth     = flag.Int("branch-depth", 1, "scatter workload: steps per branch (1 or 2 in the cloud plan); depth > 1 is where an element stored inline would be copied at every branch step")
		mixFlag         = flag.String("mix", "", "per-flow classes instead of -workload: comma-separated share:workload:payload[:width[:depth]] over linear, fanout and scatter, e.g. '0.85:linear:1K,0.15:fanout:4K:8'; shares sum to 1")
		realDur         = flag.Duration("real", 0, "real mode: offer -arrival-profile's schedule over the -mix classes until -arrival-epoch plus this, writing a per-interval series, instead of the concurrency sweep (0 = off)")
		arrivalProfile  = flag.String("arrival-profile", "", "-real: the repeating schedule of offered steps/s across the fleet, comma-separated duration:level or duration:from-to (a ramp), e.g. '25m:8000,10m:8000-12000,40m:12000,15m:12000-8000'")
		arrivalBursts   = flag.String("arrival-bursts", "", "-real: bursts into each period, comma-separated offset+duration:level, e.g. '10m+2m:16000,58m+5m:16000'")
		arrivalNoise    = flag.Float64("arrival-noise", 0.1, "-real: scale each minute of the period by a seeded factor in [1-n, 1+n]")
		arrivalSeed     = flag.Uint64("arrival-seed", 1, "-real: the noise seed; the same seed gives every process the same schedule")
		arrivalShare    = flag.Float64("arrival-share", 1, "-real: this process's share of the fleet's offered load (0.5 for each of two replicas)")
		arrivalEpoch    = flag.Int64("arrival-epoch", 0, "-real: Unix seconds the schedule starts from, the same for every process of a fleet and for one restarted mid-run (0 = now)")
		fairnessSkew    = flag.Float64("fairness-skew", 0, "spread flows over -fairness-keys by a Zipf law with this exponent (> 1; key k0 the heaviest), instead of uniformly (0); -real only")
		priorityMixFlag = flag.String("priority-mix", "", "-real: the share of flows at each priority level, \"1:0.03,2:0.12,3:0.55,4:0.2,5:0.1\" (1..5, lower runs first); empty = every flow at the engine's default, 3")
		retention       = flag.Duration("retention", 0, "-real: Purge completed flows older than this every minute, so the step table reaches a steady size (0 = keep everything)")
		deleteOnDone    = flag.Bool("delete-on-completion", false, "-real: create every flow with DeleteOnCompletion, so the reaper removes it a minute after it completes and the step table holds only work in flight")
		carryRead       = flag.Bool("carry-read", false, "carry workloads: make every task READ the carried document instead of only passing it along. Storage is identical either way, so this is the arm switch for how carried state is HELD, not how much is stored")
	)
	flag.Var(&dsns, "dsn", "shard DSN, repeatable; 'N=dsn' sets shard N, a bare dsn sets shard 1 (default: throwaway local SQLite)")
	flag.Parse()
	weights, err := parseWeights(*weightsFlag)
	if err != nil {
		return err
	}
	for idx := range weights {
		if !slices.ContainsFunc(dsns, func(sh struct {
			index int
			dsn   string
		}) bool {
			return sh.index == idx
		}) {
			return fmt.Errorf("-weights: shard %d is not one of the -dsn shards", idx)
		}
	}
	if *printPool > 0 {
		fmt.Println(engineseams.ShardBudget(*printPool, *serverMaxConns))
		return nil
	}

	// The CPU profile spans the WHOLE run, so an attribution run passes one -concurrency value; see
	// startProfiling. Started before anything else so engine startup and migrations are in it too - they
	// are process CPU like any other, and a profile that began after them would quietly under-report.
	stopProfile, err := startProfiling(*pprofDir, *label)
	if err != nil {
		return err
	}
	defer stopProfile()

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
	// One graph/task registry and one byte counter, shared by every replica's host (registered through the
	// public API only - the path a production host takes).
	sharedBytes := &atomic.Int64{}
	// One profile, anchored once and copied by value into every replica's host, so a multi-replica run has
	// every replica in the SAME phase of the cycle. Anchoring per host would stagger them and blur the
	// transition the burst exists to make sharp.
	profile := taskProfile{delay: *taskDelay, on: *taskBurst, off: *taskQuiet, start: time.Now()}
	dist, err := parseDelayDist(*taskDist)
	if err != nil {
		return err
	}
	// Parsed before any engine starts, so a mistyped mix fails the run up front rather than after its warmup.
	priorities, err := parsePriorityMix(*priorityMixFlag)
	if err != nil {
		return err
	}
	longestTask := *taskDelay
	if len(dist) > 0 {
		// A sampled class has no burst or quiet half to belong to, and a -task-delay beside it would be
		// silently ignored.
		if *taskDelay > 0 || *taskBurst > 0 || *taskQuiet > 0 {
			return fmt.Errorf("-task-delay-dist replaces -task-delay, -task-burst and -task-quiet")
		}
		longestTask = dist.longest()
	}
	// The window functions read this rather than take two more parameters; runStep already carries ten, and
	// the readout is a property of the RUN rather than of any one window.
	tracing.every, tracing.profile, tracing.dist = *statsEvery, profile, dist
	guard.drain, guard.stall = *drain, *stallTimeout
	if *stallTimeout > 0 && longestTask+*taskJitter >= *stallTimeout {
		// A task longer than the timeout is silence the watchdog would read as a hang.
		return fmt.Errorf("-stall-timeout %s must exceed the longest task (%s plus -task-jitter %s)",
			*stallTimeout, longestTask, *taskJitter)
	}
	if *drain > 0 && longestTask+*taskJitter >= *drain {
		// The same silence, read by the drain as flows that will never end. The drain counts a task start as
		// progress, as the watchdog does, so the longest gap it can see in a healthy run is one task.
		return fmt.Errorf("-drain %s must exceed the longest task (%s plus -task-jitter %s)",
			*drain, longestTask, *taskJitter)
	}
	if *workloadName == "loop" || *workloadName == "loopreplace" {
		// The final history holds every message at once, and a flow's final state over the engine's 256 MB limit
		// fails the flow at its last step: refuse the run rather than report those failures as errors.
		if history := *linearStepsFlag * (effectivePayload(*workloadName, *payload) + loopMessageOverhead); history > maxFinalState {
			return fmt.Errorf("-linear-steps %d of %d-byte messages makes a %d-byte history, over the %d-byte final state limit",
				*linearStepsFlag, effectivePayload(*workloadName, *payload), history, maxFinalState)
		}
	}
	regHost := &benchHost{
		graphs:       map[string]*workflow.Graph{},
		tasks:        map[string]func(context.Context, *workflow.Flow) error{},
		profile:      profile,
		taskJitter:   *taskJitter,
		delayDist:    dist,
		bytesWritten: sharedBytes,
	}
	workloads := registerWorkloads(regHost, "", *payload, *fanOutWidth, *linearStepsFlag, *branchDepth, *carryRead)
	pick, err := chooseWorkload(workloads, *workloadName)
	if err != nil {
		return err
	}
	meanSteps := float64(pick().stepsPerFlow)
	// classOf and stepsOf name the class each workflow URL runs and its steps per flow, for -real's per-class rates.
	classOf, stepsOf := map[string]string{}, map[string]int{}
	if *mixFlag != "" {
		classes, err := parseMix(*mixFlag, *fanOutWidth)
		if err != nil {
			return err
		}
		var picked []*workload
		pick, picked, meanSteps = buildMix(regHost, classes, *linearStepsFlag)
		for i, c := range classes {
			classShares[c.name] = c.share
			classOf[picked[i].graphURL], stepsOf[picked[i].graphURL] = c.name, picked[i].stepsPerFlow
		}
	} else if *workloadName != "mixed" {
		// One class, named for the workload.
		w := *pick()
		w.class = *workloadName
		pick = func() *workload { return &w }
		classOf[w.graphURL], stepsOf[w.graphURL] = w.class, w.stepsPerFlow
	}
	if *realDur > 0 && *arrivalProfile == "" {
		return fmt.Errorf("-real needs -arrival-profile")
	}
	if *fairnessSkew != 0 && *fairnessSkew <= 1 {
		return fmt.Errorf("-fairness-skew %g must be above 1 (or 0 for uniform)", *fairnessSkew)
	}

	// Build the fleet of in-process engine replicas. Each shares the registry above and the same
	// database(s), so they contend for every pending step at the claim CAS - exercising peer discovery and
	// the connection pool-split (open/R per replica, per shard). Each has its own metrics reader (summed
	// across the fleet). NOTE: one
	// process, so goroutine/heap readings are a fleet total and there is no per-replica RSS attribution or
	// kill-9 isolation - those need separate processes (a follow-up).
	ctx := context.Background()
	engines := make([]*engine.Engine, *replicas)
	hosts := make([]*benchHost, *replicas)
	readers := make([]*sdkmetric.ManualReader, *replicas)
	// The engines' own warnings - a replica stepping down on a stale peer row, a lost lease - go to stderr, so a
	// rig's log holds them beside the stats lines. The engine's default logger discards them.
	logger, err := benchLogger(*logLevel)
	if err != nil {
		return err
	}
	for i := range *replicas {
		reader := sdkmetric.NewManualReader()
		readers[i] = reader
		host := &benchHost{
			graphs:       regHost.graphs,
			tasks:        regHost.tasks,
			profile:      profile,
			taskJitter:   *taskJitter,
			delayDist:    dist,
			bytesWritten: sharedBytes,
		}
		hosts[i] = host
		eng := engine.NewEngine()
		for _, sh := range dsns {
			err = eng.SetShard(engine.ShardSpec{Index: sh.index, DSN: sh.dsn, MaxConnections: *maxConns, Weight: weights[sh.index]})
			if err != nil {
				return err
			}
		}
		setters := []error{
			eng.SetLogger(logger.With("replica", i)),
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
		if *refillIntervalMs > 0 {
			setters = append(setters, engineseams.SetRefillInterval(eng, time.Duration(*refillIntervalMs)*time.Millisecond))
		}
		if *stateCacheMB > 0 {
			setters = append(setters, eng.SetStateCacheSize(*stateCacheMB<<20))
		}
		for _, setter := range setters {
			if setter != nil {
				return setter
			}
		}
		engines[i] = eng
	}
	for _, eng := range engines {
		err = eng.Startup(ctx)
		if err != nil {
			return err
		}
		defer eng.Shutdown(ctx)
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
	// Same shard-0-only caveat as pgss and the RTT sampler: a multi-shard run has no server-side
	// counterpart for shards 2+.
	waits := startWaitSampler(dsns[0].dsn)
	defer waits.close()

	art := artifact{
		Label:     *label,
		StartedAt: time.Now().UTC(),
		Config: map[string]any{
			"workload": *workloadName,
			// What the workload actually writes: -payload 0 means none for linear/fanout/scatter but the
			// 64KB default for state/carry, and an artifact must not record 0 for a run that wrote 64KB.
			"payloadBytes": effectivePayload(*workloadName, *payload),
			"taskDelayMs":  taskDelay.Milliseconds(),
			// Recorded even when zero: an artifact that does not say whether the exec term was constant
			// cannot be compared with one where it alternated, and the two measure different things.
			"taskBurstMs": taskBurst.Milliseconds(),
			"taskQuietMs": taskQuiet.Milliseconds(),
			// The spec as given, and its mean: two dist arms with the same mean are the comparison M makes
			// against a single-duration arm.
			"taskDelayDist":   *taskDist,
			"taskDelayMeanMs": dist.mean().Milliseconds(),
			// The workload's own shape knobs. Without them a -fanout-width or -linear-steps sweep produces
			// artifacts that are byte-identical in configuration and wildly different in what they measured.
			"fanOutWidth":     *fanOutWidth,
			"branchDepth":     *branchDepth,
			"linearSteps":     *linearStepsFlag,
			"carryRead":       *carryRead,
			"workers":         *workers,
			"maxConnections":  *maxConns,
			"stateCacheMB":    *stateCacheMB,
			"weights":         weights,
			"replicas":        *replicas,
			"replicaWorkers":  *replicaWorkers,
			"shards":          dsns.redacted(),
			"windowSec":       window.Seconds(),
			"warmupSec":       warmup.Seconds(),
			"drainSec":        drain.Seconds(),
			"stallTimeoutSec": stallTimeout.Seconds(),
		},
		Environment: environment(),
		Valid:       true,
	}

	if *volumeMB != "" && *volume == 0 {
		*volume = math.MaxInt32 // the sizes end the fill; the row target only caps it
	}
	if *realDur > 0 {
		profile, err := parseArrivalProfile(*arrivalProfile, *arrivalBursts, *arrivalNoise, *arrivalSeed)
		if err != nil {
			return err
		}
		epoch := time.Now()
		if *arrivalEpoch > 0 {
			epoch = time.Unix(*arrivalEpoch, 0)
		}
		seriesPath := *out
		if seriesPath == "" {
			seriesPath = filepath.Join("bench/results", "run-"+art.StartedAt.Format("20060102-150405")+".json")
		}
		seriesPath = strings.TrimSuffix(seriesPath, ".json") + ".series.jsonl"
		interval := *statsEvery
		if interval <= 0 {
			interval = 10 * time.Second
		}
		for k, v := range map[string]any{"arrivalProfile": *arrivalProfile, "arrivalBursts": *arrivalBursts,
			"arrivalNoise": *arrivalNoise, "arrivalSeed": *arrivalSeed, "arrivalShare": *arrivalShare,
			"arrivalEpoch": epoch.Unix(), "realSec": realDur.Seconds(), "mix": *mixFlag, "fairnessKeys": *fairnessKeys,
			"fairnessSkew": *fairnessSkew, "priorityMix": *priorityMixFlag,
			"retentionSec": retention.Seconds(), "deleteOnCompletion": *deleteOnDone} {
			art.Config[k] = v
		}
		// A rolling restart stops a replica with SIGTERM: stop creating, write the artifact, and let the deferred
		// engine Shutdown drain what this replica holds, as a host's graceful stop would.
		sigCtx, stopSignals := signal.NotifyContext(ctx, syscall.SIGTERM, os.Interrupt)
		defer stopSignals()
		report, err := runReal(sigCtx, engines, readers, pick, realConfig{
			profile: profile, epoch: epoch, until: epoch.Add(*realDur), share: *arrivalShare, meanSteps: meanSteps,
			creators: ks[0], maxOutstanding: *maxOutstanding, fairnessKeys: *fairnessKeys, fairnessSkew: *fairnessSkew,
			priorities: priorities, retention: *retention, deleteOnCompletion: *deleteOnDone, interval: interval,
			seriesPath: seriesPath, classOf: classOf, stepsOf: stepsOf,
		})
		if err != nil {
			return err
		}
		art.Real = report
		switch {
		case report.Stalled != "":
			art.Valid, art.Invalidity = false, "real: stalled: "+report.Stalled
		case report.Errors != 0:
			art.Valid, art.Invalidity = false, fmt.Sprintf("real: %d flow errors", report.Errors)
		case report.DrainGaveUp:
			art.Valid, art.Invalidity = false, "real: the drain gave up with flows still running"
		case report.ThrottledMs != 0:
			// The cap is a fuse above the largest burst backlog; once it binds, a burst measures the cap.
			art.Valid, art.Invalidity = false, fmt.Sprintf("real: -max-outstanding bound for %dms", report.ThrottledMs)
		}
	} else if *volume > 0 {
		if *volumeSide != volumeLive && *volumeSide != volumeArchive {
			return fmt.Errorf("-volume-side must be %s or %s, not %q", volumeArchive, volumeLive, *volumeSide)
		}
		checkpoints, err := volumeCheckpoints(*volumeCheckpts, *volumeCheckpt, *volume)
		if err != nil {
			return err
		}
		checkpointsMB, err := volumeCheckpointsMB(*volumeMB)
		if err != nil {
			return err
		}
		if len(checkpointsMB) > 0 {
			if dbs == nil {
				return fmt.Errorf("-volume-checkpoints-mb needs a PostgreSQL shard, whose size it reads")
			}
			checkpoints = nil
			art.Config["volumeCheckpointMB"] = checkpointsMB
		}
		art.Config["volumeProbeTwoThirds"] = *volumeTwoThirds
		art.Config["volumeTargetSteps"] = *volume
		art.Config["volumeSide"] = *volumeSide
		art.Config["volumeCheckpointRows"] = checkpoints
		if len(checkpointsMB) > 0 {
			fmt.Printf("volume: filling by stored size at concurrency %d across %d replica(s), probing at %v MB of dwarf_steps\n",
				ks[0], *replicas, checkpointsMB)
		} else {
			fmt.Printf("volume: filling to %d step rows at concurrency %d across %d replica(s), probing at %v rows\n",
				*volume, ks[0], *replicas, checkpoints)
		}
		var probe func(rate float64) (stepResult, string)
		probeSPF := 0
		if *volumeProbe > 0 {
			probeWL := workloads[*volumeProbeWL]
			if probeWL == nil {
				return fmt.Errorf("-volume-probe-workload %q: not a fixed workload", *volumeProbeWL)
			}
			probeSPF = probeWL.stepsPerFlow
			probePick := func() *workload { return probeWL }
			art.Config["volumeProbeSec"] = volumeProbe.Seconds()
			art.Config["volumeProbeWorkload"] = *volumeProbeWL
			probeWL.prepare()
			probe = func(rate float64) (stepResult, string) {
				res := runStepOpenLoop(ctx, engines, readers, pgss, waits, dbs, sharedBytes, probePick, ks[0], *fairnessKeys, *maxOutstanding, rate, *warmup, *volumeProbe, 0)
				res.Invalidity = windowInvalidity(res, rate, *maxOutstanding)
				return res, res.Invalidity
			}
		}
		report := runVolume(ctx, engines, readers, dbs, *volumeSide, pick, ks[0], *volume, checkpoints, checkpointsMB, probe, probeSPF, *volumeTwoThirds)
		art.Volume = report
		if report.Errors != 0 {
			art.Valid = false
			art.Invalidity = fmt.Sprintf("volume: %d submit errors", report.Errors)
		} else if report.Invalidity != "" {
			art.Valid = false
			art.Invalidity = "volume: " + report.Invalidity
		}
	} else if *soak > 0 {
		art.Config["soakSec"] = soak.Seconds()
		art.Config["soakSampleSec"] = soakSample.Seconds()
		fmt.Printf("soak: %s at concurrency %d across %d replica(s), sampling every %s\n", *soak, ks[0], *replicas, *soakSample)
		workloads["linear"].prepare() // the continue and fork chains run it whatever -workload picks
		report := runSoak(ctx, engines, readers, dbs, pick, ks[0], *soak, *soakSample)
		art.Soak = report
		final := report.Samples[len(report.Samples)-1]
		totalErrs := int64(0)
		for _, n := range report.OpErrors {
			totalErrs += n
		}
		fmt.Printf("soak ops: %v  errors: %v  purged: %d  sweep errors: %d\n", report.Ops, report.OpErrors, report.Purged, report.SweepErrors)
		switch {
		case report.Stalled != "":
			art.Valid = false
			art.Invalidity = "soak: stalled: " + report.Stalled
		case final.CollectionFailed || report.FinalCounters == nil:
			// Every alarm is a difference against the soak's opening collection, so a missing reading at either
			// end reads as zero alarms.
			art.Valid = false
			art.Invalidity = "soak: metric collection or row count failed in the final sample - the alarms read 0 for want of a reading"
		case final.Unwedged != 0 || final.Orphaned != 0 || final.WriteFailed != 0 || totalErrs != 0:
			art.Valid = false
			art.Invalidity = fmt.Sprintf("soak: opErrors=%d unwedged=%d orphaned=%d writeFailed=%d",
				totalErrs, final.Unwedged, final.Orphaned, final.WriteFailed)
		case report.SweepErrors != 0:
			// A retention sweep that fails leaves the archive growing, which is what the soak watches for.
			art.Valid = false
			art.Invalidity = fmt.Sprintf("soak: the retention sweep failed %d times", report.SweepErrors)
		}
	} else {
		fmt.Printf("%-6s %10s %10s %10s %8s %8s %8s %7s %7s %9s %8s %8s %7s\n",
			"conc", "flows/s", "steps/s", "MB/s", "p50ms", "p95ms", "p99ms", "cpuCore", "cpu%", "steps/core", "netRxMB", "netTxMB", "errors")
		for _, k := range ks {
			var res stepResult
			if *openLoop {
				res = runStepOpenLoop(ctx, engines, readers, pgss, waits, dbs, sharedBytes, pick, k, *fairnessKeys, *maxOutstanding, *arrivalRate, *warmup, *window, *arrivalStop)
			} else {
				res = runStep(ctx, engines, readers, pgss, waits, dbs, sharedBytes, pick, k, *fairnessKeys, *warmup, *window)
			}
			res.Invalidity = windowInvalidity(res, *arrivalRate, *maxOutstanding)
			art.Results = append(art.Results, res)
			fmt.Printf("%-6d %10.1f %10.1f %10.2f %8.1f %8.1f %8.1f %7.2f %7.1f %9.0f %8.1f %8.1f %7d\n",
				k, res.FlowsPerSec, res.StepsPerSec, res.MBPerSec, res.P50ms, res.P95ms, res.P99ms,
				res.Host.CPUCores, res.Host.CPUPct, res.Host.StepsPerCore,
				res.Host.NetRxMBps, res.Host.NetTxMBps, res.Errors)
			gcShare := "no GC in the window"
			if res.Host.GCCPUPct >= 0 {
				gcShare = fmt.Sprintf("GC %.1f%% of CPU", res.Host.GCCPUPct)
			}
			fmt.Printf("  alloc %.1f KB/step, %.0f mallocs/step, %.1f GC/s, %s\n",
				res.Host.AllocBytesPerStep/1024, res.Host.MallocsPerStep, res.Host.GCPerSec, gcShare)
			if len(dsns) > 1 {
				fmt.Println(perShardLine(res, dsns))
			}
			if res.Invalidity != "" && art.Valid {
				art.Valid = false
				art.Invalidity = fmt.Sprintf("at concurrency %d: %s", k, res.Invalidity)
			}
			if stalledRun.Load() {
				break // the next step would start on the engines that just hung
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
	if stalledRun.Load() {
		// The deferred engine shutdowns would wait on the very thing that hung. The artifact is written;
		// exit without them.
		fmt.Println("exiting without engine shutdown after a stall")
		stopProfile() // os.Exit skips the deferred one, which would leave an empty CPU profile
		os.Exit(3)
	}
	return nil
}

// benchLogger is the engines' logger for -log-level: text on stderr at that level, or a discard for "off".
func benchLogger(level string) (*slog.Logger, error) {
	if level == "off" {
		return slog.New(slog.DiscardHandler), nil
	}
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("-log-level: %w", err)
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l})), nil
}

// volumeCheckpoints is the ascending list of step-row counts a -volume fill probes at: the -volume-checkpoints
// list when given, else every -volume-checkpoint rows (a fifth of the target by default) below the target.
func volumeCheckpoints(list string, every, target int) ([]int, error) {
	var out []int
	if list != "" {
		for part := range strings.SplitSeq(list, ",") {
			n, err := strconv.Atoi(strings.TrimSpace(part))
			if err != nil || n <= 0 {
				return nil, fmt.Errorf("-volume-checkpoints: %q is not a positive row count", part)
			}
			out = append(out, n)
		}
		slices.Sort(out)
		return slices.Compact(out), nil
	}
	if every <= 0 {
		every = max(1, target/5)
	}
	for n := every; n < target; n += every {
		out = append(out, n)
	}
	return out, nil
}

// volumeCheckpointsMB parses -volume-checkpoints-mb: ascending, distinct sizes in MB.
func volumeCheckpointsMB(list string) ([]float64, error) {
	if list == "" {
		return nil, nil
	}
	var out []float64
	for part := range strings.SplitSeq(list, ",") {
		mb, err := strconv.ParseFloat(strings.TrimSpace(part), 64)
		if err != nil || mb <= 0 {
			return nil, fmt.Errorf("-volume-checkpoints-mb: %q is not a positive size in MB", part)
		}
		out = append(out, mb)
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// windowInvalidity says why one measurement window is not a measurement, or "" when it is. Every window -
// a sweep step or a volume probe - goes through it. rate is the commanded flows/s (0 = closed-loop or flat out).
func windowInvalidity(res stepResult, rate float64, maxOutstanding int) string {
	if res.Stalled != "" {
		return "stalled: " + res.Stalled
	}
	// A MISSING collection must invalidate the run, and this check has to come before the counter tests. Each
	// of them reads a map, and an EMPTY map answers 0 to all of them - so a window whose closing collection
	// produced nothing passes every reliability check and is written out as `valid: true` with
	// stepsPerSec = 0, a plausible-looking measurement of zero throughput. Measured on a GCP turnstile ladder,
	// 2 of 9 arms, which then reported its BEST turn multiple as its worst purely by averaging those zeros in.
	// The signature is gaugesBefore present with gaugesAfter absent: the closing collection raced engine
	// teardown. len, not nil: a failed gauge collection is nil but a counter delta against one is an empty
	// map, so len is the one test that covers both.
	// A collection that failed on ANY replica comes back nil (collectAllCounters, collectAllGauges), so a fleet
	// total missing a replica reads as missing here rather than as an under-count - and a delta against a
	// failed opening collection is empty, not the raw totals. Histograms are collected separately and can fail
	// alone; an empty set is a legitimate reading, so they report the failure as HistsMissing.
	if len(res.EngineCounters) == 0 || len(res.GaugesBefore) == 0 || len(res.GaugesAfter) == 0 || res.HistsMissing {
		return fmt.Sprintf("metric collection failed (counters=%d gaugesBefore=%d gaugesAfter=%d histogramsMissing=%v) - "+
			"every derived rate in this window is meaningless",
			len(res.EngineCounters), len(res.GaugesBefore), len(res.GaugesAfter), res.HistsMissing)
	}
	if res.EngineCounters["dwarf_steps_recovered"] != 0 || res.EngineCounters["dwarf_steps_unwedged"] != 0 || res.Errors != 0 {
		return fmt.Sprintf("errors=%d recovered=%d unwedged=%d",
			res.Errors, res.EngineCounters["dwarf_steps_recovered"], res.EngineCounters["dwarf_steps_unwedged"])
	}
	if d := res.Drain; d != nil && (d.Unfinished != 0 || len(d.EndedOther) != 0 || d.missing() != 0) {
		return fmt.Sprintf("drain: of %d flows created, %d completed, %d unfinished after %.0fs (none ended for the last %s), %d missing, ended otherwise %v",
			d.Created, d.Completed, d.Unfinished, d.Sec, guard.drain, d.missing(), d.EndedOther)
	}
	if rate > 0 && res.ThrottledMs > 0 {
		// The -max-outstanding fuse bound while a rate was COMMANDED, so the window measured the cap and not
		// the rate. Nothing else in the artifact names the cause: the symptoms (depressed throughput, inflated
		// RTT sampler, a CPU-burn wait profile) read exactly like an over-connection collapse. Raise
		// -max-outstanding and re-run.
		return fmt.Sprintf("-max-outstanding=%d bound for %dms of the window against a commanded %.2f "+
			"flows/s (admitted %.2f/s) - this window measured the cap, not the commanded rate",
			maxOutstanding, res.ThrottledMs, rate, res.FlowsPerSec)
	}
	return ""
}

// environment records the host-side context a result depends on.
func environment() map[string]any {
	hostname, _ := os.Hostname()
	env := map[string]any{
		"hostname":  hostname,
		"goVersion": runtime.Version(),
		"goos":      runtime.GOOS,
		"goarch":    runtime.GOARCH,
		"numCPU":    runtime.NumCPU(),
		// The memory settings a payload arm's heap depends on: the engine's state cache defaults to a tenth
		// of the soft limit, and GOGC sets how far past its live size the heap runs.
		"GOGC":       os.Getenv("GOGC"),
		"goMemLimit": goMemLimit(),
	}
	// The commit the binary was built from, so every artifact names the build it measured. `go build` stamps
	// it; `go run` and a build outside the work tree do not, and vcs.modified=true means a dirty tree.
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, kv := range bi.Settings {
			if kv.Key == "vcs.revision" || kv.Key == "vcs.modified" {
				env[kv.Key] = kv.Value
			}
		}
	}
	return env
}

// goMemLimit returns the runtime's soft memory limit in bytes, or 0 when none is set (GOMEMLIMIT unset).
func goMemLimit() int64 {
	if limit := debug.SetMemoryLimit(-1); limit < math.MaxInt64 {
		return limit
	}
	return 0
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

// parseWeights reads -weights ("N=w,N=w") into a shard index -> weight map. A shard left out weighs 1 in the
// engine (a zero Weight means unset).
func parseWeights(v string) (map[int]float64, error) {
	out := map[int]float64{}
	if v == "" {
		return out, nil
	}
	for part := range strings.SplitSeq(v, ",") {
		n, w, ok := strings.Cut(strings.TrimSpace(part), "=")
		idx, errIdx := strconv.Atoi(n)
		weight, errW := strconv.ParseFloat(w, 64)
		if !ok || errIdx != nil || errW != nil || !(weight > 0) || math.IsInf(weight, 0) {
			return nil, fmt.Errorf("-weights: %q is not N=w with a positive weight", part)
		}
		out[idx] = weight
	}
	return out, nil
}

// perShardLine summarizes a multi-shard window per shard: flows ended per second, and how far the shard's
// backlog moved (flows placed there minus flows ended there). Equal rates with one shard's backlog growing,
// or one shard's rate below the rest, is a slow shard holding the run back. Read off the engine's own
// shard-attributed counters, which this process's engines record for the flows they placed and ended.
func perShardLine(res stepResult, dsns shardFlags) string {
	var b strings.Builder
	b.WriteString("  per shard:")
	for _, sh := range dsns {
		started := shardCounter(res.EngineCounters, "dwarf_flows_started", sh.index)
		ended := shardCounter(res.EngineCounters, "dwarf_flows_closed", sh.index)
		fmt.Fprintf(&b, "  %d: %.0f flows/s, backlog %+d", sh.index, float64(ended)/res.WindowSec, started-ended)
	}
	return b.String()
}

// shardCounter sums a counter's attributed series for one shard. Keys are spelled name|k=v|k=v with the
// attributes sorted (collectCounters).
func shardCounter(counters map[string]int64, name string, shard int) int64 {
	want := "shard=" + strconv.Itoa(shard)
	var n int64
	for k, v := range counters {
		rest, ok := strings.CutPrefix(k, name+"|")
		if !ok {
			continue
		}
		if slices.Contains(strings.Split(rest, "|"), want) {
			n += v
		}
	}
	return n
}
