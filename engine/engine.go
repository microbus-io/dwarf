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

package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"math"
	"math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"log/slog"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/lru"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ShardSummary is the health/size summary of a single database shard.
type ShardSummary struct {
	Shard     int    `json:"shard,omitzero"`
	Error     string `json:"error,omitzero"`
	LatencyMs int    `json:"latencyMs,omitzero"`
	Steps     int    `json:"steps,omitzero"`
	Flows     int    `json:"flows,omitzero"`
}

const (
	maxPollInterval     = 5 * time.Minute
	backlogPollInterval = 1 * time.Minute

	// pollErrorRetryInterval caps the wake delay after a sizing query in pollPendingSteps fails, so a
	// transient DB error (e.g. a momentary connection-limit rejection) triggers a prompt re-poll instead
	// of the timer sleeping for maxPollInterval while a due step sits undispatched.
	pollErrorRetryInterval = 1 * time.Second

	parkedNone     = 0
	parkedSubgraph = 1
)

// The recovery/await/reap timing knobs (leaseMargin, wedgeSweepInterval, parkWedgeThreshold,
// orphanFlowThreshold, deletionGrace, reapInterval, awaitPollInterval) are per-engine fields on Engine
// (defaulted in NewEngine), not package globals. They are effectively constants in production - read-only
// after NewEngine - but a white-box test in this package shortens one directly (e.g. e.reapInterval = 20ms,
// before Startup for the two read once at Startup) to exercise a recovery/await/reap path without waiting
// minutes. The rationale for what each bounds stays at its field declaration and consult site.

// awaitShutdownSignal is the sentinel drainRuntime sends on each Await waiter channel so a blocked await
// returns a shutdown error instead of re-snapshotting and re-blocking on a channel that will never be
// signaled again. The null-byte prefix guarantees it can never collide with a real flow status (the set
// await distinguishes via stopped()).
const awaitShutdownSignal = "\x00shutdown"

// Engine is the standalone workflow orchestration engine.
type Engine struct {
	// Dependencies (set before Startup)
	host           Host
	logger         *slog.Logger
	meterProvider  metric.MeterProvider
	metrics        *engineMetrics
	tracerProvider trace.TracerProvider
	tracer         trace.Tracer

	// Shard registry (construction-time only): shard index -> spec. Sparse - indices must be unique and
	// >= 1 but need not be contiguous. Guarded by shardsLock; empty means one default shard at Startup.
	shardsLock sync.Mutex
	shardSpecs map[int]ShardSpec

	// Configuration (atomically updated, safe to change after Startup)
	workers         atomic.Int32
	workersSet      atomic.Bool // SetWorkers was called: skip the derived default
	timeBudgetMs    atomic.Int64
	defaultPriority atomic.Int32
	maxOpenConns    atomic.Int32 // expert override: exact per-shard pool size; 0 = derive from ShardSpec.VirtualCPUs

	// engineID is a random positive identifier minted per engine instance (fresh on every restart).
	// It is stamped on every flow/step INSERT (creator) and overwritten by the claim CAS (claimer) -
	// forensic provenance, deliberately unindexed. instanceID is its base-36 string form, the origin
	// on every outbound peer signal: DeliverSignal discards the engine's own signals when the host's
	// SignalPeers echoes the broadcast back, and the peer-discovery map is keyed by it.
	engineID   int64
	instanceID string

	// Peer discovery: last time each peer instanceID was heard from (hello/ping/goodbye signals).
	// Initialized to {self: now} at construction. The count of fresh entries is the observed replica
	// count R that divides the derived per-shard connection pools - a lookup, not a control loop.
	peersLock sync.Mutex
	peers     map[string]time.Time
	peersStop chan struct{}
	peersLoop sync.WaitGroup
	// lastAppliedR is the replica count the pools were last derived with, to skip no-op recomputes.
	lastAppliedR atomic.Int32

	// Database
	db database.ShardSet
	// testHashedID is the hashed test database id when the engine was started in test mode (RunInTest),
	// empty in production. It becomes Config.TestID at Open, wrapping each shard in an isolated test
	// database. Written once during single-threaded startup before started.Store(true), read only after
	// started.Load()==true, so the atomic started flag is the happens-before barrier.
	testHashedID string

	// Candidate cache and worker pool. `workers` is the pool's MAXIMUM (the lease-margin ceiling, or an
	// explicit SetWorkers); the pool is grow-on-demand: initRuntime spawns `workersResident` = the
	// dispatch-sized set (8 x conns, floored at 64), and a worker that finds every spawned peer busy
	// while work is waiting spawns one more, up to the max. A worker blocked in a long ExecuteTask holds
	// no connection, so the ceiling can be large without the resident set - and therefore the candidate
	// cache and its refill scan, both sized from the resident count - paying for it.
	cache           candidatecache.Cache
	workerPool      sync.WaitGroup
	workersResident atomic.Int32 // goroutines spawned so far
	workersInTask   atomic.Int32 // goroutines currently parked inside the host's ExecuteTask (holding no connection)
	spawnLock       sync.Mutex
	spawnClosed     bool // set under spawnLock before the pool is drained: no goroutine may join
	// workersDispatch is the resident (eagerly spawned) worker count and the candidate cache's sizing
	// input, resolved at Startup. shardRTTMs is each shard's round-trip time, probed once at Startup and
	// held so the worker ceiling can be re-derived whenever the observed replica count (and with it each
	// shard's pool) changes - the ceiling is a function of the pool, so it must follow it.
	workersDispatch int
	shardRTTMs      map[int]float64

	// Single-slot refiller
	refillTrigger chan struct{}
	refillStop    chan struct{}
	refiller      sync.WaitGroup
	// Most recent refill's selected band and its distinct-fairness-key count, observed by the
	// dwarf_steps_fairness_keys gauge. Written by the single refiller goroutine, read at metric
	// collection time. lastRefillBand < 0 means no refill has selected a band yet.
	lastRefillLock sync.Mutex
	lastRefillBand int
	lastRefillKeys int

	// Timer goroutine
	nextPoll     time.Time
	nextPollLock sync.Mutex
	wakeTimer    chan struct{}
	timerStop    chan struct{}
	timerWorker  sync.WaitGroup

	// Recovery goroutine: runs the defense-in-depth parked-step wedge sweep on its own slow cadence,
	// off the hot poll path.
	recoveryStop   chan struct{}
	recoveryWorker sync.WaitGroup

	// Reaper goroutine: deletes flows whose delete_after_ms window has elapsed, on its own ~1min ticker.
	reaperStop   chan struct{}
	reaperWorker sync.WaitGroup

	// Wait registry for Await
	waitersLock sync.Mutex
	waiters     map[string][]chan string

	// Per-flow parsed-graph cache. The graph JSON is frozen at flow creation, so processStep reuses
	// the parsed *workflow.Graph across the flow's steps instead of re-unmarshalling it each step.
	graphCache *lru.Cache[graphCacheKey, *cachedGraph]

	// Lifecycle
	started        atomic.Bool
	lifetimeCtx    context.Context
	lifetimeCancel context.CancelFunc

	// Recovery/await/reap timing. Effectively constants in production (defaulted in NewEngine, then
	// read-only); a white-box test shortens one directly, before Startup for the two read once at Startup
	// (reapInterval, wedgeSweepInterval), so an otherwise minutes-long path is observable.
	leaseMargin         time.Duration // added to a step's budget when sizing the crash-recovery lease (30s)
	awaitPollInterval   time.Duration // Await lost-wake re-snapshot cadence (5s)
	pingInterval        time.Duration // peer-discovery heartbeat cadence; peers expire at 3x (20s)
	refillPace          time.Duration // pause after a refill that filled the cache (deep backlog); see refillerLoop (20ms)
	wedgeSweepInterval  time.Duration // parked-step wedge sweep tick; read once at Startup (5m)
	parkWedgeThreshold  time.Duration // min age before a parked step is treated as wedged (5m)
	orphanFlowThreshold time.Duration // min age before a stepless running flow is reported orphaned (5m)
	deletionGrace       time.Duration // DeleteOnCompletion linger window before reap (1m)
	reapInterval        time.Duration // reaper goroutine tick; read once at Startup (1m)

	// Test-only instrumentation seams (see debug.go), delegated to a seamster.Seamster constructed enabled
	// under testing.Testing() in NewEngine. Every consult is a lock-free bool read in production.
	seams *seamster.Seamster
}

// NewEngine creates a new workflow engine.
func NewEngine() *Engine {
	e := &Engine{
		logger:         slog.New(slog.DiscardHandler),
		lastRefillBand: -1,
	}
	e.workers.Store(64)
	e.timeBudgetMs.Store(int64(2 * time.Minute / time.Millisecond))
	e.defaultPriority.Store(100)
	e.engineID = int64(rand.Uint64() >> 1) // positive, 63 bits of entropy
	e.instanceID = strconv.FormatInt(e.engineID, 36)
	e.peers = map[string]time.Time{e.instanceID: time.Now()}
	e.lastAppliedR.Store(1)
	e.leaseMargin = 30 * time.Second
	e.awaitPollInterval = 5 * time.Second
	e.pingInterval = 20 * time.Second
	e.refillPace = 20 * time.Millisecond
	e.wedgeSweepInterval = 5 * time.Minute
	e.parkWedgeThreshold = 5 * time.Minute
	e.orphanFlowThreshold = 5 * time.Minute
	e.deletionGrace = 1 * time.Minute
	e.reapInterval = 1 * time.Minute
	e.seams = seamster.New(testing.Testing())
	return e
}

// --- Configuration setters ---
//
// Setters split into Live (callable any time) and construction-time-only (rejected after Startup).

// errSetAfterStartup is the error the construction-time-only setters return on a running engine.
func errSetAfterStartup(what string) error {
	return errors.New(what + " cannot be changed after Startup")
}

// ShardSpec declares one database shard: the facts about it the operator can readily provide, from
// which the engine derives its tuning (connection budget, placement weight).
type ShardSpec struct {
	// Index identifies the shard. Indices must be >= 1 and unique, but need not be contiguous (shards
	// 1 and 99 are fine). The index is encoded into every flow key created on the shard and drives
	// routing, so the index-to-DSN mapping must be identical across all replicas and stable across
	// restarts.
	Index int
	// DSN is the connection string of the shard's database (dialect auto-detected), used EXACTLY as
	// given - the engine never formats or rewrites it, so a percent-encoded credential (a password
	// "p@ss" written "p%40ss") survives intact. Each shard is declared with its own DSN; there is no
	// template. An empty DSN is only valid in test mode, where the DSN IS a template (a "%d" is replaced
	// with the shard index, which is what gives each shard its own isolated test database).
	DSN string
	// VirtualCPUs is the CPU count of the shard's database server - a fact off the instance's spec
	// sheet. It drives the shard's connection budget (the pool is capped near the measured knee of ~6x
	// the CPU count, beyond which connections only queue - and on small servers actively harm
	// throughput) and its placement weight (new flows are distributed across shards in proportion to
	// measured capacity). Left at 0, the engine assumes 2 - the smallest machine any major cloud sells
	// as a current-generation instance, so the assumed pool stays safe even if the real machine is
	// smaller. Declare it: a large database sized as if it were a 2-CPU one runs at a fraction of its
	// capacity.
	VirtualCPUs int
	// Cordoned excludes the shard from new-flow placement. Everything already resident proceeds
	// normally: existing flows keep executing, and subgraph children, thread continuations (Continue),
	// and forks - all shard-pinned - are still created on it. Use for retiring or overloaded shards.
	Cordoned bool
}

// SetShard registers a database shard. Call once per shard; see ShardSpec for field semantics. When no
// shard is registered, Startup opens a single default shard 1 (in test mode, an isolated in-memory
// database).
//
// Construction-time only: shards are opened and migrated at Startup and the set is immutable for the
// engine's life, so a call on a running engine is rejected. Changing the shard set requires a coordinated
// restart of every replica (a maintenance window): a flow created on a shard unknown to a peer replica is
// unroutable (404) there.
func (e *Engine) SetShard(spec ShardSpec) error {
	if e.started.Load() {
		return errSetAfterStartup("shards")
	}
	if spec.Index < 1 {
		return errors.New("shard index must be at least 1")
	}
	if spec.VirtualCPUs < 0 {
		return errors.New("virtual CPUs must be >= 0")
	}
	e.shardsLock.Lock()
	defer e.shardsLock.Unlock()
	if _, ok := e.shardSpecs[spec.Index]; ok {
		return errors.New("shard %d is already set", spec.Index)
	}
	if e.shardSpecs == nil {
		e.shardSpecs = map[int]ShardSpec{}
	}
	e.shardSpecs[spec.Index] = spec
	return nil
}

// SetWorkers is an expert override that pins the maximum number of worker goroutines, replacing the
// derived default. The default is the lease-margin ceiling: the largest pool that keeps a synchronized
// completion storm (every in-flight task released at once by a recovering downstream) draining inside
// the crash-recovery lease margin, derived at Startup from each shard's connection budget and its
// measured round-trip time. It contains no assumption about task duration, so it is correct for any
// workload; the pool grows into it only on demand, so a short-task deployment never pays for the
// headroom.
//
// Operators normally never call this. Reasons to: memory (the ceiling can be tens of thousands of
// workers, and each in-flight step also holds its state map - a bound the engine cannot see); a
// deliberately smaller global concurrency cap; deterministic tests (SetWorkers(1) serializes dispatch);
// and benchmark sweeps. Setting it ABOVE the ceiling is allowed - an operator may consciously trade the
// risk of duplicate task execution in a storm for long-task throughput - and is logged as a warning at
// Startup. SetWorkers(0) is a valid shape: a replica that creates, awaits, and serves reads but never
// executes tasks. Construction-time only: the pool bound is fixed at Startup, so a call on a running
// engine is rejected.
func (e *Engine) SetWorkers(n int) error {
	if e.started.Load() {
		return errSetAfterStartup("workers")
	}
	if n < 0 {
		return errors.New("workers must be >= 0", http.StatusBadRequest)
	}
	e.workers.Store(int32(n))
	e.workersSet.Store(true)
	return nil
}

// SetTimeBudget sets the default duration for a single task execution, used by any flow that does not
// override it via FlowOptions.TimeBudget. Live: read fresh on each Create (existing flows keep the budget
// frozen at their own Create).
func (e *Engine) SetTimeBudget(d time.Duration) error {
	e.timeBudgetMs.Store(int64(d / time.Millisecond))
	return nil
}

// SetDefaultPriority sets the default priority for new flows: an integer >= 1, lower runs first. Live:
// read fresh on each Create, so it takes effect on a running engine immediately.
//
// The lower bound is not cosmetic. The refiller selects the strict-minimum band with a `priority=(SELECT
// MIN(priority) ...)` subquery, but a step is only a candidate at all through predicates the scan applies
// to a positive band; a flow stamped with a non-positive priority would sit `pending` forever, invisible
// to selection, while the doorbell re-rang it in a loop. The upper bound guards the column's int32 width -
// an int that overflows it (3_000_000_000, say) would wrap NEGATIVE and produce exactly that hang.
// `FlowOptions.Priority` is separately validated at Create, where 0 means "unset, take this default".
func (e *Engine) SetDefaultPriority(p int) error {
	if p < 1 {
		return errors.New("default priority must be >= 1", http.StatusBadRequest)
	}
	if p > math.MaxInt32 {
		return errors.New("default priority must fit in an int32", http.StatusBadRequest)
	}
	e.defaultPriority.Store(int32(p))
	return nil
}

// SetMaxOpenConns is an expert override that pins every shard's connection pool to exactly n open (and
// idle) connections, replacing the per-shard budget the engine derives from ShardSpec.VirtualCPUs.
// Operators normally never call this - provide VirtualCPUs instead and let the engine size the pool at
// the measured knee (~6x the database's CPU count). The override exists for benchmarking (pool-size
// sweeps) and for deployments whose connection budget is constrained by something the engine cannot see
// (e.g. a shared database or an external pooler). Live: pushes to every open shard immediately.
func (e *Engine) SetMaxOpenConns(n int) error {
	if n < 1 {
		return errors.New("max open connections must be >= 1", http.StatusBadRequest)
	}
	e.maxOpenConns.Store(int32(n))
	e.db.SetMaxIdleConns(n)
	e.db.SetMaxOpenConns(n)
	// The worker ceiling is a function of the pool - it bounds how fast a completion storm can drain
	// through M connections - so a live pool change must re-derive it. Skipping this leaves the ceiling
	// at the size computed for the OLD pool: an override that shrinks the pool (the external-pooler case)
	// would keep a bound many times too permissive on exactly the storm the ceiling exists to contain.
	// Note this is also the only path that re-derives it once an override is set, because recomputePools
	// (the fleet-change path) early-returns while the override pins the pools.
	if e.started.Load() {
		e.recomputeWorkerCeiling(e.lifetimeCtx)
	}
	return nil
}

// SetHost registers the host the engine reaches the outside world through: it loads graphs, executes
// tasks, and (optionally) receives flow-stop notifications and carries cross-replica coordination signals.
// A host must implement LoadGraph and ExecuteTask; the remaining Host methods may be no-ops.
// Construction-time only.
func (e *Engine) SetHost(h Host) error {
	if e.started.Load() {
		return errSetAfterStartup("host")
	}
	e.host = h
	return nil
}

// SetMeterProvider sets the OpenTelemetry MeterProvider the engine builds its dwarf_* instruments from.
// Defaults to the global otel.GetMeterProvider() (the no-op provider unless the host configures the OTEL
// SDK). The engine creates instruments under the "github.com/microbus-io/dwarf" scope; the provider's
// Resource carries the host service's identity. Construction-time only - the engine resolves the meter
// once at Startup.
func (e *Engine) SetMeterProvider(mp metric.MeterProvider) error {
	if e.started.Load() {
		return errSetAfterStartup("meter provider")
	}
	e.meterProvider = mp
	return nil
}

// SetTracerProvider sets the OpenTelemetry TracerProvider the engine builds its spans from. Defaults to
// the global otel.GetTracerProvider() (the no-op provider unless the host configures the OTEL SDK). The
// engine emits one span per flow and one span per task, nested to mirror the call structure, under the
// "github.com/microbus-io/dwarf" scope; the provider's Resource carries the host's identity. The host
// injects only the provider - it writes no span or context code. Construction-time only - the engine
// resolves the tracer once at Startup.
func (e *Engine) SetTracerProvider(tp trace.TracerProvider) error {
	if e.started.Load() {
		return errSetAfterStartup("tracer provider")
	}
	e.tracerProvider = tp
	return nil
}

// SetLogger sets the structured logger. The engine logs through the *Context variants
// (DebugContext/InfoContext/WarnContext/ErrorContext) so a handler that reads the context - e.g. the
// otelslog bridge - can correlate each record with the active step span. A host routes logs to OTEL by
// passing a logger whose handler bridges there. Defaults to a discard logger: until a logger is injected
// the engine (and its sequel DB layer) stay silent rather than writing to the application-owned
// slog.Default(). A nil logger resets to that silent default. Construction-time only - the engine resolves
// the logger once at Startup.
func (e *Engine) SetLogger(l *slog.Logger) error {
	if e.started.Load() {
		return errSetAfterStartup("logger")
	}
	if l == nil {
		l = slog.New(slog.DiscardHandler)
	}
	e.logger = l
	return nil
}

// SetDebugLogger is a convenience that wires a human-readable text logger to stderr at debug level -
// shorthand for SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level:
// slog.LevelDebug}))). It is meant for development and test runs where you want to see the engine's (and
// its sequel DB layer's) internal logging without standing up an OTEL pipeline. Output goes to stderr, not
// stdout, so it never mixes with a program's data stream - the standard convention for diagnostic logs.
// Because it routes through SetLogger, it counts as an explicitly-set logger, so it also reaches sequel via
// the engine's existing SetLogger wiring (sequel's migration logs appear too). Construction-time only.
func (e *Engine) SetDebugLogger() error {
	return e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

// --- Lifecycle ---

// Startup initializes the engine: opens database connections, runs migrations,
// and starts worker goroutines.
func (e *Engine) Startup(ctx context.Context) error {
	if e.started.Load() {
		return errors.New("engine is already started")
	}
	if e.host == nil {
		return errors.New("host is required")
	}
	// Per-shard pool sizes: the SetMaxOpenConns override pins every pool; otherwise each shard's budget
	// derives from its own VirtualCPUs (heterogeneous fleets get heterogeneous pools), split across the
	// observed replicas. At Startup only this replica is known (R=1, full budget); the hello broadcast
	// at the end of Startup discovers peers within ~a heartbeat and recomputePools shrinks the shares.
	override := int(e.maxOpenConns.Load())
	replicas := e.observedReplicas()
	e.shardsLock.Lock()
	shards := make(map[int]database.ShardConfig, len(e.shardSpecs))
	for idx, spec := range e.shardSpecs {
		idle, open := shardPool(spec, override, replicas)
		shards[idx] = database.ShardConfig{DSN: spec.DSN, MaxIdleConns: idle, MaxOpenConns: open}
	}
	e.shardsLock.Unlock()
	if len(shards) == 0 {
		// No shard registered: the single default shard, sized by the same rule (no spec = defaults).
		idle, open := shardPool(ShardSpec{}, override, replicas)
		shards[1] = database.ShardConfig{MaxIdleConns: idle, MaxOpenConns: open}
	}
	// The RESIDENT worker set (and, with it, the candidate cache and every refill scan) is sized from
	// the aggregate connection budget: dispatch is database-bound, so 8 x conns keeps the pool saturated
	// for short tasks, floored at the historical 64 for the zero-config case.
	totalConns := 0
	for _, sc := range shards {
		totalConns += sc.MaxOpenConns
	}
	e.workersDispatch = max(64, workersPerConnBudget*totalConns)

	err := e.db.Open(ctx, database.Config{
		Shards:         shards,
		TestID:         e.testHashedID,
		Logger:         e.logger,
		TracerProvider: e.tracerProvider,
		MeterProvider:  e.meterProvider,
	})
	if err != nil {
		return errors.Trace(err)
	}

	// Measure the RTT to each shard (a few SELECT 1s on connections just opened) and hold it: the worker
	// ceiling is a function of the shard's POOL, which changes when peers are discovered or an override
	// lands, so the ceiling is re-derived on those events (recomputeWorkerCeiling) rather than frozen
	// here. The map is published under shardsLock because its readers are the peer-signal goroutines and
	// the live SetMaxOpenConns, and a Shutdown/Startup restart reassigns it - an unsynchronized map
	// read/write is a fatal throw, not a recoverable panic.
	rtts := make(map[int]float64, len(shards))
	for idx := range shards {
		db, dbErr := e.db.Shard(idx)
		if dbErr != nil {
			continue
		}
		rttMs := probeRTT(ctx, db)
		if rttMs <= 0 {
			rttMs = defaultRTTMs // probe failed: fall back to the measured same-zone constant
		}
		rtts[idx] = rttMs
		e.logger.DebugContext(ctx, "Shard RTT probed", "shard", idx, "rttMs", rttMs)
	}
	e.shardsLock.Lock()
	e.shardRTTMs = rtts
	e.shardsLock.Unlock()
	e.recomputeWorkerCeiling(ctx)

	e.initRuntime()
	return nil
}

// Shutdown stops all worker goroutines and closes database connections. Idempotent: a call on an engine
// that is not running (never started, or already shut down) is a no-op, so it is safe to defer and to call
// more than once.
func (e *Engine) Shutdown(ctx context.Context) error {
	if !e.started.CompareAndSwap(true, false) {
		return nil
	}
	e.drainRuntime()
	e.db.Close()
	return nil
}

// SetInTest puts the engine into test mode keyed by name, so a subsequent Startup opens per-name isolated,
// auto-dropped databases (via sequel.CreateTestingDatabase) instead of the configured ones. Construction-
// time only. It is the *testing.T-free counterpart to RunInTest, for a host running under an external test
// harness that has no *testing.T but a stable per-test isolation key: every engine sharing the
// same name resolves to the same isolated databases, so the replicas of a multi-replica test app converge
// on one set. RunInTest(t) is SetInTest(t.Name()) plus Startup and a t.Cleanup shutdown.
func (e *Engine) SetInTest(name string) error {
	if e.started.Load() {
		return errSetAfterStartup("test mode")
	}
	// Hash the name to a short, bounded id so the testing-database name sequel derives stays within the
	// strictest SQL identifier limit (Postgres 63 / MySQL 64), whatever the name's length. A non-empty
	// testHashedID becomes Config.TestID, which switches the ShardSet's open path onto the isolated-test
	// path. It is set before initRuntime flips started, so it is in place before any shard opens.
	sum := sha256.Sum256([]byte(name))
	e.testHashedID = hex.EncodeToString(sum[:])[:16]
	return nil
}

// RunInTest initializes the engine for testing with per-test isolated databases (keyed by t.Name()) and
// registers cleanup via t.Cleanup.
//
// Unless the caller wired its own logger, the engine logs to stderr at Info by default (rather than the
// production discard default) so a CI failure has engine-level clues - flow-status transitions show where a
// flow got stuck, and the Error logs surface wedge sweeps / poll / refill faults. stderr, not t.Log, because
// a `go test` timeout panic drops the failing test's buffered t.Log output but not stderr. Override the level
// with DWARF_TEST_LOG_LEVEL (e.g. "error" to quiet local runs, "debug" for the full play-by-play), or call
// SetLogger/SetDebugLogger before RunInTest to take over entirely.
func (e *Engine) RunInTest(t *testing.T) {
	t.Helper()
	if e.logger.Handler() == slog.DiscardHandler {
		level := slog.LevelInfo
		if s := os.Getenv("DWARF_TEST_LOG_LEVEL"); s != "" {
			_ = level.UnmarshalText([]byte(s))
		}
		_ = e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	}
	err := e.SetInTest(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	err = e.Startup(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		e.Shutdown(t.Context())
	})
}

// initRuntime starts all goroutines and initializes runtime state.
func (e *Engine) initRuntime() {
	e.lifetimeCtx, e.lifetimeCancel = context.WithCancel(context.Background())
	// How many workers to spawn eagerly. An explicit SetWorkers is a request for exactly that many, so it
	// is honored in full: the operator's number is the pool, not a ceiling the pool might never reach
	// (no-op tasks never park, so growth would never take it there). The DERIVED maximum is the
	// lease-margin ceiling - tens of thousands - and is emphatically not spawned up front; there the
	// resident set is the connection-derived dispatch count and growth fills the rest on demand.
	maxWorkers := int(e.workers.Load())
	resident := maxWorkers
	if !e.workersSet.Load() {
		resident = min(e.workersDispatch, maxWorkers)
	}
	// The cache (and every refill scan, which reads up to its capacity) is sized from the DISPATCH count,
	// never from the worker maximum: a worker parked in a long ExecuteTask holds no connection and
	// dispatches nothing, so letting the ceiling size the cache would scan a backlog orders of magnitude
	// larger than the engine can ever claim. In test mode the dispatch count is unresolved for an engine
	// with no shards, so fall back to the resident set.
	cacheWorkers := min(resident, e.workersDispatch)
	if e.workersDispatch == 0 {
		cacheWorkers = resident
	}
	e.cache.Init(cacheWorkers)
	e.refillTrigger = make(chan struct{}, 1)
	e.refillStop = make(chan struct{})
	e.wakeTimer = make(chan struct{}, 1)
	e.timerStop = make(chan struct{})
	e.recoveryStop = make(chan struct{})
	e.reaperStop = make(chan struct{})
	e.nextPoll = time.Now()
	e.graphCache = lru.New[graphCacheKey, *cachedGraph](4096, 15*time.Minute)
	e.waiters = nil
	e.started.Store(true)

	// Create the dwarf_* instruments and register the observable-gauge callback before workers start
	// emitting. Falls back to the global (no-op) provider when none was injected.
	err := e.initMetrics()
	if err != nil {
		e.logger.ErrorContext(e.lifetimeCtx, "Initializing metrics", "error", err)
	}

	// Resolve the tracer (no-op unless a TracerProvider was injected or the global SDK is configured).
	e.initTracer()

	// Spawn the resident set eagerly; the rest of the ceiling is spawned on demand by a worker that finds
	// every peer parked inside ExecuteTask (see maybeSpawnWorker) - the long-task case, where the resident
	// set alone would cap throughput because nobody is left to dispatch.
	e.spawnLock.Lock()
	e.spawnClosed = false
	e.workersResident.Store(0)
	e.workersInTask.Store(0)
	e.spawnLock.Unlock()
	for range resident {
		e.spawnWorker()
	}
	e.timerWorker.Go(func() {
		e.timerLoop(e.lifetimeCtx)
	})
	e.refiller.Go(func() {
		e.refillerLoop(e.lifetimeCtx)
	})
	e.recoveryWorker.Go(func() {
		e.recoveryLoop(e.lifetimeCtx)
	})
	e.reaperWorker.Go(func() {
		e.reaperLoop(e.lifetimeCtx)
	})
	// Peer discovery: refresh self, start the heartbeat, and announce this replica. Receivers reply
	// with an immediate ping, so the observed replica count converges within one round trip and
	// recomputePools shrinks the derived shares from the R=1 the pools were just opened with.
	e.peersLock.Lock()
	e.peers[e.instanceID] = time.Now()
	e.peersLock.Unlock()
	e.peersStop = make(chan struct{})
	e.peersLoop.Go(func() {
		e.runPeersLoop()
	})
	e.emitSignal(e.lifetimeCtx, signalOpHello, peerPayload{Origin: e.instanceID})
	e.requestRefill()
}

// spawnWorker adds one worker goroutine to the pool, unless the pool is draining (spawnClosed) or the
// maximum is already reached. Called for the resident set at startup and on demand thereafter. The
// spawnLock makes the resident count and the WaitGroup.Add inside Go atomic with respect to the drain,
// which sets spawnClosed before it Waits - a goroutine joining a WaitGroup concurrently with Wait is a
// panic, so the flag, not a bare counter check, is what makes shutdown safe.
func (e *Engine) spawnWorker() {
	e.spawnLock.Lock()
	defer e.spawnLock.Unlock()
	if e.spawnClosed || int(e.workersResident.Load()) >= int(e.workers.Load()) {
		return
	}
	e.workersResident.Add(1)
	e.workerPool.Go(func() {
		e.workerLoop(e.lifetimeCtx)
	})
}

// maybeSpawnWorker grows the pool by one when EVERY spawned worker is parked inside the host's
// ExecuteTask, so not one of them is left to dispatch. That is the long-task signature, and it is the
// only condition under which another worker helps: a worker inside ExecuteTask holds no database
// connection, so its replacement adds dispatch capacity rather than competing for the pool.
//
// The counter must therefore track time inside ExecuteTask and NOTHING ELSE. An earlier version counted
// time inside processStep - which includes waiting for a database connection - and so fired on ordinary
// saturation: workers queued on the pool, the engine read that as "all busy", spawned more workers, and
// those queued on the same pool too. A positive feedback loop on a signal that measured contention
// rather than parking; it cost ~20% throughput on a saturated 8-vCPU shard (2,902 vs 3,523 steps/s) and
// bloated the pool to ~1,300 workers where ~512 sufficed. Any future change here must keep asking: does
// the worker this condition counts hold a connection? If it might, the condition is measuring the wrong
// thing.
func (e *Engine) maybeSpawnWorker() {
	if int(e.workersInTask.Load()) >= int(e.workersResident.Load()) {
		e.spawnWorker()
	}
}

// drainRuntime stops all goroutines in order. The caller (Shutdown) has already flipped e.started to false
// via CompareAndSwap, which also serves as the single-shutdown guard.
func (e *Engine) drainRuntime() {
	// Close the pool to new workers BEFORE the WaitGroup is waited on below (an Add concurrent with a
	// Wait panics). After this, a worker mid-loop can still finish its step; it just cannot spawn a peer.
	e.spawnLock.Lock()
	e.spawnClosed = true
	e.spawnLock.Unlock()
	// Announce the graceful exit first (fire-and-forget) so peers drop this replica and regrow their
	// pool shares immediately rather than waiting out the 3x-ping expiry, then stop the heartbeat.
	e.emitSignal(context.Background(), signalOpGoodbye, peerPayload{Origin: e.instanceID})
	if e.peersStop != nil {
		close(e.peersStop)
	}
	e.peersLoop.Wait()
	// Unregister the observable-gauge callback first so the OTEL reader cannot invoke it (and query the
	// shards) while/after the databases are being closed.
	e.closeMetrics()
	e.cache.Close()
	e.workerPool.Wait()
	if e.timerStop != nil {
		close(e.timerStop)
	}
	e.timerWorker.Wait()
	// The recovery loop can requestRefill (a wedge sweep that unparks a stuck subgraph caller re-offers it),
	// so drain it before the refiller, mirroring the timer.
	if e.recoveryStop != nil {
		close(e.recoveryStop)
	}
	e.recoveryWorker.Wait()
	// The reaper only deletes rows (never requestRefill), so its drain order relative to the refiller does
	// not matter; stop it here and let an in-progress pass finish its current tree-delete and exit.
	if e.reaperStop != nil {
		close(e.reaperStop)
	}
	e.reaperWorker.Wait()
	if e.refillStop != nil {
		close(e.refillStop)
	}
	e.refiller.Wait()
	// Wake every blocked Await with the shutdown sentinel so it returns an error rather than re-snapshotting
	// and re-blocking on a channel no goroutine will ever signal again (workers/timer/refiller are already
	// drained above). A non-blocking send: if a real stop status is already buffered, that wins (the awaiter
	// re-snapshots and returns the outcome) and the sentinel is simply dropped.
	e.waitersLock.Lock()
	for _, chans := range e.waiters {
		for _, ch := range chans {
			select {
			case ch <- awaitShutdownSignal:
			default:
			}
		}
	}
	e.waitersLock.Unlock()
	if e.lifetimeCancel != nil {
		e.lifetimeCancel()
	}
}

// requestRefill asks the refiller to run a selection scan.
func (e *Engine) requestRefill() {
	select {
	case e.refillTrigger <- struct{}{}:
	default:
	}
}

// shortenNextPoll updates nextPoll to tm if tm is earlier, and wakes the timer.
func (e *Engine) shortenNextPoll(tm time.Time) {
	e.nextPollLock.Lock()
	// Lower nextPoll to tm, or replace it if it already lies in the past (a fired deadline the timer is
	// mid-poll on) - else a wake request later than that stale value is dropped and lost to a re-dispatch wedge.
	if tm.Before(e.nextPoll) || e.nextPoll.Before(time.Now()) {
		e.nextPoll = tm
	}
	e.nextPollLock.Unlock()
	e.nudgeTimer()
}

// nudgeTimer nudges the timer goroutine to re-evaluate its deadline.
func (e *Engine) nudgeTimer() {
	select {
	case e.wakeTimer <- struct{}{}:
	default:
	}
}

// shardOrdinals returns the open shard indices in ascending order plus a map from shard index to its
// ordinal position. Per-shard scratch under OnEach is sized len(indices) and written at pos[shard], so
// concurrent shard goroutines write distinct elements (race-free without a lock) even though the sparse
// shard indices cannot themselves index a slice.
func (e *Engine) shardOrdinals() (indices []int, pos map[int]int) {
	indices = e.db.Indices()
	pos = make(map[int]int, len(indices))
	for i, idx := range indices {
		pos[idx] = i
	}
	return indices, pos
}

// taskTimeBudget returns the current time budget for task dispatch.
func (e *Engine) taskTimeBudget() time.Duration {
	return time.Duration(e.timeBudgetMs.Load()) * time.Millisecond
}

// resolveFlowOptions applies defaults to caller-supplied options. TimeBudget falls back to the engine
// default and is frozen onto the returned options.
func (e *Engine) resolveFlowOptions(opts *workflow.FlowOptions) *workflow.FlowOptions {
	resolved := &workflow.FlowOptions{
		Priority:       int(e.defaultPriority.Load()),
		FairnessWeight: 1,
		TimeBudget:     e.taskTimeBudget(),
	}
	if opts != nil {
		if opts.Priority > 0 {
			resolved.Priority = opts.Priority
		}
		if opts.FairnessWeight > 0 {
			resolved.FairnessWeight = opts.FairnessWeight
		}
		resolved.FairnessKey = opts.FairnessKey
		resolved.Baggage = opts.Baggage
		resolved.DeleteOnCompletion = opts.DeleteOnCompletion
		resolved.ThreadKey = opts.ThreadKey
		if opts.TimeBudget > 0 {
			resolved.TimeBudget = opts.TimeBudget
		}
	}
	return resolved
}

// --- Public API ---

// Create creates a new flow for a workflow and starts it, returning the running flow's key. opts carries
// the flow's policy (scheduling, DeleteOnCompletion, Baggage, ThreadKey); nil uses defaults.
// For a flow that must wait for an external trigger, have the entry task call flow.Interrupt and resume it
// with Resume (which, unlike a separate start, also delivers a payload).
func (e *Engine) Create(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	return e.create(ctx, workflowURL, initialState, opts)
}

// Snapshot returns the current state and status of a flow.
func (e *Engine) Snapshot(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	return e.snapshot(ctx, flowKey)
}

// Fingerprint returns a fingerprint and status for change detection.
func (e *Engine) Fingerprint(ctx context.Context, flowKey string) (fingerprint string, status string, err error) {
	return e.fingerprint(ctx, flowKey)
}

// Resume continues a flow paused by flow.Interrupt.
func (e *Engine) Resume(ctx context.Context, flowKey string, resumeData any) error {
	return e.resume(ctx, flowKey, resumeData)
}

// Cancel aborts a flow.
func (e *Engine) Cancel(ctx context.Context, flowKey string, reason string) error {
	return e.cancel(ctx, flowKey, reason)
}

// Fork clones a terminal flow's prefix up to the given step into a new, self-contained running flow and
// re-executes from that step with optional stateOverrides applied to it. The original flow is never
// modified. The fork inherits the original's scheduling and baggage (it does not take FlowOptions).
// Returns the new flow's key.
func (e *Engine) Fork(ctx context.Context, stepKey string, stateOverrides any) (string, error) {
	return e.forkFlow(ctx, stepKey, stateOverrides)
}

// History returns the step-by-step execution history of a flow.
func (e *Engine) History(ctx context.Context, flowKey string) ([]workflow.FlowStep, error) {
	return e.history(ctx, flowKey)
}

// Step returns details of a single step.
func (e *Engine) Step(ctx context.Context, stepKey string) (*workflow.FlowStep, error) {
	return e.step(ctx, stepKey)
}

// List queries flows by status, workflow name, or thread key.
func (e *Engine) List(ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, string, error) {
	return e.list(ctx, query)
}

// Delete removes a flow and its steps.
func (e *Engine) Delete(ctx context.Context, flowKey string) error {
	return e.deleteFlow(ctx, flowKey)
}

// Purge marks flows matching a query (and their subgraph subtrees) for deletion; a background reaper removes
// them shortly after. Marked flows are excluded from List/History immediately. Returns the count of roots
// marked - no more than 4096 per call; iterate to mark more. Running flows are skipped.
func (e *Engine) Purge(ctx context.Context, query workflow.Query) (int, error) {
	return e.purge(ctx, query)
}

// ShardInfo returns health and size summaries for all shards.
func (e *Engine) ShardInfo(ctx context.Context) ([]ShardSummary, error) {
	return e.shardInfo(ctx)
}

// Await blocks until a flow stops, or until the ctx expires (which returns an error).
func (e *Engine) Await(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	outcome, err := e.await(ctx, flowKey)
	if err != nil {
		return nil, err
	}
	if !outcome.Stopped() {
		// await returned a non-terminal outcome only because the ctx ended before the flow stopped.
		return nil, errors.Trace(ctx.Err(), http.StatusRequestTimeout)
	}
	return outcome, nil
}

// Poll returns a flow's current outcome, blocking up to the ctx deadline for it to stop. Unlike Await, a ctx
// timeout is not an error: it returns the current non-terminal outcome, whose Stopped() reports false, so a
// caller bridging an open-ended flow to a bounded request (e.g. an HTTP poll) can answer within its budget and
// re-poll. A real failure still returns an error.
func (e *Engine) Poll(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	return e.await(ctx, flowKey)
}

// Run creates, starts, and awaits a flow in one call, returning the new flow's key alongside its outcome
// (the key is the flow's identity, not part of the outcome). opts carries scheduling and the opaque host
// Baggage; nil opts uses defaults.
//
// Error semantics differ by phase. A create failure returns flowKey "" and a nil outcome - no flow
// exists. An await failure - most commonly the caller's ctx expiring before the flow stops - leaves the
// flow running (it is durable and not bound to this call) and returns its flowKey with a nil outcome and
// the error, so the caller keeps a handle to Await/Snapshot/Cancel it later. Run never cancels the flow
// on the caller's behalf; a caller that wants the flow torn down on timeout calls Cancel explicitly.
func (e *Engine) Run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, outcome *workflow.FlowOutcome, err error) {
	return e.run(ctx, workflowURL, initialState, opts)
}

// Continue creates a new flow from the latest completed flow in a thread, inheriting that flow's policy
// (scheduling, baggage) - it does not take FlowOptions. For a turn with different policy,
// use Create with FlowOptions.ThreadKey.
func (e *Engine) Continue(ctx context.Context, threadKey string, additionalState any) (string, error) {
	return e.continueFlow(ctx, threadKey, additionalState)
}

// HistoryMermaid writes the execution DAG of a flow as a Mermaid diagram.
func (e *Engine) HistoryMermaid(ctx context.Context, flowKey string, w io.StringWriter) error {
	steps, err := e.history(ctx, flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	mmd, err := workflow.NewFlowRenderer(steps).WithLinks("step").Render()
	if err != nil {
		return errors.Trace(err)
	}
	_, err = w.WriteString(mmd)
	return errors.Trace(err)
}
