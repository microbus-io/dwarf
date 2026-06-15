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
	"io"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"log/slog"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/throttle"
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
	leaseMargin         = 30 * time.Second
	backlogPollInterval = 1 * time.Minute

	// wedgeSweepInterval is the cadence of the dedicated recovery goroutine that runs the defense-in-depth
	// parked-step wedge sweep. It is kept off the frequently-nudged poll path because the sweep's scans are
	// heavy and latency-tolerant. parkWedgeThreshold is the minimum age a parked step must reach before the
	// sweep treats it as wedged - comfortably beyond normal subgraph-completion latency and the 1-minute max
	// breaker-probe interval, so steady-state operation never trips a false positive.
	wedgeSweepInterval = 5 * time.Minute
	parkWedgeThreshold = 5 * time.Minute

	parkedNone     = 0
	parkedSubgraph = 1
	parkedBreaker  = 2

	// Bounds for the per-flow parsed-graph cache (see graphcache.go). The graph JSON is frozen on the
	// flow row at creation, so a cached graph is immutable and never needs invalidation; entry count +
	// TTL alone keep the cache bounded. A graph definition is ~1-2KB parsed, so a few thousand entries
	// costs only single-digit MB while covering the active flows of a busy replica without thrashing.
	graphCacheMaxEntries = 4096
	graphCacheTTL        = 15 * time.Minute
)

// Engine is the standalone workflow orchestration engine.
type Engine struct {
	// Dependencies (set before Startup)
	host           Host
	logger         *slog.Logger
	loggerSet      bool // true once SetLogger received a non-nil logger; gates whether sequel logs
	meterProvider  metric.MeterProvider
	metrics        *engineMetrics
	tracerProvider trace.TracerProvider
	tracer         trace.Tracer

	// Configuration (atomically updated, safe to change after Startup)
	dsn             atomic.Value // string
	numShards       atomic.Int32
	workers         atomic.Int32
	timeBudgetMs    atomic.Int64
	defaultPriority atomic.Int32
	maxOpenConns    atomic.Int32

	// Database
	dbs     []*sequel.DB
	dbsLock sync.RWMutex
	// expandLock serializes ExpandShards so two concurrent calls cannot both observe the same shard
	// count and each open the same new shard. The slow open+migrate I/O runs under this lock but
	// outside dbsLock, so it never blocks the hot path (which only takes dbsLock).
	expandLock sync.Mutex
	// testHashedID is the hashed test database id when the engine was started in test mode (RunInTest /
	// StartupInTest), empty in production. ExpandShards reads it to route new shards through the
	// isolated-test open path. Written once during single-threaded startup before started.Store(true),
	// read only after started.Load()==true, so the atomic started flag is the happens-before barrier.
	testHashedID string

	// Candidate cache and worker pool
	cache      candidateCache
	workerPool sync.WaitGroup

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
	nextProbe    atomic.Int64
	wakeTimer    chan struct{}
	timerStop    chan struct{}
	timerWorker  sync.WaitGroup

	// Recovery goroutine: runs the defense-in-depth parked-step wedge sweep on its own slow cadence,
	// off the hot poll path.
	recoveryStop   chan struct{}
	recoveryWorker sync.WaitGroup

	// Wait registry for Await
	waitersLock sync.Mutex
	waiters     map[string][]chan string

	// Adaptive per-task dispatch rate
	valves     map[string]*taskValve
	valvesLock sync.RWMutex

	// Per-task breaker
	breakers     map[string]*taskBreaker
	breakersLock sync.RWMutex
	// breakerParkLocks serializes breakerBulkPark per task within this replica. A burst of flows hitting
	// a down task trips the breaker on many workers at once; their bulk-park UPDATEs all target the same
	// task_name rows and deadlock under pessimistic locking (SQL Server). Serializing per task removes the
	// self-inflicted storm; residual cross-replica contention is still handled by the retry loop.
	breakerParkLocks sync.Map // taskName -> *sync.Mutex

	// Per-flow parsed-graph cache. The graph JSON is frozen at flow creation, so processStep reuses
	// the parsed *workflow.Graph across the flow's steps instead of re-unmarshalling it each step.
	graphCache *lruCache[graphCacheKey, *workflow.Graph]

	// Lifecycle
	started        atomic.Bool
	lifetimeCtx    context.Context
	lifetimeCancel context.CancelFunc
}

// NewEngine creates a new workflow engine.
func NewEngine() *Engine {
	e := &Engine{
		// Default to a discard logger: a library stays silent until the host opts in by injecting one
		// (SetLogger), rather than writing unbidden to the application-owned slog.Default(). Non-nil so
		// the engine's e.logger.*Context call sites are nil-safe; produces no output until configured.
		logger:         slog.New(slog.DiscardHandler),
		lastRefillBand: -1,
	}
	e.dsn.Store("")
	e.numShards.Store(1)
	e.workers.Store(64)
	e.timeBudgetMs.Store(int64(2 * time.Minute / time.Millisecond))
	e.defaultPriority.Store(100)
	e.maxOpenConns.Store(8)
	return e
}

// --- Configuration setters ---
//
// These replace the old fluent WithX builder so each can return an error. They fall in two groups by
// whether the knob can be changed safely on a running engine:
//
//   - Live (take effect immediately, callable any time): SetNumShards, SetMaxOpenConns, SetTimeBudget,
//     SetDefaultPriority. SetTimeBudget/SetDefaultPriority are read fresh on each use; SetMaxOpenConns
//     re-applies to the live pools; SetNumShards opens+migrates the added shards.
//   - Construction-time-only (return an error if called after Startup): SetDSN, SetWorkers, SetHost,
//     SetLogger, SetDebugLogger, SetMeterProvider, SetTracerProvider. Applying these on a running engine
//     would mean reopening live connections, resizing the worker pool, or re-resolving a frozen provider -
//     deliberately unsupported, so the setter rejects it rather than silently no-op'ing.
//
// errSetAfterStartup is the error the construction-time-only setters return on a running engine.
func errSetAfterStartup(what string) error {
	return errors.New(what + " cannot be changed after Startup")
}

// SetDSN sets the SQL data source name. Use "%d" for sharded DSNs; an empty DSN in test mode uses SQLite
// in-memory. Construction-time only - the shards are opened against the DSN at Startup, so changing it on
// a running engine (which would require reopening live connections) is rejected.
func (e *Engine) SetDSN(dsn string) error {
	if e.started.Load() {
		return errSetAfterStartup("DSN")
	}
	e.dsn.Store(dsn)
	return nil
}

// SetNumShards sets the number of database shards and, on a running engine, brings any added shards online
// (open + migrate) in one call, returning any error from opening/migrating them. New flows spread onto the
// added shards immediately; existing flows stay on their original shard.
//
// The count may only grow at runtime: a value at or below the current live shard count records the new
// target but removes nothing (old shards drain naturally; an actual reduction takes effect only on a
// restart, where Startup opens just numShards shards). Concurrency-safe - the open+migrate work is
// serialized internally and runs off the hot path. Before Startup it simply records the target (no shards
// are open yet). It takes no ctx because the underlying open+migrate path is not ctx-cancellable.
func (e *Engine) SetNumShards(num int) error {
	e.numShards.Store(int32(num))
	return e.expandShards()
}

// SetWorkers sets the number of worker goroutines. Construction-time only: the pool is spawned at Startup
// at this count, and live resizing (spawning/retiring workers and resizing the candidate cache) is not
// supported, so a call on a running engine is rejected.
func (e *Engine) SetWorkers(n int) error {
	if e.started.Load() {
		return errSetAfterStartup("workers")
	}
	e.workers.Store(int32(n))
	return nil
}

// SetTimeBudget sets the maximum duration for a single task execution. Live: read fresh on each dispatch,
// so it takes effect on a running engine immediately.
func (e *Engine) SetTimeBudget(d time.Duration) error {
	e.timeBudgetMs.Store(int64(d / time.Millisecond))
	return nil
}

// SetDefaultPriority sets the default priority for new flows. Live: read fresh on each Create, so it takes
// effect on a running engine immediately.
func (e *Engine) SetDefaultPriority(p int) error {
	e.defaultPriority.Store(int32(p))
	return nil
}

// SetMaxOpenConns sets the maximum number of open SQL connections per shard and re-applies it to every
// live shard pool, so it takes effect on a running engine immediately. (sequel's pool setters are
// hot/atomic.) Before Startup it records the value, applied as each shard opens.
func (e *Engine) SetMaxOpenConns(n int) error {
	e.maxOpenConns.Store(int32(n))
	e.dbsLock.RLock()
	defer e.dbsLock.RUnlock()
	for _, db := range e.dbs {
		db.SetMaxOpenConns(n)
		db.SetMaxIdleConns(n)
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
// engine mints the root "workflow" span at Create (persisted to the dwarf-owned trace_parent column) and a
// per-step span in processStep, parented to the reconstructed root and placed on the TaskExecutor's
// context so the task's downstream spans nest under it. The host injects only the provider - no span code,
// no trace_parent handling. Spans are created under the "github.com/microbus-io/dwarf" scope; the
// provider's Resource carries the host's identity. Construction-time only - the engine resolves the tracer
// once at Startup.
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
// slog.Default(). A nil logger resets to that silent default. Construction-time only - the engine wires
// the logger into the worker hot path and the shard DBs at Startup, and reads it lock-free thereafter.
func (e *Engine) SetLogger(l *slog.Logger) error {
	if e.started.Load() {
		return errSetAfterStartup("logger")
	}
	if l == nil {
		l = slog.New(slog.DiscardHandler)
		e.loggerSet = false
	} else {
		e.loggerSet = true
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
	if e.host == nil {
		return errors.New("host is required")
	}
	err := e.openDatabase(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	e.initRuntime()
	return nil
}

// StartupInTest initializes the engine against isolated, throwaway test databases - one per shard, keyed by
// testID - instead of opening the configured DSN. It is for a host that is itself running under test (so it
// has no *testing.T to hand RunInTest) but must still convey "use a disposable, isolated database" down to
// sequel. The testID must be stable for the test run and shared by every replica that should see the same
// database (e.g. a Microbus plane, which all services in one test app share); distinct test runs pass
// distinct ids for isolation. The engine never learns the host's notion of "test mode" - it only receives
// this concrete instruction. Unlike RunInTest there is no cleanup hook; the host drives teardown by calling
// Shutdown (e.g. from its own shutdown lifecycle).
func (e *Engine) StartupInTest(ctx context.Context, testID string) error {
	if e.host == nil {
		return errors.New("host is required")
	}
	err := e.openTestDatabaseWithID(testID)
	if err != nil {
		return errors.Trace(err)
	}
	e.initRuntime()
	return nil
}

// Shutdown stops all worker goroutines and closes database connections.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.drainRuntime()
	e.closeDatabase()
	return nil
}

// RunInTest initializes the engine for testing with per-test SQLite databases
// and registers cleanup via t.Cleanup.
func (e *Engine) RunInTest(t *testing.T) {
	t.Helper()
	err := e.openTestDatabaseWithID(t.Name())
	if err != nil {
		t.Fatal(err)
	}
	e.initRuntime()
	t.Cleanup(func() {
		e.drainRuntime()
		e.closeDatabase()
	})
}

// initRuntime starts all goroutines and initializes runtime state.
func (e *Engine) initRuntime() {
	e.lifetimeCtx, e.lifetimeCancel = context.WithCancel(context.Background())
	e.cache.init(int(e.workers.Load()))
	e.refillTrigger = make(chan struct{}, 1)
	e.refillStop = make(chan struct{})
	e.wakeTimer = make(chan struct{}, 1)
	e.timerStop = make(chan struct{})
	e.recoveryStop = make(chan struct{})
	e.nextPoll = time.Now()
	e.valves = map[string]*taskValve{}
	e.breakers = map[string]*taskBreaker{}
	e.graphCache = newLRUCache[graphCacheKey, *workflow.Graph](graphCacheMaxEntries, graphCacheTTL)
	e.waiters = nil
	e.started.Store(true)

	// Create the dwarf_* instruments and register the observable-gauge callback before workers start
	// emitting. Falls back to the global (no-op) provider when none was injected.
	if err := e.initMetrics(); err != nil {
		e.logger.ErrorContext(e.lifetimeCtx, "Initializing metrics", "error", err)
	}

	// Resolve the tracer (no-op unless a TracerProvider was injected or the global SDK is configured).
	e.initTracer()

	// Re-arm the breaker map from surviving parked rows before workers start dispatching, so a
	// restarting replica does not strand a breaker-parked backlog or dispatch it into a known-bad
	// endpoint. No-op on a fresh database.
	if err := e.reconstituteBreakers(e.lifetimeCtx); err != nil {
		e.logger.ErrorContext(e.lifetimeCtx, "Reconstituting breakers", "error", err)
	}

	numWorkers := int(e.workers.Load())
	for range numWorkers {
		e.workerPool.Add(1)
		go func() {
			defer e.workerPool.Done()
			e.workerLoop(e.lifetimeCtx)
		}()
	}
	e.timerWorker.Add(1)
	go func() {
		defer e.timerWorker.Done()
		e.timerLoop(e.lifetimeCtx)
	}()
	e.refiller.Add(1)
	go func() {
		defer e.refiller.Done()
		e.refillerLoop(e.lifetimeCtx)
	}()
	e.recoveryWorker.Add(1)
	go func() {
		defer e.recoveryWorker.Done()
		e.recoveryLoop(e.lifetimeCtx)
	}()
	e.requestRefill()
}

// drainRuntime stops all goroutines in order.
func (e *Engine) drainRuntime() {
	e.started.Store(false)
	// Unregister the observable-gauge callback first so the OTEL reader cannot invoke it (and query the
	// shards) while/after the databases are being closed.
	e.closeMetrics()
	e.cache.close()
	e.workerPool.Wait()
	if e.timerStop != nil {
		close(e.timerStop)
	}
	e.timerWorker.Wait()
	// The recovery loop can requestRefill (re-electing a wedged breaker probe), so drain it before the
	// refiller, mirroring the timer.
	if e.recoveryStop != nil {
		close(e.recoveryStop)
	}
	e.recoveryWorker.Wait()
	if e.refillStop != nil {
		close(e.refillStop)
	}
	e.refiller.Wait()
	e.waitersLock.Lock()
	for _, chans := range e.waiters {
		for _, ch := range chans {
			select {
			case ch <- "":
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
	if tm.Before(e.nextPoll) {
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

// taskTimeBudget returns the current time budget for task dispatch.
func (e *Engine) taskTimeBudget() time.Duration {
	return time.Duration(e.timeBudgetMs.Load()) * time.Millisecond
}

// resolveFlowOptions applies defaults to caller-supplied options.
func (e *Engine) resolveFlowOptions(opts *workflow.FlowOptions) *workflow.FlowOptions {
	resolved := &workflow.FlowOptions{
		Priority:       int(e.defaultPriority.Load()),
		FairnessWeight: 1,
	}
	if opts != nil {
		if opts.Priority > 0 {
			resolved.Priority = opts.Priority
		}
		if opts.FairnessWeight > 0 {
			resolved.FairnessWeight = opts.FairnessWeight
		}
		resolved.FairnessKey = opts.FairnessKey
		resolved.StartAt = opts.StartAt
		resolved.Baggage = opts.Baggage
		resolved.NotifyOnStop = opts.NotifyOnStop
	}
	return resolved
}

// initialParkedFor returns the parked value a new pending step should be inserted with.
func (e *Engine) initialParkedFor(taskURL string) int {
	if e.BreakerTripped(taskURL) {
		return parkedBreaker
	}
	return parkedNone
}

// BreakerTripped reports whether the breaker for the given task URL (the downstream endpoint, the key the
// breaker is partitioned on) is currently tripped.
func (e *Engine) BreakerTripped(taskURL string) bool {
	e.breakersLock.RLock()
	defer e.breakersLock.RUnlock()
	b, ok := e.breakers[taskURL]
	if !ok {
		return false
	}
	return !b.trippedAt.IsZero()
}

// ValveCount returns the number of tasks that currently have an adaptive-rate valve allocated. Intended
// for tests/fixtures that assert backpressure engaged; not a hot-path metric (use the dwarf_task_rate_limit
// gauge for monitoring).
func (e *Engine) ValveCount() int {
	e.valvesLock.RLock()
	defer e.valvesLock.RUnlock()
	return len(e.valves)
}

// refreshNextProbeLocked recomputes the soonest probe across all tripped breakers.
// Caller must hold breakersLock.
func (e *Engine) refreshNextProbeLocked() {
	var soonest int64
	for _, b := range e.breakers {
		if b.trippedAt.IsZero() {
			continue
		}
		n := b.nextProbeAt.UnixNano()
		if soonest == 0 || n < soonest {
			soonest = n
		}
	}
	e.nextProbe.Store(soonest)
	e.nudgeTimer()
}

// valvePeek reports whether the task is currently admissible without consuming a slot.
func (e *Engine) valvePeek(taskName string, now time.Time) bool {
	e.valvesLock.RLock()
	v, ok := e.valves[taskName]
	var wCong int
	var tCong time.Time
	if ok {
		wCong = v.wCong
		tCong = v.tCong
	}
	e.valvesLock.RUnlock()
	if !ok || wCong == 0 {
		return true
	}
	v.throttle.SetLimit(recoverRate(wCong, tCong, now))
	admit, _ := v.throttle.Peek()
	return admit
}

// valveCommit consumes a throttle slot, creating the valve lazily on first dispatch.
func (e *Engine) valveCommit(taskName string, now time.Time) {
	e.valvesLock.Lock()
	v, ok := e.valves[taskName]
	if !ok {
		v = &taskValve{throttle: throttle.New(time.Second, math.MaxInt32)}
		e.valves[taskName] = v
	}
	wCong := v.wCong
	tCong := v.tCong
	e.valvesLock.Unlock()
	if wCong > 0 {
		v.throttle.SetLimit(recoverRate(wCong, tCong, now))
	}
	v.throttle.Allow()
}

// --- Public API ---

// Create creates a new flow for a workflow without starting it. opts carries the flow's scheduling
// (priority/fairness/start-at) and its opaque host Baggage; nil opts uses defaults.
func (e *Engine) Create(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	return e.create(ctx, workflowURL, initialState, opts)
}

// CreateTask creates a flow for a single task (addressed by its dispatch URL) without starting it. opts
// carries scheduling and the opaque host Baggage (see Create); nil opts uses defaults.
func (e *Engine) CreateTask(ctx context.Context, taskURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	return e.createTask(ctx, taskURL, initialState, opts)
}

// Start transitions a created flow to running. Whether the flow notifies the host on stop is set at
// Create via FlowOptions.NotifyOnStop.
func (e *Engine) Start(ctx context.Context, flowKey string) error {
	return e.start(ctx, flowKey)
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
	return e.resume(ctx, flowKey, false, resumeData)
}

// ResumeBreak continues a flow paused at a BreakBefore breakpoint.
func (e *Engine) ResumeBreak(ctx context.Context, flowKey string, stateOverrides any) error {
	return e.resume(ctx, flowKey, true, stateOverrides)
}

// Cancel aborts a flow.
func (e *Engine) Cancel(ctx context.Context, flowKey string, reason string) error {
	return e.cancel(ctx, flowKey, reason)
}

// Restart re-executes a flow from the beginning.
func (e *Engine) Restart(ctx context.Context, flowKey string, stateOverrides any) error {
	return e.restart(ctx, flowKey, stateOverrides)
}

// RestartFrom re-executes a flow from a specific step.
func (e *Engine) RestartFrom(ctx context.Context, stepKey string, stateOverrides any) error {
	return e.restartFrom(ctx, stepKey, stateOverrides)
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

// Purge deletes flows matching a query.
func (e *Engine) Purge(ctx context.Context, query workflow.Query) (int, error) {
	return e.purge(ctx, query)
}

// ShardInfo returns health and size summaries for all shards.
func (e *Engine) ShardInfo(ctx context.Context) ([]ShardSummary, error) {
	return e.shardInfo(ctx)
}

// Await blocks until a flow stops.
func (e *Engine) Await(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	return e.await(ctx, flowKey)
}

// Run creates, starts, and awaits a flow in one call. opts carries scheduling and the opaque host
// Baggage; nil opts uses defaults.
func (e *Engine) Run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (*workflow.FlowOutcome, error) {
	return e.run(ctx, workflowURL, initialState, opts)
}

// Continue creates a new flow from the latest completed flow in a thread.
func (e *Engine) Continue(ctx context.Context, threadKey string, additionalState any, opts *workflow.FlowOptions) (string, error) {
	return e.continueFlow(ctx, threadKey, additionalState, opts)
}

// BreakBefore sets or clears a breakpoint before a named task.
func (e *Engine) BreakBefore(ctx context.Context, flowKey string, taskName string, enabled bool) error {
	return e.setBreakpoint(ctx, flowKey, taskName, enabled)
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

// breakerTrip records a trip in the local in-memory breaker map, keyed by the task's dispatch URL.
func (e *Engine) breakerTrip(taskURL, cause string) (fresh bool, nextProbeAt time.Time) {
	now := time.Now()
	e.breakersLock.Lock()
	defer e.breakersLock.Unlock()
	b, ok := e.breakers[taskURL]
	if !ok {
		b = &taskBreaker{}
		e.breakers[taskURL] = b
	}
	if b.trippedAt.IsZero() {
		b.trippedAt = now
		b.probeAttempt = 0
		b.nextProbeAt = now.Add(breakerProbeBackoff(1))
		b.cause = cause
		fresh = true
		e.refreshNextProbeLocked()
	}
	return fresh, b.nextProbeAt
}
