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
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"log/slog"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
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

	// pollErrorRetryInterval caps the wake delay after a sizing query in pollPendingSteps fails, so a
	// transient DB error (e.g. a momentary connection-limit rejection) triggers a prompt re-poll instead
	// of the timer sleeping for maxPollInterval while a due step sits undispatched.
	pollErrorRetryInterval = 1 * time.Second

	// wedgeSweepInterval is the cadence of the dedicated recovery goroutine that runs the defense-in-depth
	// parked-step wedge sweep. It is kept off the frequently-nudged poll path because the sweep's scans are
	// heavy and latency-tolerant. parkWedgeThreshold is the minimum age a parked step must reach before the
	// sweep treats it as wedged - comfortably beyond normal subgraph-completion latency, so steady-state
	// operation never trips a false positive.
	wedgeSweepInterval = 5 * time.Minute
	parkWedgeThreshold = 5 * time.Minute

	parkedNone     = 0
	parkedSubgraph = 1
)

// Engine is the standalone workflow orchestration engine.
type Engine struct {
	// Dependencies (set before Startup)
	host           Host
	logger         *slog.Logger
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
	maxOpenConns    atomic.Int32 // per-shard open ceiling (= per-server budget); <=0 means unbounded
	workersPerConn  atomic.Int32 // workers assumed to share one connection; the pool-sizing divisor

	// Database
	dbs     []*sequel.DB
	dbsLock sync.RWMutex
	// expandLock serializes ExpandShards so two concurrent calls cannot both observe the same shard
	// count and each open the same new shard. The slow open+migrate I/O runs under this lock but
	// outside dbsLock, so it never blocks the hot path (which only takes dbsLock).
	expandLock sync.Mutex
	// testHashedID is the hashed test database id when the engine was started in test mode (RunInTest),
	// empty in production. openDatabaseShard reads it (at startup and on later expandShards growth) to wrap
	// each shard in an isolated test database. Written once during single-threaded startup before
	// started.Store(true), read only after started.Load()==true, so the atomic started flag is the
	// happens-before barrier.
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
		logger:         slog.New(slog.DiscardHandler),
		lastRefillBand: -1,
	}
	e.dsn.Store("")
	e.numShards.Store(1)
	e.workers.Store(64)
	e.timeBudgetMs.Store(int64(2 * time.Minute / time.Millisecond))
	e.defaultPriority.Store(100)
	e.maxOpenConns.Store(8)
	e.workersPerConn.Store(8)
	return e
}

// --- Configuration setters ---
//
// Setters split into Live (callable any time) and construction-time-only (rejected after Startup).

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
	return e.expandShards(context.Background())
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

// SetTimeBudget sets the default duration for a single task execution, used by any flow that does not
// override it via FlowOptions.TimeBudget. Live: read fresh on each Create (existing flows keep the budget
// frozen at their own Create).
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

// SetMaxOpenConns sets the per-shard hard ceiling on open SQL connections.
func (e *Engine) SetMaxOpenConns(n int) error {
	if n < 1 {
		return errors.New("max open connections must be >= 1", http.StatusBadRequest)
	}
	e.maxOpenConns.Store(int32(n))
	e.dbsLock.RLock()
	for _, db := range e.dbs {
		e.applyConnPoolSizes(db)
	}
	e.dbsLock.RUnlock()
	return nil
}

// SetWorkersPerConn sets the assumed number of worker goroutines that share one database connection.
// The pool size is derived FROM the number of workers, not the other way around.
func (e *Engine) SetWorkersPerConn(n int) error {
	if n < 1 {
		return errors.New("workers per connection must be >= 1", http.StatusBadRequest)
	}
	e.workersPerConn.Store(int32(n))
	e.dbsLock.RLock()
	for _, db := range e.dbs {
		e.applyConnPoolSizes(db)
	}
	e.dbsLock.RUnlock()
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

// Shutdown stops all worker goroutines and closes database connections.
func (e *Engine) Shutdown(ctx context.Context) error {
	e.drainRuntime()
	e.closeDatabase()
	return nil
}

// SetInTest puts the engine into test mode keyed by name, so a subsequent Startup opens per-name isolated,
// auto-dropped databases (via sequel.CreateTestingDatabase) instead of the configured ones. Construction-
// time only. It is the *testing.T-free counterpart to RunInTest, for a host running under an external test
// harness that has no *testing.T but a stable isolation key such as its plane: every engine sharing the
// same name resolves to the same isolated databases, so the replicas of a multi-replica test app converge
// on one set. RunInTest(t) is SetInTest(t.Name()) plus Startup and a t.Cleanup shutdown.
func (e *Engine) SetInTest(name string) error {
	if e.started.Load() {
		return errSetAfterStartup("test mode")
	}
	// Hash the name to a short, bounded id so the testing-database name sequel derives stays within the
	// strictest SQL identifier limit (Postgres 63 / MySQL 64), whatever the name's length. A non-empty
	// testHashedID is also what switches openDatabaseShard onto the isolated-test open path. It is set before
	// initRuntime flips started, so the atomic started flag publishes it to expandShards's later reads.
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
	e.cache.init(int(e.workers.Load()))
	e.refillTrigger = make(chan struct{}, 1)
	e.refillStop = make(chan struct{})
	e.wakeTimer = make(chan struct{}, 1)
	e.timerStop = make(chan struct{})
	e.recoveryStop = make(chan struct{})
	e.nextPoll = time.Now()
	e.graphCache = newLRUCache[graphCacheKey, *workflow.Graph](4096, 15*time.Minute)
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
		resolved.NotifyOnStop = opts.NotifyOnStop
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
// the flow's policy (scheduling, NotifyOnStop, DeleteOnCompletion, Baggage, ThreadKey); nil uses defaults.
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

// Purge deletes flows matching a query, their subflows, and their step history.
// No more than 1000 flows are deleted at a time. Iterate to delete more.
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

// Run creates, starts, and awaits a flow in one call, returning the new flow's key alongside its outcome
// (the key is the flow's identity, not part of the outcome). opts carries scheduling and the opaque host
// Baggage; nil opts uses defaults. On error, flowKey is "" and outcome is nil.
func (e *Engine) Run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, outcome *workflow.FlowOutcome, err error) {
	return e.run(ctx, workflowURL, initialState, opts)
}

// Continue creates a new flow from the latest completed flow in a thread, inheriting that flow's policy
// (scheduling, baggage, notify-on-stop) - it does not take FlowOptions. For a turn with different policy,
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
