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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"log/slog"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/throttle"
)

// GraphLoader fetches a workflow graph definition by name. The metadata is the opaque
// map stored on the flow at Create time.
type GraphLoader func(ctx context.Context, workflowName string, metadata map[string]any) (*workflow.Graph, error)

// TaskExecutor executes a single task within a workflow. The flow carrier has its state
// pre-populated; the executor should call the task and let it write changes to the flow.
// The metadata is the opaque map stored on the flow at Create time.
type TaskExecutor func(ctx context.Context, taskName string, flow *workflow.Flow, metadata map[string]any) error

// FlowStoppedCallback is fired when a flow stops (completed, failed, cancelled, interrupted).
// The hostname is the notify_hostname stored on the flow via StartNotify.
type FlowStoppedCallback func(ctx context.Context, hostname string, outcome *workflow.FlowOutcome)

// PeerNotifier sends cross-replica coordination signals. All methods are fire-and-forget.
//
// Every signal must be delivered to OTHER replicas only, EXCLUDING the calling replica. The engine
// always applies a signal's effect locally before invoking the corresponding PeerNotifier method
// (e.g. startNotify rings the local doorbell via handleEnqueue and then calls Enqueue; valveRegulate
// mutates the local valve and then calls SyncValve). An implementation that echoes the signal back to
// the sender would cause it to be processed twice on the originating replica — a doubled enqueue,
// valve cut, breaker trip, or status-change wake. If the underlying transport delivers published
// messages to the publisher, the implementation is responsible for filtering out self-delivery.
type PeerNotifier interface {
	Enqueue(ctx context.Context, shard, stepID int)
	SyncValve(ctx context.Context, taskName string, wCong int, tCong time.Time)
	TripBreaker(ctx context.Context, taskName string)
	// NotifyStatusChange tells peer replicas a flow reached a stopped status (completed, failed,
	// cancelled, interrupted) so their Await callers wake and re-check. Without it, an Await on the
	// replica that did not run the flow's final step blocks until its context deadline. The receiving
	// replica routes the signal to HandleNotifyStatusChange.
	NotifyStatusChange(ctx context.Context, flowKey string, status string)
}

// Logger receives structured log messages from the engine. The foreman injects one
// whose methods delegate to svc.LogDebug/LogInfo/LogError/LogWarn. Standalone users
// pass any implementation they like, or leave the default DiscardLogger.
type Logger interface {
	LogDebug(ctx context.Context, msg string, args ...any)
	LogInfo(ctx context.Context, msg string, args ...any)
	LogWarn(ctx context.Context, msg string, args ...any)
	LogError(ctx context.Context, msg string, args ...any)
}

// StandardLogger delegates to the standard library's slog package.
type StandardLogger struct{}

func (StandardLogger) LogDebug(ctx context.Context, msg string, args ...any) {
	slog.DebugContext(ctx, msg, args...)
}
func (StandardLogger) LogInfo(ctx context.Context, msg string, args ...any) {
	slog.InfoContext(ctx, msg, args...)
}
func (StandardLogger) LogWarn(ctx context.Context, msg string, args ...any) {
	slog.WarnContext(ctx, msg, args...)
}
func (StandardLogger) LogError(ctx context.Context, msg string, args ...any) {
	slog.ErrorContext(ctx, msg, args...)
}

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
	graphLoader         GraphLoader
	taskExecutor        TaskExecutor
	flowStoppedCallback FlowStoppedCallback
	peerNotifier        PeerNotifier
	logger              Logger

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

	// Candidate cache and worker pool
	cache      candidateCache
	workerPool sync.WaitGroup

	// Single-slot refiller
	refillTrigger      chan struct{}
	refillStop         chan struct{}
	refiller           sync.WaitGroup
	lastRefillPriority int

	// Timer goroutine
	nextPoll     time.Time
	nextPollLock sync.Mutex
	nextProbe    atomic.Int64
	wakeTimer    chan struct{}
	timerStop    chan struct{}
	timerWorker  sync.WaitGroup

	// Wait registry for Await
	waitersLock sync.Mutex
	waiters     map[string][]chan string

	// Adaptive per-task dispatch rate
	valves     map[string]*taskValve
	valvesLock sync.RWMutex

	// Per-task breaker
	breakers     map[string]*taskBreaker
	breakersLock sync.RWMutex

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
		logger: StandardLogger{},
	}
	e.dsn.Store("")
	e.numShards.Store(1)
	e.workers.Store(64)
	e.timeBudgetMs.Store(int64(2 * time.Minute / time.Millisecond))
	e.defaultPriority.Store(100)
	e.maxOpenConns.Store(8)
	return e
}

// --- Builder methods (callable before AND after Startup) ---

// WithDSN sets the SQL data source name. Use "%d" for sharded DSNs.
// An empty DSN in test mode uses SQLite in-memory.
func (e *Engine) WithDSN(dsn string) *Engine {
	e.dsn.Store(dsn)
	return e
}

// WithNumShards sets the number of database shards.
func (e *Engine) WithNumShards(n int) *Engine {
	e.numShards.Store(int32(n))
	return e
}

// WithWorkers sets the number of worker goroutines.
func (e *Engine) WithWorkers(n int) *Engine {
	e.workers.Store(int32(n))
	return e
}

// WithTimeBudget sets the maximum duration for a single task execution.
func (e *Engine) WithTimeBudget(d time.Duration) *Engine {
	e.timeBudgetMs.Store(int64(d / time.Millisecond))
	return e
}

// WithDefaultPriority sets the default priority for new flows.
func (e *Engine) WithDefaultPriority(p int) *Engine {
	e.defaultPriority.Store(int32(p))
	return e
}

// WithMaxOpenConns sets the maximum number of open SQL connections per shard.
func (e *Engine) WithMaxOpenConns(n int) *Engine {
	e.maxOpenConns.Store(int32(n))
	return e
}

// --- Dependency injection (must be set before Startup) ---

// WithGraphLoader sets the function that fetches workflow graph definitions.
func (e *Engine) WithGraphLoader(gl GraphLoader) *Engine {
	e.graphLoader = gl
	return e
}

// WithTaskExecutor sets the function that executes workflow tasks.
func (e *Engine) WithTaskExecutor(te TaskExecutor) *Engine {
	e.taskExecutor = te
	return e
}

// WithFlowStoppedCallback sets the callback fired when a flow stops.
func (e *Engine) WithFlowStoppedCallback(cb FlowStoppedCallback) *Engine {
	e.flowStoppedCallback = cb
	return e
}

// WithPeerNotifier sets the cross-replica coordination interface.
func (e *Engine) WithPeerNotifier(pn PeerNotifier) *Engine {
	e.peerNotifier = pn
	return e
}

// WithLogger sets the logging callback. The level argument is "debug", "info", "warn", or "error".
// Args follow the slog name=value pair pattern.
func (e *Engine) WithLogger(l Logger) *Engine {
	e.logger = l
	return e
}

// --- Lifecycle ---

// Startup initializes the engine: opens database connections, runs migrations,
// and starts worker goroutines.
func (e *Engine) Startup(ctx context.Context) error {
	if e.graphLoader == nil {
		return errors.New("graph loader is required")
	}
	if e.taskExecutor == nil {
		return errors.New("task executor is required")
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

// RunInTest initializes the engine for testing with per-test SQLite databases
// and registers cleanup via t.Cleanup.
func (e *Engine) RunInTest(t *testing.T) {
	t.Helper()
	err := e.openTestDatabase(t)
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
	e.nextPoll = time.Now()
	e.valves = map[string]*taskValve{}
	e.breakers = map[string]*taskBreaker{}
	e.graphCache = newLRUCache[graphCacheKey, *workflow.Graph](graphCacheMaxEntries, graphCacheTTL)
	e.waiters = nil
	e.started.Store(true)

	// Re-arm the breaker map from surviving parked rows before workers start dispatching, so a
	// restarting replica does not strand a breaker-parked backlog or dispatch it into a known-bad
	// endpoint. No-op on a fresh database.
	if err := e.reconstituteBreakers(e.lifetimeCtx); err != nil {
		e.logger.LogError(e.lifetimeCtx, "Reconstituting breakers", "error", err)
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
	e.requestRefill()
}

// drainRuntime stops all goroutines in order.
func (e *Engine) drainRuntime() {
	e.started.Store(false)
	e.cache.close()
	e.workerPool.Wait()
	if e.timerStop != nil {
		close(e.timerStop)
	}
	e.timerWorker.Wait()
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
	e.signalTimer()
}

// signalTimer nudges the timer goroutine to re-evaluate its deadline.
func (e *Engine) signalTimer() {
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
func (e *Engine) resolveFlowOptions(opts *workflow.FlowOptions, metadata map[string]any) *workflow.FlowOptions {
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
	}
	return resolved
}

// initialParkedFor returns the parked value a new pending step should be inserted with.
func (e *Engine) initialParkedFor(taskName string) int {
	if e.BreakerTripped(taskName) {
		return parkedBreaker
	}
	return parkedNone
}

// BreakerTripped reports whether the breaker for taskName is currently tripped.
func (e *Engine) BreakerTripped(taskName string) bool {
	e.breakersLock.RLock()
	defer e.breakersLock.RUnlock()
	b, ok := e.breakers[taskName]
	if !ok {
		return false
	}
	return !b.trippedAt.IsZero()
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
	e.signalTimer()
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

// Create creates a new flow for a workflow without starting it.
func (e *Engine) Create(ctx context.Context, workflowName string, initialState any, metadata map[string]any, opts *workflow.FlowOptions) (flowKey string, err error) {
	return e.create(ctx, workflowName, initialState, metadata, opts)
}

// CreateTask creates a flow for a single task without starting it.
func (e *Engine) CreateTask(ctx context.Context, taskName string, initialState any, metadata map[string]any) (flowKey string, err error) {
	return e.createTask(ctx, taskName, initialState, metadata)
}

// Start transitions a created flow to running.
func (e *Engine) Start(ctx context.Context, flowKey string) error {
	return e.startNotify(ctx, flowKey, "")
}

// StartNotify starts a flow and registers a hostname for stop notifications.
func (e *Engine) StartNotify(ctx context.Context, flowKey string, notifyHostname string) error {
	return e.startNotify(ctx, flowKey, notifyHostname)
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

// Run creates, starts, and awaits a flow in one call.
func (e *Engine) Run(ctx context.Context, workflowName string, initialState any, metadata map[string]any, opts *workflow.FlowOptions) (*workflow.FlowOutcome, error) {
	return e.run(ctx, workflowName, initialState, metadata, opts)
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

// --- Handle methods (for foreman adapter to wire bus endpoints) ---

// HandleEnqueue processes an inbound doorbell signal from another replica.
func (e *Engine) HandleEnqueue(ctx context.Context, shard, stepID int) error {
	e.handleEnqueue(ctx, shard, stepID)
	return nil
}

// HandleSyncValve processes an inbound valve gossip signal from another replica.
func (e *Engine) HandleSyncValve(ctx context.Context, taskName string, wCong int, tCong time.Time) error {
	if taskName == "" {
		return nil
	}
	e.valvesLock.Lock()
	defer e.valvesLock.Unlock()
	cur, ok := e.valves[taskName]
	if !ok {
		e.valves[taskName] = &taskValve{
			wCong:    wCong,
			tCong:    tCong,
			throttle: throttle.New(time.Second, math.MaxInt32),
		}
		return nil
	}
	if tCong.After(cur.tCong) || (tCong.Equal(cur.tCong) && wCong < cur.wCong) {
		cur.wCong = wCong
		cur.tCong = tCong
	}
	return nil
}

// HandleTripBreaker processes an inbound breaker trip signal from another replica.
func (e *Engine) HandleTripBreaker(ctx context.Context, taskName string) error {
	if taskName == "" {
		return nil
	}
	// Stamp the local clock and drive bulk-park exactly as a local trip does so this replica's view
	// of the task's pending steps converges with the originating replica's. Closures are not gossiped;
	// each peer closes locally when its own probe succeeds.
	fresh, nextProbeAt := e.breakerTrip(taskName, breakerCauseAckTimeout)
	if !fresh {
		return nil // already tripped here too; no fresh bulk-park needed
	}
	err := e.breakerBulkPark(ctx, taskName, nextProbeAt, 0, 0)
	if err != nil {
		e.logger.LogError(ctx, "Bulk-park on gossip trip", "task", taskName, "error", err)
	}
	return nil
}

// HandleNotifyStatusChange processes an inbound status change notification.
func (e *Engine) HandleNotifyStatusChange(ctx context.Context, flowKey string, status string) error {
	e.notifyStatusChange(flowKey, status)
	return nil
}

// breakerTrip records a trip in the local in-memory breaker map.
func (e *Engine) breakerTrip(taskName, cause string) (fresh bool, nextProbeAt time.Time) {
	now := time.Now()
	e.breakersLock.Lock()
	defer e.breakersLock.Unlock()
	b, ok := e.breakers[taskName]
	if !ok {
		b = &taskBreaker{}
		e.breakers[taskName] = b
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
