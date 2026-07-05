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
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/candidatecache"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// create inserts a new flow for a workflow and runs it.
func (e *Engine) create(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	if workflowURL == "" {
		return "", errors.New("workflow URL is required", http.StatusBadRequest)
	}
	// Reject a negative priority/weight rather than silently coercing it to the default (resolveFlowOptions
	// treats <=0 as "unset"): a negative value is a caller bug, and swallowing it hides it. 0 stays "use the
	// engine default" for both, so this only rejects genuinely-invalid input. Genesis-only (Create/Run);
	// derived ops (Continue/Fork/subgraph) inherit already-validated values and take no FlowOptions.
	if opts != nil {
		if opts.Priority < 0 {
			return "", errors.New("priority must be >= 0", http.StatusBadRequest)
		}
		if opts.FairnessWeight < 0 {
			return "", errors.New("fairness weight must be >= 0", http.StatusBadRequest)
		}
	}
	opts = e.resolveFlowOptions(opts)
	// The create-time GraphLoader sees the baggage on ctx in the same decoded shape every dispatch will.
	loaderCtx := workflow.ContextWithBaggage(ctx, baggageMap(opts.Baggage))
	var graph *workflow.Graph
	err = errors.CatchPanic(func() error {
		var lerr error
		graph, lerr = e.host.LoadGraph(loaderCtx, workflowURL)
		return lerr
	})
	if err == nil && e.isFault(faultLoadGraph, workflowURL) {
		err = errors.New("injected fault: "+faultKey(faultLoadGraph, workflowURL), http.StatusInternalServerError)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	// A host that returns (nil, nil) is a not-found, not a nil-deref: guard before EntryPoint/Validate.
	if graph == nil {
		return "", errors.New("workflow graph not found: %s", workflowURL, http.StatusNotFound)
	}
	// Validate at create (documented behavior). Besides rejecting a structurally invalid graph up front,
	// Validate's side effect populates fanOutToFanIn - which the empty-forEach fan-in path (FanInFor) reads
	// - and that map is frozen into the graph JSON below, so every dispatch of this flow sees it. Cheap:
	// once per create, never per step.
	if verr := graph.Validate(); verr != nil {
		return "", errors.New("invalid workflow graph %s: %v", workflowURL, verr, http.StatusBadRequest)
	}

	// FlowOptions.ThreadKey joins an existing thread: the shard is encoded in the key, and the new flow
	// adopts that flow's thread_id/thread_token (a mid-thread flow's thread_id is not its own flow_id).
	// Empty starts a fresh thread on a random shard.
	shardNum, threadID, threadToken := 0, 0, ""
	if opts.ThreadKey != "" {
		shardNum, threadID, threadToken, err = e.resolveThread(ctx, opts.ThreadKey)
		if err != nil {
			return "", errors.Trace(err)
		}
	} else {
		shardNum = rand.IntN(e.db.NumShards()) + 1
	}
	flowKey, err = e.createWithGraph(ctx, shardNum, workflowURL, graph, initialState, threadID, threadToken, "", opts, 0, 0, 0, 0)
	return flowKey, errors.Trace(err)
}

// resolveThread parses a FlowKey identifying a thread and returns the thread's shard, id, and token. The
// shard is encoded in the key; the thread_id/thread_token are read from the referenced flow (which may be
// mid-thread, so its thread_id differs from its own flow_id). Verifies the flow exists (token-checked).
func (e *Engine) resolveThread(ctx context.Context, threadKey string) (shardNum, threadID int, threadToken string, err error) {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(threadKey)
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	err = db.QueryRowContext(ctx,
		"SELECT thread_id, thread_token FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&threadID, &threadToken)
	if err == sql.ErrNoRows {
		return 0, 0, "", errors.New("thread not found", http.StatusNotFound)
	}
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	return shardNum, threadID, strings.TrimSpace(threadToken), nil
}

// baggageMap normalizes an opaque baggage value to the map delivered on the context. It round-trips
// through JSON - the same path the value takes through the baggage column - so the create-time
// GraphLoader sees exactly what every dispatch-time callback sees (e.g. JSON numbers as float64),
// rather than the caller's original Go types. A nil value, or a value that does not decode to a JSON
// object, yields nil.
func baggageMap(v any) map[string]any {
	if v == nil {
		return nil
	}
	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var m map[string]any
	_ = json.Unmarshal(data, &m)
	return m
}

// flowSeed carries the precomputed, retry-stable values for inserting one new flow plus its entry step.
// It is built outside the transaction (tokens, spans, marshalled JSON) so a lock-contention retry of the
// insert closure reuses the same values rather than rerolling tokens.
type flowSeed struct {
	workflowURL        string
	workflowName       string
	graphJSON          string
	baggageJSON        string
	stateJSON          string
	traceParent        string
	flowToken          string
	stepToken          string
	entryPoint         string
	entryURL           string
	timeBudgetMs       int64
	deleteOnCompletion int
	surgraphFlowID     int
	surgraphStepID     int
	callerStepDepth    int
	threadID           int
	threadToken        string
	rootFlowID         int
	priority           int
	fairnessKey        string
	fairnessWeight     float64
}

// insertFlowTx inserts the flow row (already `running`) and its entry step (`pending`, immediately
// claimable), then fixes thread_id/step_id/root_flow_id - the shared write-half of every creation path,
// run inside the caller's transaction. Returns the new flow and step ids. createWithGraph wraps it in a
// bare transaction; continueFlow wraps it in a transaction that first locks the thread and re-checks the
// latest turn, so both share exactly one copy of the insert SQL.
func insertFlowTx(ctx context.Context, tx *sequel.Tx, s flowSeed) (newFlowID, newStepID int64, err error) {
	newFlowID, err = tx.InsertReturnID(ctx, "flow_id",
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, baggage, trace_parent, status, surgraph_flow_id, surgraph_step_id, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms, started_at)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC())",
		s.flowToken, s.workflowURL, s.workflowName, s.graphJSON, s.baggageJSON, s.traceParent, workflow.StatusRunning, s.surgraphFlowID, s.surgraphStepID, s.deleteOnCompletion, s.priority, s.fairnessKey, s.fairnessWeight, s.timeBudgetMs,
	)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	// Entry step is pending and immediately claimable (not_before=NOW, lease_expires=NOW). Its depth
	// continues from the caller: callerStepDepth+1 (1 for a top-level flow, where callerStepDepth is 0).
	// A flow that should wait before running uses an entry gate task with flow.Sleep, not a creation-time
	// delay.
	newStepID, err = tx.InsertReturnID(ctx, "step_id",
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, time_budget_ms, not_before, lease_expires, priority, fairness_key, fairness_weight)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC(), NOW_UTC(), ?, ?, ?)",
		newFlowID, s.callerStepDepth+1, s.stepToken, s.entryPoint, s.entryURL, s.stateJSON, workflow.StatusPending, s.timeBudgetMs, s.priority, s.fairnessKey, s.fairnessWeight,
	)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	// Derive the thread from the new flow inside the closure so a retry (with a new flow_id) recomputes
	// it rather than reusing the prior attempt's id.
	tid, ttok := s.threadID, s.threadToken
	if tid == 0 {
		tid = int(newFlowID)
		ttok = s.flowToken
	}
	// root_flow_id is the denormalized tree-membership index: a top-level flow (rootFlowID==0) is its
	// own root; a subgraph child inherits the parent's root. Written once here, immutable thereafter, so
	// a whole tree is reachable by `WHERE root_flow_id=?` without a recursive surgraph walk.
	rfid := s.rootFlowID
	if rfid == 0 {
		rfid = int(newFlowID)
	}
	_, err = tx.ExecContext(ctx,
		"UPDATE dwarf_flows SET thread_id=?, thread_token=?, step_id=?, root_flow_id=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=?",
		tid, ttok, newStepID, rfid, newFlowID,
	)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}
	return newFlowID, newStepID, nil
}

// createWithGraph is the shared creation path for Create, Continue, and subgraph children. It inserts the
// flow (already `running`) and its entry step (`pending`, immediately claimable) in one transaction, then
// rings the doorbell - so a flow always creates-and-runs; there is no externally-visible `created` resting
// state. opts.Baggage is the opaque host value marshalled to the baggage column. parentTraceParent controls
// the flow's "workflow" span: empty mints a detached root span (top-level Create/Continue, each its own
// trace); non-empty parents it under that context (a subgraph nests under the caller step's span).
//
// surgraphFlowID/surgraphStepID link a subgraph child to its parent caller step; they are 0 for a
// top-level flow. The linkage is written in the SAME insert transaction, so the child is fully
// parent-linked before it can be dispatched and complete - otherwise its completion could not revive the
// parent. (The parent caller is parked by processStep before this is called, the complementary half of
// that ordering.) callerStepDepth is the caller step's step_depth (0 for a top-level flow): the entry step
// is created at callerStepDepth+1, so a subgraph's depths continue from the caller (informational only).
func (e *Engine) createWithGraph(ctx context.Context, shardNum int, workflowURL string, graph *workflow.Graph, initialState any, threadID int, threadToken string, parentTraceParent string, opts *workflow.FlowOptions, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID int) (flowKey string, err error) {
	entryPoint := graph.EntryPoint()
	if entryPoint == "" {
		return "", errors.New("workflow has no entry point", http.StatusBadRequest)
	}

	traceParent := e.mintWorkflowSpan(ctx, workflowURL, parentTraceParent)

	baggageJSON, err := json.Marshal(opts.Baggage)
	if err != nil {
		return "", errors.Trace(err)
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		return "", errors.Trace(err)
	}
	stateJSON, err := json.Marshal(initialState)
	if err != nil {
		return "", errors.Trace(err)
	}

	flowToken := keys.RandomIdentifier(16)
	stepToken := keys.RandomIdentifier(16)
	// opts.TimeBudget is resolved (create/continue) or inherited (subgraph). Fall back to the
	// live default only as defense for an unresolved path.
	timeBudget := opts.TimeBudget
	if timeBudget <= 0 {
		timeBudget = e.taskTimeBudget()
	}
	deleteOnCompletion := 0
	if opts.DeleteOnCompletion {
		deleteOnCompletion = 1
	}

	db, err := e.db.Shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}
	entryURL := dispatchURLOf(graph, entryPoint)

	seed := flowSeed{
		workflowURL:        workflowURL,
		workflowName:       graph.Name(),
		graphJSON:          string(graphJSON),
		baggageJSON:        string(baggageJSON),
		stateJSON:          string(stateJSON),
		traceParent:        traceParent,
		flowToken:          flowToken,
		stepToken:          stepToken,
		entryPoint:         entryPoint,
		entryURL:           entryURL,
		timeBudgetMs:       timeBudget.Milliseconds(),
		deleteOnCompletion: deleteOnCompletion,
		surgraphFlowID:     surgraphFlowID,
		surgraphStepID:     surgraphStepID,
		callerStepDepth:    callerStepDepth,
		threadID:           threadID,
		threadToken:        threadToken,
		rootFlowID:         rootFlowID,
		priority:           opts.Priority,
		fairnessKey:        opts.FairnessKey,
		fairnessWeight:     opts.FairnessWeight,
	}

	var newFlowID, newStepID int64
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		var err error
		newFlowID, newStepID, err = insertFlowTx(ctx, tx, seed)
		return err
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	flowKey = fmt.Sprintf("%d-%d-%s", shardNum, newFlowID, flowToken)
	e.logger.DebugContext(ctx, "Flow created and started", "workflow", workflowURL, "task", entryPoint)
	e.metricFlowStarted(ctx, workflowURL)
	// Ring the doorbell so a replica with spare capacity claims the entry step immediately, rather than
	// waiting for the backstop poll. A missed doorbell is recovered by pollPendingSteps.
	e.enqueueStep(ctx, shardNum, int(newStepID))
	return flowKey, nil
}

// snapshot returns the current outcome of a flow.
func (e *Engine) snapshot(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var flowStatus string
	var finalStateJSON string
	var flowErrorMsg string
	var flowCancelReason string
	err = db.QueryRowContext(ctx,
		"SELECT status, final_state, error, cancel_reason FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus, &finalStateJSON, &flowErrorMsg, &flowCancelReason)
	if err == sql.ErrNoRows {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	flowStatus = strings.TrimSpace(flowStatus)
	flowErrorMsg = strings.TrimSpace(flowErrorMsg)
	flowCancelReason = strings.TrimSpace(flowCancelReason)

	out := &workflow.FlowOutcome{
		Status: flowStatus,
	}

	switch flowStatus {
	case workflow.StatusCompleted:
		var state map[string]any
		unmarshalJSONMap(finalStateJSON, &state)
		out.State = state
	case workflow.StatusFailed:
		var state map[string]any
		unmarshalJSONMap(finalStateJSON, &state)
		out.State = state
		out.Error = flowErrorMsg
	case workflow.StatusCancelled:
		var state map[string]any
		unmarshalJSONMap(finalStateJSON, &state)
		out.State = state
		out.CancelReason = flowCancelReason
	case workflow.StatusInterrupted:
		// For interrupted, query the leaf step's state and interrupt payload
		var stepStateJSON, stepChangesJSON string
		var interruptPayloadJSON sql.NullString
		// Pick the same interrupted leaf Resume's chain walk would act on (earliest-updated, step_id
		// tiebreak) - not by step_depth, which is only an informational ordering and varies with branch
		// length (loops/gotos) without indicating which interrupt resolves next.
		err = db.QueryRowContext(ctx,
			"SELECT state, changes, interrupt_payload FROM dwarf_steps"+
				" WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"' ORDER BY updated_at, step_id LIMIT_OFFSET(1, 0)",
			flowID,
		).Scan(&stepStateJSON, &stepChangesJSON, &interruptPayloadJSON)
		// A missing interrupted step (ErrNoRows) is a tolerated race - report empty state; a real DB
		// error must not masquerade as that empty state.
		if err != nil && err != sql.ErrNoRows {
			return nil, errors.Trace(err)
		}
		if err == nil {
			var stepState, stepChanges map[string]any
			unmarshalJSONMap(stepStateJSON, &stepState)
			unmarshalJSONMap(stepChangesJSON, &stepChanges)
			merged, _ := workflow.MergeState(stepState, stepChanges, nil)
			out.State = merged
			if interruptPayloadJSON.Valid {
				var payload map[string]any
				unmarshalJSONMap(interruptPayloadJSON.String, &payload)
				out.InterruptPayload = payload
			}
		}
	case workflow.StatusRunning, workflow.StatusCreated:
		out.State = map[string]any{}
	}

	return out, nil
}

// await blocks until a flow stops. A DeleteOnCompletion flow stays `completed` (outcome observable) for the
// deletion grace window, then the reaper removes it and await 404s.
func (e *Engine) await(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	stopped := func(s string) bool {
		return s != "" && s != workflow.StatusCreated && s != workflow.StatusPending && s != workflow.StatusRunning
	}

	ch := make(chan string, 1)
	e.waitersLock.Lock()
	if e.waiters == nil {
		e.waiters = make(map[string][]chan string)
	}
	e.waiters[flowKey] = append(e.waiters[flowKey], ch)
	e.waitersLock.Unlock()

	defer func() {
		e.waitersLock.Lock()
		chans := e.waiters[flowKey]
		for i, c := range chans {
			if c == ch {
				e.waiters[flowKey] = append(chans[:i], chans[i+1:]...)
				break
			}
		}
		if len(e.waiters[flowKey]) == 0 {
			delete(e.waiters, flowKey)
		}
		e.waitersLock.Unlock()
	}()

	// signalStop is a post-commit, in-memory, fire-and-forget wake, so it can be lost: a worker crash
	// between committing the terminal status and signaling, a dropped peer broadcast, or a no-op
	// SignalPeers on a multi-replica host. Any of those would leave this waiter blocked until its ctx
	// deadline (forever on a deadline-less ctx) while the flow already sits stopped in the DB. The ticker
	// is the safety net: re-snapshot every awaitPollInterval even absent a notification, so the worst-case
	// hang past the actual stop is bounded by that interval rather than unbounded.
	ticker := time.NewTicker(e.awaitPollInterval)
	defer ticker.Stop()

	for {
		outcome, err := e.snapshot(ctx, flowKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if outcome != nil && stopped(outcome.Status) {
			return outcome, nil
		}
		select {
		case s := <-ch:
			// drainRuntime sends this sentinel at Shutdown so a waiter on a still-running flow returns
			// instead of spinning on the ticker until the caller's ctx expires. A real stop status buffered
			// ahead of it was already caught by the snapshot at the top of this loop, so reaching here on the
			// sentinel means the flow has not stopped and never will under this engine.
			if s == awaitShutdownSignal {
				return nil, errors.New("engine is shutting down", http.StatusServiceUnavailable)
			}
		case <-ticker.C:
		case <-ctx.Done():
			// The ctx ended before the flow stopped. Return the current non-terminal outcome (Stopped() is
			// false) rather than an error; the public Await turns a not-stopped result into a timeout error,
			// while Poll returns it as-is so a caller can re-poll.
			if outcome != nil {
				return outcome, nil
			}
			return &workflow.FlowOutcome{Status: workflow.StatusRunning}, nil
		}
	}
}

// signalStop wakes local Await callers waiting on the given flow and broadcasts the stopped status to
// peer replicas so their Await callers wake too. Use it at every flow-stop site (completed, failed,
// cancelled, interrupted); non-terminal transitions (running) need only the local notifyStatusChange.
func (e *Engine) signalStop(ctx context.Context, flowKey string, status string) {
	// Test rendezvous: a flow just reached a committed stop (this runs post-commit). Placed before the
	// drop-fault below so it fires even when the wake itself is dropped - a test waiting on "the flow stopped"
	// should observe the DB-committed stop regardless of wake delivery. Inert in production.
	e.checkpoint(ctx, checkpointFlowStopped)
	// faultDropSignalStop simulates a lost terminal wake (worker crash between commit and signal, dropped
	// broadcast, no-op SignalPeers) so a test can prove Await still returns via its periodic re-snapshot.
	if e.isFault(faultDropSignalStop) {
		return
	}
	e.notifyStatusChange(flowKey, status)
	e.signalStatusChange(ctx, flowKey, status)
}

// notifyStatusChange wakes up all Await callers waiting on the given flow.
func (e *Engine) notifyStatusChange(flowKey string, status string) {
	e.waitersLock.Lock()
	chans := e.waiters[flowKey]
	waiting := make([]chan string, len(chans))
	copy(waiting, chans)
	e.waitersLock.Unlock()

	for _, ch := range waiting {
		select {
		case ch <- status:
		default:
		}
	}
}

// enqueueStep rings the work doorbell on this replica AND wakes peer replicas. Use it at every
// step-origination site (start, restart, resume, retry, fan-out, fan-in, surgraph re-dispatch), so a
// replica without spare capacity does not strand a freshly-pending step until a peer's backstop poll
// (up to maxPollInterval away). A single logical doorbell must reach both the local replica and its
// peers. SignalPeers is self-excluded (see its contract), so
// the local ring is done directly here. Do NOT call this from DeliverSignal's enqueue path (the
// inbound peer signal): re-broadcasting an inbound doorbell would echo back to the sender and storm.
// That path uses the local-only handleEnqueue primitive.
func (e *Engine) enqueueStep(ctx context.Context, shard, stepID int) {
	// faultDropDoorbell simulates a lost work doorbell so a test can prove the pending step is still picked
	// up by the pollPendingSteps backstop rather than stranding.
	if e.isFault(faultDropDoorbell) {
		return
	}
	e.handleEnqueue(ctx, shard, stepID)
	e.signalEnqueue(ctx, shard, stepID)
}

// handleEnqueue processes a doorbell signal on the local replica only.
func (e *Engine) handleEnqueue(ctx context.Context, shard, stepID int) {
	priority := math.MaxInt
	var notBeforeDelayMs sql.NullFloat64
	db, err := e.db.Shard(shard)
	if err == nil {
		db.QueryRowContext(ctx,
			"SELECT priority, DATE_DIFF_MILLIS(not_before, NOW_UTC()) FROM dwarf_steps WHERE step_id=?",
			stepID,
		).Scan(&priority, &notBeforeDelayMs)
	}
	if notBeforeDelayMs.Valid && notBeforeDelayMs.Float64 > 0 {
		wakeAt := time.Now().Add(time.Duration(notBeforeDelayMs.Float64 * float64(time.Millisecond)))
		e.shortenNextPoll(wakeAt)
		e.logger.DebugContext(ctx, "Doorbell deferred", "stepID", stepID, "delayMs", notBeforeDelayMs.Float64)
		return
	}
	ring := e.cache.Offer(candidatecache.Job{StepID: stepID, Shard: shard}, priority)
	e.logger.DebugContext(ctx, "Doorbell", "stepID", stepID, "priority", priority, "ring", ring)
	if ring {
		e.requestRefill()
	}
}

// cancel aborts a flow and its entire surgraph chain + descendants.
func (e *Engine) cancel(ctx context.Context, flowKey string, reason string) error {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	var flowStatus string
	var surgraphFlowID int
	err = db.QueryRowContext(ctx,
		"SELECT status, surgraph_flow_id FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus, &surgraphFlowID)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	// Cancel tears down the whole tree (it walks up to the root and down to all descendants), so it must be
	// addressed by the root flow key; a subgraph child key is read-only (introspection/Fork only).
	if surgraphFlowID != 0 {
		return errors.New("cannot cancel a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	flowStatus = strings.TrimSpace(flowStatus)
	if flowStatus == workflow.StatusCompleted || flowStatus == workflow.StatusFailed || flowStatus == workflow.StatusCancelled {
		return errors.New("flow is already in terminal status", http.StatusConflict)
	}

	surgraphFlowIDs, surgraphStepIDs, surgraphCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}
	descendantFlowIDs, descendantCompositeIDs, err := e.allSubgraphFlows(ctx, shardNum, flowID)
	if err != nil {
		return errors.Trace(err)
	}

	allFlowIDs := append([]any{}, surgraphFlowIDs...)
	allFlowIDs = append(allFlowIDs, descendantFlowIDs...)
	allCompositeIDs := append([]string{}, surgraphCompositeIDs...)
	allCompositeIDs = append(allCompositeIDs, descendantCompositeIDs...)

	reason = strings.TrimSpace(reason)
	finalStates := make([]string, len(allFlowIDs))
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		// faultCancelCommit fails this transaction once, before any write, so the test proves it rolls back
		// atomically (the tree is untouched) and a retry then cancels cleanly.
		if e.isFault(faultCancelCommit) {
			return errors.New("injected fault: " + faultCancelCommit)
		}
		flowPlaceholders := strings.Repeat("?,", len(allFlowIDs)-1) + "?"
		stepArgs := append([]any{workflow.StatusCancelled, parkedNone}, allFlowIDs...)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusInterrupted+"', '"+workflow.StatusRunning+"')",
			stepArgs...,
		)

		if len(surgraphStepIDs) > 0 {
			surgraphStepPlaceholders := strings.Repeat("?,", len(surgraphStepIDs)-1) + "?"
			surgraphStepArgs := append([]any{workflow.StatusCancelled, parkedNone}, surgraphStepIDs...)
			tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE step_id IN ("+surgraphStepPlaceholders+") AND status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusInterrupted+"', '"+workflow.StatusRunning+"')",
				surgraphStepArgs...,
			)
		}

		for i, fid := range allFlowIDs {
			fs, _, err := e.computeFinalState(ctx, tx, fid.(int))
			if err != nil {
				return errors.Trace(err)
			}
			finalStates[i] = fs
		}

		var caseClause strings.Builder
		caseClause.WriteString("CASE")
		var flowArgs []any
		for i, fid := range allFlowIDs {
			caseClause.WriteString(" WHEN flow_id=? THEN ?")
			flowArgs = append(flowArgs, fid, finalStates[i])
		}
		caseClause.WriteString(" END")
		flowArgs = append(flowArgs, workflow.StatusCancelled, reason)
		flowArgs = append(flowArgs, allFlowIDs...)
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET final_state="+caseClause.String()+", status=?, cancel_reason=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id IN ("+flowPlaceholders+") AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
			flowArgs...,
		)
		if err != nil {
			return errors.Trace(err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return errors.New("flow is already in terminal status", http.StatusConflict)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	for i, cid := range allCompositeIDs {
		e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, allFlowIDs[i].(int)), "to", workflow.StatusCancelled)
		e.signalStop(ctx, cid, workflow.StatusCancelled)
	}
	return nil
}

// deleteFlow schedules a flow (and its subgraph subtree) for deletion by the reaper - it does NOT delete rows
// inline. It stamps delete_after_ms=1 (due immediately) on the root; the reaper removes the whole tree on its
// next pass. An interrupted flow is terminalized (interrupted -> cancelled) in the same UPDATE, which is
// mutually exclusive with a racing Resume (WHERE status<>'running', re-checked under the row lock), so the
// old strand race (delete steps while Resume revives) is gone by construction - no lock-first selection needed.
func (e *Engine) deleteFlow(ctx context.Context, flowKey string) error {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	// Test checkpoint: a breakpoint here freezes Delete just before its transaction (holding no lock, so a
	// racing Resume can commit), letting a test drive the Delete-vs-Resume race in either order. Placed before
	// the transaction, not inside, for the same SQLite-deadlock reason as checkpointResumeBeforeFlowWrite.
	e.checkpoint(ctx, checkpointBeforeDeleteWrite)
	return errors.Trace(db.Transact(ctx, func(tx *sequel.Tx) error {
		var flowStatus string
		var surgraphFlowID, deleteAfterMs int
		err := tx.QueryRowContext(ctx,
			"SELECT status, surgraph_flow_id, delete_after_ms FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
			flowID, flowToken,
		).Scan(&flowStatus, &surgraphFlowID, &deleteAfterMs)
		if err == sql.ErrNoRows {
			return errors.New("flow not found", http.StatusNotFound)
		}
		if err != nil {
			return errors.Trace(err)
		}
		// A subgraph child cannot be deleted directly (its parent's surgraph step would dangle). Address the
		// tree by the root key; the reaper removes descendants via root_flow_id. A child key is read-only.
		if surgraphFlowID != 0 {
			return errors.New("cannot delete a subgraph child; use the root flow key", http.StatusBadRequest)
		}
		// The 409 guards only the ROOT's status; a running subgraph descendant does not block the delete. The
		// reaper later removes the whole root_flow_id tree regardless of descendant status - safe because the
		// only running descendant a terminal-rooted tree can hold is a live orphan (Cancel-vs-spawn residue) the
		// wedge sweep would cancel anyway, and a worker mid-dispatch on it no-ops via the lease fence. This is a
		// deliberate change from the old inline delete, which 409'd on any running descendant (see reapDueFlows).
		if strings.TrimSpace(flowStatus) == workflow.StatusRunning {
			return errors.New("cannot delete a running flow; cancel it first", http.StatusConflict)
		}
		if deleteAfterMs > 0 {
			return nil // already scheduled - idempotent
		}

		// Stamp due-now; terminalize an interrupted flow in the same write (the Resume gate). status<>'running'
		// re-guards a Resume that raced in after the SELECT: it either wins (row is running -> 0 rows here, a
		// benign lost delete, flow stays alive) or loses (we stamp; its interrupted CAS then finds cancelled).
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET delete_after_ms=1, status=CASE WHEN status='"+workflow.StatusInterrupted+"' THEN '"+workflow.StatusCancelled+"' ELSE status END WHERE flow_id=? AND flow_token=? AND status<>'"+workflow.StatusRunning+"' AND delete_after_ms=0",
			flowID, flowToken,
		)
		return nil
	}))
}

// run creates, starts, and awaits a flow in one call.
func (e *Engine) run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, outcome *workflow.FlowOutcome, err error) {
	flowKey, err = e.create(ctx, workflowURL, initialState, opts) // auto-starts
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	outcome, err = e.await(ctx, flowKey)
	if err != nil {
		return flowKey, nil, errors.Trace(err)
	}
	if !outcome.Stopped() {
		// The caller's ctx expired before the flow stopped. The flow is durable and already running on the
		// engine's own worker lifetime, independent of this call - so do NOT tear it down. Cancelling here
		// would destroy healthy, in-progress work (a durable retry-until-success job especially) just because
		// the caller stopped waiting - an availability footgun. Return the flowKey with a timeout error
		// instead, so the caller keeps a handle to re-Await/Snapshot/Cancel on its own terms.
		return flowKey, nil, errors.Trace(ctx.Err(), http.StatusRequestTimeout)
	}
	return flowKey, outcome, nil
}
