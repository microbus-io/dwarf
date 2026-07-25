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
	// Reject a negative priority/weight/budget rather than silently coercing it to the default
	// (resolveFlowOptions treats <=0 as "unset"): a negative value is a caller bug, and swallowing it hides
	// it. 0 stays "use the engine default" for all three, so this only rejects genuinely-invalid input.
	// Genesis-only (Create/Run); derived ops (Continue/Fork/subgraph) inherit already-validated values and
	// take no FlowOptions.
	if opts != nil {
		if opts.Priority < 0 {
			return "", errors.New("priority must be >= 0", http.StatusBadRequest)
		}
		if opts.FairnessWeight < 0 {
			return "", errors.New("fairness weight must be >= 0", http.StatusBadRequest)
		}
		// 0 means "use the engine default"; anything else must survive the millisecond persistence, or the
		// step is stamped with a 0 budget and its task's deadline has already passed when it is dispatched.
		if opts.TimeBudget < 0 || (opts.TimeBudget > 0 && opts.TimeBudget < time.Millisecond) {
			return "", errors.New("time budget must be 0 (engine default) or at least 1ms", http.StatusBadRequest)
		}
	}
	opts = e.resolveFlowOptions(opts)
	// The create-time GraphLoader sees the baggage on ctx in the same JSON-decoded shape (numbers as
	// float64) every dispatch will - NewState round-trips the raw Go value through JSON.
	loaderBaggage, _ := workflow.NewState(opts.Baggage)
	loaderCtx := workflow.ContextWithBaggage(ctx, loaderBaggage)
	var graph *workflow.Graph
	err = errors.CatchPanic(func() error {
		var lerr error
		graph, lerr = e.host.LoadGraph(loaderCtx, workflowURL)
		return lerr
	})
	if err == nil && e.seams.IsFault(FaultLoadGraph, workflowURL) {
		err = errors.New("injected fault: "+FaultLoadGraph+" "+workflowURL, http.StatusInternalServerError)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	// A host that returns (nil, nil) is a not-found, not a nil-deref: guard before EntryPoint/Validate.
	if graph == nil {
		return "", errors.New("workflow graph not found: %s", workflowURL, http.StatusNotFound)
	}
	// Validate at create (documented behavior): reject a structurally invalid graph up front. Validation is
	// pure - the fan-in convergence it checks is computed into a local map and not stored on the graph. The
	// engine derives that map per flow at dispatch (internal/faninmap), so only the author's definition is
	// serialized into the flow's graph JSON below.
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
		shardNum, err = e.pickShard()
		if err != nil {
			return "", errors.Trace(err)
		}
	}
	// Normalize the caller's initialState (a struct, map, or nil) into a State at the door. A nil yields
	// empty state (NewState's nil case), so a marshaled "null" never reaches the strict object parser.
	state, err := workflow.NewState(initialState)
	if err != nil {
		return "", errors.Trace(err, http.StatusBadRequest)
	}
	flowKey, err = e.createWithGraph(ctx, shardNum, workflowURL, graph, state, threadID, threadToken, "", opts, 0, 0, 0, 0)
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
	var surgraphFlowID int
	err = db.QueryRowContext(ctx,
		"SELECT thread_id, thread_token, surgraph_flow_id FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&threadID, &threadToken, &surgraphFlowID)
	if err == sql.ErrNoRows {
		return 0, 0, "", errors.New("thread not found", http.StatusNotFound)
	}
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	// A subgraph child key is read-only, like everywhere else (Resume/Cancel/Delete/Continue all 400 it).
	// A child runs on its own private thread precisely so it cannot contaminate the parent's continuation
	// chain, so joining that thread is never what a caller means: the new flow would be a top-level root
	// grouped under a subgraph's thread, and a later Continue of it would build on the subgraph's turns.
	// Reject rather than silently widen to the child's root - the caller must name the root it means.
	if surgraphFlowID != 0 {
		return 0, 0, "", errors.New("thread key is a subgraph flow; address the root flow", http.StatusBadRequest)
	}
	return shardNum, threadID, strings.TrimSpace(threadToken), nil
}

// flowSeed carries the precomputed, retry-stable values for inserting one new flow plus its entry step.
// It is built outside the transaction (tokens, spans, marshalled JSON) so a lock-contention retry of the
// insert closure reuses the same values rather than rerolling tokens.
type flowSeed struct {
	workflowURL        string
	workflowName       string
	graphJSON          []byte
	baggageJSON        []byte
	stateJSON          []byte
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
	engineID           int64 // creator stamp: the inserting engine's random id (provenance, unindexed)
}

// insertFlowTx inserts the flow row (already `running`) and its entry step (`pending`, immediately
// claimable), then fixes thread_id/step_id/root_flow_id - the shared write-half of every creation path,
// run inside the caller's transaction. Returns the new flow and step ids. createWithGraph wraps it in a
// bare transaction; continueFlow wraps it in a transaction that first locks the thread and re-checks the
// latest turn, so both share exactly one copy of the insert SQL.
func insertFlowTx(ctx context.Context, tx *sequel.Tx, s flowSeed) (newFlowID, newStepID int64, err error) {
	newFlowID, err = tx.InsertReturnID(ctx, "flow_id",
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, baggage, trace_parent, status, surgraph_flow_id, surgraph_step_id, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms, engine_id, started_at)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC())",
		s.flowToken, s.workflowURL, s.workflowName, s.graphJSON, s.baggageJSON, s.traceParent, workflow.StatusRunning, s.surgraphFlowID, s.surgraphStepID, s.deleteOnCompletion, s.priority, s.fairnessKey, s.fairnessWeight, s.timeBudgetMs, s.engineID,
	)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	// Entry step is pending and immediately claimable (not_before=NOW, lease_expires=NOW). Its depth
	// continues from the caller: callerStepDepth+1 (1 for a top-level flow, where callerStepDepth is 0).
	// A flow that should wait before running uses an entry gate task with flow.Sleep, not a creation-time
	// delay.
	newStepID, err = tx.InsertReturnID(ctx, "step_id",
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, time_budget_ms, not_before, lease_expires, priority, fairness_key, fairness_weight, engine_id)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC(), NOW_UTC(), ?, ?, ?, ?)",
		newFlowID, s.callerStepDepth+1, s.stepToken, s.entryPoint, s.entryURL, s.stateJSON, workflow.StatusPending, s.timeBudgetMs, s.priority, s.fairnessKey, s.fairnessWeight, s.engineID,
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
func (e *Engine) createWithGraph(ctx context.Context, shardNum int, workflowURL string, graph *workflow.Graph, initialState workflow.State, threadID int, threadToken string, parentTraceParent string, opts *workflow.FlowOptions, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID int) (flowKey string, err error) {
	entryPoint := graph.EntryPoint()
	if entryPoint == "" {
		return "", errors.New("workflow has no entry point", http.StatusBadRequest)
	}

	traceParent := e.mintWorkflowSpan(ctx, workflowURL, parentTraceParent)

	// A non-marshalable host-supplied value (a NaN/Inf/channel in Baggage or a map initialState, which
	// NewState copies through without marshaling) surfaces here as a 400 - it is caller input.
	baggageJSON, err := json.Marshal(opts.Baggage)
	if err != nil {
		return "", errors.New("baggage is not JSON-marshalable: %v", err, http.StatusBadRequest)
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		return "", errors.Trace(err)
	}
	stateJSON, err := json.Marshal(initialState)
	if err != nil {
		return "", errors.New("initial state is not JSON-marshalable: %v", err, http.StatusBadRequest)
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
		graphJSON:          graphJSON,
		baggageJSON:        baggageJSON,
		stateJSON:          stateJSON,
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
		engineID:           e.engineID,
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
	// The entry step's initial-state snapshot, written by insertFlowTx above.
	e.metricStateWriteBytes(ctx, workflowURL, "state", len(seed.stateJSON))
	// Ring the doorbell so a replica with spare capacity claims the entry step immediately, rather than
	// waiting for the backstop poll. A missed doorbell is recovered by pollPendingSteps. The entry step
	// was just inserted due-now with the resolved priority, so the fast-path doorbell applies.
	e.enqueueStepDue(ctx, shardNum, int(newStepID), seed.priority)
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
	var finalStateJSON []byte
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
		out.State, _ = workflow.NewState(finalStateJSON)
	case workflow.StatusFailed:
		out.State, _ = workflow.NewState(finalStateJSON)
		out.Error = flowErrorMsg
	case workflow.StatusCancelled:
		out.State, _ = workflow.NewState(finalStateJSON)
		out.CancelReason = flowCancelReason
	case workflow.StatusInterrupted:
		// For interrupted, query the leaf step's state and interrupt payload
		var stepStateJSON, stepChangesJSON, stepRefsJSON, interruptPayloadJSON []byte
		// Pick the same interrupted leaf Resume's chain walk would act on (earliest-updated, step_id
		// tiebreak) - not by step_depth, which is only an informational ordering and varies with branch
		// length (loops/gotos) without indicating which interrupt resolves next.
		err = db.QueryRowContext(ctx,
			"SELECT state, changes, state_refs, interrupt_payload FROM dwarf_steps"+
				" WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"' ORDER BY updated_at, step_id LIMIT_OFFSET(1, 0)",
			flowID,
		).Scan(&stepStateJSON, &stepChangesJSON, &stepRefsJSON, &interruptPayloadJSON)
		// A missing interrupted step (ErrNoRows) is a tolerated race - report empty state; a real DB
		// error must not masquerade as that empty state.
		if err != nil && err != sql.ErrNoRows {
			return nil, errors.Trace(err)
		}
		if err == nil {
			stepState, _ := workflow.NewState(stepStateJSON)
			stepChanges, _ := workflow.NewState(stepChangesJSON)
			// The ref encoding is a storage detail, never API-visible: a caller sees the state the step
			// actually saw (invariant 6). resolveStateRefs mutates the map in place, so it gets the live map.
			if rerr := e.resolveStateRefs(ctx, db, shardNum, stepState, parseStateRefs(stepRefsJSON), nil, ""); rerr != nil {
				return nil, errors.Trace(rerr)
			}
			// Materialize the interrupted step's view: state + changes with pending deletes enacted.
			_ = stepState.Merge(stepChanges)
			stepState.DelNils()
			out.State = stepState
			out.InterruptPayload, _ = workflow.NewState(interruptPayloadJSON)
		}
	case workflow.StatusRunning, workflow.StatusCreated:
		out.State = workflow.State{}
	}

	return out, nil
}

// await blocks until a flow stops. A DeleteOnCompletion flow stays `completed` (outcome observable) for the
// deletion grace window, then the reaper removes it and await 404s.
func (e *Engine) await(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	stopped := func(s string) bool {
		return s != "" && s != workflow.StatusCreated && s != workflow.StatusPending && s != workflow.StatusRunning
	}

	// Stamp the flow as awaited so its stop sites broadcast the terminal status to peer replicas
	// (signalStop skips the SignalPeers statusChange for a never-awaited flow - the broadcast's only
	// purpose is to wake remote awaiters). Stamped before the first snapshot below, so a stop that
	// commits after the snapshot observes the flag. A stop transaction that read awaited=0 concurrently
	// with this write may still skip the broadcast; the awaitPollInterval re-snapshot bounds that miss,
	// exactly as it bounds any other lost wake. Write-once: the awaited=0 guard keeps repeated
	// Await/Poll calls from re-locking the row.
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET awaited=1, touch=1-touch WHERE flow_id=? AND flow_token=? AND awaited=0",
		flowID, flowToken,
	)
	if err != nil {
		return nil, errors.Trace(err)
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
// awaited is the flow row's awaited flag (set by Await/Poll): when false, the peer broadcast is skipped -
// its only purpose is to wake remote awaiters, and a never-awaited flow has none. The local wake and the
// stop checkpoint still run either way (they are free), and a waiter that raced the stop's awaited read
// is caught by its own periodic re-snapshot. When the flag is unknown (a read failed), pass true -
// broadcasting is always safe; skipping is only the optimization.
func (e *Engine) signalStop(ctx context.Context, flowKey string, status string, awaited bool) {
	// Test rendezvous: a flow just reached a committed stop (this runs post-commit). Fired both unscoped and
	// scoped to this flow+status, so a test can wait for "any stop" or for one specific flow to reach one
	// specific status. Placed before the drop-fault below so both fire even when the wake itself is dropped - a
	// test waiting on "the flow stopped" should observe the DB-committed stop regardless of wake delivery.
	// Inert in production: the Enabled gate short-circuits before the scoped name is built.
	e.seams.Checkpoint(ctx, CheckpointFlowStopped)
	e.seams.Checkpoint(ctx, CheckpointFlowStopped, flowKey, status)
	// FaultDropSignalStop simulates a lost terminal wake (worker crash between commit and signal, dropped
	// broadcast, no-op SignalPeers) so a test can prove Await still returns via its periodic re-snapshot.
	if e.seams.IsFault(FaultDropSignalStop) {
		return
	}
	e.notifyStatusChange(flowKey, status)
	if awaited {
		e.signalStatusChange(ctx, flowKey, status)
	}
}

// awaitedFlows returns the subset of the given flow ids whose awaited flag is set, for gating the
// signalStop peer broadcast on the multi-flow stop paths (Cancel, interrupt propagation, orphan
// recovery) with one batched read. On any read error it returns nil, which callers treat as
// "all awaited" (broadcast; see signalStop).
func (e *Engine) awaitedFlows(ctx context.Context, shardNum int, flowIDs []any) map[int]bool {
	if len(flowIDs) == 0 {
		return map[int]bool{}
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil
	}
	placeholders := strings.Repeat("?,", len(flowIDs)-1) + "?"
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id FROM dwarf_flows WHERE flow_id IN ("+placeholders+") AND awaited=1",
		flowIDs...,
	)
	if err != nil {
		return nil
	}
	defer rows.Close()
	awaited := map[int]bool{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil
		}
		awaited[id] = true
	}
	if rows.Err() != nil {
		return nil
	}
	return awaited
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

// The work doorbell is PURELY LOCAL - it reaches this replica's candidate cache and nothing else.
//
// It used to also broadcast to peers (op `enqueue`, one message per step per peer). That broadcast was
// removed: under load every peer's refiller is already scanning at its derived scan floor, so the
// doorbell bought no dispatch latency the next scan would not have covered, while costing R-1 messages
// per step AND a PK lookup on every receiver (the inbound path had to resolve the announced step's
// priority and due-ness against its own clock, which is exactly the round-trip enqueueStepDue exists to
// avoid locally). It also head-inserted UNPARTITIONED on the receiver, so a peer could race the residue
// class's owner to the claim CAS. What replaced it is the refiller's idle tick (refillIdleInterval, ~1s -
// see refillerLoop): a peer discovers work by scanning, on a cadence bounded with no message at all.
//
// Consequence to keep in mind when reading the origination sites: a step created here is offered to THIS
// replica's cache only. If this replica cannot serve it (its partition is non-empty so the Offer no-ops,
// and the step falls in a peer's residue class), the step waits for that peer's next scan - bounded by
// the idle tick, not by a message. Do not reintroduce a per-step peer broadcast to shave that; see
// _NO_SIGNALS.md's coalescing note for the shape any revival would have to take.

// enqueueStep rings the local work doorbell for a step whose priority the caller does NOT hold, resolving
// it (and the step's due-ness) with one PK lookup. Use it at the cold step-origination sites - surgraph
// revive, resume, fork's leaf, the wedge sweep - where that value is not in hand; the hot paths use
// enqueueStepDue instead and skip the lookup.
func (e *Engine) enqueueStep(ctx context.Context, shard, stepID int) {
	// FaultDropDoorbell simulates a lost work doorbell so a test can prove the pending step is still picked
	// up by a later refiller scan rather than stranding.
	if e.seams.IsFault(FaultDropDoorbell) {
		return
	}
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
		// Not due yet: nothing to preempt, so leave the cache untouched and let the poll timer wake at the
		// right moment instead.
		wakeAt := time.Now().Add(time.Duration(notBeforeDelayMs.Float64 * float64(time.Millisecond)))
		e.shortenNextPoll(wakeAt)
		e.logger.DebugContext(ctx, "Doorbell deferred", "stepID", stepID, "delayMs", notBeforeDelayMs.Float64)
		return
	}
	ring := e.cache.Offer(candidatecache.Job{StepID: stepID, Shard: shard}, priority)
	e.logger.DebugContext(ctx, "Doorbell", "stepID", stepID, "priority", priority, "ring", ring)
	if ring {
		e.requestRefill(shard)
	}
}

// enqueueStepDue is the fast-path doorbell for a step the caller just created or reset for immediate
// dispatch and whose priority it already holds: it offers the step to the local cache directly, skipping
// enqueueStep's PK lookup - one round-trip per completed step on the hot path, re-reading a row this
// replica just wrote. Use it only where due-ness is certain (the caller's own sleep/not_before branch
// already diverged) and the priority is the value just bound into the INSERT/reset.
func (e *Engine) enqueueStepDue(ctx context.Context, shard, stepID, priority int) {
	// FaultDropDoorbell: see enqueueStep.
	if e.seams.IsFault(FaultDropDoorbell) {
		return
	}
	ring := e.cache.Offer(candidatecache.Job{StepID: stepID, Shard: shard}, priority)
	e.logger.DebugContext(ctx, "Doorbell (due)", "stepID", stepID, "priority", priority, "ring", ring)
	if ring {
		e.requestRefill(shard)
	}
}

// cancel aborts a flow and its whole subgraph subtree. Root-only: a subgraph child is not an independently
// cancellable unit (its parent is parked on it, and the unit of any lifecycle change is the tree), so a child key
// is rejected rather than silently widened into a tree-wide cancel.
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
	if surgraphFlowID != 0 {
		return errors.New("cannot cancel a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	flowStatus = strings.TrimSpace(flowStatus)
	if flowStatus == workflow.StatusCompleted || flowStatus == workflow.StatusFailed || flowStatus == workflow.StatusCancelled {
		return errors.New("flow is already in terminal status", http.StatusConflict)
	}

	// The root has no ancestors by construction, so this walks DOWN only - see cancelSubtree, which the orphan
	// sweep shares. A racing terminalization that empties the flow UPDATE is a 409 here: the caller asked to stop
	// something that had already stopped.
	return errors.Trace(e.cancelSubtree(ctx, shardNum, flowID, flowToken, reason, FaultCancelCommit, true))
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
	// the transaction, not inside, for the same SQLite-deadlock reason as CheckpointResumeBeforeFlowWrite.
	e.seams.Checkpoint(ctx, CheckpointBeforeDeleteWrite)
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
