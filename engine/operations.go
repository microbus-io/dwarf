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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// Create creates a new flow for a workflow without starting it.
func (e *Engine) create(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	if workflowURL == "" {
		return "", errors.New("workflow URL is required", http.StatusBadRequest)
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
	if err != nil {
		return "", errors.Trace(err)
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
		shardNum = rand.IntN(e.numDBShards()) + 1
	}
	flowKey, err = e.createWithGraph(ctx, shardNum, workflowURL, graph, initialState, threadID, threadToken, "", opts, 0, 0, 0, 0)
	return flowKey, errors.Trace(err)
}

// resolveThread parses a FlowKey identifying a thread and returns the thread's shard, id, and token. The
// shard is encoded in the key; the thread_id/thread_token are read from the referenced flow (which may be
// mid-thread, so its thread_id differs from its own flow_id). Verifies the flow exists (token-checked).
func (e *Engine) resolveThread(ctx context.Context, threadKey string) (shardNum, threadID int, threadToken string, err error) {
	shardNum, flowID, flowToken, err := parseFlowKey(threadKey)
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	db, err := e.shard(shardNum)
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

	flowToken := randomIdentifier(16)
	stepToken := randomIdentifier(16)
	// opts.TimeBudget is resolved (create/continue) or inherited (subgraph). Fall back to the
	// live default only as defense for an unresolved path.
	timeBudget := opts.TimeBudget
	if timeBudget <= 0 {
		timeBudget = e.taskTimeBudget()
	}
	notifyOnStop := 0
	if opts.NotifyOnStop {
		notifyOnStop = 1
	}
	deleteOnCompletion := 0
	if opts.DeleteOnCompletion {
		deleteOnCompletion = 1
	}

	db, err := e.shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}
	entryURL := dispatchURLOf(graph, entryPoint)

	var newFlowID, newStepID int64
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		var err error
		newFlowID, err = tx.InsertReturnID(ctx, "flow_id",
			"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, baggage, trace_parent, status, surgraph_flow_id, surgraph_step_id, notify_on_stop, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms, started_at)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC())",
			flowToken, workflowURL, graph.Name(), string(graphJSON), string(baggageJSON), traceParent, workflow.StatusRunning, surgraphFlowID, surgraphStepID, notifyOnStop, deleteOnCompletion, opts.Priority, opts.FairnessKey, opts.FairnessWeight, timeBudget.Milliseconds(),
		)
		if err != nil {
			return errors.Trace(err)
		}

		// Entry step is pending and immediately claimable (not_before=NOW, lease_expires=NOW). Its depth
		// continues from the caller: callerStepDepth+1 (1 for a top-level flow, where callerStepDepth is 0).
		// A flow that should wait before running uses an entry gate task with flow.Sleep, not a creation-time
		// delay.
		newStepID, err = tx.InsertReturnID(ctx, "step_id",
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, time_budget_ms, not_before, lease_expires, priority, fairness_key, fairness_weight)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC(), NOW_UTC(), ?, ?, ?)",
			newFlowID, callerStepDepth+1, stepToken, entryPoint, entryURL, string(stateJSON), workflow.StatusPending, timeBudget.Milliseconds(), opts.Priority, opts.FairnessKey, opts.FairnessWeight,
		)
		if err != nil {
			return errors.Trace(err)
		}

		// Derive the thread from the new flow inside the closure so a retry (with a new flow_id) recomputes
		// it rather than reusing the prior attempt's id.
		tid, ttok := threadID, threadToken
		if tid == 0 {
			tid = int(newFlowID)
			ttok = flowToken
		}
		// root_flow_id is the denormalized tree-membership index: a top-level flow (rootFlowID==0) is its
		// own root; a subgraph child inherits the parent's root. Written once here, immutable thereafter, so
		// a whole tree is reachable by `WHERE root_flow_id=?` without a recursive surgraph walk.
		rfid := rootFlowID
		if rfid == 0 {
			rfid = int(newFlowID)
		}
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET thread_id=?, thread_token=?, step_id=?, root_flow_id=?, updated_at=NOW_UTC() WHERE flow_id=?",
			tid, ttok, newStepID, rfid, newFlowID,
		)
		return nil
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	flowKey = fmt.Sprintf("%d-%d-%s", shardNum, newFlowID, flowToken)
	e.logger.DebugContext(ctx, "Flow created and started", "flow", workflowURL, "task", entryPoint)
	e.metricFlowStarted(ctx, workflowURL)
	// Ring the doorbell so a replica with spare capacity claims the entry step immediately, rather than
	// waiting for the backstop poll. A missed doorbell is recovered by pollPendingSteps.
	e.enqueueStep(ctx, shardNum, int(newStepID))
	return flowKey, nil
}


// snapshot returns the current outcome of a flow.
func (e *Engine) snapshot(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.shard(shardNum)
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
				" WHERE flow_id=? AND status=? ORDER BY updated_at, step_id LIMIT_OFFSET(1, 0)",
			flowID, workflow.StatusInterrupted,
		).Scan(&stepStateJSON, &stepChangesJSON, &interruptPayloadJSON)
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

// await blocks until a flow stops. A completed DeleteOnCompletion flow is gone, so await returns its 404 as
// the completion signal (see "Data Retention" in CLAUDE.md).
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

	for {
		outcome, err := e.snapshot(ctx, flowKey)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if outcome != nil && stopped(outcome.Status) {
			return outcome, nil
		}
		select {
		case <-ch:
			continue
		case <-ctx.Done():
			return nil, errors.Trace(ctx.Err(), http.StatusRequestTimeout)
		}
	}
}

// signalStop wakes local Await callers waiting on the given flow and broadcasts the stopped status to
// peer replicas so their Await callers wake too. Use it at every flow-stop site (completed, failed,
// cancelled, interrupted); non-terminal transitions (running) need only the local notifyStatusChange.
func (e *Engine) signalStop(ctx context.Context, flowKey string, status string) {
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
	e.handleEnqueue(ctx, shard, stepID)
	e.signalEnqueue(ctx, shard, stepID)
}

// handleEnqueue processes a doorbell signal on the local replica only.
func (e *Engine) handleEnqueue(ctx context.Context, shard, stepID int) {
	priority := math.MaxInt
	var notBeforeDelayMs sql.NullFloat64
	db, err := e.shard(shard)
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
	ring := e.cache.offer(job{stepID: stepID, shard: shard}, priority)
	e.logger.DebugContext(ctx, "Doorbell", "stepID", stepID, "priority", priority, "ring", ring)
	if ring {
		e.requestRefill()
	}
}

// cancel aborts a flow and its entire surgraph chain + descendants.
func (e *Engine) cancel(ctx context.Context, flowKey string, reason string) error {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	var flowStatus string
	err = db.QueryRowContext(ctx,
		"SELECT status FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
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
		flowPlaceholders := strings.Repeat("?,", len(allFlowIDs)-1) + "?"
		stepArgs := append([]any{workflow.StatusCancelled, parkedNone}, allFlowIDs...)
		stepArgs = append(stepArgs, workflow.StatusCreated, workflow.StatusPending, workflow.StatusInterrupted, workflow.StatusRunning)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status IN (?, ?, ?, ?)",
			stepArgs...,
		)

		if len(surgraphStepIDs) > 0 {
			surgraphStepPlaceholders := strings.Repeat("?,", len(surgraphStepIDs)-1) + "?"
			surgraphStepArgs := append([]any{workflow.StatusCancelled, parkedNone}, surgraphStepIDs...)
			surgraphStepArgs = append(surgraphStepArgs, workflow.StatusCreated, workflow.StatusPending, workflow.StatusInterrupted, workflow.StatusRunning)
			tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE step_id IN ("+surgraphStepPlaceholders+") AND status IN (?, ?, ?, ?)",
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

		caseClause := "CASE"
		var flowArgs []any
		for i, fid := range allFlowIDs {
			caseClause += " WHEN flow_id=? THEN ?"
			flowArgs = append(flowArgs, fid, finalStates[i])
		}
		caseClause += " END"
		flowArgs = append(flowArgs, workflow.StatusCancelled, reason)
		flowArgs = append(flowArgs, allFlowIDs...)
		flowArgs = append(flowArgs, workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled)
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET final_state="+caseClause+", status=?, cancel_reason=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status NOT IN (?, ?, ?)",
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

	rootIdx := len(surgraphFlowIDs) - 1
	rootCompositeID := surgraphCompositeIDs[rootIdx]
	var rootNotifyOnStop bool
	var rootBaggageJSON string
	db.QueryRowContext(ctx, "SELECT notify_on_stop, baggage FROM dwarf_flows WHERE flow_id=?", surgraphFlowIDs[rootIdx]).Scan(&rootNotifyOnStop, &rootBaggageJSON)
	if rootNotifyOnStop {
		var finalState map[string]any
		json.Unmarshal([]byte(finalStates[rootIdx]), &finalState)
		e.fireFlowStopped(ctx, rootCompositeID, rootBaggageJSON, &workflow.FlowOutcome{
			Status:       workflow.StatusCancelled,
			State:        finalState,
			CancelReason: reason,
		})
	}
	for _, cid := range allCompositeIDs {
		e.logger.InfoContext(ctx, "Flow status transition", "to", workflow.StatusCancelled)
		e.signalStop(ctx, cid, workflow.StatusCancelled)
	}
	return nil
}

// deleteFlow removes a flow and its steps.
func (e *Engine) deleteFlow(ctx context.Context, flowKey string) error {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(db.Transact(ctx, func(tx *sequel.Tx) error {
		var flowStatus string
		err := tx.QueryRowContext(ctx,
			"SELECT status FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
			flowID, flowToken,
		).Scan(&flowStatus)
		if err == sql.ErrNoRows {
			return errors.New("flow not found", http.StatusNotFound)
		}
		if err != nil {
			return errors.Trace(err)
		}
		if strings.TrimSpace(flowStatus) == workflow.StatusRunning {
			return errors.New("cannot delete a running flow; cancel it first", http.StatusConflict)
		}

		// Delete cascades to the flow's subgraph descendants, recursively, so deleting a flow does not
		// strand its children (whose only inbound reference is the now-deleted surgraph step). Subgraph
		// children have parent-shard affinity, so all descendants live on this same shard/transaction.
		descendants, err := e.allDescendantSubgraphFlows(ctx, tx, flowID)
		if err != nil {
			return errors.Trace(err)
		}
		if len(descendants) > 0 {
			ph := strings.Repeat("?,", len(descendants)-1) + "?"
			args := make([]any, 0, len(descendants))
			for _, id := range descendants {
				args = append(args, id)
			}
			// Refuse the whole cascade if any descendant is still running, mirroring the root guard -
			// no partial delete that would orphan a live child's parent.
			var runningChildren int
			err = tx.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM dwarf_flows WHERE flow_id IN ("+ph+") AND status=?",
				append(append([]any{}, args...), workflow.StatusRunning)...,
			).Scan(&runningChildren)
			if err != nil {
				return errors.Trace(err)
			}
			if runningChildren > 0 {
				return errors.New("cannot delete a flow with a running subgraph descendant; cancel it first", http.StatusConflict)
			}
			tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+ph+")", args...)
			tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id IN ("+ph+")", args...)
		}

		tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id=?", flowID)
		tx.ExecContext(ctx,
			"DELETE FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND status<>?",
			flowID, flowToken, workflow.StatusRunning,
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
		e.cancel(ctx, flowKey, "")
		return "", nil, errors.Trace(err)
	}
	return flowKey, outcome, nil
}
