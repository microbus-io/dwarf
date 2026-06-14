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
func (e *Engine) create(ctx context.Context, workflowName string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	if workflowName == "" {
		return "", errors.New("workflow name is required", http.StatusBadRequest)
	}
	opts = e.resolveFlowOptions(opts)
	// The create-time GraphLoader sees the baggage on ctx in the same decoded shape every dispatch will.
	loaderCtx := workflow.ContextWithBaggage(ctx, baggageMap(opts.Baggage))
	graph, err := e.host.LoadGraph(loaderCtx, workflowName)
	if err != nil {
		return "", errors.Trace(err)
	}
	shardNum := rand.IntN(e.numDBShards()) + 1
	flowKey, err = e.createWithGraph(ctx, shardNum, workflowName, graph, initialState, 0, "", "", opts)
	return flowKey, errors.Trace(err)
}

// createTask creates a flow that executes a single task and then terminates.
func (e *Engine) createTask(ctx context.Context, taskName string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	if taskName == "" {
		return "", errors.New("task name is required", http.StatusBadRequest)
	}
	graph := workflow.NewGraph(taskName)
	graph.AddTransition(taskName, workflow.END)
	shardNum := rand.IntN(e.numDBShards()) + 1
	flowKey, err = e.createWithGraph(ctx, shardNum, taskName, graph, initialState, 0, "", "", e.resolveFlowOptions(opts))
	return flowKey, errors.Trace(err)
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

// createWithGraph is the shared implementation for create, createTask, and continue. opts.Baggage is
// the opaque host value, marshalled to the flow's baggage column (any JSON value, like initialState).
// parentTraceParent controls the flow's own "workflow" span: empty mints a detached root span (top-level
// Create / CreateTask / Continue, each its own trace); non-empty parents the span under that context (a
// subgraph nests under the caller step's span). The minted span's context is stored in the flow's
// trace_parent column and reconstructed as the parent of every per-step span.
func (e *Engine) createWithGraph(ctx context.Context, shardNum int, workflowName string, graph *workflow.Graph, initialState any, threadID int, threadToken string, parentTraceParent string, opts *workflow.FlowOptions) (flowKey string, err error) {
	entryPoint := graph.EntryPoint()
	if entryPoint == "" {
		return "", errors.New("workflow has no entry point", http.StatusBadRequest)
	}

	traceParent := e.mintWorkflowSpan(ctx, workflowName, parentTraceParent)

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
	timeBudget := e.taskTimeBudget()

	db, err := e.shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}

	newFlowID, err := e.createWithGraphTx(ctx, db, flowToken, workflowName, graphJSON, baggageJSON, traceParent, threadID, threadToken, entryPoint, stateJSON, stepToken, timeBudget, opts)
	if err != nil {
		return "", errors.Trace(err)
	}
	e.logger.DebugContext(ctx, "Flow created", "flow", workflowName, "task", entryPoint)
	return fmt.Sprintf("%d-%d-%s", shardNum, newFlowID, flowToken), nil
}

// createWithGraphTx inserts a flow and its entry step in one retryable transaction.
func (e *Engine) createWithGraphTx(ctx context.Context, db *sequel.DB, flowToken, workflowName string, graphJSON, baggageJSON []byte, traceParent string, threadID int, threadToken, entryPoint string, stateJSON []byte, stepToken string, timeBudget time.Duration, opts *workflow.FlowOptions) (int64, error) {
	var newFlowID int64
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		var err error
		newFlowID, err = tx.InsertReturnID(ctx, "flow_id",
			"INSERT INTO dwarf_flows (flow_token, workflow_name, graph, baggage, trace_parent, status, priority, fairness_key, fairness_weight)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
			flowToken, workflowName, string(graphJSON), string(baggageJSON), traceParent, workflow.StatusCreated, opts.Priority, opts.FairnessKey, opts.FairnessWeight,
		)
		if err != nil {
			return errors.Trace(err)
		}

		startDelayMs := int64(0)
		if !opts.StartAt.IsZero() {
			if d := time.Until(opts.StartAt).Milliseconds(); d > 0 {
				startDelayMs = d
			}
		}
		newStepID, err := tx.InsertReturnID(ctx, "step_id",
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, state, status, time_budget_ms, not_before, lease_expires, priority, fairness_key, fairness_weight)"+
				" VALUES (?, 1, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?)",
			newFlowID, stepToken, entryPoint, string(stateJSON), workflow.StatusCreated, timeBudget.Milliseconds(), startDelayMs, leaseMargin.Milliseconds(), opts.Priority, opts.FairnessKey, opts.FairnessWeight,
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
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET thread_id=?, thread_token=?, step_id=?, updated_at=NOW_UTC() WHERE flow_id=?",
			tid, ttok, newStepID, newFlowID,
		)
		return nil
	})
	return newFlowID, errors.Trace(err)
}

// startNotify transitions a created flow to running.
func (e *Engine) startNotify(ctx context.Context, flowKey string, notifyHostname string) error {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	var flowStatus, workflowName string
	var stepID int
	err = db.QueryRowContext(ctx,
		"SELECT status, step_id, workflow_name FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus, &stepID, &workflowName)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	flowStatus = strings.TrimSpace(flowStatus)
	if flowStatus != workflow.StatusCreated {
		return errors.New("flow is not in created status (status: %s)", flowStatus, http.StatusConflict)
	}

	notifyHostname = strings.TrimSpace(notifyHostname)
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if _, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE flow_id=? AND status=?",
			workflow.StatusPending, flowID, workflow.StatusCreated,
		); err != nil {
			return errors.Trace(err)
		}
		// If any of the just-transitioned steps belong to a task whose local breaker is currently
		// tripped, park them at the moment CREATED becomes PENDING so selection never sees them.
		// Skipped entirely when no breakers are tripped (the common case).
		if err := e.parkTrippedSteps(ctx, tx, flowID); err != nil {
			return errors.Trace(err)
		}
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, notify_hostname=?, started_at=NOW_UTC(), updated_at=NOW_UTC() WHERE flow_id=? AND status=?",
			workflow.StatusRunning, notifyHostname, flowID, workflow.StatusCreated,
		)
		if err != nil {
			return errors.Trace(err)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return errors.New("flow is already started", http.StatusConflict)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", flowID, "from", workflow.StatusCreated, "to", workflow.StatusRunning)
	e.metricFlowStarted(ctx, workflowName)

	// Ring the doorbell locally and wake peer replicas: a replica that started the flow but has no spare
	// capacity (or zero workers) must not leave the first step unclaimed until a peer's backstop poll.
	e.enqueueStep(ctx, shardNum, stepID)
	return nil
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
		FlowKey: flowKey,
		Status:  flowStatus,
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
		err = db.QueryRowContext(ctx,
			"SELECT state, changes, interrupt_payload FROM dwarf_steps"+
				" WHERE flow_id=? AND status=? ORDER BY step_depth DESC, step_id DESC LIMIT_OFFSET(1, 0)",
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

// await blocks until a flow stops.
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
// (up to maxPollInterval away). This mirrors foreman, where a single self-inclusive multicast doorbell
// reached both the local replica and its peers. SignalPeers is self-excluded (see its contract), so
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
	if db, err := e.shard(shard); err == nil {
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
	var rootNotifyHostname string
	db.QueryRowContext(ctx, "SELECT notify_hostname FROM dwarf_flows WHERE flow_id=?", surgraphFlowIDs[rootIdx]).Scan(&rootNotifyHostname)
	rootNotifyHostname = strings.TrimSpace(rootNotifyHostname)
	if rootNotifyHostname != "" {
		var finalState map[string]any
		json.Unmarshal([]byte(finalStates[rootIdx]), &finalState)
		e.host.FlowStopped(ctx, rootNotifyHostname, &workflow.FlowOutcome{
			FlowKey:      rootCompositeID,
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
		tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id=?", flowID)
		tx.ExecContext(ctx,
			"DELETE FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND status<>?",
			flowID, flowToken, workflow.StatusRunning,
		)
		return nil
	}))
}

// run creates, starts, and awaits a flow in one call.
func (e *Engine) run(ctx context.Context, workflowName string, initialState any, opts *workflow.FlowOptions) (*workflow.FlowOutcome, error) {
	flowKey, err := e.create(ctx, workflowName, initialState, opts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = e.startNotify(ctx, flowKey, "")
	if err != nil {
		e.cancel(ctx, flowKey, "")
		return nil, errors.Trace(err)
	}
	outcome, err := e.await(ctx, flowKey)
	if err != nil {
		e.cancel(ctx, flowKey, "")
		return nil, errors.Trace(err)
	}
	return outcome, nil
}
