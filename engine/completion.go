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
	"net/http"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// createSubgraphFlow creates a subgraph flow for a dynamic subgraph transition.
func (e *Engine) createSubgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, surgraphStepDepth int, surgraphStepID int, subgraphWorkflowName string, subgraphGraph *workflow.Graph, childState map[string]any, metadataJSON string, breakpointsJSON string) (string, error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}

	var inherited workflow.FlowOptions
	err = db.QueryRowContext(ctx,
		"SELECT priority, fairness_key, fairness_weight FROM dwarf_flows WHERE flow_id=?",
		surgraphFlowID,
	).Scan(&inherited.Priority, &inherited.FairnessKey, &inherited.FairnessWeight)
	if err != nil {
		return "", errors.Trace(err)
	}

	subgraphFlowKey, err := e.createWithGraph(ctx, shardNum, subgraphWorkflowName, subgraphGraph, childState, nil, 0, "", &inherited)
	if err != nil {
		return "", errors.Trace(err)
	}
	_, subgraphFlowID, _, err := parseFlowKey(subgraphFlowKey)
	if err != nil {
		return "", errors.Trace(err)
	}

	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET surgraph_flow_id=?, surgraph_step_depth=?, surgraph_step_id=?, actor_claims=?, breakpoints=?, updated_at=NOW_UTC() WHERE flow_id=?",
		surgraphFlowID, surgraphStepDepth, surgraphStepID, metadataJSON, breakpointsJSON, subgraphFlowID,
	)
	if err != nil {
		return "", errors.Trace(err)
	}

	return subgraphFlowKey, nil
}

// completeFlowSequential marks a flow completed when no successor exists.
func (e *Engine) completeFlowSequential(ctx context.Context, shardNum int, db *sequel.DB, flowID int, flowToken string, stepID int, notifyHostname, workflowName string) error {
	e.logger.DebugContext(ctx, "Flow completed", "flow", workflowName)
	_, err := e.completeFlow(ctx, shardNum, flowID, flowToken, notifyHostname)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(db.Transact(ctx, func(tx *sequel.Tx) error {
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, updated_at=NOW_UTC() WHERE step_id=?",
			workflow.StatusCompleted, stepID,
		)
		return nil
	}))
}

// mergeTerminalSteps computes a flow's terminal state from the execution-DAG tail.
func (e *Engine) mergeTerminalSteps(ctx context.Context, db sequel.Executor, flowID int, reducers map[string]workflow.Reducer) (map[string]any, error) {
	merge := func(query string, args ...any) (map[string]any, bool, error) {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, false, errors.Trace(err)
		}
		defer rows.Close()

		var baseState map[string]any
		var allChanges []map[string]any
		found := false
		for rows.Next() {
			found = true
			var stateJSON, changesJSON string
			err := rows.Scan(&stateJSON, &changesJSON)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			if baseState == nil {
				if err := json.Unmarshal([]byte(stateJSON), &baseState); err != nil {
					return nil, false, errors.Trace(err)
				}
			}
			var changes map[string]any
			if err := json.Unmarshal([]byte(changesJSON), &changes); err != nil {
				return nil, false, errors.Trace(err)
			}
			allChanges = append(allChanges, changes)
		}
		if err = rows.Err(); err != nil {
			return nil, false, errors.Trace(err)
		}
		if !found {
			return nil, false, nil
		}

		merged := baseState
		for _, changes := range allChanges {
			merged, err = workflow.MergeState(merged, changes, reducers)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
		}
		if merged == nil {
			merged = map[string]any{}
		}
		return merged, true, nil
	}

	merged, found, err := merge(
		"SELECT state, changes FROM dwarf_steps WHERE flow_id=? AND successor_id=0 AND status=? ORDER BY step_id",
		flowID, workflow.StatusCompleted,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if found {
		return merged, nil
	}

	merged, found, err = merge(
		"SELECT state, changes FROM dwarf_steps WHERE flow_id=? AND successor_id=0 ORDER BY step_id",
		flowID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if found {
		return merged, nil
	}
	return map[string]any{}, nil
}

// computeFinalState computes the merged state for a flow.
func (e *Engine) computeFinalState(ctx context.Context, db sequel.Executor, flowID int) (string, string, error) {
	var graphJSON, workflowName string
	err := db.QueryRowContext(ctx,
		"SELECT graph, workflow_name FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&graphJSON, &workflowName)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	var graph workflow.Graph
	err = json.Unmarshal([]byte(graphJSON), &graph)
	if err != nil {
		return "", "", errors.Trace(err)
	}

	merged, err := e.mergeTerminalSteps(ctx, db, flowID, graph.Reducers())
	if err != nil {
		return "", "", errors.Trace(err)
	}

	data, err := json.Marshal(merged)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	return string(data), workflowName, nil
}

// completeFlow transitions a flow to completed and propagates to surgraph.
func (e *Engine) completeFlow(ctx context.Context, shardNum int, flowID int, flowToken string, notifyHostname string) (bool, error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}
	var finalStateJSON string
	completed := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		completed = false
		fs, _, err := e.computeFinalState(ctx, tx, flowID)
		if err != nil {
			return errors.Trace(err)
		}
		finalStateJSON = fs
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, final_state=?, updated_at=NOW_UTC() WHERE flow_id=? AND status NOT IN (?, ?, ?)",
			workflow.StatusCompleted, finalStateJSON, flowID,
			workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled,
		)
		if err != nil {
			return errors.Trace(err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			completed = true
		}
		return nil
	})
	if err != nil {
		return false, errors.Trace(err)
	}
	if !completed {
		return false, nil
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", flowID, "to", workflow.StatusCompleted)
	compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)
	notifyHostname = strings.TrimSpace(notifyHostname)
	if notifyHostname != "" && e.flowStoppedCallback != nil {
		var finalState map[string]any
		json.Unmarshal([]byte(finalStateJSON), &finalState)
		e.flowStoppedCallback(ctx, notifyHostname, &workflow.FlowOutcome{
			FlowKey: compositeID,
			Status:  workflow.StatusCompleted,
			State:   finalState,
		})
	}
	e.signalStop(ctx, compositeID, workflow.StatusCompleted)
	if e.peerNotifier != nil {
		e.peerNotifier.Enqueue(ctx, 0, 0) // Wake peers
	}

	// Propagate to surgraph
	var surgraphFlowID, surgraphStepDepth, surgraphStepID int
	err = db.QueryRowContext(ctx,
		"SELECT surgraph_flow_id, surgraph_step_depth, surgraph_step_id FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&surgraphFlowID, &surgraphStepDepth, &surgraphStepID)
	if err != nil {
		return true, errors.Trace(err)
	}
	if surgraphFlowID != 0 {
		if err := e.completeSurgraphFlow(ctx, shardNum, surgraphFlowID, surgraphStepDepth, surgraphStepID, finalStateJSON); err != nil {
			return true, errors.Trace(err)
		}
	}

	return true, nil
}

// completeSurgraphFlow re-dispatches a parked surgraph step after its child completes.
func (e *Engine) completeSurgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, surgraphStepDepth int, surgraphStepID int, subgraphFinalStateJSON string) error {
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	resultJSON := subgraphFinalStateJSON
	if strings.TrimSpace(resultJSON) == "" {
		resultJSON = "{}"
	}
	reDispatch := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		reDispatch = false
		// Guard the revive on the exact park state (running + parkedSubgraph), mirroring
		// deliverSubgraphError. Without it, a Cancel that cascaded to this caller step (between the child's
		// completion and this revive) would be resurrected to pending: keying on step_id alone overwrites
		// the just-cancelled row. The guard also subsumes the "step still live" check — a step that is no
		// longer running/parked matches no row — and the rows-affected gate keeps Enqueue off a no-op.
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=1, subgraph_result=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=? AND parked=?",
			workflow.StatusPending, parkedNone, resultJSON, surgraphStepID, workflow.StatusRunning, parkedSubgraph,
		)
		if err != nil {
			return errors.Trace(err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			reDispatch = true
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if reDispatch {
		e.logger.DebugContext(ctx, "Resuming surgraph task after subgraph flow completion",
			"surgraphFlow", surgraphFlowID, "surgraphStep", surgraphStepDepth)
		e.enqueueStep(ctx, shardNum, surgraphStepID)
	}
	return nil
}

// failStep handles a task failure.
func (e *Engine) failStep(ctx context.Context, shardNum int, stepID int, flowID int, flowToken string, taskErr error, taskName string) error {
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	// Check if this is a dynamic subgraph child
	parentStepID, isDynamic, err := e.dynamicSubgraphParent(ctx, db, flowID)
	if err != nil {
		return errors.Trace(err)
	}
	if isDynamic {
		return e.deliverSubgraphError(ctx, shardNum, stepID, flowID, parentStepID, taskErr)
	}

	var stepLineageID int
	err = db.QueryRowContext(ctx,
		"SELECT lineage_id FROM dwarf_steps WHERE step_id=?",
		stepID,
	).Scan(&stepLineageID)
	if err != nil {
		return errors.Trace(err)
	}

	errMsg := taskErr.Error()
	failFlow := false
	var finalStateJSON string
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		failFlow = stepLineageID == 0
		finalStateJSON = ""
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, error=?, updated_at=NOW_UTC() WHERE step_id=?",
			workflow.StatusFailed, parkedNone, errMsg, stepID,
		)
		if !failFlow {
			var err error
			failFlow, err = e.propagateCohortFailure(ctx, tx, stepLineageID)
			if err != nil {
				return errors.Trace(err)
			}
		}
		if failFlow {
			var err error
			finalStateJSON, _, err = e.computeFinalState(ctx, tx, flowID)
			if err != nil {
				return errors.Trace(err)
			}
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET final_state=?, status=?, error=?, updated_at=NOW_UTC() WHERE flow_id=? AND status NOT IN (?, ?, ?)",
				finalStateJSON, workflow.StatusFailed, errMsg, flowID,
				workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled,
			)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	if !failFlow {
		return nil
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", flowID, "to", workflow.StatusFailed)
	compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, strings.TrimSpace(flowToken))
	var notifyHostname string
	db.QueryRowContext(ctx, "SELECT notify_hostname FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&notifyHostname)
	notifyHostname = strings.TrimSpace(notifyHostname)
	if notifyHostname != "" && e.flowStoppedCallback != nil {
		var finalState map[string]any
		json.Unmarshal([]byte(finalStateJSON), &finalState)
		e.flowStoppedCallback(ctx, notifyHostname, &workflow.FlowOutcome{
			FlowKey: compositeID,
			Status:  workflow.StatusFailed,
			State:   finalState,
			Error:   errMsg,
		})
	}
	e.signalStop(ctx, compositeID, workflow.StatusFailed)
	return nil
}

// propagateCohortFailure bumps a spawn step's cohort_arrivals and cohort_failures.
func (e *Engine) propagateCohortFailure(ctx context.Context, tx sequel.Executor, spawnStepID int) (bool, error) {
	current := spawnStepID
	for {
		_, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals + 1, cohort_failures = cohort_failures + 1 WHERE step_id=?",
			current,
		)
		if err != nil {
			return false, errors.Trace(err)
		}
		var arrivals, size, lineageID int
		err = tx.QueryRowContext(ctx,
			"SELECT cohort_arrivals, cohort_size, lineage_id FROM dwarf_steps WHERE step_id=?",
			current,
		).Scan(&arrivals, &size, &lineageID)
		if err != nil {
			return false, errors.Trace(err)
		}
		if arrivals < size {
			return false, nil
		}
		if lineageID == 0 {
			return true, nil
		}
		current = lineageID
	}
}

// dynamicSubgraphParent reports whether the given flow is a subgraph child.
func (e *Engine) dynamicSubgraphParent(ctx context.Context, db *sequel.DB, flowID int) (int, bool, error) {
	var surgraphFlowID, surgraphStepID int
	err := db.QueryRowContext(ctx,
		"SELECT surgraph_flow_id, surgraph_step_id FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&surgraphFlowID, &surgraphStepID)
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	if surgraphFlowID == 0 || surgraphStepID == 0 {
		return 0, false, nil
	}
	return surgraphStepID, true, nil
}

// deliverSubgraphError fails a dynamic subgraph child and re-dispatches the parent.
func (e *Engine) deliverSubgraphError(ctx context.Context, shardNum int, childStepID int, childFlowID int, parentStepID int, taskErr error) error {
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	errMsg := taskErr.Error()
	reDispatchParent := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		reDispatchParent = false
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, error=?, updated_at=NOW_UTC() WHERE step_id=?",
			workflow.StatusFailed, parkedNone, errMsg, childStepID,
		)
		childFinalState, _, _ := e.computeFinalState(ctx, tx, childFlowID)
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, error=?, final_state=?, updated_at=NOW_UTC() WHERE flow_id=? AND status NOT IN (?, ?, ?)",
			workflow.StatusFailed, errMsg, childFinalState, childFlowID, workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled,
		)
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=1, subgraph_error=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=? AND parked=?",
			workflow.StatusPending, parkedNone, errMsg, parentStepID, workflow.StatusRunning, parkedSubgraph,
		)
		if err != nil {
			return errors.Trace(err)
		}
		if n, _ := res.RowsAffected(); n > 0 {
			reDispatchParent = true
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if reDispatchParent {
		e.enqueueStep(ctx, shardNum, parentStepID)
	}
	return nil
}

// allSubgraphFlows iteratively finds all active descendant subgraph flows.
func (e *Engine) allSubgraphFlows(ctx context.Context, shardNum int, flowID int) (flowIDs []any, compositeFlowIDs []string, err error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	current := []any{flowID}
	for len(current) > 0 {
		placeholders := strings.Repeat("?,", len(current)-1) + "?"
		args := append([]any{}, current...)
		args = append(args, workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled)
		rows, err := db.QueryContext(ctx,
			"SELECT flow_id, flow_token FROM dwarf_flows WHERE surgraph_flow_id IN ("+placeholders+") AND status NOT IN (?, ?, ?)",
			args...,
		)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		current = nil
		for rows.Next() {
			var id int
			var token string
			rows.Scan(&id, &token)
			flowIDs = append(flowIDs, id)
			compositeFlowIDs = append(compositeFlowIDs, fmt.Sprintf("%d-%d-%s", shardNum, id, strings.TrimSpace(token)))
			current = append(current, id)
		}
		rows.Close()
	}
	return flowIDs, compositeFlowIDs, nil
}

// interruptedSubgraphChain walks down from a flow through interrupted subgraph steps to find the leaf.
func (e *Engine) interruptedSubgraphChain(ctx context.Context, shardNum int, flowID int, flowToken string) (flowIDs []any, stepIDs []any, compositeFlowIDs []string, err error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	flowIDs = []any{flowID}
	compositeFlowIDs = []string{fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)}

	currentFlowID := flowID
	for {
		var stepID, stepDepth int
		err = db.QueryRowContext(ctx,
			"SELECT step_id, step_depth FROM dwarf_steps WHERE flow_id=? AND status=? ORDER BY updated_at LIMIT_OFFSET(1, 0)",
			currentFlowID, workflow.StatusInterrupted,
		).Scan(&stepID, &stepDepth)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		stepIDs = append(stepIDs, stepID)

		var subFlowID int
		var subFlowToken string
		err = db.QueryRowContext(ctx,
			"SELECT flow_id, flow_token FROM dwarf_flows WHERE surgraph_flow_id=? AND surgraph_step_depth=? AND status=?",
			currentFlowID, stepDepth, workflow.StatusInterrupted,
		).Scan(&subFlowID, &subFlowToken)
		if err == sql.ErrNoRows {
			return flowIDs, stepIDs, compositeFlowIDs, nil
		}
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}

		subFlowToken = strings.TrimSpace(subFlowToken)
		flowIDs = append(flowIDs, subFlowID)
		compositeFlowIDs = append(compositeFlowIDs, fmt.Sprintf("%d-%d-%s", shardNum, subFlowID, subFlowToken))
		currentFlowID = subFlowID
	}
}

// resume is the shared machinery for Resume (interrupt) and ResumeBreak.
func (e *Engine) resume(ctx context.Context, flowKey string, breakpoint bool, data any) error {
	shardNum, flowID, flowToken, err := parseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	var flowStatus string
	err = db.QueryRowContext(ctx, "SELECT status FROM dwarf_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&flowStatus)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	flowStatus = strings.TrimSpace(flowStatus)
	if flowStatus != workflow.StatusInterrupted {
		return errors.New("flow is not interrupted (status: %s)", flowStatus, http.StatusConflict)
	}

	upFlowIDs, upStepIDs, upCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}
	downFlowIDs, downStepIDs, downCompositeIDs, err := e.interruptedSubgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	chainFlowIDs := append([]any{}, upFlowIDs...)
	chainCompositeIDs := append([]string{}, upCompositeIDs...)
	chainFlowIDs = append(chainFlowIDs, downFlowIDs[1:]...)
	chainCompositeIDs = append(chainCompositeIDs, downCompositeIDs[1:]...)

	leafStepID := downStepIDs[len(downStepIDs)-1]
	parkStepIDs := append([]any{}, upStepIDs...)
	parkStepIDs = append(parkStepIDs, downStepIDs[:len(downStepIDs)-1]...)

	var leafInterruptDone, leafBreakpointHit bool
	var leafStateJSON string
	db.QueryRowContext(ctx, "SELECT interrupt_done, breakpoint_hit, state FROM dwarf_steps WHERE step_id=?", leafStepID).Scan(&leafInterruptDone, &leafBreakpointHit, &leafStateJSON)
	if breakpoint && (leafInterruptDone || !leafBreakpointHit) {
		return errors.New("flow is not paused at a breakpoint; use Resume to continue an interrupt", http.StatusConflict)
	}
	if !breakpoint && !leafInterruptDone {
		return errors.New("flow is not paused at an interrupt; use ResumeBreak to continue a breakpoint", http.StatusConflict)
	}

	resumeDataJSON := "{}"
	breakpointStateJSON := leafStateJSON
	if breakpoint {
		var leafState map[string]any
		json.Unmarshal([]byte(leafStateJSON), &leafState)
		merged, _ := workflow.MergeState(leafState, data, nil)
		mergedJSON, _ := json.Marshal(merged)
		breakpointStateJSON = string(mergedJSON)
	} else if data != nil {
		b, _ := json.Marshal(data)
		var resumeMap map[string]any
		json.Unmarshal(b, &resumeMap)
		if len(resumeMap) > 0 {
			resumeDataJSON = string(b)
		}
	}

	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		allStepIDs := append([]any{leafStepID}, parkStepIDs...)
		clearPlaceholders := strings.Repeat("?,", len(allStepIDs)-1) + "?"
		tx.ExecContext(ctx, "UPDATE dwarf_steps SET interrupt_payload='{}' WHERE step_id IN ("+clearPlaceholders+")", allStepIDs...)

		if len(parkStepIDs) > 0 {
			parkPlaceholders := strings.Repeat("?,", len(parkStepIDs)-1) + "?"
			parkArgs := append([]any{workflow.StatusRunning, parkedSubgraph}, parkStepIDs...)
			parkArgs = append(parkArgs, workflow.StatusInterrupted)
			tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE step_id IN ("+parkPlaceholders+") AND status=?", parkArgs...)
		}

		if breakpoint {
			tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, state=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
				workflow.StatusPending, breakpointStateJSON, leafStepID, workflow.StatusInterrupted)
		} else {
			tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, resume_data=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
				workflow.StatusPending, resumeDataJSON, leafStepID, workflow.StatusInterrupted)
		}

		for _, chainFlowID := range chainFlowIDs {
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET status=?, updated_at=NOW_UTC() WHERE flow_id=? AND status=? AND (SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status=?)=0",
				workflow.StatusRunning, chainFlowID, workflow.StatusInterrupted, chainFlowID, workflow.StatusInterrupted,
			)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	for _, compositeID := range chainCompositeIDs {
		e.notifyStatusChange(compositeID, workflow.StatusRunning)
	}
	e.enqueueStep(ctx, shardNum, leafStepID.(int))
	return nil
}

// surgraphChain walks from a flow up to the root surgraph.
func (e *Engine) surgraphChain(ctx context.Context, shardNum int, flowID int, flowToken string) (flowIDs []any, stepIDs []any, compositeFlowIDs []string, err error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	flowIDs = []any{flowID}
	compositeFlowIDs = []string{fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)}

	currentFlowID := flowID
	for {
		var surgraphFlowID, surgraphStepDepth, surgraphStepID int
		var wfName string
		err = db.QueryRowContext(ctx,
			"SELECT surgraph_flow_id, surgraph_step_depth, surgraph_step_id, workflow_name FROM dwarf_flows WHERE flow_id=?",
			currentFlowID,
		).Scan(&surgraphFlowID, &surgraphStepDepth, &surgraphStepID, &wfName)
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		if surgraphFlowID == 0 {
			break
		}
		var surgraphFlowToken string
		db.QueryRowContext(ctx,
			"SELECT flow_token FROM dwarf_flows WHERE flow_id=?",
			surgraphFlowID,
		).Scan(&surgraphFlowToken)
		if surgraphStepID == 0 {
			db.QueryRowContext(ctx,
				"SELECT step_id FROM dwarf_steps WHERE flow_id=? AND step_depth=? AND task_name=?",
				surgraphFlowID, surgraphStepDepth, strings.TrimSpace(wfName),
			).Scan(&surgraphStepID)
		}
		flowIDs = append(flowIDs, surgraphFlowID)
		stepIDs = append(stepIDs, surgraphStepID)
		compositeFlowIDs = append(compositeFlowIDs, fmt.Sprintf("%d-%d-%s", shardNum, surgraphFlowID, strings.TrimSpace(surgraphFlowToken)))
		currentFlowID = surgraphFlowID
	}
	return flowIDs, stepIDs, compositeFlowIDs, nil
}
