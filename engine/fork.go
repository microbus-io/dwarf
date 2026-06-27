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

// forkFlow clones a terminal flow's execution tree up to a chosen step into a brand-new, self-contained
// root flow, then re-runs from that step with optional state overrides. The fork point may be ANY recorded
// step - in the root flow or deep inside a subgraph. The original is never mutated (terminal flows are
// immutable); recovery/exploration is non-destructive.
//
// The design (copy-only-keep clone, re-parking ancestor callers up the surgraph chain, the created->pending
// crash-gate, and uniform scheduling resolution) is documented in CLAUDE.md, "Fork".
func (e *Engine) forkFlow(ctx context.Context, stepKey string, stateOverrides any) (string, error) {
	shardNum, forkStepID, forkStepToken, err := parseStepKey(stepKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	db, err := e.shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}

	// Resolve the fork step, its owning flow, and that flow's token (needed to walk the surgraph chain).
	var leafFlowID int
	var forkStepState, leafFlowToken string
	err = db.QueryRowContext(ctx,
		"SELECT s.flow_id, s.state, f.flow_token FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.step_id=? AND s.step_token=?",
		forkStepID, forkStepToken,
	).Scan(&leafFlowID, &forkStepState, &leafFlowToken)
	if err == sql.ErrNoRows {
		return "", errors.New("step not found", http.StatusNotFound)
	}
	if err != nil {
		return "", errors.Trace(err)
	}

	// Walk up to the root, recording each flow's rewind step: the leaf fork step in its own flow, and the
	// caller step that spawned the lower flow in each ancestor.
	chainFlowIDs, chainStepIDs, _, err := e.surgraphChain(ctx, shardNum, leafFlowID, strings.TrimSpace(leafFlowToken))
	if err != nil {
		return "", errors.Trace(err)
	}
	rootFlowID := chainFlowIDs[len(chainFlowIDs)-1].(int)
	rewindByFlow := map[int]int{leafFlowID: forkStepID}
	for i, callerStep := range chainStepIDs {
		rewindByFlow[chainFlowIDs[i+1].(int)] = callerStep.(int)
	}

	// Validate the root (the fork's identity) is terminal and gather the root-flow overrides.
	var rootStatus, rootWorkflowURL, rootThreadToken string
	var rootThreadID, rootPriority, rootTimeBudgetMs int
	var rootFairnessKey string
	var rootFairnessWeight float64
	err = db.QueryRowContext(ctx,
		"SELECT status, workflow_url, thread_id, thread_token, priority, fairness_key, fairness_weight, time_budget_ms FROM dwarf_flows WHERE flow_id=?",
		rootFlowID,
	).Scan(&rootStatus, &rootWorkflowURL, &rootThreadID, &rootThreadToken, &rootPriority, &rootFairnessKey, &rootFairnessWeight, &rootTimeBudgetMs)
	if err != nil {
		return "", errors.Trace(err)
	}
	rootStatus = strings.TrimSpace(rootStatus)
	if rootStatus != workflow.StatusCompleted && rootStatus != workflow.StatusFailed && rootStatus != workflow.StatusCancelled {
		return "", errors.New("can only fork a terminal flow (status: %s)", rootStatus, http.StatusConflict)
	}

	mergedLeafState, err := mergeWithOverrides(forkStepState, stateOverrides)
	if err != nil {
		return "", errors.Trace(err)
	}

	// The fork inherits the origin root's scheduling and baggage (no FlowOptions), applied uniformly to
	// every cloned flow and step.
	cc := &forkClone{
		leafFlowID:      leafFlowID,
		leafStepID:      forkStepID,
		mergedLeafState: mergedLeafState,
		rewindByFlow:    rewindByFlow,
		rootFlowToken:   randomIdentifier(16),
		rootTraceParent: e.mintWorkflowSpan(ctx, rootWorkflowURL, ""), // detached, like Continue
		threadID:        rootThreadID,
		threadToken:     strings.TrimSpace(rootThreadToken),
		priority:        rootPriority,
		fairnessKey:     rootFairnessKey,
		fairnessWeight:  rootFairnessWeight,
		timeBudgetMs:    rootTimeBudgetMs,
	}

	var newRootFlowID int
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		cc.newLeafStepID = 0
		id, cloneErr := e.cloneSubtree(ctx, tx, cc, rootFlowID, 0, 0, true)
		if cloneErr != nil {
			return errors.Trace(cloneErr)
		}
		newRootFlowID = id
		// Mapping complete - flip the gated leaf step created->pending. The flow chain is already running.
		_, txErr := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
			workflow.StatusPending, cc.newLeafStepID, workflow.StatusCreated,
		)
		return errors.Trace(txErr)
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	newFlowKey := fmt.Sprintf("%d-%d-%s", shardNum, newRootFlowID, cc.rootFlowToken)
	e.logger.InfoContext(ctx, "Flow forked", "fromRoot", rootFlowID, "forkStep", forkStepID, "to", newRootFlowID)
	e.notifyStatusChange(newFlowKey, workflow.StatusRunning)
	e.enqueueStep(ctx, shardNum, cc.newLeafStepID)
	return newFlowKey, nil
}

// forkClone carries cross-recursion state for a Fork clone.
type forkClone struct {
	leafFlowID      int
	leafStepID      int
	mergedLeafState string
	rewindByFlow    map[int]int
	rootFlowToken   string
	rootTraceParent string
	threadID        int
	threadToken     string
	priority        int
	fairnessKey     string
	fairnessWeight  float64
	timeBudgetMs    int
	newLeafStepID   int
}

// cloneSubtree clones originFlowID into a new flow under the given new-surgraph context, then recurses into
// its kept subgraph-caller children. Returns the new flow id. A flow on the rewind chain keeps everything
// above its rewind step and is set running; an off-path completed-prefix subgraph (rewind 0) is cloned
// whole and keeps its status.
func (e *Engine) cloneSubtree(ctx context.Context, tx *sequel.Tx, cc *forkClone, originFlowID, newSurgFlowID, newSurgStepID int, isRoot bool) (int, error) {
	rewind := cc.rewindByFlow[originFlowID] // 0 => full clone (off-path completed prefix subgraph)

	pruned := map[int]bool{}
	if rewind != 0 {
		sub, err := e.collectDAGSubtree(ctx, tx, originFlowID, rewind)
		if err != nil {
			return 0, errors.Trace(err)
		}
		for _, m := range sub {
			pruned[m.stepID] = true
		}
	}

	var status, workflowURL, workflowName, graphJSON, baggageJSON, traceParent string
	var notifyOnStop, deleteOnCompletion int
	err := tx.QueryRowContext(ctx,
		"SELECT status, workflow_url, workflow_name, graph, baggage, trace_parent, notify_on_stop, delete_on_completion FROM dwarf_flows WHERE flow_id=?",
		originFlowID,
	).Scan(&status, &workflowURL, &workflowName, &graphJSON, &baggageJSON, &traceParent, &notifyOnStop, &deleteOnCompletion)
	if err != nil {
		return 0, errors.Trace(err)
	}
	status = strings.TrimSpace(status)

	newStatus := status
	if isRoot || rewind != 0 { // on the rewind chain
		newStatus = workflow.StatusRunning
	}
	// Scheduling (cc.*) is resolved once and applied uniformly to every cloned flow and step; see
	// CLAUDE.md, "Fork", for why (uniform tree + deep-subgraph leaf carries the override).
	forkedFromStep, newTrace := 0, traceParent
	flowPriority, flowFairnessKey, flowFairnessWeight, flowBudget := cc.priority, cc.fairnessKey, cc.fairnessWeight, cc.timeBudgetMs
	if isRoot {
		newStatus = workflow.StatusCreated // gate; flipped to running below once this flow is mapped
		forkedFromStep, newTrace = cc.leafStepID, cc.rootTraceParent
		notifyOnStop, deleteOnCompletion = 0, 0
	}

	newFlowID64, err := tx.InsertReturnID(ctx, "flow_id",
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, baggage, status, surgraph_flow_id, surgraph_step_id, forked_from_step, trace_parent, notify_on_stop, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		randomIdentifier(16), workflowURL, workflowName, graphJSON, baggageJSON, newStatus, newSurgFlowID, newSurgStepID, forkedFromStep, newTrace, notifyOnStop, deleteOnCompletion, flowPriority, flowFairnessKey, flowFairnessWeight, flowBudget,
	)
	if err != nil {
		return 0, errors.Trace(err)
	}
	newFlowID := int(newFlowID64)
	if isRoot {
		// The root's flow_token must match the key returned to the caller.
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_flows SET flow_token=?, thread_id=?, thread_token=? WHERE flow_id=?",
			cc.rootFlowToken, cc.threadID, cc.threadToken, newFlowID)
		if err != nil {
			return 0, errors.Trace(err)
		}
	} else {
		// Subgraph flows are their own thread.
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_flows SET thread_id=? WHERE flow_id=?", newFlowID, newFlowID)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	// Direct subgraph children of this flow, keyed by caller step (latest child per caller).
	childByCaller := map[int]int{}
	crows, err := tx.QueryContext(ctx, "SELECT flow_id, surgraph_step_id FROM dwarf_flows WHERE surgraph_flow_id=? ORDER BY flow_id", originFlowID)
	if err != nil {
		return 0, errors.Trace(err)
	}
	for crows.Next() {
		var cFlow, cCaller int
		crows.Scan(&cFlow, &cCaller)
		childByCaller[cCaller] = cFlow
	}
	crows.Close()

	type stepMeta struct {
		oldID, predID, succID, lineageID, cohortSize int
		status                                       string
	}
	mrows, err := tx.QueryContext(ctx,
		"SELECT step_id, predecessor_id, successor_id, lineage_id, cohort_size, status FROM dwarf_steps WHERE flow_id=? ORDER BY step_id",
		originFlowID,
	)
	if err != nil {
		return 0, errors.Trace(err)
	}
	var keep []stepMeta
	for mrows.Next() {
		var s stepMeta
		mrows.Scan(&s.oldID, &s.predID, &s.succID, &s.lineageID, &s.cohortSize, &s.status)
		if pruned[s.oldID] {
			continue
		}
		s.status = strings.TrimSpace(s.status)
		keep = append(keep, s)
	}
	mrows.Close()

	// Copy kept steps (all columns DB-side, native timestamps), overriding flow_id, a fresh token, and the
	// flow's scheduling. The leaf fork step is inserted `created` (gated); all others keep their status.
	idMap := make(map[int]int, len(keep))
	isLeafFlow := originFlowID == cc.leafFlowID
	for _, s := range keep {
		var newID int64
		if isLeafFlow && s.oldID == cc.leafStepID {
			newID, err = tx.InsertReturnID(ctx, "step_id",
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, changes, interrupt_payload, status, goto_next, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, priority, fairness_key, fairness_weight, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at)"+
					" SELECT ?, step_depth, ?, task_name, task_url, state, changes, interrupt_payload, ?, goto_next, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, ?, ?, ?, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at FROM dwarf_steps WHERE step_id=?",
				newFlowID, randomIdentifier(16), workflow.StatusCreated, flowPriority, flowFairnessKey, flowFairnessWeight, s.oldID,
			)
		} else {
			newID, err = tx.InsertReturnID(ctx, "step_id",
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, changes, interrupt_payload, status, goto_next, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, priority, fairness_key, fairness_weight, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at)"+
					" SELECT ?, step_depth, ?, task_name, task_url, state, changes, interrupt_payload, status, goto_next, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, ?, ?, ?, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at FROM dwarf_steps WHERE step_id=?",
				newFlowID, randomIdentifier(16), flowPriority, flowFairnessKey, flowFairnessWeight, s.oldID,
			)
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
		idMap[s.oldID] = int(newID)
	}

	// Remap intra-flow references (a ref to a pruned/absent step -> 0) and stamp the resolved time budget
	// (kept uniform with priority/fairness; the INSERT...SELECT copied the source step's budget).
	for _, s := range keep {
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET predecessor_id=?, successor_id=?, lineage_id=?, time_budget_ms=? WHERE step_id=?",
			idMap[s.predID], idMap[s.succID], idMap[s.lineageID], flowBudget, idMap[s.oldID],
		)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	// Recompute cohort counters on cloned spawns from cloned members' terminal states, excluding this flow's
	// rewind step (the re-run / re-parked branch).
	membersByLineage := map[int][]stepMeta{}
	for _, s := range keep {
		if s.lineageID != 0 {
			membersByLineage[s.lineageID] = append(membersByLineage[s.lineageID], s)
		}
	}
	for _, s := range keep {
		if s.cohortSize == 0 {
			continue
		}
		arrivals, failures := 0, 0
		for _, m := range membersByLineage[s.oldID] {
			if m.oldID == rewind {
				continue
			}
			switch m.status {
			case workflow.StatusCompleted, workflow.StatusCancelled:
				arrivals++
			case workflow.StatusFailed:
				arrivals++
				failures++
			}
		}
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_arrivals=?, cohort_failures=? WHERE step_id=?", arrivals, failures, idMap[s.oldID])
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	// Recurse into kept subgraph-caller children, skipping the leaf fork step (it re-spawns a fresh child).
	for _, s := range keep {
		childFlow, ok := childByCaller[s.oldID]
		if !ok || (isLeafFlow && s.oldID == cc.leafStepID) {
			continue
		}
		if _, err = e.cloneSubtree(ctx, tx, cc, childFlow, newFlowID, idMap[s.oldID], false); err != nil {
			return 0, errors.Trace(err)
		}
	}

	// Apply the rewind treatment.
	if rewind != 0 {
		newRewindID := idMap[rewind]
		if isLeafFlow && rewind == cc.leafStepID {
			// Leaf fork step: merged input, cleared output/park/cohort, gated `created`.
			_, err = tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, parked=?, state=?, changes='{}', error='', goto_next='', attempt=0, interrupt_done=0, resume_data='{}', subgraph_done=0, subgraph_result='{}', subgraph_error='', successor_id=0, cohort_size=0, cohort_arrivals=0, cohort_failures=0, not_before=NOW_UTC(), lease_expires=NOW_UTC(), created_at=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
				workflow.StatusCreated, parkedNone, cc.mergedLeafState, newRewindID,
			)
			cc.newLeafStepID = newRewindID
		} else {
			// Ancestor caller: re-park so completeSurgraphFlow revives it when the re-run child completes.
			_, err = tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=0, subgraph_result='{}', subgraph_error='', successor_id=0, error='', goto_next='', not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
				workflow.StatusRunning, parkedSubgraph, newRewindID,
			)
		}
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	if isRoot {
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, step_id=?, started_at=NOW_UTC(), updated_at=NOW_UTC() WHERE flow_id=?",
			workflow.StatusRunning, idMap[rewind], newFlowID,
		)
		if err != nil {
			return 0, errors.Trace(err)
		}
	}

	return newFlowID, nil
}

func mergeWithOverrides(originalJSON string, overrides any) (string, error) {
	var state map[string]any
	if originalJSON != "" && originalJSON != "{}" {
		json.Unmarshal([]byte(originalJSON), &state)
	}
	if state == nil {
		state = map[string]any{}
	}
	if overrides == nil {
		out, _ := json.Marshal(state)
		return string(out), nil
	}
	overridesJSON, err := json.Marshal(overrides)
	if err != nil {
		return "", errors.Trace(err)
	}
	var ov map[string]any
	err = json.Unmarshal(overridesJSON, &ov)
	if err != nil {
		return "", errors.Trace(err)
	}
	for k, v := range ov {
		if v == nil {
			delete(state, k)
		} else {
			state[k] = v
		}
	}
	out, _ := json.Marshal(state)
	return string(out), nil
}

type sweptMember struct {
	stepID    int
	lineageID int
	status    string
}

func (e *Engine) collectDAGSubtree(ctx context.Context, db sequel.Executor, flowID, startStepID int) ([]sweptMember, error) {
	visited := map[int]bool{startStepID: true}
	var collected []sweptMember
	frontier := []any{startStepID}
	for len(frontier) > 0 {
		ph := strings.Repeat("?,", len(frontier)-1) + "?"
		args := append([]any{flowID}, frontier...)
		query := "SELECT step_id, lineage_id, status FROM dwarf_steps WHERE flow_id=? AND (" +
			"step_id IN (SELECT successor_id FROM dwarf_steps WHERE step_id IN (" + ph + ") AND successor_id<>0)" +
			" OR predecessor_id IN (" + ph + "))"
		args = append(args, frontier...)
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var nextFrontier []any
		for rows.Next() {
			var sid, lid int
			var status string
			rows.Scan(&sid, &lid, &status)
			if visited[sid] {
				continue
			}
			visited[sid] = true
			collected = append(collected, sweptMember{stepID: sid, lineageID: lid, status: strings.TrimSpace(status)})
			nextFrontier = append(nextFrontier, sid)
		}
		rows.Close()
		frontier = nextFrontier
	}
	return collected, nil
}

func (e *Engine) allDescendantSubgraphFlows(ctx context.Context, db sequel.Executor, flowID int) ([]int, error) {
	var collected []int
	current := []any{flowID}
	for len(current) > 0 {
		ph := strings.Repeat("?,", len(current)-1) + "?"
		rows, err := db.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id IN ("+ph+")", current...)
		if err != nil {
			return nil, errors.Trace(err)
		}
		current = nil
		for rows.Next() {
			var id int
			rows.Scan(&id)
			collected = append(collected, id)
			current = append(current, id)
		}
		rows.Close()
	}
	return collected, nil
}

func (e *Engine) deleteSubgraphFlowsRootedAt(ctx context.Context, tx sequel.Executor, surgraphStepID int) error {
	var rootChildren []int
	rows, err := tx.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_step_id=?", surgraphStepID)
	if err != nil {
		return errors.Trace(err)
	}
	for rows.Next() {
		var id int
		rows.Scan(&id)
		rootChildren = append(rootChildren, id)
	}
	rows.Close()
	if len(rootChildren) == 0 {
		return nil
	}
	allIDs := append([]int{}, rootChildren...)
	current := make([]any, 0, len(rootChildren))
	for _, id := range rootChildren {
		current = append(current, id)
	}
	for len(current) > 0 {
		ph := strings.Repeat("?,", len(current)-1) + "?"
		nestedRows, err := tx.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id IN ("+ph+")", current...)
		if err != nil {
			return errors.Trace(err)
		}
		current = nil
		for nestedRows.Next() {
			var id int
			nestedRows.Scan(&id)
			allIDs = append(allIDs, id)
			current = append(current, id)
		}
		nestedRows.Close()
	}
	args := make([]any, 0, len(allIDs))
	for _, id := range allIDs {
		args = append(args, id)
	}
	ph := strings.Repeat("?,", len(allIDs)-1) + "?"
	tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+ph+")", args...)
	tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id IN ("+ph+")", args...)
	return nil
}
