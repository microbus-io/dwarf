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
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// createSubgraphFlow creates a subgraph flow for a dynamic subgraph transition. callerStepDepth is the
// caller step's step_depth, so the child's entry step (and thus its whole subtree) is numbered as a
// continuation of the caller (callerStepDepth+1).
func (e *Engine) createSubgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, callerStepDepth int, surgraphStepID int, subgraphWorkflowURL string, subgraphGraph *workflow.Graph, childState map[string]any, baggageJSON string, callerTraceParent string) (string, error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}

	// Inherit the parent's frozen scheduling/budget and its baggage, all passed into the child's insert so
	// the child is fully formed (surgraph-linked + baggage) in one transaction.
	var inherited workflow.FlowOptions
	var inheritedBudgetMs, rootFlowID int
	err = db.QueryRowContext(ctx,
		"SELECT priority, fairness_key, fairness_weight, time_budget_ms, root_flow_id FROM dwarf_flows WHERE flow_id=?",
		surgraphFlowID,
	).Scan(&inherited.Priority, &inherited.FairnessKey, &inherited.FairnessWeight, &inheritedBudgetMs, &rootFlowID)
	if err != nil {
		return "", errors.Trace(err)
	}
	inherited.TimeBudget = time.Duration(inheritedBudgetMs) * time.Millisecond
	var inheritedBaggage map[string]any
	unmarshalJSONMap(baggageJSON, &inheritedBaggage)
	inherited.Baggage = inheritedBaggage

	// The child is inserted already surgraph-linked and running in one transaction, so it can never complete
	// before its parent pointer is set (which would lose the parent's revive). The caller step is parked by
	// processStep before this call - the complementary half of that ordering. The child's "workflow" span is
	// parented to the caller step's span (callerTraceParent), nesting the subtree under the launching task.
	return e.createWithGraph(ctx, shardNum, subgraphWorkflowURL, subgraphGraph, childState, 0, "", callerTraceParent, &inherited, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID)
}

// fireFlowStopped invokes the host's FlowStopped callback with the flow's baggage on the context, so the
// host can resolve the notification target from it. Callers guard on the flow's notify_on_stop opt-in.
func (e *Engine) fireFlowStopped(ctx context.Context, flowKey string, baggageJSON string, outcome *workflow.FlowOutcome) {
	var baggage map[string]any
	unmarshalJSONMap(baggageJSON, &baggage)
	err := errors.CatchPanic(func() error {
		e.host.FlowStopped(workflow.ContextWithBaggage(ctx, baggage), flowKey, outcome)
		return nil
	})
	if err != nil {
		e.logger.ErrorContext(ctx, "FlowStopped callback panicked", "flow", flowKey, "error", err)
	}
}

// completeFlowSequential marks a flow completed when no successor exists.
func (e *Engine) completeFlowSequential(ctx context.Context, shardNum int, db *sequel.DB, flowID int, flowToken string, stepID int, notifyOnStop bool, baggageJSON, workflowURL string) error {
	e.logger.DebugContext(ctx, "Flow completed", "flow", workflowURL)
	_, err := e.completeFlow(ctx, shardNum, flowID, flowToken, notifyOnStop, baggageJSON)
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
				err := json.Unmarshal([]byte(stateJSON), &baseState)
				if err != nil {
					return nil, false, errors.Trace(err)
				}
			}
			var changes map[string]any
			err = json.Unmarshal([]byte(changesJSON), &changes)
			if err != nil {
				return nil, false, errors.Trace(err)
			}
			allChanges = append(allChanges, changes)
		}
		err = rows.Err()
		if err != nil {
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
	var graphJSON, workflowURL string
	err := db.QueryRowContext(ctx,
		"SELECT graph, workflow_url FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&graphJSON, &workflowURL)
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
	return string(data), workflowURL, nil
}

// completeFlow transitions a flow to completed and propagates to surgraph.
func (e *Engine) completeFlow(ctx context.Context, shardNum int, flowID int, flowToken string, notifyOnStop bool, baggageJSON string) (bool, error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}
	var finalStateJSON, workflowURL string
	var surgraphFlowID, surgraphStepID int
	var deleteOnCompletion bool
	completed := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		completed = false
		// Write-first: take the flow row's write lock before computeFinalState's reads. Without this the
		// transaction is read-first (SELECT graph + terminal steps, then UPDATE), and on SQLite with
		// cache=shared two concurrent completions both hold SHARED locks and deadlock on the upgrade to
		// write - which under load exhausts Transact's retries and errors. Because the terminal step is
		// already marked completed by processStep, the lease recovery (which only resets running rows)
		// cannot re-dispatch it, leaving the flow stranded 'running' with all steps terminal (an orphan
		// flow). Mirrors advanceFlow and the fan-in transaction, which write first for the same reason.
		_, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=NOW_UTC() WHERE flow_id=?", flowID)
		if err != nil {
			return errors.Trace(err)
		}
		// Read the surgraph linkage and disposable flag under the write lock - needed both for the post-tx
		// surgraph revival and for the atomic disposable delete below.
		err = tx.QueryRowContext(ctx,
			"SELECT surgraph_flow_id, surgraph_step_id, delete_on_completion FROM dwarf_flows WHERE flow_id=?",
			flowID,
		).Scan(&surgraphFlowID, &surgraphStepID, &deleteOnCompletion)
		if err != nil {
			return errors.Trace(err)
		}
		fs, wf, err := e.computeFinalState(ctx, tx, flowID)
		if err != nil {
			return errors.Trace(err)
		}
		finalStateJSON = fs
		workflowURL = wf
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

		// A DeleteOnCompletion root deletes itself (and its subgraph descendants) in this SAME transaction,
		// so the flow transitions running -> gone atomically with no committed `completed` state in between.
		// Doing it as a separate post-completion delete left an observable window: a Snapshot/Await whose
		// read landed between the status=completed commit and the delete saw a `completed` outcome instead
		// of the intended uniform 404 (await's first snapshot, before it ever waits on signalStop, is the
		// common way to hit it). Folding the delete in removes the window entirely. Root-only
		// (surgraph_flow_id=0); the FlowStopped notification below still fires with the computed outcome.
		if completed && deleteOnCompletion && surgraphFlowID == 0 {
			descendants, derr := e.allDescendantSubgraphFlows(ctx, tx, flowID)
			if derr != nil {
				return errors.Trace(derr)
			}
			if len(descendants) > 0 {
				ph := strings.Repeat("?,", len(descendants)-1) + "?"
				args := make([]any, 0, len(descendants))
				for _, id := range descendants {
					args = append(args, id)
				}
				if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+ph+")", args...); err != nil {
					return errors.Trace(err)
				}
				if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id IN ("+ph+")", args...); err != nil {
					return errors.Trace(err)
				}
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id=?", flowID); err != nil {
				return errors.Trace(err)
			}
			if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id=?", flowID); err != nil {
				return errors.Trace(err)
			}
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
	e.metricFlowTerminated(ctx, workflowURL, workflow.StatusCompleted)
	compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)

	if notifyOnStop {
		var finalState map[string]any
		json.Unmarshal([]byte(finalStateJSON), &finalState)
		e.fireFlowStopped(ctx, compositeID, baggageJSON, &workflow.FlowOutcome{
			Status: workflow.StatusCompleted,
			State:  finalState,
		})
	}

	e.signalStop(ctx, compositeID, workflow.StatusCompleted)
	e.signalEnqueue(ctx, 0, 0) // Wake peers

	if surgraphFlowID != 0 {
		err := e.completeSurgraphFlow(ctx, shardNum, surgraphFlowID, surgraphStepID, finalStateJSON)
		if err != nil {
			return true, errors.Trace(err)
		}
	}

	return true, nil
}

// completeSurgraphFlow re-dispatches a parked surgraph step after its child completes.
func (e *Engine) completeSurgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, surgraphStepID int, subgraphFinalStateJSON string) error {
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
			"surgraphFlow", surgraphFlowID, "surgraphStep", surgraphStepID)
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
	// The step is now failed regardless of whether the whole flow fails - count it.
	e.metricStepExecuted(ctx, taskName, workflow.StatusFailed)

	if !failFlow {
		return nil
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", flowID, "to", workflow.StatusFailed)
	compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, strings.TrimSpace(flowToken))
	var notifyOnStop bool
	var baggageJSON string
	db.QueryRowContext(ctx, "SELECT notify_on_stop, baggage FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&notifyOnStop, &baggageJSON)
	if notifyOnStop {
		var finalState map[string]any
		json.Unmarshal([]byte(finalStateJSON), &finalState)
		e.fireFlowStopped(ctx, compositeID, baggageJSON, &workflow.FlowOutcome{
			Status: workflow.StatusFailed,
			State:  finalState,
			Error:  errMsg,
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

// allSubgraphFlows finds all active (non-terminal) descendant subgraph flows of flowID. It fetches the
// whole tree in one scan via the denormalized root_flow_id, then BFS in memory through non-terminal nodes
// only - the same set the former level-by-level recursion produced (which likewise stopped descending at a
// terminal node), one round-trip regardless of depth. root_flow_id gives tree membership; surgraph_flow_id
// gives the parent/child structure the BFS walks.
func (e *Engine) allSubgraphFlows(ctx context.Context, shardNum int, flowID int) (flowIDs []any, compositeFlowIDs []string, err error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, flow_token, surgraph_flow_id, status FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)",
		flowID,
	)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	type node struct {
		token    string
		terminal bool
	}
	byID := map[int]node{}
	childrenByParent := map[int][]int{}
	for rows.Next() {
		var id, parent int
		var token, status string
		rows.Scan(&id, &token, &parent, &status)
		status = strings.TrimSpace(status)
		term := status == workflow.StatusCompleted || status == workflow.StatusFailed || status == workflow.StatusCancelled
		byID[id] = node{token: strings.TrimSpace(token), terminal: term}
		if parent != 0 {
			childrenByParent[parent] = append(childrenByParent[parent], id)
		}
	}
	rows.Close()

	queue := []int{flowID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, child := range childrenByParent[cur] {
			n := byID[child]
			if n.terminal { // a terminal node is neither collected nor descended through (matches the old walk)
				continue
			}
			flowIDs = append(flowIDs, child)
			compositeFlowIDs = append(compositeFlowIDs, fmt.Sprintf("%d-%d-%s", shardNum, child, n.token))
			queue = append(queue, child)
		}
	}
	return flowIDs, compositeFlowIDs, nil
}

// interruptedSubgraphChain walks down from a flow through interrupted subgraph steps to find the leaf. It
// loads the tree once via root_flow_id (structure + per-flow tokens/status) and each flow's interrupted-leaf
// step in one batched query (SQL does the earliest-updated_at ordering, so there is no Go-side timestamp
// comparison), then descends in memory - one round-trip per concern regardless of depth, vs the former two
// queries per level. The leaf is picked the SAME way Snapshot does (earliest updated_at, step_id tiebreak),
// so a Snapshot reports exactly the interrupt the next Resume resolves. Descent is keyed on surgraph_step_id
// (the caller step's PK), never depth, which is ambiguous when parallel subgraph callers share a depth.
func (e *Engine) interruptedSubgraphChain(ctx context.Context, shardNum int, flowID int, flowToken string) (flowIDs []any, stepIDs []any, compositeFlowIDs []string, err error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	frows, err := db.QueryContext(ctx,
		"SELECT flow_id, flow_token, surgraph_step_id, status FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)",
		flowID,
	)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	tokenByID := map[int]string{}
	childByCallerStep := map[int]int{} // surgraph_step_id -> interrupted child flow_id
	for frows.Next() {
		var id, ssid int
		var token, status string
		frows.Scan(&id, &token, &ssid, &status)
		tokenByID[id] = strings.TrimSpace(token)
		if ssid != 0 && strings.TrimSpace(status) == workflow.StatusInterrupted {
			childByCallerStep[ssid] = id
		}
	}
	frows.Close()

	// Each tree flow's interrupted leaf: order in SQL, take the first row per flow_id in memory.
	interruptedLeafByFlow := map[int]int{}
	srows, err := db.QueryContext(ctx,
		"SELECT flow_id, step_id FROM dwarf_steps WHERE status=? AND flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)) ORDER BY flow_id, updated_at, step_id",
		workflow.StatusInterrupted, flowID,
	)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	for srows.Next() {
		var fid, sid int
		srows.Scan(&fid, &sid)
		if _, seen := interruptedLeafByFlow[fid]; !seen {
			interruptedLeafByFlow[fid] = sid // first row per flow_id = earliest updated_at, step_id
		}
	}
	srows.Close()

	flowIDs = []any{flowID}
	compositeFlowIDs = []string{fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)}
	cur := flowID
	for {
		leaf, ok := interruptedLeafByFlow[cur]
		if !ok {
			// An interrupted flow on the descent always has an interrupted step; its absence is the
			// degenerate case the per-flow leaf SELECT used to surface as ErrNoRows.
			return nil, nil, nil, errors.Trace(sql.ErrNoRows)
		}
		stepIDs = append(stepIDs, leaf)
		child, ok := childByCallerStep[leaf]
		if !ok {
			return flowIDs, stepIDs, compositeFlowIDs, nil // no interrupted child spawned here - leaf reached
		}
		flowIDs = append(flowIDs, child)
		compositeFlowIDs = append(compositeFlowIDs, fmt.Sprintf("%d-%d-%s", shardNum, child, tokenByID[child]))
		cur = child
	}
}

// resume continues a flow paused by flow.Interrupt, delivering resume data to the leaf interrupt park.
func (e *Engine) resume(ctx context.Context, flowKey string, data any) error {
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

	var leafInterruptDone bool
	db.QueryRowContext(ctx, "SELECT interrupt_done FROM dwarf_steps WHERE step_id=?", leafStepID).Scan(&leafInterruptDone)
	if !leafInterruptDone {
		return errors.New("flow is not paused at an interrupt", http.StatusConflict)
	}

	resumeDataJSON := "{}"
	if data != nil {
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

		tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, resume_data=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
			workflow.StatusPending, resumeDataJSON, leafStepID, workflow.StatusInterrupted)

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

// surgraphChain walks from a flow up to the root surgraph. It loads the whole tree once via the denormalized
// root_flow_id, then follows surgraph_flow_id/surgraph_step_id pointers from flowID up to the root in memory -
// one round-trip regardless of nesting depth, vs the former two queries per level. root_flow_id gives the tree
// membership; the surgraph links give the parent/caller structure the walk follows.
func (e *Engine) surgraphChain(ctx context.Context, shardNum int, flowID int, flowToken string) (flowIDs []any, stepIDs []any, compositeFlowIDs []string, err error) {
	db, err := e.shard(shardNum)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, flow_token, surgraph_flow_id, surgraph_step_id FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)",
		flowID,
	)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	type fnode struct {
		token      string
		surgFlowID int
		surgStepID int
	}
	byID := map[int]fnode{}
	for rows.Next() {
		var id, sfid, ssid int
		var token string
		rows.Scan(&id, &token, &sfid, &ssid)
		byID[id] = fnode{token: strings.TrimSpace(token), surgFlowID: sfid, surgStepID: ssid}
	}
	rows.Close()

	flowIDs = []any{flowID}
	compositeFlowIDs = []string{fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)}
	cur := flowID
	for {
		n, ok := byID[cur]
		if !ok || n.surgFlowID == 0 {
			break
		}
		flowIDs = append(flowIDs, n.surgFlowID)
		stepIDs = append(stepIDs, n.surgStepID)
		compositeFlowIDs = append(compositeFlowIDs, fmt.Sprintf("%d-%d-%s", shardNum, n.surgFlowID, byID[n.surgFlowID].token))
		cur = n.surgFlowID
	}
	return flowIDs, stepIDs, compositeFlowIDs, nil
}
