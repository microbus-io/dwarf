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

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// createSubgraphFlow creates a subgraph flow for a dynamic subgraph transition. callerStepDepth is the
// caller step's step_depth, so the child's entry step (and thus its whole subtree) is numbered as a
// continuation of the caller (callerStepDepth+1).
func (e *Engine) createSubgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, callerStepDepth int, surgraphStepID int, subgraphWorkflowURL string, subgraphGraph *workflow.Graph, childState map[string]any, baggageJSON string, callerTraceParent string) (string, error) {
	db, err := e.db.Shard(shardNum)
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

// completeFlowSequential marks a flow completed when no successor exists.
func (e *Engine) completeFlowSequential(ctx context.Context, shardNum int, db *sequel.DB, flowID int, flowToken string, stepID int, workflowURL string) error {
	e.logger.DebugContext(ctx, "Flow completed", "workflow", workflowURL)
	_, err := e.completeFlow(ctx, shardNum, flowID, flowToken)
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
		"SELECT state, changes FROM dwarf_steps WHERE flow_id=? AND successor_id=0 AND status='"+workflow.StatusCompleted+"' ORDER BY step_id",
		flowID,
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
func (e *Engine) completeFlow(ctx context.Context, shardNum int, flowID int, flowToken string) (bool, error) {
	db, err := e.db.Shard(shardNum)
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
		// Lock-grab flips the non-indexed `touch`, not `updated_at`: the flow's `updated_at` moves only
		// on the genuine status transition below, so idx_dwarf_flows_status is not churned twice per
		// completion. `touch` always changes, so a later RowsAffected check stays meaningful.
		_, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=?", flowID)
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
		// A DeleteOnCompletion root schedules its own deletion by stamping delete_after_ms = deletionGrace, so
		// the reaper removes it (and its subgraph descendants, keyed on root_flow_id) after the grace window.
		// It is NOT deleted inline: the flow stays `completed` for the window so its outcome is
		// Await/Snapshot-observable (an inline delete would 404 the caller). Root-only (surgraph_flow_id=0);
		// the stamp is part of the same status transition, so delete_after_ms > 0 always implies terminal.
		deleteAfterMs := 0
		if deleteOnCompletion && surgraphFlowID == 0 {
			deleteAfterMs = int(deletionGrace.Milliseconds())
		}
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, final_state=?, delete_after_ms=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
			workflow.StatusCompleted, finalStateJSON, deleteAfterMs, flowID,
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

	e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, flowID), "to", workflow.StatusCompleted)
	e.metricFlowTerminated(ctx, workflowURL, workflow.StatusCompleted)
	compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)

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
	db, err := e.db.Shard(shardNum)
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
			"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=1, subgraph_result=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND parked=?",
			workflow.StatusPending, parkedNone, resultJSON, surgraphStepID, parkedSubgraph,
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

// failAndReturn fails the step and returns the error processStep should surface. If failStep's own
// transaction errored - most plausibly its lock-contention retries exhausting under a deadlock storm,
// which leaves the step still leased-and-running because the fail UPDATE never committed - that error is
// returned so processStep's recovery defer sees a lock-contention error and rewinds the step
// (running→pending) for immediate re-dispatch, instead of leaving it stranded running until lease expiry
// (budget+leaseMargin, minutes past the poll cadence). When failStep succeeds the original failure reason
// is returned unchanged (the step is already terminal, so the defer's guarded reset is a no-op). When the
// dispatch's lease was re-granted to a peer (failStep's fenced step UPDATE matched zero rows), nil is
// returned so processStep abandons quietly - the peer owns the step and will settle it. All failStep call
// sites in processStep go through this so the defer covers every fail path uniformly.
func (e *Engine) failAndReturn(ctx context.Context, shardNum int, stepID int, leaseSeq int, flowID int, flowToken string, taskErr error, taskName string) error {
	fenced, ferr := e.failStep(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, taskErr, taskName)
	if ferr != nil {
		return errors.Trace(ferr)
	}
	if fenced {
		return nil
	}
	return errors.Trace(taskErr)
}

// failStep handles a task failure. It returns fenced=true (with a nil error) when the dispatch's lease was
// re-granted to a peer - its fenced step UPDATE matched zero rows, so it wrote nothing and the caller must
// abandon quietly rather than fail a flow the peer is healthily re-executing (see "Lease fencing").
func (e *Engine) failStep(ctx context.Context, shardNum int, stepID int, leaseSeq int, flowID int, flowToken string, taskErr error, taskName string) (fenced bool, err error) {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}

	// A subgraph child's failure is surfaced to the parent's flow.Subgraph call (via the parked caller step,
	// carried in subgraph_error) rather than notified directly - but only when the child flow ACTUALLY fails,
	// which for a child with a fan-out is after its cohort fully resolves (the same cohort accounting below
	// that governs a top-level flow), never eagerly on the first branch error. Failing the child eagerly
	// while a sibling branch is still live would strand that sibling and any subgraph descendants it parked
	// on: the interrupt/resume/cancel tree walks all skip a terminal flow, so nothing could ever release
	// them. parentStepID>0 iff this flow is a subgraph child.
	parentStepID, isSubgraphChild, err := e.dynamicSubgraphParent(ctx, db, flowID)
	if err != nil {
		return false, errors.Trace(err)
	}

	var stepLineageID int
	err = db.QueryRowContext(ctx,
		"SELECT lineage_id FROM dwarf_steps WHERE step_id=?",
		stepID,
	).Scan(&stepLineageID)
	if err != nil {
		return false, errors.Trace(err)
	}

	errMsg := taskErr.Error()
	failFlow := false
	reDispatchParent := false
	var finalStateJSON string
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		fenced = false
		failFlow = stepLineageID == 0
		finalStateJSON = ""
		reDispatchParent = false
		// Fence the fail on our lease generation: a zombie whose lease was re-granted to a peer must not
		// fail the flow (its unguarded predecessor was finding #1's "late error → healthy-flow kill"). This
		// is the first write in the transaction, so a zero-row match means nothing was written - commit the
		// empty tx and report fenced so the caller abandons without failing the flow the peer is re-running.
		res, uerr := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, error=?, updated_at=NOW_UTC() WHERE step_id=? AND lease_seq=?",
			workflow.StatusFailed, parkedNone, errMsg, stepID, leaseSeq,
		)
		if uerr != nil {
			return errors.Trace(uerr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			fenced = true
			return nil
		}
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
				"UPDATE dwarf_flows SET final_state=?, status=?, error=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
				finalStateJSON, workflow.StatusFailed, errMsg, flowID,
			)
			if isSubgraphChild {
				var derr error
				reDispatchParent, derr = e.deliverFlowFailureToParent(ctx, tx, parentStepID, errMsg)
				if derr != nil {
					return errors.Trace(derr)
				}
			}
		}
		return nil
	})
	if err != nil {
		return false, errors.Trace(err)
	}
	if fenced {
		// Lease re-granted to a peer; we wrote nothing. The peer owns this step and settles it.
		return true, nil
	}
	// The step is now failed regardless of whether the whole flow fails - count it.
	e.metricStepExecuted(ctx, taskName, workflow.StatusFailed)

	if !failFlow {
		return false, nil
	}

	// A subgraph child delivers its failure to the parent's flow.Subgraph call for re-dispatch - but it has
	// still stopped, so wake any Await on the child key (legal read-only introspection), locally and on peers,
	// exactly as the top-level path below does. Without this signalStop, Await(childKey) blocks until its
	// context deadline despite the child being terminal.
	if isSubgraphChild {
		e.signalStop(ctx, fmt.Sprintf("%d-%d-%s", shardNum, flowID, strings.TrimSpace(flowToken)), workflow.StatusFailed)
		if reDispatchParent {
			e.enqueueStep(ctx, shardNum, parentStepID)
		}
		return false, nil
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, flowID), "to", workflow.StatusFailed)
	compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, strings.TrimSpace(flowToken))
	e.signalStop(ctx, compositeID, workflow.StatusFailed)
	return false, nil
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
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	errMsg := taskErr.Error()
	reDispatchParent := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		reDispatchParent = false
		// Write-first (the failed-child-step UPDATE) per the flow-terminating-transaction rule; a discarded
		// error here or below would commit a half-failed child while still re-dispatching the parent.
		_, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, error=?, updated_at=NOW_UTC() WHERE step_id=?",
			workflow.StatusFailed, parkedNone, errMsg, childStepID,
		)
		if err != nil {
			return errors.Trace(err)
		}
		childFinalState, _, err := e.computeFinalState(ctx, tx, childFlowID)
		if err != nil {
			return errors.Trace(err)
		}
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, error=?, final_state=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
			workflow.StatusFailed, errMsg, childFinalState, childFlowID,
		)
		if err != nil {
			return errors.Trace(err)
		}
		var derr error
		reDispatchParent, derr = e.deliverFlowFailureToParent(ctx, tx, parentStepID, errMsg)
		return errors.Trace(derr)
	})
	if err != nil {
		return errors.Trace(err)
	}
	if reDispatchParent {
		e.enqueueStep(ctx, shardNum, parentStepID)
	}
	return nil
}

// deliverFlowFailureToParent re-arms a parked parent caller step with a failed child flow's error, so the
// parent's flow.Subgraph call re-dispatches and observes it (yield=false, err set from subgraph_error).
// Called inside the child flow's terminating transaction, after the child flow row has been marked failed.
// Returns true when the caller step was still parked and got re-armed - the caller then enqueues it after
// the transaction. Returns false for a top-level flow (parentStepID==0) or a caller step no longer parked
// (already resolved, cancelled, or retried away), in which case there is nothing to re-dispatch.
func (e *Engine) deliverFlowFailureToParent(ctx context.Context, tx sequel.Executor, parentStepID int, errMsg string) (bool, error) {
	if parentStepID == 0 {
		return false, nil
	}
	res, err := tx.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=1, subgraph_error=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND parked=?",
		workflow.StatusPending, parkedNone, errMsg, parentStepID, parkedSubgraph,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// allSubgraphFlows finds all active (non-terminal) descendant subgraph flows of flowID. It fetches the
// whole tree in one scan via the denormalized root_flow_id, then BFS in memory through non-terminal nodes
// only - the same set the former level-by-level recursion produced (which likewise stopped descending at a
// terminal node), one round-trip regardless of depth. root_flow_id gives tree membership; surgraph_flow_id
// gives the parent/child structure the BFS walks.
func (e *Engine) allSubgraphFlows(ctx context.Context, shardNum int, flowID int) (flowIDs []any, compositeFlowIDs []string, err error) {
	db, err := e.db.Shard(shardNum)
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
		if err := rows.Scan(&id, &token, &parent, &status); err != nil {
			rows.Close()
			return nil, nil, errors.Trace(err)
		}
		status = strings.TrimSpace(status)
		term := status == workflow.StatusCompleted || status == workflow.StatusFailed || status == workflow.StatusCancelled
		byID[id] = node{token: strings.TrimSpace(token), terminal: term}
		if parent != 0 {
			childrenByParent[parent] = append(childrenByParent[parent], id)
		}
	}
	rows.Close()
	// A truncated read (mid-stream error) would make the tree walk act on a partial tree; this is a
	// non-tx read (no Transact latch backstops it), so the check is explicit.
	if err := rows.Err(); err != nil {
		return nil, nil, errors.Trace(err)
	}

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
	db, err := e.db.Shard(shardNum)
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
		if err := frows.Scan(&id, &token, &ssid, &status); err != nil {
			frows.Close()
			return nil, nil, nil, errors.Trace(err)
		}
		tokenByID[id] = strings.TrimSpace(token)
		if ssid != 0 && strings.TrimSpace(status) == workflow.StatusInterrupted {
			childByCallerStep[ssid] = id
		}
	}
	frows.Close()
	if err := frows.Err(); err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	// Each tree flow's interrupted leaf: order in SQL, take the first row per flow_id in memory.
	interruptedLeafByFlow := map[int]int{}
	srows, err := db.QueryContext(ctx,
		"SELECT flow_id, step_id FROM dwarf_steps WHERE status='"+workflow.StatusInterrupted+"' AND flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)) ORDER BY flow_id, updated_at, step_id",
		flowID,
	)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	for srows.Next() {
		var fid, sid int
		if err := srows.Scan(&fid, &sid); err != nil {
			srows.Close()
			return nil, nil, nil, errors.Trace(err)
		}
		if _, seen := interruptedLeafByFlow[fid]; !seen {
			interruptedLeafByFlow[fid] = sid // first row per flow_id = earliest updated_at, step_id
		}
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	flowIDs = []any{flowID}
	compositeFlowIDs = []string{fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)}
	cur := flowID
	for {
		leaf, ok := interruptedLeafByFlow[cur]
		if !ok {
			// An interrupted flow on the descent always has an interrupted step; its absence means a
			// concurrent Resume already resolved this leaf (a double-Resume race). Surface it as a 409,
			// not a raw ErrNoRows (which resume() would trace into an opaque 500).
			return nil, nil, nil, errors.New("flow is not paused at an interrupt", http.StatusConflict)
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
	err = db.QueryRowContext(ctx, "SELECT status, surgraph_flow_id FROM dwarf_flows WHERE flow_id=? AND flow_token=?", flowID, flowToken).Scan(&flowStatus, &surgraphFlowID)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	// Resume acts on the whole interrupt chain (it walks up to the root and down to the leaf), so it must be
	// addressed by the root flow key; a subgraph child key is read-only (introspection/Fork only).
	if surgraphFlowID != 0 {
		return errors.New("cannot resume a subgraph child; use the root flow key", http.StatusBadRequest)
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
	err = db.QueryRowContext(ctx, "SELECT interrupt_done FROM dwarf_steps WHERE step_id=?", leafStepID).Scan(&leafInterruptDone)
	// ErrNoRows means the leaf step is gone (a concurrent Resume/state change) - a 409, not a false one.
	// Any other scan error is a real DB failure; surface it rather than masking it as "not paused" (409).
	if err == sql.ErrNoRows {
		return errors.New("flow is not paused at an interrupt", http.StatusConflict)
	}
	if err != nil {
		return errors.Trace(err)
	}
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
			tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE step_id IN ("+parkPlaceholders+") AND status='"+workflow.StatusInterrupted+"'", parkArgs...)
		}

		tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, resume_data=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusInterrupted+"'",
			workflow.StatusPending, resumeDataJSON, leafStepID)

		for _, chainFlowID := range chainFlowIDs {
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET status=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"' AND (SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"')=0",
				workflow.StatusRunning, chainFlowID, chainFlowID,
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
	db, err := e.db.Shard(shardNum)
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
		if err := rows.Scan(&id, &token, &sfid, &ssid); err != nil {
			rows.Close()
			return nil, nil, nil, errors.Trace(err)
		}
		byID[id] = fnode{token: strings.TrimSpace(token), surgFlowID: sfid, surgStepID: ssid}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

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
