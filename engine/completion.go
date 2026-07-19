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
	"bytes"
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
func (e *Engine) createSubgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, callerStepDepth int, surgraphStepID int, subgraphWorkflowURL string, subgraphGraph *workflow.Graph, childState workflow.State, baggageJSON []byte, callerTraceParent string) (string, error) {
	// faultSubgraphSpawnErr simulates the spawn failing after the caller step already parked (processStep's
	// park-then-create ordering): no child is inserted, and the caller must be failed cleanly (failAndReturn)
	// rather than left parked forever. Scoped by the child workflow URL.
	if e.seams.IsFault(faultSubgraphSpawnErr, subgraphWorkflowURL) {
		return "", errors.New("injected fault: "+faultSubgraphSpawnErr+" "+subgraphWorkflowURL, http.StatusInternalServerError)
	}
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
	inheritedBaggage, _ := workflow.NewState(baggageJSON)
	inherited.Baggage = inheritedBaggage

	// The child is inserted already surgraph-linked and running in one transaction, so it can never complete
	// before its parent pointer is set (which would lose the parent's revive). The caller step is parked by
	// processStep before this call - the complementary half of that ordering. The child's "workflow" span is
	// parented to the caller step's span (callerTraceParent), nesting the subtree under the launching task.
	return e.createWithGraph(ctx, shardNum, subgraphWorkflowURL, subgraphGraph, childState, 0, "", callerTraceParent, &inherited, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID)
}

// completeFlowSequential marks a flow completed when no successor exists.
//
// It does NOT touch the step. Both call sites are downstream of processStep's fenced completion UPDATE, which
// already set status='completed' (and bailed on a zero-row fence match), so the step is terminal before this
// runs. The trailing "UPDATE dwarf_steps SET status=completed" that used to live here re-wrote that same value:
// one wasted BEGIN/UPDATE/COMMIT on the hot path of EVERY flow completion, whose only real effect was to bump
// the step's updated_at a second time - to AFTER completeFlow's transaction, inflating the step's recorded task
// duration (History, and the FlowRenderer's node label, read updated_at - started_at) by the cost of completing
// the flow. It was also the one post-execution step write carrying neither a status guard nor a lease_seq fence,
// harmless only because the fenced gate upstream had already made the step terminal. Do not reintroduce it.
func (e *Engine) completeFlowSequential(ctx context.Context, shardNum int, flowID int, flowToken string, workflowURL string) error {
	e.logger.DebugContext(ctx, "Flow completed", "workflow", workflowURL)
	_, err := e.completeFlow(ctx, shardNum, flowID, flowToken)
	return errors.Trace(err)
}

// mergeTerminalSteps computes a flow's terminal state from the execution-DAG tail.
// It also returns the merge base tail's lineage_id, so the caller can strip exactly the forEach bookkeeping of
// the cohorts that tail is inside - and nothing else.
func (e *Engine) mergeTerminalSteps(ctx context.Context, db sequel.Executor, shardNum, flowID int, reducers map[string]workflow.Reducer, workflowURL string) (workflow.State, int, error) {
	merge := func(query string, args ...any) (workflow.State, int, bool, error) {
		rows, err := db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, 0, false, errors.Trace(err)
		}
		defer rows.Close()

		var baseState workflow.State
		var baseRefs stateRefs
		baseLineage := 0
		var allChanges []workflow.State
		found := false
		for rows.Next() {
			var stateJSON, changesJSON, refsJSON []byte
			var lineageID int
			if err := rows.Scan(&stateJSON, &changesJSON, &refsJSON, &lineageID); err != nil {
				return nil, 0, false, errors.Trace(err)
			}
			if !found {
				found = true
				baseState, err = workflow.NewState(stateJSON)
				if err != nil {
					return nil, 0, false, errors.Trace(err)
				}
				baseRefs = parseStateRefs(refsJSON)
				baseLineage = lineageID
			}
			changes, err := workflow.NewState(changesJSON)
			if err != nil {
				return nil, 0, false, errors.Trace(err)
			}
			allChanges = append(allChanges, changes)
		}
		if err := rows.Err(); err != nil {
			return nil, 0, false, errors.Trace(err)
		}
		if !found {
			return nil, 0, false, nil
		}
		// FLATTEN: final_state is a flow-boundary value (a dwarf_flows column that Continue feeds into the next
		// turn, and that outlives this flow's steps - DeleteOnCompletion may reap them). A ref that escaped the
		// flow would dangle, so the terminal merge always materializes, whatever it costs on this one row.
		// It is not a regression either: a large field surviving to the end is copied into final_state today.
		// resolveStateRefs mutates the state map in place, so it gets the live map.
		if err := e.resolveStateRefs(ctx, db, shardNum, baseState, baseRefs, nil, workflowURL); err != nil {
			return nil, 0, false, errors.Trace(err)
		}

		merged := baseState
		for _, changes := range allChanges {
			if err := merged.MergeReduce(changes, reducers); err != nil {
				return nil, 0, false, errors.Trace(err)
			}
		}
		merged.DelNils()
		return merged, baseLineage, true, nil
	}

	merged, lineage, found, err := merge(
		"SELECT state, changes, state_refs, lineage_id FROM dwarf_steps WHERE flow_id=? AND successor_id=0 AND status='"+workflow.StatusCompleted+"' ORDER BY step_id",
		flowID,
	)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	if found {
		return merged, lineage, nil
	}

	merged, lineage, found, err = merge(
		"SELECT state, changes, state_refs, lineage_id FROM dwarf_steps WHERE flow_id=? AND successor_id=0 ORDER BY step_id",
		flowID,
	)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	if found {
		return merged, lineage, nil
	}
	return workflow.State{}, 0, nil
}

// computeFinalState computes the merged state for a flow.
func (e *Engine) computeFinalState(ctx context.Context, db sequel.Executor, shardNum, flowID int) ([]byte, string, error) {
	var workflowURL string
	var graphJSON []byte
	err := db.QueryRowContext(ctx,
		"SELECT graph, workflow_url FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&graphJSON, &workflowURL)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	var graph workflow.Graph
	err = json.Unmarshal(graphJSON, &graph)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	merged, baseLineageID, err := e.mergeTerminalSteps(ctx, db, shardNum, flowID, graph.Reducers(), workflowURL)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	// The tails are COHORT MEMBERS (lineage_id != 0), which means a fan-out resolved with failures and so never
	// reached its fan-in. Rebuild the state the way the convergence would have, rather than from the tail rows.
	//
	// mergeTerminalSteps bases on the FIRST tail's `state` and folds in every tail's `changes` - and that loses
	// the intermediate output of every branch but one. In a multi-step branch (`forEach -> Cell -> Enrich -> J`),
	// Cell's output lives in Enrich's `state`, not in its `changes`: only the first tail's state is consulted, so
	// branch 0's Cell output rides in via the base while branches 1..N-1's is simply dropped. The converged path
	// (insertFanInStep) has no such hole - it merges EVERY cohort member's `changes` - so the two paths disagreed
	// on what a fan-out's terminal state means, which is exactly the drift the shared strip below exists to stop.
	//
	// Every step of a per-element sub-pipeline inherits the spawn's lineage, so "every cohort member" is one
	// indexed scan and it covers Cell AND Enrich, for every branch.
	if baseLineageID != 0 {
		merged, err = e.mergeCohortState(ctx, db, shardNum, flowID, baseLineageID, &graph, workflowURL)
		if err != nil {
			return nil, "", errors.Trace(err)
		}
	}

	// Drop per-branch forEach bookkeeping, exactly as insertFanInStep does when a cohort converges - and, like
	// it, only for the cohorts the merge base is actually INSIDE.
	//
	// A FAILED fan-out is the case that needs it: the merge base is the first tail row, and a completed sibling
	// that transitioned toward a fan-in that never fired keeps successor_id=0 - so it IS a tail, and its `state`
	// is a BRANCH-LOCAL snapshot carrying the element it was handed (`<as>`) and its ordinal context
	// (`<as>Index`/`<as>Count`). Left in, whichever branch happens to have the lowest step_id donates its
	// private bookkeeping to the flow's final_state, and with 3+ branches which one wins is arbitrary.
	//
	// The cohorts to strip are read from the tail's OWN lineage chain, never from "every forEach in the graph":
	// a tail that is not in a cohort (the ordinary completed flow, whose terminal step is downstream of every
	// fan-in) is inside no cohort and must be stripped of NOTHING. Stripping by graph made the three injected
	// names of every forEach globally reserved and silently deleted a same-named field a task legitimately
	// wrote - a `forEach ... as "page"` reserved `pageCount` for the whole workflow, so a task writing its own
	// `pageCount` saw it vanish from final_state while History still showed it.
	spawns, err := e.cohortSpawnTasks(ctx, db, baseLineageID)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	for _, spawnTask := range spawns {
		stripForEachBookkeeping(merged, &graph, spawnTask)
	}

	data, err := json.Marshal(merged)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	return data, workflowURL, nil
}

// mergeCohortState rebuilds the state of a cohort that resolved with failures, and so never reached its fan-in.
// It runs the SAME merge insertFanInStep runs at a successful convergence - the spawn step's `state + changes` as
// the base, then every COMPLETED member's `changes` folded through the graph's reducers in `fan_out_ordinal,
// step_depth, step_id` order - so a fan-out's terminal state means one thing whether it converged or failed.
//
// Why the base is the SPAWN and not a tail: a branch's intermediate output lives in the NEXT step's `state`, not in
// any tail's `changes`, so merging tails alone silently drops everything but the last step of each branch (and, for
// the state half, everything but the first tail's branch). The spawn's own row is the only base that is common to
// the whole cohort, and every member's `changes` is exactly the set of per-branch outputs.
//
// Reducer ORDER matters (append/union/concat are not commutative), which is why the scan is ordered the same way
// the convergence orders it: by the branch's position in the spawn loop, not by completion order.
func (e *Engine) mergeCohortState(ctx context.Context, db sequel.Executor, shardNum, flowID, cohortSpawnID int, graph *workflow.Graph, workflowURL string) (workflow.State, error) {
	var spawnStateJSON, spawnChangesJSON, spawnRefsJSON []byte
	err := db.QueryRowContext(ctx,
		"SELECT state, changes, state_refs FROM dwarf_steps WHERE step_id=?", cohortSpawnID,
	).Scan(&spawnStateJSON, &spawnChangesJSON, &spawnRefsJSON)
	if err == sql.ErrNoRows {
		return workflow.State{}, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	spawnState, _ := workflow.NewState(spawnStateJSON)
	spawnChanges, _ := workflow.NewState(spawnChangesJSON)
	// FLATTEN: final_state is a flow-boundary value, so everything the spawn carried by reference is materialized
	// here - unlike the fan-in, which may pass a carried ref through. resolveStateRefs mutates the map in place.
	err = e.resolveStateRefs(ctx, db, shardNum, spawnState, parseStateRefs(spawnRefsJSON), nil, workflowURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// The failed cohort's terminal state is built the SAME way convergence would (insertFanInStep): the spawn's
	// state with its own delta folded via the reducers, as the base the completed members fold onto below.
	merged := spawnState.Clone()
	if err := merged.MergeReduce(spawnChanges, graph.Reducers()); err != nil {
		return nil, errors.Trace(err)
	}

	rows, err := db.QueryContext(ctx,
		"SELECT status, changes FROM dwarf_steps WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_depth, step_id",
		flowID, cohortSpawnID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var status string
		var changesJSON []byte
		if err := rows.Scan(&status, &changesJSON); err != nil {
			return nil, errors.Trace(err)
		}
		// Only completed members contribute, matching insertFanInStep: a failed or cancelled branch's partial
		// output is not a fact the flow can build on (an error voids the task's changes).
		if strings.TrimSpace(status) != workflow.StatusCompleted {
			continue
		}
		changes, _ := workflow.NewState(changesJSON)
		if err := merged.MergeReduce(changes, graph.Reducers()); err != nil {
			return nil, errors.Trace(err)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}
	merged.DelNils()
	return merged, nil
}

// cancelSubtree cancels a flow and every non-terminal flow beneath it, in one transaction: the steps first
// (write-first, per the flow-terminating-transaction rule), then each flow's `final_state`, then one CASE-per-flow
// terminalization guarded on non-terminal status. It signals every flow it stopped.
//
// It walks DOWN only. Cancellation is always initiated at a flow that has no live ancestor to reconcile with -
// the public Cancel is root-only (a subgraph-child key is rejected, not silently widened), and the internal
// orphan sweep starts at a child whose whole ancestor chain is already terminal. The `surgraphChain` up-walk the
// public path used to run was therefore dead: on a root it returns no ancestor steps at all, so the block that
// cancelled them could never execute, and the call itself was an expensive way (a full root_flow_id tree scan) to
// learn the flow's own id and token, which the caller already holds.
//
// conflictIfSettled is the only behavioral difference between the two callers, and it is a real one: an operator
// Cancel of an already-terminal flow is a 409 (the caller asked to stop something that had stopped), while the
// orphan sweep racing a concurrent terminalization is a benign no-op (it asked for an outcome that already
// happened). commitFault is a test-only seam name, checked before any write so a fired fault proves the whole
// transaction rolls back atomically; pass "" for none.
func (e *Engine) cancelSubtree(ctx context.Context, shardNum, flowID int, flowToken, reason, commitFault string, conflictIfSettled bool) error {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	descendantFlowIDs, descendantCompositeIDs, err := e.allSubgraphFlows(ctx, shardNum, flowID)
	if err != nil {
		return errors.Trace(err)
	}
	allFlowIDs := append([]any{flowID}, descendantFlowIDs...)
	allCompositeIDs := append(
		[]string{fmt.Sprintf("%d-%d-%s", shardNum, flowID, strings.TrimSpace(flowToken))},
		descendantCompositeIDs...,
	)

	reason = strings.TrimSpace(reason)
	finalStates := make([][]byte, len(allFlowIDs))
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if commitFault != "" && e.seams.IsFault(commitFault) {
			return errors.New("injected fault: " + commitFault)
		}
		flowPlaceholders := strings.Repeat("?,", len(allFlowIDs)-1) + "?"
		// Write-first (the step-cancel UPDATE) per the flow-terminating-transaction rule.
		stepArgs := append([]any{workflow.StatusCancelled, parkedNone}, allFlowIDs...)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusInterrupted+"', '"+workflow.StatusRunning+"')",
			stepArgs...,
		)

		for i, fid := range allFlowIDs {
			fs, _, err := e.computeFinalState(ctx, tx, shardNum, fid.(int))
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
		if n, _ := res.RowsAffected(); n == 0 && conflictIfSettled {
			return errors.New("flow is already in terminal status", http.StatusConflict)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	awaitedSet := e.awaitedFlows(ctx, shardNum, allFlowIDs)
	for i, cid := range allCompositeIDs {
		e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, allFlowIDs[i].(int)), "to", workflow.StatusCancelled)
		e.signalStop(ctx, cid, workflow.StatusCancelled, awaitedSet == nil || awaitedSet[allFlowIDs[i].(int)])
	}
	return nil
}

// cohortSpawnTasks walks a step's lineage chain upward and returns the task name of every fan-out spawn whose
// cohort the step is inside - innermost first. `lineage_id` names the spawn step of the cohort a step belongs
// to (0 outside any cohort), and a spawn is itself a step with its own lineage, so nested fan-outs form a
// chain. Each level costs one primary-key lookup and the chain is as deep as the graph's fan-out nesting
// (typically zero or one), on a terminal path.
func (e *Engine) cohortSpawnTasks(ctx context.Context, db sequel.Executor, lineageID int) ([]string, error) {
	var tasks []string
	seen := map[int]bool{}
	for lineageID != 0 && !seen[lineageID] {
		seen[lineageID] = true // a malformed chain must not spin
		var taskName string
		var next int
		err := db.QueryRowContext(ctx,
			"SELECT task_name, lineage_id FROM dwarf_steps WHERE step_id=?", lineageID,
		).Scan(&taskName, &next)
		if err == sql.ErrNoRows {
			return tasks, nil
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		tasks = append(tasks, strings.TrimSpace(taskName))
		lineageID = next
	}
	return tasks, nil
}

// stripForEachBookkeeping deletes the per-branch fields the engine injects into a forEach branch's state - the
// element (`<as>`) and its ordinal context (`<as>Index`, `<as>Count`) - from a materialized state map, for the
// ONE cohort spawned by spawnTask. They are a branch's private context and have no meaning once that cohort is
// behind the flow: with the Replace reducer one arbitrary branch's element would otherwise ride forward.
//
// Scoping to the spawning task is load-bearing in both directions. Stripping every forEach in the graph (a) made
// the three names of every `as` globally reserved, silently deleting a same-named field a task wrote, and (b)
// broke nesting: at an INNER fan-in it also deleted the OUTER cohort's bookkeeping, so a step still inside the
// outer branch could no longer see which element it was working on. The names are reserved only WITHIN their own
// cohort; a workflow wanting the element past that cohort's fan-in forwards it under a different key.
//
// Applied at both places a cohort's state leaves the cohort: insertFanInStep (the convergence) and
// computeFinalState (a fan-out that never converged), so the two cannot drift on what a fan-out's state means.
func stripForEachBookkeeping(state workflow.State, graph *workflow.Graph, spawnTask string) {
	if state.Len() == 0 || spawnTask == "" {
		return
	}
	for _, tr := range graph.Transitions() {
		if tr.From != spawnTask || tr.ForEach == "" || tr.As == "" {
			continue
		}
		state.Del(tr.As, tr.As+"Index", tr.As+"Count")
	}
}

// completeFlow transitions a flow to completed and propagates to surgraph.
func (e *Engine) completeFlow(ctx context.Context, shardNum int, flowID int, flowToken string) (bool, error) {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}
	var workflowURL, currentStatus string
	var finalStateJSON []byte
	var surgraphFlowID, surgraphStepID int
	var deleteOnCompletion, awaited bool
	completed := false
	// Test checkpoint: a breakpoint here freezes completion just before its transaction (holding no lock, so a
	// racing Cancel can commit), letting a test drive the completeFlow-vs-Cancel race in either order. Placed
	// before the transaction, not inside, for the same SQLite-deadlock reason as checkpointResumeBeforeFlowWrite.
	e.seams.Checkpoint(ctx, checkpointBeforeCompleteFlowWrite)
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		completed = false
		// faultCompleteFlowCommit fails the flow-completion transaction after processStep has already
		// marked the terminal step completed, so the test proves the write-first ordering + recovery defer
		// keep the flow from stranding `running` with every step terminal. Non-retryable (a plain error).
		if e.seams.IsFault(faultCompleteFlowCommit) {
			return errors.New("injected fault: " + faultCompleteFlowCommit)
		}
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
		// Read the surgraph linkage, disposable flag, and awaited flag under the write lock - needed for
		// the post-tx surgraph revival, the atomic disposable delete below, and the signalStop broadcast gate.
		err = tx.QueryRowContext(ctx,
			"SELECT surgraph_flow_id, surgraph_step_id, delete_on_completion, awaited, status FROM dwarf_flows WHERE flow_id=?",
			flowID,
		).Scan(&surgraphFlowID, &surgraphStepID, &deleteOnCompletion, &awaited, &currentStatus)
		if err != nil {
			return errors.Trace(err)
		}
		fs, wf, err := e.computeFinalState(ctx, tx, shardNum, flowID)
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
			deleteAfterMs = int(e.deletionGrace.Milliseconds())
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
		// Our status UPDATE matched no row, so the flow was already terminal at the lock. If it is `completed`
		// with a surgraph parent, the revive still owes a re-drive - the revive-lost-on-retry case: on
		// the FIRST attempt this transaction committed the completion and the post-tx completeSurgraphFlow then
		// hit a transient DB error, so persist re-ran this idempotent closure; on this retry the status UPDATE
		// no-ops (already completed) and, without re-driving the revive here, persist would read the nil below as
		// "the write landed" and the parent's caller step would strand running+parkedSubgraph until the ~10m
		// parked-step wedge sweep (raising a false wedge alarm). completeSurgraphFlow is CAS-guarded on
		// running+parkedSubgraph, so re-driving is idempotent (a peer that already revived, or that raced us to
		// complete, makes this a harmless no-op). signalStop/metrics are deliberately NOT repeated - they fired
		// on the attempt that transitioned the flow. A failed/cancelled flow (status != completed) gets no revive
		// here; its parent is handled by failure delivery / the Cancel cascade.
		if strings.TrimSpace(currentStatus) == workflow.StatusCompleted && surgraphFlowID != 0 {
			if rerr := e.completeSurgraphFlow(ctx, shardNum, surgraphFlowID, surgraphStepID, finalStateJSON); rerr != nil {
				return false, errors.Trace(rerr)
			}
		}
		return false, nil
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, flowID), "to", workflow.StatusCompleted)
	e.metricFlowTerminated(ctx, workflowURL, workflow.StatusCompleted)
	compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)

	e.signalStop(ctx, compositeID, workflow.StatusCompleted, awaited)
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
func (e *Engine) completeSurgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, surgraphStepID int, subgraphFinalStateJSON []byte) error {
	// faultSubgraphReviveLost simulates the revive being lost after the child went terminal (the wedge the
	// parked-step sweep exists to catch): the caller step stays running+parkedSubgraph with no live child,
	// so the test proves recoverWedgedSubgraphParks re-drives the release. Returns nil so the caller path
	// believes it succeeded, exactly as a lost revive would look.
	if e.seams.IsFault(faultSubgraphReviveLost) {
		return nil
	}
	// faultCompleteSurgraphErr makes the revive fail with a synthetic non-contention error (consumed per attempt),
	// so a test can prove persist re-drives it on retry rather than losing it - the whole point of the re-drive.
	if e.seams.IsFault(faultCompleteSurgraphErr) {
		return errors.New("injected fault: " + faultCompleteSurgraphErr)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	resultJSON := subgraphFinalStateJSON
	if len(bytes.TrimSpace(resultJSON)) == 0 {
		resultJSON = []byte("{}")
	}
	// Test checkpoint: a breakpoint here freezes the worker after the child completed but before the caller
	// revive, so a test can Cancel the caller in exactly the window the revive's running+parkedSubgraph guard
	// exists to survive (the revive must not resurrect the just-cancelled caller).
	e.seams.Checkpoint(ctx, checkpointBeforeReviveWrite)
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
	parentStepID, isSubgraphChild, awaited, err := e.dynamicSubgraphParent(ctx, db, flowID)
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

	// The message lands in a text column, so it is sanitized: a control byte echoed out of a driver error (a
	// NUL, which Postgres rejects in `text` exactly as it does in `jsonb`) would kill the very write that is
	// supposed to be the clean one, and failOnPersistError would misread that as an unreachable database.
	errMsg := sanitizeErrorMessage(taskErr.Error())
	failFlow := false
	reDispatchParent := false
	var finalStateJSON []byte
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		fenced = false
		// A trunk step (lineage_id==0) has no concurrent sibling, so its failure fails the flow at once.
		// A fan-out branch (lineage_id!=0) instead defers to cohort accounting below: siblings run to
		// completion and the flow fails only once the whole cohort arrives with cohort_failures>0.
		failFlow = stepLineageID == 0
		finalStateJSON = nil
		reDispatchParent = false
		// Fence the fail on BOTH our lease generation AND the two states a worker legitimately fails a step
		// from: `running` (a task error, or the step-completion write itself failing) and `completed` (a
		// transition/eval failure that failOnPersistError fails the already-completed step for, to escape the
		// re-execution loop). The lease fence alone guards the zombie "late error → healthy-flow kill" hazard;
		// the status guard - which every sibling post-execution write carries and this one was missing - closes
		// the rest. Without it a step a racing Cancel just cancelled (cancelSubtree does NOT bump lease_seq, so it
		// still matches our generation) is rewritten cancelled→failed, violating step immutability and seeding a
		// phantom branch failure a later Fork re-derives from step status; and a step recovery reset to `pending`
		// (also lease_seq-preserving) would be terminalized out from under the peer re-claiming it. This is the
		// first write in the transaction, so a zero-row match means nothing was written - commit the empty tx and
		// report fenced so the caller abandons without failing a flow that is cancelled or that a peer is re-running.
		res, uerr := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, error=?, updated_at=NOW_UTC() WHERE step_id=? AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusCompleted+"') AND lease_seq=?",
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
			finalStateJSON, _, err = e.computeFinalState(ctx, tx, shardNum, flowID)
			if err != nil {
				// A fuse that can itself be poisoned is not a fuse. failStep is the ONLY way out of a step whose
				// outcome cannot be persisted (failOnPersistError uses it as the clean, payload-free write that
				// tells a permanent failure from an unreachable database) - and computeFinalState is the one part
				// of it that reads other steps' payloads. If a value that cannot be read back had already landed
				// in a row, this merge would die the same way as the write we are failing, failStep would report
				// an error, and the classifier would misread it as "the database is down" - leaving the step to
				// re-execute forever, which is exactly the loop this closes. So the flow still terminalizes; it
				// just terminalizes with an empty final_state and the error that explains why.
				e.logger.ErrorContext(ctx, "Computing final_state while failing a step; terminalizing with empty state",
					"flow", keys.CorrelationID(shardNum, flowID), "step", stepID, "error", err)
				finalStateJSON = []byte("{}")
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
		e.signalStop(ctx, fmt.Sprintf("%d-%d-%s", shardNum, flowID, strings.TrimSpace(flowToken)), workflow.StatusFailed, awaited)
		if reDispatchParent {
			e.enqueueStep(ctx, shardNum, parentStepID)
		}
		return false, nil
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, flowID), "to", workflow.StatusFailed)
	compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, strings.TrimSpace(flowToken))
	e.signalStop(ctx, compositeID, workflow.StatusFailed, awaited)
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

// dynamicSubgraphParent reports whether the given flow is a subgraph child. It also returns the flow's
// awaited flag (piggybacked on the same row read) for the signalStop broadcast gate.
func (e *Engine) dynamicSubgraphParent(ctx context.Context, db *sequel.DB, flowID int) (parentStepID int, isSubgraphChild bool, awaited bool, err error) {
	var surgraphFlowID, surgraphStepID int
	err = db.QueryRowContext(ctx,
		"SELECT surgraph_flow_id, surgraph_step_id, awaited FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&surgraphFlowID, &surgraphStepID, &awaited)
	if err != nil {
		return 0, false, false, errors.Trace(err)
	}
	if surgraphFlowID == 0 || surgraphStepID == 0 {
		return 0, false, awaited, nil
	}
	return surgraphStepID, true, awaited, nil
}

// deliverSubgraphError terminalizes a subgraph child (when there is one, and it is not already terminal)
// and re-arms its parked caller step with the error, so the caller's flow.Subgraph call re-dispatches,
// returns that error, and the caller step fails through its normal disposition (or routes via onError).
//
// Used only by the parked-step wedge sweep - the live failure path is cohort accounting
// (deliverFlowFailureToParent from failStep / processStep). Both call sites therefore pass a child that
// is already terminal, or none at all:
//
//   - childFlowID != 0: the child went terminal but the revive was lost. The child writes are no-ops
//     under the status guard; the point is the parent re-arm.
//   - childFlowID == 0: the child flow is GONE (a worker died between committing the caller's park and
//     inserting the child, or the child was deleted). Nothing to terminalize - failing the caller is the
//     only way out, and it is what the parent re-arm does.
func (e *Engine) deliverSubgraphError(ctx context.Context, shardNum int, childFlowID int, parentStepID int, taskErr error) error {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	errMsg := taskErr.Error()
	reDispatchParent := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		reDispatchParent = false
		// childFlowID==0 is the "caller parked, child flow gone" wedge: a worker committed the park and
		// then died before inserting the child, or the child was deleted. There is no child to terminalize
		// - the only recovery is to fail the CALLER, which the parent re-arm below does (it writes the
		// error onto the parked step, so the re-dispatched flow.Subgraph returns it and the task fails
		// through its normal disposition). Everything child-related must therefore be skipped, not merely
		// aimed at id 0: computeFinalState(0) SELECTs `WHERE flow_id=0`, gets sql.ErrNoRows, and rolls the
		// whole transaction back - so the sweep's designated last-resort recovery could never succeed and
		// the flow hung forever.
		if childFlowID != 0 {
			// Write-first per the flow-terminating-transaction rule: take the child flow row's write lock
			// (the non-indexed `touch` flip) BEFORE computeFinalState reads, so this transaction can never
			// be the read-first half of a SHARED-lock upgrade deadlock on SQLite. The status guard means a
			// zero-row match is the ordinary case here - the wedge sweep calls this precisely to deliver an
			// already-terminal child's error - and then there is nothing to terminalize.
			res, err := tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
				childFlowID,
			)
			if err != nil {
				return errors.Trace(err)
			}
			if n, _ := res.RowsAffected(); n > 0 {
				childFinalState, _, err := e.computeFinalState(ctx, tx, shardNum, childFlowID)
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
			}
		}
		// The parent re-arm. With no child flow this is the transaction's first statement and is itself a
		// write (the parked caller step), so write-first holds on that path too.
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
	// faultDeliverFailureErr simulates this re-dispatch being lost: the child still commits terminal (this
	// runs inside its terminating tx, after the flow row was marked failed), but the parked caller is never
	// re-armed - the wedge shape (caller running+parkedSubgraph with a terminal child) that
	// recoverWedgedSubgraphParks must backstop. Returning (false, nil), not an error, so the child's
	// terminalization is NOT rolled back (an error would retry and deliver, producing no wedge).
	if e.deliverFailureLost(ctx, tx, parentStepID) {
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

// errResumeLost is an in-transaction sentinel: the root flow was terminalized by a concurrent
// Delete/Cancel between resume's pre-tx status read and its own writes, so the transaction must roll
// back (undoing the step re-park/leaf-reset) and the caller must 409 rather than falsely report success.
var errResumeLost = errors.New("resume lost to a concurrent terminalization")

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

	// Normalize the caller's resume data (a struct, map, or nil) into a State; an empty one stays "{}".
	resumeDataJSON := []byte("{}")
	if resumeState, _ := workflow.NewState(data); len(resumeState) > 0 {
		b, _ := json.Marshal(resumeState)
		resumeDataJSON = b
	}

	// Test-only checkpoint: a breakpoint here lets a test freeze resume before its transaction so a racing
	// Delete/Cancel can commit its interrupted->cancelled flip deterministically, then confirm the gate write
	// below rolls this transaction back (409) instead of falsely succeeding. Placed *before* the transaction,
	// not mid-tx: on SQLite the racing Delete would deadlock against this transaction's write lock if frozen
	// inside it. No-op in production.
	e.seams.Checkpoint(ctx, checkpointResumeBeforeFlowWrite)

	lost := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		lost = false
		// faultResumeCommit fails this transaction once, before any write, so the test proves it rolls back
		// atomically (the flow stays interrupted, its steps untouched) and a retry then resumes cleanly.
		if e.seams.IsFault(faultResumeCommit) {
			return errors.New("injected fault: " + faultResumeCommit)
		}
		allStepIDs := append([]any{leafStepID}, parkStepIDs...)
		clearPlaceholders := strings.Repeat("?,", len(allStepIDs)-1) + "?"
		tx.ExecContext(ctx, "UPDATE dwarf_steps SET interrupt_payload=? WHERE step_id IN ("+clearPlaceholders+")",
			append([]any{emptyJSON}, allStepIDs...)...)

		if len(parkStepIDs) > 0 {
			parkPlaceholders := strings.Repeat("?,", len(parkStepIDs)-1) + "?"
			parkArgs := append([]any{workflow.StatusRunning, parkedSubgraph}, parkStepIDs...)
			tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE step_id IN ("+parkPlaceholders+") AND status='"+workflow.StatusInterrupted+"'", parkArgs...)
		}

		tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, resume_data=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusInterrupted+"'",
			workflow.StatusPending, resumeDataJSON, leafStepID)

		// Race gate against a concurrent Delete/Cancel. The step writes above are unconditional (their
		// WHERE status='interrupted' still matches, because Delete/Cancel terminalize the flow row without
		// touching steps), so without this gate a Delete that flipped the root interrupted→cancelled first
		// would let resume re-park ancestors and reset the leaf, then match 0 rows on the flow update below,
		// and still return success - a resume that did not take effect reported as if it had, leaving a
		// transient cancelled-flow-with-non-terminal-steps until the reaper mops it. The root flow row is the
		// serialization point: this guarded write takes its lock and confirms it is still interrupted (touch
		// flips unconditionally, so RowsAffected reflects the status match on every driver, MySQL included).
		// A zero-row match means Delete/Cancel won - roll the whole transaction back and 409, mutually
		// exclusive with Delete/Cancel's own WHERE status='interrupted'. flowID is always the root here
		// (subgraph-child keys were rejected above), which is exactly the row Delete/Cancel terminalize.
		res, gerr := tx.ExecContext(ctx, "UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'", flowID)
		if gerr != nil {
			return errors.Trace(gerr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			lost = true
			return errResumeLost
		}

		for _, chainFlowID := range chainFlowIDs {
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET status=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"' AND (SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"')=0",
				workflow.StatusRunning, chainFlowID, chainFlowID,
			)
		}
		return nil
	})
	if lost {
		return errors.New("flow is not interrupted", http.StatusConflict)
	}
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
