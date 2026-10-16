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
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/cancelmarker"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// createSubgraphFlow creates a subgraph flow for a dynamic subgraph transition. callerStepDepth is the
// caller step's step_depth, so the child's entry step (and thus its whole subtree) is numbered as a
// continuation of the caller (callerStepDepth+1). parkCaller runs first in the child's insert transaction, so
// the caller is parked and its child created atomically; see processStep for why.
func (e *Engine) createSubgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, callerStepDepth int, surgraphStepID int, subgraphWorkflowURL string, subgraphGraph *workflow.Graph, childState workflow.State, baggageJSON []byte, callerTraceParent string, parkCaller func(tx *sequel.Tx) error) (string, error) {
	// engineseams.FaultSubgraphSpawnErr simulates the spawn failing before its transaction: the caller is not
	// parked and no child is inserted, so the caller must be failed cleanly (failAndReturn). Scoped by the
	// child workflow URL.
	if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultSubgraphSpawnErr, subgraphWorkflowURL)) {
		return "", errors.New("injected fault: "+engineseams.FaultSubgraphSpawnErr+" "+subgraphWorkflowURL, http.StatusInternalServerError)
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

	// The child is inserted already surgraph-linked and running, in the transaction that parks the caller, so it
	// can never complete before its parent pointer is set or before the caller is parked (either would lose the
	// parent's revive). The child's "workflow" span is parented to the caller step's span (callerTraceParent),
	// nesting the subtree under the launching task.
	childKey, err := e.createWithGraph(ctx, shardNum, subgraphWorkflowURL, subgraphGraph, childState, 0, "", callerTraceParent, &inherited, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID, parkCaller)
	// engineseams.FaultSubgraphSpawnCommitLost reports an error for a spawn whose transaction COMMITTED - a commit
	// whose acknowledgement was lost - so a test can prove a retried spawn does not create a second child.
	if err == nil && e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultSubgraphSpawnCommitLost, subgraphWorkflowURL)) {
		return "", errors.New("injected fault: "+engineseams.FaultSubgraphSpawnCommitLost+" "+subgraphWorkflowURL, http.StatusInternalServerError)
	}
	return childKey, errors.Trace(err)
}

// completeFlowSequential marks a flow completed when no successor exists.
//
// It does NOT touch the step. Both call sites are downstream of processStep's fenced completion UPDATE, which
// already set status='completed' (and bailed on a zero-row fence match), so the step is terminal before this
// runs. Do not add a trailing "UPDATE dwarf_steps SET status=completed" here: it re-writes that same value at
// the cost of a BEGIN/UPDATE/COMMIT on every flow completion, and its only real effect is to bump the step's
// updated_at to AFTER completeFlow's transaction, inflating the step's recorded task duration (History and the
// FlowRenderer read updated_at - started_at) by the cost of completing the flow. It would also be a
// post-execution step write with neither a status guard nor a lease_seq fence.
func (e *Engine) completeFlowSequential(ctx context.Context, shardNum int, flowID int, flowToken string, workflowURL string) error {
	e.logger.DebugContext(ctx, "Flow completed", "workflow", workflowURL)
	_, err := e.completeFlow(ctx, shardNum, flowID, flowToken)
	return errors.Trace(err)
}

// completedTailsSQL selects a flow's completed tail steps (successor_id=0) for its final state; anyTailsSQL is the
// fallback when it finds none. Each carries the flow's payload_ver, because the final state is computed for flows
// this replica did not check at a claim (Terminate, the wedge sweep, a failStep past the deferral bound); the join
// puts the check on a read it makes anyway. Snapshot, Step and Fork check on their own reads.
const (
	completedTailsSQL = "SELECT s.state, s.changes, s.state_refs, s.lineage_id, f.payload_ver FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.flow_id=? AND s.successor_id=0 AND s.status='" + workflow.StatusCompleted + "' ORDER BY s.step_id"
	anyTailsSQL       = "SELECT s.state, s.changes, s.state_refs, s.lineage_id, f.payload_ver FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.flow_id=? AND s.successor_id=0 ORDER BY s.step_id"
)

// terminalTails is a flow's tail steps as read for its final state: the first tail's state is the merge base,
// and every tail's changes fold onto it. Rows are kept as read and decoded only by a merge that uses them: when
// the first tail is a cohort member, the final state is rebuilt from its cohort and the tails go unused.
type terminalTails struct {
	found       bool
	baseState   []byte
	baseRefs    []byte
	baseLineage int
	changes     [][]byte
}

// scan reads one tail row.
func (t *terminalTails) scan(row interface{ Scan(dest ...any) error }) error {
	var stateJSON, changesJSON, refsJSON []byte
	var lineageID, flowPayloadVer int
	if err := row.Scan(&stateJSON, &changesJSON, &refsJSON, &lineageID, &flowPayloadVer); err != nil {
		return errors.Trace(err)
	}
	if err := checkPayloadVer(flowPayloadVer); err != nil {
		return err
	}
	if !t.found {
		t.found = true
		t.baseState, t.baseRefs, t.baseLineage = stateJSON, refsJSON, lineageID
	}
	t.changes = append(t.changes, changesJSON)
	return nil
}

// queueCompletedTails queues the read of a flow's completed tails into t. On PostgreSQL every tail row is read when
// the batch is sent, which costs one tail's state for a flow that completes normally: a validated graph converges
// every fan-out on its fan-in, so such a flow ends in a single tail.
func queueCompletedTails(b *txBatch, flowID int, t *terminalTails) {
	b.queue(func(res *sequel.BatchResults) error {
		rows, err := res.Query()
		if err != nil {
			return errors.Trace(err)
		}
		for rows.Next() {
			if err := t.scan(rows); err != nil {
				return err
			}
		}
		return errors.Trace(rows.Err())
	}, completedTailsSQL, flowID)
}

// readTerminalTails reads a flow's tails by query into t.
func readTerminalTails(ctx context.Context, db sequel.Executor, query string, flowID int, t *terminalTails) error {
	rows, err := db.QueryContext(ctx, query, flowID)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		if err := t.scan(rows); err != nil {
			return err
		}
	}
	return errors.Trace(rows.Err())
}

// terminalTailsOf returns a flow's tails for its terminal state: its completed tails when it has any, and every
// tail otherwise.
func terminalTailsOf(ctx context.Context, db sequel.Executor, flowID int, completed *terminalTails) (*terminalTails, error) {
	if completed.found {
		return completed, nil
	}
	t := &terminalTails{}
	if err := readTerminalTails(ctx, db, anyTailsSQL, flowID, t); err != nil {
		return nil, errors.Trace(err)
	}
	return t, nil
}

// mergeTerminalSteps computes a flow's terminal state from the execution-DAG tail: the tails' changes fold onto
// the first tail's state through the reducers, as a linear transition's do. A flow with no tails has an empty
// state.
func (e *Engine) mergeTerminalSteps(ctx context.Context, db sequel.Executor, shardNum int, t *terminalTails, workflowURL string, reducers map[string]workflow.Reducer) (workflow.State, error) {
	if !t.found {
		return workflow.State{}, nil
	}
	base, err := workflow.NewState(t.baseState)
	if err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	baseRefs, err := staterefs.Parse(t.baseRefs)
	if err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	changes := make([]workflow.Changes, len(t.changes))
	for i, c := range t.changes {
		changes[i] = changesOf(c)
	}
	// FLATTEN: final_state is a flow-boundary value (a dwarf_flows column that Continue feeds into the next
	// turn, and that outlives this flow's steps - DeleteOnCompletion may reap them). A ref that escaped the
	// flow would dangle, so the terminal merge always materializes, whatever it costs on this one row.
	// It is not a regression either: a large field surviving to the end is copied into final_state today.
	// resolveStateRefs mutates the state map in place, so it gets the live map.
	if _, err := e.resolveStateRefs(ctx, db, shardNum, base, baseRefs, nil, workflowURL); err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	// A tail's changes are its delta onto its input, folded through the reducers like any hand-off. A value a
	// reducer cannot combine is a pure function of stored data, so it is permanent.
	if err := base.ApplyAll(changes, reducers); err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	return base, nil
}

// flowGraph returns a flow's parsed graph and workflow URL, from the graph cache when the flow has been
// dispatched on this replica and from its row otherwise. The returned graph may be shared with concurrent
// dispatches and must not be mutated. A cache hit reads nothing, so it does not prove the flow row still exists.
func (e *Engine) flowGraph(ctx context.Context, db sequel.Executor, shardNum, flowID int) (*workflow.Graph, string, error) {
	if cg, ok := e.cachedFlowGraph(shardNum, flowID); ok {
		return cg.graph, cg.workflowURL, nil
	}
	var workflowURL, graphHash string
	var graphJSON []byte
	err := db.QueryRowContext(ctx,
		"SELECT graph, graph_hash, workflow_url FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&graphJSON, &graphHash, &workflowURL)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	cg, err := e.internGraph(graphJSON, workflowURL, graphHash)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	return cg.graph, workflowURL, nil
}

// computeFinalState computes the merged state for a flow.
func (e *Engine) computeFinalState(ctx context.Context, db sequel.Executor, shardNum, flowID int) ([]byte, string, error) {
	var completed terminalTails
	if err := readTerminalTails(ctx, db, completedTailsSQL, flowID, &completed); err != nil {
		return nil, "", errors.Trace(err)
	}
	return e.computeFinalStateFrom(ctx, db, shardNum, flowID, &completed)
}

// terminalFinalState is computeFinalState for a path that terminalizes a flow whatever its state holds -
// Terminate, Delete, Purge and the wedge sweep. A merge that cannot fold (a permanentError) yields an empty
// final_state rather than an error: the merge would fail identically on every attempt, so propagating it would
// leave the flow impossible to stop. A database error still propagates.
func (e *Engine) terminalFinalState(ctx context.Context, db sequel.Executor, shardNum, flowID int) ([]byte, error) {
	fs, _, err := e.computeFinalState(ctx, db, shardNum, flowID)
	if isPermanentError(err) {
		e.logger.ErrorContext(ctx, "Computing final_state while terminalizing a flow; terminalizing with empty state",
			"flow", keys.CorrelationID(shardNum, flowID), "error", err)
		return []byte("{}"), nil
	}
	return fs, errors.Trace(err)
}

// computeFinalStateFrom is computeFinalState from the flow's completed tails, already read (queueCompletedTails).
func (e *Engine) computeFinalStateFrom(ctx context.Context, db sequel.Executor, shardNum, flowID int, completed *terminalTails) ([]byte, string, error) {
	graph, workflowURL, err := e.flowGraph(ctx, db, shardNum, flowID)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	tails, err := terminalTailsOf(ctx, db, flowID, completed)
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	// When the tails are COHORT MEMBERS (lineage_id != 0), a fan-out resolved with failures and so never reached
	// its fan-in. Rebuild the state the way the convergence would have, rather than from the tail rows - and
	// decide that from the first tail's lineage alone, before decoding, resolving or folding any tail: on a wide
	// failed fan-out that is every branch's row and the forEach array, under Terminate's and failStep's locks.
	//
	// mergeTerminalSteps bases on the FIRST tail's `state` and folds in every tail's `changes` - and that loses
	// the intermediate output of every branch but one. In a multi-step branch (`forEach -> Cell -> Enrich -> J`),
	// Cell's output lives in Enrich's `state`, not in its `changes`: only the first tail's state is consulted, so
	// branch 0's Cell output rides in via the base while branches 1..N-1's is simply dropped. The converged path
	// (fanInRow) has no such hole - it merges EVERY cohort member's `changes` - so the two paths disagreed
	// on what a fan-out's terminal state means, which is exactly the drift the shared strip below exists to stop.
	//
	// Every step of a per-element sub-pipeline inherits the spawn's lineage, so "every cohort member" is one
	// indexed scan and it covers Cell AND Enrich, for every branch.
	//
	// The rebuild starts at the OUTERMOST cohort on the tail's lineage chain, whose spawn is on the trunk. The
	// tail may sit deep inside a nested fan-out whose inner cohort failed, and rebuilding only that inner cohort
	// would drop everything outside it - the flow's base state and every other branch at every level.
	spawns, outermost, err := e.cohortSpawnChain(ctx, db, tails.baseLineage)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	var merged workflow.State
	if outermost != 0 {
		merged, err = e.mergeCohortState(ctx, db, shardNum, flowID, outermost, graph, workflowURL)
	} else {
		merged, err = e.mergeTerminalSteps(ctx, db, shardNum, tails, workflowURL, graph.Reducers())
	}
	if err != nil {
		return nil, "", errors.Trace(err)
	}

	// Drop per-branch forEach bookkeeping, exactly as fanInRow does when a cohort converges - and, like
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
	for _, spawnTask := range spawns {
		stripForEachBookkeeping(merged, graph, spawnTask)
	}

	data, err := json.Marshal(merged)
	if err != nil {
		return nil, "", errors.Trace(err)
	}
	return data, workflowURL, nil
}

// mergeCohortState rebuilds the state of a cohort that resolved with failures, and so never reached its fan-in.
// It runs the SAME merge fanInRow runs at a successful convergence - the spawn step's `state + changes` as
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
//
// A member contributes as it would at a convergence (cohortContributions), so an inner fan-in brings its whole
// cohort, and an inner cohort that never converged is rebuilt in its place.
func (e *Engine) mergeCohortState(ctx context.Context, db sequel.Executor, shardNum, flowID, cohortSpawnID int, graph *workflow.Graph, workflowURL string) (workflow.State, error) {
	var spawnStateJSON, spawnChangesJSON, spawnRefsJSON []byte
	err := db.QueryRowContext(ctx,
		"SELECT state, changes, state_refs FROM dwarf_steps WHERE step_id=?", cohortSpawnID,
	).Scan(&spawnStateJSON, &spawnChangesJSON, &spawnRefsJSON)
	if err == sql.ErrNoRows {
		return workflow.State{}, nil
	}
	if err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	spawnState, err := workflow.NewState(spawnStateJSON)
	if err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	spawnChanges := changesOf(spawnChangesJSON)
	// FLATTEN: final_state is a flow-boundary value, so everything the spawn carried by reference is materialized
	// here - unlike the fan-in, which may pass a carried ref through. resolveStateRefs mutates the map in place.
	spawnRefs, err := staterefs.Parse(spawnRefsJSON)
	if err == nil {
		_, err = e.resolveStateRefs(ctx, db, shardNum, spawnState, spawnRefs, nil, workflowURL)
	}
	if err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	// The failed cohort's terminal state is built the SAME way convergence would (fanInRow): the spawn's
	// changes folded onto its state, as the base the completed members fold onto below.
	merged := spawnState.Clone()
	if err := merged.Apply(spawnChanges, graph.Reducers()); err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}

	contributions, err := e.cohortContributions(ctx, db, flowID, cohortSpawnID, graph)
	if err != nil {
		return workflow.State{}, errors.Trace(err)
	}
	// Folded in ONE pass: a combining reducer re-encodes its accumulator, so folding per member re-encodes a
	// growing value once per member - quadratic across a wide cohort.
	if err := merged.ApplyAll(contributions, graph.Reducers()); err != nil {
		return workflow.State{}, errors.Trace(&permanentError{err})
	}
	return merged, nil
}

// cohortContributions returns what each completed member of a cohort adds to its fold, in fold order. A member
// that is an inner fan-in brings what its own cohort added (memberContribution). A member that spawned an inner
// cohort which never converged - it failed, or the flow stopped first - has no fan-in to carry that cohort's
// work, so the inner cohort's contributions follow the member's own, rebuilt the same way, at any depth.
//
// It runs only on the path that terminalizes a flow whose fan-out never converged, so reading a cohort per
// level is affordable.
func (e *Engine) cohortContributions(ctx context.Context, db sequel.Executor, flowID, cohortSpawnID int, graph *workflow.Graph) ([]workflow.Changes, error) {
	type member struct {
		cohortMember
		cohortSize, arrivals, failures int
	}
	rows, err := db.QueryContext(ctx,
		"SELECT step_id, task_name, status, changes, cohort_size, cohort_arrivals, cohort_failures FROM dwarf_steps"+
			" WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_depth, step_id",
		flowID, cohortSpawnID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var members []member
	for rows.Next() {
		var m member
		var status string
		var changesJSON []byte
		if err := rows.Scan(&m.stepID, &m.taskName, &status, &changesJSON, &m.cohortSize, &m.arrivals, &m.failures); err != nil {
			rows.Close()
			return nil, errors.Trace(err)
		}
		// Only a completed member's changes contribute, matching fanInRow: a failed, terminated, or cancelled
		// branch's partial output is not a fact the flow can build on (an error voids the task's changes). An
		// inner fan-in that did not complete still brings its cohort's part (memberContribution).
		if m.completed = status == workflow.StatusCompleted; m.completed {
			m.changes = changesOf(changesJSON)
		}
		members = append(members, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, errors.Trace(err)
	}

	asMembers := make([]cohortMember, len(members))
	for i, m := range members {
		asMembers[i] = m.cohortMember
	}
	reducers := graph.Reducers()
	var parts map[int]workflow.Changes
	if ids := innerFanInIDs(graph, asMembers); len(ids) > 0 {
		parts, _, err = cohortParts(ctx, db, ids, reducers)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	var contributions []workflow.Changes
	for _, m := range members {
		contribution, ok, err := memberContribution(parts[m.stepID], m.changes, reducers)
		if err != nil {
			return nil, errors.Trace(&permanentError{err})
		}
		if ok {
			contributions = append(contributions, contribution)
		}
		// A spawn's cohort exists only once the spawn completed.
		if m.completed && m.cohortSize > 0 && (m.arrivals < m.cohortSize || m.failures > 0) {
			inner, err := e.cohortContributions(ctx, db, flowID, m.stepID, graph)
			if err != nil {
				return nil, errors.Trace(err)
			}
			contributions = append(contributions, inner...)
		}
	}
	return contributions, nil
}

// errTreeSettled is an in-transaction sentinel: every flow of the tree was already terminal, so terminateSubtree
// rolls back and reports success.
var errTreeSettled = errors.New("tree already terminal")

// errRootNotInterrupted is returned by terminateSubtree called with onlyIfInterrupted when the root is no longer
// interrupted under its row lock - a Resume won the race - and nothing was written.
var errRootNotInterrupted = errors.New("flow is no longer interrupted", http.StatusConflict)

// errTreeGrew is an in-transaction sentinel: terminateSubtree's scan found flows its write set did not cover, so
// the transaction rolls back and re-runs over the larger set.
var errTreeGrew = errors.New("tree grew during terminate")

// maxTerminateRuns bounds terminateSubtree's re-runs. Each re-run needs a flow found by the previous one to have
// committed a subgraph spawn before the re-run locked its steps, so a tree that keeps outrunning it is pathological;
// failing then is safe, since every run rolls back cleanly, and the caller may retry.
const maxTerminateRuns = 16

// terminateSubtree forcefully stops a flow and every non-terminal flow beneath it, in one transaction, and signals
// every flow it stopped. Inside the transaction it terminates the tree's steps (write-first, per the
// flow-terminating-transaction rule), locks the tree's flow rows, terminates the steps again, scans for flows its
// set does not cover, and only then writes each flow's `final_state` and terminal status. A scan that finds new
// flows rolls back and re-runs over the larger set, at most maxTerminateRuns times.
//
// It walks DOWN only. Termination is always initiated at a flow that has no live ancestor to reconcile with - the
// public Terminate is root-only (a subgraph-child key is rejected, not silently widened), and the internal orphan
// sweep starts at a child whose whole ancestor chain is already terminal.
//
// A tree that went terminal concurrently is a no-op, for every caller: the transaction rolls back so the step
// writes never touch a frozen tree, and nothing is counted or signalled. With onlyIfInterrupted (Delete and Purge,
// which may end an interrupted flow but never a running one) the root must still be interrupted under its row
// lock, or the transaction rolls back and errRootNotInterrupted is returned: a Resume that committed first wins.
// commitFault is a test-only seam name, checked before any write so a fired fault proves the whole transaction
// rolls back atomically; pass "" for none.
func (e *Engine) terminateSubtree(ctx context.Context, shardNum, flowID int, flowToken, reason string, onlyIfInterrupted bool, commitFault string) error {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	descendantFlowIDs, descendantCompositeIDs, labelByFlowID, err := e.allSubgraphFlows(ctx, db, shardNum, flowID)
	if err != nil {
		return errors.Trace(err)
	}
	e.seams.Checkpoint(ctx, engineseams.CheckpointTerminateAfterScan)
	allFlowIDs := append([]any{flowID}, descendantFlowIDs...)
	allCompositeIDs := append(
		[]string{keys.New(shardNum, flowID, flowToken)},
		descendantCompositeIDs...,
	)

	reason = strings.TrimSpace(reason)
	settled := false
	for run := 1; ; run++ {
		if run > maxTerminateRuns {
			return errors.New("terminate did not settle after %d runs; the tree kept spawning subgraphs", maxTerminateRuns, http.StatusServiceUnavailable)
		}
		grew := false
		err = db.Transact(ctx, func(tx *sequel.Tx) error {
			settled, grew = false, false
			if commitFault != "" && e.seams.IsFault(commitFault) {
				return errors.New("injected fault: " + commitFault)
			}
			// The flow-id lists are chunked: a tree can hold more flows than a statement can bind.
			terminateSteps := func() {
				for chunk := range slices.Chunk(allFlowIDs, inListChunk) {
					ph, ids := inList(chunk)
					tx.ExecContext(ctx,
						"UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE flow_id IN ("+ph+") AND status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusInterrupted+"', '"+workflow.StatusRunning+"')",
						append([]any{workflow.StatusTerminated, parkedNone}, ids...)...,
					)
				}
			}
			// Write-first (the step-terminate UPDATE) per the flow-terminating-transaction rule, and steps before
			// flows, the order Resume and handleInterrupt take the same rows in. The second step UPDATE below takes
			// steps AFTER flows; it reaches only steps created in between, so it can cross only a transaction that
			// took such a just-created step first.
			terminateSteps()
			e.seams.Checkpoint(ctx, engineseams.CheckpointTerminateAfterStepsLocked)
			// Lock the flow rows, then terminate the steps again. A transition locks its flow row before it inserts
			// a step, so once these rows are held no step can be added to these flows - but one that committed
			// between the UPDATE above and this lock exists now and was not covered by it. The second UPDATE
			// catches it, so no step of the tree is left running, and none can go on to spawn a subgraph.
			var locked int64
			for chunk := range slices.Chunk(allFlowIDs, inListChunk) {
				ph, ids := inList(chunk)
				lockRes, err := tx.ExecContext(ctx,
					"UPDATE dwarf_flows SET touch=1-touch WHERE flow_id IN ("+ph+") AND status NOT IN ("+terminalStatusesSQL+")",
					ids...,
				)
				if err != nil {
					return errors.Trace(err)
				}
				n, _ := lockRes.RowsAffected()
				locked += n
			}
			if locked == 0 {
				settled = true
				return errTreeSettled
			}
			if onlyIfInterrupted {
				// The root's row is locked now, so its status cannot move under this check. touch flips, so the
				// count is honest on MySQL too.
				gateRes, err := tx.ExecContext(ctx,
					"UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'",
					flowID,
				)
				if err != nil {
					return errors.Trace(err)
				}
				if n, _ := gateRes.RowsAffected(); n == 0 {
					return errRootNotInterrupted
				}
			}
			terminateSteps()
			// The scan before this transaction misses a child spawned since. A spawn parks its caller step in the
			// transaction that inserts the child, and every live step of these flows is now locked here, so each
			// spawn from them has either committed or will find its caller terminated: ONE scan settles them. It is
			// the transaction's first plain read, so on a snapshot-per-transaction dialect (MySQL's REPEATABLE READ)
			// its snapshot is taken now - a second scan in this transaction would reuse it and miss a grandchild
			// spawned in between. A scan that finds new flows therefore rolls back, and the whole transaction re-runs
			// over the larger set.
			rescannedIDs, rescannedComposite, rescannedLabels, err := e.allSubgraphFlows(ctx, tx, shardNum, flowID)
			if err != nil {
				return errors.Trace(err)
			}
			e.seams.Checkpoint(ctx, engineseams.CheckpointTerminateAfterRescan)
			// The scan returns the tree's live flows. A flow of the set that is missing from it ended or was
			// removed after the first scan - a flow.Retry on a subgraph caller deletes its old child - and is
			// dropped, so the final-state reads below do not look for a row that is gone. The root stays: its own
			// status is what the flow UPDATE's guard settles.
			live := make(map[int]bool, len(rescannedIDs)+1)
			live[flowID] = true
			for _, fid := range rescannedIDs {
				live[fid.(int)] = true
			}
			keptIDs, keptComposite := allFlowIDs[:0:0], allCompositeIDs[:0:0]
			for i, fid := range allFlowIDs {
				if live[fid.(int)] {
					keptIDs = append(keptIDs, fid)
					keptComposite = append(keptComposite, allCompositeIDs[i])
				}
			}
			allFlowIDs, allCompositeIDs = keptIDs, keptComposite
			known := make(map[int]bool, len(allFlowIDs))
			for _, fid := range allFlowIDs {
				known[fid.(int)] = true
			}
			for i, fid := range rescannedIDs {
				if known[fid.(int)] {
					continue
				}
				grew = true
				allFlowIDs = append(allFlowIDs, fid)
				allCompositeIDs = append(allCompositeIDs, rescannedComposite[i])
				labelByFlowID[fid.(int)] = rescannedLabels[fid.(int)]
			}
			if grew {
				return errTreeGrew
			}

			finalStates := make([][]byte, len(allFlowIDs))
			for i, fid := range allFlowIDs {
				fs, err := e.terminalFinalState(ctx, tx, shardNum, fid.(int))
				if err != nil {
					return errors.Trace(err)
				}
				finalStates[i] = fs
			}

			// One UPDATE per flow, each binding its own final state, sent as one batch. A single UPDATE choosing
			// the state by a CASE binds two parameters per flow and overruns SQL Server's ceiling at ~700 flows.
			// cancel_reason is the one reason column both Terminate and Cancel write into; the two are told apart
			// by status at the Go API level (FlowOutcome's TerminateReason vs CancelReason, in snapshot()).
			batch := newTxBatch(ctx, tx)
			var ended int64
			for i, fid := range allFlowIDs {
				batch.queue(func(res *sequel.BatchResults) error {
					r, err := res.Exec()
					if err != nil {
						return errors.Trace(err)
					}
					n, _ := r.RowsAffected()
					ended += n
					return nil
				},
					"UPDATE dwarf_flows SET final_state=?, status=?, cancel_reason=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ("+terminalStatusesSQL+")",
					finalStates[i], workflow.StatusTerminated, reason, fid,
				)
			}
			if err := batch.flush(); err != nil {
				return err
			}
			if ended == 0 {
				settled = true
				return errTreeSettled
			}
			return nil
		})
		if grew {
			continue
		}
		break
	}
	if settled {
		return nil
	}
	if errors.Is(err, errRootNotInterrupted) {
		return errRootNotInterrupted
	}
	if err != nil {
		return errors.Trace(err)
	}

	for i, cid := range allCompositeIDs {
		fid := allFlowIDs[i].(int)
		e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, fid), "to", workflow.StatusTerminated)
		// Counted per flow, alongside the signal and on the same set, because every one of these was started
		// (createWithGraph counts subgraph children too) and the in-flight panel is started minus ended.
		// The set is the flows the in-transaction rescan found live under the flow-row lock, plus the root,
		// which the rescan keeps regardless. A root that terminalized between the first scan and the lock is
		// excluded by the UPDATE's own status guard but still counted here, so a terminate racing the root's
		// completion can over-count by one. That window is microseconds, so it is not worth a round trip to
		// close.
		label := labelByFlowID[fid]
		e.metricFlowEnded(ctx, label.workflowURL, workflow.StatusTerminated, label.priority, shardNum)
		e.signalStop(ctx, cid, workflow.StatusTerminated)
	}
	return nil
}

// cohortSpawnChain walks a step's lineage chain upward and returns the task name of every fan-out spawn whose
// cohort the step is inside - innermost first - and the step_id of the outermost spawn (0 for a step outside
// any cohort). `lineage_id` names the spawn step of the cohort a step belongs
// to (0 outside any cohort), and a spawn is itself a step with its own lineage, so nested fan-outs form a
// chain. Each level costs one primary-key lookup and the chain is as deep as the graph's fan-out nesting
// (typically zero or one), on a terminal path.
func (e *Engine) cohortSpawnChain(ctx context.Context, db sequel.Executor, lineageID int) (tasks []string, outermost int, err error) {
	seen := map[int]bool{}
	for lineageID != 0 && !seen[lineageID] {
		seen[lineageID] = true // a malformed chain must not spin
		var taskName string
		var next int
		err := db.QueryRowContext(ctx,
			"SELECT task_name, lineage_id FROM dwarf_steps WHERE step_id=?", lineageID,
		).Scan(&taskName, &next)
		if err == sql.ErrNoRows {
			return tasks, outermost, nil
		}
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		tasks = append(tasks, strings.TrimSpace(taskName))
		outermost = lineageID
		lineageID = next
	}
	return tasks, outermost, nil
}

// stripForEachBookkeeping deletes the per-branch fields the engine injects into a forEach branch's state - the
// element (`<as>`) and its ordinal context (`<as>Index`, `<as>Count`) - from a materialized state map, for the
// ONE cohort spawned by spawnTask. They are a branch's private context and have no meaning once that cohort is
// behind the flow: with the Replace reducer one arbitrary branch's element would otherwise ride forward.
//
// Scoping to the spawning task is load-bearing in both directions. Do not strip every forEach in the graph: that
// (a) makes the three names of every `as` globally reserved, silently deleting a same-named field a task wrote,
// and (b) breaks nesting - at an INNER fan-in it would also delete the OUTER cohort's bookkeeping, so a step
// still inside the outer branch could no longer see which element it was working on. The names are reserved
// only WITHIN their own cohort; a workflow wanting the element past that cohort's fan-in forwards it under a different key.
//
// Applied at both places a cohort's state leaves the cohort: fanInRow (the convergence) and
// computeFinalState (a fan-out that never converged), so the two cannot drift on what a fan-out's state means.
//
// It returns the names it deleted, so a caller minting the stripped state can keep them from being re-emitted
// through a carried ref.
func stripForEachBookkeeping(state workflow.State, graph *workflow.Graph, spawnTask string) []string {
	if state.Len() == 0 || spawnTask == "" {
		return nil
	}
	var names []string
	for _, tr := range graph.Transitions() {
		if tr.From != spawnTask || tr.ForEach == "" || tr.As == "" {
			continue
		}
		names = append(names, tr.As, tr.As+"Index", tr.As+"Count")
	}
	state.Del(names...)
	return names
}

// completeFlow transitions a flow to completed and propagates to surgraph.
func (e *Engine) completeFlow(ctx context.Context, shardNum int, flowID int, flowToken string) (bool, error) {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}
	var workflowURL, currentStatus string
	var finalStateJSON []byte
	var surgraphFlowID, surgraphStepID, priority int
	var deleteOnCompletion bool
	completed := false
	// Test checkpoint: a breakpoint here freezes completion just before its transaction (holding no lock, so a
	// racing Terminate can commit), letting a test drive the completeFlow-vs-Terminate race in either order.
	// Placed before the transaction, not inside, for the same SQLite-deadlock reason as
	// engineseams.CheckpointResumeBeforeFlowWrite.
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeCompleteFlowWrite)
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		completed = false
		// engineseams.FaultCompleteFlowCommit fails the flow-completion transaction after processStep has already
		// marked the terminal step completed, so the test proves the write-first ordering + recovery defer
		// keep the flow from stranding `running` with every step terminal. Non-retryable (a plain error).
		if e.seams.IsFault(engineseams.FaultCompleteFlowCommit) {
			return errors.New("injected fault: " + engineseams.FaultCompleteFlowCommit)
		}
		// Write-first: take the flow row's write lock before computeFinalState's reads. Without this the
		// transaction is read-first (SELECT graph + terminal steps, then UPDATE), and on SQLite with
		// cache=shared two concurrent completions both hold SHARED locks and deadlock on the upgrade to
		// write - which under load exhausts Transact's retries and errors. Because the terminal step is
		// already marked completed by processStep, the lease recovery (which only resets running rows)
		// cannot re-dispatch it, leaving the flow stranded 'running' with all steps terminal (an orphan
		// flow). Mirrors the transition transaction and fireFanInDirect, which write first for the same reason.
		// Lock-grab flips the non-indexed `touch`, not `updated_at`: the flow's `updated_at` moves only
		// on the genuine status transition below, so idx_dwarf_flows_status is not churned twice per
		// completion. `touch` always changes, so a later RowsAffected check stays meaningful.
		//
		// The lock grab also reads back the surgraph linkage, the disposable flag and the priority - needed for
		// the post-tx surgraph revival, the atomic disposable delete below and the ended counter - and it goes out in one statement batch with
		// the read of the flow's completed tails, which the final state is merged from. It stays unguarded on
		// status: the status gate is the later UPDATE, and the status read here drives the revive re-drive.
		const lockGrab = "UPDATE dwarf_flows SET touch=1-touch"
		const lockWhere = " WHERE flow_id=?"
		batch := newTxBatch(ctx, tx)
		readLocked := func(res *sequel.BatchResults) error {
			// A pipelined batch scans with the PostgreSQL driver's own conversions, which do not turn a SMALLINT
			// into a bool, so the flag is scanned as the integer it is stored as.
			var disposable int
			err := res.QueryRow().Scan(&surgraphFlowID, &surgraphStepID, &disposable, &currentStatus, &priority)
			deleteOnCompletion = disposable != 0
			return errors.Trace(err)
		}
		switch tx.DriverName() {
		case "pgx", "sqlite":
			batch.queue(readLocked, lockGrab+lockWhere+" RETURNING surgraph_flow_id, surgraph_step_id, delete_on_completion, status, priority", flowID)
		case "mssql":
			batch.queue(readLocked, lockGrab+" OUTPUT INSERTED.surgraph_flow_id, INSERTED.surgraph_step_id, INSERTED.delete_on_completion, INSERTED.status, INSERTED.priority"+lockWhere, flowID)
		default:
			// MySQL lacks RETURNING: lock, then read the row this transaction now holds.
			batch.queue(execOnly, lockGrab+lockWhere, flowID)
			batch.queue(readLocked, "SELECT surgraph_flow_id, surgraph_step_id, delete_on_completion, status, priority FROM dwarf_flows WHERE flow_id=?", flowID)
		}
		var tails terminalTails
		queueCompletedTails(batch, flowID, &tails)
		if err := batch.flush(); err != nil {
			return err
		}
		fs, wf, err := e.computeFinalStateFrom(ctx, tx, shardNum, flowID, &tails)
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
			"UPDATE dwarf_flows SET status=?, final_state=?, delete_after_ms=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ("+terminalStatusesSQL+")",
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
		// on the attempt that transitioned the flow. A failed/terminated/cancelled flow (status != completed)
		// gets no revive here; its parent is handled by failure delivery / the Terminate cascade.
		if currentStatus == workflow.StatusCompleted && surgraphFlowID != 0 {
			if rerr := e.completeSurgraphFlow(ctx, shardNum, surgraphFlowID, surgraphStepID, finalStateJSON); rerr != nil {
				return false, errors.Trace(rerr)
			}
		}
		return false, nil
	}

	e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, flowID), "to", workflow.StatusCompleted)
	e.metricFlowEnded(ctx, workflowURL, workflow.StatusCompleted, priority, shardNum)
	compositeID := keys.New(shardNum, flowID, flowToken)

	e.signalStop(ctx, compositeID, workflow.StatusCompleted)

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
	// engineseams.FaultSubgraphReviveLost simulates the revive being lost after the child went terminal (the wedge the
	// parked-step sweep exists to catch): the caller step stays running+parkedSubgraph with no live child,
	// so the test proves recoverWedgedSubgraphParks re-drives the release. Returns nil so the caller path
	// believes it succeeded, exactly as a lost revive would look.
	if e.seams.IsFault(engineseams.FaultSubgraphReviveLost) {
		return nil
	}
	// engineseams.FaultCompleteSurgraphErr makes the revive fail with a synthetic non-contention error (consumed per attempt),
	// so a test can prove persist re-drives it on retry rather than losing it - the whole point of the re-drive.
	if e.seams.IsFault(engineseams.FaultCompleteSurgraphErr) {
		return errors.New("injected fault: " + engineseams.FaultCompleteSurgraphErr)
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
	// revive, so a test can Terminate the caller in exactly the window the revive's running+parkedSubgraph guard
	// exists to survive (the revive must not resurrect the just-terminated caller).
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeReviveWrite)
	reDispatch := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		reDispatch = false
		// Guard the revive on the exact park state (running + parkedSubgraph), mirroring
		// deliverSubgraphError. Without it, a Terminate that cascaded to this caller step (between the child's
		// completion and this revive) would be resurrected to pending: keying on step_id alone overwrites
		// the just-terminated row. The guard also subsumes the "step still live" check — a step that is no
		// longer running/parked matches no row — and the rows-affected gate keeps Enqueue off a no-op.
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=1, subgraph_result=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND parked=?",
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
	if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultFailStepErr, taskName)) {
		return false, errors.New("injected fault: " + engineseams.FaultFailStepErr)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}

	// A subgraph child's failure is surfaced to the parent's flow.Subgraph call (via the parked caller step,
	// carried in subgraph_error) rather than notified directly - but only when the child flow ACTUALLY fails,
	// which for a child with a fan-out is after its cohort fully resolves (the same cohort accounting below
	// that governs a top-level flow), never eagerly on the first branch error. Failing the child eagerly
	// while a sibling branch is still live would strand that sibling and any subgraph descendants it parked
	// on: the interrupt/resume/terminate tree walks all skip a terminal flow, so nothing could ever release
	// them. parentStepID>0 iff this flow is a subgraph child.
	parentStepID, lostLabel, err := e.dynamicSubgraphParent(ctx, db, flowID)
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
	// A cancellation settles the step as cancelled. Either value differs from the running/completed row it
	// replaces, which is what keeps the RowsAffected fence below honest on MySQL.
	stepCancelled := workflow.IsCancelled(taskErr)
	stepStatus := workflow.StatusFailed
	if stepCancelled {
		stepStatus = workflow.StatusCancelled
	}
	failFlow := false
	var loss flowLoss
	// Serialize a failing BRANCH's arrival through the same per-peer stripe the success path uses (keyed on
	// its cohort's spawn, stepLineageID), so a failed sibling parks on the stripe holding no connection
	// rather than piling onto the spawn row's write lock. A trunk step (lineage 0) fails the flow directly,
	// with no cohort to contend, so it takes nothing. Only this bottom level is locked; propagateCohortFailure's
	// ancestor bumps walk UP and stay DB-serialized, which is what keeps the stripe leaf-only and cycle-free.
	// A simple defer suffices here: nothing in this transaction or its post-commit tail re-locks the stripe.
	//
	// Parked on holding NO TURN, the same as the success path: every caller of this reaches it holding one,
	// and a turn held across a lock is the occupancy the stripe exists to remove, one level up. The wait keeps
	// the step's lease alive; if it finds the step taken back, this worker no longer owns it - fenced.
	if stepLineageID != 0 {
		stripe := &e.cohortLocks[cohortLockStripe(shardNum, stepLineageID)]
		held := false
		yieldHeldTurn(ctx, func() {
			held = e.lockCohortStripe(ctx, stripe, db, shardNum, stepID, leaseSeq)
		})
		if !held {
			return true, nil
		}
		defer stripe.unlock()
	}
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		fenced = false
		// A trunk step (lineage_id==0) has no concurrent sibling, so its failure fails the flow at once.
		// A fan-out branch (lineage_id!=0) instead defers to cohort accounting below: siblings run to
		// completion and the flow fails only once the whole cohort arrives with cohort_failures>0.
		failFlow = stepLineageID == 0
		flowCancelled := stepCancelled
		loss = flowLoss{}
		// Fence the fail on BOTH our lease generation AND the two states a worker legitimately fails a step
		// from: `running` (a task error, or the step-completion write itself failing) and `completed` (a
		// transition/eval failure that failOnPersistError fails the already-completed step for, to escape the
		// re-execution loop). The lease fence alone guards the zombie "late error → healthy-flow kill" hazard;
		// the status guard - which every sibling post-execution write carries and this one was missing - closes
		// the rest. Without it a step a racing Terminate just terminated (terminateSubtree does NOT bump lease_seq,
		// so it still matches our generation) is rewritten terminated→failed, violating step immutability and
		// seeding a phantom branch failure a later Fork re-derives from step status; and a step recovery reset to
		// `pending` (also lease_seq-preserving) would be terminalized out from under the peer re-claiming it. This
		// is the first write in the transaction, so a zero-row match means nothing was written - commit the empty
		// tx and report fenced so the caller abandons without failing a flow that is terminated or that a peer is
		// re-running.
		res, uerr := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, error=?, updated_at=NOW_UTC() WHERE step_id=? AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusCompleted+"') AND lease_seq=?",
			stepStatus, parkedNone, errMsg, stepID, leaseSeq,
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
			failFlow, flowCancelled, err = e.propagateCohortFailure(ctx, tx, stepLineageID, stepCancelled)
			if err != nil {
				return errors.Trace(err)
			}
		}
		if failFlow {
			var err error
			loss, err = e.failFlowTx(ctx, tx, shardNum, flowID, flowCancelled, taskErr, parentStepID)
			if err != nil {
				return errors.Trace(err)
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
	// The step is now failed (or cancelled) regardless of whether the whole flow fails - count it.
	e.metricStepExecuted(ctx, taskName, stepStatus, shardNum)

	if !failFlow {
		return false, nil
	}
	e.endLostFlow(ctx, shardNum, flowID, flowToken, lostLabel, loss)
	return false, nil
}

// flowLoss is what failFlowTx decided inside its transaction, for endLostFlow to act on after the commit.
type flowLoss struct {
	status           string // failed or cancelled; empty when no flow was failed
	parentStepID     int    // the parked caller step of a subgraph child, else 0
	reDispatchParent bool   // whether that caller step must be offered for re-dispatch
}

// failFlowTx terminalizes a flow as a loss inside tx: it records the flow's final state, status and error and, for
// a subgraph child (parentStepID != 0), delivers the failure to the parked caller step. It is shared by failStep
// and by a completing last arrival that resolves a cohort carrying failures, so the two cannot disagree on what a
// lost flow looks like. lastLoss is the resolving step's own error, or nil for a completing arrival.
func (e *Engine) failFlowTx(ctx context.Context, tx sequel.Executor, shardNum, flowID int, cancelled bool, lastLoss error, parentStepID int) (flowLoss, error) {
	loss := flowLoss{status: workflow.StatusFailed, parentStepID: parentStepID}
	if cancelled {
		loss.status = workflow.StatusCancelled
	}
	lossErr := e.flowLossError(ctx, tx, flowID, cancelled, lastLoss)
	finalStateJSON, _, err := e.computeFinalState(ctx, tx, shardNum, flowID)
	if err == nil && e.seams.IsFault(engineseams.FaultFailFlowFinalState) {
		err = errors.New("injected fault: " + engineseams.FaultFailFlowFinalState)
	}
	if err != nil {
		// A fuse that can itself be poisoned is not a fuse. failStep is the ONLY way out of a step whose outcome
		// cannot be persisted (failOnPersistError uses it as the clean, payload-free write that tells a permanent
		// failure from an unreachable database) - and computeFinalState is the one part of it that reads other
		// steps' payloads. If a value that cannot be read back had already landed in a row, this merge would die
		// the same way as the write being failed, failStep would report an error, and the classifier would misread
		// it as "the database is down" - leaving the step to re-execute forever. So the flow still terminalizes,
		// with an empty final_state and the error logged.
		e.logger.ErrorContext(ctx, "Computing final_state while failing a flow; terminalizing with empty state",
			"flow", keys.CorrelationID(shardNum, flowID), "error", err)
		finalStateJSON = []byte("{}")
	}
	tx.ExecContext(ctx,
		"UPDATE dwarf_flows SET final_state=?, status=?, error=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ("+terminalStatusesSQL+")",
		finalStateJSON, loss.status, flowErrorText(cancelled, lossErr), flowID,
	)
	if parentStepID != 0 {
		loss.reDispatchParent, err = e.deliverFlowFailureToParent(ctx, tx, parentStepID, lossErr)
		if err != nil {
			return flowLoss{}, errors.Trace(err)
		}
	}
	return loss, nil
}

// endLostFlow is failFlowTx's post-commit tail. It counts the flow ended - a subgraph child included, since every
// flow was counted as started - and wakes any Await on it, including on a subgraph child's key (legal read-only
// introspection), whose failure otherwise travels only to its parent's flow.Subgraph call.
func (e *Engine) endLostFlow(ctx context.Context, shardNum, flowID int, flowToken string, label flowLabel, loss flowLoss) {
	e.metricFlowEnded(ctx, label.workflowURL, loss.status, label.priority, shardNum)
	e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, flowID), "to", loss.status)
	e.signalStop(ctx, keys.New(shardNum, flowID, flowToken), loss.status)
	if loss.reDispatchParent {
		e.enqueueStep(ctx, shardNum, loss.parentStepID)
	}
}

// propagateCohortFailure bumps a spawn step's cohort_arrivals and cohort_failures.
// cancelled says whether the loss arriving is a cancellation, and is counted in cohort_cancellations. When a
// cohort resolves, its own loss - carried to the enclosing cohort, or to the flow - is a cancellation only if
// every failure in it was one, so a single real error anywhere below makes the whole resolution a failure.
// Returns whether the flow fails, and if so whether it resolves cancelled.
func (e *Engine) propagateCohortFailure(ctx context.Context, tx sequel.Executor, spawnStepID int, cancelled bool) (failFlow bool, flowCancelled bool, err error) {
	current := spawnStepID
	for {
		cancelledInc := 0
		if cancelled {
			cancelledInc = 1
		}
		_, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals + 1, cohort_failures = cohort_failures + 1, cohort_cancellations = cohort_cancellations + ? WHERE step_id=?",
			cancelledInc, current,
		)
		if err != nil {
			return false, false, errors.Trace(err)
		}
		var arrivals, size, failures, cancellations, lineageID int
		err = tx.QueryRowContext(ctx,
			"SELECT cohort_arrivals, cohort_size, cohort_failures, cohort_cancellations, lineage_id FROM dwarf_steps WHERE step_id=?",
			current,
		).Scan(&arrivals, &size, &failures, &cancellations, &lineageID)
		if err != nil {
			return false, false, errors.Trace(err)
		}
		if arrivals < size {
			return false, false, nil
		}
		cancelled = cancellations == failures
		if lineageID == 0 {
			return true, cancelled, nil
		}
		current = lineageID
	}
}

// flowErrorText is the error column's text for a flow resolving as a loss: the error for a failed flow, and
// nothing for a cancelled one, whose explanation is its cancel reason - as a terminated flow's is. Every reader
// then agrees that a cancelled flow carries no error.
func flowErrorText(cancelled bool, lossErr error) string {
	if cancelled {
		return ""
	}
	return sanitizeErrorMessage(lossErr.Error())
}

// flowLossError picks the error a flow resolving as a loss records, and delivers to its surgraph caller. A
// cancelled flow reports the cancellation. A failed flow reports a real failure even when the loss that
// resolved it was a cancellation - a real error was mixed into its cohort, and a cancellation on a failed flow
// would have IsCancelled report true for it. lastLoss is the resolving step's own error, or nil when the
// resolver is a completing arrival that has none.
func (e *Engine) flowLossError(ctx context.Context, tx sequel.Executor, flowID int, cancelled bool, lastLoss error) error {
	if cancelled {
		if workflow.IsCancelled(lastLoss) {
			return lastLoss
		}
		var reason string
		tx.QueryRowContext(ctx, "SELECT cancel_reason FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&reason)
		return cancelmarker.New(strings.TrimSpace(reason))
	}
	if lastLoss != nil && !workflow.IsCancelled(lastLoss) {
		return lastLoss
	}
	var sample string
	tx.QueryRowContext(ctx,
		"SELECT error FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusFailed+"' AND error!='' ORDER BY step_id LIMIT_OFFSET(1, 0)",
		flowID,
	).Scan(&sample)
	sample = strings.TrimSpace(sample)
	if sample == "" {
		sample = "cohort failed"
	}
	return errors.New("%s", sample)
}

// dynamicSubgraphParent returns the caller step of the given flow when it is a subgraph child, else 0.
// The flow's workflow URL and priority ride this read rather than costing one of their own: failStep needs
// them to label the flow's termination, and this is the flow row it was already fetching.
func (e *Engine) dynamicSubgraphParent(ctx context.Context, db *sequel.DB, flowID int) (parentStepID int, label flowLabel, err error) {
	var surgraphFlowID, surgraphStepID int
	err = db.QueryRowContext(ctx,
		"SELECT surgraph_flow_id, surgraph_step_id, workflow_url, priority FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&surgraphFlowID, &surgraphStepID, &label.workflowURL, &label.priority)
	if err != nil {
		return 0, flowLabel{}, errors.Trace(err)
	}
	if surgraphFlowID == 0 {
		return 0, label, nil
	}
	return surgraphStepID, label, nil
}

// deliverSubgraphError re-arms a parked caller step with the error, so the caller's flow.Subgraph call
// re-dispatches, returns that error, and the caller step fails through its normal disposition (or routes via
// onError).
//
// Used only by the parked-step wedge sweep - the live failure path is cohort accounting
// (deliverFlowFailureToParent from failStep / processStep) - and the sweep calls it only for a caller whose
// latest child is already terminal or gone, so there is no child to terminalize here:
//
//   - a terminal child: it went terminal but the revive was lost, and the re-arm is the whole recovery;
//   - no child: the child flow is GONE. The caller's park and the child's insert commit in one transaction,
//     so only a bug or a hand-deleted row leaves this, and failing the caller is the only way out.
func (e *Engine) deliverSubgraphError(ctx context.Context, shardNum int, parentStepID int, taskErr error) error {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	reDispatchParent := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		// The parent re-arm is the transaction's first statement and is itself a write (the parked caller
		// step), so it is write-first.
		var derr error
		reDispatchParent, derr = e.deliverFlowFailureToParent(ctx, tx, parentStepID, taskErr)
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

// deliverFlowFailureToParent re-arms a parked parent caller step with a failed or cancelled child flow's
// error, so the parent's flow.Subgraph call re-dispatches and observes it (yield=false, err set from
// subgraph_error). Called inside the child flow's terminating transaction, after the child flow row has been
// terminalized. Returns true when the caller step was still parked and got re-armed - the caller then
// enqueues it after the transaction. Returns false for a top-level flow (parentStepID==0) or a caller step no
// longer parked (already resolved, terminated, or retried away), in which case there is nothing to re-dispatch.
//
// The error is stored as its TracedError JSON, stack stripped, not as its message: flow.Subgraph decodes it,
// so its status code and properties reach the caller - the cancellation marker among them, which is what
// lets workflow.IsCancelled recognize a cancelled child. A bare message would arrive as a new error without
// any of them.
func (e *Engine) deliverFlowFailureToParent(ctx context.Context, tx sequel.Executor, parentStepID int, childErr error) (bool, error) {
	if parentStepID == 0 {
		return false, nil
	}
	// engineseams.FaultDeliverFailureErr simulates this re-dispatch being lost: the child still commits terminal (this
	// runs inside its terminating tx, after the flow row was marked failed), but the parked caller is never
	// re-armed - the wedge shape (caller running+parkedSubgraph with a terminal child) that
	// recoverWedgedSubgraphParks must backstop. Returning (false, nil), not an error, so the child's
	// terminalization is NOT rolled back (an error would retry and deliver, producing no wedge).
	//
	// It fires either unscoped (the single-level lost-delivery pin) OR scoped to the parked caller's task
	// name, so a multi-level test drops the delivery at one chosen level while the other level's succeeds -
	// each caller task's FIRST delivery is lost and its sweep-driven re-delivery goes through, letting a
	// depth-N tree wedge (and unwedge) at every level. This is the one fault consult needing a DB read to
	// build its scope, so the Enabled gate short-circuits ahead of the SELECT: a live binary runs neither the
	// query nor any consult.
	if e.seams.Enabled() {
		lost := e.seams.IsFault(engineseams.FaultDeliverFailureErr)
		if !lost {
			var taskName string
			if err := tx.QueryRowContext(ctx, "SELECT task_name FROM dwarf_steps WHERE step_id=?", parentStepID).Scan(&taskName); err == nil {
				lost = e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultDeliverFailureErr, strings.TrimSpace(taskName)))
			}
		}
		if lost {
			return false, nil
		}
	}
	delivered := *errors.Convert(childErr)
	delivered.Stack = nil
	encoded, err := json.Marshal(&delivered)
	if err != nil {
		return false, errors.Trace(err)
	}
	res, err := tx.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=1, subgraph_error=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND parked=?",
		workflow.StatusPending, parkedNone, string(encoded), parentStepID, parkedSubgraph,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// allSubgraphFlows finds all active (non-terminal) descendant subgraph flows of flowID. It fetches the
// whole tree in one scan via the denormalized root_flow_id, then BFS in memory through non-terminal nodes
// only, one round-trip regardless of depth. root_flow_id gives tree membership; surgraph_flow_id
// gives the parent/child structure the BFS walks.
//
// labelByFlowID carries every scanned flow's workflow_url and priority, keyed by flow id and covering the ROOT
// as well as the returned descendants, so a caller terminalizing the tree can count each flow under the
// attributes its start was counted under. It rides the scan that was already happening: the columns are two
// more in a projection, not a query.
func (e *Engine) allSubgraphFlows(ctx context.Context, db sequel.Executor, shardNum int, flowID int) (flowIDs []any, compositeFlowIDs []string, labelByFlowID map[int]flowLabel, err error) {
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, flow_token, surgraph_flow_id, status, workflow_url, priority FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)",
		flowID,
	)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	type node struct {
		token    string
		terminal bool
		label    flowLabel
	}
	byID := map[int]node{}
	childrenByParent := map[int][]int{}
	for rows.Next() {
		var id, parent, priority int
		var token, status, workflowURL string
		if err := rows.Scan(&id, &token, &parent, &status, &workflowURL, &priority); err != nil {
			rows.Close()
			return nil, nil, nil, errors.Trace(err)
		}
		term := workflow.IsTerminalStatus(status)
		byID[id] = node{token: token, terminal: term, label: flowLabel{workflowURL: workflowURL, priority: priority}}
		if parent != 0 {
			childrenByParent[parent] = append(childrenByParent[parent], id)
		}
	}
	rows.Close()
	// A truncated read (mid-stream error) would make the tree walk act on a partial tree; outside a transaction
	// no Transact latch backstops it, so the check is explicit.
	if err := rows.Err(); err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	queue := []int{flowID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, child := range childrenByParent[cur] {
			n := byID[child]
			if n.terminal { // a terminal node is neither collected nor descended through
				continue
			}
			flowIDs = append(flowIDs, child)
			compositeFlowIDs = append(compositeFlowIDs, keys.New(shardNum, child, n.token))
			queue = append(queue, child)
		}
	}
	// Every flow in the tree, root included - not just the collected descendants - because the caller
	// terminalizes the root too and each flow carries its own workflow name.
	labelByFlowID = make(map[int]flowLabel, len(byID))
	for id, n := range byID {
		labelByFlowID[id] = n.label
	}
	return flowIDs, compositeFlowIDs, labelByFlowID, nil
}

// flowLabel is what a flow's lifecycle counters are attributed by, besides the shard and status.
type flowLabel struct {
	workflowURL string
	priority    int
}

// interruptedSubgraphChain walks down from a flow through interrupted subgraph steps to find the leaf. It
// loads the tree once via root_flow_id (structure + per-flow tokens/status) and each flow's interrupted-leaf
// step in one batched query (SQL does the earliest-updated_at ordering, so there is no Go-side timestamp
// comparison), then descends in memory - one round-trip per concern regardless of depth. The leaf is picked the SAME way Snapshot does (earliest updated_at, step_id tiebreak),
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
		tokenByID[id] = token
		if ssid != 0 && status == workflow.StatusInterrupted {
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
	compositeFlowIDs = []string{keys.New(shardNum, flowID, flowToken)}
	cur := flowID
	for {
		leaf, ok := interruptedLeafByFlow[cur]
		if !ok {
			// An interrupted flow on the descent always has an interrupted step; its absence means a
			// concurrent Resume already resolved this leaf (a double-Resume race). Surface it as a 409,
			// not a raw ErrNoRows (which resume() would trace into an opaque 500).
			return nil, nil, nil, errors.New("flow is not interrupted", http.StatusConflict)
		}
		stepIDs = append(stepIDs, leaf)
		child, ok := childByCallerStep[leaf]
		if !ok {
			return flowIDs, stepIDs, compositeFlowIDs, nil // no interrupted child spawned here - leaf reached
		}
		flowIDs = append(flowIDs, child)
		compositeFlowIDs = append(compositeFlowIDs, keys.New(shardNum, child, tokenByID[child]))
		cur = child
	}
}

// errResumeLost is an in-transaction sentinel: the root flow was terminalized by a concurrent
// Delete/Terminate between resume's pre-tx status read and its own writes, so the transaction must roll
// back (undoing the step re-park/leaf-reset) and the caller must 409 rather than falsely report success.
var errResumeLost = errors.New("resume lost to a concurrent terminalization")

// resume continues a flow paused by flow.Interrupt, delivering resume data to the leaf interrupt park.
func (e *Engine) resume(ctx context.Context, flowKey string, data any) error {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	// Normalize the caller's resume data (a struct, map, or nil) into a State; an empty one stays "{}".
	resumeState, err := workflow.NewState(data)
	if err != nil {
		return errors.Trace(err, http.StatusBadRequest)
	}
	resumeDataJSON := []byte("{}")
	if resumeState.Len() > 0 {
		resumeDataJSON, err = json.Marshal(resumeState)
		if err != nil {
			return errors.Trace(err)
		}
	}

	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "resume")
	defer doneTurn()

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
		return errors.New("flow is not interrupted", http.StatusConflict)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if !leafInterruptDone {
		return errors.New("flow is not interrupted", http.StatusConflict)
	}

	// Test-only checkpoint: a breakpoint here lets a test freeze resume before its transaction so a racing
	// Delete/Terminate can commit its interrupted->terminated flip deterministically, then confirm the gate
	// write below rolls this transaction back (409) instead of falsely succeeding. Placed *before* the
	// transaction, not mid-tx: on SQLite the racing Delete would deadlock against this transaction's write
	// lock if frozen inside it. No-op in production.
	e.seams.Checkpoint(ctx, engineseams.CheckpointResumeBeforeFlowWrite)

	lost := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		lost = false
		// engineseams.FaultResumeCommit fails this transaction once, before any write, so the test proves it rolls back
		// atomically (the flow stays interrupted, its steps untouched) and a retry then resumes cleanly.
		if e.seams.IsFault(engineseams.FaultResumeCommit) {
			return errors.New("injected fault: " + engineseams.FaultResumeCommit)
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

		tx.ExecContext(ctx, "UPDATE dwarf_steps SET status=?, resume_data=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusInterrupted+"'",
			workflow.StatusPending, resumeDataJSON, leafStepID)

		// Race gate against a concurrent Delete/Terminate. The step writes above are unconditional (their
		// WHERE status='interrupted' still matches, because Delete terminalizes the flow row without
		// touching steps), so without this gate a Delete that flipped the root interrupted→terminated first
		// would let resume re-park ancestors and reset the leaf, then match 0 rows on the flow update below,
		// and still return success - a resume that did not take effect reported as if it had, leaving a
		// transient terminated-flow-with-non-terminal-steps until the reaper mops it. The root flow row is the
		// serialization point: this guarded write takes its lock and confirms it is still interrupted (touch
		// flips unconditionally, so RowsAffected reflects the status match on every driver, MySQL included).
		// A zero-row match means Delete/Terminate won - roll the whole transaction back and 409, mutually
		// exclusive with Delete's own WHERE status='interrupted'. flowID is always the root here
		// (subgraph-child keys were rejected above), which is exactly the row Delete/Terminate terminalize.
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

	// No wake for the interrupted -> running transition, on this flow or its surgraph chain. An Await
	// returns on `interrupted` (it is a stop), so nobody is parked here to wake - and a caller who parked
	// while the flow was running is waiting for it to SETTLE, which resuming is the opposite of.
	e.enqueueStep(ctx, shardNum, leafStepID.(int))
	return nil
}

// surgraphChain walks from a flow up to the root surgraph. It loads the whole tree once via the denormalized
// root_flow_id, then follows surgraph_flow_id/surgraph_step_id pointers from flowID up to the root in memory -
// one round-trip regardless of nesting depth. root_flow_id gives the tree
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
		byID[id] = fnode{token: token, surgFlowID: sfid, surgStepID: ssid}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, nil, errors.Trace(err)
	}

	flowIDs = []any{flowID}
	compositeFlowIDs = []string{keys.New(shardNum, flowID, flowToken)}
	cur := flowID
	for {
		n, ok := byID[cur]
		if !ok || n.surgFlowID == 0 {
			break
		}
		flowIDs = append(flowIDs, n.surgFlowID)
		stepIDs = append(stepIDs, n.surgStepID)
		compositeFlowIDs = append(compositeFlowIDs, keys.New(shardNum, n.surgFlowID, byID[n.surgFlowID].token))
		cur = n.surgFlowID
	}
	return flowIDs, stepIDs, compositeFlowIDs, nil
}
