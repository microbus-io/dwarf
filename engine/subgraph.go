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

	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/keys"
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
	childKey, err := e.createWithGraph(ctx, shardNum, subgraphWorkflowURL, subgraphGraph, childState, callerTraceParent, &inherited, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID, parkCaller)
	// engineseams.FaultSubgraphSpawnCommitLost reports an error for a spawn whose transaction COMMITTED - a commit
	// whose acknowledgement was lost - so a test can prove a retried spawn does not create a second child.
	if err == nil && e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultSubgraphSpawnCommitLost, subgraphWorkflowURL)) {
		return "", errors.New("injected fault: "+engineseams.FaultSubgraphSpawnCommitLost+" "+subgraphWorkflowURL, http.StatusInternalServerError)
	}
	return childKey, errors.Trace(err)
}

// errTreeClosed is an in-transaction sentinel: every flow of the tree was already closed, so terminateSubtree
// rolls back and reports success.
var errTreeClosed = errors.New("tree already closed")

// errTreeGrew is an in-transaction sentinel: terminateSubtree's scan found flows its write set did not cover, so
// the transaction rolls back and re-runs over the larger set.
var errTreeGrew = errors.New("tree grew during terminate")

// maxTerminateRuns bounds terminateSubtree's re-runs. Each re-run needs a flow found by the previous one to have
// committed a subgraph spawn before the re-run locked its steps, so a tree that keeps outrunning it is pathological;
// failing then is safe, since every run rolls back cleanly, and the caller may retry.
const maxTerminateRuns = 16

// terminateSubtree forcefully stops a flow and every open flow beneath it, in one transaction, and signals
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
// writes never touch a frozen tree, and nothing is counted or signalled. ended reports whether this call ended the
// flow at flowID. commitFault is a test-only seam name, checked before any write so a fired fault proves the whole
// transaction rolls back atomically; pass "" for none.
func (e *Engine) terminateSubtree(ctx context.Context, shardNum, flowID, flowHour int, flowToken, reason, commitFault string) (ended bool, err error) {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}
	descendantFlowIDs, descendantCompositeIDs, labelByFlowID, err := e.allSubgraphFlows(ctx, db, shardNum, flowID)
	if err != nil {
		return false, errors.Trace(err)
	}
	e.seams.Checkpoint(ctx, engineseams.CheckpointTerminateAfterScan)
	allFlowIDs := append([]any{flowID}, descendantFlowIDs...)
	allCompositeIDs := append(
		[]string{keys.NewFlowKey(shardNum, flowHour, flowID, flowToken)},
		descendantCompositeIDs...,
	)

	reason = strings.TrimSpace(reason)
	settled := false
	var terminalized []bool // per allFlowIDs entry: its UPDATE matched, so this transaction ended it
	for run := 1; ; run++ {
		if run > maxTerminateRuns {
			return false, errors.New("terminate did not settle after %d runs; the tree kept spawning subgraphs", maxTerminateRuns, http.StatusServiceUnavailable)
		}
		grew := false
		err = db.Transact(ctx, func(tx *sequel.Tx) error {
			settled, grew = false, false
			if commitFault != "" && e.seams.IsFault(commitFault) {
				return errors.New("injected fault: " + commitFault)
			}
			// The flow-id lists are chunked: a tree can hold more flows than a statement can bind. The lock passes
			// run in ascending id order (lockOrder), recomputed per run because a re-run covers a larger set.
			lockIDs := lockOrder(allFlowIDs)
			if e.seams.Enabled() {
				e.seams.Variable(engineseams.Join(engineseams.VariableLockOrder, "terminate"), slices.Clone(lockIDs))
			}
			terminateSteps := func() {
				for chunk := range slices.Chunk(lockIDs, inListChunk) {
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
			for chunk := range slices.Chunk(lockIDs, inListChunk) {
				ph, ids := inList(chunk)
				n, err := lockFlowRows(ctx, tx, "flow_id IN ("+ph+") AND status NOT IN ("+terminalStatusesSQL+")", ids...)
				if err != nil {
					return err
				}
				locked += n
			}
			if locked == 0 {
				settled = true
				return errTreeClosed
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
			// The scan returns the tree's open flows. A flow of the set that is missing from it ended or was
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
				_, fHour, _, fToken, err := keys.ParseFlowKey(allCompositeIDs[i])
				if err != nil {
					return errors.Trace(err)
				}
				fs, err := e.terminalFinalState(ctx, tx, shardNum, fid.(int), fHour, fToken)
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
			terminalized = make([]bool, len(allFlowIDs))
			for i, fid := range allFlowIDs {
				batch.queue(func(res *sequel.BatchResults) error {
					r, err := res.Exec()
					if err != nil {
						return errors.Trace(err)
					}
					n, _ := r.RowsAffected()
					ended += n
					terminalized[i] = n > 0
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
				return errTreeClosed
			}
			return nil
		})
		if grew {
			continue
		}
		break
	}
	if settled {
		return false, nil
	}
	if err != nil {
		return false, errors.Trace(err)
	}

	for i, cid := range allCompositeIDs {
		// Only the flows this transaction ended: a flow that terminalized on its own path first (a root completing
		// between the first scan and the lock, which the rescan keeps regardless) was counted and signalled there,
		// and its UPDATE's status guard matched nothing here. Every flow ended here was started (createWithGraph
		// counts subgraph children too), and the in-flight panel is started minus ended.
		if !terminalized[i] {
			continue
		}
		fid := allFlowIDs[i].(int)
		_, fHour, _, _, _ := keys.ParseFlowKey(cid) // parsed once already, in the transaction
		e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, fHour, fid), "to", workflow.StatusTerminated)
		label := labelByFlowID[fid]
		e.metricFlowClosed(ctx, label.workflowURL, workflow.StatusTerminated, label.priority, shardNum)
		e.signalStop(ctx, cid, workflow.StatusTerminated)
	}
	return len(terminalized) > 0 && terminalized[0], nil
}

// parkOwnerGuard confines a delivery to a parked caller to the park its child owns: the caller row's
// parked_child_flow_id, written by the spawn that parked it, must name the delivering child. The park conditions
// alone (running, parkedSubgraph) do not identify it: a Retry reaps the child and rewinds the same step_id, which
// can park again on a new child under exactly those conditions, so a stalled worker delivering the old child's
// outcome late would settle the new park with it. The owner must be a column of the row being updated: a
// subquery on the child flows reads PostgreSQL's statement snapshot, which a delivery that waited on the row lock
// rechecks the row against but does not refresh. A delivery that consumes the park clears the owner.
const parkOwnerGuard = " AND parked_child_flow_id=?"

// completeSurgraphFlow re-dispatches a parked surgraph step after its child, childFlowID, completes.
func (e *Engine) completeSurgraphFlow(ctx context.Context, shardNum int, surgraphFlowID int, surgraphStepID int, childFlowID int, subgraphFinalStateJSON []byte) error {
	// engineseams.FaultSubgraphReviveLost simulates the revive being lost after the child went terminal (the wedge the
	// parked-step sweep exists to catch): the caller step stays running+parkedSubgraph with no open child,
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
		// parkOwnerGuard confines it to the park this child owns.
		res, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, parked_child_flow_id=0, subgraph_done=1, subgraph_result=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND parked=?"+parkOwnerGuard,
			workflow.StatusPending, parkedNone, resultJSON, surgraphStepID, parkedSubgraph, childFlowID,
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
// owned child is already terminal or gone, so there is no child to terminalize here. childFlowID is the child the
// sweep found owning the park, whether or not its row still exists:
//
//   - a terminal child: it went terminal but the revive was lost, and the re-arm is the whole recovery;
//   - no child: the child flow is GONE. The caller's park and the child's insert commit in one transaction,
//     so only a bug or a hand-deleted row leaves this, and failing the caller is the only way out.
func (e *Engine) deliverSubgraphError(ctx context.Context, shardNum int, parentStepID int, childFlowID int, taskErr error) error {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	reDispatchParent := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		// The parent re-arm is the transaction's first statement and is itself a write (the parked caller
		// step), so it is write-first.
		var derr error
		reDispatchParent, derr = e.deliverFlowFailureToParent(ctx, tx, parentStepID, childFlowID, taskErr)
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
// longer parked on childFlowID (already resolved, terminated, or retried away), in which case there is nothing to
// re-dispatch.
//
// The error is stored as its TracedError JSON, stack stripped, not as its message: flow.Subgraph decodes it,
// so its status code and properties reach the caller - the cancellation marker among them, which is what
// lets workflow.IsCancelled recognize a cancelled child. A bare message would arrive as a new error without
// any of them.
//
// This write runs inside the child's terminating transaction, so it must not fail on what the error holds. The
// message passes sanitizeErrorMessage: a NUL encodes as \u0000, which a JSONB column refuses. Properties are the
// task's own, unbounded and unchecked, so JSON over the payload limit or still holding a NUL keeps only the
// message and status code - a cancellation marker among the properties is then lost, a smaller cost than a
// failure write that cannot land, which the classifier reads as the database being down.
func (e *Engine) deliverFlowFailureToParent(ctx context.Context, tx sequel.Executor, parentStepID int, childFlowID int, childErr error) (bool, error) {
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
				lost = e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultDeliverFailureErr, taskName))
			}
		}
		if lost {
			return false, nil
		}
	}
	encoded, err := e.encodeSubgraphError(childErr)
	if err != nil {
		return false, errors.Trace(err)
	}
	res, err := tx.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, parked=?, parked_child_flow_id=0, subgraph_done=1, subgraph_error=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND parked=?"+parkOwnerGuard,
		workflow.StatusPending, parkedNone, encoded, parentStepID, parkedSubgraph, childFlowID,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// encodeSubgraphError is the subgraph_error column's value for err: its TracedError JSON, stack stripped. Its
// writers must not fail on what the error holds, so the message passes sanitizeErrorMessage, and JSON over the
// payload limit or still holding a NUL keeps only the message and status code.
func (e *Engine) encodeSubgraphError(err error) ([]byte, error) {
	delivered := *errors.Convert(err)
	delivered.Stack = nil
	delivered.Err = errors.New("%s", sanitizeErrorMessage(delivered.Error()))
	encoded, merr := json.Marshal(&delivered)
	if merr != nil || len(encoded) > e.maxPayloadBytes || bytes.Contains(encoded, []byte(`\u0000`)) {
		encoded, merr = json.Marshal(&errors.TracedError{Err: delivered.Err, StatusCode: delivered.StatusCode})
		if merr != nil {
			return nil, errors.Trace(merr)
		}
	}
	return encoded, nil
}

// subgraphErrorText is what a subgraph_error column delivers to flow.Subgraph: the child's error as TracedError
// JSON, or "" for none, which the column holds as {}.
func subgraphErrorText(col []byte) string {
	s := strings.TrimSpace(string(col))
	if s == "{}" {
		return ""
	}
	return s
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
		"SELECT flow_id, flow_token, created_hour, surgraph_flow_id, status, workflow_url, priority FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)",
		flowID,
	)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	type node struct {
		token    string
		hour     int
		terminal bool
		label    flowLabel
	}
	byID := map[int]node{}
	childrenByParent := map[int][]int{}
	for rows.Next() {
		var id, hour, parent, priority int
		var token, status, workflowURL string
		if err := rows.Scan(&id, &token, &hour, &parent, &status, &workflowURL, &priority); err != nil {
			rows.Close()
			return nil, nil, nil, errors.Trace(err)
		}
		term := workflow.IsTerminalStatus(status)
		byID[id] = node{token: token, hour: hour, terminal: term, label: flowLabel{workflowURL: workflowURL, priority: priority}}
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
			compositeFlowIDs = append(compositeFlowIDs, keys.NewFlowKey(shardNum, n.hour, child, n.token))
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
// leafLeaseSeq is the leaf's lease_seq as this same query saw it: it names the interrupt selected, which Resume
// fences on. Read it anywhere later and a Retry that re-interrupted the row in between is taken for the selection.
func (e *Engine) interruptedSubgraphChain(ctx context.Context, shardNum int, flowID int, flowHour int, flowToken string) (flowIDs []any, stepIDs []any, compositeFlowIDs []string, leafLeaseSeq int, err error) {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}

	frows, err := db.QueryContext(ctx,
		"SELECT flow_id, flow_token, created_hour, surgraph_step_id, status FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)",
		flowID,
	)
	if err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}
	tokenByID := map[int]string{}
	hourByID := map[int]int{}
	childByCallerStep := map[int]int{} // surgraph_step_id -> interrupted child flow_id
	for frows.Next() {
		var id, hour, ssid int
		var token, status string
		if err := frows.Scan(&id, &token, &hour, &ssid, &status); err != nil {
			frows.Close()
			return nil, nil, nil, 0, errors.Trace(err)
		}
		tokenByID[id] = token
		hourByID[id] = hour
		if ssid != 0 && status == workflow.StatusInterrupted {
			childByCallerStep[ssid] = id
		}
	}
	frows.Close()
	if err := frows.Err(); err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}

	// Each tree flow's interrupted leaf: order in SQL, take the first row per flow_id in memory. The order is
	// Snapshot's - the step the flow row recorded as the one it stopped at, then the earliest-updated - so Resume
	// resolves the interrupt Snapshot and StoppedTaskName report.
	interruptedLeafByFlow := map[int]int{}
	leaseSeqByStep := map[int]int{}
	srows, err := db.QueryContext(ctx,
		"SELECT s.flow_id, s.step_id, s.lease_seq FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id"+
			" WHERE s.status='"+workflow.StatusInterrupted+"' AND f.root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)"+
			" ORDER BY s.flow_id, CASE WHEN s.step_id=f.stopped_step_id THEN 0 ELSE 1 END, s.updated_at, s.step_id",
		flowID,
	)
	if err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}
	for srows.Next() {
		var fid, sid, seq int
		if err := srows.Scan(&fid, &sid, &seq); err != nil {
			srows.Close()
			return nil, nil, nil, 0, errors.Trace(err)
		}
		if _, seen := interruptedLeafByFlow[fid]; !seen {
			interruptedLeafByFlow[fid] = sid // first row per flow_id = earliest updated_at, step_id
			leaseSeqByStep[sid] = seq
		}
	}
	srows.Close()
	if err := srows.Err(); err != nil {
		return nil, nil, nil, 0, errors.Trace(err)
	}

	flowIDs = []any{flowID}
	compositeFlowIDs = []string{keys.NewFlowKey(shardNum, flowHour, flowID, flowToken)}
	cur := flowID
	for {
		leaf, ok := interruptedLeafByFlow[cur]
		if !ok {
			// An interrupted flow on the descent always has an interrupted step; its absence means a
			// concurrent Resume already resolved this leaf (a double-Resume race). Surface it as a 409,
			// not a raw ErrNoRows (which resume() would trace into an opaque 500).
			return nil, nil, nil, 0, errors.New("flow is not interrupted", http.StatusConflict)
		}
		stepIDs = append(stepIDs, leaf)
		child, ok := childByCallerStep[leaf]
		if !ok {
			return flowIDs, stepIDs, compositeFlowIDs, leaseSeqByStep[leaf], nil // no interrupted child spawned here - leaf reached
		}
		flowIDs = append(flowIDs, child)
		compositeFlowIDs = append(compositeFlowIDs, keys.NewFlowKey(shardNum, hourByID[child], child, tokenByID[child]))
		cur = child
	}
}

// surgraphChain walks from a flow up to the root surgraph, following surgraph_flow_id/surgraph_step_id one
// level per round trip: each query reads a flow's caller link joined to its parent's token and own link, so
// it touches only the chain, never the rest of the tree.
func (e *Engine) surgraphChain(ctx context.Context, shardNum int, flowID int, flowHour int, flowToken string) (flowIDs []any, stepIDs []any, compositeFlowIDs []string, err error) {
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, nil, nil, errors.Trace(err)
	}
	return e.surgraphChainIn(ctx, db, liveTables, shardNum, flowID, flowHour, flowToken)
}

// surgraphChainIn is surgraphChain on q, over the flows table of t.
func (e *Engine) surgraphChainIn(ctx context.Context, q sequel.Executor, t flowTables, shardNum int, flowID int, flowHour int, flowToken string) (flowIDs []any, stepIDs []any, compositeFlowIDs []string, err error) {
	flowIDs = []any{flowID}
	compositeFlowIDs = []string{keys.NewFlowKey(shardNum, flowHour, flowID, flowToken)}
	cur := flowID
	for {
		var surgFlowID, surgStepID, surgHour, nextSurgFlowID int
		var surgToken string
		err := q.QueryRowContext(ctx,
			"SELECT f.surgraph_flow_id, f.surgraph_step_id, COALESCE(p.flow_token, ''), COALESCE(p.created_hour, 0), COALESCE(p.surgraph_flow_id, 0)"+
				" FROM "+t.flows+" f LEFT JOIN "+t.flows+" p ON p.flow_id=f.surgraph_flow_id WHERE f.flow_id=?",
			cur,
		).Scan(&surgFlowID, &surgStepID, &surgToken, &surgHour, &nextSurgFlowID)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && surgFlowID == 0) {
			break
		}
		if err != nil {
			return nil, nil, nil, errors.Trace(err)
		}
		flowIDs = append(flowIDs, surgFlowID)
		stepIDs = append(stepIDs, surgStepID)
		compositeFlowIDs = append(compositeFlowIDs, keys.NewFlowKey(shardNum, surgHour, surgFlowID, surgToken))
		if nextSurgFlowID == 0 {
			break
		}
		cur = surgFlowID
	}
	return flowIDs, stepIDs, compositeFlowIDs, nil
}
