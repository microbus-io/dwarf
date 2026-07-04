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
	"fmt"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// recoveryLoop runs the parked-step wedge sweep on its own slow cadence, kept off the frequently-nudged
// poll path (pollPendingSteps can fire sub-second under load) because the sweep's NOT EXISTS / DISTINCT
// scans are heavy and the wedge condition it guards against is latency-tolerant. A plain ticker - no
// nudging - so the sweep runs at most once per wedgeSweepInterval.
func (e *Engine) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(wedgeSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.recoveryStop:
			return
		case <-ticker.C:
			e.onEachShard(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
				e.sweepWedgedParks(ctx, db, shard)
				e.detectOrphanedFlows(ctx, db, shard)
				return nil
			})
		}
	}
}

// sweepWedgedParks is a defense-in-depth recovery pass for parked steps whose releasing condition can no
// longer occur, so they would otherwise sit forever (a parked step is invisible to selection and, for
// parkedSubgraph, to lease recovery too). It runs on the dedicated recoveryLoop at wedgeSweepInterval, and
// every detector carries a parkWedgeThreshold age guard so steady-state operation never trips a false
// positive. Each recovery re-invokes the normal release mechanism (which is guarded by a CAS on the park
// state), so it is idempotent and harmless under a concurrent resolution, a false positive, or a peer
// replica sweeping the same shard. A nonzero dwarf_steps_unwedged means a latent bug let a step wedge
// - the sweep papered over the effect but the cause is worth finding.
func (e *Engine) sweepWedgedParks(ctx context.Context, db *sequel.DB, shard int) {
	e.recoverWedgedSubgraphParks(ctx, db, shard, parkWedgeThreshold)
	e.recoverOrphanedSubgraphChildren(ctx, db, shard, parkWedgeThreshold)
}

// recoverWedgedSubgraphParks finds parkedSubgraph caller steps whose child flow can no longer revive them -
// the child reached terminal but completeSurgraphFlow's revive was lost, or the child was deleted - and
// re-drives the release. A healthy caller step has one non-terminal child (created/running/interrupted)
// bound to it by surgraph_step_id; a fan-out has several such caller steps, each checked independently
// here, and flow.Retry leaves older terminal children whose latest sibling is still active. completeSurgraphFlow
// runs within milliseconds of child completion in steady state, so a step older than parkWedgeThreshold with
// no non-terminal child is genuinely wedged.
func (e *Engine) recoverWedgedSubgraphParks(ctx context.Context, db *sequel.DB, shard int, minAge time.Duration) {
	rows, err := db.QueryContext(ctx,
		"SELECT s.step_id, s.flow_id FROM dwarf_steps s"+
			" WHERE s.parked=? AND s.status='"+workflow.StatusRunning+"' AND s.updated_at < DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_flows f WHERE f.surgraph_step_id=s.step_id AND f.status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"'))",
		parkedSubgraph, -minAge.Milliseconds(),
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: querying parked subgraph steps", "shard", shard, "error", err)
		return
	}
	type wedgedCaller struct{ stepID, flowID int }
	var hits []wedgedCaller
	for rows.Next() {
		var w wedgedCaller
		err := rows.Scan(&w.stepID, &w.flowID)
		if err != nil {
			rows.Close()
			e.logger.ErrorContext(ctx, "Wedge sweep: scanning parked subgraph step", "shard", shard, "error", err)
			return
		}
		hits = append(hits, w)
	}
	rows.Close()

	for _, w := range hits {
		// The latest child for this caller step decides the disposition; older children are completed
		// retry attempts. flow_id DESC mirrors how the live completion path keys the surgraph.
		var childFlowID int
		var childStatus, childFinalState, childError string
		err := db.QueryRowContext(ctx,
			"SELECT flow_id, status, final_state, error FROM dwarf_flows WHERE surgraph_step_id=? ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)",
			w.stepID,
		).Scan(&childFlowID, &childStatus, &childFinalState, &childError)
		switch {
		case err == sql.ErrNoRows:
			// The child flow is gone (e.g. deleted/purged): fail the caller so the flow can terminate.
			e.logger.ErrorContext(ctx, "Wedge sweep: parked subgraph caller has no child flow; failing it",
				"shard", shard, "step", w.stepID, "flow", w.flowID)
			if rerr := e.deliverSubgraphError(ctx, shard, 0, 0, w.stepID, errors.New("subgraph flow not found (wedge recovery)")); rerr != nil {
				e.logger.ErrorContext(ctx, "Wedge sweep: failing orphaned subgraph caller", "shard", shard, "step", w.stepID, "error", rerr)
				continue
			}
			e.metricStepUnwedged(ctx, "subgraph")
		case err != nil:
			e.logger.ErrorContext(ctx, "Wedge sweep: reading child flow", "shard", shard, "step", w.stepID, "error", err)
		default:
			childStatus = strings.TrimSpace(childStatus)
			e.logger.ErrorContext(ctx, "Wedge sweep: reviving wedged subgraph caller",
				"shard", shard, "step", w.stepID, "childFlow", childFlowID, "childStatus", childStatus)
			var rerr error
			if childStatus == workflow.StatusCompleted {
				rerr = e.completeSurgraphFlow(ctx, shard, w.flowID, w.stepID, childFinalState)
			} else {
				// failed / cancelled: deliver the child's error (or a synthesized one) to the caller.
				msg := strings.TrimSpace(childError)
				if msg == "" {
					msg = "subgraph " + childStatus
				}
				rerr = e.deliverSubgraphError(ctx, shard, 0, childFlowID, w.stepID, errors.New(msg))
			}
			if rerr != nil {
				e.logger.ErrorContext(ctx, "Wedge sweep: reviving subgraph caller", "shard", shard, "step", w.stepID, "error", rerr)
				continue
			}
			e.metricStepUnwedged(ctx, "subgraph")
		}
	}
}

// recoverOrphanedSubgraphChildren finds non-terminal subgraph child flows whose parent flow has already gone
// terminal - the mirror image of recoverWedgedSubgraphParks (which handles a parked caller whose child is gone;
// this handles a live child whose caller/parent is gone). Such a child is orphaned: its parent can never process
// its completion or interrupt, and no lifecycle op reaches it - the root key 409s (the root is terminal), the
// child's own key is read-only (Resume/Cancel 400), and recoverWedgedSubgraphParks is blind to it (the caller
// step is terminal, not running+parked). It is the residue of a Cancel that terminalized the tree in the narrow
// window after the caller step parked but before the child flow was inserted, so the teardown, working from a scan
// taken before the child existed, missed it. (A fan-out sibling's failStep no longer produces this residue - a
// subgraph child now fails via cohort accounting after every branch settles, never eagerly while a sibling is
// live - so this is defense in depth for the Cancel race and any future orphan cause.) The sweep tears the orphan
// down by cancelling its subtree, sharing the parent's terminal fate. In steady state a parent goes terminal only
// after its live subgraphs resolve, so a non-terminal child under a terminal parent older than minAge is genuinely
// orphaned; the age guard excludes the sub-second window in which a just-terminalized parent's sibling child is
// still being cleaned up by the normal completion/error path. An interrupted parent is deliberately excluded (not
// terminal): a Resume of the root revives the interrupted branch, and a sibling child running under it is healthy.
func (e *Engine) recoverOrphanedSubgraphChildren(ctx context.Context, db *sequel.DB, shard int, minAge time.Duration) {
	rows, err := db.QueryContext(ctx,
		"SELECT c.flow_id, c.flow_token FROM dwarf_flows c"+
			" JOIN dwarf_flows p ON p.flow_id=c.surgraph_flow_id"+
			" WHERE c.surgraph_flow_id>0 AND c.status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')"+
			" AND c.updated_at < DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND p.status IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
		-minAge.Milliseconds(),
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: querying orphaned subgraph children", "shard", shard, "error", err)
		return
	}
	type orphan struct {
		flowID int
		token  string
	}
	var hits []orphan
	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.flowID, &o.token); err != nil {
			rows.Close()
			e.logger.ErrorContext(ctx, "Wedge sweep: scanning orphaned subgraph child", "shard", shard, "error", err)
			return
		}
		hits = append(hits, o)
	}
	rows.Close()

	for _, o := range hits {
		e.logger.ErrorContext(ctx, "Wedge sweep: cancelling orphaned subgraph child whose parent is terminal",
			"shard", shard, "childFlow", flowCorrelationID(shard, o.flowID))
		if rerr := e.cancelOrphanedSubtree(ctx, shard, o.flowID, o.token); rerr != nil {
			e.logger.ErrorContext(ctx, "Wedge sweep: cancelling orphaned subgraph child", "shard", shard, "childFlow", o.flowID, "error", rerr)
			continue
		}
		e.metricStepUnwedged(ctx, "orphaned_child")
	}
}

// cancelOrphanedSubtree cancels an orphaned subgraph child and its own non-terminal descendants in one
// transaction - a subtree-scoped cancel (no surgraph up-walk: the ancestor chain is already terminal, which is
// why the child is orphaned). It mirrors Cancel's transaction shape (write-first step cancel, per-flow
// computeFinalState, one CASE flow-cancel) but does not error on a zero-row flow update: the child may have
// terminalized concurrently between the sweep's SELECT and this write, which is a benign no-op, not a 409.
// No FlowStopped notification fires - subgraph children never notify (notify_on_stop is root-only), and this
// whole subtree is descendants of an already-terminal root.
func (e *Engine) cancelOrphanedSubtree(ctx context.Context, shard int, childFlowID int, childFlowToken string) error {
	db, err := e.shard(shard)
	if err != nil {
		return errors.Trace(err)
	}
	descFlowIDs, descCompositeIDs, err := e.allSubgraphFlows(ctx, shard, childFlowID)
	if err != nil {
		return errors.Trace(err)
	}
	allFlowIDs := append([]any{childFlowID}, descFlowIDs...)
	allCompositeIDs := append([]string{fmt.Sprintf("%d-%d-%s", shard, childFlowID, strings.TrimSpace(childFlowToken))}, descCompositeIDs...)

	const reason = "parent flow terminated (orphan recovery)"
	finalStates := make([]string, len(allFlowIDs))
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		flowPlaceholders := strings.Repeat("?,", len(allFlowIDs)-1) + "?"
		// Write-first (the step-cancel UPDATE) per the flow-terminating-transaction rule.
		stepArgs := append([]any{workflow.StatusCancelled, parkedNone}, allFlowIDs...)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusInterrupted+"', '"+workflow.StatusRunning+"')",
			stepArgs...,
		)

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
		_, err := tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET final_state="+caseClause+", status=?, cancel_reason=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id IN ("+flowPlaceholders+") AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
			flowArgs...,
		)
		return errors.Trace(err)
	})
	if err != nil {
		return errors.Trace(err)
	}
	for _, cid := range allCompositeIDs {
		e.signalStop(ctx, cid, workflow.StatusCancelled)
	}
	return nil
}

// detectOrphanedFlows reports `running` flows that have no non-terminal step and have not advanced for
// orphanFlowThreshold - a flow stranded by a post-completion transition that failed to commit its
// successor (see the processStep recovery defer, whose own reset UPDATE can lose to a contention storm).
// It is a bug signal, logged at error level only: auto-recovery is deliberately not attempted here, since
// re-driving the flow would duplicate the transition-evaluation logic and a false positive could
// double-advance it. The processStep defer is the actual recovery; this is the last-resort alarm for the
// residual case it cannot cover. A flow legitimately waiting - on a subgraph child (its caller step is
// `running`+parked), a sleep/retry backoff (its next step is `pending`), or a human (`interrupted`) - has
// a non-terminal step and is excluded, so steady-state operation never trips it.
func (e *Engine) detectOrphanedFlows(ctx context.Context, db *sequel.DB, shard int) {
	rows, err := db.QueryContext(ctx,
		"SELECT f.flow_id FROM dwarf_flows f"+
			" WHERE f.status='"+workflow.StatusRunning+"' AND f.updated_at < DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"'))",
		-orphanFlowThreshold.Milliseconds(),
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Orphan detection: querying running flows", "shard", shard, "error", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var flowID int
		if err := rows.Scan(&flowID); err != nil {
			e.logger.ErrorContext(ctx, "Orphan detection: scanning running flow", "shard", shard, "error", err)
			return
		}
		// Token-free correlation id: this is an operator alarm, not a capability. See "Tracing".
		e.logger.ErrorContext(ctx, "Orphaned flow: running with all steps terminal and no successor",
			"flow", flowCorrelationID(shard, flowID))
	}
	if err := rows.Err(); err != nil {
		e.logger.ErrorContext(ctx, "Orphan detection: iterating running flows", "shard", shard, "error", err)
	}
}
