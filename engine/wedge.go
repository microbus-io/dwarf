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
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// recoveryLoop runs the parked-step wedge sweep on its own slow cadence, kept off the frequently-nudged
// poll path (pollPendingSteps can fire sub-second under load) because the sweep's NOT EXISTS / DISTINCT
// scans are heavy and the wedge condition it guards against is latency-tolerant. A plain ticker - no
// nudging - so the sweep runs at most once per wedgeSweepInterval.
func (e *Engine) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(e.wedgeSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.recoveryStop:
			return
		case <-ticker.C:
			e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
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
	e.recoverWedgedSubgraphParks(ctx, db, shard, e.parkWedgeThreshold)
	e.recoverOrphanedSubgraphChildren(ctx, db, shard, e.parkWedgeThreshold)
}

// recoverWedgedSubgraphParks finds parkedSubgraph caller steps whose child flow can no longer revive them -
// the child reached terminal but completeSurgraphFlow's revive was lost, or the child was deleted - and
// re-drives the release. A healthy caller step has one non-terminal child (created/running/interrupted)
// bound to it by surgraph_step_id; a fan-out has several such caller steps, each checked independently
// here, and flow.Retry leaves older terminal children whose latest sibling is still active. completeSurgraphFlow
// runs within milliseconds of child completion in steady state, so a step older than parkWedgeThreshold with
// no non-terminal child is genuinely wedged.
//
// The age comparison is `<=`, not `<`, so that minAge=0 means "no age guard" - which is exactly what every
// caller passing it reads it as. NOW_UTC() is millisecond-precision, so a strict `<` silently excludes a
// step stamped inside the CURRENT millisecond: a genuinely wedged caller whose park happens to share a tick
// with the sweep is skipped, the sweep reports nothing to do, and the wedge it exists to clear survives.
// The boundary is immaterial at the production threshold (one millisecond either side of five minutes), so
// `<=` costs nothing there and makes the zero case honest.
func (e *Engine) recoverWedgedSubgraphParks(ctx context.Context, db *sequel.DB, shard int, minAge time.Duration) {
	rows, err := db.QueryContext(ctx,
		"SELECT s.step_id, s.flow_id FROM dwarf_steps s"+
			" WHERE s.parked=? AND s.status='"+workflow.StatusRunning+"' AND s.updated_at <= DATE_ADD_MILLIS(NOW_UTC(), ?)"+
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
	if err := rows.Err(); err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: iterating parked subgraph steps", "shard", shard, "error", err)
		return
	}

	for _, w := range hits {
		// The latest child for this caller step decides the disposition; older children are completed
		// retry attempts. flow_id DESC mirrors how the live completion path keys the surgraph.
		var childFlowID int
		var childStatus, childError string
		var childFinalState []byte
		err := db.QueryRowContext(ctx,
			"SELECT flow_id, status, final_state, error FROM dwarf_flows WHERE surgraph_step_id=? ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)",
			w.stepID,
		).Scan(&childFlowID, &childStatus, &childFinalState, &childError)
		switch {
		case err == sql.ErrNoRows:
			// The child flow is gone (e.g. deleted/purged): fail the caller so the flow can terminate.
			e.logger.ErrorContext(ctx, "Wedge sweep: parked subgraph caller has no child flow; failing it",
				"shard", shard, "step", w.stepID, "flow", keys.CorrelationID(shard, w.flowID))
			if rerr := e.deliverSubgraphError(ctx, shard, 0, w.stepID, errors.New("subgraph flow not found (wedge recovery)")); rerr != nil {
				e.logger.ErrorContext(ctx, "Wedge sweep: failing orphaned subgraph caller", "shard", shard, "step", w.stepID, "error", rerr)
				continue
			}
			e.metricStepUnwedged(ctx, "subgraph")
		case err != nil:
			e.logger.ErrorContext(ctx, "Wedge sweep: reading child flow", "shard", shard, "step", w.stepID, "error", err)
		default:
			childStatus = strings.TrimSpace(childStatus)
			e.logger.ErrorContext(ctx, "Wedge sweep: reviving wedged subgraph caller",
				"shard", shard, "step", w.stepID, "childFlow", keys.CorrelationID(shard, childFlowID), "childStatus", childStatus)
			var rerr error
			if childStatus == workflow.StatusCompleted {
				rerr = e.completeSurgraphFlow(ctx, shard, w.flowID, w.stepID, childFinalState)
			} else {
				// failed / cancelled: deliver the child's error (or a synthesized one) to the caller.
				msg := strings.TrimSpace(childError)
				if msg == "" {
					msg = "subgraph " + childStatus
				}
				rerr = e.deliverSubgraphError(ctx, shard, childFlowID, w.stepID, errors.New(msg))
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
			// `<=`, so minAge=0 means "no age guard" - see recoverWedgedSubgraphParks for why a strict `<`
			// silently skips a row stamped inside the sweep's own millisecond.
			" AND c.updated_at <= DATE_ADD_MILLIS(NOW_UTC(), ?)"+
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
	if err := rows.Err(); err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: iterating orphaned subgraph children", "shard", shard, "error", err)
		return
	}

	for _, o := range hits {
		e.logger.ErrorContext(ctx, "Wedge sweep: cancelling orphaned subgraph child whose parent is terminal",
			"shard", shard, "childFlow", keys.CorrelationID(shard, o.flowID))
		if rerr := e.cancelOrphanedSubtree(ctx, shard, o.flowID, o.token); rerr != nil {
			e.logger.ErrorContext(ctx, "Wedge sweep: cancelling orphaned subgraph child", "shard", shard, "childFlow", keys.CorrelationID(shard, o.flowID), "error", rerr)
			continue
		}
		e.metricStepUnwedged(ctx, "orphaned_child")
	}
}

// cancelOrphanedSubtree cancels an orphaned subgraph child and its own non-terminal descendants, sharing the
// public Cancel's transaction (cancelSubtree). No surgraph up-walk is needed - and none exists: the ancestor
// chain is already terminal, which is precisely why the child is orphaned. A zero-row flow update is a benign
// no-op rather than a 409: the child may have terminalized concurrently between the sweep's SELECT and this
// write, which is the outcome the sweep wanted anyway.
func (e *Engine) cancelOrphanedSubtree(ctx context.Context, shard int, childFlowID int, childFlowToken string) error {
	const reason = "parent flow terminated (orphan recovery)"
	return errors.Trace(e.cancelSubtree(ctx, shard, childFlowID, childFlowToken, reason, "", false))
}

// detectOrphanedFlows reports a `running` flow that is stranded: every step terminal AND no step touched for
// orphanFlowThreshold. It is the shape a post-completion transition leaves when it fails to commit its
// successor (see the processStep recovery defer, whose own reset UPDATE can lose to a contention storm).
// It is a bug signal, logged at error level and counted by dwarf_flows_orphaned: auto-recovery is deliberately
// not attempted here, since re-driving the flow would duplicate the transition-evaluation logic and a false
// positive could double-advance it. The processStep defer is the actual recovery; this is the last-resort alarm
// for the residual case it cannot cover.
//
// Both correctness conditions live on dwarf_steps, and that is the point. The age guard was originally on the
// flow row (`dwarf_flows.updated_at`), but the touch-column refactor froze that column at the flow's
// go-`running` time (it moves only on a status change now), so `f.updated_at` no longer tracks per-step
// progress and stopped discriminating a strand from a healthy transition. A step row's `updated_at`, by
// contrast, still moves on every pending->running->terminal transition, so it is the honest "last activity"
// signal. The frozen `f.updated_at` is kept only as a cheap index pre-filter (narrowing the scan to flows old
// enough to qualify); it can never exclude a real orphan, since one has been running longer than the threshold.
//
//   - all steps terminal (NOT EXISTS a non-terminal step) excludes every legitimate long-wait, because each
//     holds a non-terminal step: a task mid-execution (`running`, even for 10 minutes with no DB activity),
//     a sleep/retry backoff (`pending`), a parked subgraph caller (`running`+parked), a human (`interrupted`).
//   - no step touched within the threshold (NOT EXISTS a step with a recent `updated_at`) excludes the only
//     real false positive: the brief window in a NORMAL transition between the standalone step->`completed`
//     UPDATE and the tx that inserts the successor, where the flow momentarily has all-terminal steps. The
//     just-`completed` step's `updated_at` is fresh, so the flow is excluded (this window stretches to seconds
//     under persist's retry backoff, still far inside the threshold).
//
// A genuine orphan satisfies both - every step terminal, the last one completed longer ago than the threshold
// - and is flagged, with the same detection latency the flow-level guard intended.
func (e *Engine) detectOrphanedFlows(ctx context.Context, db *sequel.DB, shard int) {
	rows, err := db.QueryContext(ctx,
		"SELECT f.flow_id, f.workflow_url FROM dwarf_flows f"+
			// Index pre-filter, NOT the correctness signal (the step clauses below are). f.updated_at is frozen
			// at go-`running` time, so this narrows the idx_dwarf_flows_status scan to flows that went running
			// long enough ago to possibly hold a threshold-stale step - and it can never exclude a real orphan,
			// which has been running (and therefore had its flow row stamped) longer ago than the threshold.
			" WHERE f.status='"+workflow.StatusRunning+"' AND f.updated_at < DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"'))"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s2 WHERE s2.flow_id=f.flow_id AND s2.updated_at >= DATE_ADD_MILLIS(NOW_UTC(), ?))",
		-e.orphanFlowThreshold.Milliseconds(),
		-e.orphanFlowThreshold.Milliseconds(),
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Orphan detection: querying running flows", "shard", shard, "error", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var flowID int
		var workflowURL string
		if err := rows.Scan(&flowID, &workflowURL); err != nil {
			e.logger.ErrorContext(ctx, "Orphan detection: scanning running flow", "shard", shard, "error", err)
			return
		}
		// Token-free correlation id: this is an operator alarm, not a capability. See "Tracing".
		e.logger.ErrorContext(ctx, "Orphaned flow: running with all steps terminal and no successor",
			"flow", keys.CorrelationID(shard, flowID))
		e.metricOrphanedFlow(ctx, workflowURL)
	}
	if err := rows.Err(); err != nil {
		e.logger.ErrorContext(ctx, "Orphan detection: iterating running flows", "shard", shard, "error", err)
	}
}
