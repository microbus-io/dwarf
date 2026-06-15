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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// recoveryLoop runs the parked-step wedge sweep on its own slow cadence, kept off the frequently-nudged
// poll path (pollPendingSteps can fire sub-second under load) because the sweep's NOT EXISTS / DISTINCT
// scans are heavy and the wedge condition it guards against is latency-tolerant. A plain ticker - no
// nudging - so the sweep runs at most once per wedgeSweepInterval. Drained before the refiller in
// drainRuntime, since recoverWedgedBreakerParks can requestRefill.
func (e *Engine) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(wedgeSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.recoveryStop:
			return
		case <-ticker.C:
			e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
				e.sweepWedgedParks(ctx, db, shard)
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
// replica sweeping the same shard. A nonzero dwarf_steps_unwedged_total means a latent bug let a step wedge
// - the sweep papered over the effect but the cause is worth finding.
func (e *Engine) sweepWedgedParks(ctx context.Context, db *sequel.DB, shard int) {
	e.recoverWedgedSubgraphParks(ctx, db, shard, parkWedgeThreshold)
	e.recoverWedgedBreakerParks(ctx, db, shard, parkWedgeThreshold)
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
		"SELECT s.step_id, s.step_depth, s.flow_id FROM dwarf_steps s"+
			" WHERE s.parked=? AND s.status=? AND s.updated_at < DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_flows f WHERE f.surgraph_step_id=s.step_id AND f.status IN (?, ?, ?))",
		parkedSubgraph, workflow.StatusRunning, -minAge.Milliseconds(),
		workflow.StatusCreated, workflow.StatusRunning, workflow.StatusInterrupted,
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: querying parked subgraph steps", "shard", shard, "error", err)
		return
	}
	type wedgedCaller struct{ stepID, stepDepth, flowID int }
	var hits []wedgedCaller
	for rows.Next() {
		var w wedgedCaller
		if err := rows.Scan(&w.stepID, &w.stepDepth, &w.flowID); err != nil {
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
				rerr = e.completeSurgraphFlow(ctx, shard, w.flowID, w.stepDepth, w.stepID, childFinalState)
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

// recoverWedgedBreakerParks re-establishes a probe for any task whose breaker backlog (parked=2) has lost
// its probe: no parkedNone pending step and no running step for the task on this shard, so nothing would
// ever dispatch to re-trip or close the breaker. A scheduled probe is itself a parkedNone pending row (with
// a future not_before), so a task mid-probe-schedule has a live probe and is not flagged. Recovery re-arms
// the in-memory breaker (idempotent if already tripped; necessary because breakerClose only unparks a
// tripped breaker) and elevates the oldest parked step to a due probe; the normal cycle then resumes - a
// 404 re-parks it and elects the next probe, a success runs breakerClose to unpark the shard.
func (e *Engine) recoverWedgedBreakerParks(ctx context.Context, db *sequel.DB, shard int, minAge time.Duration) {
	rows, err := db.QueryContext(ctx,
		"SELECT DISTINCT s.task_url FROM dwarf_steps s"+
			" WHERE s.status=? AND s.parked=? AND s.updated_at < DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps p WHERE p.task_url=s.task_url AND p.parked=? AND p.status IN (?, ?))",
		workflow.StatusPending, parkedBreaker, -minAge.Milliseconds(),
		parkedNone, workflow.StatusPending, workflow.StatusRunning,
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: querying breaker-parked steps", "shard", shard, "error", err)
		return
	}
	var tasks []string
	for rows.Next() {
		var taskURL string
		if err := rows.Scan(&taskURL); err != nil {
			rows.Close()
			e.logger.ErrorContext(ctx, "Wedge sweep: scanning breaker-parked task", "shard", shard, "error", err)
			return
		}
		tasks = append(tasks, taskURL)
	}
	rows.Close()

	for _, taskURL := range tasks {
		// Re-arm the in-memory breaker so breakerClose can unpark the backlog when the probe succeeds.
		e.breakerTrip(taskURL, breakerCauseAckTimeout)
		// Elevate the oldest parked step as the probe. SELECT-then-UPDATE in a tx (a single
		// UPDATE-with-self-subquery is rejected by MySQL), mirroring breakerBulkPark's election.
		elevated := false
		terr := db.Transact(ctx, func(tx *sequel.Tx) error {
			elevated = false
			var probeID int
			err := tx.QueryRowContext(ctx,
				"SELECT step_id FROM dwarf_steps WHERE task_url=? AND status=? AND parked=? ORDER BY created_at ASC, step_id ASC LIMIT_OFFSET(1, 0)",
				taskURL, workflow.StatusPending, parkedBreaker,
			).Scan(&probeID)
			if err == sql.ErrNoRows {
				return nil
			}
			if err != nil {
				return errors.Trace(err)
			}
			res, err := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET parked=?, not_before=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=? AND parked=?",
				parkedNone, probeID, workflow.StatusPending, parkedBreaker,
			)
			if err != nil {
				return errors.Trace(err)
			}
			if n, _ := res.RowsAffected(); n > 0 {
				elevated = true
			}
			return nil
		})
		if terr != nil {
			e.logger.ErrorContext(ctx, "Wedge sweep: electing breaker probe", "shard", shard, "task", taskURL, "error", terr)
			continue
		}
		if elevated {
			e.logger.ErrorContext(ctx, "Wedge sweep: re-elected lost breaker probe", "shard", shard, "task", taskURL)
			e.metricStepUnwedged(ctx, "breaker")
			e.requestRefill()
		}
	}
}
