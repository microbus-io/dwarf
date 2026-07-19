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
	"encoding/json"
	"strings"

	"github.com/microbus-io/dwarf/internal/faninmap"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// cohortResolution reports what a resolve did, so the caller can run the post-commit bookkeeping - the
// doorbell for a new fan-in step, the stop signal for a failed flow - outside the transaction.
type cohortResolution struct {
	fanInStepID      int
	flowFailed       bool
	failedAwaited    bool
	parentStepID     int
	reDispatchParent bool
	bytes            stateByteCount
}

// cohortCounts reads a spawn's size and lineage plus the arrival counts derived from its members'
// cohort_arrived markers. Members are the steps whose lineage_id is the spawn, and only one row per
// BRANCH is ever marked, so these counts are branch counts - not lineage-member counts, which would
// overshoot on any multi-step branch.
func (e *Engine) cohortCounts(ctx context.Context, q sequel.Executor, flowID, spawnID int) (size, lineageID, arrived, failed int, err error) {
	err = q.QueryRowContext(ctx,
		"SELECT sp.cohort_size, sp.lineage_id,"+
			" (SELECT COUNT(*) FROM dwarf_steps m WHERE m.flow_id=? AND m.lineage_id=sp.step_id AND m.cohort_arrived>0),"+
			" (SELECT COUNT(*) FROM dwarf_steps m WHERE m.flow_id=? AND m.lineage_id=sp.step_id AND m.cohort_arrived=?)"+
			" FROM dwarf_steps sp WHERE sp.step_id=?",
		flowID, flowID, cohortArrivedFailed, spawnID,
	).Scan(&size, &lineageID, &arrived, &failed)
	return size, lineageID, arrived, failed, errors.Trace(err)
}

// resolveCohort settles a fan-out cohort once every branch has arrived: it inserts the fan-in step, or
// fails the flow if any branch failed unhandled. It runs AFTER the arrival that triggered it has already
// committed, and that ordering is the whole design.
//
// A sibling records its arrival on its own row and commits; only then does it come here and count. Because
// it is reading committed state, whichever sibling commits LAST is guaranteed to see every arrival. Counting
// inside the arrival's own transaction instead would let the final two siblings each see W-1 under READ
// COMMITTED - neither believing itself last - and the cohort would strand forever, silently. That is the
// opposite of the obvious hazard (several members thinking they are last, which the arbitration below makes
// harmless) and it is why the count must not move back into the arrival transaction.
//
// Called by both settle paths - a branch completing into its fan-in, and a branch failing - so the decision
// lives in exactly one place regardless of which sibling happens to settle last.
//
// One consequence to know about: between the arrival's commit and this resolve, a flow has every step
// terminal while still `running` - the orphan shape. It is normally sub-millisecond, and detectOrphanedFlows
// tolerates it by also requiring no step to have been touched for five minutes. But if this resolve FAILS the
// flow stays in that shape until the reconciler settles it, and the orphan detector will alarm meanwhile,
// which is correct: the flow really is stranded until something resolves the cohort.
func (e *Engine) resolveCohort(ctx context.Context, db *sequel.DB, shardNum, flowID, spawnID, predecessorStepID int) (cohortResolution, error) {
	var res cohortResolution

	// Cheap pre-check outside any transaction. Most arrivals are not the last one, and they must not open a
	// transaction or touch the spawn row at all - that shared-row write is what this design exists to remove.
	// A count is safe to act on because arrivals only ever accumulate within a flow: nothing clears a marker
	// except Fork, which works on a different flow's rows.
	size, _, arrived, _, err := e.cohortCounts(ctx, db, flowID, spawnID)
	if err != nil {
		return res, errors.Trace(err)
	}
	if size == 0 || arrived < size {
		return res, nil
	}

	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		res = cohortResolution{}
		cur := spawnID
		// The first level's completeness was established by the pre-check above, so the claim can be the
		// transaction's first statement - write-first, keeping the SQLite SHARED->EXCLUSIVE upgrade deadlock
		// closed. Levels above (reached only by the failure walk) must test completeness before claiming, and
		// may read freely by then because the transaction already holds a write.
		complete := true
		for {
			if !complete {
				var lvlSize, lvlArrived int
				lvlSize, _, lvlArrived, _, cerr := e.cohortCounts(ctx, tx, flowID, cur)
				if cerr != nil {
					return errors.Trace(cerr)
				}
				if lvlSize == 0 || lvlArrived < lvlSize {
					return nil
				}
			}

			// The arbiter. Exactly one worker resolves a cohort, however many observe it complete: the losers
			// match zero rows and stop. This is what replaces the spawn row's write lock, which used to serialize
			// every sibling in order to give the same guarantee.
			claim, cerr := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET cohort_resolved=1 WHERE step_id=? AND cohort_resolved=0", cur)
			if cerr != nil {
				return errors.Trace(cerr)
			}
			if n, _ := claim.RowsAffected(); n == 0 {
				return nil
			}

			_, lineageID, _, failed, rerr := e.cohortCounts(ctx, tx, flowID, cur)
			if rerr != nil {
				return errors.Trace(rerr)
			}

			if failed == 0 {
				ok, lerr := e.lockFlowRowTx(ctx, tx, flowID)
				if lerr != nil {
					return errors.Trace(lerr)
				}
				if !ok {
					return nil // Cancel/failStep terminalized the flow first: a clean no-op
				}
				fc, ferr := e.fanInContext(ctx, tx, shardNum, flowID, cur, predecessorStepID)
				if ferr != nil {
					return errors.Trace(ferr)
				}
				fanInStepID, bytes, ierr := e.insertFanInStep(ctx, tx, shardNum, flowID, fc.nextStepDepth, cur, predecessorStepID, fc.fanInTaskName, fc.graph, fc.workflowURL, 0, fc.priority, fc.fairnessKey, fc.fairnessWeight, fc.timeBudgetMs)
				if ierr != nil {
					return errors.Trace(ierr)
				}
				res.fanInStepID = fanInStepID
				res.bytes = bytes
				e.countFlowRowWrite(ctx, flowID)
				tx.ExecContext(ctx, "UPDATE dwarf_flows SET step_id=?, touch=1-touch WHERE flow_id=?", fanInStepID, flowID)
				return nil
			}

			// The cohort settled with an unhandled failure, so it never reaches its fan-in.
			if lineageID == 0 {
				return errors.Trace(e.failFlowForCohort(ctx, tx, shardNum, flowID, &res))
			}

			// A nested cohort: its spawn has itself settled - badly - as a member of the cohort one level out,
			// and since this branch produced no fan-in step the spawn row IS the branch's last row. Mark it and
			// continue up. The outer cohort may now be complete too, which the top of the loop re-tests.
			if _, merr := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET cohort_arrived=? WHERE step_id=?", cohortArrivedFailed, cur,
			); merr != nil {
				return errors.Trace(merr)
			}
			// Transitional: keep the shared counters in step with the markers so the invariant comparing them
			// still holds. This mirrors what propagateCohortFailure does for the same settlement one level out.
			// Goes when the counters go.
			if _, berr := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals + 1, cohort_failures = cohort_failures + 1 WHERE step_id=?",
				lineageID,
			); berr != nil {
				return errors.Trace(berr)
			}
			cur = lineageID
			complete = false
		}
	})
	if err != nil {
		return cohortResolution{}, errors.Trace(err)
	}
	return res, nil
}

// lockFlowRowTx takes the flow row's write lock, guarded on the flow still being non-terminal. A zero-row
// match means a concurrent Cancel/failStep terminalized it, and the caller must bail without writing:
// extending or terminalizing a flow that already stopped would insert orphan work.
func (e *Engine) lockFlowRowTx(ctx context.Context, tx sequel.Executor, flowID int) (bool, error) {
	e.countFlowRowWrite(ctx, flowID)
	res, err := tx.ExecContext(ctx,
		"UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
		flowID,
	)
	if err != nil {
		return false, errors.Trace(err)
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// failFlowForCohort terminalizes a flow whose top-level cohort resolved with an unhandled branch failure.
// Every branch has settled by here, so nothing is left running to strand.
func (e *Engine) failFlowForCohort(ctx context.Context, tx sequel.Executor, shardNum, flowID int, res *cohortResolution) error {
	ok, err := e.lockFlowRowTx(ctx, tx, flowID)
	if err != nil {
		return errors.Trace(err)
	}
	if !ok {
		return nil
	}

	var sampleErr string
	tx.QueryRowContext(ctx,
		"SELECT error FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusFailed+"' AND error!='' ORDER BY step_id LIMIT_OFFSET(1, 0)",
		flowID,
	).Scan(&sampleErr)
	sampleErr = strings.TrimSpace(sampleErr)
	if sampleErr == "" {
		sampleErr = "cohort failed"
	}

	finalStateJSON, _, err := e.computeFinalState(ctx, tx, shardNum, flowID)
	if err != nil {
		return errors.Trace(err)
	}
	tx.ExecContext(ctx,
		"UPDATE dwarf_flows SET final_state=?, status=?, error=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
		finalStateJSON, workflow.StatusFailed, sampleErr, flowID,
	)
	res.flowFailed = true

	// A subgraph child delivers its failure to the parked caller's flow.Subgraph call rather than notifying
	// directly; the child has still stopped, so its own Await is woken either way.
	var parentStepID int
	tx.QueryRowContext(ctx, "SELECT surgraph_step_id, awaited FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&parentStepID, &res.failedAwaited)
	if parentStepID != 0 {
		reDispatch, derr := e.deliverFlowFailureToParent(ctx, tx, parentStepID, sampleErr)
		if derr != nil {
			return errors.Trace(derr)
		}
		res.parentStepID = parentStepID
		res.reDispatchParent = reDispatch
	}
	return nil
}

// fanInContext loads everything insertFanInStep needs. It is read only by the ONE arrival that actually
// resolves a cohort, never by the others, so a cohort pays for it once rather than per branch. Loading it
// here rather than threading it down from the caller is what lets failStep and the reconciler resolve a
// cohort with nothing but its spawn id.
type fanInContext struct {
	graph          *workflow.Graph
	fanInTaskName  string
	workflowURL    string
	nextStepDepth  int
	priority       int
	fairnessKey    string
	fairnessWeight float64
	timeBudgetMs   int
}

func (e *Engine) fanInContext(ctx context.Context, tx sequel.Executor, shardNum, flowID, spawnID, predecessorStepID int) (fanInContext, error) {
	var fc fanInContext
	cg, err := e.cachedFlowGraph(ctx, tx, shardNum, flowID)
	if err != nil {
		return fc, errors.Trace(err)
	}
	fc.graph = cg.graph

	var spawnTaskName string
	if err := tx.QueryRowContext(ctx, "SELECT task_name FROM dwarf_steps WHERE step_id=?", spawnID).Scan(&spawnTaskName); err != nil {
		return fc, errors.Trace(err)
	}
	fc.fanInTaskName = cg.fanIn.For(strings.TrimSpace(spawnTaskName))
	if fc.fanInTaskName == "" {
		return fc, errors.New("cohort spawn '%s' has no fan-in target", strings.TrimSpace(spawnTaskName))
	}

	// The fan-in sits below the last arriver; insertFanInStep raises this to below the DEEPEST branch.
	var predDepth int
	if err := tx.QueryRowContext(ctx, "SELECT step_depth FROM dwarf_steps WHERE step_id=?", predecessorStepID).Scan(&predDepth); err != nil {
		return fc, errors.Trace(err)
	}
	fc.nextStepDepth = predDepth + 1

	err = tx.QueryRowContext(ctx,
		"SELECT workflow_url, priority, fairness_key, fairness_weight, time_budget_ms FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&fc.workflowURL, &fc.priority, &fc.fairnessKey, &fc.fairnessWeight, &fc.timeBudgetMs)
	if err != nil {
		return fc, errors.Trace(err)
	}
	fc.workflowURL = strings.TrimSpace(fc.workflowURL)
	fc.fairnessKey = strings.TrimSpace(fc.fairnessKey)
	return fc, nil
}

// cachedFlowGraph returns the flow's parsed graph and fan-in routing map, reusing the per-flow cache the
// dispatch path fills. The graph JSON is frozen at flow creation, so a cached parse is always current.
func (e *Engine) cachedFlowGraph(ctx context.Context, q sequel.Executor, shardNum, flowID int) (*cachedGraph, error) {
	key := graphCacheKey{shard: shardNum, flowID: flowID}
	if cg, ok := e.graphCache.Load(key); ok {
		return cg, nil
	}
	var graphJSON []byte
	if err := q.QueryRowContext(ctx, "SELECT graph FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&graphJSON); err != nil {
		return nil, errors.Trace(err)
	}
	parsed := &workflow.Graph{}
	if err := json.Unmarshal(graphJSON, parsed); err != nil {
		return nil, errors.Trace(err)
	}
	cg := &cachedGraph{graph: parsed, fanIn: faninmap.New(parsed)}
	e.graphCache.Store(key, cg)
	return cg, nil
}
