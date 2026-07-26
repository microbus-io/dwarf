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
	"slices"
	"strings"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// forkFlow clones a terminal flow's execution tree up to a chosen step into a brand-new, self-contained
// root flow, then re-runs from that step with optional state overrides. The fork point may be ANY recorded
// step - in the root flow or deep inside a subgraph. The original is never mutated (terminal flows are
// immutable); recovery/exploration is non-destructive.
//
// The design is a copy-only-keep clone, re-parking ancestor callers up the surgraph chain, a created->pending
// crash-gate, and uniform scheduling resolution.
func (e *Engine) forkFlow(ctx context.Context, stepKey string, stateOverrides any) (string, error) {
	shardNum, forkStepID, forkStepToken, err := keys.ParseStepKey(stepKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}

	// Resolve the fork step, its owning flow, and that flow's token (needed to walk the surgraph chain).
	var leafFlowID int
	var forkStepState, forkStepRefs []byte
	var leafFlowToken string
	err = db.QueryRowContext(ctx,
		"SELECT s.flow_id, s.state, s.state_refs, f.flow_token FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE s.step_id=? AND s.step_token=?",
		forkStepID, forkStepToken,
	).Scan(&leafFlowID, &forkStepState, &forkStepRefs, &leafFlowToken)
	if err == sql.ErrNoRows {
		return "", errors.New("step not found", http.StatusNotFound)
	}
	if err != nil {
		return "", errors.Trace(err)
	}

	// Walk up to the root, recording each flow's rewind step: the leaf fork step in its own flow, and the
	// caller step that spawned the lower flow in each ancestor.
	chainFlowIDs, chainStepIDs, _, err := e.surgraphChain(ctx, shardNum, leafFlowID, leafFlowToken)
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
	var rootThreadID, rootPriority, rootTimeBudgetMs, rootDeleteAfterMs int
	var rootFairnessKey string
	var rootFairnessWeight float64
	err = db.QueryRowContext(ctx,
		"SELECT status, workflow_url, thread_id, thread_token, priority, fairness_key, fairness_weight, time_budget_ms, delete_after_ms FROM dwarf_flows WHERE flow_id=?",
		rootFlowID,
	).Scan(&rootStatus, &rootWorkflowURL, &rootThreadID, &rootThreadToken, &rootPriority, &rootFairnessKey, &rootFairnessWeight, &rootTimeBudgetMs, &rootDeleteAfterMs)
	if err != nil {
		return "", errors.Trace(err)
	}
	if rootStatus != workflow.StatusCompleted && rootStatus != workflow.StatusFailed && rootStatus != workflow.StatusCancelled {
		return "", errors.New("can only fork a terminal flow (status: %s)", rootStatus, http.StatusConflict)
	}
	// A flow scheduled for deletion is on its way out; unlike Continue (which searches for a base), Fork names
	// a specific step, so naming a doomed flow is an error, not a fallback.
	if rootDeleteAfterMs > 0 {
		return "", errors.New("cannot fork a flow scheduled for deletion", http.StatusConflict)
	}

	// The fork's leaf step is rewritten with fully MATERIALIZED state (and state_refs cleared below): the
	// caller's overrides merge onto the real values, and the leaf is the one row the fork re-runs from, so it
	// must not depend on anchors whose ids are about to be remapped. Its successors re-mint refs normally.
	mergedLeafState, err := e.resolvedStepState(ctx, db, shardNum, forkStepState, forkStepRefs, rootWorkflowURL)
	if err != nil {
		return "", errors.Trace(err)
	}
	mergedLeafState, err = mergeWithOverrides(mergedLeafState, stateOverrides)
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
		rootFlowToken:   keys.RandomIdentifier(16),
		rootTraceParent: e.mintWorkflowSpan(ctx, rootWorkflowURL, ""), // detached, like Continue
		threadID:        rootThreadID,
		threadToken:     rootThreadToken,
		priority:        rootPriority,
		fairnessKey:     rootFairnessKey,
		fairnessWeight:  rootFairnessWeight,
		timeBudgetMs:    rootTimeBudgetMs,
	}

	var newRootFlowID int
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		cc.newLeafStepID = 0
		// FaultForkCommit fails this transaction once, before the clone runs, so the test proves the whole
		// clone rolls back atomically (origin byte-identical, zero partial clone rows) and a retry then
		// forks cleanly - pinning Fork's "crash mid-clone rolls back, origin never mutated" claim.
		if e.seams.IsFault(FaultForkCommit) {
			return errors.New("injected fault: " + FaultForkCommit)
		}
		id, cloneErr := e.cloneTree(ctx, tx, cc, rootFlowID)
		if cloneErr != nil {
			return errors.Trace(cloneErr)
		}
		newRootFlowID = id
		// Mapping complete - flip the gated leaf step created->pending. The flow chain is already running.
		_, txErr := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusCreated+"'",
			workflow.StatusPending, cc.newLeafStepID,
		)
		return errors.Trace(txErr)
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	newFlowKey := fmt.Sprintf("%d-%d-%s", shardNum, newRootFlowID, cc.rootFlowToken)
	e.logger.InfoContext(ctx, "Flow forked", "fromRoot", rootFlowID, "forkStep", forkStepID, "to", newRootFlowID)
	// A fork starts a new, self-contained root flow, and its completion runs through the same completeFlow
	// that increments dwarf_flows_terminated - so it MUST also be counted here, or the standard in-flight
	// panel (started - terminated) drifts negative by one per fork. Create and Continue count their starts
	// at the same point; Fork's own INSERT...SELECT clone path simply never did.
	e.metricFlowStarted(ctx, cc.rootWorkflowURL, shardNum)
	e.enqueueStep(ctx, shardNum, cc.newLeafStepID)
	return newFlowKey, nil
}

// forkClone carries cross-recursion state for a Fork clone.
type forkClone struct {
	leafFlowID      int
	leafStepID      int
	mergedLeafState []byte
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
	newRootFlowID   int    // the cloned root's flow_id; descendants inherit it as their root_flow_id
	rootWorkflowURL string // the cloned root's workflow_url, for the flows_started metric after commit
}

// forkChild is a queued subgraph-caller child awaiting clone: its origin flow plus the new-surgraph context
// (the parent's freshly-cloned flow id and caller step id) it hangs under. Carrying only three ints keeps the
// worklist tiny regardless of how much state each cloned flow holds.
type forkChild struct {
	originFlowID  int
	newSurgFlowID int
	newSurgStepID int
	isRoot        bool
}

// cloneTree clones the whole terminal tree rooted at rootFlowID into a fresh flow tree and returns the new
// root's flow id. It walks the tree with an explicit LIFO worklist rather than recursion: each flow is cloned
// to completion by cloneOneFlow, which returns its kept subgraph-caller children to enqueue. Only ONE flow's
// clone state (its step metadata, id map, and that flow's graph/baggage JSON) is live at a time, and the
// goroutine stack stays O(1) at any nesting depth. Depth-proportional recursion here would instead hold every
// ancestor's clone state simultaneously and, at pathological depth, overflow the goroutine stack - which is
// fatal (unrecoverable by errors.CatchPanic, unlike a host-call panic). Children are pushed in reverse so they
// pop in discovery order (DFS preorder, matching the former recursion), keeping cloned-id assignment stable.
func (e *Engine) cloneTree(ctx context.Context, tx *sequel.Tx, cc *forkClone, rootFlowID int) (int, error) {
	stack := []forkChild{{originFlowID: rootFlowID, isRoot: true}}
	var rootNewID int
	for len(stack) > 0 {
		it := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		newFlowID, children, err := e.cloneOneFlow(ctx, tx, cc, it.originFlowID, it.newSurgFlowID, it.newSurgStepID, it.isRoot)
		if err != nil {
			return 0, errors.Trace(err)
		}
		if it.isRoot {
			rootNewID = newFlowID
		}
		for _, c := range slices.Backward(children) {
			stack = append(stack, c)
		}
	}
	return rootNewID, nil
}

// cloneOneFlow clones a single flow (originFlowID) into a new flow under the given new-surgraph context and
// returns the new flow id plus its kept subgraph-caller children for cloneTree to enqueue. A flow on the
// rewind chain keeps everything above its rewind step and is set running; an off-path completed-prefix
// subgraph (rewind 0) is cloned whole and keeps its status.
func (e *Engine) cloneOneFlow(ctx context.Context, tx *sequel.Tx, cc *forkClone, originFlowID, newSurgFlowID, newSurgStepID int, isRoot bool) (int, []forkChild, error) {
	rewind := cc.rewindByFlow[originFlowID] // 0 => full clone (off-path completed prefix subgraph)

	pruned := map[int]bool{}
	if rewind != 0 {
		sub, err := e.collectDAGSubtree(ctx, tx, originFlowID, rewind)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		for _, m := range sub {
			pruned[m.stepID] = true
		}
	}

	var status, workflowURL, workflowName, traceParent string
	var graphJSON, baggageJSON []byte
	var deleteOnCompletion, originStepID int
	err := tx.QueryRowContext(ctx,
		"SELECT status, workflow_url, workflow_name, graph, baggage, trace_parent, delete_on_completion, step_id FROM dwarf_flows WHERE flow_id=?",
		originFlowID,
	).Scan(&status, &workflowURL, &workflowName, &graphJSON, &baggageJSON, &traceParent, &deleteOnCompletion, &originStepID)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	newStatus := status
	if isRoot || rewind != 0 { // on the rewind chain
		newStatus = workflow.StatusRunning
	}
	// Scheduling (cc.*) is resolved once and applied uniformly to every cloned flow and step - the tree is
	// uniform and a deep-subgraph fork's re-running leaf must carry the same overridden values.
	forkedFromStep, newTrace := 0, traceParent
	flowPriority, flowFairnessKey, flowFairnessWeight, flowBudget := cc.priority, cc.fairnessKey, cc.fairnessWeight, cc.timeBudgetMs
	if isRoot {
		newStatus = workflow.StatusCreated // gate; flipped to running below once this flow is mapped
		forkedFromStep, newTrace = cc.leafStepID, cc.rootTraceParent
		deleteOnCompletion = 0
		cc.rootWorkflowURL = workflowURL // for the flows_started metric, emitted after the tx commits
	}

	// Hoist the minted token: the root overwrites it with cc.rootFlowToken below, but a non-root subgraph
	// flow keeps it AND must echo it into thread_token (it is its own thread), or List builds a malformed
	// ThreadKey ("{shard}-{id}-") that the API's thread resolution then 404s on.
	newFlowToken := keys.RandomIdentifier(16)
	newFlowID64, err := tx.InsertReturnID(ctx, "flow_id",
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, baggage, status, surgraph_flow_id, surgraph_step_id, forked_from_step, trace_parent, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms, engine_id)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		newFlowToken, workflowURL, workflowName, graphJSON, baggageJSON, newStatus, newSurgFlowID, newSurgStepID, forkedFromStep, newTrace, deleteOnCompletion, flowPriority, flowFairnessKey, flowFairnessWeight, flowBudget, e.engineID,
	)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	newFlowID := int(newFlowID64)
	if isRoot {
		// The cloned root is its own root; descendants inherit this id as their root_flow_id.
		cc.newRootFlowID = newFlowID
		// The root's flow_token must match the key returned to the caller.
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_flows SET flow_token=?, thread_id=?, thread_token=?, root_flow_id=? WHERE flow_id=?",
			cc.rootFlowToken, cc.threadID, cc.threadToken, newFlowID, newFlowID)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	} else {
		// Subgraph flows are their own thread (thread_id/thread_token = own flow_id/flow_token) but share
		// the cloned root's tree-membership id. step_id is set below, once idMap is built.
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_flows SET thread_id=?, thread_token=?, root_flow_id=? WHERE flow_id=?", newFlowID, newFlowToken, cc.newRootFlowID, newFlowID)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	// Direct subgraph children of this flow, keyed by caller step (latest child per caller).
	childByCaller := map[int]int{}
	crows, err := tx.QueryContext(ctx, "SELECT flow_id, surgraph_step_id FROM dwarf_flows WHERE surgraph_flow_id=? ORDER BY flow_id", originFlowID)
	if err != nil {
		return 0, nil, errors.Trace(err)
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
		refs                                         staterefs.Refs
	}
	mrows, err := tx.QueryContext(ctx,
		"SELECT step_id, predecessor_id, successor_id, lineage_id, cohort_size, status, state_refs FROM dwarf_steps WHERE flow_id=? ORDER BY step_id",
		originFlowID,
	)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	var keep []stepMeta
	for mrows.Next() {
		var s stepMeta
		var refsJSON []byte
		mrows.Scan(&s.oldID, &s.predID, &s.succID, &s.lineageID, &s.cohortSize, &s.status, &refsJSON)
		if pruned[s.oldID] {
			continue
		}
		s.refs = staterefs.Parse(refsJSON)
		keep = append(keep, s)
	}
	mrows.Close()

	// A terminal fork origin never holds an interrupted step (interrupt forces the whole surgraph chain -
	// up to the root - non-terminal, and Fork rejects a non-terminal root), so a KEPT interrupted step (one
	// not on this flow's rewind path, which is reset/re-parked below) can only arise from a broken invariant.
	// Copied verbatim it would clone into the running fork as an orphan: it can never be resumed (resume needs
	// flow status interrupted, but the fork is running) and, if a cohort member, its cohort can never fully
	// arrive - wedging the fork permanently at its fan-in. Reject the fork loudly rather than silently wedge.
	for _, s := range keep {
		if s.status == workflow.StatusInterrupted && s.oldID != rewind {
			return 0, nil, errors.New("cannot fork: tree holds an unresolved interrupted step (%d) off the fork path", s.oldID, http.StatusConflict)
		}
	}

	// A kept non-rewind step that is still non-terminal (running/pending) is a straggler: a fan-out sibling
	// that had not settled when the origin terminalized, or a step of a Cancel that raced. Unlike an
	// interrupted step (an invariant violation, rejected above), this is a legitimate window - but the origin
	// froze its outcome with that branch in flight, so the fork must freeze it too. Copied verbatim it would
	// (a) let lease recovery reset its stale-leased `running` row to pending and re-dispatch it in the running
	// fork - a surprise duplicate execution - and (b) as a cohort member count as neither an arrival nor a
	// failure, wedging the fork's fan-in forever. Normalize it to cancelled: the keep-meta update makes the
	// cohort recompute below count it as an arrival (so the fan-in still converges), and the cloned rows are
	// frozen after the inserts.
	var stragglers []int
	for i := range keep {
		s := &keep[i]
		if s.oldID != rewind && s.status != workflow.StatusCompleted && s.status != workflow.StatusFailed && s.status != workflow.StatusCancelled {
			s.status = workflow.StatusCancelled
			stragglers = append(stragglers, s.oldID)
		}
	}

	// Copy kept steps (all columns DB-side, native timestamps), overriding flow_id, a fresh token, and the
	// flow's scheduling. The leaf fork step is inserted `created` (gated); all others keep their status.
	idMap := make(map[int]int, len(keep))
	isLeafFlow := originFlowID == cc.leafFlowID
	for _, s := range keep {
		var newID int64
		if isLeafFlow && s.oldID == cc.leafStepID {
			newID, err = tx.InsertReturnID(ctx, "step_id",
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_refs, changes, interrupt_payload, status, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, priority, fairness_key, fairness_weight, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at, engine_id)"+
					" SELECT ?, step_depth, ?, task_name, task_url, state, state_refs, changes, interrupt_payload, ?, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, ?, ?, ?, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at, ? FROM dwarf_steps WHERE step_id=?",
				newFlowID, keys.RandomIdentifier(16), workflow.StatusCreated, flowPriority, flowFairnessKey, flowFairnessWeight, e.engineID, s.oldID,
			)
		} else {
			newID, err = tx.InsertReturnID(ctx, "step_id",
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_refs, changes, interrupt_payload, status, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, priority, fairness_key, fairness_weight, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at, engine_id)"+
					" SELECT ?, step_depth, ?, task_name, task_url, state, state_refs, changes, interrupt_payload, status, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, ?, ?, ?, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at, ? FROM dwarf_steps WHERE step_id=?",
				newFlowID, keys.RandomIdentifier(16), flowPriority, flowFairnessKey, flowFairnessWeight, e.engineID, s.oldID,
			)
		}
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		idMap[s.oldID] = int(newID)
	}

	// Remap intra-flow references (a ref to a pruned/absent step -> 0) and stamp the resolved time budget
	// (kept uniform with priority/fairness; the INSERT...SELECT copied the source step's budget).
	//
	// state_refs is remapped the same way, and this is exactly why refs live in their own COLUMN rather than
	// inline in the state JSON: the payload columns ride the DB-side INSERT...SELECT above and never pass
	// through the engine, so remapping an anchor costs one tiny UPDATE - where an inline `$ref` would have
	// forced reading every large state blob into Go to rewrite it, hauling precisely the payloads state refs
	// exist to stop hauling. An anchor is always an ANCESTOR of the step that refs it, and pruning removes
	// only DESCENDANTS of the rewind step, so a kept step's anchors are always kept too; a zero mapping would
	// mean that invariant broke, and it is caught rather than silently written as a dangling ref.
	for _, s := range keep {
		refsJSON := []byte("{}")
		if len(s.refs) > 0 {
			remapped := make(staterefs.Refs, len(s.refs))
			for field, anchor := range s.refs {
				newAnchor, ok := idMap[anchor]
				if !ok || newAnchor == 0 {
					return 0, nil, errors.New("cannot fork: step %d refs field %q at step %d, which is not in the clone", s.oldID, field, anchor)
				}
				remapped[field] = newAnchor
			}
			data, merr := json.Marshal(remapped)
			if merr != nil {
				return 0, nil, errors.Trace(merr)
			}
			refsJSON = data
		}
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET predecessor_id=?, successor_id=?, lineage_id=?, time_budget_ms=?, state_refs=? WHERE step_id=?",
			idMap[s.predID], idMap[s.succID], idMap[s.lineageID], flowBudget, refsJSON, idMap[s.oldID],
		)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	// Freeze the cloned straggler rows: cancelled + cleared park/lease, so lease recovery (running-only) and
	// the selection scan both ignore them (terminal implies parkedNone). The INSERT above copied their origin
	// status/lease verbatim; this is the DB counterpart of the keep-meta normalization done earlier.
	for _, oldID := range stragglers {
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
			workflow.StatusCancelled, parkedNone, idMap[oldID],
		)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	// Recompute cohort counters on cloned spawns, counting BRANCHES - not lineage members.
	//
	// `lineage_id` is a cohort-COUNTING device, not a DAG: every step of a per-element sub-pipeline inherits
	// the spawn's childLineageID, so a branch of depth D contributes D members to the lineage. The live
	// engine bumps `cohort_arrivals` once per BRANCH (when the branch's exit step transitions to the fan-in),
	// so `arrivals <= size` always holds. Counting members instead - as this once did - breaks that on any
	// multi-step branch: `Seed --forEach(3)--> Cell --> Enrich --> Join` has 6 members for size 3, so a fork
	// wrote arrivals=5, size=3 and violated the pinned invariant `cohort_arrivals <= cohort_size`
	// (`invariants_test.go`, asserted across the chaos-soak and race suites). The existing fork fixture hid
	// it because its branch is exactly ONE step - the degenerate case where members == branches.
	//
	// A branch is the sub-DAG rooted at a direct child of the spawn (predecessor_id = spawn) that shares the
	// spawn's cohort lineage. Fork requires a TERMINAL origin, so every kept step is settled: a branch has
	// arrived unless it is the one being rewound (that branch re-runs, and its post-rewind steps were pruned
	// anyway), and it counts a failure if any of its steps failed. Excluding the whole rewound BRANCH - not
	// merely the rewind step - is the other half of the fix: with `rewind = Enrich1`, its sibling `Cell1` sat
	// in the same branch and was still counted as an arrival.
	//
	// MEMBERSHIP IS THE LINEAGE CHAIN, NOT THE LINEAGE ID. A NESTED fan-out re-lineages its own children
	// (`childLineageID = stepID`), so in `Seed -forEach-> Cell -forEach-> Chunk -> JoinChunk -> JoinCell` the
	// Chunk steps carry `lineage_id = Cell`, not `Seed`. Testing `c.lineageID == s.oldID` - as this once did -
	// therefore DEAD-ENDS the outer walk at the inner spawn: the Chunks are filtered out, and JoinChunk (which
	// does carry the outer lineage) is only reachable THROUGH them, since its predecessor is the inner cohort's
	// last completer. Everything past the inner cohort in that branch became invisible, with two consequences:
	// a rewind at or past the inner frame was never seen, so the branch was counted as ARRIVED and its re-run
	// pushed `cohort_arrivals` past `cohort_size`; and a failure inside a kept branch's inner cohort was never
	// seen, so the clone lost `cohort_failures` and a fork that had to re-fail COMPLETED instead - silently
	// absorbing an unrecovered branch failure, which inverts the whole point of the partial-recovery fork.
	//
	// So a step belongs to spawn S's cohort - at ANY nesting depth - iff walking its lineage chain upward
	// reaches S. That descends through nested cohorts and still stops cleanly at the outer fan-in, whose lineage
	// is the spawn's OWN lineage and so never reaches S. Every spawn in a kept step's chain is itself kept (a
	// lineage ancestor is a DAG ancestor, and pruning removes only descendants of the rewind step), so this is a
	// pure in-memory walk over rows already loaded - no extra query, at any depth.
	childrenByPred := map[int][]stepMeta{}
	lineageOf := make(map[int]int, len(keep))
	for _, s := range keep {
		childrenByPred[s.predID] = append(childrenByPred[s.predID], s)
		lineageOf[s.oldID] = s.lineageID
	}
	for _, s := range keep {
		if s.cohortSize == 0 {
			continue
		}
		// inCohort memoizes per spawn: the same chain is re-walked for every step of a branch.
		memo := map[int]bool{}
		var inCohort func(lineageID int) bool
		inCohort = func(lineageID int) bool {
			if lineageID == 0 {
				return false
			}
			if v, ok := memo[lineageID]; ok {
				return v
			}
			memo[lineageID] = false // breaks a malformed chain rather than spinning on it
			v := lineageID == s.oldID || inCohort(lineageOf[lineageID])
			memo[lineageID] = v
			return v
		}
		arrivals, failures := 0, 0
		for _, head := range childrenByPred[s.oldID] {
			if head.lineageID != s.oldID {
				continue // not a member of this spawn's cohort (e.g. a nested cohort's own frame)
			}
			// Walk this branch's sub-DAG, descending THROUGH any nested cohort and stopping at the fan-in.
			branchFailed, branchRewound := false, false
			stack := []stepMeta{head}
			for len(stack) > 0 {
				n := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if n.oldID == rewind {
					branchRewound = true
				}
				if n.status == workflow.StatusFailed {
					branchFailed = true
				}
				for _, c := range childrenByPred[n.oldID] {
					if inCohort(c.lineageID) {
						stack = append(stack, c)
					}
				}
			}
			if branchRewound {
				continue // this branch re-runs in the fork; it has not arrived
			}
			arrivals++
			if branchFailed {
				failures++
			}
		}
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_arrivals=?, cohort_failures=? WHERE step_id=?", arrivals, failures, idMap[s.oldID])
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	// Collect kept subgraph-caller children for the worklist (skipping the leaf fork step, which re-spawns a
	// fresh child). cloneTree processes them iteratively, so nesting depth costs no goroutine stack.
	var children []forkChild
	for _, s := range keep {
		childFlow, ok := childByCaller[s.oldID]
		if !ok || (isLeafFlow && s.oldID == cc.leafStepID) {
			continue
		}
		children = append(children, forkChild{originFlowID: childFlow, newSurgFlowID: newFlowID, newSurgStepID: idMap[s.oldID]})
	}

	// Apply the rewind treatment.
	if rewind != 0 {
		newRewindID := idMap[rewind]
		if isLeafFlow && rewind == cc.leafStepID {
			// Leaf fork step: merged input, cleared output/park/cohort, gated `created`.
			_, err = tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, parked=?, state=?, state_refs=?, changes=?, error='', attempt=0, interrupt_done=0, resume_data=?, subgraph_done=0, subgraph_result=?, subgraph_error='', successor_id=0, cohort_size=0, cohort_arrivals=0, cohort_failures=0, not_before=NOW_UTC(), lease_expires=NOW_UTC(), created_at=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
				workflow.StatusCreated, parkedNone, cc.mergedLeafState, emptyJSON, emptyJSON, emptyJSON, emptyJSON, newRewindID,
			)
			cc.newLeafStepID = newRewindID
		} else {
			// Ancestor caller: re-park so completeSurgraphFlow revives it when the re-run child completes.
			_, err = tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, parked=?, subgraph_done=0, subgraph_result=?, subgraph_error='', successor_id=0, error='', not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
				workflow.StatusRunning, parkedSubgraph, emptyJSON, newRewindID,
			)
		}
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	if isRoot {
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, step_id=?, started_at=NOW_UTC(), updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=?",
			workflow.StatusRunning, idMap[rewind], newFlowID,
		)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	} else {
		// Point the non-root flow at its current step, mirroring insertFlowTx (which leaves no creation path
		// with step_id=0): the rewound caller on the rewind chain, else the origin's own current step for an
		// off-path completed-prefix subgraph.
		currentStep := idMap[originStepID]
		if rewind != 0 {
			currentStep = idMap[rewind]
		}
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET step_id=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=?",
			currentStep, newFlowID,
		)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	return newFlowID, children, nil
}

func mergeWithOverrides(originalJSON []byte, overrides any) ([]byte, error) {
	state, err := workflow.NewState(originalJSON)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ov, err := workflow.NewState(overrides)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// Replace-merge the overrides onto the leaf state; a nil override is a delete (Merge keeps the tombstone,
	// DelNils enacts it).
	if err := state.Merge(ov); err != nil {
		return nil, errors.Trace(err)
	}
	state.DelNils()
	return json.Marshal(state)
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
			collected = append(collected, sweptMember{stepID: sid, lineageID: lid, status: status})
			nextFrontier = append(nextFrontier, sid)
		}
		rows.Close()
		frontier = nextFrontier
	}
	return collected, nil
}

// allDescendantSubgraphFlows returns every subgraph flow descended from flowID (any status), recursively.
// It fetches the whole tree (root + all descendants) in one scan via the denormalized root_flow_id, then
// derives flowID's descendants in memory from the surgraph_flow_id parent links - the same set the former
// level-by-level recursion produced, but one round-trip regardless of tree depth. root_flow_id gives tree
// *membership*; the surgraph link still gives the parent/child *structure* the BFS walks.
func (e *Engine) allDescendantSubgraphFlows(ctx context.Context, db sequel.Executor, flowID int) ([]int, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, surgraph_flow_id FROM dwarf_flows WHERE root_flow_id=(SELECT root_flow_id FROM dwarf_flows WHERE flow_id=?)",
		flowID,
	)
	if err != nil {
		return nil, errors.Trace(err)
	}
	childrenByParent := map[int][]int{}
	for rows.Next() {
		var id, parent int
		if err := rows.Scan(&id, &parent); err != nil {
			rows.Close()
			return nil, errors.Trace(err)
		}
		if parent != 0 {
			childrenByParent[parent] = append(childrenByParent[parent], id)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, errors.Trace(err)
	}

	var collected []int
	queue := []int{flowID}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, child := range childrenByParent[cur] {
			collected = append(collected, child)
			queue = append(queue, child)
		}
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
