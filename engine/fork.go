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
	"net/http"
	"slices"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// forkFlow clones a closed flow's execution tree up to a chosen step into a brand-new, self-contained
// root flow, then re-runs from that step with optional state overrides. The fork point may be ANY recorded
// step - in the root flow or deep inside a subgraph. The original is never mutated (closed flows are
// immutable); recovery/exploration is non-destructive.
//
// The design is a copy-only-keep clone, re-parking ancestor callers up the surgraph chain, a created->pending
// crash-gate, and uniform scheduling resolution.
func (e *Engine) forkFlow(ctx context.Context, stepKey string, stateOverrides any) (string, error) {
	shardNum, forkStepHour, forkFlowID, forkStepID, forkStepToken, err := keys.ParseStepKey(stepKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	overrides, err := workflow.NewState(stateOverrides)
	if err != nil {
		return "", errors.Trace(err, http.StatusBadRequest)
	}
	if err := checkFieldNames(overrides); err != nil {
		return "", errors.Trace(err)
	}
	sh, err := e.db.Shard(shardNum)
	if err != nil {
		return "", errors.New("step not found", http.StatusNotFound) // an unknown shard reads as an unknown step
	}
	db := sh.DB()

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "fork")
	defer doneTurn()

	// The live tables first, then the day the tree was archived in. Each pass opens its transaction by locking the
	// source root, and reads the source only under that lock: the reaper locks a root before it moves the tree, so a
	// tree the live pass locked stays live for the whole clone, and a live pass that locks nothing finds the tree
	// archived or never there. Nothing moves back out of the archive. The fork's root span is minted once, by
	// whichever pass first gets that far.
	e.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointForkBeforeClone)
	var traceParent string
	flowKey, err := e.forkFrom(ctx, db, liveTables, shardNum, forkFlowID, forkStepID, forkStepToken, forkStepHour, overrides, &traceParent)
	if !errors.Is(err, errForkSourceNotHere) {
		return flowKey, errors.Trace(err)
	}
	set := e.archives.Load()
	if set == nil || set.byShard[shardNum] == nil {
		return "", errors.New("step not found", http.StatusNotFound)
	}
	day, err := e.locateRetained(ctx, set, shardNum, forkFlowID, forkStepHour)
	if errors.StatusCode(err) == http.StatusNotFound {
		return "", errors.New("step not found", http.StatusNotFound)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	src := flowTables{flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day)}
	flowKey, err = e.forkFrom(ctx, db, src, shardNum, forkFlowID, forkStepID, forkStepToken, forkStepHour, overrides, &traceParent)
	// A day dropped since the locate, or a directory row outliving its day, fails on its missing table: the source is
	// gone.
	if errors.Is(err, errForkSourceNotHere) || sequel.IsUndefinedTableError(err) {
		return "", errors.New("step not found", http.StatusNotFound)
	}
	return flowKey, errors.Trace(err)
}

// errForkSourceNotHere is forkFrom's report that the tables it was given do not hold the fork step's flow.
var errForkSourceNotHere = errors.New("fork source not in these tables")

// forkFrom forks the tree of the fork step read from src - the live tables, or the archive day's tables the tree is
// in - into a new tree in the live tables, in one transaction. It returns errForkSourceNotHere when src does not
// hold the step's flow. traceParent holds the fork's root span once minted.
func (e *Engine) forkFrom(ctx context.Context, db *sequel.DB, src flowTables, shardNum, forkFlowID, forkStepID int,
	forkStepToken string, forkStepHour int, overrides workflow.State, traceParent *string) (string, error) {
	var cc *forkClone
	var rootFlowID, newRootFlowID int
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		// diagnostics.FaultForkCommit fails this transaction once, before the clone runs, so the test proves the whole
		// clone rolls back atomically (origin byte-identical, zero partial clone rows) and a retry then
		// forks cleanly - pinning Fork's "crash mid-clone rolls back, origin never mutated" claim.
		if e.diags.Seams().IsFault(diagnostics.FaultForkCommit) {
			return errors.New("injected fault: " + diagnostics.FaultForkCommit)
		}
		// Write-first, on the source root named by the fork step's flow. The clone reads the source before its first
		// write, and on SQLite a transaction that reads first holds a SHARED lock it must upgrade at that write:
		// against a concurrent writer the upgrade cannot wait, so SQLite returns SQLITE_BUSY (SQLITE_LOCKED under a
		// shared cache) at once whatever the busy timeout, and every Transact retry is read-first again (measured:
		// ~1 Fork in 8 failed under the bench soak). Locking the source root first makes the first statement a write
		// there; on the server dialects it is a locking read, which holds off the reaper and a Delete or Purge of
		// the source while it is cloned. An archived source is locked in its day's table by the same statement: on
		// SQLite and SQL Server it flips the archived root's touch, the one write an archived row takes.
		var cerr error
		cc, rootFlowID, cerr = e.forkSource(ctx, tx, src, shardNum, forkFlowID, forkStepID, forkStepToken, forkStepHour, overrides, traceParent)
		if cerr != nil {
			return errors.Trace(cerr)
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
	if errors.Is(err, errForkSourceNotHere) {
		return "", errForkSourceNotHere
	}
	if err != nil {
		return "", errors.Trace(err)
	}

	newFlowKey := keys.NewFlowKey(shardNum, cc.newRootFlowHour, newRootFlowID, cc.rootFlowToken)
	e.diags.Logger().InfoContext(ctx, "Flow forked", "fromRoot", rootFlowID, "forkStep", forkStepID, "to", newRootFlowID)
	// Every flow on the rewind chain - the new root and each subgraph flow between it and the fork step - is
	// cloned running and ends through completeFlow, which counts it in dwarf_flows_closed. Each must be
	// counted as started too, or the in-flight panel (started - ended) drifts by one per flow.
	for _, workflowURL := range cc.startedURLs {
		e.metricFlowStarted(ctx, workflowURL, cc.priority, shardNum)
	}
	e.enqueueStep(ctx, shardNum, cc.newLeafStepID)
	return newFlowKey, nil
}

// forkSource locks the source root in src, as tx's first statement, then reads on tx what the clone of the fork
// step's tree needs: the step, the surgraph chain above it, and the root's scheduling, with the leaf's state
// materialized and overridden. It returns the clone's shared state and the source root's id, and
// errForkSourceNotHere when src holds no flow the key names.
func (e *Engine) forkSource(ctx context.Context, tx *sequel.Tx, src flowTables, shardNum, forkFlowID, forkStepID int,
	forkStepToken string, forkStepHour int, overrides workflow.State, traceParent *string) (*forkClone, int, error) {
	// The root is locked only if it has closed and is not marked for deletion, tested on the row the lock takes: that is
	// the row as it stands once the lock is granted. A root not closed is not locked at all, so a Fork refused for it
	// crosses no lock order with Terminate. The root is named through the fork step and its token, so a key with a
	// forged token locks nothing: the lock is a write on SQLite and SQL Server, and holds off the reaper everywhere.
	rootOf := "flow_id=(SELECT f.root_flow_id FROM " + src.flows + " f JOIN " + src.steps + " s ON s.flow_id=f.flow_id" +
		" WHERE f.flow_id=? AND f.created_hour=? AND s.step_id=? AND s.step_token=? AND s.flow_created_hour=?)"
	rootArgs := []any{forkFlowID, forkStepHour, forkStepID, forkStepToken, forkStepHour}
	locked, err := lockRows(ctx, tx, src.flows, rootOf+" AND status IN ("+terminalStatusesSQL+") AND delete_after_ms=0",
		rootArgs...)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	if locked == 0 {
		// Why nothing was locked is read from the row as it stands now. On MySQL a plain read here would see the
		// snapshot the transaction's first read took, before a move or a Delete that committed while the lock waited:
		// it would find the root still there and closed, and answer that the flow is scheduled for deletion where the
		// tree has in fact moved to the archive. A locking read is a current read there.
		current := ""
		if d := tx.DriverName(); d == "mysql" || d == "mariadb" {
			current = " LOCK IN SHARE MODE"
		}
		var rootStatus string
		var published int
		publication, args := publicationColumn(tx.DriverName(), src)
		err := tx.QueryRowContext(ctx, "SELECT "+publication+", status FROM "+src.flows+" WHERE "+rootOf+current, append(args, rootArgs...)...).
			Scan(&published, &rootStatus)
		if err == sql.ErrNoRows {
			return nil, 0, errForkSourceNotHere
		}
		if err != nil {
			return nil, 0, errors.Trace(err)
		}
		if !workflow.IsTerminalStatus(rootStatus) {
			return nil, 0, errors.New("can only fork a closed flow (status: %s)", rootStatus, http.StatusConflict)
		}
		if published != 0 {
			e.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointForkPublished)
			return nil, 0, errForkSourceNotHere
		}
		if src != liveTables {
			// An archived root marked for deletion hides its tree: it reads as deleted, as every other reader has it,
			// whichever way the locate found the day.
			return nil, 0, errForkSourceNotHere
		}

		// A flow scheduled for deletion is on its way out, so naming it is an error, as for Continue.
		return nil, 0, errors.New("cannot fork a flow scheduled for deletion", http.StatusConflict)
	}
	e.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointForkRootLocked)

	// Resolve the fork step, its owning flow, and that flow's token (needed to walk the surgraph chain).
	var leafFlowID, leafFlowPayloadVer int
	var forkStepState, forkStepRefs, forkStepReduced []byte
	var leafFlowToken string
	err = tx.QueryRowContext(ctx,
		"SELECT s.flow_id, s.state, s.state_refs, s.state_reduced, f.payload_ver, f.flow_token FROM "+src.steps+" s JOIN "+src.flows+" f ON f.flow_id=s.flow_id WHERE s.step_id=? AND s.flow_id=? AND s.step_token=? AND s.flow_created_hour=?",
		forkStepID, forkFlowID, forkStepToken, forkStepHour,
	).Scan(&leafFlowID, &forkStepState, &forkStepRefs, &forkStepReduced, &leafFlowPayloadVer, &leafFlowToken)
	if err == sql.ErrNoRows {
		return nil, 0, errors.New("step not found", http.StatusNotFound)
	}
	if err == nil {
		err = checkPayloadVer(leafFlowPayloadVer)
	}
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// Walk up to the root, recording each flow's rewind step: the leaf fork step in its own flow, and the
	// caller step that spawned the lower flow in each ancestor.
	chainFlowIDs, chainStepIDs, _, err := e.surgraphChainIn(ctx, tx, src, shardNum, leafFlowID, forkStepHour, leafFlowToken)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	rootFlowID := chainFlowIDs[len(chainFlowIDs)-1].(int)
	rewindByFlow := map[int]int{leafFlowID: forkStepID}
	for i, callerStep := range chainStepIDs {
		rewindByFlow[chainFlowIDs[i+1].(int)] = callerStep.(int)
	}

	// Gather the root-flow overrides.
	// The root's status and deletion stamp were tested by the lock.
	var rootWorkflowURL string
	var rootPriority, rootTimeBudgetMs int
	var rootFairnessKey string
	var rootFairnessWeight float64
	err = tx.QueryRowContext(ctx,
		"SELECT workflow_url, priority, fairness_key, fairness_weight, time_budget_ms FROM "+src.flows+" WHERE flow_id=?",
		rootFlowID,
	).Scan(&rootWorkflowURL, &rootPriority, &rootFairnessKey, &rootFairnessWeight, &rootTimeBudgetMs)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}

	// The fork's leaf step is rewritten with fully MATERIALIZED state (and state_refs cleared below): the
	// caller's overrides merge onto the real values, and the leaf is the one row the fork re-runs from, so it
	// must not depend on anchors whose ids are about to be remapped. Its successors re-mint refs normally.
	resolvedLeafState, err := e.resolvedStepState(ctx, tx, src.steps, shardNum, leafFlowToken, forkStepState, forkStepRefs, rootWorkflowURL)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	leafReduced, err := e.forkLeafReduced(ctx, tx, src, shardNum, leafFlowID, leafFlowToken, resolvedLeafState, forkStepReduced)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	mergedLeafState, err := mergeWithOverrides(resolvedLeafState, overrides)
	if err != nil {
		return nil, 0, errors.Trace(err)
	}
	if err := e.checkPayloadSize("state of the fork step", mergedLeafState); err != nil {
		return nil, 0, err
	}

	if *traceParent == "" {
		*traceParent = e.mintWorkflowSpan(ctx, rootWorkflowURL, "") // detached, like Continue
	}
	// The fork inherits the origin root's scheduling and baggage (no FlowOptions), applied uniformly to
	// every cloned flow and step.
	return &forkClone{
		src:             src,
		leafFlowID:      leafFlowID,
		leafStepID:      forkStepID,
		mergedLeafState: mergedLeafState,
		leafReduced:     leafReduced,
		rewindByFlow:    rewindByFlow,
		rootFlowToken:   keys.RandomIdentifier(16),
		rootTraceParent: *traceParent,
		priority:        rootPriority,
		fairnessKey:     rootFairnessKey,
		fairnessWeight:  rootFairnessWeight,
		timeBudgetMs:    rootTimeBudgetMs,
	}, rootFlowID, nil
}

// forkClone carries the state shared across the flows of one Fork clone.
type forkClone struct {
	src             flowTables // the tables the origin tree is read from: the live ones, or its archive day's
	leafFlowID      int
	leafStepID      int
	mergedLeafState []byte
	leafReduced     []byte // the leaf's state_reduced, frozen against mergedLeafState replacing its state
	rewindByFlow    map[int]int
	rootFlowToken   string
	rootTraceParent string
	priority        int
	fairnessKey     string
	fairnessWeight  float64
	timeBudgetMs    int
	newLeafStepID   int
	newRootFlowID   int      // the cloned root's flow_id; descendants inherit it as their root_flow_id
	newRootFlowHour int      // the cloned root's created_hour, for the key Fork returns
	startedURLs     []string // workflow_url of every flow cloned to run again, for flows_started after commit
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

// cloneTree clones the whole closed tree rooted at rootFlowID into a fresh flow tree and returns the new
// root's flow id. It walks the tree with an explicit LIFO worklist rather than recursion: each flow is cloned
// to completion by cloneOneFlow, which returns its kept subgraph-caller children to enqueue. Only ONE flow's
// clone state (its step metadata, id map, and that flow's graph/baggage JSON) is live at a time, and the
// goroutine stack stays O(1) at any nesting depth. Depth-proportional recursion here would instead hold every
// ancestor's clone state simultaneously and, at pathological depth, overflow the goroutine stack - which is
// fatal (unrecoverable by errors.CatchPanic, unlike a host-call panic). Children are pushed in reverse so they
// pop in discovery order (DFS preorder), keeping cloned-id assignment stable.
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
		sub, err := e.collectDAGSubtree(ctx, tx, cc.src.steps, originFlowID, rewind)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		for _, m := range sub {
			pruned[m.stepID] = true
		}
	}

	var status, workflowURL, workflowName, traceParent, cancelReason, graphHash, flowError string
	var graphJSON, baggageJSON, finalStateJSON []byte
	var deleteOnCompletion, originStoppedStepID, originPayloadVer int
	err := tx.QueryRowContext(ctx,
		"SELECT status, workflow_url, workflow_name, graph, graph_hash, baggage, trace_parent, delete_on_completion, stopped_step_id, cancel_reason, payload_ver, final_state, error FROM "+cc.src.flows+" WHERE flow_id=?",
		originFlowID,
	).Scan(&status, &workflowURL, &workflowName, &graphJSON, &graphHash, &baggageJSON, &traceParent, &deleteOnCompletion, &originStoppedStepID, &cancelReason, &originPayloadVer, &finalStateJSON, &flowError)
	if err == nil {
		// Each flow of the tree has its own format (a subgraph child is created in its creator's), and the clone
		// copies its rows, so it keeps the origin's.
		err = checkPayloadVer(originPayloadVer)
	}
	if err != nil {
		return 0, nil, errors.Trace(err)
	}

	newStatus := status
	if isRoot || rewind != 0 { // on the rewind chain
		newStatus = workflow.StatusRunning
		cc.startedURLs = append(cc.startedURLs, workflowURL)
		// It runs again and writes its own outcome; it is closed in no form yet.
		finalStateJSON, flowError = emptyJSON, ""
	}
	// A flow off the chain keeps its status, so it keeps the outcome frozen with it: nothing recomputes the outcome
	// of a closed flow, and Snapshot of the clone's child key would report the defaults.
	// Scheduling (cc.*) is resolved once and applied uniformly to every cloned flow and step - the tree is
	// uniform and a deep-subgraph fork's re-running leaf must carry the same overridden values.
	forkedFromStep, newTrace := 0, traceParent
	flowPriority, flowFairnessKey, flowFairnessWeight, flowBudget := cc.priority, cc.fairnessKey, cc.fairnessWeight, cc.timeBudgetMs
	if isRoot {
		newStatus = workflow.StatusCreated // gate; flipped to running below once this flow is mapped
		forkedFromStep, newTrace = cc.leafStepID, cc.rootTraceParent
		deleteOnCompletion = 0
	}

	// The cloned root takes cc.rootFlowToken, the token in the key returned to the caller; it is its own root, and
	// its descendants inherit its id as their root_flow_id.
	// The origin's cancel reason comes along: a kept fan-out branch lost to the origin's cancellation is a loss
	// the clone inherits, and a fork whose only losses are such branches resolves cancelled - so it reports the
	// reason that explains it, rather than a cancellation nobody gave a reason for. cancel_watermark is
	// deliberately NOT copied: the fork is not under cancellation, so none of its steps may pass one on.
	columns := "flow_token, workflow_url, workflow_name, graph, graph_bytes, graph_hash, baggage, baggage_bytes, status, surgraph_flow_id, surgraph_step_id, forked_from_step, trace_parent, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms, engine_id, cancel_reason, payload_ver, final_state, final_state_bytes, error"
	values := "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?"
	flowToken := cc.rootFlowToken
	if !isRoot {
		flowToken = keys.RandomIdentifier(16)
	}
	args := []any{flowToken, workflowURL, workflowName, graphJSON, len(graphJSON), graphHash, baggageJSON, len(baggageJSON), newStatus, newSurgFlowID, newSurgStepID, forkedFromStep, newTrace, deleteOnCompletion, flowPriority, flowFairnessKey, flowFairnessWeight, flowBudget, e.engineID, cancelReason, originPayloadVer, finalStateJSON, len(finalStateJSON), flowError}
	var self []string
	if isRoot {
		self = append(self, "root_flow_id")
	} else {
		columns, values, args = columns+", root_flow_id", values+", ?", append(args, cc.newRootFlowID)
	}
	insert, fixup := selfFlowInsertSQL(tx.DriverName(), columns, values, self...)
	newFlowID64, err := tx.InsertReturnID(ctx, "flow_id", insert, args...)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	newFlowID := int(newFlowID64)
	if fixup != "" {
		_, err = tx.ExecContext(ctx, fixup, selfFlowFixupArgs(newFlowID64, len(self))...)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
		e.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointFlowSelfFixup)
	}
	if newSurgStepID != 0 {
		// The step clone does not copy parked_child_flow_id, whose origin value names an origin flow. An ancestor
		// caller the rewind re-parked, already written by its own flow's clone, is owned by this cloned child, or its
		// re-run child's completion could never revive it. A caller off the rewind path is not parked and keeps 0.
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_steps SET parked_child_flow_id=? WHERE step_id=? AND parked=?", newFlowID, newSurgStepID, parkedSubgraph)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}
	// The clone is a new flow, so its creation hour is its own, from the database clock (the column's default).
	// Its steps carry it, not the origin's: every step carries its own flow's hour.
	var newFlowHour int
	err = tx.QueryRowContext(ctx, "SELECT created_hour FROM dwarf_flows WHERE flow_id=?", newFlowID).Scan(&newFlowHour)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	if isRoot {
		cc.newRootFlowID, cc.newRootFlowHour = newFlowID, newFlowHour
	}

	// Direct subgraph children of this flow, keyed by caller step (latest child per caller).
	childByCaller := map[int]int{}
	crows, err := tx.QueryContext(ctx, "SELECT flow_id, surgraph_step_id FROM "+cc.src.flows+" WHERE surgraph_flow_id=? ORDER BY flow_id", originFlowID)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	for crows.Next() {
		var cFlow, cCaller int
		if err := crows.Scan(&cFlow, &cCaller); err != nil {
			crows.Close()
			return 0, nil, errors.Trace(err)
		}
		childByCaller[cCaller] = cFlow
	}
	crows.Close()
	if err := crows.Err(); err != nil {
		return 0, nil, errors.Trace(err)
	}

	type stepMeta struct {
		oldID, predID, succID, lineageID, cohortSize int
		status                                       string
		refs                                         staterefs.Refs
	}
	mrows, err := tx.QueryContext(ctx,
		"SELECT step_id, predecessor_id, successor_id, lineage_id, cohort_size, status, state_refs FROM "+cc.src.steps+" WHERE flow_id=? ORDER BY step_id",
		originFlowID,
	)
	if err != nil {
		return 0, nil, errors.Trace(err)
	}
	var keep []stepMeta
	for mrows.Next() {
		var s stepMeta
		var refsJSON []byte
		if err := mrows.Scan(&s.oldID, &s.predID, &s.succID, &s.lineageID, &s.cohortSize, &s.status, &refsJSON); err != nil {
			mrows.Close()
			return 0, nil, errors.Trace(err)
		}
		if pruned[s.oldID] {
			continue
		}
		var perr error
		s.refs, perr = staterefs.Parse(refsJSON)
		if perr != nil {
			mrows.Close()
			return 0, nil, errors.Trace(perr)
		}
		keep = append(keep, s)
	}
	mrows.Close()
	if err := mrows.Err(); err != nil {
		return 0, nil, errors.Trace(err)
	}

	// A closed fork origin never holds an interrupted step (interrupt forces the whole surgraph chain -
	// up to the root - open, and Fork rejects an open root), so a KEPT interrupted step (one
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
	// that had not settled when the origin terminalized, or a step of a Terminate that raced. Unlike an
	// interrupted step (an invariant violation, rejected above), this is a legitimate window - but the origin
	// froze its outcome with that branch in flight, so the fork must freeze it too. Copied verbatim it would
	// (a) let lease recovery reset its stale-leased `running` row to pending and re-dispatch it in the running
	// fork - a surprise duplicate execution - and (b) as a cohort member count as neither an arrival nor a
	// failure, wedging the fork's fan-in forever. Normalize it to terminated (it never got a chance to react -
	// no onError delivery happens here, matching the forceful, non-catchable shape of the race that produced
	// it): the keep-meta update makes the cohort recompute below count its branch as a loss, like any branch a
	// Terminate stopped, and the cloned rows are frozen after the inserts.
	var stragglers []int
	for i := range keep {
		s := &keep[i]
		if s.oldID != rewind && !workflow.IsTerminalStatus(s.status) {
			s.status = workflow.StatusTerminated
			stragglers = append(stragglers, s.oldID)
		}
	}

	// Copy kept steps (all columns DB-side, native timestamps), overriding flow_id, a fresh token, and the
	// flow's scheduling. The leaf fork step is inserted `created` (gated); all others keep their status.
	idMap := make(map[int]int, len(keep))
	isLeafFlow := originFlowID == cc.leafFlowID
	for _, s := range keep {
		status, args := "status", []any{newFlowID, keys.RandomIdentifier(16)}
		if isLeafFlow && s.oldID == cc.leafStepID {
			status, args = "?", append(args, workflow.StatusCreated)
		}
		args = append(args, flowPriority, flowFairnessKey, flowFairnessWeight, e.engineID, newFlowHour, s.oldID)
		newID, err := tx.InsertReturnID(ctx, "step_id", cloneStepSQL(cc.src.steps, status), args...)
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
			for field, ref := range s.refs {
				mapped, missing, ok := ref.Remap(func(anchor int) (int, bool) {
					to, ok := idMap[anchor]
					return to, ok && to != 0
				})
				if !ok {
					return 0, nil, errors.New("cannot fork: step %d refs field %q at step %d, which is not in the clone", s.oldID, field, missing)
				}
				remapped[field] = mapped
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

	// Freeze the cloned straggler rows: terminated + cleared park/lease, so lease recovery (running-only) and
	// the selection scan both ignore them (terminal implies parkedNone). The INSERT above copied their origin
	// status/lease verbatim; this is the DB counterpart of the keep-meta normalization done earlier.
	for _, oldID := range stragglers {
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
			workflow.StatusTerminated, parkedNone, idMap[oldID],
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
	// so `arrivals <= size` always holds. Do not count members: that breaks it on any multi-step branch -
	// `Seed --forEach(3)--> Cell --> Enrich --> Join` has 6 members for size 3, so a fork writes arrivals=5,
	// size=3 against the pinned invariant `cohort_arrivals <= cohort_size` (`invariants_test.go`, asserted
	// across the chaos-soak and race suites). A fixture whose branches are ONE step cannot see it: there
	// members == branches.
	//
	// A branch is the sub-DAG rooted at a direct child of the spawn (predecessor_id = spawn) that shares the
	// spawn's cohort lineage. Fork requires a TERMINAL origin, so every kept step is settled: a branch has
	// arrived unless it is the one being rewound (that branch re-runs, and its post-rewind steps were pruned
	// anyway), and it counts a failure if any of its steps failed. Exclude the whole rewound BRANCH, not merely
	// the rewind step: with `rewind = Enrich1`, its sibling `Cell1` sits in the same branch and must not count
	// as an arrival.
	//
	// MEMBERSHIP IS THE LINEAGE CHAIN, NOT THE LINEAGE ID. A NESTED fan-out re-lineages its own children
	// (`childLineageID = stepID`), so in `Seed -forEach-> Cell -forEach-> Chunk -> JoinChunk -> JoinCell` the
	// Chunk steps carry `lineage_id = Cell`, not `Seed`. Do not test `c.lineageID == s.oldID`: it DEAD-ENDS
	// the outer walk at the inner spawn - the Chunks are filtered out, and JoinChunk (which does carry the
	// outer lineage) is only reachable THROUGH them, since its predecessor is the inner cohort's last
	// completer. Everything past the inner cohort in that branch becomes invisible, with two consequences: a
	// rewind at or past the inner frame is never seen, so the branch counts as ARRIVED and its re-run pushes
	// `cohort_arrivals` past `cohort_size`; and a failure inside a kept branch's inner cohort is never seen, so
	// the clone loses `cohort_failures` and a fork that must re-fail COMPLETES instead - silently absorbing an
	// unrecovered branch failure, which inverts the whole point of the partial-recovery fork.
	//
	// So a step belongs to spawn S's cohort - at ANY nesting depth - iff walking its lineage chain upward
	// reaches S. That descends through nested cohorts and still stops cleanly at the outer fan-in, whose lineage
	// is the spawn's OWN lineage and so never reaches S. Every spawn in a kept step's chain is itself kept (a
	// lineage ancestor is a DAG ancestor, and pruning removes only descendants of the rewind step), so this is a
	// pure in-memory walk over rows already loaded - no extra query, at any depth.
	//
	// NOT EVERY ARRIVAL IS A BRANCH. A source that fans out to its own fan-in as well (`Split -> Work` plus
	// `Split -> Join`) counts that edge in cohort_size and bumps cohort_arrivals on itself as it spawns, so no
	// branch head carries it. Those self-arrivals are cohort_size minus the heads; recount only the branches
	// and the clone sits one short of cohort_size forever. A rewound spawn re-spawns, so it gets none.
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
		arrivals, failures, cancellations, heads := 0, 0, 0, 0
		for _, head := range childrenByPred[s.oldID] {
			if head.lineageID != s.oldID {
				continue // not a member of this spawn's cohort (e.g. a nested cohort's own frame)
			}
			heads++
			// Walk this branch's sub-DAG, descending THROUGH any nested cohort and stopping at the fan-in.
			branchFailed, branchCancelled, branchRewound := false, false, false
			stack := []stepMeta{head}
			for len(stack) > 0 {
				n := stack[len(stack)-1]
				stack = stack[:len(stack)-1]
				if n.oldID == rewind {
					branchRewound = true
				}
				// A cancelled step is an unrecovered loss like a failed one - failStep counted it in
				// cohort_failures - and the branch's loss is a cancellation only if nothing in it failed, the
				// same rule the live counters apply when a nested cohort resolves. Counted any other way, the
				// clone re-derives a fan-in that converges, or resolves cancelled, where the original failed.
				//
				// A terminated step is a loss too, and not a cancellation: a Terminate stopped the branch before
				// it completed, and nothing in the fork runs it again. Counted as an arrival, the fan-in converges
				// on whatever the branch finished and silently lacks the rest, at any depth - an inner cohort the
				// branch had started has no fan-in row to carry its work. To re-run such a branch, fork at or
				// before its fan-out.
				switch n.status {
				case workflow.StatusFailed, workflow.StatusTerminated:
					branchFailed = true
				case workflow.StatusCancelled:
					branchCancelled = true
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
			if branchFailed || branchCancelled {
				failures++
			}
			if branchCancelled && !branchFailed {
				cancellations++
			}
		}
		if s.oldID != rewind {
			arrivals += max(0, s.cohortSize-heads)
		}
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_arrivals=?, cohort_failures=?, cohort_cancellations=? WHERE step_id=?", arrivals, failures, cancellations, idMap[s.oldID])
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
				"UPDATE dwarf_steps SET status=?, parked=?, state=?, state_bytes=?, state_refs=?, state_reduced=?, changes=?, changes_bytes=?, error='', attempt=0, interrupt_done=0, resume_data=?, resume_data_bytes=?, subgraph_done=0, subgraph_result=?, subgraph_result_bytes=?, subgraph_error=?, successor_id=0, cohort_size=0, cohort_arrivals=0, cohort_failures=0, cohort_cancellations=0, not_before=NOW_UTC(), lease_expires=NOW_UTC(), created_at=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
				workflow.StatusCreated, parkedNone, cc.mergedLeafState, len(cc.mergedLeafState), emptyJSON, cc.leafReduced, emptyJSON, len(emptyJSON), emptyJSON, len(emptyJSON), emptyJSON, len(emptyJSON), emptyJSON, newRewindID,
			)
			cc.newLeafStepID = newRewindID
		} else {
			// Ancestor caller: re-park so completeSurgraphFlow revives it when the re-run child completes.
			// Its changes are cleared: the revived run re-executes the task from the top, so keeping the
			// origin's changes as prior changes would fold every post-Subgraph contribution twice.
			_, err = tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, parked=?, changes=?, changes_bytes=?, subgraph_done=0, subgraph_result=?, subgraph_result_bytes=?, subgraph_error=?, successor_id=0, error='', not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
				workflow.StatusRunning, parkedSubgraph, emptyJSON, len(emptyJSON), emptyJSON, len(emptyJSON), emptyJSON, newRewindID,
			)
		}
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	if isRoot {
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, stopped_step_id=0, started_at=NOW_UTC(), updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=?",
			workflow.StatusRunning, newFlowID,
		)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	} else {
		// stopped_step_id is the step a flow stopped at: none for a flow on the rewind chain, which runs again, and the
		// origin's own, remapped, for an off-path completed-prefix subgraph.
		stoppedStep := idMap[originStoppedStepID]
		if rewind != 0 {
			stoppedStep = 0
		}
		_, err = tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET stopped_step_id=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=?",
			stoppedStep, newFlowID,
		)
		if err != nil {
			return 0, nil, errors.Trace(err)
		}
	}

	return newFlowID, children, nil
}

// forkLeafReduced is the state_reduced column for a Fork's leaf: the original record, if any, frozen against the
// overrides about to replace the state it describes (frozenReduced). stateJSON is the leaf's original state,
// resolved.
func (e *Engine) forkLeafReduced(ctx context.Context, db sequel.Executor, src flowTables, shardNum, flowID int, flowToken string, stateJSON, reducedJSON []byte) ([]byte, error) {
	var rec map[string]*reduced
	if err := json.Unmarshal(reducedJSON, &rec); err != nil {
		return nil, errors.Trace(err)
	}
	if len(rec) == 0 {
		return emptyJSON, nil
	}
	graph, _, err := e.flowGraphIn(ctx, db, src, shardNum, flowID, flowToken)
	if err != nil {
		return nil, errors.Trace(err)
	}
	state, err := workflow.NewState(stateJSON)
	if err != nil {
		return nil, errors.Trace(err)
	}
	frozen, err := frozenReduced(state, rec, graph.Reducers())
	if err != nil {
		return nil, errors.Trace(err)
	}
	return reducedColumn(frozen), nil
}

func mergeWithOverrides(originalJSON []byte, overrides workflow.State) ([]byte, error) {
	state, err := workflow.NewState(originalJSON)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// The overrides REPLACE, reduced fields included - they edit the forked step's input, they are not a step's
	// write, and folding them would make a corrupted history unrepairable. A nil override deletes.
	if err := state.Apply(workflow.ChangesOf(overrides), nil); err != nil {
		return nil, errors.Trace(err)
	}
	return json.Marshal(state)
}

type sweptMember struct {
	stepID    int
	lineageID int
	status    string
}

func (e *Engine) collectDAGSubtree(ctx context.Context, db sequel.Executor, steps string, flowID, startStepID int) ([]sweptMember, error) {
	visited := map[int]bool{startStepID: true}
	var collected []sweptMember
	frontier := []int{startStepID}
	for len(frontier) > 0 {
		var nextFrontier []int
		// Chunked: the frontier past a spawn is the whole cohort, and the query binds it twice.
		for chunk := range slices.Chunk(frontier, inListChunk) {
			ph, ids := inList(chunk)
			args := append([]any{flowID}, ids...)
			args = append(args, ids...)
			rows, err := db.QueryContext(ctx,
				"SELECT step_id, lineage_id, status FROM "+steps+" WHERE flow_id=? AND ("+
					"step_id IN (SELECT successor_id FROM "+steps+" WHERE step_id IN ("+ph+") AND successor_id<>0)"+
					" OR predecessor_id IN ("+ph+"))",
				args...)
			if err != nil {
				return nil, errors.Trace(err)
			}
			for rows.Next() {
				var sid, lid int
				var status string
				if err := rows.Scan(&sid, &lid, &status); err != nil {
					rows.Close()
					return nil, errors.Trace(err)
				}
				if visited[sid] {
					continue
				}
				visited[sid] = true
				collected = append(collected, sweptMember{stepID: sid, lineageID: lid, status: status})
				nextFrontier = append(nextFrontier, sid)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		frontier = nextFrontier
	}
	return collected, nil
}

// allDescendantSubgraphFlows returns every subgraph flow descended from flowID (any status), recursively.
// It fetches the whole tree (root + all descendants) in one scan via the denormalized root_flow_id, then
// derives flowID's descendants in memory from the surgraph_flow_id parent links - the same set a
// level-by-level walk would find, but in one round trip regardless of tree depth. root_flow_id gives tree
// *membership*; the surgraph link still gives the parent/child *structure* the BFS walks.
func (e *Engine) allDescendantSubgraphFlows(ctx context.Context, db sequel.Executor, flowID int) ([]int, error) {
	return e.allDescendantSubgraphFlowsIn(ctx, db, liveTables, flowID)
}

// allDescendantSubgraphFlowsIn is allDescendantSubgraphFlows over the flows table of t.
func (e *Engine) allDescendantSubgraphFlowsIn(ctx context.Context, db sequel.Executor, t flowTables, flowID int) ([]int, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT flow_id, surgraph_flow_id FROM "+t.flows+" WHERE root_flow_id=(SELECT root_flow_id FROM "+t.flows+" WHERE flow_id=?)",
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

// callerChildrenSQL selects every child flow of a caller step. It runs on every flow.Retry rewind, so the literal
// surgraph_step_id>0 matters: without it SQLite and SQL Server cannot use the partial index for the bound id, and
// scan dwarf_flows.
const callerChildrenSQL = "SELECT flow_id FROM dwarf_flows WHERE surgraph_step_id=? AND surgraph_step_id>0"

func (e *Engine) deleteSubgraphFlowsRootedAt(ctx context.Context, tx sequel.Executor, surgraphStepID int) error {
	var rootChildren []int
	rows, err := tx.QueryContext(ctx, callerChildrenSQL, surgraphStepID)
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
	// The id lists are chunked: a caller step's subtree can hold more flows than a statement can bind.
	allIDs := append([]int{}, rootChildren...)
	current := rootChildren
	for len(current) > 0 {
		var next []int
		for chunk := range slices.Chunk(current, inListChunk) {
			ph, ids := inList(chunk)
			nestedRows, err := tx.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE surgraph_flow_id IN ("+ph+")", ids...)
			if err != nil {
				return errors.Trace(err)
			}
			for nestedRows.Next() {
				var id int
				if err := nestedRows.Scan(&id); err != nil {
					nestedRows.Close()
					return errors.Trace(err)
				}
				next = append(next, id)
			}
			err = nestedRows.Err()
			nestedRows.Close()
			if err != nil {
				return errors.Trace(err)
			}
		}
		allIDs = append(allIDs, next...)
		current = next
	}
	// Every step before any flow, the order the lifecycle deletes take.
	for _, table := range []string{"dwarf_steps", "dwarf_flows"} {
		for chunk := range slices.Chunk(allIDs, inListChunk) {
			ph, ids := inList(chunk)
			tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE flow_id IN ("+ph+")", ids...)
		}
	}
	return nil
}

// cloneStepSQL copies one step row of steps into a cloned flow DB-side, every column but the id: under the new flow
// id, a fresh token, the root's scheduling, this engine and the clone's creation hour (bound in that order, the
// status between the token and the scheduling when status is "?"), then the source step id. status is the status
// column's expression: the source's own ("status"), or a bind for the leaf, which is held created.
func cloneStepSQL(steps, status string) string {
	return "INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_bytes, state_refs, state_reduced, changes, changes_bytes, interrupt_payload, interrupt_payload_bytes, status, error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, priority, fairness_key, fairness_weight, interrupt_done, resume_data, resume_data_bytes, subgraph_done, subgraph_result, subgraph_result_bytes, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at, engine_id, flow_created_hour)" +
		" SELECT ?, step_depth, ?, task_name, task_url, state, state_bytes, state_refs, state_reduced, changes, changes_bytes, interrupt_payload, interrupt_payload_bytes, " + status + ", error, time_budget_ms, attempt, lineage_id, cohort_size, cohort_arrivals, cohort_failures, fan_out_ordinal, predecessor_id, successor_id, ?, ?, ?, interrupt_done, resume_data, resume_data_bytes, subgraph_done, subgraph_result, subgraph_result_bytes, subgraph_error, parked, not_before, lease_expires, created_at, started_at, updated_at, ?, ? FROM " + steps + " WHERE step_id=?"
}
