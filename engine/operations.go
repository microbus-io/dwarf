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
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/graphcache"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/latch"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// create inserts a new flow for a workflow and runs it.
func (e *Engine) create(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, err error) {
	if workflowURL == "" {
		return "", errors.New("workflow URL is required", http.StatusBadRequest)
	}
	// Reject a negative priority/weight/budget rather than silently coercing it to the default
	// (resolveFlowOptions treats <=0 as "unset"): a negative value is a caller bug, and swallowing it hides
	// it. 0 stays "use the engine default" for all three, so this only rejects genuinely-invalid input.
	// Genesis-only (Create/Run); derived ops (Continue/Fork/subgraph) inherit already-validated values and
	// take no FlowOptions.
	if opts != nil {
		if opts.Priority < 0 {
			return "", errors.New("priority must be >= 0", http.StatusBadRequest)
		}
		// The range is the contract callers configure: a priority beyond it would run, but only as the selection
		// scan's overflow, after every level in the range.
		if opts.Priority > e.maxPriority {
			return "", errors.New("priority must be at most the maximum priority %d", e.maxPriority, http.StatusBadRequest)
		}
		if opts.FairnessWeight < 0 {
			return "", errors.New("fairness weight must be >= 0", http.StatusBadRequest)
		}
		// 0 means "use the engine default"; anything else must survive the millisecond persistence, or the
		// step is stamped with a 0 budget and its task's deadline has already passed when it is dispatched.
		if opts.TimeBudget < 0 || (opts.TimeBudget > 0 && opts.TimeBudget < time.Millisecond) {
			return "", errors.New("time budget must be 0 (engine default) or at least 1ms", http.StatusBadRequest)
		}
		if opts.TimeBudget > maxTimeBudget {
			return "", errors.New("time budget must be at most %v", maxTimeBudget, http.StatusBadRequest)
		}
	}
	opts = e.resolveFlowOptions(opts)
	// The create-time LoadGraph sees the baggage on ctx in the same shape every dispatch will: a State built by
	// NewState, which marshals the host's Go value to per-field JSON as a dispatch's read of the column does.
	loaderBaggage, _ := workflow.NewState(opts.Baggage)
	loaderCtx := workflow.ContextWithBaggage(ctx, loaderBaggage)
	var graph *workflow.Graph
	err = errors.CatchPanic(func() error {
		var lerr error
		graph, lerr = e.host.LoadGraph(loaderCtx, workflowURL)
		return lerr
	})
	if err == nil && e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultLoadGraph, workflowURL)) {
		err = errors.New("injected fault: "+engineseams.FaultLoadGraph+" "+workflowURL, http.StatusInternalServerError)
	}
	if err != nil {
		return "", errors.Trace(err)
	}
	// A host that returns (nil, nil) is a not-found, not a nil-deref: guard before EntryPoint/Validate.
	if graph == nil {
		return "", errors.New("workflow graph not found: %s", workflowURL, http.StatusNotFound)
	}
	// Validate at create (documented behavior): reject a structurally invalid graph up front. Validation is
	// pure - the fan-in convergence it checks is computed into a local map and not stored on the graph. The
	// engine derives that map once per graph per replica (internal/faninmap), so only the author's definition is
	// serialized into the flow's graph JSON below.
	if verr := graph.Validate(); verr != nil {
		return "", errors.New("invalid workflow graph %s: %v", workflowURL, verr, http.StatusBadRequest)
	}

	shardNum, err := e.pickShard()
	if err != nil {
		return "", errors.Trace(err)
	}
	// Normalize the caller's initialState (a struct, map, or nil) into a State at the door. A nil yields
	// empty state (NewState's nil case), so a marshaled "null" never reaches the strict object parser.
	state, err := workflow.NewState(initialState)
	if err != nil {
		return "", errors.Trace(err, http.StatusBadRequest)
	}
	// Creation takes a turn like any other database work, and it is the one caller that most needs to: it
	// arrives at a rate the caller chooses rather than one dispatch sets, so left unordered it competes for
	// connections against every step already running and wins a share that grows with the offered rate -
	// admitting flows the replica has no capacity to execute.
	//
	// Its claim is stamped HERE, so its age is the moment this request arrived. That puts a create behind
	// every step whose work at the database began before it and ahead of every one that begins after it,
	// which is what keeps a burst of creation from overtaking the work it just created.
	//
	// The turn wraps createWithGraph rather than living inside it, because that helper is shared with the
	// subgraph-spawn path, which is already holding a turn when it calls in. A turn taken while holding one
	// is the nesting the enclosure rule forbids: with as many turns as connections, holders waiting on a
	// second turn deadlock against each other.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "create")
	defer doneTurn()
	flowKey, err = e.createWithGraph(ctx, shardNum, workflowURL, graph, state, "", opts, 0, 0, 0, 0, nil)
	return flowKey, errors.Trace(err)
}

// flowSeed carries the precomputed, retry-stable values for inserting one new flow plus its entry step.
// It is built outside the transaction (tokens, spans, marshalled JSON) so a lock-contention retry of the
// insert closure reuses the same values rather than rerolling tokens.
type flowSeed struct {
	workflowURL        string
	workflowName       string
	graphJSON          []byte
	graphHash          string
	baggageJSON        []byte
	stateJSON          []byte
	traceParent        string
	flowToken          string
	stepToken          string
	entryPoint         string
	entryURL           string
	timeBudgetMs       int64
	deleteOnCompletion int
	surgraphFlowID     int
	surgraphStepID     int
	callerStepDepth    int
	rootFlowID         int
	priority           int
	fairnessKey        string
	fairnessWeight     float64
	engineID           int64 // creator stamp: the inserting engine's random id (provenance, unindexed)
}

// insertFlowTx inserts the flow row (already `running`) and its entry step (`pending`, immediately
// claimable) - the shared write-half of every creation path, run inside the caller's transaction. Returns the
// new flow and step ids. createWithGraph and continueFlow each wrap it in a bare transaction, so they share
// exactly one copy of the insert SQL.
func insertFlowTx(ctx context.Context, tx *sequel.Tx, s flowSeed) (newFlowID, newStepID int64, newFlowHour int, err error) {
	// root_flow_id is the denormalized tree-membership index: a top-level flow is its own root; a subgraph child
	// inherits the parent's. Written once here, immutable thereafter, so a whole tree is reachable by
	// `WHERE root_flow_id=?` without a recursive surgraph walk. A known root is bound; a new root's is a "self"
	// column, taking the new row's own id (selfFlowInsertSQL), so a retry, with a new flow_id, recomputes it
	// rather than reusing the prior attempt's.
	columns := "flow_token, workflow_url, workflow_name, graph, graph_hash, baggage, trace_parent, status, surgraph_flow_id, surgraph_step_id, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms, engine_id, payload_ver, started_at"
	values := "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC()"
	args := []any{s.flowToken, s.workflowURL, s.workflowName, s.graphJSON, s.graphHash, s.baggageJSON, s.traceParent, workflow.StatusRunning, s.surgraphFlowID, s.surgraphStepID, s.deleteOnCompletion, s.priority, s.fairnessKey, s.fairnessWeight, s.timeBudgetMs, s.engineID, payloadVer}
	var self []string
	if s.rootFlowID == 0 {
		self = append(self, "root_flow_id")
	} else {
		columns, values, args = columns+", root_flow_id", values+", ?", append(args, s.rootFlowID)
	}
	insert, fixup := selfFlowInsertSQL(tx.DriverName(), columns, values, self...)

	// The step and, off PostgreSQL, the fix-up take the flow's id - a chain insertedID makes one statement
	// batch on PostgreSQL.
	batch := newTxBatch(ctx, tx)
	queueInsertFlow(batch, func(id int64, hour int) { newFlowID, newFlowHour = id, hour }, insert, args...)
	flowID, flowArgs, err := batch.insertedID(lastFlowIDSQL, func() any { return newFlowID })
	if err != nil {
		return 0, 0, 0, err
	}
	// The entry step carries its flow's creation hour, which the database chose for the flow row. On PostgreSQL the
	// INSERT's result is not read until the batch is, so the step reads the row; elsewhere the flush above read it.
	flowHour, flowHourArgs := "(SELECT created_hour FROM dwarf_flows WHERE flow_id=(SELECT "+lastFlowIDSQL+"))", []any(nil)
	if tx.DriverName() != "pgx" {
		flowHour, flowHourArgs = "?", []any{newFlowHour}
	}

	// Entry step is pending and immediately claimable (not_before=NOW, lease_expires=NOW). Its depth
	// continues from the caller: callerStepDepth+1 (1 for a top-level flow, where callerStepDepth is 0).
	// A flow that should wait before running uses an entry gate task with flow.Sleep, not a creation-time
	// delay.
	queueInsertID(batch, "step_id", func(id int64) { newStepID = id },
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, time_budget_ms, not_before, lease_expires, priority, fairness_key, fairness_weight, engine_id, flow_created_hour)"+
			" VALUES ("+flowID+", ?, ?, ?, ?, ?, ?, ?, NOW_UTC(), NOW_UTC(), ?, ?, ?, ?, "+flowHour+")",
		append(append(append([]any{}, flowArgs...), s.callerStepDepth+1, s.stepToken, s.entryPoint, s.entryURL, s.stateJSON, workflow.StatusPending, s.timeBudgetMs, s.priority, s.fairnessKey, s.fairnessWeight, s.engineID), flowHourArgs...)...,
	)
	if fixup != "" {
		// Only where the INSERT could not name its own id, which insertedID has therefore already read.
		batch.queue(execOnly, fixup, selfFlowFixupArgs(newFlowID, len(self))...)
	}
	if err := batch.flush(); err != nil {
		return 0, 0, 0, err
	}
	return newFlowID, newStepID, newFlowHour, nil
}

/*
selfFlowInsertSQL returns the INSERT of one dwarf_flows row of the given columns and VALUES expressions whose self
columns - a new tree's root_flow_id - hold the row's own flow_id, and the fix-up UPDATE that writes them, or "" when
the INSERT writes them itself or there are none. Bind the fix-up with selfFlowFixupArgs.

On PostgreSQL the INSERT draws the id from the sequence in a subselect and writes it to flow_id and every self
column, so no UPDATE has to follow. A fix-up there is an UPDATE of the indexed root_flow_id, which can never be
HOT: a dead row version and a new entry in each of the row's indexes, per flow. nextval also sets currval, so
lastFlowIDSQL names the row as it does after a default-filled flow_id. Keep the subselect: in a VALUES list,
nextval(...) and currval(...) side by side have no evaluation order PostgreSQL promises, while a subselect whose
target list is volatile is never pulled up into the outer query, so its nextval runs once.

The other dialects leave the self columns at their default for the fix-up, which updates the row in place (an
undo log, a version store or a B-tree rewrite, not a dead heap tuple). The INSERT stamped updated_at in the same
transaction, so the fix-up writes nothing else.
*/
func selfFlowInsertSQL(driver, columns, values string, self ...string) (insert, fixup string) {
	if driver == "pgx" {
		var selfColumns string
		for _, col := range self {
			selfColumns += col + ", "
		}
		return "INSERT INTO dwarf_flows (flow_id, " + selfColumns + columns + ")" +
			" SELECT n, " + strings.Repeat("n, ", len(self)) + values +
			" FROM (SELECT nextval(pg_get_serial_sequence('dwarf_flows', 'flow_id')) AS n) s", ""
	}
	insert = "INSERT INTO dwarf_flows (" + columns + ") VALUES (" + values + ")"
	if len(self) > 0 {
		fixup = "UPDATE dwarf_flows SET " + strings.Join(self, "=?, ") + "=? WHERE flow_id=?"
	}
	return insert, fixup
}

// selfFlowFixupArgs binds selfFlowInsertSQL's fix-up of selfColumns self columns to the row flowID.
func selfFlowFixupArgs(flowID int64, selfColumns int) []any {
	args := make([]any, selfColumns+1)
	for i := range args {
		args[i] = flowID
	}
	return args
}

// createWithGraph is the shared creation path for Create, Continue, and subgraph children. It inserts the
// flow (already `running`) and its entry step (`pending`, immediately claimable) in one transaction, then
// rings the doorbell - so a flow always creates-and-runs; there is no externally-visible `created` resting
// state. opts.Baggage is the opaque host value marshalled to the baggage column. parentTraceParent controls
// the flow's "workflow" span: empty mints a detached root span (top-level Create/Continue, each its own
// trace); non-empty parents it under that context (a subgraph nests under the caller step's span).
//
// surgraphFlowID/surgraphStepID link a subgraph child to its parent caller step; they are 0 for a
// top-level flow. The linkage is written in the SAME insert transaction, so the child is fully
// parent-linked before it can be dispatched and complete - otherwise its completion could not revive the
// parent. (The parent caller is parked by beforeInsert, in the same transaction, the complementary half of
// that ordering.) callerStepDepth is the caller step's step_depth (0 for a top-level flow): the entry step
// is created at callerStepDepth+1, so a subgraph's depths continue from the caller (informational only).
// beforeInsert, when not nil, runs first in the insert's transaction; an error from it rolls the insert back.
func (e *Engine) createWithGraph(ctx context.Context, shardNum int, workflowURL string, graph *workflow.Graph, initialState workflow.State, parentTraceParent string, opts *workflow.FlowOptions, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID int, beforeInsert func(tx *sequel.Tx) error) (flowKey string, err error) {
	entryPoint := graph.EntryPoint()
	if entryPoint == "" {
		return "", errors.New("workflow has no entry point", http.StatusBadRequest)
	}

	// The span is minted before the insert because the insert writes its context (trace_parent). A spawn that
	// beforeInsert rolls back has therefore already exported a span for a child that never exists - accepted:
	// the alternative is a second write to fill trace_parent in after the commit.
	traceParent := e.mintWorkflowSpan(ctx, workflowURL, parentTraceParent)

	// A non-marshalable host-supplied value (a NaN/Inf/channel in Baggage or a map initialState, which
	// NewState copies through without marshaling) surfaces here as a 400 - it is caller input.
	baggageJSON, err := json.Marshal(opts.Baggage)
	if err != nil {
		return "", errors.New("baggage is not JSON-marshalable: %v", err, http.StatusBadRequest)
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		return "", errors.Trace(err)
	}
	if initialState.IsZero() {
		initialState, _ = workflow.NewState()
	}
	stateJSON, err := json.Marshal(initialState)
	if err != nil {
		return "", errors.New("initial state is not JSON-marshalable: %v", err, http.StatusBadRequest)
	}
	// A subgraph spawn reaches here inside persist, which must not retry a size refusal: hence a permanentError.
	if err := e.checkPayloadSize("initial state", stateJSON); err != nil {
		return "", err
	}
	if err := e.checkPayloadSize("baggage", baggageJSON); err != nil {
		return "", err
	}

	flowToken := keys.RandomIdentifier(16)
	stepToken := keys.RandomIdentifier(16)
	// opts.TimeBudget is resolved (create/continue) or inherited (subgraph). Fall back to the
	// live default only as defense for an unresolved path.
	timeBudget := opts.TimeBudget
	if timeBudget <= 0 {
		timeBudget = e.taskTimeBudget()
	}
	deleteOnCompletion := 0
	if opts.DeleteOnCompletion {
		deleteOnCompletion = 1
	}

	db, err := e.db.Shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}
	entryURL := dispatchURLOf(graph, entryPoint)

	seed := flowSeed{
		workflowURL:        workflowURL,
		workflowName:       graph.Name(),
		graphJSON:          graphJSON,
		graphHash:          graphcache.HashOf(graphJSON),
		baggageJSON:        baggageJSON,
		stateJSON:          stateJSON,
		traceParent:        traceParent,
		flowToken:          flowToken,
		stepToken:          stepToken,
		entryPoint:         entryPoint,
		entryURL:           entryURL,
		timeBudgetMs:       timeBudget.Milliseconds(),
		deleteOnCompletion: deleteOnCompletion,
		surgraphFlowID:     surgraphFlowID,
		surgraphStepID:     surgraphStepID,
		callerStepDepth:    callerStepDepth,
		rootFlowID:         rootFlowID,
		priority:           opts.Priority,
		fairnessKey:        opts.FairnessKey,
		fairnessWeight:     opts.FairnessWeight,
		engineID:           e.engineID,
	}

	var newFlowID, newStepID int64
	var newFlowHour int
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if beforeInsert != nil {
			if err := beforeInsert(tx); err != nil {
				return err
			}
			e.seams.Checkpoint(ctx, engineseams.CheckpointSubgraphSpawnParked)
		}
		var err error
		newFlowID, newStepID, newFlowHour, err = insertFlowTx(ctx, tx, seed)
		return err
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	flowKey = keys.NewFlowKey(shardNum, newFlowHour, int(newFlowID), flowToken)
	// Seed the graph cache when this replica dispatches the shard: the doorbell below offers the entry step to
	// its own candidate cache, so it is about to claim the step and would otherwise read back the graph it just
	// wrote. After the commit, so a rolled-back flow id - which SQLite can hand out again - is never cached.
	if e.dispatchesOn(shardNum) {
		if cg, perr := e.internGraph(graphJSON, workflowURL, seed.graphHash); perr == nil {
			e.graphs.Point(graphcache.Key{Shard: shardNum, FlowID: int(newFlowID), Token: flowToken}, cg)
		}
	}
	e.logger.DebugContext(ctx, "Flow created and started", "workflow", workflowURL, "task", entryPoint)
	e.metricFlowStarted(ctx, workflowURL, opts.Priority, shardNum)
	// The entry step's initial-state snapshot, written by insertFlowTx above.
	e.metricStateWriteBytes(ctx, workflowURL, "state", len(seed.stateJSON))
	// Ring the doorbell so a replica with spare capacity claims the entry step immediately, rather than
	// waiting out a piston cycle. A missed doorbell costs only that: the step is `pending` and due, so the
	// shard's next cycle selects it like any other. (Lease recovery is NOT the backstop here - it only
	// resets `running` rows.) The entry step was just inserted due-now with the resolved priority, so the
	// fast-path doorbell applies.
	e.enqueueStepDue(ctx, shardNum, int(newStepID), seed.priority)
	return flowKey, nil
}

// splitTerminateCancelReason maps the shared cancel_reason column onto whichever reason field the given
// terminal status has declared it feeds - Terminate's if "terminated", Cancel's if "cancelled" - so
// snapshot's FlowOutcome and list's FlowSummary, which both split this one column, agree on the mapping.
func splitTerminateCancelReason(status, reason string) (terminateReason, cancelReason string) {
	switch status {
	case workflow.StatusTerminated:
		return reason, ""
	case workflow.StatusCancelled:
		return "", reason
	}
	return "", ""
}

// snapshot returns the current outcome of a flow.
func (e *Engine) snapshot(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "snapshot")
	defer doneTurn()

	var flowStatus string
	var finalStateJSON []byte
	var flowErrorMsg string
	// cancel_reason is the one column both Terminate and Cancel write their reason into. Which FlowOutcome
	// field it surfaces as is decided below, by status.
	var flowCancelReason string
	var flowPayloadVer int
	var archived bool
	found, err := e.queryLiveOrArchive(ctx, db, shardNum, flowID, flowHour, true, func(t flowTables) (bool, error) {
		err := db.QueryRowContext(ctx,
			"SELECT status, final_state, error, cancel_reason, payload_ver FROM "+t.flows+" WHERE flow_id=? AND flow_token=? AND created_hour=?",
			flowID, flowToken, flowHour,
		).Scan(&flowStatus, &finalStateJSON, &flowErrorMsg, &flowCancelReason, &flowPayloadVer)
		if err == sql.ErrNoRows {
			return false, nil
		}
		archived = t != liveTables
		return err == nil, errors.Trace(err)
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	// Only closed trees are archived. An archived flow that has not closed is one only a bug leaves, and reported as it
	// stands it would hold an Await until its deadline.
	if archived && !workflow.IsTerminalStatus(flowStatus) {
		return nil, errors.New("archived flow is %s", flowStatus, http.StatusInternalServerError)
	}
	flowErrorMsg = strings.TrimSpace(flowErrorMsg)
	flowCancelReason = strings.TrimSpace(flowCancelReason)

	out := &workflow.FlowOutcome{
		Status: flowStatus,
	}

	// A column that does not decode is an error, never an empty State: the caller would read it as the flow's
	// whole outcome.
	var decodeErr error
	switch flowStatus {
	case workflow.StatusCompleted:
		out.State, decodeErr = workflow.NewState(finalStateJSON)
	case workflow.StatusFailed:
		out.State, decodeErr = workflow.NewState(finalStateJSON)
		out.Error = flowErrorMsg
	case workflow.StatusTerminated, workflow.StatusCancelled:
		out.State, decodeErr = workflow.NewState(finalStateJSON)
		out.TerminateReason, out.CancelReason = splitTerminateCancelReason(flowStatus, flowCancelReason)
	case workflow.StatusInterrupted:
		// For interrupted, query the leaf step's state and interrupt payload, from the live tables: an interrupted
		// flow is never archived. One terminated between these reads stays live for archiveGrace after it ends,
		// and its leaf no longer interrupted reads as an empty state.
		var stepStateJSON, stepChangesJSON, stepRefsJSON, interruptPayloadJSON []byte
		// Pick the same interrupted leaf Resume's chain walk would act on: the step the flow row recorded as the one
		// it stopped at (the interrupt whose flow-row write committed first, which StoppedTaskName also reports),
		// then the earliest-updated - not by step_depth, which is only an informational ordering and varies with
		// branch length (loops/gotos) without indicating which interrupt resolves next.
		err = db.QueryRowContext(ctx,
			"SELECT s.state, s.changes, s.state_refs, s.interrupt_payload FROM dwarf_steps s"+
				" JOIN dwarf_flows f ON f.flow_id=s.flow_id"+
				" WHERE s.flow_id=? AND s.status='"+workflow.StatusInterrupted+"'"+
				" ORDER BY CASE WHEN s.step_id=f.stopped_step_id THEN 0 ELSE 1 END, s.updated_at, s.step_id LIMIT_OFFSET(1, 0)",
			flowID,
		).Scan(&stepStateJSON, &stepChangesJSON, &stepRefsJSON, &interruptPayloadJSON)
		// A missing interrupted step (ErrNoRows) is a tolerated race - report empty state; a real DB
		// error must not masquerade as that empty state.
		if err != nil && err != sql.ErrNoRows {
			return nil, errors.Trace(err)
		}
		if err == nil {
			// A closed flow's final_state is plain JSON, readable in any format; an interrupted one's state is
			// read from its step rows.
			if err := checkPayloadVer(flowPayloadVer); err != nil {
				return nil, errors.Trace(err)
			}
			stepState, err := workflow.NewState(stepStateJSON)
			if err != nil {
				return nil, errors.Trace(err)
			}
			// The ref encoding is a storage detail, never API-visible: a caller sees the state the step
			// actually saw. resolveStateRefs mutates the map in place, so it gets the live map.
			stepRefs, rerr := staterefs.Parse(stepRefsJSON)
			if rerr == nil {
				_, rerr = e.resolveStateRefs(ctx, db, shardNum, flowToken, stepState, stepRefs, nil, "")
			}
			if rerr != nil {
				return nil, errors.Trace(rerr)
			}
			// The interrupted step's view: its changes so far folded onto its state, through the graph's reducers.
			graph, _, gerr := e.flowGraph(ctx, db, shardNum, flowID, flowToken)
			if gerr != nil {
				return nil, errors.Trace(gerr)
			}
			stepChanges, cerr := changesOf(stepChangesJSON)
			if cerr != nil {
				return nil, errors.Trace(cerr)
			}
			if err := stepState.Apply(stepChanges, graph.Reducers()); err != nil {
				return nil, errors.Trace(err)
			}
			out.State = stepState
			out.InterruptPayload, decodeErr = workflow.NewState(interruptPayloadJSON)
		}
	case workflow.StatusRunning, workflow.StatusCreated:
		out.State = workflow.State{}
	}
	if decodeErr != nil {
		return nil, errors.Trace(decodeErr)
	}

	return out, nil
}

// await blocks until a flow stops. A DeleteOnCompletion flow stays `completed` (outcome observable) for the
// deletion grace window, then the reaper removes it and await 404s.
func (e *Engine) await(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	// A caller that named no deadline is given one. Nothing else bounds the park: a waiter is released only
	// by a status the board reports, so a flow that never stops would block its caller for the life of the
	// process, and the engine keeps no timer of its own behind the board - a periodic re-read would be a
	// query rate that grows with blocked callers while buying nothing the sweep does not already cover.
	//
	// The budget is deliberately far longer than any wait a synchronous caller should be making. It is a
	// stop against blocking forever, not an opinion on how long awaiting is reasonable, and it applies ONLY
	// where the caller expressed nothing - an explicit deadline is honored exactly as given, however long.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, e.awaitDefaultBudget)
		defer cancel()
	}
	// READ, then PARK and READ until a read sees a stop. The reads answer different questions:
	//
	// The FIRST read is what makes an already-settled flow answer AT ONCE. Parking instead would be
	// correct - the detector reports the current state of every parked key, so a stop that happened before
	// this caller arrived is still found - but it would be found on the sweep's cadence, turning every
	// `Await` on a finished flow into a wait of up to one full sweep. Measured: dropping this read doubled
	// the engine package's test time. It costs one indexed lookup, and only on calls that are about to
	// block anyway.
	//
	// Each LATER read builds the outcome once the board says the flow STOPPED. A release always means the row
	// reached a stop (or stopped resolving), never merely "look again" - do not release on a non-settled
	// status, which `signalStop` guards. But an interrupted flow can be Resumed between the release and the
	// read, so the read can find it running again; then the caller parks again rather than reporting a
	// timeout on a live ctx. That is a re-park per release, not a poll. A sweep that read the row before a
	// Resume can release the re-parked caller once more for the same stop - one more read, bounded.
	//
	// Registering AFTER the first read is safe for the same reason parking at all is: the board is POLLED,
	// so a stop landing in the gap is reported by the next sweep rather than being an event the caller
	// arrived too late to hear. A signal-driven board would have to arm first and read second.
	outcome, err := e.snapshot(ctx, flowKey)
	if err != nil {
		// A read that failed because the CALLER's ctx ended is the deadline this call is allowed to hit,
		// not an infrastructure failure - so it degrades to the same not-stopped answer the park below
		// gives, and `Poll` keeps its contract of returning a re-pollable outcome rather than a timeout
		// error. The window is small and entirely latency-dependent (the read has to straddle the
		// deadline), which is why it shows up against a real server and not against local SQLite.
		if ctx.Err() != nil {
			return &workflow.FlowOutcome{Status: workflow.StatusRunning}, nil
		}
		return nil, errors.Trace(err)
	}
	if isStoppedStatus(outcome.Status) {
		return outcome, nil
	}

	for {
		// Park for the WHOLE remaining budget, in one call. Waiting costs no query of its own - the detector
		// already asks about every parked key on its own cadence - so waking a caller early to re-read a row
		// the sweep is polling anyway would add a per-caller query rate for nothing.
		_, waitErr := e.latches.Latch(ctx, flowKey)
		switch {
		case errors.Is(waitErr, latch.ErrClosed):
			return nil, errors.New("engine is shutting down", http.StatusServiceUnavailable)
		case waitErr != nil:
			// The wait ran out before the flow settled. Hand back the non-terminal outcome already in hand
			// rather than reading again on a spent ctx: `Await` turns a not-stopped result into a timeout
			// error, while `Poll` returns it as-is so the caller can re-poll.
			return outcome, nil
		}
		// Test rendezvous: released, about to read. Holding a caller here is how a test makes the read straddle
		// its deadline.
		if e.seams.Enabled() { // Enabled gates the assembled name in production
			e.seams.Checkpoint(ctx, engineseams.Join(engineseams.CheckpointAwaitReleased, flowKey))
		}
		next, err := e.snapshot(ctx, flowKey)
		if err != nil {
			// The caller's deadline, as at the first read. The read takes a turn, so a release landing just
			// before the deadline on a saturated shard can straddle it; hand back the outcome in hand, as an
			// expired wait does.
			if ctx.Err() != nil {
				return outcome, nil
			}
			return nil, errors.Trace(err)
		}
		outcome = next
		if isStoppedStatus(outcome.Status) || ctx.Err() != nil {
			return outcome, nil
		}
	}
}

// signalStop wakes local Await callers waiting on the given flow. Use it at every flow-stop site
// (completed, failed, terminated, cancelled, interrupted) and NOWHERE ELSE: a woken caller pays a read, and
// re-parks if the flow is no longer stopped, so a wake for a status the flow is merely passing through buys
// nothing. A non-terminal transition needs no wake at all - nobody is parked on it.
//
// It reaches THIS replica only, and nothing here needs to reach further: a peer's awaiters find the stop
// by reading the flow row on the latch detector's own cadence, so the wake is an accelerator for the
// local case rather than the mechanism awaiting rests on.
func (e *Engine) signalStop(ctx context.Context, flowKey string, status string) {
	// Test rendezvous: a flow just reached a committed stop (this runs post-commit). Fired both unscoped and
	// scoped to this flow+status, so a test can wait for "any stop" or for one specific flow to reach one
	// specific status. Placed before the drop-fault below so both fire even when the wake itself is dropped - a
	// test waiting on "the flow stopped" should observe the DB-committed stop regardless of wake delivery.
	// Inert in production: the Enabled gate short-circuits before the scoped name is built.
	if e.seams.Enabled() { // Enabled gates the assembled name in production
		e.seams.Checkpoint(ctx, engineseams.CheckpointFlowStopped)
		e.seams.Checkpoint(ctx, engineseams.Join(engineseams.CheckpointFlowStopped, flowKey, status))
	}
	// engineseams.FaultDropSignalStop simulates a lost terminal wake - a worker crash between the commit and the
	// signal - so a test can prove Await still returns without one, via the latch detector reading the
	// committed row.
	if e.seams.IsFault(engineseams.FaultDropSignalStop) {
		return
	}
	// Guarded, not merely documented: a release is a promise that the flow reached a stop. A non-stopped status
	// here would wake a caller to a read that finds nothing to return, so it is dropped rather than delivered -
	// the flow's real stop will wake them later.
	if !isStoppedStatus(status) {
		return
	}
	if e.latches == nil {
		return // not started: nobody can be awaiting
	}
	e.latches.Release(flowKey, status)
}

// The work doorbell is PURELY LOCAL - it reaches this replica's candidate cache and nothing else.
//
// Do not broadcast it to peers (one message per step per peer). Under load every peer's piston is already
// cycling at its derived interval, so a broadcast buys no dispatch latency the next cycle would not cover,
// while costing R-1 messages per step AND a PK lookup on every receiver (the inbound path must resolve the
// announced step's priority and due-ness against its own clock, which is exactly the round-trip
// enqueueStepDue exists to avoid locally). It would also offer the step UNPARTITIONED on the receiver, so a
// peer could race the residue class's owner to the claim CAS. Every piston cycles unconditionally instead: a
// peer discovers work by scanning, on a bounded cadence, with no message at all.
//
// The doorbell rings no refiller either - there is no trigger to ring, because a piston's cycle is not
// on-demand. What it does is Offer: an empty partition ADMITS the step, so a sequential chain's next hop
// dispatches immediately instead of waiting out a cycle.
//
// Consequence to keep in mind when reading the origination sites: a step created here is offered to THIS
// replica's cache only. If this replica cannot serve it (the step falls in a peer's residue class), the
// step waits for that peer's next cycle - bounded by the interval, not by a message. Do not reintroduce a
// per-step peer broadcast to shave that.

// enqueueStep rings the local work doorbell for a step whose priority the caller does NOT hold, resolving
// it (and the step's due-ness) with one PK lookup. Use it at the cold step-origination sites - surgraph
// revive, resume, fork's leaf, the wedge sweep - where that value is not in hand; the hot paths use
// enqueueStepDue instead and skip the lookup.
func (e *Engine) enqueueStep(ctx context.Context, shard, stepID int) {
	// engineseams.FaultDropDoorbell simulates a lost work doorbell so a test can prove the pending step is still picked
	// up by a later refiller scan rather than stranding.
	if e.seams.IsFault(engineseams.FaultDropDoorbell) {
		return
	}
	// Every caller of this cold path is RE-offering a step this replica already dispatched once - a revived
	// surgraph caller, a resumed interrupt leaf, an unwedged park - so the step still carries the claim
	// reservation its earlier dispatch took, and those dispatches finish far inside the ~1-2s window. Left
	// in place it makes this replica skip its own re-dispatch: every worker that pops the step is turned
	// away by TryClaim, and the refiller keeps re-selecting it, until the reservation ages out. Same reason
	// and same treatment as the recovery-defer reset and the flow.Retry rewind (dispatch.go), which is the
	// company this site belongs in. A no-op for the callers that offer a genuinely new step id (Fork's leaf,
	// Continue), which have no reservation to drop.
	e.claims.RelinquishClaim(shard, stepID)
	if !e.dispatchesOn(shard) {
		return // see enqueueStepDue; after the relinquish, which is this replica's own bookkeeping either way
	}
	priority := math.MaxInt
	var notBeforeDelayMs sql.NullFloat64
	db, err := e.db.Shard(shard)
	if err == nil {
		db.QueryRowContext(ctx,
			"SELECT priority, DATE_DIFF_MILLIS(not_before, NOW_UTC()) FROM dwarf_steps WHERE step_id=?",
			stepID,
		).Scan(&priority, &notBeforeDelayMs)
	}
	if notBeforeDelayMs.Valid && notBeforeDelayMs.Float64 > 0 {
		// Not due yet: nothing to preempt, so leave the cache untouched. Nothing is scheduled either - the
		// step's own not_before makes it invisible to selection until it comes due, and visible to the
		// next cycle after that.
		e.logger.DebugContext(ctx, "Doorbell deferred", "stepID", stepID, "delayMs", notBeforeDelayMs.Float64)
		return
	}
	admitted := e.cache.Offer(candidates.Job{StepID: stepID, Shard: shard}, priority)
	if admitted {
		e.metricStepOffered(ctx)
	}
	e.logger.DebugContext(ctx, "Doorbell", "stepID", stepID, "priority", priority, "admitted", admitted)
}

// enqueueStepDue is the fast-path doorbell for a step the caller just created or reset for immediate
// dispatch and whose priority it already holds: it offers the step to the local cache directly, skipping
// enqueueStep's PK lookup - one round-trip per completed step on the hot path, re-reading a row this
// replica just wrote. Use it only where due-ness is certain (the caller's own sleep/not_before branch
// already diverged) and the priority is the value just bound into the INSERT/reset.
func (e *Engine) enqueueStepDue(ctx context.Context, shard, stepID, priority int) {
	// engineseams.FaultDropDoorbell: see enqueueStep.
	if e.seams.IsFault(engineseams.FaultDropDoorbell) {
		return
	}
	// ONLY A DISPATCHER MAY RING ITS OWN BELL. The cache admits a step for any shard, and whoever pops it
	// executes it and offers every successor back to the same cache - so a replica that rang the bell for a
	// shard it does not dispatch would run the whole flow through its reader pool. The step is already
	// committed and due, so a dispatcher's next scan takes it; the chain then stays on that dispatcher.
	if !e.dispatchesOn(shard) {
		return
	}
	admitted := e.cache.Offer(candidates.Job{StepID: stepID, Shard: shard}, priority)
	if admitted {
		e.metricStepOffered(ctx)
	}
	e.logger.DebugContext(ctx, "Doorbell (due)", "stepID", stepID, "priority", priority, "admitted", admitted)
}

// maxReasonBytes bounds a Terminate or Cancel reason. A reason is copied into every step a Cancel covers, as that
// step's error, so it is held to the size of a sentence a person typed rather than a payload.
const maxReasonBytes = 4096

// checkReason refuses a Terminate or Cancel reason the reason and error columns cannot hold: one over
// maxReasonBytes, or one that is not valid UTF-8 or holds a NUL, which PostgreSQL rejects in a text column and
// MySQL rejects under its default strict mode.
func checkReason(reason string) error {
	if len(reason) > maxReasonBytes {
		return errors.New("reason is %d bytes, over the %d-byte limit", len(reason), maxReasonBytes, http.StatusBadRequest)
	}
	if !utf8.ValidString(reason) || strings.ContainsRune(reason, 0) {
		return errors.New("reason must be valid UTF-8 without NUL characters", http.StatusBadRequest)
	}
	return nil
}

// terminate forcefully stops a flow and its whole subgraph subtree, unconditionally. Root-only: a subgraph
// child is not an independently terminable unit (its parent is parked on it, and the unit of any lifecycle
// change is the tree), so a child key is rejected rather than silently widened into a tree-wide terminate.
func (e *Engine) terminate(ctx context.Context, flowKey string, reason string) error {
	if err := checkReason(reason); err != nil {
		return errors.Trace(err)
	}
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "terminate")
	defer doneTurn()

	var flowStatus string
	var surgraphFlowID int
	err = db.QueryRowContext(ctx,
		"SELECT status, surgraph_flow_id FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND created_hour=?",
		flowID, flowToken, flowHour,
	).Scan(&flowStatus, &surgraphFlowID)
	if err == sql.ErrNoRows {
		// An archived flow is closed, so it answers below as any closed flow does: a no-op.
		_, flowStatus, surgraphFlowID, err = e.archivedFlowRow(ctx, db, shardNum, flowID, flowToken, flowHour)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if surgraphFlowID != 0 {
		return errors.New("cannot terminate a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	if workflow.IsTerminalStatus(flowStatus) {
		return nil // already closed: terminating a closed flow is a no-op, as for Cancel
	}

	// The root has no ancestors by construction, so this walks DOWN only - see terminateSubtree, which the orphan
	// sweep shares.
	_, err = e.terminateSubtree(ctx, shardNum, flowID, flowHour, flowToken, reason, engineseams.FaultTerminateCommit)
	return errors.Trace(err)
}

// cancel marks every pending, running or interrupted step in the tree with cancelling=1. Each marked step
// consumes its own mark at its next checkpoint - the claim (preempted without running) or the completion
// write after a dispatch that was already under way - so Cancel itself touches nothing but these two writes.
//
// Parked subgraph callers are marked like any other running step. A caller's cancellation is its own, not
// its child's to decide: when the child returns, the caller runs once more to see the child's result, and is
// then redirected by the mark. Excluding parked callers would lose the cancellation outright whenever it lands
// between the child's completion and the caller's revive - the caller is skipped and the child is already
// terminal, so nothing is marked at all.
//
// The first write is to the flow rows - the reason, and cancel_watermark - and must come BEFORE the mark.
// A step claimed between the two reads the reason off its flow row, and the opposite order hands it an empty
// one. cancel_watermark (the flow's highest step_id at the Cancel) closes the one gap the mark cannot reach: a
// step whose completion write has landed but whose successors are not yet inserted is terminal and so unmarked,
// and its successors do not exist yet. The transition that inserts them holds the flow row's lock, so this write
// either lands before it - and the successors inherit a mark, their source being at or below the watermark - or
// waits for it to commit, after which the mark below finds them pending. Every step inserted after the Cancel
// sits above the watermark, which is what keeps a handler that caught the cancellation from passing it on.
//
// ONE CANCEL PER FLOW: a nonzero cancel_watermark means the flow was already cancelled, and a repeat call
// writes no reason and no watermark. Not cancel_reason, which may be empty and is shared with Terminate. A
// repeat does re-apply the mark, which is what makes a Cancel that failed after its watermark committed safe to
// retry: otherwise the steps it left unmarked run on, and a flow whose last step was among them completes
// uncancelled while the retry reports success. Re-applying it reaches only steps at or below each flow's
// watermark, so it cannot touch the onError handler that caught the first cancellation, or anything else
// inserted after it, and a flow that recovered is left alone; a flow that recovered and must still stop is
// Terminated. Raising the watermark again would reach that handler, which is why a repeat never does. The
// watermark write is itself guarded on cancel_watermark=0, so two concurrent calls that both read zero raise
// it once. A no-op on a closed flow, rather than a 409: the flow write is guarded to open flows, so
// it can never rewrite a recorded outcome.
func (e *Engine) cancel(ctx context.Context, flowKey string, reason string) error {
	if err := checkReason(reason); err != nil {
		return errors.Trace(err)
	}
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	ctx, doneTurn := e.opTurn(ctx, shardNum, "cancel")
	defer doneTurn()

	var surgraphFlowID, watermark int
	var status string
	err = db.QueryRowContext(ctx,
		"SELECT surgraph_flow_id, cancel_watermark, status FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND created_hour=?",
		flowID, flowToken, flowHour,
	).Scan(&surgraphFlowID, &watermark, &status)
	if err == sql.ErrNoRows {
		// An archived flow is closed, so it answers below as any closed flow does: a no-op.
		_, status, surgraphFlowID, err = e.archivedFlowRow(ctx, db, shardNum, flowID, flowToken, flowHour)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if surgraphFlowID != 0 {
		return errors.New("cannot cancel a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	if workflow.IsTerminalStatus(status) {
		return nil // already closed - see above. The writes below re-guard it.
	}

	// Same tree-membership shape Terminate's descendant walk uses (root_flow_id scan, non-terminal nodes
	// only) - a terminal descendant's steps are already all-terminal (step immutability), so it would
	// contribute nothing to the mark below even if included; excluding it here is just one fewer flow_id
	// in the IN-list.
	descendantFlowIDs, _, _, err := e.allSubgraphFlows(ctx, db, shardNum, flowID)
	if err != nil {
		return errors.Trace(err)
	}
	// Ascending, so the chunked lock passes below take their rows in the order Terminate's do (lockOrder).
	allFlowIDs := lockOrder(append([]any{flowID}, descendantFlowIDs...))
	if e.seams.Enabled() {
		e.seams.Variable(engineseams.Join(engineseams.VariableLockOrder, "cancel"), slices.Clone(allFlowIDs))
	}

	reason = strings.TrimSpace(reason)
	// The watermark is read by a statement AFTER the one that takes the flow rows' locks. On Postgres an UPDATE's
	// subquery reads the snapshot taken before the statement waited for its row lock, so a single statement would
	// miss a successor that a transition committed while holding that lock - and that successor could then run to
	// completion before the mark below, unmarked, and pass nothing on. Every lock is taken before any watermark is
	// read. The flow-id lists are chunked: a tree can hold more flows than a statement can bind. A repeat
	// Cancel skips it and only re-applies the mark (see "one cancel per flow" above).
	//
	// The watermarks are read by a plain SELECT and bound into the UPDATE, not computed by a subquery inside it:
	// on MySQL a subquery inside DML takes shared locks on the steps it reads, after this transaction's flow-row
	// locks - the reverse of Terminate's steps-then-flows order, a deadlock (measured against a held Terminate).
	// A plain SELECT is a consistent read there and takes none. It is also this transaction's first
	// consistent read, after the locking reads, so its snapshot postdates every lock and sees every successor
	// a transition committed before the lock was granted.
	if watermark == 0 {
		err = db.Transact(ctx, func(tx *sequel.Tx) error {
			for chunk := range slices.Chunk(allFlowIDs, inListChunk) {
				ph, ids := inList(chunk)
				lockFlowRows(ctx, tx, "flow_id IN ("+ph+") AND status NOT IN ("+terminalStatusesSQL+")", ids...)
			}
			for chunk := range slices.Chunk(allFlowIDs, inListChunk) {
				ph, ids := inList(chunk)
				maxStep := map[int]int{}
				rows, err := tx.QueryContext(ctx,
					"SELECT flow_id, MAX(step_id) FROM dwarf_steps WHERE flow_id IN ("+ph+") GROUP BY flow_id", ids...)
				if err != nil {
					return errors.Trace(err)
				}
				for rows.Next() {
					var fid, sid int
					if err := rows.Scan(&fid, &sid); err != nil {
						rows.Close()
						return errors.Trace(err)
					}
					maxStep[fid] = sid
				}
				rows.Close()
				if err := rows.Err(); err != nil {
					return errors.Trace(err)
				}
				// One CASE arm per flow of the chunk (2 binds each, under SQL Server's ceiling at inListChunk).
				// Every flow has an entry step, so each watermark is at least 1 and the guard below marks it once.
				args := []any{reason}
				for _, id := range chunk {
					args = append(args, id, maxStep[id.(int)])
				}
				args = append(args, ids...)
				tx.ExecContext(ctx,
					"UPDATE dwarf_flows SET cancel_reason=?,"+
						" cancel_watermark=CASE flow_id"+strings.Repeat(" WHEN ? THEN ?", len(chunk))+" ELSE cancel_watermark END,"+
						" touch=1-touch WHERE flow_id IN ("+ph+") AND status NOT IN ("+terminalStatusesSQL+")"+
						" AND cancel_watermark=0",
					args...,
				)
			}
			return nil
		})
		if err != nil {
			return errors.Trace(err)
		}
	}
	e.seams.Checkpoint(ctx, engineseams.CheckpointCancelBeforeMark)
	// The mark reaches only steps at or below their flow's watermark. A step above it was inserted after the
	// watermark committed, and its insert already decided its mark - inherited if its source was reached by
	// this Cancel, not if its source was covered. Unbounded, the mark would catch the onError handler of a
	// step preempted in the window between that commit and this statement, and deliver the cancellation a
	// second time to the handler written to catch it. A chunk holds whole flows, so each flow is marked by one
	// statement.
	for chunk := range slices.Chunk(allFlowIDs, inListChunk) {
		if e.seams.IsFault(engineseams.FaultCancelMarkErr) {
			return errors.New("injected fault: " + engineseams.FaultCancelMarkErr)
		}
		ph, ids := inList(chunk)
		_, err = db.ExecContext(ctx,
			"UPDATE dwarf_steps SET cancelling=1 WHERE flow_id IN ("+ph+")"+
				" AND status IN ('"+workflow.StatusPending+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')"+
				" AND step_id <= (SELECT f.cancel_watermark FROM dwarf_flows f WHERE f.flow_id=dwarf_steps.flow_id)",
			ids...,
		)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// deleteFlow removes a closed flow and its subgraph subtree. A live one is scheduled for deletion by the reaper - its
// rows are not deleted inline: it stamps delete_after_ms=1 (due immediately) on the root, and the reaper removes the
// whole tree on its next pass. An archived one is deleted at once in its day (deleteArchivedFlow). A flow that has not closed is a 409: the caller terminates it first, so a live delete_after_ms never
// coexists with a resumable flow, and nothing a Delete does can race a Resume.
func (e *Engine) deleteFlow(ctx context.Context, flowKey string) error {
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "delete")
	defer doneTurn()

	var flowStatus string
	var surgraphFlowID, deleteAfterMs int
	err = db.QueryRowContext(ctx,
		"SELECT status, surgraph_flow_id, delete_after_ms FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND created_hour=?",
		flowID, flowToken, flowHour,
	).Scan(&flowStatus, &surgraphFlowID, &deleteAfterMs)
	if err == sql.ErrNoRows {
		return e.deleteArchivedFlow(ctx, db, shardNum, flowID, flowToken, flowHour)
	}
	if err != nil {
		return errors.Trace(err)
	}
	// A subgraph child cannot be deleted directly (its parent's surgraph step would dangle). Address the
	// tree by the root key; the reaper removes descendants via root_flow_id. A child key is read-only.
	if surgraphFlowID != 0 {
		return errors.New("cannot delete a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	// The 409 guards only the ROOT's status; a running subgraph descendant of a closed root does not block the
	// delete. The reaper later removes the whole root_flow_id tree regardless of descendant status - safe because
	// the only running descendant a terminal-rooted tree can hold is a live orphan (which only a bug leaves) the
	// wedge sweep would terminate anyway, and a worker mid-dispatch on it no-ops via the lease fence (see
	// reapDueFlows).
	if !workflow.IsTerminalStatus(flowStatus) {
		return errors.New("flow is open (status: %s); terminate it first", flowStatus, http.StatusConflict)
	}
	if deleteAfterMs > 0 {
		return nil // already scheduled - idempotent
	}
	// Test checkpoint: the reaper can archive the tree between the read and the stamp.
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeDeleteStamp)
	res, err := db.ExecContext(ctx,
		"UPDATE dwarf_flows SET delete_after_ms=1 WHERE flow_id=? AND flow_token=? AND created_hour=? AND status IN ("+terminalStatusesSQL+") AND delete_after_ms=0",
		flowID, flowToken, flowHour,
	)
	if err != nil {
		return errors.Trace(err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		e.metricFlowsRemoved(ctx, shardNum, removedFromLive, "delete", 1)
		return nil
	}
	// The stamp matched nothing, so the row moved since the read: the reaper archived the tree (deleted from the
	// archive instead), or a racing Delete stamped it first (success).
	err = db.QueryRowContext(ctx,
		"SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND created_hour=?",
		flowID, flowToken, flowHour,
	).Scan(&deleteAfterMs)
	if err == sql.ErrNoRows {
		return e.deleteArchivedFlow(ctx, db, shardNum, flowID, flowToken, flowHour)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if deleteAfterMs > 0 {
		return nil
	}
	return errors.New("the flow changed while it was being deleted; try again", http.StatusConflict)
}

// archivedFlowRow reads an archived flow's status and surgraph link from the day the archive's locate finds it in,
// with its token, for an operation whose live read missed: the day, or a 404 when neither side holds the flow - never
// archived, deleted, or on a day dropped since.
func (e *Engine) archivedFlowRow(ctx context.Context, db *sequel.DB, shardNum, flowID int, flowToken string, flowHour int) (
	day int, status string, surgraphFlowID int, err error) {
	notFound := errors.New("flow not found", http.StatusNotFound)
	set := e.archives.Load()
	if set == nil || set.byShard[shardNum] == nil {
		return 0, "", 0, notFound
	}
	day, err = e.locateRetained(ctx, set, shardNum, flowID, flowHour)
	if errors.StatusCode(err) == http.StatusNotFound {
		return 0, "", 0, notFound
	}
	if err != nil {
		return 0, "", 0, errors.Trace(err)
	}
	err = db.QueryRowContext(ctx, "SELECT status, surgraph_flow_id FROM "+migrations.FlowsTable(day)+
		" WHERE flow_id=? AND flow_token=? AND created_hour=?", flowID, flowToken, flowHour).Scan(&status, &surgraphFlowID)
	// A day dropped since the locate, or a directory row outliving its day, is a missing table: the flow is gone.
	if err == sql.ErrNoRows || sequel.IsUndefinedTableError(err) {
		return 0, "", 0, notFound
	}
	if err != nil {
		return 0, "", 0, errors.Trace(err)
	}
	return day, status, surgraphFlowID, nil
}

// deleteArchivedFlow deletes an archived flow's tree at once in its day's tables (deleteArchivedTrees): it is not
// marked, since the reaper reads only the live tables and an archived tree has nothing left to race. A subgraph
// child's key is a 400, as on the live side.
func (e *Engine) deleteArchivedFlow(ctx context.Context, db *sequel.DB, shardNum, flowID int, flowToken string, flowHour int) error {
	day, _, surgraphFlowID, err := e.archivedFlowRow(ctx, db, shardNum, flowID, flowToken, flowHour)
	if err != nil {
		return errors.Trace(err)
	}
	if surgraphFlowID != 0 {
		return errors.New("cannot delete a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	n, err := deleteArchivedTrees(ctx, db, day, []int{flowID})
	if sequel.IsUndefinedTableError(err) || (err == nil && n == 0) {
		return errors.New("flow not found", http.StatusNotFound) // dropped, or deleted by a racing Delete or Purge
	}
	if err != nil {
		return errors.Trace(err)
	}
	e.metricFlowsRemoved(ctx, shardNum, removedFromArchive, "delete", n)
	return nil
}

// run creates, starts, and awaits a flow in one call.
func (e *Engine) run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, outcome *workflow.FlowOutcome, err error) {
	flowKey, err = e.create(ctx, workflowURL, initialState, opts) // auto-starts
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	// A wait that runs out does NOT tear the flow down: it is durable and already running on the engine's own
	// worker lifetime, independent of this call. Terminating here would destroy healthy, in-progress work (a
	// durable retry-until-success job especially) just because the caller stopped waiting. The flowKey comes
	// back with the timeout error, so the caller keeps a handle to re-Await/Snapshot/Terminate on its own terms.
	outcome, err = e.awaitStopped(ctx, flowKey)
	if err != nil {
		return flowKey, nil, errors.Trace(err)
	}
	return flowKey, outcome, nil
}

// awaitStopped is await for a caller that wants a stopped flow or an error: a wait that runs out - the
// caller's ctx, or await's default budget for a ctx with no deadline, which leaves ctx.Err() nil - is a 408 that
// satisfies errors.Is(err, context.DeadlineExceeded).
func (e *Engine) awaitStopped(ctx context.Context, flowKey string) (*workflow.FlowOutcome, error) {
	outcome, err := e.await(ctx, flowKey)
	if err != nil {
		return nil, err
	}
	if !outcome.Stopped() {
		return nil, errors.New("timed out awaiting flow", cmp.Or(ctx.Err(), context.DeadlineExceeded), http.StatusRequestTimeout)
	}
	return outcome, nil
}

// errResumeLost is an in-transaction sentinel: the root flow was terminalized by a concurrent
// Terminate between resume's pre-tx status read and its own writes, so the transaction must roll
// back (undoing the step re-park/leaf-reset) and the caller must 409 rather than falsely report success.
var errResumeLost = errors.New("resume lost to a concurrent terminalization")

// resume continues a flow paused by flow.Interrupt, delivering resume data to the leaf interrupt park.
func (e *Engine) resume(ctx context.Context, flowKey string, data any) error {
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
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
		if err := e.checkPayloadSize("resume data", resumeDataJSON); err != nil {
			return err
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
	err = db.QueryRowContext(ctx, "SELECT status, surgraph_flow_id FROM dwarf_flows WHERE flow_id=? AND flow_token=? AND created_hour=?", flowID, flowToken, flowHour).Scan(&flowStatus, &surgraphFlowID)
	if err == sql.ErrNoRows {
		// An archived flow is closed, so it answers below as any closed flow does: not interrupted.
		_, flowStatus, surgraphFlowID, err = e.archivedFlowRow(ctx, db, shardNum, flowID, flowToken, flowHour)
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

	upFlowIDs, upStepIDs, upCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowHour, flowToken)
	if err != nil {
		return errors.Trace(err)
	}
	downFlowIDs, downStepIDs, downCompositeIDs, err := e.interruptedSubgraphChain(ctx, shardNum, flowID, flowHour, flowToken)
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
	// Terminate can commit its interrupted->terminated flip deterministically, then confirm the gate
	// write below rolls this transaction back (409) instead of falsely succeeding. Placed *before* the
	// transaction, not mid-tx: on SQLite the racing Terminate would deadlock against this transaction's write
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

		// Race gate against a concurrent Terminate. A Terminate that committed first has terminated the tree's
		// steps, so the step writes above matched nothing and the flow update below would match nothing too:
		// without this gate resume would return success for a resume that did not take effect. The root flow
		// row is the serialization point: this guarded lock takes the row and confirms it is still interrupted.
		// A zero-row match means Terminate won - roll the whole transaction back and 409. flowID is always the
		// root here (subgraph-child keys were rejected above), which is exactly the row Terminate terminalizes.
		n, gerr := lockFlowRows(ctx, tx, "flow_id=? AND status='"+workflow.StatusInterrupted+"'", flowID)
		if gerr != nil {
			return gerr
		}
		if n == 0 {
			lost = true
			return errResumeLost
		}

		for _, chainFlowID := range chainFlowIDs {
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET status=?, stopped_step_id=0, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"' AND (SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"')=0",
				workflow.StatusRunning, chainFlowID, chainFlowID,
			)
			// A flow still interrupted by another step now records that one - the step Snapshot reports and the
			// next Resume acts on - rather than the step just resumed.
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET stopped_step_id=(SELECT step_id FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"' ORDER BY updated_at, step_id LIMIT_OFFSET(1, 0)), touch=1-touch WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"'",
				chainFlowID, chainFlowID,
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

func (e *Engine) continueFlow(ctx context.Context, flowKey string, additionalState any) (nextKey string, err error) {
	shardNum, flowHour, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		return "", errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return "", errors.Trace(err)
	}

	// One turn for this operation's whole database interaction, taken at the entry point so nothing it
	// calls has to know about turns. Its age is stamped here, so it queues behind work already running
	// and ahead of anything arriving after it.
	ctx, doneTurn := e.opTurn(ctx, shardNum, "continue")
	defer doneTurn()

	// The caller's raw Go value is the only reducer input that does not arrive decoded from the database, so
	// canonicalize it: NewState JSON-round-trips a map/struct, so a struct's declaration-order fields become
	// sorted-key map form and a fold never compares two spellings of the same value (reducers compare
	// marshalled bytes).
	additional, err := workflow.NewState(additionalState)
	if err != nil {
		return "", errors.Trace(err, http.StatusBadRequest)
	}

	// The source is read with no lock and outside the insert's transaction, from the live tables or the archive: a
	// completed flow is immutable, and the new flow copies its state, graph and policy rather than referencing it, so
	// a deletion or a move racing this read leaves the copy valid.
	var flowStatus, workflowURL, fairnessKey, graphHash string
	var finalStateJSON, graphJSON, baggageJSON []byte
	var surgraphFlowID, deleteAfterMs, priority, timeBudgetMs int
	var fairnessWeight float64
	found, err := e.queryLiveOrArchive(ctx, db, shardNum, flowID, flowHour, true, func(t flowTables) (bool, error) {
		err := db.QueryRowContext(ctx,
			"SELECT status, surgraph_flow_id, delete_after_ms, final_state, graph, graph_hash, workflow_url, baggage, priority, fairness_key, fairness_weight, time_budget_ms FROM "+t.flows+" WHERE flow_id=? AND flow_token=? AND created_hour=?",
			flowID, flowToken, flowHour,
		).Scan(&flowStatus, &surgraphFlowID, &deleteAfterMs, &finalStateJSON, &graphJSON, &graphHash, &workflowURL, &baggageJSON, &priority, &fairnessKey, &fairnessWeight, &timeBudgetMs)
		if err == sql.ErrNoRows {
			return false, nil
		}
		return err == nil, errors.Trace(err)
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	if !found {
		return "", errors.New("flow not found", http.StatusNotFound)
	}
	// Continuing a subgraph child would spin up a detached top-level flow from the subgraph's final state, which
	// is never what a caller means: Continue must be addressed by a root flow's key.
	if surgraphFlowID != 0 {
		return "", errors.New("cannot continue a subgraph child; use a root flow key", http.StatusBadRequest)
	}
	if flowStatus != workflow.StatusCompleted {
		return "", errors.New("flow is not completed (status: %s)", flowStatus, http.StatusConflict)
	}
	// A flow scheduled for deletion is logically gone, as for Fork.
	if deleteAfterMs > 0 {
		return "", errors.New("flow is scheduled for deletion", http.StatusConflict)
	}

	finalState, err := workflow.NewState(finalStateJSON)
	if err != nil {
		return "", errors.Trace(err)
	}
	var graph workflow.Graph
	if err := json.Unmarshal(graphJSON, &graph); err != nil {
		return "", errors.Trace(err)
	}
	entryPoint := graph.EntryPoint()
	if entryPoint == "" {
		return "", errors.New("workflow has no entry point", http.StatusBadRequest)
	}
	// Fold the caller's additionalState onto the source's final_state through the graph's reducers. A nil
	// deletes a plain field and contributes nothing to a reduced one, so a new flow cannot reset a reduced
	// field; its first task can, with Overwrite.
	if err := finalState.Apply(workflow.ChangesOf(additional), graph.Reducers()); err != nil {
		// A value that will not fold onto the source's state is the caller's mistake, not a server fault.
		return "", errors.Trace(err, http.StatusBadRequest)
	}
	mergedStateJSON, err := json.Marshal(finalState)
	if err != nil {
		return "", errors.Trace(err)
	}
	if err := e.checkPayloadSize("initial state", mergedStateJSON); err != nil {
		return "", err
	}

	// Inherit the source's baggage; DeleteOnCompletion is forced off (a disposable flow deletes itself, so it
	// could never have been a Continue source). A flow wanting different policy is made with Create instead.
	inheritedBaggage, _ := workflow.NewState(baggageJSON)
	baggageOut, err := json.Marshal(inheritedBaggage)
	if err != nil {
		return "", errors.Trace(err)
	}
	timeBudget := time.Duration(timeBudgetMs) * time.Millisecond
	if timeBudget <= 0 {
		timeBudget = e.taskTimeBudget()
	}

	// The new flow starts its own trace (fresh detached root span, empty parent) and is its own root.
	newFlowToken := keys.RandomIdentifier(16)
	seed := flowSeed{
		workflowURL:    workflowURL,
		workflowName:   graph.Name(),
		graphJSON:      graphJSON,
		graphHash:      graphHash,
		baggageJSON:    baggageOut,
		stateJSON:      mergedStateJSON,
		traceParent:    e.mintWorkflowSpan(ctx, workflowURL, ""),
		flowToken:      newFlowToken,
		stepToken:      keys.RandomIdentifier(16),
		entryPoint:     entryPoint,
		entryURL:       dispatchURLOf(&graph, entryPoint),
		timeBudgetMs:   timeBudget.Milliseconds(),
		priority:       priority,
		fairnessKey:    fairnessKey,
		fairnessWeight: fairnessWeight,
		engineID:       e.engineID,
	}
	var newFlowID, newStepID int64
	var newFlowHour int
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		var err error
		newFlowID, newStepID, newFlowHour, err = insertFlowTx(ctx, tx, seed)
		return err
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	nextKey = keys.NewFlowKey(shardNum, newFlowHour, int(newFlowID), newFlowToken)
	e.logger.DebugContext(ctx, "Flow continued and started", "workflow", workflowURL)
	e.metricFlowStarted(ctx, workflowURL, priority, shardNum)
	// Ring the local doorbell so this replica claims the entry step without waiting for a refiller scan. The
	// entry step is due now and its priority is in hand, so no lookup is needed.
	e.enqueueStepDue(ctx, shardNum, int(newStepID), priority)
	return nextKey, nil
}
