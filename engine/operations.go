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

	"github.com/microbus-io/dwarf/internal/candidates"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/latch"
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
		if opts.FairnessWeight < 0 {
			return "", errors.New("fairness weight must be >= 0", http.StatusBadRequest)
		}
		// 0 means "use the engine default"; anything else must survive the millisecond persistence, or the
		// step is stamped with a 0 budget and its task's deadline has already passed when it is dispatched.
		if opts.TimeBudget < 0 || (opts.TimeBudget > 0 && opts.TimeBudget < time.Millisecond) {
			return "", errors.New("time budget must be 0 (engine default) or at least 1ms", http.StatusBadRequest)
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

	// FlowOptions.ThreadKey joins an existing thread: the shard is encoded in the key, and the new flow
	// adopts that flow's thread_id/thread_token (a mid-thread flow's thread_id is not its own flow_id).
	// Empty starts a fresh thread on a random shard.
	shardNum, threadID, threadToken := 0, 0, ""
	if opts.ThreadKey != "" {
		shardNum, threadID, threadToken, err = e.resolveThread(ctx, opts.ThreadKey)
		if err != nil {
			return "", errors.Trace(err)
		}
	} else {
		shardNum, err = e.pickShard()
		if err != nil {
			return "", errors.Trace(err)
		}
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
	flowKey, err = e.createWithGraph(ctx, shardNum, workflowURL, graph, state, threadID, threadToken, "", opts, 0, 0, 0, 0, nil)
	return flowKey, errors.Trace(err)
}

// resolveThread parses a FlowKey identifying a thread and returns the thread's shard, id, and token. The
// shard is encoded in the key; the thread_id/thread_token are read from the referenced flow (which may be
// mid-thread, so its thread_id differs from its own flow_id). Verifies the flow exists (token-checked).
func (e *Engine) resolveThread(ctx context.Context, threadKey string) (shardNum, threadID int, threadToken string, err error) {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(threadKey)
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	// Its own turn: create resolves the thread BEFORE picking up the turn that covers the insert, so this
	// read would otherwise be the one part of creation that jumps the queue.
	tctx, doneTurn := e.dbTurn(ctx, shardNum)
	var surgraphFlowID int
	err = db.QueryRowContext(tctx,
		"SELECT thread_id, thread_token, surgraph_flow_id FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&threadID, &threadToken, &surgraphFlowID)
	doneTurn()
	if err == sql.ErrNoRows {
		return 0, 0, "", errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	// A subgraph child key is read-only, like everywhere else (Resume/Terminate/Delete/Continue all 400 it).
	// A child runs on its own private thread precisely so it cannot contaminate the parent's continuation
	// chain, so joining that thread is never what a caller means: the new flow would be a top-level root
	// grouped under a subgraph's thread, and a later Continue of it would build on the subgraph's turns.
	// Reject rather than silently widen to the child's root - the caller must name the root it means.
	if surgraphFlowID != 0 {
		return 0, 0, "", errors.New("thread key is a subgraph flow; address the root flow", http.StatusBadRequest)
	}
	return shardNum, threadID, threadToken, nil
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
	threadID           int
	threadToken        string
	rootFlowID         int
	priority           int
	fairnessKey        string
	fairnessWeight     float64
	engineID           int64 // creator stamp: the inserting engine's random id (provenance, unindexed)
}

// insertFlowTx inserts the flow row (already `running`) and its entry step (`pending`, immediately
// claimable), then fixes thread_id/root_flow_id - the shared write-half of every creation path,
// run inside the caller's transaction. Returns the new flow and step ids. createWithGraph wraps it in a
// bare transaction; continueFlow wraps it in a transaction that first locks the thread and re-checks the
// latest turn, so both share exactly one copy of the insert SQL.
func insertFlowTx(ctx context.Context, tx *sequel.Tx, s flowSeed) (newFlowID, newStepID int64, err error) {
	// The three writes form a chain - the step and the fix-up take the flow's id - which insertedID makes one
	// statement batch on PostgreSQL.
	batch := newTxBatch(ctx, tx)
	queueInsertID(batch, "flow_id", func(id int64) { newFlowID = id },
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, baggage, trace_parent, status, surgraph_flow_id, surgraph_step_id, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms, engine_id, payload_ver, started_at)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC())",
		s.flowToken, s.workflowURL, s.workflowName, s.graphJSON, s.graphHash, s.baggageJSON, s.traceParent, workflow.StatusRunning, s.surgraphFlowID, s.surgraphStepID, s.deleteOnCompletion, s.priority, s.fairnessKey, s.fairnessWeight, s.timeBudgetMs, s.engineID, payloadVer,
	)
	flowID, flowArgs, err := batch.insertedID(lastFlowIDSQL, func() any { return newFlowID })
	if err != nil {
		return 0, 0, err
	}

	// Entry step is pending and immediately claimable (not_before=NOW, lease_expires=NOW). Its depth
	// continues from the caller: callerStepDepth+1 (1 for a top-level flow, where callerStepDepth is 0).
	// A flow that should wait before running uses an entry gate task with flow.Sleep, not a creation-time
	// delay.
	queueInsertID(batch, "step_id", func(id int64) { newStepID = id },
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, time_budget_ms, not_before, lease_expires, priority, fairness_key, fairness_weight, engine_id)"+
			" VALUES ("+flowID+", ?, ?, ?, ?, ?, ?, ?, NOW_UTC(), NOW_UTC(), ?, ?, ?, ?)",
		append(append([]any{}, flowArgs...), s.callerStepDepth+1, s.stepToken, s.entryPoint, s.entryURL, s.stateJSON, workflow.StatusPending, s.timeBudgetMs, s.priority, s.fairnessKey, s.fairnessWeight, s.engineID)...,
	)

	// A top-level flow starts its own thread; a Continue joins the thread it continues. root_flow_id is the
	// denormalized tree-membership index: a top-level flow is its own root; a subgraph child inherits the
	// parent's root. Written once here, immutable thereafter, so a whole tree is reachable by
	// `WHERE root_flow_id=?` without a recursive surgraph walk. Both derive from the new flow inside the
	// closure, so a retry (with a new flow_id) recomputes them rather than reusing the prior attempt's id.
	threadID, threadArgs, threadToken := "?", []any{s.threadID}, s.threadToken
	if s.threadID == 0 {
		threadID, threadArgs, threadToken = flowID, flowArgs, s.flowToken
	}
	rootID, rootArgs := "?", []any{s.rootFlowID}
	if s.rootFlowID == 0 {
		rootID, rootArgs = flowID, flowArgs
	}
	args := append(append([]any{}, threadArgs...), threadToken)
	args = append(args, rootArgs...)
	args = append(args, flowArgs...)
	flowKey := flowID
	if tx.DriverName() == "pgx" {
		flowKey = "(SELECT " + flowID + ")" // currval, evaluated once so the primary key is used
	}
	batch.queue(execOnly,
		"UPDATE dwarf_flows SET thread_id="+threadID+", thread_token=?, root_flow_id="+rootID+", updated_at=NOW_UTC(), touch=1-touch WHERE flow_id="+flowKey,
		args...,
	)
	if err := batch.flush(); err != nil {
		return 0, 0, err
	}
	return newFlowID, newStepID, nil
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
func (e *Engine) createWithGraph(ctx context.Context, shardNum int, workflowURL string, graph *workflow.Graph, initialState workflow.State, threadID int, threadToken string, parentTraceParent string, opts *workflow.FlowOptions, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID int, beforeInsert func(tx *sequel.Tx) error) (flowKey string, err error) {
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
		graphHash:          graphHashOf(graphJSON),
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
		threadID:           threadID,
		threadToken:        threadToken,
		rootFlowID:         rootFlowID,
		priority:           opts.Priority,
		fairnessKey:        opts.FairnessKey,
		fairnessWeight:     opts.FairnessWeight,
		engineID:           e.engineID,
	}

	var newFlowID, newStepID int64
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if beforeInsert != nil {
			if err := beforeInsert(tx); err != nil {
				return err
			}
			e.seams.Checkpoint(ctx, engineseams.CheckpointSubgraphSpawnParked)
		}
		var err error
		newFlowID, newStepID, err = insertFlowTx(ctx, tx, seed)
		return err
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	flowKey = keys.New(shardNum, int(newFlowID), flowToken)
	// Seed the graph cache when this replica dispatches the shard: the doorbell below offers the entry step to
	// its own candidate cache, so it is about to claim the step and would otherwise read back the graph it just
	// wrote. After the commit, so a rolled-back flow id - which SQLite can hand out again - is never cached.
	if e.dispatchesOn(shardNum) {
		if cg, perr := e.internGraph(graphJSON, workflowURL, seed.graphHash); perr == nil {
			e.cacheFlowGraph(shardNum, int(newFlowID), cg)
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
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
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
	err = db.QueryRowContext(ctx,
		"SELECT status, final_state, error, cancel_reason, payload_ver FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus, &finalStateJSON, &flowErrorMsg, &flowCancelReason, &flowPayloadVer)
	if err == sql.ErrNoRows {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return nil, errors.Trace(err)
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
		// For interrupted, query the leaf step's state and interrupt payload
		var stepStateJSON, stepChangesJSON, stepRefsJSON, interruptPayloadJSON []byte
		// Pick the same interrupted leaf Resume's chain walk would act on (earliest-updated, step_id
		// tiebreak) - not by step_depth, which is only an informational ordering and varies with branch
		// length (loops/gotos) without indicating which interrupt resolves next.
		err = db.QueryRowContext(ctx,
			"SELECT state, changes, state_refs, interrupt_payload FROM dwarf_steps"+
				" WHERE flow_id=? AND status='"+workflow.StatusInterrupted+"' ORDER BY updated_at, step_id LIMIT_OFFSET(1, 0)",
			flowID,
		).Scan(&stepStateJSON, &stepChangesJSON, &stepRefsJSON, &interruptPayloadJSON)
		// A missing interrupted step (ErrNoRows) is a tolerated race - report empty state; a real DB
		// error must not masquerade as that empty state.
		if err != nil && err != sql.ErrNoRows {
			return nil, errors.Trace(err)
		}
		if err == nil {
			// A terminal flow's final_state is plain JSON, readable in any format; an interrupted one's state is
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
				_, rerr = e.resolveStateRefs(ctx, db, shardNum, stepState, stepRefs, nil, "")
			}
			if rerr != nil {
				return nil, errors.Trace(rerr)
			}
			// The interrupted step's view: its changes so far folded onto its state, through the graph's reducers.
			graph, _, gerr := e.flowGraph(ctx, db, shardNum, flowID)
			if gerr != nil {
				return nil, errors.Trace(gerr)
			}
			if err := stepState.Apply(changesOf(stepChangesJSON), graph.Reducers()); err != nil {
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
	// and same treatment as the recovery-defer reset and the flow.Retry rewind (execution.go), which is the
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

// terminate forcefully stops a flow and its whole subgraph subtree, unconditionally. Root-only: a subgraph
// child is not an independently terminable unit (its parent is parked on it, and the unit of any lifecycle
// change is the tree), so a child key is rejected rather than silently widened into a tree-wide terminate.
func (e *Engine) terminate(ctx context.Context, flowKey string, reason string) error {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
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
		"SELECT status, surgraph_flow_id FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus, &surgraphFlowID)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if surgraphFlowID != 0 {
		return errors.New("cannot terminate a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	if workflow.IsTerminalStatus(flowStatus) {
		return nil // already ended: stopping a stopped flow is a no-op, as for Cancel
	}

	// The root has no ancestors by construction, so this walks DOWN only - see terminateSubtree, which the orphan
	// sweep shares.
	return errors.Trace(e.terminateSubtree(ctx, shardNum, flowID, flowToken, reason, false, engineseams.FaultTerminateCommit))
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
// ONE CANCEL PER FLOW: a nonzero cancel_watermark means the flow was already cancelled, and a repeat call is
// a no-op. Not cancel_reason, which may be empty and is shared with Terminate. A repeat that re-marked would
// preempt the onError handler that caught the first cancellation - a host retrying a Cancel whose response
// was lost would cancel a flow that had recovered - so a flow that recovers and must still stop is
// Terminated. The watermark write is itself guarded on cancel_watermark=0, so two concurrent calls that both
// read zero raise it once. A no-op on a terminal flow too, rather than a 409: the flow write is guarded to
// non-terminal flows, so it can never rewrite a recorded outcome.
func (e *Engine) cancel(ctx context.Context, flowKey string, reason string) error {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
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
		"SELECT surgraph_flow_id, cancel_watermark, status FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&surgraphFlowID, &watermark, &status)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if surgraphFlowID != 0 {
		return errors.New("cannot cancel a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	if watermark != 0 || workflow.IsTerminalStatus(status) {
		return nil // already cancelled, or already ended - see above. The writes below re-guard both.
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

	reason = strings.TrimSpace(reason)
	// The watermark is read by a statement AFTER the one that takes the flow rows' locks. On Postgres an UPDATE's
	// subquery reads the snapshot taken before the statement waited for its row lock, so a single statement would
	// miss a successor that a transition committed while holding that lock - and that successor could then run to
	// completion before the mark below, unmarked, and pass nothing on. Every lock is taken before any watermark is
	// read. The flow-id lists are chunked: a tree can hold more flows than a statement can bind.
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		for chunk := range slices.Chunk(allFlowIDs, inListChunk) {
			ph, ids := inList(chunk)
			lockFlowRows(ctx, tx, "flow_id IN ("+ph+") AND status NOT IN ("+terminalStatusesSQL+")", ids...)
		}
		for chunk := range slices.Chunk(allFlowIDs, inListChunk) {
			ph, ids := inList(chunk)
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET cancel_reason=?,"+
					" cancel_watermark=(SELECT COALESCE(MAX(s.step_id), 0) FROM dwarf_steps s WHERE s.flow_id=dwarf_flows.flow_id),"+
					" touch=1-touch WHERE flow_id IN ("+ph+") AND status NOT IN ("+terminalStatusesSQL+")"+
					" AND cancel_watermark=0",
				append([]any{reason}, ids...)...,
			)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	e.seams.Checkpoint(ctx, engineseams.CheckpointCancelBeforeMark)
	// The mark reaches only steps at or below their flow's watermark. A step above it was inserted after the
	// watermark committed, and its insert already decided its mark - inherited if its source was reached by
	// this Cancel, not if its source was covered. Unbounded, the mark would catch the onError handler of a
	// step preempted in the window between that commit and this statement, and deliver the cancellation a
	// second time to the handler written to catch it. A chunk holds whole flows, so each flow is marked by one
	// statement.
	for chunk := range slices.Chunk(allFlowIDs, inListChunk) {
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

// deleteFlow schedules a flow (and its subgraph subtree) for deletion by the reaper - it does NOT delete rows
// inline. It stamps delete_after_ms=1 (due immediately) on the root; the reaper removes the whole tree on its
// next pass. Only a terminal root is stamped, so a live delete_after_ms never coexists with a resumable flow.
//
// An interrupted flow is terminated first, exactly as Terminate would (terminateSubtree): its whole tree is
// stopped, given a final state and counted in dwarf_flows_ended once. Deleting a pending approval is terminating
// it. A racing Resume and Delete serialize on the root's row: whichever commits first wins, and a Delete that
// loses is a 409, as for any running flow.
func (e *Engine) deleteFlow(ctx context.Context, flowKey string) error {
	shardNum, flowID, flowToken, err := keys.ParseFlowKey(flowKey)
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

	// Test checkpoint: a breakpoint here freezes Delete just before it reads the flow's status (holding no lock,
	// so a racing Resume can commit), letting a test drive the Delete-vs-Resume race in either order.
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeDeleteWrite)
	var flowStatus string
	var surgraphFlowID, deleteAfterMs int
	err = db.QueryRowContext(ctx,
		"SELECT status, surgraph_flow_id, delete_after_ms FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus, &surgraphFlowID, &deleteAfterMs)
	if err == sql.ErrNoRows {
		return errors.New("flow not found", http.StatusNotFound)
	}
	if err != nil {
		return errors.Trace(err)
	}
	// A subgraph child cannot be deleted directly (its parent's surgraph step would dangle). Address the
	// tree by the root key; the reaper removes descendants via root_flow_id. A child key is read-only.
	if surgraphFlowID != 0 {
		return errors.New("cannot delete a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	// The 409 guards only the ROOT's status; a running subgraph descendant of a terminal root does not block the
	// delete. The reaper later removes the whole root_flow_id tree regardless of descendant status - safe because
	// the only running descendant a terminal-rooted tree can hold is a live orphan (which only a bug leaves) the
	// wedge sweep would terminate anyway, and a worker mid-dispatch on it no-ops via the lease fence (see
	// reapDueFlows).
	if flowStatus == workflow.StatusRunning {
		return errors.New("cannot delete a running flow; terminate it first", http.StatusConflict)
	}
	if deleteAfterMs > 0 {
		return nil // already scheduled - idempotent
	}
	if flowStatus == workflow.StatusInterrupted {
		err = e.terminateSubtree(ctx, shardNum, flowID, flowToken, "deleted", true, "")
		if errors.Is(err, errRootNotInterrupted) {
			return errors.New("cannot delete a running flow; terminate it first", http.StatusConflict)
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET delete_after_ms=1 WHERE flow_id=? AND flow_token=? AND status IN ("+terminalStatusesSQL+") AND delete_after_ms=0",
		flowID, flowToken,
	)
	return errors.Trace(err)
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
