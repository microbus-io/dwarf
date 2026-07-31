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
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/candidates"
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
	// The create-time GraphLoader sees the baggage on ctx in the same JSON-decoded shape (numbers as
	// float64) every dispatch will - NewState round-trips the raw Go value through JSON.
	loaderBaggage, _ := workflow.NewState(opts.Baggage)
	loaderCtx := workflow.ContextWithBaggage(ctx, loaderBaggage)
	var graph *workflow.Graph
	err = errors.CatchPanic(func() error {
		var lerr error
		graph, lerr = e.host.LoadGraph(loaderCtx, workflowURL)
		return lerr
	})
	if err == nil && e.seams.Enabled() && e.seams.IsFault(seamsJoin(FaultLoadGraph, workflowURL)) {
		err = errors.New("injected fault: "+FaultLoadGraph+" "+workflowURL, http.StatusInternalServerError)
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
	// engine derives that map per flow at dispatch (internal/faninmap), so only the author's definition is
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
	// every step already under way and ahead of every one admitted after it, which is what keeps a burst of
	// creation from overtaking the work it just created.
	//
	// The turn wraps createWithGraph rather than living inside it, because that helper is shared with the
	// subgraph-spawn path, which is already holding a turn when it calls in. A turn taken while holding one
	// is the nesting the enclosure rule forbids: with as many turns as connections, holders waiting on a
	// second turn deadlock against each other.
	ctx, doneTurn := e.dbTurn(ctx, shardNum)
	flowKey, err = e.createWithGraph(ctx, shardNum, workflowURL, graph, state, threadID, threadToken, "", opts, 0, 0, 0, 0)
	doneTurn()
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
		return 0, 0, "", errors.New("thread not found", http.StatusNotFound)
	}
	if err != nil {
		return 0, 0, "", errors.Trace(err)
	}
	// A subgraph child key is read-only, like everywhere else (Resume/Cancel/Delete/Continue all 400 it).
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
// claimable), then fixes thread_id/step_id/root_flow_id - the shared write-half of every creation path,
// run inside the caller's transaction. Returns the new flow and step ids. createWithGraph wraps it in a
// bare transaction; continueFlow wraps it in a transaction that first locks the thread and re-checks the
// latest turn, so both share exactly one copy of the insert SQL.
func insertFlowTx(ctx context.Context, tx *sequel.Tx, s flowSeed) (newFlowID, newStepID int64, err error) {
	newFlowID, err = tx.InsertReturnID(ctx, "flow_id",
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, baggage, trace_parent, status, surgraph_flow_id, surgraph_step_id, delete_on_completion, priority, fairness_key, fairness_weight, time_budget_ms, engine_id, started_at)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC())",
		s.flowToken, s.workflowURL, s.workflowName, s.graphJSON, s.baggageJSON, s.traceParent, workflow.StatusRunning, s.surgraphFlowID, s.surgraphStepID, s.deleteOnCompletion, s.priority, s.fairnessKey, s.fairnessWeight, s.timeBudgetMs, s.engineID,
	)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	// Entry step is pending and immediately claimable (not_before=NOW, lease_expires=NOW). Its depth
	// continues from the caller: callerStepDepth+1 (1 for a top-level flow, where callerStepDepth is 0).
	// A flow that should wait before running uses an entry gate task with flow.Sleep, not a creation-time
	// delay.
	newStepID, err = tx.InsertReturnID(ctx, "step_id",
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, time_budget_ms, not_before, lease_expires, priority, fairness_key, fairness_weight, engine_id)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, NOW_UTC(), NOW_UTC(), ?, ?, ?, ?)",
		newFlowID, s.callerStepDepth+1, s.stepToken, s.entryPoint, s.entryURL, s.stateJSON, workflow.StatusPending, s.timeBudgetMs, s.priority, s.fairnessKey, s.fairnessWeight, s.engineID,
	)
	if err != nil {
		return 0, 0, errors.Trace(err)
	}

	// Derive the thread from the new flow inside the closure so a retry (with a new flow_id) recomputes
	// it rather than reusing the prior attempt's id.
	tid, ttok := s.threadID, s.threadToken
	if tid == 0 {
		tid = int(newFlowID)
		ttok = s.flowToken
	}
	// root_flow_id is the denormalized tree-membership index: a top-level flow (rootFlowID==0) is its
	// own root; a subgraph child inherits the parent's root. Written once here, immutable thereafter, so
	// a whole tree is reachable by `WHERE root_flow_id=?` without a recursive surgraph walk.
	rfid := s.rootFlowID
	if rfid == 0 {
		rfid = int(newFlowID)
	}
	_, err = tx.ExecContext(ctx,
		"UPDATE dwarf_flows SET thread_id=?, thread_token=?, step_id=?, root_flow_id=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=?",
		tid, ttok, newStepID, rfid, newFlowID,
	)
	if err != nil {
		return 0, 0, errors.Trace(err)
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
// parent. (The parent caller is parked by processStep before this is called, the complementary half of
// that ordering.) callerStepDepth is the caller step's step_depth (0 for a top-level flow): the entry step
// is created at callerStepDepth+1, so a subgraph's depths continue from the caller (informational only).
func (e *Engine) createWithGraph(ctx context.Context, shardNum int, workflowURL string, graph *workflow.Graph, initialState workflow.State, threadID int, threadToken string, parentTraceParent string, opts *workflow.FlowOptions, surgraphFlowID, callerStepDepth, surgraphStepID, rootFlowID int) (flowKey string, err error) {
	entryPoint := graph.EntryPoint()
	if entryPoint == "" {
		return "", errors.New("workflow has no entry point", http.StatusBadRequest)
	}

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
		var err error
		newFlowID, newStepID, err = insertFlowTx(ctx, tx, seed)
		return err
	})
	if err != nil {
		return "", errors.Trace(err)
	}

	flowKey = keys.New(shardNum, int(newFlowID), flowToken)
	e.logger.DebugContext(ctx, "Flow created and started", "workflow", workflowURL, "task", entryPoint)
	e.metricFlowStarted(ctx, workflowURL, shardNum)
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
	ctx, doneTurn := e.dbTurn(ctx, shardNum)
	defer doneTurn()

	var flowStatus string
	var finalStateJSON []byte
	var flowErrorMsg string
	var flowCancelReason string
	err = db.QueryRowContext(ctx,
		"SELECT status, final_state, error, cancel_reason FROM dwarf_flows WHERE flow_id=? AND flow_token=?",
		flowID, flowToken,
	).Scan(&flowStatus, &finalStateJSON, &flowErrorMsg, &flowCancelReason)
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

	switch flowStatus {
	case workflow.StatusCompleted:
		out.State, _ = workflow.NewState(finalStateJSON)
	case workflow.StatusFailed:
		out.State, _ = workflow.NewState(finalStateJSON)
		out.Error = flowErrorMsg
	case workflow.StatusCancelled:
		out.State, _ = workflow.NewState(finalStateJSON)
		out.CancelReason = flowCancelReason
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
			stepState, _ := workflow.NewState(stepStateJSON)
			stepChanges, _ := workflow.NewState(stepChangesJSON)
			// The ref encoding is a storage detail, never API-visible: a caller sees the state the step
			// actually saw (the ref encoding is internal storage, never API-visible). resolveStateRefs mutates the map in place, so it gets the live map.
			if _, rerr := e.resolveStateRefs(ctx, db, shardNum, stepState, staterefs.Parse(stepRefsJSON), nil, ""); rerr != nil {
				return nil, errors.Trace(rerr)
			}
			// Materialize the interrupted step's view: state + changes with pending deletes enacted.
			_ = stepState.Merge(stepChanges)
			stepState.DelNils()
			out.State = stepState
			out.InterruptPayload, _ = workflow.NewState(interruptPayloadJSON)
		}
	case workflow.StatusRunning, workflow.StatusCreated:
		out.State = workflow.State{}
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
	// READ, PARK, READ - at most twice, never in a loop. The two reads answer different questions and both
	// have to be here:
	//
	// The FIRST read is what makes an already-settled flow answer AT ONCE. Parking instead would be
	// correct - the detector reports the current state of every parked key, so a stop that happened before
	// this caller arrived is still found - but it would be found on the sweep's cadence, turning every
	// `Await` on a finished flow into a wait of up to one full sweep. Measured: dropping this read doubled
	// the engine package's test time. It costs one indexed lookup, and only on calls that are about to
	// block anyway.
	//
	// The SECOND read builds the outcome once the board says the flow has SETTLED. It is not a re-check:
	// every release means the row reached a stop (or stopped resolving), never merely "look again", which
	// is what removes the loop this used to need. Do not release on a non-settled status - a caller would
	// take a running flow for a finished one. `signalStop` guards that.
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
	return e.snapshot(ctx, flowKey)
}

// signalStop wakes local Await callers waiting on the given flow. Use it at every flow-stop site
// (completed, failed, cancelled, interrupted) and NOWHERE ELSE: a woken caller reads the flow once and
// returns what it finds, so waking it for a status the flow is merely passing through would hand a running
// flow back as an outcome. A non-terminal transition needs no wake at all - nobody is parked on it.
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
		e.seams.Checkpoint(ctx, CheckpointFlowStopped)
		e.seams.Checkpoint(ctx, seamsJoin(CheckpointFlowStopped, flowKey, status))
	}
	// FaultDropSignalStop simulates a lost terminal wake - a worker crash between the commit and the
	// signal - so a test can prove Await still returns without one, via the latch detector reading the
	// committed row.
	if e.seams.IsFault(FaultDropSignalStop) {
		return
	}
	// Guarded, not merely documented: a release is a promise that the flow has settled, and the caller acts
	// on it without re-checking. A non-stopped status here would be a wrong answer to a caller, so it is
	// dropped rather than delivered - the flow's real stop will wake them later.
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
// It used to also broadcast to peers (op `enqueue`, one message per step per peer). That broadcast was
// removed: under load every peer's piston is already cycling at its derived interval, so the doorbell
// bought no dispatch latency the next cycle would not have covered, while costing R-1 messages per step
// AND a PK lookup on every receiver (the inbound path had to resolve the announced step's priority and
// due-ness against its own clock, which is exactly the round-trip enqueueStepDue exists to avoid
// locally). It also head-inserted UNPARTITIONED on the receiver, so a peer could race the residue class's
// owner to the claim CAS. What replaced it is that every piston cycles unconditionally: a peer discovers
// work by scanning, on a bounded cadence, with no message at all.
//
// The doorbell no longer rings a refiller either - there is no trigger to ring, because a piston's cycle
// is not on-demand. What it still does, and what makes that affordable, is Offer: an empty partition
// ADMITS the step, so a sequential chain's next hop dispatches immediately instead of waiting out a cycle.
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
	// FaultDropDoorbell simulates a lost work doorbell so a test can prove the pending step is still picked
	// up by a later refiller scan rather than stranding.
	if e.seams.IsFault(FaultDropDoorbell) {
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
	// FaultDropDoorbell: see enqueueStep.
	if e.seams.IsFault(FaultDropDoorbell) {
		return
	}
	admitted := e.cache.Offer(candidates.Job{StepID: stepID, Shard: shard}, priority)
	if admitted {
		e.metricStepOffered(ctx)
	}
	e.logger.DebugContext(ctx, "Doorbell (due)", "stepID", stepID, "priority", priority, "admitted", admitted)
}

// cancel aborts a flow and its whole subgraph subtree. Root-only: a subgraph child is not an independently
// cancellable unit (its parent is parked on it, and the unit of any lifecycle change is the tree), so a child key
// is rejected rather than silently widened into a tree-wide cancel.
func (e *Engine) cancel(ctx context.Context, flowKey string, reason string) error {
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
	ctx, doneTurn := e.dbTurn(ctx, shardNum)
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
		return errors.New("cannot cancel a subgraph child; use the root flow key", http.StatusBadRequest)
	}
	if flowStatus == workflow.StatusCompleted || flowStatus == workflow.StatusFailed || flowStatus == workflow.StatusCancelled {
		return errors.New("flow is already in terminal status", http.StatusConflict)
	}

	// The root has no ancestors by construction, so this walks DOWN only - see cancelSubtree, which the orphan
	// sweep shares. A racing terminalization that empties the flow UPDATE is a 409 here: the caller asked to stop
	// something that had already stopped.
	return errors.Trace(e.cancelSubtree(ctx, shardNum, flowID, flowToken, reason, FaultCancelCommit, true))
}

// deleteFlow schedules a flow (and its subgraph subtree) for deletion by the reaper - it does NOT delete rows
// inline. It stamps delete_after_ms=1 (due immediately) on the root; the reaper removes the whole tree on its
// next pass. An interrupted flow is terminalized (interrupted -> cancelled) in the same UPDATE, which is
// mutually exclusive with a racing Resume (WHERE status<>'running', re-checked under the row lock), so the
// old strand race (delete steps while Resume revives) is gone by construction - no lock-first selection needed.
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
	ctx, doneTurn := e.dbTurn(ctx, shardNum)
	defer doneTurn()

	// Test checkpoint: a breakpoint here freezes Delete just before its transaction (holding no lock, so a
	// racing Resume can commit), letting a test drive the Delete-vs-Resume race in either order. Placed before
	// the transaction, not inside, for the same SQLite-deadlock reason as CheckpointResumeBeforeFlowWrite.
	e.seams.Checkpoint(ctx, CheckpointBeforeDeleteWrite)
	return errors.Trace(db.Transact(ctx, func(tx *sequel.Tx) error {
		var flowStatus string
		var surgraphFlowID, deleteAfterMs int
		err := tx.QueryRowContext(ctx,
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
		// The 409 guards only the ROOT's status; a running subgraph descendant does not block the delete. The
		// reaper later removes the whole root_flow_id tree regardless of descendant status - safe because the
		// only running descendant a terminal-rooted tree can hold is a live orphan (Cancel-vs-spawn residue) the
		// wedge sweep would cancel anyway, and a worker mid-dispatch on it no-ops via the lease fence. This is a
		// deliberate change from the old inline delete, which 409'd on any running descendant (see reapDueFlows).
		if flowStatus == workflow.StatusRunning {
			return errors.New("cannot delete a running flow; cancel it first", http.StatusConflict)
		}
		if deleteAfterMs > 0 {
			return nil // already scheduled - idempotent
		}

		// Stamp due-now; terminalize an interrupted flow in the same write (the Resume gate). status<>'running'
		// re-guards a Resume that raced in after the SELECT: it either wins (row is running -> 0 rows here, a
		// benign lost delete, flow stays alive) or loses (we stamp; its interrupted CAS then finds cancelled).
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET delete_after_ms=1, status=CASE WHEN status='"+workflow.StatusInterrupted+"' THEN '"+workflow.StatusCancelled+"' ELSE status END WHERE flow_id=? AND flow_token=? AND status<>'"+workflow.StatusRunning+"' AND delete_after_ms=0",
			flowID, flowToken,
		)
		return nil
	}))
}

// run creates, starts, and awaits a flow in one call.
func (e *Engine) run(ctx context.Context, workflowURL string, initialState any, opts *workflow.FlowOptions) (flowKey string, outcome *workflow.FlowOutcome, err error) {
	flowKey, err = e.create(ctx, workflowURL, initialState, opts) // auto-starts
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	outcome, err = e.await(ctx, flowKey)
	if err != nil {
		return flowKey, nil, errors.Trace(err)
	}
	if !outcome.Stopped() {
		// The caller's ctx expired before the flow stopped. The flow is durable and already running on the
		// engine's own worker lifetime, independent of this call - so do NOT tear it down. Cancelling here
		// would destroy healthy, in-progress work (a durable retry-until-success job especially) just because
		// the caller stopped waiting - an availability footgun. Return the flowKey with a timeout error
		// instead, so the caller keeps a handle to re-Await/Snapshot/Cancel on its own terms.
		return flowKey, nil, errors.Trace(ctx.Err(), http.StatusRequestTimeout)
	}
	return flowKey, outcome, nil
}
