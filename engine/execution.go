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
	"maps"
	"net/http"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/faninmap"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// processStep acquires a step, executes its task, and enqueues the next step if applicable.
func (e *Engine) processStep(ctx context.Context, stepID int, shardNum int) (err error) {
	// stepMarkedComplete is set once the step is flipped to `completed` below, before its forward
	// progress (transitions / fan-in / flow completion) is committed in a separate transaction. If
	// that later commit fails, the recovery defer rolls the step back so it re-dispatches.
	var stepMarkedComplete bool
	// leaseSeq is this dispatch's lease generation, read from the claim CAS below (0 = not captured).
	// Every post-execution write to the dispatched step carries `AND lease_seq=?` to fence it.
	var leaseSeq int
	defer func() {
		if err == nil {
			return
		}
		// Reset a step that processStep left in a non-pending state so recovery can re-dispatch it,
		// then re-poll. Two mutually-exclusive cases per step, selected by from-status:
		//   running→pending    a lock-contention error left the step leased mid-processStep.
		//   completed→pending  the step was marked `completed` but the follow-up transaction that
		//                       inserts successors / bumps cohort_arrivals / completes the flow failed
		//                       to commit. Re-dispatch re-runs the task and re-evaluates transitions.
		// Both are guarded on the from-status for idempotency and retried via Transact since this
		// recovery can itself race a contention storm; the reset UPDATE losing that race is the residual
		// wedge that detectOrphanedFlows surfaces.
		fromStatus := ""
		if stepMarkedComplete {
			fromStatus = workflow.StatusCompleted
		} else if sequel.IsLockContentionError(err) {
			fromStatus = workflow.StatusRunning
		}
		if fromStatus == "" {
			return
		}
		if db, derr := e.db.Shard(shardNum); derr == nil {
			// Fence the reset on our lease generation so a zombie (lease already re-granted to a peer)
			// cannot rewind the peer's freshly-claimed step. leaseSeq==0 means we failed before capturing
			// it (a claim/read error microseconds after claiming, before any ExecuteTask ran) - there a
			// peer cannot have stolen the lease yet, so the reset stays unfenced to preserve prompt
			// recovery; every reset that follows an execution has leaseSeq>0 and is fenced.
			resetSQL := "UPDATE dwarf_steps SET status=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?"
			resetArgs := []any{workflow.StatusPending, stepID, fromStatus}
			if leaseSeq > 0 {
				resetSQL += " AND lease_seq=?"
				resetArgs = append(resetArgs, leaseSeq)
			}
			// Test checkpoint: a breakpoint here freezes a zombie between capturing its (stale) reset and
			// running it, so a test can let a peer re-claim+bump the step first and prove the lease_seq fence
			// makes the zombie's reset a zero-row no-op (never rewinding the peer's freshly-claimed step).
			e.seams.Checkpoint(ctx, checkpointBeforeRecoveryReset)
			db.Transact(ctx, func(tx *sequel.Tx) error {
				// faultRecoveryResetErr makes this last-resort reset itself fail (a non-retryable error, as if
				// it lost to a contention storm), so the step stays `completed` and the flow strands `running`
				// with every step terminal - the residual orphan hole that only detectOrphanedFlows surfaces.
				// Process-wide (no scope): taskName is declared after this defer, so it is not in scope here.
				if e.seams.IsFault(faultRecoveryResetErr) {
					return errors.New("injected fault: " + faultRecoveryResetErr)
				}
				_, terr := tx.ExecContext(ctx, resetSQL, resetArgs...)
				return terr
			})
		}
		e.shortenNextPoll(time.Now())
	}()
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	// Lease = the step's own time_budget_ms (added in the claim UPDATE below) + a fixed margin, so it always
	// outlasts the ExecuteTask deadline. Only the margin is bound; the per-step budget comes from the column.
	leaseMarginMs := int(e.leaseMargin.Milliseconds())

	// Claim the step and read its data in one round-trip where the driver supports RETURNING.
	var n int64
	var stepDepth int
	var taskName, stepToken, stateJSON, priorChangesJSON string
	var attempt, lineageID, flowID, timeBudgetMs int
	var interruptDone bool
	var resumeDataJSON string
	var subgraphDone bool
	var subgraphResultJSON, subgraphErrorStr string
	var stepCreatedAt time.Time
	var stateRefsJSON string

	switch db.DriverName() {
	case "pgx", "sqlite":
		err = db.QueryRowContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), lease_seq=lease_seq+1, engine_id=?, updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" WHERE step_id=? AND status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
				" RETURNING step_depth, task_name, step_token, state, changes, state_refs, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, created_at, lease_seq",
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &stateRefsJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	case "mssql":
		err = db.QueryRowContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), lease_seq=lease_seq+1, engine_id=?, updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" OUTPUT INSERTED.step_depth, INSERTED.task_name, INSERTED.step_token, INSERTED.state, INSERTED.changes, INSERTED.state_refs, INSERTED.attempt, INSERTED.lineage_id, INSERTED.flow_id, INSERTED.time_budget_ms, INSERTED.interrupt_done, INSERTED.resume_data, INSERTED.subgraph_done, INSERTED.subgraph_result, INSERTED.subgraph_error, INSERTED.created_at, INSERTED.lease_seq"+
				" WHERE step_id=? AND status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &stateRefsJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	default:
		// MySQL lacks RETURNING, so claim and read are two statements. They must run SERIALLY, not in
		// parallel: the read pulls columns a concurrent Resume/flow.Retry/subgraph-completion mutates
		// (resume_data, subgraph_result, attempt, interrupt_done, subgraph_done) in the SAME transaction
		// that flips the step to pending. On separate connections with independent snapshots, the read
		// could observe the pre-transition row (empty resume_data) while the claim observes the committed
		// pending row and succeeds - delivering an empty resume payload to the task. Claiming first, then
		// reading only on a successful claim, guarantees the read's snapshot is after the claim commit (and
		// thus after the pending-setter's commit). A lost claim skips the read entirely (n==0 returns below).
		res, claimErr := db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), lease_seq=lease_seq+1, engine_id=?, updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" WHERE step_id=? AND status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		)
		if claimErr != nil {
			err = claimErr
			break
		}
		n, _ = res.RowsAffected()
		if n == 1 {
			readErr := db.QueryRowContext(ctx,
				"SELECT step_depth, task_name, step_token, state, changes, state_refs, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, created_at, lease_seq FROM dwarf_steps WHERE step_id=?",
				stepID,
			).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &stateRefsJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq)
			if readErr != nil && readErr != sql.ErrNoRows {
				err = readErr
			}
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	if n == 0 || flowID == 0 {
		return nil
	}

	// Read flow data
	var flowToken, flowStatus, workflowURL, graphJSON, baggageJSON, traceParent string
	var flowCreatedAt, flowUpdatedAt time.Time
	var flowPriority int
	var flowFairnessKey string
	var flowFairnessWeight float64
	var flowTimeBudgetMs int
	err = db.QueryRowContext(ctx,
		"SELECT flow_token, status, workflow_url, graph, baggage, trace_parent, created_at, updated_at, priority, fairness_key, fairness_weight, time_budget_ms FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&flowToken, &flowStatus, &workflowURL, &graphJSON, &baggageJSON, &traceParent, &flowCreatedAt, &flowUpdatedAt, &flowPriority, &flowFairnessKey, &flowFairnessWeight, &flowTimeBudgetMs)
	if err != nil {
		return errors.Trace(err)
	}
	// The flow's frozen budget seeds the steps this dispatch creates and bounds the subgraph LoadGraph. The
	// fallback is pure defense; createWithGraphTx always stores a concrete value.
	if flowTimeBudgetMs <= 0 {
		flowTimeBudgetMs = int(e.taskTimeBudget().Milliseconds())
	}
	e.metricStateReadBytes(ctx, workflowURL, "state", len(stateJSON))
	e.metricStateReadBytes(ctx, workflowURL, "changes", len(priorChangesJSON))
	e.metricStateReadBytes(ctx, workflowURL, "resume_data", len(resumeDataJSON))
	e.metricStateReadBytes(ctx, workflowURL, "subgraph_result", len(subgraphResultJSON))

	flowStatus = strings.TrimSpace(flowStatus)
	flowToken = strings.TrimSpace(flowToken)
	if flowStatus == workflow.StatusCancelled || flowStatus == workflow.StatusFailed || flowStatus == workflow.StatusCompleted {
		_, err = db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
			flowStatus, parkedNone, stepID,
		)
		return errors.Trace(err)
	}

	// Parse graph, reusing the cached parse — graphJSON is frozen at flow creation, so every step of
	// the same flow sees identical bytes.
	graphKey := graphCacheKey{shard: shardNum, flowID: flowID} // scopes the cache by shard, since flow_id is only unique within a shard
	cg, cached := e.graphCache.Load(graphKey)
	if !cached {
		parsed := &workflow.Graph{}
		err = json.Unmarshal([]byte(graphJSON), parsed)
		if err != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		// Derive the fan-in routing map once per flow and cache it with the graph; it is not persisted.
		cg = &cachedGraph{graph: parsed, fanIn: faninmap.New(parsed)}
		e.graphCache.Store(graphKey, cg)
	}
	graph := cg.graph

	// Build the Flow carrier
	var state map[string]any
	unmarshalJSONMap(stateJSON, &state)
	// Materialize any field this step carries BY REFERENCE (see staterefs.go). Resolving here, once, is what
	// keeps the ref encoding out of everything downstream: the carrier, `when` evaluation, forEach expansion,
	// the transport to a remote task, and the transition machinery all work on literals and never learn refs
	// exist. The refs the step inherited are kept, because minting the successors' state needs to know which
	// fields arrived as refs (and so must be carried, never re-anchored against this step).
	inheritedRefs := parseStateRefs(stateRefsJSON)
	if err := e.resolveStateRefs(ctx, db, shardNum, state, inheritedRefs, nil, workflowURL); err != nil {
		err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
		return errors.Trace(err)
	}
	var priorChanges map[string]any
	unmarshalJSONMap(priorChangesJSON, &priorChanges)
	mergedInputState, _ := workflow.MergeState(state, priorChanges, nil)
	flow := workflow.NewRawFlow()
	flow.SetRawState(mergedInputState)
	flow.SetRawChanges(priorChanges)
	flow.SetAttempt(attempt)
	flow.SetCreatedAt(flowCreatedAt)
	flow.SetUpdatedAt(flowUpdatedAt)
	flow.SetStepCreatedAt(stepCreatedAt)
	// The task's own identity, so it can correlate logs/traces or call back into the engine
	// (e.g. History/Step) with its own keys. flow_token is loaded with the flow row, step_token
	// alongside the claim - both available here.
	flow.SetFlowKey(fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken))
	flow.SetStepKey(fmt.Sprintf("%d-%d-%s", shardNum, stepID, stepToken))

	if interruptDone {
		var resumeData map[string]any
		unmarshalJSONMap(resumeDataJSON, &resumeData)
		flow.SetInterruptResolution(resumeData)
	}
	if subgraphDone {
		var subgraphResult map[string]any
		unmarshalJSONMap(subgraphResultJSON, &subgraphResult)
		flow.SetSubgraphResolution(subgraphResult, subgraphErrorStr)
	}

	// Parse baggage for the task executor
	var baggage map[string]any
	unmarshalJSONMap(baggageJSON, &baggage)

	// Execute the task. The step's time_budget_ms bounds the executor call's context deadline; the
	// surrounding DB work keeps using the undeadlined ctx so persistence is never cut short.
	e.logger.DebugContext(ctx, "Executing task", "task", taskName, "workflow", workflowURL)
	dispatchURL := dispatchURLOf(graph, taskName)
	taskCtx := workflow.ContextWithBaggage(ctx, baggage)

	// Open a per-step span parented to the flow's root "workflow" span (reconstructed from the stored
	// trace_parent), and place it on the executor's context so the task's downstream spans nest under it.
	// The span is named by the task; no-op unless a TracerProvider is configured. workflow.id carries the
	// token-free correlation id, never the flowKey: a trace backend is typically readable far more broadly
	// than the workflow data, and the key is a bearer write-capability.
	taskCtx = injectTraceParent(taskCtx, traceParent)
	taskCtx, taskSpan := e.tracer.Start(taskCtx, taskName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("workflow.id", keys.CorrelationID(shardNum, flowID)),
			attribute.String("workflow.name", workflowURL),
		),
	)
	defer taskSpan.End()

	if timeBudgetMs > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(taskCtx, time.Duration(timeBudgetMs)*time.Millisecond)
		defer cancel()
	}
	// Account for this worker as parked in the task, and grow the pool if that leaves NO worker free to
	// dispatch. The counter must wrap the host call and nothing else: a worker anywhere else in
	// processStep is either using a database connection or waiting for one, and spawning a peer then only
	// adds contention for the same pool - the runaway that measures saturation and calls it "long tasks".
	// Inside ExecuteTask a worker holds no connection, so a peer is pure added dispatch capacity.
	e.workersInTask.Add(1)
	e.maybeSpawnWorker()
	// A panic in the in-process host is caught here so it flows through the normal error disposition
	// rather than wedging this leased step until lease expiry.
	execErr := errors.CatchPanic(func() error {
		// faultPanicExecuteTask panics inside the wrapper so it exercises the host-call panic isolation
		// (caught here, routed as a normal task error), scoped to this task name.
		if e.seams.IsFault(faultPanicExecuteTask, taskName) {
			panic("injected fault: " + faultPanicExecuteTask + " " + taskName)
		}
		return e.host.ExecuteTask(taskCtx, dispatchURL, &flow.Flow)
	})
	e.workersInTask.Add(-1)
	if execErr == nil && e.seams.IsFault(faultExecuteTask, taskName) {
		execErr = errors.New("injected fault: "+faultExecuteTask+" "+taskName, http.StatusInternalServerError)
	}
	recordSpanError(taskSpan, execErr)

	var resultFlow *workflow.RawFlow
	errorRouted := false
	var errorTarget string

	if execErr != nil {
		// The engine never inspects status codes or error text: a task that wants to back off (rate limit,
		// transient unavailability) reads its own signal and arms flow.Retry. Any error that reaches here is
		// terminal for this attempt - routed via the graph's onError transition if one exists, else it fails
		// the step.
		if tr, ok := graph.ErrorTransition(taskName); ok {
			e.logger.DebugContext(ctx, "Task error routed", "task", taskName, "workflow", workflowURL, "error", execErr)
			tracedErr := errors.Convert(execErr)
			// Persist the error into flow state WITHOUT its stack frames. onErr rides into changes ->
			// final_state and is readable by any flow reader (History/Snapshot/List), and internal stack
			// traces are code-structure disclosure. The onError handler still gets the message, status
			// code, trace id and properties for routing - only the frames are dropped. Shallow-copy so the
			// shared error object keeps its stack (e.g. for the debug log above / a later failStep).
			redactedErr := *tracedErr
			redactedErr.Stack = nil
			// A FRESH flow, deliberately: an error voids the task's changes. Whatever it wrote before
			// returning the error is dropped rather than carried to the handler - execution is at-least-once,
			// so a lease-lost re-run recomputes those changes and a half-written attempt is not a fact
			// anything can be built on. failStep does the same (it writes status/error only), so the contract
			// does not turn on whether the author declared a handler. Do not "restore" rawChanges here.
			resultFlow = workflow.NewRawFlow()
			resultFlow.SetRawState(state)
			resultFlow.Set("onErr", &redactedErr)
			errorRouted = true
			errorTarget = tr.To
		} else {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, execErr, taskName)
			return errors.Trace(err)
		}
	} else {
		// Re-read the flow's changes after execution: the task executor wrote to the Flow
		// directly (it receives *workflow.Flow, which is the embedded Flow inside our RawFlow).
		resultFlow = flow
	}

	// Accumulate changes
	var accumulatedChanges map[string]any
	var changesJSON []byte
	rawChanges := resultFlow.RawChanges()
	if len(rawChanges) == 0 {
		accumulatedChanges = priorChanges
		changesJSON = []byte(priorChangesJSON)
	} else {
		// Overlay, not MergeState: this builds the persisted changes delta, where a cleared (null) entry is
		// a pending-delete marker that must survive. It only takes effect (drops the key) when changes later
		// fold onto state via MergeState.
		accumulatedChanges = make(map[string]any, len(priorChanges)+len(rawChanges))
		maps.Copy(accumulatedChanges, priorChanges)
		maps.Copy(accumulatedChanges, rawChanges)
		changesJSON, _ = json.Marshal(accumulatedChanges)
	}

	// Competing signals check
	{
		signalCount := 0
		if _, interrupted := resultFlow.InterruptRequested(); interrupted {
			signalCount++
		}
		if _, _, _, retryRequested := resultFlow.RetryRequested(); retryRequested {
			signalCount++
		}
		if resultFlow.GotoRequested() != "" {
			signalCount++
		}
		if _, _, ok := resultFlow.SubgraphRequested(); ok {
			signalCount++
		}
		if signalCount > 1 {
			err = errors.New("task '%s' set multiple competing control signals", taskName)
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
	}

	// Single-park guard
	{
		_, interruptArmed := resultFlow.InterruptRequested()
		_, _, subgraphArmed := resultFlow.SubgraphRequested()
		if (interruptArmed || subgraphArmed) && (interruptDone || subgraphDone) {
			err = errors.New("task '%s' armed a second park on an already-resolved step", taskName)
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
	}

	// Handle interrupt
	if interruptPayload, interrupted := resultFlow.InterruptRequested(); interrupted {
		e.logger.DebugContext(ctx, "Task interrupted", "task", taskName, "workflow", workflowURL)
		e.metricStepExecuted(ctx, taskName, workflow.StatusInterrupted)
		return e.handleInterrupt(ctx, shardNum, db, stepID, leaseSeq, flowID, flowToken, workflowURL, changesJSON, interruptPayload)
	}

	// Handle subgraph
	if subgraphURL, subgraphInput, subgraphRequested := resultFlow.SubgraphRequested(); subgraphRequested {
		e.logger.DebugContext(ctx, "Task requested subgraph", "task", taskName, "workflow", workflowURL, "subgraph", subgraphURL)
		// Bound the subgraph LoadGraph by the caller flow's budget (the create-time LoadGraph uses the caller's ctx).
		loadCtx, loadCancel := context.WithTimeout(workflow.ContextWithBaggage(ctx, baggage), time.Duration(flowTimeBudgetMs)*time.Millisecond)
		var subgraphGraph *workflow.Graph
		lerr := errors.CatchPanic(func() error {
			var e2 error
			subgraphGraph, e2 = e.host.LoadGraph(loadCtx, subgraphURL)
			return e2
		})
		if lerr == nil && e.seams.IsFault(faultLoadGraph, subgraphURL) {
			lerr = errors.New("injected fault: "+faultLoadGraph+" "+subgraphURL, http.StatusInternalServerError)
		}
		loadCancel() // a panic here fails the step like any LoadGraph error rather than wedging it
		if lerr != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, lerr, taskName)
			return errors.Trace(err)
		}
		// Same create-time guarantees for a subgraph child: reject a nil or structurally invalid child graph
		// (failing the caller step like any LoadGraph error). Validation is pure; the child's fan-in map is
		// derived per flow at dispatch (internal/faninmap), not frozen into its JSON.
		if subgraphGraph == nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken,
				errors.New("subgraph graph not found: %s", subgraphURL, http.StatusNotFound), taskName)
			return errors.Trace(err)
		}
		if verr := subgraphGraph.Validate(); verr != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken,
				errors.New("invalid subgraph graph %s: %v", subgraphURL, verr, http.StatusBadRequest), taskName)
			return errors.Trace(err)
		}
		// Persist the task's changes AND park the caller step in one UPDATE, BEFORE the child flow is made
		// dispatchable by start below. The ordering is load-bearing: completeSurgraphFlow revives this
		// caller only WHERE parked=parkedSubgraph, and a parkedSubgraph step is excluded from lease recovery,
		// so if the child completes and runs that revive before a later park lands, the no-op revive loses
		// the wakeup and the caller is stranded permanently - its fan-in then never fires and the flow hangs.
		// Observed deterministically when the caller is one of several fan-out siblings (the workers stay busy
		// so the child wins the race), e.g. examples/creditflow's identity-verification branch. The
		// status=running guard parks no row (n==0) if the step was concurrently cancelled; the error is
		// checked so a lost park fails the step rather than stranding it.
		parkRes, err := db.ExecContext(ctx,
			"UPDATE dwarf_steps SET changes=?, parked=?, updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND lease_seq=?",
			string(changesJSON), parkedSubgraph, stepID, leaseSeq,
		)
		if err != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		if n, _ := parkRes.RowsAffected(); n == 0 {
			return nil
		}
		e.metricStateWriteBytes(ctx, workflowURL, "changes", len(changesJSON))
		// Test checkpoint: a breakpoint here freezes the worker after the caller step is parked but before the
		// child flow is inserted, so a test can Cancel the tree in exactly the window that produces an orphaned
		// subgraph child (the recoverOrphanedSubgraphChildren case).
		e.seams.Checkpoint(ctx, checkpointAfterCallerPark)
		childInputState := subgraphInput
		if childInputState == nil {
			childInputState = map[string]any{}
		}
		// The caller step's span is still live on taskCtx; parent the subgraph's "workflow" span under it
		// so the subgraph subtree nests beneath this task in the trace.
		callerTraceParent := extractTraceParent(taskCtx)
		// createSubgraphFlow inserts the child already surgraph-linked and running, so no separate start.
		_, err = e.createSubgraphFlow(ctx, shardNum, flowID, stepDepth, stepID, subgraphURL, subgraphGraph, childInputState, baggageJSON, callerTraceParent)
		if err != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		e.metricStepExecuted(ctx, taskName, "subgraph")
		return nil
	}

	sleepDur := resultFlow.SleepRequested()

	// Handle retry
	if initialDelay, multiplier, maxDelay, retryRequested := resultFlow.RetryRequested(); retryRequested {
		e.logger.DebugContext(ctx, "Task retried", "task", taskName, "workflow", workflowURL, "step", stepID, "attempt", attempt)
		// Sleep is the floor and the backoff adds on top: total = Sleep + min(backoff, maxDelay). This lets a
		// task set a precise wait (e.g. a downstream's Retry-After via Sleep) and still get exponential backoff
		// on repeated attempts. maxDelay caps the backoff component, not the total.
		retrySleepMs := sleepDur.Milliseconds()
		{
			// Clamp in float space, and stop multiplying at the cap, so delay never overflows int64 ns.
			delay := float64(initialDelay)
			if multiplier > 0 {
				for range attempt {
					if maxDelay > 0 && delay >= float64(maxDelay) {
						break
					}
					delay *= multiplier
				}
			}
			if maxDelay > 0 && delay > float64(maxDelay) {
				delay = float64(maxDelay)
			}
			retrySleepMs += time.Duration(delay).Milliseconds()
		}
		// A retry rewinds this step in place and clears its subgraph park slot, so on
		// re-dispatch flow.Subgraph re-arms and spawns a *fresh* child. The prior attempt's
		// child (always terminal by now - the park only resolves on a terminal child) must be
		// reaped, recursively, in the same transaction as the rewind: leaving it dangling makes
		// the execution DAG claim two paths (X -> iter1 -> iter2 -> Y) when the model is
		// single-path, and lets history attach the discarded child's subtree to this caller.
		// The reap is step-scoped (only this caller's children), so a retrying fan-out sibling's
		// cohort is untouched.
		// The rewind is guarded to status='running': a Cancel landing mid-task flips this step terminal
		// (cancelled), and an unguarded rewind would both revive an immutable terminal step and, via the reap,
		// delete the now-terminal tree's subgraph children. So rewind first under the guard and reap (and
		// re-dispatch) only when it actually rewound a still-running step; a lost guard (n==0) leaves the step
		// terminal - the Cancel already cancelled its children - and returns without reaping or re-dispatching.
		// Test checkpoint: a breakpoint here freezes the worker before the rewind, so a test can Cancel the flow
		// (terminalizing this running step) in exactly the window the status='running' rewind guard protects.
		e.seams.Checkpoint(ctx, checkpointBeforeRetryRewind)
		var rewound bool
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			res, execErr := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, changes=?, attempt=?, not_before=DATE_ADD_MILLIS(NOW_UTC(), ?), lease_expires=NOW_UTC(), updated_at=NOW_UTC(), interrupt_done=0, resume_data='{}', subgraph_done=0, subgraph_result='{}', subgraph_error='' WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND lease_seq=?",
				workflow.StatusPending, string(changesJSON), attempt+1, retrySleepMs, stepID, leaseSeq,
			)
			if execErr != nil {
				return errors.Trace(execErr)
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return nil
			}
			rewound = true
			return errors.Trace(e.deleteSubgraphFlowsRootedAt(ctx, tx, stepID))
		})
		if err != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		if !rewound {
			return nil
		}
		e.metricStateWriteBytes(ctx, workflowURL, "changes", len(changesJSON))
		if retrySleepMs > 0 {
			e.shortenNextPoll(time.Now().Add(time.Duration(retrySleepMs) * time.Millisecond))
		} else {
			// The step was reset in place (same row, same denormalized priority) due now.
			e.enqueueStepDue(ctx, shardNum, stepID, flowPriority)
		}
		e.metricStepExecuted(ctx, taskName, "retried")
		return nil
	}

	// Complete the step
	if errorRouted {
		e.logger.DebugContext(ctx, "Task error routed", "task", taskName, "workflow", workflowURL)
		e.metricStepExecuted(ctx, taskName, "error_routed")
	} else {
		e.logger.DebugContext(ctx, "Task completed", "task", taskName, "workflow", workflowURL)
		e.metricStepExecuted(ctx, taskName, workflow.StatusCompleted)
	}
	// faultLeaseStaleWrite makes this completion write carry a stale lease generation, exactly as a zombie
	// worker (whose lease was re-granted to a peer) would. The fence must reject it (zero rows -> benign
	// no-op below), so the step stays claimable and lease recovery re-runs it cleanly - the test proves a
	// late/slow worker's write can never corrupt or terminalize a flow a peer is healthily re-executing.
	writeSeq := leaseSeq
	if e.seams.IsFault(faultLeaseStaleWrite, taskName) {
		writeSeq = leaseSeq - 1
	}
	// THE task has already run - its side effects have fired - so this write is the only record that it did.
	// A database error here must therefore retry the WRITE, never the task (see persist): an ephemeral blip is
	// absorbed with zero re-execution, and a write that will never land is classified and terminalized rather
	// than left to lease recovery, which would re-execute the task every `budget + leaseMargin`, forever.
	var stepRowsAffected int64
	err = e.persist(ctx, db, shardNum, stepID, leaseSeq, func() error {
		// faultPersistErr makes this write fail with a synthetic NON-contention error, consumed per attempt -
		// so InjectN(1) is a transient blip the retry must absorb with NO re-execution, and InjectN(large) is a
		// permanent failure the classifier must terminalize rather than loop on.
		if e.seams.IsFault(faultPersistErr, taskName) {
			return errors.New("injected fault: " + faultPersistErr + " " + taskName)
		}
		res, werr := db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, changes=?, updated_at=NOW_UTC() WHERE step_id=? AND status!='"+workflow.StatusCancelled+"' AND lease_seq=?",
			workflow.StatusCompleted, string(changesJSON), stepID, writeSeq,
		)
		if werr != nil {
			return errors.Trace(werr)
		}
		stepRowsAffected, _ = res.RowsAffected()
		return nil
	})
	switch {
	case errors.Is(err, errPersistFenced), errors.Is(err, errPersistDrained):
		return nil // a peer owns the step now; abandon it silently
	case sequel.IsLockContentionError(err):
		// Contention is NOT a persistence failure and must never be classified as one: terminalizing on it
		// would kill live flows during a contention storm - exactly backwards. Transact already retried it to
		// exhaustion; hand it to the recovery defer, which rewinds the step and re-polls.
		return errors.Trace(err)
	case err != nil:
		return errors.Trace(e.failOnPersistError(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName))
	}
	if stepRowsAffected == 0 {
		return nil
	}
	stepMarkedComplete = true
	e.metricStateWriteBytes(ctx, workflowURL, "changes", len(changesJSON))

	// Evaluate transitions
	var nextTasks []nextStep
	if errorRouted {
		nextTasks = []nextStep{{taskName: errorTarget}}
	} else {
		nextTasks, err = evaluateTransitions(graph, taskName, resultFlow)
		if err != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
	}

	var realTasks []nextStep
	for _, t := range nextTasks {
		if t.taskName != "" && t.taskName != workflow.END {
			realTasks = append(realTasks, t)
		}
	}

	isFanOutSource := graph.IsFanOutSource(taskName)
	isPushTransition := isFanOutSource && !errorRouted && resultFlow.GotoRequested() == ""
	cohortSize := len(realTasks)

	// The remaining post-completion writes. Like the transition transaction, these run with the step already
	// `completed`, so a database error here would drive the recovery defer to rewind and re-dispatch - RE-EXECUTING
	// the task - and would do so forever if the error is permanent. Same class, same treatment: retry the write in
	// place, and classify it if it will not land (see persistStep).

	// A fan-out source can route STRAIGHT to its own fan-in with no branches - its forEach array was empty, or
	// the task steered there directly (Goto/onError/a `when` or `switch` edge), e.g. "if the array to fan out on
	// is empty, Goto the fan-in". In every such case the source IS the spawn, and the fan-in converges
	// immediately on the source's own state, exactly as an empty cohort would (fireFanInDirect). The source's
	// lineage_id carries through, so a NESTED source's direct fan-in stays a member of its outer cohort. This
	// must be detected regardless of edge kind: a Goto/onError makes isPushTransition false and a `when`/`switch`
	// leaves cohortSize at 1, so the plain cohort path below would otherwise mis-attribute the fan-in arrival -
	// the trunk case fails the not-in-a-cohort guard, and a nested source bumps arrivals on its OUTER spawn and
	// silently drops the branch. The shape is legal, not rejected.
	fanInOfSource := ""
	if isFanOutSource {
		fanInOfSource = cg.fanIn.For(taskName)
	}
	if isFanOutSource && fanInOfSource != "" && (cohortSize == 0 || (cohortSize == 1 && realTasks[0].taskName == fanInOfSource)) {
		return e.persistStep(ctx, db, shardNum, stepID, leaseSeq, flowID, flowToken, taskName, func() error {
			return e.fireFanInDirect(ctx, shardNum, db, flowID, stepID, stepDepth, lineageID, fanInOfSource, dispatchURLOf(graph, fanInOfSource), workflowURL, graph, sleepDur, flowPriority, flowFairnessKey, flowFairnessWeight, flowTimeBudgetMs)
		})
	}
	if isFanOutSource && cohortSize == 0 {
		// An empty forEach with NO fan-in to converge on (fanInOfSource == ""). Only a TRUNK source
		// (lineage_id == 0) may complete the flow; a fan-out source that is itself a cohort MEMBER (a nested
		// fan-out) would terminalize the flow while its outer siblings are still running. (Unreachable for a
		// validated graph - Validate requires a fan-out source to converge on a SetFanIn node - so this is
		// defense in depth for a graph frozen before that check.)
		if lineageID != 0 {
			return e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken,
				errors.New("fan-out source '%s' is inside a cohort but has no fan-in node to converge on", taskName),
				taskName)
		}
		return e.persistStep(ctx, db, shardNum, stepID, leaseSeq, flowID, flowToken, taskName, func() error {
			return e.completeFlowSequential(ctx, shardNum, flowID, flowToken, workflowURL)
		})
	}

	if cohortSize == 0 {
		// No matching transition. Only a TRUNK step (lineage_id == 0) completes the flow. A cohort MEMBER
		// (lineage_id != 0) that matched no onward transition has dead-ended before reaching its fan-in -
		// whether the dead end came from a `when` guard that evaluated false or a `switch`/`goto` with no
		// live target. Completing the flow here would terminalize it while sibling branches are still running
		// and skip the fan-in entirely, silently dropping every downstream task and the siblings' work. Fail
		// the branch loudly instead: every branch of a fan-out must reach a fan-in node. This mirrors the
		// fan-in-from-outside-a-cohort guard below, and a fan-in step is always trunk (its lineage_id is its
		// spawn source's OWN lineage, 0 for a top-level cohort), so a legitimate `<fan-in> -> END` still
		// completes here.
		if lineageID != 0 {
			return e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken,
				errors.New("task '%s' is inside a fan-out cohort but matched no onward transition; every branch must reach a fan-in node", taskName),
				taskName)
		}
		return e.persistStep(ctx, db, shardNum, stepID, leaseSeq, flowID, flowToken, taskName, func() error {
			return e.completeFlowSequential(ctx, shardNum, flowID, flowToken, workflowURL)
		})
	}

	cohortSpawnID := lineageID
	childLineageID := lineageID
	if isPushTransition {
		cohortSpawnID = stepID
		childLineageID = stepID
	}

	var normalNexts []nextStep
	var fanInTaskName string
	fanInArrivals := 0
	for _, next := range realTasks {
		if graph.IsFanIn(next.taskName) {
			fanInTaskName = next.taskName
			fanInArrivals++
		} else {
			normalNexts = append(normalNexts, next)
		}
	}

	// A fan-in arrival must have a cohort to arrive at. cohortSpawnID is the dispatched step's lineage_id,
	// which is 0 for any step OUTSIDE a fan-out cohort - so a trunk step routed into a fan-in node would
	// bump cohort_arrivals on step_id=0 (zero rows, no error) and then SELECT that step: sql.ErrNoRows,
	// which aborts the transition transaction. The recovery defer then rewinds the just-completed step to
	// pending, it re-dispatches, the task RE-RUNS - side effects and all - and it fails identically. An
	// unbounded hot loop that hammers the database and never advances the flow.
	//
	// A fan-out source routing to its OWN fan-in is legal and was already handled above (fireFanInDirect) -
	// it never reaches here. What remains is a step with no cohort of its own reaching a fan-in: a trunk
	// non-source step routed into a fan-in node (validateLineage rejects it, so a graph validated today
	// cannot produce it; the guard covers a graph frozen onto a flow BEFORE that check, replayed unvalidated
	// on every dispatch). Failing the step is the honest outcome - the flow terminates with a clear error
	// instead of hot-looping forever.
	if fanInArrivals > 0 && cohortSpawnID == 0 {
		return e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken,
			errors.New("task '%s' is routed to fan-in node '%s' but is not part of a fan-out cohort", taskName, fanInTaskName),
			taskName)
	}

	childInputState, _ := workflow.MergeState(state, accumulatedChanges, nil)
	// Mint state refs for the successors: a field big enough to be worth an anchor is omitted from their
	// `state` and recorded in `state_refs` instead, pointing at THIS step (which holds the bytes in its
	// `changes` if the task just wrote them, or in its `state` if it merely carried them). A field that
	// arrived here as a ref keeps that ref. The bar scales with the fan-out width - see mintStateRefs.
	//
	// The linear/static-fan-out successors all share this one snapshot, so it is minted once; a forEach
	// branch re-mints because its state carries the injected element (below).
	linearStateJSON, linearRefsJSON, err := mintStateRefs(childInputState, accumulatedChanges, inheritedRefs, stepID, len(normalNexts), nil)
	if err != nil {
		err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
		return errors.Trace(err)
	}
	nextStepDepth := stepDepth + 1
	sleepMs := sleepDur.Milliseconds()

	var newStepIDs []int
	flowFailed := false
	// State bytes moved by this transition (successor snapshots, fan-in merge), accumulated in the
	// closure and emitted only after commit (see stateByteCount).
	var txBytes stateByteCount
	// When the failing flow is a subgraph child, its failure is delivered to the parent's flow.Subgraph
	// call rather than notified directly (see failStep). Captured in the transaction, acted on after it.
	flowFailedParentStepID := 0
	flowFailedReDispatchParent := false
	flowFailedAwaited := false

	// Test checkpoint: a breakpoint here freezes the worker after the step is marked completed but before
	// the transition transaction, so a test can Cancel the flow in exactly the window the transition's
	// write-first terminal-status guard exists to survive (the transition must become a no-op, inserting no
	// successors into the cancelled flow).
	e.seams.Checkpoint(ctx, checkpointBeforeTransitionTx)

	// The transition (insert next steps, then advance or fail the flow) runs as one retryable
	// transaction. Under pessimistic locking it can deadlock with a concurrent worker, and a deadlocked
	// attempt MUST re-run rather than leave the just-completed step with no successor — which would wedge
	// the flow, since a completed step is not lease-recoverable. Transact rolls back and re-runs the
	// closure (re-reading the fan-in counts so the decision stays correct), and its Tx records any
	// statement error so a partial transition can never commit.
	// The step is already `completed` at this point, so a database error here is the OTHER half of the eternal
	// loop: the recovery defer rewinds it to pending and the task RE-EXECUTES. Retry the transaction in place
	// first (Transact re-runs its own closure, which re-derives every value it writes), and if it will not land,
	// classify it rather than spin - see persist / failOnPersistError.
	err = e.persist(ctx, db, shardNum, stepID, leaseSeq, func() error {
		return db.Transact(ctx, func(tx *sequel.Tx) error {
			newStepIDs = newStepIDs[:0]
			flowFailed = false
			txBytes = stateByteCount{}
			flowFailedParentStepID, flowFailedReDispatchParent = 0, false
			flowFailedAwaited = false

			// faultContention returns a retryable lock-contention error (consumed on the first attempt), so the
			// test proves Transact rolls back and re-runs the closure to a clean commit. faultTransitionCommit
			// returns a non-retryable error, so the tx fails after the step was already marked completed and the
			// processStep recovery defer must reset it (completed->pending) and re-dispatch. Both are scoped to
			// the completing task and checked before the flow-row write, so a fired fault rolls back nothing.
			if e.seams.IsFault(faultContention, taskName) {
				return errors.New("database is locked (injected fault: " + faultContention + " " + taskName + ")")
			}
			if e.seams.IsFault(faultTransitionCommit, taskName) {
				return errors.New("injected fault: " + faultTransitionCommit + " " + taskName)
			}

			// Write-first: take the flow row's lock before any step, guarded on non-terminal status. If a
			// concurrent Cancel/failStep terminalized this flow after the step was marked completed but before
			// this transition committed (the Cancel-vs-transition window, and the retry after a lock-contention
			// rollback), the guard yields zero rows and the transition becomes a clean no-op. Without it, the tx
			// would insert pending successors into an already-terminal flow — orphan work only reaped later by the
			// claim-time terminal-flow guard. The completed step is left as a harmless tail on the final flow.
			// The lock-grab flips the non-indexed `touch` column, not `updated_at`: a running flow's
			// `updated_at` now moves only on a genuine status transition, so the running band of
			// idx_dwarf_flows_status is not churned once per step. `touch` always changes value, so
			// RowsAffected still reflects the WHERE match (the terminal guard below) on every driver.
			flowRes, flowErr := tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
				flowID,
			)
			if flowErr != nil {
				return errors.Trace(flowErr)
			}
			if n, _ := flowRes.RowsAffected(); n == 0 {
				return nil
			}

			for i, next := range normalNexts {
				stepStateJSON, stepRefsJSON := linearStateJSON, linearRefsJSON
				if next.item != nil {
					// A forEach branch's state is the flow state plus its element and ordinal context. The
					// source array is NOT stripped: the engine once deleted it from each branch's local state
					// to avoid N copies of it in every step row of every branch, but that was a byte
					// optimization for the one field the engine happens to know the name of, while every other
					// carried field paid the same cost unaddressed. It also made a branch's state a LIE - the
					// branch could not see the array its own element came from - and it is what made a failed
					// fan-out's final_state come back missing the array (a completed sibling's branch-local
					// snapshot is the merge base there). The general mechanism that owns the cost is state refs:
					// the array is REF'd, so the branches share one copy of it and each still sees it whole.
					perStepState := make(map[string]any, len(childInputState)+3)
					maps.Copy(perStepState, childInputState)
					perStepState[next.itemKey] = next.item
					if next.forEachKey != "" {
						perStepState[next.itemKey+"Index"] = next.cohortIndex
						perStepState[next.itemKey+"Count"] = next.cohortCount
					}
					// The injected element and its ordinal context are SYNTHESIZED per branch - their bytes are
					// in no step row at all - so they can never be ref'd; a ref to them would dangle. Everything
					// else in the branch's state comes from this step and re-mints exactly as the linear case.
					noRef := map[string]bool{next.itemKey: true}
					if next.forEachKey != "" {
						noRef[next.itemKey+"Index"] = true
						noRef[next.itemKey+"Count"] = true
					}
					var mintErr error
					stepStateJSON, stepRefsJSON, mintErr = mintStateRefs(perStepState, accumulatedChanges, inheritedRefs, stepID, len(normalNexts), noRef)
					if mintErr != nil {
						return errors.Trace(mintErr)
					}
				}
				nextURL := dispatchURLOf(graph, next.taskName)
				newStepID, err := tx.InsertReturnID(ctx, "step_id",
					"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_refs, status, parked, time_budget_ms, lineage_id, fan_out_ordinal, predecessor_id, not_before, priority, fairness_key, fairness_weight, engine_id)"+
						" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?)",
					flowID, nextStepDepth, keys.RandomIdentifier(16), next.taskName, nextURL, stepStateJSON, stepRefsJSON, workflow.StatusPending, parkedNone, flowTimeBudgetMs, childLineageID, i, stepID, sleepMs, flowPriority, flowFairnessKey, flowFairnessWeight, e.engineID,
				)
				if err != nil {
					return errors.Trace(err)
				}
				txBytes.stateWritten += len(stepStateJSON)
				newStepIDs = append(newStepIDs, int(newStepID))
			}

			if len(newStepIDs) > 0 {
				tx.ExecContext(ctx, "UPDATE dwarf_steps SET successor_id=? WHERE step_id=?", newStepIDs[0], stepID)
			}

			if isPushTransition {
				tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_size=? WHERE step_id=?", cohortSize, stepID)
				e.logger.DebugContext(ctx, "Fan-out cohort spawned", "flow", keys.CorrelationID(shardNum, flowID), "spawnStep", stepID, "task", taskName, "cohortSize", cohortSize)
			}

			if fanInArrivals > 0 {
				tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals + ? WHERE step_id=?", fanInArrivals, cohortSpawnID)
				var arrivals, size, failures, spawnLineageID int
				err := tx.QueryRowContext(ctx,
					"SELECT cohort_arrivals, cohort_size, cohort_failures, lineage_id FROM dwarf_steps WHERE step_id=?",
					cohortSpawnID,
				).Scan(&arrivals, &size, &failures, &spawnLineageID)
				if err != nil {
					return errors.Trace(err)
				}
				fullyResolved := size > 0 && arrivals >= size
				if fullyResolved && failures == 0 {
					fanInStepID, fanInBytes, err := e.insertFanInStep(ctx, tx, shardNum, flowID, nextStepDepth, cohortSpawnID, stepID, fanInTaskName, graph, workflowURL, sleepMs, flowPriority, flowFairnessKey, flowFairnessWeight, flowTimeBudgetMs)
					if err != nil {
						return errors.Trace(err)
					}
					txBytes.stateWritten += fanInBytes.stateWritten
					txBytes.stateRead += fanInBytes.stateRead
					txBytes.changesRead += fanInBytes.changesRead
					newStepIDs = append(newStepIDs, fanInStepID)
				} else if fullyResolved && failures > 0 {
					failFlow := spawnLineageID == 0
					if !failFlow {
						var pcfErr error
						failFlow, pcfErr = e.propagateCohortFailure(ctx, tx, spawnLineageID)
						if pcfErr != nil {
							return errors.Trace(pcfErr)
						}
					}
					if failFlow {
						var sampleErr string
						tx.QueryRowContext(ctx,
							"SELECT error FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusFailed+"' AND error!='' ORDER BY step_id LIMIT_OFFSET(1, 0)",
							flowID,
						).Scan(&sampleErr)
						sampleErr = strings.TrimSpace(sampleErr)
						if sampleErr == "" {
							sampleErr = "cohort failed"
						}
						finalStateJSON, _, cfsErr := e.computeFinalState(ctx, tx, shardNum, flowID)
						if cfsErr != nil {
							return errors.Trace(cfsErr)
						}
						tx.ExecContext(ctx,
							"UPDATE dwarf_flows SET final_state=?, status=?, error=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
							finalStateJSON, workflow.StatusFailed, sampleErr, flowID,
						)
						flowFailed = true
						// The cohort fully resolved here (this branch completed last), so every sibling has
						// settled - nothing is stranded. If this flow is a subgraph child, deliver the failure
						// to the parked parent caller step rather than notifying directly.
						var parentStepID int
						tx.QueryRowContext(ctx, "SELECT surgraph_step_id, awaited FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&parentStepID, &flowFailedAwaited)
						if parentStepID != 0 {
							rd, derr := e.deliverFlowFailureToParent(ctx, tx, parentStepID, sampleErr)
							if derr != nil {
								return errors.Trace(derr)
							}
							flowFailedParentStepID = parentStepID
							flowFailedReDispatchParent = rd
						}
					}
				}
			}

			nextFlowStepID := 0
			if len(newStepIDs) == 1 {
				nextFlowStepID = newStepIDs[0]
			}
			if !flowFailed {
				tx.ExecContext(ctx, "UPDATE dwarf_flows SET step_id=?, touch=1-touch WHERE flow_id=?", nextFlowStepID, flowID)
			}
			return nil
		})
	})
	switch {
	case errors.Is(err, errPersistFenced), errors.Is(err, errPersistDrained):
		return nil // a peer owns the step now; abandon it silently
	case sequel.IsLockContentionError(err):
		// Contention is NOT a persistence failure and must never be classified as one: terminalizing on it
		// would kill live flows during a contention storm - exactly backwards. Transact already retried it to
		// exhaustion; hand it to the recovery defer, which rewinds the step and re-polls.
		return errors.Trace(err)
	case err != nil:
		return errors.Trace(e.failOnPersistError(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName))
	}
	e.metricStateWriteBytes(ctx, workflowURL, "state", txBytes.stateWritten)
	e.metricStateReadBytes(ctx, workflowURL, "state", txBytes.stateRead)
	e.metricStateReadBytes(ctx, workflowURL, "changes", txBytes.changesRead)

	if flowFailed {
		if flowFailedParentStepID != 0 {
			// Subgraph child: the failure is delivered to the parent's flow.Subgraph call - but the child has
			// still stopped, so wake any Await on the child key (legal read-only introspection), mirroring
			// failStep's subgraph-child branch. Without this, an Await(childKey) on a child failed by its
			// completing last arriver (this path) would idle until the lost-wake poll backstop.
			e.signalStop(ctx, fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken), workflow.StatusFailed, flowFailedAwaited)
			if flowFailedReDispatchParent {
				e.enqueueStep(ctx, shardNum, flowFailedParentStepID)
			}
			return nil
		}
		compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)
		e.signalStop(ctx, compositeID, workflow.StatusFailed, flowFailedAwaited)
		return nil
	}

	if sleepDur > 0 {
		e.shortenNextPoll(time.Now().Add(sleepDur))
	} else if len(newStepIDs) > 0 {
		// Priority is the flow's, just bound into the successor INSERTs; the sleep branch diverged above.
		e.enqueueStepDue(ctx, shardNum, newStepIDs[0], flowPriority)
	}
	return nil
}

// errLeaseFenced is an in-transaction sentinel: a post-execution write found the dispatch's lease had
// been re-granted to a peer, so the transaction must roll back and the worker must abandon quietly. It is
// never surfaced - callers detect it via a captured `fenced` bool and return nil. See "Lease fencing".
var errLeaseFenced = errors.New("dispatch lease fenced")

// handleInterrupt pauses a flow for external input.
func (e *Engine) handleInterrupt(ctx context.Context, shardNum int, db *sequel.DB, stepID, leaseSeq int, flowID int, flowToken string, workflowURL string, changesJSON []byte, interruptPayload map[string]any) error {
	chainFlowIDs, chainStepIDs, chainCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	fenced := false
	payloadLen := 0
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		fenced = false
		payloadLen = 0
		// Steps-first-then-flow lock ordering, matching resume and Cancel (which walk this same surgraph
		// chain). Interrupt is non-terminating, so it carries no write-first orphan obligation; the only
		// requirement is that the first statement be a write (satisfied here, keeping SQLite's shared-lock
		// upgrade deadlock closed). Ordering steps before flows removes the cycle with a concurrent
		// resume, which holds the chain step rows and wants the chain flow rows: acquiring in the same order
		// on both sides means one blocks rather than deadlocks. The flow-first transition/completion cluster
		// (advanceFlow, completeFlow) is unaffected — it shares only the single flow row with this path (it
		// never locks a sibling's step row), and one shared resource cannot cycle.
		allStepIDs := append([]any{stepID}, chainStepIDs...)
		stepPlaceholders := strings.Repeat("?,", len(allStepIDs)-1) + "?"
		stepArgs := []any{stepID, string(changesJSON), stepID, workflow.StatusInterrupted, parkedNone}
		stepArgs = append(stepArgs, allStepIDs...)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET changes=CASE WHEN step_id=? THEN ? ELSE changes END, interrupt_done=CASE WHEN step_id=? THEN 1 ELSE interrupt_done END, status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN ("+stepPlaceholders+") AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')",
			stepArgs...,
		)

		// Lease fence. The combined UPDATE above locked and re-parked the whole chain (in PK order, before
		// any flow row - the ordering the comment guards), but a zombie whose leaf lease was re-granted to a
		// peer must not be allowed to interrupt the chain: flipping the ancestor callers out of
		// parkedSubgraph would strand the parent's revive. The combined UPDATE leaves lease_seq untouched, so
		// a peer's re-claim shows up here as a bumped generation on the leaf; on mismatch we roll the entire
		// interrupt back (undoing the ancestor re-park) and abandon. The check reads the leaf inside the tx
		// (not a same-table subquery, which MySQL rejects on an UPDATE target) after the lock is held. A leaf
		// merely reset to pending by lease recovery keeps our generation (recovery does not bump lease_seq),
		// so the benign before-a-peer-claims case still proceeds and the peer re-does it correctly on claim.
		var curLeaseSeq int
		if serr := tx.QueryRowContext(ctx, "SELECT lease_seq FROM dwarf_steps WHERE step_id=?", stepID).Scan(&curLeaseSeq); serr != nil {
			return errors.Trace(serr)
		}
		// faultInterruptStaleWrite forces the generation to look re-granted (as a real zombie would, after a peer
		// re-claimed the leaf), so the fence trips and the WHOLE transaction rolls back - undoing the ancestor
		// re-park - and the worker abandons. The only fence in the engine that rolls back rather than no-ops.
		if e.seams.IsFault(faultInterruptStaleWrite) {
			curLeaseSeq = leaseSeq + 1
		}
		if curLeaseSeq != leaseSeq {
			fenced = true
			return errLeaseFenced
		}

		if len(interruptPayload) > 0 {
			payloadJSON, _ := json.Marshal(interruptPayload)
			payloadLen = len(payloadJSON)
			payloadArgs := []any{string(payloadJSON)}
			payloadArgs = append(payloadArgs, allStepIDs...)
			// Guard: write the payload only to chain steps still at the default empty object, so a
			// concurrent fan-out interrupt does not clobber a payload already set on a shared ancestor
			// (first-writer-wins). MySQL's JSON column does not match a bare string literal with '=',
			// so interrupt_payload='{}' silently matches nothing there; compare its textual form. The
			// TEXT/JSONB/NVARCHAR columns on the other dialects match the literal directly.
			emptyGuard := "interrupt_payload='{}'"
			if db.DriverName() == "mysql" {
				emptyGuard = "CAST(interrupt_payload AS CHAR)='{}'"
			}
			tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET interrupt_payload=? WHERE step_id IN ("+stepPlaceholders+") AND "+emptyGuard,
				payloadArgs...,
			)
		}

		flowPlaceholders := strings.Repeat("?,", len(chainFlowIDs)-1) + "?"
		flowArgs := append([]any{workflow.StatusInterrupted}, chainFlowIDs...)
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id IN ("+flowPlaceholders+") AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')",
			flowArgs...,
		)
		return nil
	})
	if err != nil {
		if fenced {
			// Our lease was re-granted to a peer mid-execution; the peer owns this step and re-runs the
			// interrupt. Abandon quietly - not an error (see "Lease fencing").
			return nil
		}
		return errors.Trace(err)
	}

	e.metricStateWriteBytes(ctx, workflowURL, "changes", len(changesJSON))
	e.metricStateWriteBytes(ctx, workflowURL, "interrupt_payload", payloadLen)
	awaitedSet := e.awaitedFlows(ctx, shardNum, chainFlowIDs)
	for i, compositeID := range chainCompositeIDs {
		e.signalStop(ctx, compositeID, workflow.StatusInterrupted, awaitedSet == nil || awaitedSet[chainFlowIDs[i].(int)])
	}
	return nil
}

// fireFanInDirect creates the fan-in step immediately for an empty-cohort case.
func (e *Engine) fireFanInDirect(ctx context.Context, shardNum int, db *sequel.DB, flowID int, stepID int, stepDepth int, lineageID int, fanInTarget, fanInURL string, workflowURL string, graph *workflow.Graph, sleepDur time.Duration, priority int, fairnessKey string, fairnessWeight float64, timeBudgetMs int) error {
	var fanInStepID int64
	var txBytes stateByteCount
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		fanInStepID = 0
		txBytes = stateByteCount{}
		// Write-first, guarded on non-terminal status - the same lock-grab the transition tx uses. If a
		// concurrent Cancel/failStep terminalized this flow, the guard yields zero rows and this becomes a
		// clean no-op; without it we would insert a pending fan-in step and overwrite step_id on a terminal
		// flow (orphan work reaped only later by the claim-time terminal-flow guard). `touch` always changes
		// value, so RowsAffected reflects the WHERE match on every driver.
		flowRes, flowErr := tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
			flowID,
		)
		if flowErr != nil {
			return errors.Trace(flowErr)
		}
		if n, _ := flowRes.RowsAffected(); n == 0 {
			return nil
		}
		tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_size=0 WHERE step_id=?", stepID)

		var ourStateJSON, ourChangesJSON, ourRefsJSON string
		tx.QueryRowContext(ctx, "SELECT state, changes, state_refs FROM dwarf_steps WHERE step_id=?", stepID).Scan(&ourStateJSON, &ourChangesJSON, &ourRefsJSON)
		txBytes.stateRead = len(ourStateJSON)
		txBytes.changesRead = len(ourChangesJSON)
		var ourState, ourChanges map[string]any
		unmarshalJSONMap(ourStateJSON, &ourState)
		unmarshalJSONMap(ourChangesJSON, &ourChanges)
		// Resolve only what the reducers actually fold; a merely-carried ref rides through the (empty) cohort
		// untouched, still pointing at its original anchor - see resolveReducedRefs.
		ourRefs := parseStateRefs(ourRefsJSON)
		_, rerr := e.resolveReducedRefs(ctx, tx, shardNum, ourState, ourRefs, graph.Reducers(), workflowURL)
		if rerr != nil {
			return errors.Trace(rerr)
		}
		mergedState, _ := workflow.MergeState(ourState, ourChanges, graph.Reducers())
		// This step IS the cohort spawn (an empty forEach spawns no branches), so it anchors exactly as a
		// populated cohort's spawn does in insertFanInStep. There are no members, but a field this step wrote
		// through a COMBINING reducer still has a merged value (reduce(ourState[k], ourChanges[k])) that exists
		// in no row, so it must be inlined rather than anchored - the same exclusion insertFanInStep applies.
		exclude := combinedReducerFields(ourState, ourChanges, graph.Reducers())
		mergedJSON, refsJSON, merr := mintStateRefs(mergedState, ourChanges, ourRefs, stepID, 1, exclude)
		if merr != nil {
			return errors.Trace(merr)
		}
		txBytes.stateWritten = len(mergedJSON)

		nextStepDepth := stepDepth + 1
		sleepMs := sleepDur.Milliseconds()
		var err error
		fanInStepID, err = tx.InsertReturnID(ctx, "step_id",
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_refs, status, parked, time_budget_ms, lineage_id, predecessor_id, not_before, priority, fairness_key, fairness_weight, engine_id)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?)",
			flowID, nextStepDepth, keys.RandomIdentifier(16), fanInTarget, fanInURL, mergedJSON, refsJSON, workflow.StatusPending, parkedNone, timeBudgetMs, lineageID, stepID, sleepMs, priority, fairnessKey, fairnessWeight, e.engineID,
		)
		if err != nil {
			return errors.Trace(err)
		}
		tx.ExecContext(ctx, "UPDATE dwarf_steps SET successor_id=? WHERE step_id=?", int(fanInStepID), stepID)
		tx.ExecContext(ctx, "UPDATE dwarf_flows SET step_id=?, touch=1-touch WHERE flow_id=?", int(fanInStepID), flowID)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if fanInStepID == 0 {
		// The terminal-status guard bailed - the flow was cancelled/failed concurrently, no fan-in step
		// was inserted, nothing to dispatch.
		return nil
	}
	e.metricStateWriteBytes(ctx, workflowURL, "state", txBytes.stateWritten)
	e.metricStateReadBytes(ctx, workflowURL, "state", txBytes.stateRead)
	e.metricStateReadBytes(ctx, workflowURL, "changes", txBytes.changesRead)

	if sleepDur > 0 {
		e.shortenNextPoll(time.Now().Add(sleepDur))
	} else {
		// Priority was just bound into the fan-in INSERT; the sleep branch diverged above.
		e.enqueueStepDue(ctx, shardNum, int(fanInStepID), priority)
	}
	return nil
}

// stateByteCount accumulates state/changes payload bytes moved by a transaction, emitted to the
// dwarf_state_*_bytes counters by the caller only after the transaction commits (a contention retry
// re-runs the closure, so inline emission would double-count).
type stateByteCount struct {
	stateWritten int // "state" column: snapshots written (successor, fan-in inserts)
	stateRead    int // "state" column: snapshots read (fan-in spawn/own state)
	changesRead  int // "changes" column: deltas read (fan-in spawn + cohort members)
}

// insertFanInStep creates the fan-in step after the cohort completes. It also returns the state bytes it
// moved (read: spawn snapshot + every cohort member's changes; written: the merged fan-in snapshot), which
// the caller emits after its transaction commits.
func (e *Engine) insertFanInStep(ctx context.Context, tx sequel.Executor, shardNum, flowID, nextStepDepth, cohortSpawnID, predecessorStepID int, fanInTaskName string, graph *workflow.Graph, workflowURL string, sleepMs int64, priority int, fairnessKey string, fairnessWeight float64, timeBudgetMs int) (int, stateByteCount, error) {
	var spawnStateJSON, spawnChangesJSON, spawnRefsJSON, spawnTaskName string
	var spawnLineageID int
	tx.QueryRowContext(ctx,
		"SELECT state, changes, state_refs, lineage_id, task_name FROM dwarf_steps WHERE step_id=?",
		cohortSpawnID,
	).Scan(&spawnStateJSON, &spawnChangesJSON, &spawnRefsJSON, &spawnLineageID, &spawnTaskName)
	bytes := stateByteCount{stateRead: len(spawnStateJSON), changesRead: len(spawnChangesJSON)}
	var spawnState, spawnChanges map[string]any
	unmarshalJSONMap(spawnStateJSON, &spawnState)
	unmarshalJSONMap(spawnChangesJSON, &spawnChanges)
	// Materialize only the ref'd fields a reducer will FOLD - a combining reducer (append/add/union/...) needs
	// its accumulated base, and folding a delta onto an absent base would silently lose everything so far. A
	// merely-carried ref is left alone and re-emitted onto the fan-in step below, still pointing at its
	// original anchor: resolving it here would materialize the payload and re-anchor it at EVERY fan-in,
	// giving back the win in exactly the fan-out graphs this design exists for.
	spawnRefs := parseStateRefs(spawnRefsJSON)
	_, err := e.resolveReducedRefs(ctx, tx, shardNum, spawnState, spawnRefs, graph.Reducers(), workflowURL)
	if err != nil {
		return 0, stateByteCount{}, errors.Trace(err)
	}
	merged, _ := workflow.MergeState(spawnState, spawnChanges, graph.Reducers())
	// Fields a cohort MEMBER contributed. Their bytes are in that member's `changes` - not in the spawn's row -
	// and a reducer's COMBINED output exists in no row at all, so neither can be anchored at the spawn. They are
	// inlined into the fan-in step's own state, which becomes their anchor for everything downstream (the third
	// of the three places an anchor's bytes can sit). A stale spawn ref for such a field is dropped with them.
	memberWrites := map[string]bool{}

	// The cohort-exit steps whose successor_id must point at the fan-in step. Collected from this same
	// cohort scan so the successor_id write can target them by primary key (step_id IN ...) below,
	// rather than re-scanning dwarf_steps by (flow_id, lineage_id, task_name) - that unindexed predicate
	// took an Update lock across the whole table and deadlocked two concurrent fan-ins on SQL Server.
	exitTaskSet := make(map[string]bool)
	for _, t := range fanInPredecessorTasks(graph, fanInTaskName) {
		exitTaskSet[t] = true
	}
	var exitStepIDs []int

	rows, err := tx.QueryContext(ctx,
		"SELECT step_id, task_name, status, changes, step_depth FROM dwarf_steps WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_id",
		flowID, cohortSpawnID,
	)
	if err != nil {
		return 0, stateByteCount{}, errors.Trace(err)
	}
	defer rows.Close()
	maxCohortDepth := 0
	for rows.Next() {
		var memberStepID int
		var memberTaskName, status, changesJSON string
		var depth int
		rows.Scan(&memberStepID, &memberTaskName, &status, &changesJSON, &depth)
		bytes.changesRead += len(changesJSON)
		if depth > maxCohortDepth {
			maxCohortDepth = depth
		}
		// Match the prior predicate's row set exactly: every cohort member with an exit task name,
		// regardless of status (the scan it replaces had no status filter).
		if exitTaskSet[strings.TrimSpace(memberTaskName)] {
			exitStepIDs = append(exitStepIDs, memberStepID)
		}
		status = strings.TrimSpace(status)
		if status != workflow.StatusCompleted {
			continue
		}
		var changes map[string]any
		unmarshalJSONMap(changesJSON, &changes)
		for k := range changes {
			memberWrites[k] = true
		}
		merged, _ = workflow.MergeState(merged, changes, graph.Reducers())
	}
	rows.Close()

	// The fan-in sits one level below the DEEPEST cohort branch, not merely below the last sibling to
	// complete - branch lengths can differ (loops, gotos, varying chains), so step_depth must reflect the
	// deepest path the flow took. nextStepDepth (last-completer+1) is the floor for the defensive empty case.
	fanInDepth := max(maxCohortDepth+1, nextStepDepth)

	// Drop the per-branch forEach bookkeeping of THIS cohort - the one spawnTaskName fanned out - and of no
	// other. An outer cohort's bookkeeping must survive an inner fan-in: a step converging out of the inner
	// cohort is still inside the outer branch and must still see which element that branch is working on. The
	// same scoped strip runs in computeFinalState for a fan-out that FAILED (and so never reached this
	// convergence), so the two paths agree on what a fan-out's state means once its cohort is behind it.
	stripForEachBookkeeping(merged, graph, strings.TrimSpace(spawnTaskName))

	// A field the SPAWN itself wrote through a COMBINING reducer (reduce(spawnState[k], spawnChanges[k])) is the
	// other value that exists in no row - the spawn's `changes` holds only its delta - so it too must be inlined,
	// not anchored at the spawn. memberWrites already excludes member-contributed fields; add the spawn's own
	// combined ones. (A spawn field with a combining reducer but no base has merged[k]==changes[k] and stays a
	// sound anchor, so combinedReducerFields leaves it off.)
	for k := range combinedReducerFields(spawnState, spawnChanges, graph.Reducers()) {
		memberWrites[k] = true
	}

	// Mint against the SPAWN step, not this one: a field the cohort merely carried has its bytes in the spawn's
	// row (its `state` if the spawn received it - e.g. the entry step holding the flow's initial input - or its
	// `changes` if the spawn's task wrote it), so the spawn is a legitimate anchor and the fan-in must not
	// re-copy the payload into its own row. A field that arrived at the spawn as a ref keeps that ref, so the
	// chain stays one hop. Member-contributed and reduced fields are excluded and therefore inline (above).
	mergedJSON, refsJSON, err := mintStateRefs(merged, spawnChanges, spawnRefs, cohortSpawnID, 1, memberWrites)
	if err != nil {
		return 0, stateByteCount{}, errors.Trace(err)
	}
	bytes.stateWritten = len(mergedJSON)
	fanInURL := dispatchURLOf(graph, fanInTaskName)
	fanInStepID, err := tx.InsertReturnID(ctx, "step_id",
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_refs, status, parked, time_budget_ms, lineage_id, predecessor_id, not_before, priority, fairness_key, fairness_weight, engine_id)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?)",
		flowID, fanInDepth, keys.RandomIdentifier(16), fanInTaskName, fanInURL, mergedJSON, refsJSON, workflow.StatusPending, parkedNone, timeBudgetMs, spawnLineageID, predecessorStepID, sleepMs, priority, fairnessKey, fairnessWeight, e.engineID,
	)
	if err != nil {
		return 0, stateByteCount{}, errors.Trace(err)
	}

	// Record the cohort-exit -> fan-in edges, targeting the exit steps by primary key (collected above)
	// so the write locks only those rows. The previous (flow_id, lineage_id, task_name IN ...) predicate
	// had no supporting index and scan-locked dwarf_steps, deadlocking concurrent fan-ins on SQL Server.
	if len(exitStepIDs) > 0 {
		placeholders := strings.Repeat("?,", len(exitStepIDs)-1) + "?"
		args := []any{int(fanInStepID)}
		for _, id := range exitStepIDs {
			args = append(args, id)
		}
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET successor_id=? WHERE step_id IN ("+placeholders+")",
			args...,
		)
	}
	return int(fanInStepID), bytes, nil
}

// dispatchURLOf resolves a graph node name to its dispatch URL.
func dispatchURLOf(graph *workflow.Graph, name string) string {
	if name == workflow.END {
		return name
	}
	if u := graph.URLOf(name); u != "" {
		return u
	}
	return name
}
