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
			e.checkpoint(ctx, checkpointBeforeRecoveryReset)
			db.Transact(ctx, func(tx *sequel.Tx) error {
				// faultRecoveryResetErr makes this last-resort reset itself fail (a non-retryable error, as if
				// it lost to a contention storm), so the step stays `completed` and the flow strands `running`
				// with every step terminal - the residual orphan hole that only detectOrphanedFlows surfaces.
				// Process-wide (no scope): taskName is declared after this defer, so it is not in scope here.
				if e.isFault(faultRecoveryResetErr) {
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

	switch db.DriverName() {
	case "pgx", "sqlite":
		err = db.QueryRowContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), lease_seq=lease_seq+1, updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" WHERE step_id=? AND status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
				" RETURNING step_depth, task_name, step_token, state, changes, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, created_at, lease_seq",
			workflow.StatusRunning, leaseMarginMs, stepID, parkedNone,
		).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	case "mssql":
		err = db.QueryRowContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), lease_seq=lease_seq+1, updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" OUTPUT INSERTED.step_depth, INSERTED.task_name, INSERTED.step_token, INSERTED.state, INSERTED.changes, INSERTED.attempt, INSERTED.lineage_id, INSERTED.flow_id, INSERTED.time_budget_ms, INSERTED.interrupt_done, INSERTED.resume_data, INSERTED.subgraph_done, INSERTED.subgraph_result, INSERTED.subgraph_error, INSERTED.created_at, INSERTED.lease_seq"+
				" WHERE step_id=? AND status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
			workflow.StatusRunning, leaseMarginMs, stepID, parkedNone,
		).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq)
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
		res, e := db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), lease_seq=lease_seq+1, updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" WHERE step_id=? AND status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
			workflow.StatusRunning, leaseMarginMs, stepID, parkedNone,
		)
		if e != nil {
			err = e
			break
		}
		n, _ = res.RowsAffected()
		if n == 1 {
			e = db.QueryRowContext(ctx,
				"SELECT step_depth, task_name, step_token, state, changes, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, created_at, lease_seq FROM dwarf_steps WHERE step_id=?",
				stepID,
			).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq)
			if e != nil && e != sql.ErrNoRows {
				err = e
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
	graph, cached := e.graphCache.Load(graphKey)
	if !cached {
		graph = &workflow.Graph{}
		err = json.Unmarshal([]byte(graphJSON), graph)
		if err != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		e.graphCache.Store(graphKey, graph)
	}

	// Build the Flow carrier
	var state map[string]any
	unmarshalJSONMap(stateJSON, &state)
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
	// A panic in the in-process host is caught here so it flows through the normal error disposition
	// rather than wedging this leased step until lease expiry.
	execErr := errors.CatchPanic(func() error {
		// faultPanicExecuteTask panics inside the wrapper so it exercises the host-call panic isolation
		// (caught here, routed as a normal task error), scoped to this task name.
		if e.isFault(faultPanicExecuteTask, taskName) {
			panic("injected fault: " + faultKey(faultPanicExecuteTask, taskName))
		}
		return e.host.ExecuteTask(taskCtx, dispatchURL, &flow.Flow)
	})
	if execErr == nil && e.isFault(faultExecuteTask, taskName) {
		execErr = errors.New("injected fault: "+faultKey(faultExecuteTask, taskName), http.StatusInternalServerError)
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
		return e.handleInterrupt(ctx, shardNum, db, stepID, leaseSeq, flowID, flowToken, changesJSON, interruptPayload)
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
		if lerr == nil && e.isFault(faultLoadGraph, subgraphURL) {
			lerr = errors.New("injected fault: "+faultKey(faultLoadGraph, subgraphURL), http.StatusInternalServerError)
		}
		loadCancel() // a panic here fails the step like any LoadGraph error rather than wedging it
		if lerr != nil {
			err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, lerr, taskName)
			return errors.Trace(err)
		}
		// Same create-time guarantees for a subgraph child: reject a nil or structurally invalid child graph
		// (failing the caller step like any LoadGraph error), and let Validate populate the child's
		// fanOutToFanIn before createSubgraphFlow freezes its JSON.
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
		// Test checkpoint: a breakpoint here freezes the worker after the caller step is parked but before the
		// child flow is inserted, so a test can Cancel the tree in exactly the window that produces an orphaned
		// subgraph child (the recoverOrphanedSubgraphChildren case).
		e.checkpoint(ctx, checkpointAfterCallerPark)
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
		e.checkpoint(ctx, checkpointBeforeRetryRewind)
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
		if retrySleepMs > 0 {
			e.shortenNextPoll(time.Now().Add(time.Duration(retrySleepMs) * time.Millisecond))
		} else {
			e.enqueueStep(ctx, shardNum, stepID)
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
	gotoTarget := resultFlow.GotoRequested()
	// faultLeaseStaleWrite makes this completion write carry a stale lease generation, exactly as a zombie
	// worker (whose lease was re-granted to a peer) would. The fence must reject it (zero rows -> benign
	// no-op below), so the step stays claimable and lease recovery re-runs it cleanly - the test proves a
	// late/slow worker's write can never corrupt or terminalize a flow a peer is healthily re-executing.
	writeSeq := leaseSeq
	if e.isFault(faultLeaseStaleWrite, taskName) {
		writeSeq = leaseSeq - 1
	}
	stepRes, err := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, changes=?, goto_next=?, updated_at=NOW_UTC() WHERE step_id=? AND status!='"+workflow.StatusCancelled+"' AND lease_seq=?",
		workflow.StatusCompleted, string(changesJSON), gotoTarget, stepID, writeSeq,
	)
	if err != nil {
		return errors.Trace(err)
	}
	if nn, _ := stepRes.RowsAffected(); nn == 0 {
		return nil
	}
	stepMarkedComplete = true

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

	isPushTransition := graph.IsFanOutSource(taskName) && !errorRouted && resultFlow.GotoRequested() == ""
	cohortSize := len(realTasks)

	if isPushTransition && cohortSize == 0 {
		fanInTarget := graph.FanInFor(taskName)
		if fanInTarget == "" {
			return e.completeFlowSequential(ctx, shardNum, db, flowID, flowToken, stepID, workflowURL)
		}
		return e.fireFanInDirect(ctx, shardNum, db, flowID, stepID, stepDepth, lineageID, fanInTarget, dispatchURLOf(graph, fanInTarget), graph, sleepDur, flowPriority, flowFairnessKey, flowFairnessWeight, flowTimeBudgetMs)
	}

	if cohortSize == 0 {
		return e.completeFlowSequential(ctx, shardNum, db, flowID, flowToken, stepID, workflowURL)
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

	childInputState, _ := workflow.MergeState(state, accumulatedChanges, nil)
	childInputJSON, _ := json.Marshal(childInputState)
	nextStepDepth := stepDepth + 1
	sleepMs := sleepDur.Milliseconds()

	var newStepIDs []int
	flowFailed := false
	// When the failing flow is a subgraph child, its failure is delivered to the parent's flow.Subgraph
	// call rather than notified directly (see failStep). Captured in the transaction, acted on after it.
	flowFailedParentStepID := 0
	flowFailedReDispatchParent := false

	// Test checkpoint: a breakpoint here freezes the worker after the step is marked completed but before
	// the transition transaction, so a test can Cancel the flow in exactly the window the transition's
	// write-first terminal-status guard exists to survive (the transition must become a no-op, inserting no
	// successors into the cancelled flow).
	e.checkpoint(ctx, checkpointBeforeTransitionTx)

	// The transition (insert next steps, then advance or fail the flow) runs as one retryable
	// transaction. Under pessimistic locking it can deadlock with a concurrent worker, and a deadlocked
	// attempt MUST re-run rather than leave the just-completed step with no successor — which would wedge
	// the flow, since a completed step is not lease-recoverable. Transact rolls back and re-runs the
	// closure (re-reading the fan-in counts so the decision stays correct), and its Tx records any
	// statement error so a partial transition can never commit.
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		newStepIDs = newStepIDs[:0]
		flowFailed = false
		flowFailedParentStepID, flowFailedReDispatchParent = 0, false

		// faultContention returns a retryable lock-contention error (consumed on the first attempt), so the
		// test proves Transact rolls back and re-runs the closure to a clean commit. faultTransitionCommit
		// returns a non-retryable error, so the tx fails after the step was already marked completed and the
		// processStep recovery defer must reset it (completed->pending) and re-dispatch. Both are scoped to
		// the completing task and checked before the flow-row write, so a fired fault rolls back nothing.
		if e.isFault(faultContention, taskName) {
			return errors.New("database is locked (injected fault: " + faultKey(faultContention, taskName) + ")")
		}
		if e.isFault(faultTransitionCommit, taskName) {
			return errors.New("injected fault: " + faultKey(faultTransitionCommit, taskName))
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
			stepStateJSON := childInputJSON
			if next.item != nil {
				perStepState := make(map[string]any, len(childInputState)+3)
				maps.Copy(perStepState, childInputState)
				if next.forEachKey != "" && next.forEachKey != next.itemKey {
					delete(perStepState, next.forEachKey)
				}
				perStepState[next.itemKey] = next.item
				if next.forEachKey != "" {
					perStepState[next.itemKey+"Index"] = next.cohortIndex
					perStepState[next.itemKey+"Count"] = next.cohortCount
				}
				stepStateJSON, _ = json.Marshal(perStepState)
			}
			nextURL := dispatchURLOf(graph, next.taskName)
			newStepID, err := tx.InsertReturnID(ctx, "step_id",
				"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, parked, time_budget_ms, lineage_id, fan_out_ordinal, predecessor_id, not_before, priority, fairness_key, fairness_weight)"+
					" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?)",
				flowID, nextStepDepth, keys.RandomIdentifier(16), next.taskName, nextURL, string(stepStateJSON), workflow.StatusPending, parkedNone, flowTimeBudgetMs, childLineageID, i, stepID, sleepMs, flowPriority, flowFairnessKey, flowFairnessWeight,
			)
			if err != nil {
				return errors.Trace(err)
			}
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
				fanInStepID, err := e.insertFanInStep(ctx, tx, flowID, nextStepDepth, cohortSpawnID, stepID, fanInTaskName, graph, sleepMs, flowPriority, flowFairnessKey, flowFairnessWeight, flowTimeBudgetMs)
				if err != nil {
					return errors.Trace(err)
				}
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
					finalStateJSON, _, cfsErr := e.computeFinalState(ctx, tx, flowID)
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
					tx.QueryRowContext(ctx, "SELECT surgraph_step_id FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&parentStepID)
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
	if err != nil {
		return errors.Trace(err)
	}

	if flowFailed {
		if flowFailedParentStepID != 0 {
			// Subgraph child: delivered to the parent's flow.Subgraph call.
			if flowFailedReDispatchParent {
				e.enqueueStep(ctx, shardNum, flowFailedParentStepID)
			}
			return nil
		}
		compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)
		e.signalStop(ctx, compositeID, workflow.StatusFailed)
		return nil
	}

	if sleepDur > 0 {
		e.shortenNextPoll(time.Now().Add(sleepDur))
	} else if len(newStepIDs) > 0 {
		e.enqueueStep(ctx, shardNum, newStepIDs[0])
	}
	return nil
}

// errLeaseFenced is an in-transaction sentinel: a post-execution write found the dispatch's lease had
// been re-granted to a peer, so the transaction must roll back and the worker must abandon quietly. It is
// never surfaced - callers detect it via a captured `fenced` bool and return nil. See "Lease fencing".
var errLeaseFenced = errors.New("dispatch lease fenced")

// handleInterrupt pauses a flow for external input.
func (e *Engine) handleInterrupt(ctx context.Context, shardNum int, db *sequel.DB, stepID, leaseSeq int, flowID int, flowToken string, changesJSON []byte, interruptPayload map[string]any) error {
	chainFlowIDs, chainStepIDs, chainCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	fenced := false
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		fenced = false
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
		if e.isFault(faultInterruptStaleWrite) {
			curLeaseSeq = leaseSeq + 1
		}
		if curLeaseSeq != leaseSeq {
			fenced = true
			return errLeaseFenced
		}

		if len(interruptPayload) > 0 {
			payloadJSON, _ := json.Marshal(interruptPayload)
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

	for _, compositeID := range chainCompositeIDs {
		e.signalStop(ctx, compositeID, workflow.StatusInterrupted)
	}
	return nil
}

// fireFanInDirect creates the fan-in step immediately for an empty-cohort case.
func (e *Engine) fireFanInDirect(ctx context.Context, shardNum int, db *sequel.DB, flowID int, stepID int, stepDepth int, lineageID int, fanInTarget, fanInURL string, graph *workflow.Graph, sleepDur time.Duration, priority int, fairnessKey string, fairnessWeight float64, timeBudgetMs int) error {
	var fanInStepID int64
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		fanInStepID = 0
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

		var ourStateJSON, ourChangesJSON string
		tx.QueryRowContext(ctx, "SELECT state, changes FROM dwarf_steps WHERE step_id=?", stepID).Scan(&ourStateJSON, &ourChangesJSON)
		var ourState, ourChanges map[string]any
		unmarshalJSONMap(ourStateJSON, &ourState)
		unmarshalJSONMap(ourChangesJSON, &ourChanges)
		mergedState, _ := workflow.MergeState(ourState, ourChanges, graph.Reducers())
		mergedJSON, _ := json.Marshal(mergedState)

		nextStepDepth := stepDepth + 1
		sleepMs := sleepDur.Milliseconds()
		var err error
		fanInStepID, err = tx.InsertReturnID(ctx, "step_id",
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, parked, time_budget_ms, lineage_id, predecessor_id, not_before, priority, fairness_key, fairness_weight)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?)",
			flowID, nextStepDepth, keys.RandomIdentifier(16), fanInTarget, fanInURL, string(mergedJSON), workflow.StatusPending, parkedNone, timeBudgetMs, lineageID, stepID, sleepMs, priority, fairnessKey, fairnessWeight,
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

	if sleepDur > 0 {
		e.shortenNextPoll(time.Now().Add(sleepDur))
	} else {
		e.enqueueStep(ctx, shardNum, int(fanInStepID))
	}
	return nil
}

// insertFanInStep creates the fan-in step after the cohort completes.
func (e *Engine) insertFanInStep(ctx context.Context, tx sequel.Executor, flowID, nextStepDepth, cohortSpawnID, predecessorStepID int, fanInTaskName string, graph *workflow.Graph, sleepMs int64, priority int, fairnessKey string, fairnessWeight float64, timeBudgetMs int) (int, error) {
	var spawnStateJSON, spawnChangesJSON, spawnTaskName string
	var spawnLineageID int
	tx.QueryRowContext(ctx,
		"SELECT state, changes, lineage_id, task_name FROM dwarf_steps WHERE step_id=?",
		cohortSpawnID,
	).Scan(&spawnStateJSON, &spawnChangesJSON, &spawnLineageID, &spawnTaskName)
	var spawnState, spawnChanges map[string]any
	unmarshalJSONMap(spawnStateJSON, &spawnState)
	unmarshalJSONMap(spawnChangesJSON, &spawnChanges)
	merged, _ := workflow.MergeState(spawnState, spawnChanges, graph.Reducers())

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
		return 0, errors.Trace(err)
	}
	defer rows.Close()
	maxCohortDepth := 0
	for rows.Next() {
		var memberStepID int
		var memberTaskName, status, changesJSON string
		var depth int
		rows.Scan(&memberStepID, &memberTaskName, &status, &changesJSON, &depth)
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
		merged, _ = workflow.MergeState(merged, changes, graph.Reducers())
	}
	rows.Close()

	// The fan-in sits one level below the DEEPEST cohort branch, not merely below the last sibling to
	// complete - branch lengths can differ (loops, gotos, varying chains), so step_depth must reflect the
	// deepest path the flow took. nextStepDepth (last-completer+1) is the floor for the defensive empty case.
	fanInDepth := max(maxCohortDepth+1, nextStepDepth)

	// Drop per-branch forEach bookkeeping
	for _, tr := range graph.Transitions() {
		if tr.From != spawnTaskName || tr.ForEach == "" || tr.As == "" {
			continue
		}
		delete(merged, tr.As)
		delete(merged, tr.As+"Index")
		delete(merged, tr.As+"Count")
	}

	mergedJSON, _ := json.Marshal(merged)
	fanInURL := dispatchURLOf(graph, fanInTaskName)
	fanInStepID, err := tx.InsertReturnID(ctx, "step_id",
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, parked, time_budget_ms, lineage_id, predecessor_id, not_before, priority, fairness_key, fairness_weight)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?)",
		flowID, fanInDepth, keys.RandomIdentifier(16), fanInTaskName, fanInURL, string(mergedJSON), workflow.StatusPending, parkedNone, timeBudgetMs, spawnLineageID, predecessorStepID, sleepMs, priority, fairnessKey, fairnessWeight,
	)
	if err != nil {
		return 0, errors.Trace(err)
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
	return int(fanInStepID), nil
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
