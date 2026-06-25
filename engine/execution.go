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
	"strings"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// processStep acquires a step, executes its task, and enqueues the next step if applicable.
func (e *Engine) processStep(ctx context.Context, stepID int, shardNum int) (err error) {
	defer func() {
		if sequel.IsLockContentionError(err) {
			if db, derr := e.shard(shardNum); derr == nil {
				// Reset the leased step so the immediate re-poll can re-dispatch it. This recovery
				// runs precisely during a lock-contention storm, so a single best-effort UPDATE can
				// itself lose to contention and silently fail - leaving the step `running` with its
				// full lease (TimeBudget+margin) and stranding the flow until lease expiry, minutes
				// past the poll cadence. Transact retries on contention; the WHERE status='running'
				// guard keeps it idempotent.
				db.Transact(ctx, func(tx *sequel.Tx) error {
					_, terr := tx.ExecContext(ctx,
						"UPDATE dwarf_steps SET status=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
						workflow.StatusPending, stepID, workflow.StatusRunning,
					)
					return terr
				})
			}
			e.shortenNextPoll(time.Now())
		}
	}()
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	// Lease = the step's own time_budget_ms (added in the claim UPDATE below) + a fixed margin, so it always
	// outlasts the ExecuteTask deadline. Only the margin is bound; the per-step budget comes from the column.
	leaseMarginMs := int(leaseMargin.Milliseconds())

	// Claim the step and read its data in one round-trip where the driver supports RETURNING.
	var n int64
	var stepDepth int
	var taskName, stepToken, stateJSON, priorChangesJSON string
	var breakpointHit bool
	var attempt, lineageID, flowID, timeBudgetMs int
	var interruptDone bool
	var resumeDataJSON string
	var subgraphDone bool
	var subgraphResultJSON, subgraphErrorStr string
	var stepCreatedAt time.Time

	switch db.DriverName() {
	case "pgx", "sqlite":
		err = db.QueryRowContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" WHERE step_id=? AND status=? AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
				" RETURNING step_depth, task_name, step_token, state, changes, breakpoint_hit, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, created_at",
			workflow.StatusRunning, leaseMarginMs, stepID, workflow.StatusPending, parkedNone,
		).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &breakpointHit, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	case "mssql":
		err = db.QueryRowContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" OUTPUT INSERTED.step_depth, INSERTED.task_name, INSERTED.step_token, INSERTED.state, INSERTED.changes, INSERTED.breakpoint_hit, INSERTED.attempt, INSERTED.lineage_id, INSERTED.flow_id, INSERTED.time_budget_ms, INSERTED.interrupt_done, INSERTED.resume_data, INSERTED.subgraph_done, INSERTED.subgraph_result, INSERTED.subgraph_error, INSERTED.created_at"+
				" WHERE step_id=? AND status=? AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
			workflow.StatusRunning, leaseMarginMs, stepID, workflow.StatusPending, parkedNone,
		).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &breakpointHit, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	default:
		// MySQL: parallel claim + read
		var claimErr, readErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			res, e := db.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), updated_at=NOW_UTC(),"+
					" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
					" WHERE step_id=? AND status=? AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
				workflow.StatusRunning, leaseMarginMs, stepID, workflow.StatusPending, parkedNone,
			)
			if e != nil {
				claimErr = e
				return
			}
			n, _ = res.RowsAffected()
		}()
		go func() {
			defer wg.Done()
			e := db.QueryRowContext(ctx,
				"SELECT step_depth, task_name, step_token, state, changes, breakpoint_hit, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, created_at FROM dwarf_steps WHERE step_id=?",
				stepID,
			).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &breakpointHit, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt)
			if e != nil && e != sql.ErrNoRows {
				readErr = e
			}
		}()
		wg.Wait()
		if claimErr != nil {
			err = claimErr
		} else if readErr != nil {
			err = readErr
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
	var breakpointsJSON string
	var notifyOnStop bool
	var flowCreatedAt, flowUpdatedAt time.Time
	var flowPriority int
	var flowFairnessKey string
	var flowFairnessWeight float64
	var flowTimeBudgetMs int
	err = db.QueryRowContext(ctx,
		"SELECT flow_token, status, workflow_url, graph, baggage, trace_parent, notify_on_stop, breakpoints, created_at, updated_at, priority, fairness_key, fairness_weight, time_budget_ms FROM dwarf_flows WHERE flow_id=?",
		flowID,
	).Scan(&flowToken, &flowStatus, &workflowURL, &graphJSON, &baggageJSON, &traceParent, &notifyOnStop, &breakpointsJSON, &flowCreatedAt, &flowUpdatedAt, &flowPriority, &flowFairnessKey, &flowFairnessWeight, &flowTimeBudgetMs)
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
	graphKey := graphCacheKey{shard: shardNum, flowID: flowID}
	graph, cached := e.graphCache.Load(graphKey)
	if !cached {
		graph = &workflow.Graph{}
		err = json.Unmarshal([]byte(graphJSON), graph)
		if err != nil {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
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

	// Breakpoint check
	if !breakpointHit {
		breakpoints := map[string]string{}
		if breakpointsJSON != "" && breakpointsJSON != "{}" {
			json.Unmarshal([]byte(breakpointsJSON), &breakpoints)
		}
		breakpointMatch := breakpoints[taskName] == "b"
		if len(breakpoints) > 0 && breakpointMatch {
			e.logger.DebugContext(ctx, "Breakpoint hit", "task", taskName, "step", stepDepth, "flow", workflowURL)
			e.metricStepExecuted(ctx, taskName, workflow.StatusInterrupted)
			return e.handleBreakpoint(ctx, shardNum, db, stepID, flowID, flowToken)
		}
	}

	// Execute the task. The step's time_budget_ms bounds the executor call's context deadline; the
	// surrounding DB work keeps using the undeadlined ctx so persistence is never cut short.
	e.logger.DebugContext(ctx, "Executing task", "task", taskName, "flow", workflowURL)
	dispatchURL := dispatchURLOf(graph, taskName)
	taskCtx := workflow.ContextWithBaggage(ctx, baggage)

	// Open a per-step span parented to the flow's root "workflow" span (reconstructed from the stored
	// trace_parent), and place it on the executor's context so the task's downstream spans nest under it.
	// The span is named by the task; no-op unless a TracerProvider is configured.
	flowKey := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)
	taskCtx = injectTraceParent(taskCtx, traceParent)
	taskCtx, taskSpan := e.tracer.Start(taskCtx, taskName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("workflow.id", flowKey),
			attribute.String("workflow.name", workflowURL),
		),
	)
	defer taskSpan.End()

	if timeBudgetMs > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(taskCtx, time.Duration(timeBudgetMs)*time.Millisecond)
		defer cancel()
	}
	execErr := e.host.ExecuteTask(taskCtx, dispatchURL, &flow.Flow)
	recordSpanError(taskSpan, execErr)

	var resultFlow *workflow.RawFlow
	errorRouted := false

	if execErr != nil {
		// The engine never inspects status codes or error text: a task that wants to back off (rate limit,
		// transient unavailability) reads its own signal and arms flow.Retry. Any error that reaches here is
		// terminal for this attempt - routed via the graph's onError transition if one exists, else it fails
		// the step.
		if _, ok := graph.ErrorTransition(taskName); ok {
			e.logger.DebugContext(ctx, "Task error routed", "task", taskName, "flow", workflowURL, "error", execErr)
			tracedErr := errors.Convert(execErr)
			resultFlow = workflow.NewRawFlow()
			resultFlow.SetRawState(state)
			resultFlow.Set("onErr", tracedErr)
			errorRouted = true
		} else {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, execErr, taskName)
			return errors.Trace(execErr)
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
		accumulatedChanges, _ = workflow.MergeState(priorChanges, rawChanges, nil)
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
		if _, _, _, ok := resultFlow.SubgraphRequested(); ok {
			signalCount++
		}
		if signalCount > 1 {
			err = errors.New("task '%s' set multiple competing control signals", taskName)
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
	}

	// Single-park guard
	{
		_, interruptArmed := resultFlow.InterruptRequested()
		_, _, _, subgraphArmed := resultFlow.SubgraphRequested()
		if (interruptArmed || subgraphArmed) && (interruptDone || subgraphDone) {
			err = errors.New("task '%s' armed a second park on an already-resolved step", taskName)
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
	}

	// Handle interrupt
	if interruptPayload, interrupted := resultFlow.InterruptRequested(); interrupted {
		e.logger.DebugContext(ctx, "Task interrupted", "task", taskName, "flow", workflowURL)
		e.metricStepExecuted(ctx, taskName, workflow.StatusInterrupted)
		return e.handleInterrupt(ctx, shardNum, db, stepID, flowID, flowToken, changesJSON, interruptPayload)
	}

	// Handle subgraph
	if subgraphURL, subgraphInput, subgraphTaskName, subgraphRequested := resultFlow.SubgraphRequested(); subgraphRequested {
		e.logger.DebugContext(ctx, "Task requested subflow", "task", taskName, "flow", workflowURL, "subflow", subgraphURL, "subtask", subgraphTaskName)
		// taskName != "" => Subtask: synthesize a trivial single-task graph in-engine (no LoadGraph,
		// no host round-trip). Empty => regular Subgraph: load the graph by URL from the host.
		var subgraphGraph *workflow.Graph
		if subgraphTaskName != "" {
			subgraphGraph = singleTaskGraph(subgraphTaskName, subgraphURL)
		} else {
			// Bound the subgraph LoadGraph by the caller flow's budget (the create-time LoadGraph uses the caller's ctx).
			loadCtx, loadCancel := context.WithTimeout(workflow.ContextWithBaggage(ctx, baggage), time.Duration(flowTimeBudgetMs)*time.Millisecond)
			g, lerr := e.host.LoadGraph(loadCtx, subgraphURL)
			loadCancel()
			if lerr != nil {
				e.failStep(ctx, shardNum, stepID, flowID, flowToken, lerr, taskName)
				return errors.Trace(lerr)
			}
			subgraphGraph = g
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
			"UPDATE dwarf_steps SET changes=?, parked=?, updated_at=NOW_UTC() WHERE step_id=? AND status=?",
			string(changesJSON), parkedSubgraph, stepID, workflow.StatusRunning,
		)
		if err != nil {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		if n, _ := parkRes.RowsAffected(); n == 0 {
			return nil
		}
		childInputState := subgraphInput
		if childInputState == nil {
			childInputState = map[string]any{}
		}
		// The caller step's span is still live on taskCtx; parent the subgraph's "workflow" span under it
		// so the subgraph subtree nests beneath this task in the trace.
		callerTraceParent := extractTraceParent(taskCtx)
		subgraphFlowKey, err := e.createSubgraphFlow(ctx, shardNum, flowID, stepDepth, stepID, subgraphURL, subgraphGraph, childInputState, baggageJSON, callerTraceParent, breakpointsJSON)
		if err != nil {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		err = e.start(ctx, subgraphFlowKey)
		if err != nil {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		disposition := "subgraph"
		if subgraphTaskName != "" {
			disposition = "subtask"
		}
		e.metricStepExecuted(ctx, taskName, disposition)
		return nil
	}

	sleepDur := resultFlow.SleepRequested()

	// Handle retry
	if initialDelay, multiplier, maxDelay, retryRequested := resultFlow.RetryRequested(); retryRequested {
		e.logger.DebugContext(ctx, "Task retried", "task", taskName, "flow", workflowURL, "step", stepID, "attempt", attempt)
		// Sleep is the floor and the backoff adds on top: total = Sleep + min(backoff, maxDelay). This lets a
		// task set a precise wait (e.g. a downstream's Retry-After via Sleep) and still get exponential backoff
		// on repeated attempts. maxDelay caps the backoff component, not the total.
		retrySleepMs := sleepDur.Milliseconds()
		{
			delay := float64(initialDelay)
			if multiplier > 0 {
				for range attempt {
					delay *= multiplier
				}
			}
			if maxDelay > 0 && time.Duration(delay) > maxDelay {
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
		// This mirrors RestartFrom, the operator-facing in-place rewind, which already reaps its
		// descendant subgraph flows. Step-scoped (only this caller's children), unlike
		// RestartFrom's flow-scoped sweep, so a retrying fan-out sibling's cohort is untouched.
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			if reapErr := e.deleteSubgraphFlowsRootedAt(ctx, tx, stepID); reapErr != nil {
				return errors.Trace(reapErr)
			}
			_, execErr := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, changes=?, attempt=?, not_before=DATE_ADD_MILLIS(NOW_UTC(), ?), lease_expires=NOW_UTC(), updated_at=NOW_UTC(), interrupt_done=0, resume_data='{}', subgraph_done=0, subgraph_result='{}', subgraph_error='' WHERE step_id=?",
				workflow.StatusPending, string(changesJSON), attempt+1, retrySleepMs, stepID,
			)
			return errors.Trace(execErr)
		})
		if err != nil {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
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
		e.logger.DebugContext(ctx, "Task error routed", "task", taskName, "flow", workflowURL)
		e.metricStepExecuted(ctx, taskName, "error_routed")
	} else {
		e.logger.DebugContext(ctx, "Task completed", "task", taskName, "flow", workflowURL)
		e.metricStepExecuted(ctx, taskName, workflow.StatusCompleted)
	}
	gotoTarget := resultFlow.GotoRequested()
	stepRes, err := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, changes=?, goto_next=?, updated_at=NOW_UTC() WHERE step_id=? AND status!=?",
		workflow.StatusCompleted, string(changesJSON), gotoTarget, stepID, workflow.StatusCancelled,
	)
	if err != nil {
		return errors.Trace(err)
	}
	if nn, _ := stepRes.RowsAffected(); nn == 0 {
		return nil
	}

	// Evaluate transitions
	var nextTasks []nextStep
	if errorRouted {
		nextTasks, err = evaluateErrorTransitions(graph, taskName, resultFlow)
	} else {
		nextTasks, err = evaluateTransitions(graph, taskName, resultFlow)
	}
	if err != nil {
		e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
		return errors.Trace(err)
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
			return e.completeFlowSequential(ctx, shardNum, db, flowID, flowToken, stepID, notifyOnStop, baggageJSON, workflowURL)
		}
		return e.fireFanInDirect(ctx, shardNum, db, flowID, stepID, stepDepth, lineageID, fanInTarget, dispatchURLOf(graph, fanInTarget), sleepDur, flowPriority, flowFairnessKey, flowFairnessWeight, flowTimeBudgetMs)
	}

	if cohortSize == 0 {
		return e.completeFlowSequential(ctx, shardNum, db, flowID, flowToken, stepID, notifyOnStop, baggageJSON, workflowURL)
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
	var flowFailedErr, flowFailedFinalState string

	// The transition (insert next steps, then advance or fail the flow) runs as one retryable
	// transaction. Under pessimistic locking it can deadlock with a concurrent worker, and a deadlocked
	// attempt MUST re-run rather than leave the just-completed step with no successor — which would wedge
	// the flow, since a completed step is not lease-recoverable. Transact rolls back and re-runs the
	// closure (re-reading the fan-in counts so the decision stays correct), and its Tx records any
	// statement error so a partial transition can never commit.
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		newStepIDs = newStepIDs[:0]
		flowFailed = false
		flowFailedErr, flowFailedFinalState = "", ""

		tx.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=NOW_UTC() WHERE flow_id=?", flowID)

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
				flowID, nextStepDepth, randomIdentifier(16), next.taskName, nextURL, string(stepStateJSON), workflow.StatusPending, parkedNone, flowTimeBudgetMs, childLineageID, i, stepID, sleepMs, flowPriority, flowFairnessKey, flowFairnessWeight,
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
			e.logger.DebugContext(ctx, "Fan-out cohort spawned", "flow", flowID, "spawnStep", stepID, "task", taskName, "cohortSize", cohortSize)
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
					failFlow, _ = e.propagateCohortFailure(ctx, tx, spawnLineageID)
				}
				if failFlow {
					var sampleErr string
					tx.QueryRowContext(ctx,
						"SELECT error FROM dwarf_steps WHERE flow_id=? AND status=? AND error!='' ORDER BY step_id LIMIT_OFFSET(1, 0)",
						flowID, workflow.StatusFailed,
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
						"UPDATE dwarf_flows SET final_state=?, status=?, error=?, updated_at=NOW_UTC() WHERE flow_id=? AND status NOT IN (?, ?, ?)",
						finalStateJSON, workflow.StatusFailed, sampleErr, flowID,
						workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled,
					)
					flowFailed = true
					flowFailedErr = sampleErr
					flowFailedFinalState = finalStateJSON
				}
			}
		}

		nextFlowStepID := 0
		if len(newStepIDs) == 1 {
			nextFlowStepID = newStepIDs[0]
		}
		if !flowFailed {
			tx.ExecContext(ctx, "UPDATE dwarf_flows SET step_id=?, updated_at=NOW_UTC() WHERE flow_id=?", nextFlowStepID, flowID)
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	if flowFailed {
		compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)
		if notifyOnStop {
			var finalState map[string]any
			json.Unmarshal([]byte(flowFailedFinalState), &finalState)
			e.fireFlowStopped(ctx, compositeID, baggageJSON, &workflow.FlowOutcome{
				Status: workflow.StatusFailed,
				State:  finalState,
				Error:  flowFailedErr,
			})
		}
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

// handleBreakpoint pauses execution before a task due to a breakpoint.
func (e *Engine) handleBreakpoint(ctx context.Context, shardNum int, db *sequel.DB, stepID int, flowID int, flowToken string) error {
	chainFlowIDs, chainStepIDs, chainCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		flowPlaceholders := strings.Repeat("?,", len(chainFlowIDs)-1) + "?"
		flowArgs := append([]any{workflow.StatusInterrupted}, chainFlowIDs...)
		flowArgs = append(flowArgs, workflow.StatusRunning, workflow.StatusInterrupted)
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status IN (?, ?)",
			flowArgs...,
		)

		allStepIDs := append([]any{stepID}, chainStepIDs...)
		stepPlaceholders := strings.Repeat("?,", len(allStepIDs)-1) + "?"
		stepArgs := append([]any{workflow.StatusInterrupted}, allStepIDs...)
		stepArgs = append(stepArgs, workflow.StatusRunning, workflow.StatusInterrupted)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN ("+stepPlaceholders+") AND status IN (?, ?)",
			stepArgs...,
		)
		tx.ExecContext(ctx, "UPDATE dwarf_steps SET breakpoint_hit=1 WHERE step_id=?", stepID)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	for _, compositeID := range chainCompositeIDs {
		e.signalStop(ctx, compositeID, workflow.StatusInterrupted)
	}

	rootCompositeID := chainCompositeIDs[len(chainCompositeIDs)-1]
	rootFlowID := chainFlowIDs[len(chainFlowIDs)-1]
	var rootNotifyOnStop bool
	var rootBaggageJSON string
	db.QueryRowContext(ctx, "SELECT notify_on_stop, baggage FROM dwarf_flows WHERE flow_id=?", rootFlowID).Scan(&rootNotifyOnStop, &rootBaggageJSON)
	if rootNotifyOnStop {
		e.fireFlowStopped(ctx, rootCompositeID, rootBaggageJSON, &workflow.FlowOutcome{
			Status: workflow.StatusInterrupted,
		})
	}
	return nil
}

// handleInterrupt pauses a flow for external input.
func (e *Engine) handleInterrupt(ctx context.Context, shardNum int, db *sequel.DB, stepID int, flowID int, flowToken string, changesJSON []byte, interruptPayload map[string]any) error {
	chainFlowIDs, chainStepIDs, chainCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		flowPlaceholders := strings.Repeat("?,", len(chainFlowIDs)-1) + "?"
		flowArgs := append([]any{workflow.StatusInterrupted}, chainFlowIDs...)
		flowArgs = append(flowArgs, workflow.StatusRunning, workflow.StatusInterrupted)
		tx.ExecContext(ctx,
			"UPDATE dwarf_flows SET status=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status IN (?, ?)",
			flowArgs...,
		)

		allStepIDs := append([]any{stepID}, chainStepIDs...)
		stepPlaceholders := strings.Repeat("?,", len(allStepIDs)-1) + "?"
		stepArgs := []any{stepID, string(changesJSON), stepID, workflow.StatusInterrupted, parkedNone}
		stepArgs = append(stepArgs, allStepIDs...)
		stepArgs = append(stepArgs, workflow.StatusRunning, workflow.StatusInterrupted)
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET changes=CASE WHEN step_id=? THEN ? ELSE changes END, interrupt_done=CASE WHEN step_id=? THEN 1 ELSE interrupt_done END, status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN ("+stepPlaceholders+") AND status IN (?, ?)",
			stepArgs...,
		)

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
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	for _, compositeID := range chainCompositeIDs {
		e.signalStop(ctx, compositeID, workflow.StatusInterrupted)
	}

	rootCompositeID := chainCompositeIDs[len(chainCompositeIDs)-1]
	rootFlowID := chainFlowIDs[len(chainFlowIDs)-1]
	var rootNotifyOnStop bool
	var rootBaggageJSON string
	db.QueryRowContext(ctx, "SELECT notify_on_stop, baggage FROM dwarf_flows WHERE flow_id=?", rootFlowID).Scan(&rootNotifyOnStop, &rootBaggageJSON)
	if rootNotifyOnStop {
		e.fireFlowStopped(ctx, rootCompositeID, rootBaggageJSON, &workflow.FlowOutcome{
			Status:           workflow.StatusInterrupted,
			InterruptPayload: interruptPayload,
		})
	}
	return nil
}

// fireFanInDirect creates the fan-in step immediately for an empty-cohort case.
func (e *Engine) fireFanInDirect(ctx context.Context, shardNum int, db *sequel.DB, flowID int, stepID int, stepDepth int, lineageID int, fanInTarget, fanInURL string, sleepDur time.Duration, priority int, fairnessKey string, fairnessWeight float64, timeBudgetMs int) error {
	var fanInStepID int64
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		tx.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=NOW_UTC() WHERE flow_id=?", flowID)
		tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_size=0 WHERE step_id=?", stepID)

		var ourStateJSON, ourChangesJSON string
		tx.QueryRowContext(ctx, "SELECT state, changes FROM dwarf_steps WHERE step_id=?", stepID).Scan(&ourStateJSON, &ourChangesJSON)
		var ourState, ourChanges map[string]any
		unmarshalJSONMap(ourStateJSON, &ourState)
		unmarshalJSONMap(ourChangesJSON, &ourChanges)
		mergedState, _ := workflow.MergeState(ourState, ourChanges, nil)
		mergedJSON, _ := json.Marshal(mergedState)

		nextStepDepth := stepDepth + 1
		sleepMs := sleepDur.Milliseconds()
		var err error
		fanInStepID, err = tx.InsertReturnID(ctx, "step_id",
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, status, parked, time_budget_ms, lineage_id, predecessor_id, not_before, priority, fairness_key, fairness_weight)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?)",
			flowID, nextStepDepth, randomIdentifier(16), fanInTarget, fanInURL, string(mergedJSON), workflow.StatusPending, parkedNone, timeBudgetMs, lineageID, stepID, sleepMs, priority, fairnessKey, fairnessWeight,
		)
		if err != nil {
			return errors.Trace(err)
		}
		tx.ExecContext(ctx, "UPDATE dwarf_steps SET successor_id=? WHERE step_id=?", int(fanInStepID), stepID)
		tx.ExecContext(ctx, "UPDATE dwarf_flows SET step_id=?, updated_at=NOW_UTC() WHERE flow_id=?", int(fanInStepID), flowID)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
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

	rows, err := tx.QueryContext(ctx,
		"SELECT status, changes FROM dwarf_steps WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_id",
		flowID, cohortSpawnID,
	)
	if err != nil {
		return 0, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var status, changesJSON string
		rows.Scan(&status, &changesJSON)
		status = strings.TrimSpace(status)
		if status != workflow.StatusCompleted {
			continue
		}
		var changes map[string]any
		unmarshalJSONMap(changesJSON, &changes)
		merged, _ = workflow.MergeState(merged, changes, graph.Reducers())
	}
	rows.Close()

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
		flowID, nextStepDepth, randomIdentifier(16), fanInTaskName, fanInURL, string(mergedJSON), workflow.StatusPending, parkedNone, timeBudgetMs, spawnLineageID, predecessorStepID, sleepMs, priority, fairnessKey, fairnessWeight,
	)
	if err != nil {
		return 0, errors.Trace(err)
	}

	exitTasks := fanInPredecessorTasks(graph, fanInTaskName)
	if len(exitTasks) > 0 {
		placeholders := strings.Repeat("?,", len(exitTasks)-1) + "?"
		args := []any{int(fanInStepID), flowID, cohortSpawnID}
		for _, t := range exitTasks {
			args = append(args, t)
		}
		tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET successor_id=? WHERE flow_id=? AND lineage_id=? AND task_name IN ("+placeholders+")",
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
