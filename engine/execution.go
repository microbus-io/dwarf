/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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
	"sync"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// processStep acquires a step, executes its task, and enqueues the next step if applicable.
func (e *Engine) processStep(ctx context.Context, stepID int, shardNum int) (err error) {
	defer func() {
		if sequel.IsLockContentionError(err) {
			if db, derr := e.shard(shardNum); derr == nil {
				db.ExecContext(ctx,
					"UPDATE microbus_steps SET status=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
					workflow.StatusPending, stepID, workflow.StatusRunning,
				)
			}
			e.shortenNextPoll(time.Now())
		}
	}()
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	leaseMs := int(e.taskTimeBudget().Milliseconds() + leaseMargin.Milliseconds())

	// Claim the step and read its data in one round-trip where the driver supports RETURNING.
	var n int64
	var stepDepth int
	var taskName, stateJSON, priorChangesJSON string
	var breakpointHit bool
	var attempt, lineageID, flowID, timeBudgetMs int
	var interruptDone bool
	var resumeDataJSON string
	var subgraphDone bool
	var subgraphResultJSON, subgraphErrorStr string

	switch db.DriverName() {
	case "pgx", "sqlite":
		err = db.QueryRowContext(ctx,
			"UPDATE microbus_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), ?), updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" WHERE step_id=? AND status=? AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
				" RETURNING step_depth, task_name, state, changes, breakpoint_hit, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error",
			workflow.StatusRunning, leaseMs, stepID, workflow.StatusPending, parkedNone,
		).Scan(&stepDepth, &taskName, &stateJSON, &priorChangesJSON, &breakpointHit, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	case "mssql":
		err = db.QueryRowContext(ctx,
			"UPDATE microbus_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), ?), updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" OUTPUT INSERTED.step_depth, INSERTED.task_name, INSERTED.state, INSERTED.changes, INSERTED.breakpoint_hit, INSERTED.attempt, INSERTED.lineage_id, INSERTED.flow_id, INSERTED.time_budget_ms, INSERTED.interrupt_done, INSERTED.resume_data, INSERTED.subgraph_done, INSERTED.subgraph_result, INSERTED.subgraph_error"+
				" WHERE step_id=? AND status=? AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
			workflow.StatusRunning, leaseMs, stepID, workflow.StatusPending, parkedNone,
		).Scan(&stepDepth, &taskName, &stateJSON, &priorChangesJSON, &breakpointHit, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr)
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
				"UPDATE microbus_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), ?), updated_at=NOW_UTC(),"+
					" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
					" WHERE step_id=? AND status=? AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
				workflow.StatusRunning, leaseMs, stepID, workflow.StatusPending, parkedNone,
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
				"SELECT step_depth, task_name, state, changes, breakpoint_hit, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error FROM microbus_steps WHERE step_id=?",
				stepID,
			).Scan(&stepDepth, &taskName, &stateJSON, &priorChangesJSON, &breakpointHit, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr)
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
	var flowToken, flowStatus, workflowName, graphJSON, metadataJSON string
	var notifyHostname, breakpointsJSON string
	var flowCreatedAt, flowUpdatedAt time.Time
	var flowPriority int
	var flowFairnessKey string
	var flowFairnessWeight float64
	err = db.QueryRowContext(ctx,
		"SELECT flow_token, status, workflow_name, graph, actor_claims, notify_hostname, breakpoints, created_at, updated_at, priority, fairness_key, fairness_weight FROM microbus_flows WHERE flow_id=?",
		flowID,
	).Scan(&flowToken, &flowStatus, &workflowName, &graphJSON, &metadataJSON, &notifyHostname, &breakpointsJSON, &flowCreatedAt, &flowUpdatedAt, &flowPriority, &flowFairnessKey, &flowFairnessWeight)
	if err != nil {
		return errors.Trace(err)
	}

	flowStatus = strings.TrimSpace(flowStatus)
	flowToken = strings.TrimSpace(flowToken)
	if flowStatus == workflow.StatusCancelled || flowStatus == workflow.StatusFailed || flowStatus == workflow.StatusCompleted {
		_, err = db.ExecContext(ctx,
			"UPDATE microbus_steps SET status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
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
	flow.SetTimestamps(flowCreatedAt, flowUpdatedAt)

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

	// Parse metadata for the task executor
	var metadata map[string]any
	unmarshalJSONMap(metadataJSON, &metadata)

	// Breakpoint check
	if !breakpointHit {
		breakpoints := map[string]string{}
		if breakpointsJSON != "" && breakpointsJSON != "{}" {
			json.Unmarshal([]byte(breakpointsJSON), &breakpoints)
		}
		breakpointMatch := breakpoints[taskName] == "b"
		if !breakpointMatch {
			if u := graph.URLOf(taskName); u != "" && breakpoints[u] == "b" && len(graph.NamesForURL(u)) == 1 {
				breakpointMatch = true
			}
		}
		if len(breakpoints) > 0 && breakpointMatch {
			e.logger.LogDebug(ctx, "Breakpoint hit", "task", taskName, "step", stepDepth, "flow", workflowName)
			return e.handleBreakpoint(ctx, shardNum, db, stepID, flowID, flowToken)
		}
	}

	// Execute the task. The step's time_budget_ms bounds the executor call's context deadline; the
	// surrounding DB work keeps using the undeadlined ctx so persistence is never cut short.
	e.logger.LogDebug(ctx, "Executing task", "task", taskName, "flow", workflowName)
	e.breakerCommit(taskName)
	dispatchURL := dispatchURLOf(graph, taskName)
	taskCtx := ctx
	if timeBudgetMs > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(ctx, time.Duration(timeBudgetMs)*time.Millisecond)
		defer cancel()
	}
	execErr := e.taskExecutor(taskCtx, dispatchURL, &flow.Flow, metadata)

	var resultFlow *workflow.RawFlow
	errorRouted := false
	errStatusCode := 0

	if execErr != nil {
		if errors.StatusCode(execErr) == http.StatusTooManyRequests {
			return e.handleBackpressure(ctx, shardNum, stepID, taskName)
		}
		var breakerCause string
		switch {
		case errors.StatusCode(execErr) == http.StatusNotFound && strings.HasPrefix(execErr.Error(), "ack timeout"):
			breakerCause = breakerCauseAckTimeout
		case errors.StatusCode(execErr) == http.StatusServiceUnavailable:
			breakerCause = breakerCauseUnavailable
		case errors.StatusCode(execErr) == 529:
			breakerCause = breakerCauseOverloaded
		}
		if breakerCause != "" {
			return e.handleBreakerTrip(ctx, shardNum, stepID, taskName, breakerCause)
		}

		if _, ok := graph.ErrorTransition(taskName); ok {
			e.logger.LogDebug(ctx, "Task error routed", "task", taskName, "flow", workflowName, "error", execErr)
			tracedErr := errors.Convert(execErr)
			errStatusCode = tracedErr.StatusCode
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
		e.breakerClose(ctx, taskName, shardNum)
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
		if _, _, _, _, retryRequested := resultFlow.RetryRequested(); retryRequested {
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
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
	}

	// Single-park guard
	{
		_, interruptArmed := resultFlow.InterruptRequested()
		_, _, subgraphArmed := resultFlow.SubgraphRequested()
		if (interruptArmed || subgraphArmed) && (interruptDone || subgraphDone) {
			err = errors.New("task '%s' armed a second park on an already-resolved step", taskName)
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
	}

	// Handle interrupt
	if interruptPayload, interrupted := resultFlow.InterruptRequested(); interrupted {
		e.logger.LogDebug(ctx, "Task interrupted", "task", taskName, "flow", workflowName)
		return e.handleInterrupt(ctx, shardNum, db, stepID, flowID, flowToken, changesJSON, interruptPayload)
	}

	// Handle subgraph
	if subgraphWorkflow, subgraphInput, subgraphRequested := resultFlow.SubgraphRequested(); subgraphRequested {
		e.logger.LogDebug(ctx, "Task requested subgraph", "task", taskName, "flow", workflowName, "subgraph", subgraphWorkflow)
		db.ExecContext(ctx,
			"UPDATE microbus_steps SET changes=?, updated_at=NOW_UTC() WHERE step_id=? AND status=?",
			string(changesJSON), stepID, workflow.StatusRunning,
		)
		subgraphGraph, err := e.graphLoader(ctx, subgraphWorkflow, metadata)
		if err != nil {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		childInputState := subgraphInput
		if childInputState == nil {
			childInputState = map[string]any{}
		}
		subgraphFlowKey, err := e.createSubgraphFlow(ctx, shardNum, flowID, stepDepth, stepID, subgraphWorkflow, subgraphGraph, childInputState, metadataJSON, breakpointsJSON)
		if err != nil {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		err = e.startNotify(ctx, subgraphFlowKey, "")
		if err != nil {
			e.failStep(ctx, shardNum, stepID, flowID, flowToken, err, taskName)
			return errors.Trace(err)
		}
		db.ExecContext(ctx,
			"UPDATE microbus_steps SET parked=?, updated_at=NOW_UTC() WHERE step_id=? AND status=?",
			parkedSubgraph, stepID, workflow.StatusRunning,
		)
		return nil
	}

	sleepDur := resultFlow.SleepRequested()

	// Handle retry
	if maxAttempts, initialDelay, multiplier, maxDelay, retryRequested := resultFlow.RetryRequested(); retryRequested {
		e.logger.LogDebug(ctx, "Task retried", "task", taskName, "flow", workflowName, "step", stepID, "attempt", attempt)
		retrySleepMs := sleepDur.Milliseconds()
		if maxAttempts > 0 {
			delay := float64(initialDelay)
			if multiplier > 0 {
				for range attempt {
					delay *= multiplier
				}
			}
			if maxDelay > 0 && time.Duration(delay) > maxDelay {
				delay = float64(maxDelay)
			}
			retrySleepMs = time.Duration(delay).Milliseconds()
		}
		db.ExecContext(ctx,
			"UPDATE microbus_steps SET status=?, changes=?, attempt=?, not_before=DATE_ADD_MILLIS(NOW_UTC(), ?), lease_expires=NOW_UTC(), updated_at=NOW_UTC(), interrupt_done=0, resume_data='{}', subgraph_done=0, subgraph_result='{}', subgraph_error='' WHERE step_id=?",
			workflow.StatusPending, string(changesJSON), attempt+1, retrySleepMs, stepID,
		)
		if retrySleepMs > 0 {
			e.shortenNextPoll(time.Now().Add(time.Duration(retrySleepMs) * time.Millisecond))
		} else {
			e.enqueueStep(ctx, shardNum, stepID)
		}
		return nil
	}

	// Complete the step
	if errorRouted {
		e.logger.LogDebug(ctx, "Task error routed", "task", taskName, "flow", workflowName)
	} else {
		e.logger.LogDebug(ctx, "Task completed", "task", taskName, "flow", workflowName)
	}
	gotoTarget := resultFlow.GotoRequested()
	stepRes, err := db.ExecContext(ctx,
		"UPDATE microbus_steps SET status=?, changes=?, goto_next=?, updated_at=NOW_UTC() WHERE step_id=? AND status!=?",
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
		nextTasks, err = evaluateErrorTransitions(graph, taskName, resultFlow, errStatusCode)
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
			return e.completeFlowSequential(ctx, shardNum, db, flowID, flowToken, stepID, strings.TrimSpace(notifyHostname), workflowName)
		}
		return e.fireFanInDirect(ctx, shardNum, db, flowID, stepID, stepDepth, lineageID, fanInTarget, sleepDur, flowPriority, flowFairnessKey, flowFairnessWeight)
	}

	if cohortSize == 0 {
		return e.completeFlowSequential(ctx, shardNum, db, flowID, flowToken, stepID, strings.TrimSpace(notifyHostname), workflowName)
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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	tx.ExecContext(ctx, "UPDATE microbus_flows SET updated_at=NOW_UTC() WHERE flow_id=?", flowID)

	childInputState, _ := workflow.MergeState(state, accumulatedChanges, nil)
	childInputJSON, _ := json.Marshal(childInputState)

	nextStepDepth := stepDepth + 1
	sleepMs := sleepDur.Milliseconds()
	var newStepIDs []int

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
		nextTimeBudget := e.taskTimeBudget()
		newStepID, err := tx.InsertReturnID(ctx, "step_id",
			"INSERT INTO microbus_steps (flow_id, step_depth, step_token, task_name, state, status, parked, time_budget_ms, lineage_id, fan_out_ordinal, predecessor_id, not_before, priority, fairness_key, fairness_weight)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?)",
			flowID, nextStepDepth, randomIdentifier(16), next.taskName, string(stepStateJSON), workflow.StatusPending, e.initialParkedFor(next.taskName), nextTimeBudget.Milliseconds(), childLineageID, i, stepID, sleepMs, flowPriority, flowFairnessKey, flowFairnessWeight,
		)
		if err != nil {
			return errors.Trace(err)
		}
		newStepIDs = append(newStepIDs, int(newStepID))
	}

	if len(newStepIDs) > 0 {
		tx.ExecContext(ctx, "UPDATE microbus_steps SET successor_id=? WHERE step_id=?", newStepIDs[0], stepID)
	}

	if isPushTransition {
		tx.ExecContext(ctx, "UPDATE microbus_steps SET cohort_size=? WHERE step_id=?", cohortSize, stepID)
		e.logger.LogDebug(ctx, "Fan-out cohort spawned", "flow", flowID, "spawnStep", stepID, "task", taskName, "cohortSize", cohortSize)
	}

	flowFailed := false
	var flowFailedErr, flowFailedFinalState string
	if fanInArrivals > 0 {
		tx.ExecContext(ctx, "UPDATE microbus_steps SET cohort_arrivals = cohort_arrivals + ? WHERE step_id=?", fanInArrivals, cohortSpawnID)
		var arrivals, size, failures, spawnLineageID int
		err = tx.QueryRowContext(ctx,
			"SELECT cohort_arrivals, cohort_size, cohort_failures, lineage_id FROM microbus_steps WHERE step_id=?",
			cohortSpawnID,
		).Scan(&arrivals, &size, &failures, &spawnLineageID)
		if err != nil {
			return errors.Trace(err)
		}
		fullyResolved := size > 0 && arrivals >= size
		if fullyResolved && failures == 0 {
			fanInStepID, err := e.insertFanInStep(ctx, tx, flowID, nextStepDepth, cohortSpawnID, stepID, fanInTaskName, graph, sleepMs, flowPriority, flowFairnessKey, flowFairnessWeight)
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
					"SELECT error FROM microbus_steps WHERE flow_id=? AND status=? AND error!='' LIMIT 1",
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
					"UPDATE microbus_flows SET final_state=?, status=?, error=?, updated_at=NOW_UTC() WHERE flow_id=? AND status NOT IN (?, ?, ?)",
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
		tx.ExecContext(ctx, "UPDATE microbus_flows SET step_id=?, updated_at=NOW_UTC() WHERE flow_id=?", nextFlowStepID, flowID)
	}
	err = tx.Commit()
	if err != nil {
		return errors.Trace(err)
	}

	if flowFailed {
		compositeID := fmt.Sprintf("%d-%d-%s", shardNum, flowID, flowToken)
		notifyHostnameTrimmed := strings.TrimSpace(notifyHostname)
		if notifyHostnameTrimmed != "" && e.flowStoppedCallback != nil {
			var finalState map[string]any
			json.Unmarshal([]byte(flowFailedFinalState), &finalState)
			e.flowStoppedCallback(ctx, notifyHostnameTrimmed, &workflow.FlowOutcome{
				FlowKey: compositeID,
				Status:  workflow.StatusFailed,
				State:   finalState,
				Error:   flowFailedErr,
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

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	flowPlaceholders := strings.Repeat("?,", len(chainFlowIDs)-1) + "?"
	flowArgs := append([]any{workflow.StatusInterrupted}, chainFlowIDs...)
	flowArgs = append(flowArgs, workflow.StatusRunning, workflow.StatusInterrupted)
	tx.ExecContext(ctx,
		"UPDATE microbus_flows SET status=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status IN (?, ?)",
		flowArgs...,
	)

	allStepIDs := append([]any{stepID}, chainStepIDs...)
	stepPlaceholders := strings.Repeat("?,", len(allStepIDs)-1) + "?"
	stepArgs := append([]any{workflow.StatusInterrupted}, allStepIDs...)
	stepArgs = append(stepArgs, workflow.StatusRunning, workflow.StatusInterrupted)
	tx.ExecContext(ctx,
		"UPDATE microbus_steps SET status=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN ("+stepPlaceholders+") AND status IN (?, ?)",
		stepArgs...,
	)
	tx.ExecContext(ctx, "UPDATE microbus_steps SET breakpoint_hit=1 WHERE step_id=?", stepID)

	err = tx.Commit()
	if err != nil {
		return errors.Trace(err)
	}

	for _, compositeID := range chainCompositeIDs {
		e.signalStop(ctx, compositeID, workflow.StatusInterrupted)
	}

	rootCompositeID := chainCompositeIDs[len(chainCompositeIDs)-1]
	rootFlowID := chainFlowIDs[len(chainFlowIDs)-1]
	if e.flowStoppedCallback != nil {
		var rootNotifyHostname string
		db.QueryRowContext(ctx, "SELECT notify_hostname FROM microbus_flows WHERE flow_id=?", rootFlowID).Scan(&rootNotifyHostname)
		rootNotifyHostname = strings.TrimSpace(rootNotifyHostname)
		if rootNotifyHostname != "" {
			e.flowStoppedCallback(ctx, rootNotifyHostname, &workflow.FlowOutcome{
				FlowKey: rootCompositeID,
				Status:  workflow.StatusInterrupted,
			})
		}
	}
	return nil
}

// handleInterrupt pauses a flow for external input.
func (e *Engine) handleInterrupt(ctx context.Context, shardNum int, db *sequel.DB, stepID int, flowID int, flowToken string, changesJSON []byte, interruptPayload map[string]any) error {
	chainFlowIDs, chainStepIDs, chainCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	flowPlaceholders := strings.Repeat("?,", len(chainFlowIDs)-1) + "?"
	flowArgs := append([]any{workflow.StatusInterrupted}, chainFlowIDs...)
	flowArgs = append(flowArgs, workflow.StatusRunning, workflow.StatusInterrupted)
	tx.ExecContext(ctx,
		"UPDATE microbus_flows SET status=?, updated_at=NOW_UTC() WHERE flow_id IN ("+flowPlaceholders+") AND status IN (?, ?)",
		flowArgs...,
	)

	allStepIDs := append([]any{stepID}, chainStepIDs...)
	stepPlaceholders := strings.Repeat("?,", len(allStepIDs)-1) + "?"
	stepArgs := []any{stepID, string(changesJSON), stepID, workflow.StatusInterrupted, parkedNone}
	stepArgs = append(stepArgs, allStepIDs...)
	stepArgs = append(stepArgs, workflow.StatusRunning, workflow.StatusInterrupted)
	tx.ExecContext(ctx,
		"UPDATE microbus_steps SET changes=CASE WHEN step_id=? THEN ? ELSE changes END, interrupt_done=CASE WHEN step_id=? THEN 1 ELSE interrupt_done END, status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN ("+stepPlaceholders+") AND status IN (?, ?)",
		stepArgs...,
	)

	if len(interruptPayload) > 0 {
		payloadJSON, _ := json.Marshal(interruptPayload)
		payloadArgs := []any{string(payloadJSON)}
		payloadArgs = append(payloadArgs, allStepIDs...)
		tx.ExecContext(ctx,
			"UPDATE microbus_steps SET interrupt_payload=? WHERE step_id IN ("+stepPlaceholders+") AND interrupt_payload='{}'",
			payloadArgs...,
		)
	}

	err = tx.Commit()
	if err != nil {
		return errors.Trace(err)
	}

	for _, compositeID := range chainCompositeIDs {
		e.signalStop(ctx, compositeID, workflow.StatusInterrupted)
	}

	rootCompositeID := chainCompositeIDs[len(chainCompositeIDs)-1]
	rootFlowID := chainFlowIDs[len(chainFlowIDs)-1]
	if e.flowStoppedCallback != nil {
		var rootNotifyHostname string
		db.QueryRowContext(ctx, "SELECT notify_hostname FROM microbus_flows WHERE flow_id=?", rootFlowID).Scan(&rootNotifyHostname)
		rootNotifyHostname = strings.TrimSpace(rootNotifyHostname)
		if rootNotifyHostname != "" {
			e.flowStoppedCallback(ctx, rootNotifyHostname, &workflow.FlowOutcome{
				FlowKey:          rootCompositeID,
				Status:           workflow.StatusInterrupted,
				InterruptPayload: interruptPayload,
			})
		}
	}
	return nil
}

// handleBackpressure bounces a step back to pending after a 429.
func (e *Engine) handleBackpressure(ctx context.Context, shardNum, stepID int, taskName string) error {
	e.valveRegulate(ctx, taskName)
	db, err := e.shard(shardNum)
	if err != nil {
		return errors.Trace(err)
	}
	res, err := db.ExecContext(ctx,
		"UPDATE microbus_steps SET status=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
		workflow.StatusPending, stepID, workflow.StatusRunning,
	)
	if err != nil {
		return errors.Trace(err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil // step was cancelled / failed / completed by a concurrent path
	}
	e.logger.LogDebug(ctx, "Task backpressured (429)", "task", taskName, "step", stepID)
	e.shortenNextPoll(time.Now())
	return nil
}

// valveRegulate cuts wCong by 1 and gossips the new point.
func (e *Engine) valveRegulate(ctx context.Context, taskName string) {
	now := time.Now()
	e.valvesLock.Lock()
	v := e.valves[taskName]
	_, observed := v.throttle.Peek()
	if v.tCong.IsZero() || now.Sub(v.tCong) > time.Second {
		if observed > v.wCong {
			v.wCong = observed
		}
	}
	v.wCong = max(v.wCong-1, 1)
	v.tCong = now
	newW := v.wCong
	e.valvesLock.Unlock()
	// tCong was just set to now under the lock, so pass now for both.
	v.throttle.SetLimit(recoverRate(newW, now, now))
	if e.peerNotifier != nil {
		e.peerNotifier.SyncValve(ctx, taskName, newW, now)
	}
}

// handleBreakerTrip bounces a step and trips the breaker.
func (e *Engine) handleBreakerTrip(ctx context.Context, shardNum, stepID int, taskName, cause string) error {
	fresh, nextProbeAt := e.breakerTrip(taskName, cause)
	if fresh && e.peerNotifier != nil {
		e.peerNotifier.TripBreaker(ctx, taskName)
	}
	e.logger.LogDebug(ctx, "Task breaker tripped", "task", taskName, "step", stepID, "cause", cause, "fresh", fresh)
	// Demote the just-failed probe and re-elect a probe atomically. Doing the demote as a separate
	// statement before the bulk-park lets a contended bulk-park transaction roll back after the demote
	// has committed, stranding the task with zero parked=0 probes and no future trigger to re-elect one,
	// which permanently wedges recovery. Folding the demote into the bulk-park transaction makes the two
	// outcomes all-or-nothing. Retry on lock contention so a probe is re-elected before we return.
	const maxBulkParkAttempts = 5
	for attempt := range maxBulkParkAttempts {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * time.Millisecond)
		}
		err := e.breakerBulkPark(ctx, taskName, nextProbeAt, shardNum, stepID)
		if err == nil {
			break
		}
		if !sequel.IsLockContentionError(err) || attempt == maxBulkParkAttempts-1 {
			return errors.Trace(err)
		}
	}
	return nil
}

// breakerCommit advances the probe schedule when the genuinely-scheduled probe is dispatched.
func (e *Engine) breakerCommit(taskName string) {
	now := time.Now()
	e.breakersLock.Lock()
	defer e.breakersLock.Unlock()
	b, ok := e.breakers[taskName]
	if !ok || b.trippedAt.IsZero() {
		return
	}
	// Only advance when this dispatch is the scheduled probe (its probe time has arrived). A burst of
	// concurrent in-flight dispatches admitted just before the breaker tripped would otherwise each bump
	// probeAttempt, ramping the backoff straight to the cap and stalling recovery; those are not probes.
	if now.Before(b.nextProbeAt) {
		return
	}
	b.probeAttempt++
	b.nextProbeAt = now.Add(breakerProbeBackoff(b.probeAttempt))
	e.refreshNextProbeLocked()
}

// breakerClose flips a tripped breaker back to closed.
func (e *Engine) breakerClose(ctx context.Context, taskName string, shardNum int) {
	e.breakersLock.Lock()
	b, ok := e.breakers[taskName]
	if !ok || b.trippedAt.IsZero() {
		e.breakersLock.Unlock()
		return
	}
	b.trippedAt = time.Time{}
	b.probeAttempt = 0
	b.nextProbeAt = time.Time{}
	b.cause = ""
	e.refreshNextProbeLocked()
	e.breakersLock.Unlock()
	e.breakerBulkUnpark(ctx, taskName, shardNum)
}

// breakerBulkPark parks all pending steps for a tripped task and designates one probe per shard. The
// just-failed probe (failedStepID on failedShard, may be 0 when no step triggered the trip) is demoted
// from running into the held-back set inside the same transaction as the re-election, so a rollback can
// never leave the task with the probe demoted but no replacement elevated.
func (e *Engine) breakerBulkPark(ctx context.Context, taskName string, nextProbeAt time.Time, failedShard, failedStepID int) error {
	probeBackoffMs := time.Until(nextProbeAt).Milliseconds()
	if probeBackoffMs < 0 {
		probeBackoffMs = 0
	}
	return e.eachShard(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return errors.Trace(err)
		}
		defer tx.Rollback()
		if shard == failedShard && failedStepID != 0 {
			tx.ExecContext(ctx,
				"UPDATE microbus_steps SET status=?, parked=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
				workflow.StatusPending, parkedBreaker, failedStepID, workflow.StatusRunning,
			)
		}
		tx.ExecContext(ctx,
			"UPDATE microbus_steps SET parked=?, updated_at=NOW_UTC() WHERE task_name=? AND status=? AND parked IN (?,?)",
			parkedBreaker, taskName, workflow.StatusPending, parkedNone, parkedBreaker,
		)
		var probeID int
		err = tx.QueryRowContext(ctx,
			"SELECT step_id FROM microbus_steps WHERE task_name=? AND status=? AND parked=? ORDER BY created_at ASC, step_id ASC LIMIT_OFFSET(1, 0)",
			taskName, workflow.StatusPending, parkedBreaker,
		).Scan(&probeID)
		if err == sql.ErrNoRows {
			return tx.Commit()
		}
		if err != nil {
			return errors.Trace(err)
		}
		tx.ExecContext(ctx,
			"UPDATE microbus_steps SET parked=?, not_before=DATE_ADD_MILLIS(NOW_UTC(), ?), updated_at=NOW_UTC() WHERE step_id=? AND status=?",
			parkedNone, probeBackoffMs, probeID, workflow.StatusPending,
		)
		return tx.Commit()
	})
}

// breakerBulkUnpark releases breaker-parked steps for a task on a single shard.
func (e *Engine) breakerBulkUnpark(ctx context.Context, taskName string, shardNum int) {
	db, err := e.shard(shardNum)
	if err != nil {
		return
	}
	db.ExecContext(ctx,
		"UPDATE microbus_steps SET parked=?, updated_at=NOW_UTC() WHERE task_name=? AND parked=?",
		parkedNone, taskName, parkedBreaker,
	)
}

// fireFanInDirect creates the fan-in step immediately for an empty-cohort case.
func (e *Engine) fireFanInDirect(ctx context.Context, shardNum int, db *sequel.DB, flowID int, stepID int, stepDepth int, lineageID int, fanInTarget string, sleepDur time.Duration, priority int, fairnessKey string, fairnessWeight float64) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}
	defer tx.Rollback()

	tx.ExecContext(ctx, "UPDATE microbus_flows SET updated_at=NOW_UTC() WHERE flow_id=?", flowID)
	tx.ExecContext(ctx, "UPDATE microbus_steps SET cohort_size=0 WHERE step_id=?", stepID)

	var ourStateJSON, ourChangesJSON string
	tx.QueryRowContext(ctx, "SELECT state, changes FROM microbus_steps WHERE step_id=?", stepID).Scan(&ourStateJSON, &ourChangesJSON)
	var ourState, ourChanges map[string]any
	unmarshalJSONMap(ourStateJSON, &ourState)
	unmarshalJSONMap(ourChangesJSON, &ourChanges)
	mergedState, _ := workflow.MergeState(ourState, ourChanges, nil)
	mergedJSON, _ := json.Marshal(mergedState)

	nextStepDepth := stepDepth + 1
	sleepMs := sleepDur.Milliseconds()
	nextTimeBudget := e.taskTimeBudget()
	fanInStepID, err := tx.InsertReturnID(ctx, "step_id",
		"INSERT INTO microbus_steps (flow_id, step_depth, step_token, task_name, state, status, parked, time_budget_ms, lineage_id, predecessor_id, not_before, priority, fairness_key, fairness_weight)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?)",
		flowID, nextStepDepth, randomIdentifier(16), fanInTarget, string(mergedJSON), workflow.StatusPending, e.initialParkedFor(fanInTarget), nextTimeBudget.Milliseconds(), lineageID, stepID, sleepMs, priority, fairnessKey, fairnessWeight,
	)
	if err != nil {
		return errors.Trace(err)
	}
	tx.ExecContext(ctx, "UPDATE microbus_steps SET successor_id=? WHERE step_id=?", int(fanInStepID), stepID)
	tx.ExecContext(ctx, "UPDATE microbus_flows SET step_id=?, updated_at=NOW_UTC() WHERE flow_id=?", int(fanInStepID), flowID)
	err = tx.Commit()
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
func (e *Engine) insertFanInStep(ctx context.Context, tx sequel.Executor, flowID, nextStepDepth, cohortSpawnID, predecessorStepID int, fanInTaskName string, graph *workflow.Graph, sleepMs int64, priority int, fairnessKey string, fairnessWeight float64) (int, error) {
	var spawnStateJSON, spawnChangesJSON, spawnTaskName string
	var spawnLineageID int
	tx.QueryRowContext(ctx,
		"SELECT state, changes, lineage_id, task_name FROM microbus_steps WHERE step_id=?",
		cohortSpawnID,
	).Scan(&spawnStateJSON, &spawnChangesJSON, &spawnLineageID, &spawnTaskName)
	var spawnState, spawnChanges map[string]any
	unmarshalJSONMap(spawnStateJSON, &spawnState)
	unmarshalJSONMap(spawnChangesJSON, &spawnChanges)
	merged, _ := workflow.MergeState(spawnState, spawnChanges, graph.Reducers())

	rows, err := tx.QueryContext(ctx,
		"SELECT status, changes FROM microbus_steps WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_id",
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
	nextTimeBudget := e.taskTimeBudget()
	fanInStepID, err := tx.InsertReturnID(ctx, "step_id",
		"INSERT INTO microbus_steps (flow_id, step_depth, step_token, task_name, state, status, parked, time_budget_ms, lineage_id, predecessor_id, not_before, priority, fairness_key, fairness_weight)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?)",
		flowID, nextStepDepth, randomIdentifier(16), fanInTaskName, string(mergedJSON), workflow.StatusPending, e.initialParkedFor(fanInTaskName), nextTimeBudget.Milliseconds(), spawnLineageID, predecessorStepID, sleepMs, priority, fairnessKey, fairnessWeight,
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
			"UPDATE microbus_steps SET successor_id=? WHERE flow_id=? AND lineage_id=? AND task_name IN ("+placeholders+")",
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
