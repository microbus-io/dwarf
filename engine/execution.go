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
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/microbus-io/dwarf/internal/faninmap"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	// cohortLockStripesLog2 sizes e.cohortLocks at 2^13 = 8192 stripes (~64KB of sync.Mutex). Fixed, never
	// resized: a stripe array's mutual exclusion rests on a key always mapping to the same mutex instance,
	// which any resize would break for a holder in flight. The number is generous rather than tuned because
	// a collision is benign - two live cohorts sharing a stripe cost one arrival a brief connection-free
	// wait, never correctness - so there is no cliff to size against, only a birthday-bound to stay clear
	// of (expected collisions stay < 1 up to ~90 concurrent live cohorts on one peer, well past the pool).
	cohortLockStripesLog2 = 13
	cohortLockStripes     = 1 << cohortLockStripesLog2
)

// cohortLockStripe maps a cohort's spawn step to one of e.cohortLocks. Fibonacci hashing (multiply by the
// golden-ratio constant, keep the high bits) spreads the per-shard, near-sequential step ids so cohorts
// spaced by a constant stride do not cluster onto one stripe; the shard is mixed in because step-id
// sequences are per-shard, not global.
func cohortLockStripe(shard, spawnStepID int) int {
	h := (uint64(spawnStepID) ^ uint64(shard)*0x9E3779B97F4A7C15) * 0x9E3779B97F4A7C15
	return int(h >> (64 - cohortLockStripesLog2))
}

// processStep acquires a step, executes its task, and enqueues the next step if applicable.
//
// releasePermit hands back the admission permit the crew took on this worker's behalf. It is called on
// exactly one line - immediately before the host's ExecuteTask - because that is where this worker stops
// competing for a database connection. Everything before it (claim CAS, step read, flow/graph load) holds
// or awaits one; the host call holds nothing. The crew wraps it in sync.OnceFunc and calls it again on the
// way out, so every early return below is covered and a double call is a no-op.
func (e *Engine) processStep(ctx context.Context, shardNum int, stepID int, releasePermit func()) (err error) {
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
			e.seams.Checkpoint(ctx, CheckpointBeforeRecoveryReset)
			db.Transact(ctx, func(tx *sequel.Tx) error {
				// FaultRecoveryResetErr makes this last-resort reset itself fail (a non-retryable error, as if
				// it lost to a contention storm), so the step stays `completed` and the flow strands `running`
				// with every step terminal - the residual orphan hole that only detectOrphanedFlows surfaces.
				// Process-wide (no scope): taskName is declared after this defer, so it is not in scope here.
				if e.seams.IsFault(FaultRecoveryResetErr) {
					return errors.New("injected fault: " + FaultRecoveryResetErr)
				}
				_, terr := tx.ExecContext(ctx, resetSQL, resetArgs...)
				return terr
			})
		}
		// This step is being deliberately re-offered to the same replica, so drop its in-flight reservation
		// rather than let it linger the full window - otherwise the immediate re-poll below re-selects the
		// step and this replica skips its own re-dispatch until the reservation expires (up to ~2s). A peer
		// would still take it, but a solo replica would eat the delay. Safe in the fenced-zombie case too: a
		// peer that re-claimed owns it via its own tracker, and this relinquish at worst costs one lost CAS.
		e.claims.RelinquishClaim(shardNum, stepID)
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
	var taskName, stepToken string
	var stateJSON, priorChangesJSON []byte
	var attempt, lineageID, fanOutOrdinal, flowID, timeBudgetMs int
	var interruptDone bool
	var resumeDataJSON []byte
	var subgraphDone bool
	var subgraphResultJSON []byte
	var subgraphErrorStr string
	var stepCreatedAt time.Time
	var stateRefsJSON []byte

	switch db.DriverName() {
	case "pgx", "sqlite":
		err = db.QueryRowContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), lease_seq=lease_seq+1, engine_id=?, updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" WHERE step_id=? AND status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"+
				" RETURNING step_depth, task_name, step_token, state, changes, state_refs, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, created_at, lease_seq, fan_out_ordinal",
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &stateRefsJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq, &fanOutOrdinal)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	case "mssql":
		err = db.QueryRowContext(ctx,
			"UPDATE dwarf_steps SET status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), time_budget_ms + ?), lease_seq=lease_seq+1, engine_id=?, updated_at=NOW_UTC(),"+
				" started_at=CASE WHEN attempt>0 OR subgraph_done=1 OR interrupt_done=1 THEN started_at ELSE NOW_UTC() END"+
				" OUTPUT INSERTED.step_depth, INSERTED.task_name, INSERTED.step_token, INSERTED.state, INSERTED.changes, INSERTED.state_refs, INSERTED.attempt, INSERTED.lineage_id, INSERTED.flow_id, INSERTED.time_budget_ms, INSERTED.interrupt_done, INSERTED.resume_data, INSERTED.subgraph_done, INSERTED.subgraph_result, INSERTED.subgraph_error, INSERTED.created_at, INSERTED.lease_seq, INSERTED.fan_out_ordinal"+
				" WHERE step_id=? AND status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()",
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &stateRefsJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq, &fanOutOrdinal)
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
				"SELECT step_depth, task_name, step_token, state, changes, state_refs, attempt, lineage_id, flow_id, time_budget_ms, interrupt_done, resume_data, subgraph_done, subgraph_result, subgraph_error, created_at, lease_seq, fan_out_ordinal FROM dwarf_steps WHERE step_id=?",
				stepID,
			).Scan(&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &stateRefsJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq, &fanOutOrdinal)
			if readErr != nil && readErr != sql.ErrNoRows {
				err = readErr
			}
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	if n == 0 {
		// A lost claim is routine, not an error: the candidate cache holds hints, not ownership, so the row
		// may have been claimed by a peer or left the claimable state between selection and claim. Counted
		// because the RATE is the lease-contention signal - it is the one cost of overlapping candidate
		// selection that no other instrument sees (refill waste counts candidates dropped before dispatch,
		// not round trips spent losing the CAS).
		//
		// Gated on n==0 ALONE, not on the flowID==0 arm below: on the MySQL path a successful claim (n==1)
		// whose follow-up read finds no row leaves flowID at 0, and counting that as a lost claim would
		// attribute a read miss to contention - in the one instrument whose whole purpose is attribution.
		e.metricStepClaimLost(ctx)
		return nil
	}
	if flowID == 0 {
		return nil
	}

	// Read flow data
	var flowToken, flowStatus, workflowURL, traceParent string
	var graphJSON, baggageJSON []byte
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
		err = json.Unmarshal(graphJSON, parsed)
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
	state, _ := workflow.NewState(stateJSON)
	// Materialize any field this step carries BY REFERENCE (see staterefs.go). Resolving here, once, is what
	// keeps the ref encoding out of everything downstream: the carrier, `when` evaluation, forEach expansion,
	// the transport to a remote task, and the transition machinery all work on literals and never learn refs
	// exist. The refs the step inherited are kept, because minting the successors' state needs to know which
	// fields arrived as refs (and so must be carried, never re-anchored against this step). resolveStateRefs
	// mutates the state map in place, so it gets the live backing map.
	inheritedRefs := staterefs.Parse(stateRefsJSON)
	if err := e.resolveStateRefs(ctx, db, shardNum, state, inheritedRefs, nil, workflowURL); err != nil {
		err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
		return errors.Trace(err)
	}
	priorChanges, _ := workflow.NewState(priorChangesJSON)
	// The carrier's input is state + priorChanges, materialized: Merge accumulates (keeping a prior
	// delete's tombstone), then DelNils enacts it so the carrier sees the key absent. Clone so `state`
	// stays the pristine resolved snapshot the successor mint re-uses below.
	mergedInputState := state.Clone()
	_ = mergedInputState.Merge(priorChanges)
	mergedInputState.DelNils()
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
	flow.SetFlowKey(keys.New(shardNum, flowID, flowToken))
	flow.SetStepKey(keys.New(shardNum, stepID, stepToken))

	if interruptDone {
		resumeData, _ := workflow.NewState(resumeDataJSON)
		flow.SetInterruptResolution(resumeData)
	}
	if subgraphDone {
		subgraphResult, _ := workflow.NewState(subgraphResultJSON)
		flow.SetSubgraphResolution(subgraphResult, subgraphErrorStr)
	}

	// Parse baggage for the task executor. A "null" column (from a nil FlowOptions.Baggage) and an empty
	// column both yield an empty State, delivered as "no baggage".
	baggage, _ := workflow.NewState(baggageJSON)

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
	// Hand the admission permit back: from here to the end of ExecuteTask this worker holds no database
	// connection, so continuing to hold a permit would bound admission on a worker that is not competing
	// for anything. This is the one line the release belongs on, and it is not deferred - a defer at this
	// function's scope would hold the permit across the persist half below, which DOES hold a connection
	// and is deliberately gated by a debit instead.
	releasePermit()
	// A panic in the in-process host is caught here so it flows through the normal error disposition
	// rather than wedging this leased step until lease expiry.
	execErr := errors.CatchPanic(func() error {
		// FaultPanicExecuteTask panics inside the wrapper so it exercises the host-call panic isolation
		// (caught here, routed as a normal task error), scoped to this task name.
		if e.seams.IsFault(FaultPanicExecuteTask, taskName) {
			panic("injected fault: " + FaultPanicExecuteTask + " " + taskName)
		}
		return e.host.ExecuteTask(taskCtx, dispatchURL, &flow.Flow)
	})
	// The task has now RUN. Everything below records that fact, and it holds a connection to do so - so the
	// persist half takes a permit from the EXIT reservation, which nothing entering a step can draw on.
	//
	// That dedication is what lets this simply block. Sharing one pool between the two populations forces a
	// choice about who wins, and both answers were measured failing: served evenly, completions lose at
	// random and queue behind admission; served with completions preferred, dispatch starves and short-task
	// throughput collapses 3x. With its own reservation a completion waits only behind other completions,
	// which are themselves finishing, so the wait is bounded by the shard's own service rate - and what
	// bounds the worst case (every in-flight task unblocking at once) is workerCeiling, not this permit.
	//
	// !ok means the engine is DRAINING, and the work is recorded anyway: the outcome exists nowhere but in
	// this goroutine, and the release is a no-op then, so the deferred call is safe either way. The release
	// is mandatory and has no crew-side backstop (unlike the entry permit, which the crew re-releases on the
	// way out): a lost one shrinks this shard's exit reservation permanently.
	permitWaitStart := time.Now()
	persistRelease, _ := e.permits.AcquireExit(shardNum)
	defer persistRelease()
	// Timed HERE rather than inside the permit set, which holds no metrics. Recorded on EVERY completion,
	// not only the ones that waited: count is then completions and sum/count the mean, so the exit side
	// becoming the constraint shows up as a shift in the distribution - where the free-permit gauge, being
	// instantaneous, can miss a reservation that was saturated all window without being sampled empty.
	e.metrics.exitWait.Record(ctx, time.Since(permitWaitStart).Seconds(),
		metric.WithAttributes(attribute.String("shard", strconv.Itoa(shardNum))))

	if execErr == nil && e.seams.IsFault(FaultExecuteTask, taskName) {
		execErr = errors.New("injected fault: "+FaultExecuteTask+" "+taskName, http.StatusInternalServerError)
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
	var accumulatedChanges workflow.State
	var changesJSON []byte
	rawChanges := resultFlow.RawChanges()
	if len(rawChanges) == 0 {
		accumulatedChanges = priorChanges
		changesJSON = priorChangesJSON
	} else {
		// Accumulation, not materialization: Merge preserves a cleared (null) entry as a pending-delete
		// marker. It is enacted (via DelNils) only when the changes later fold onto state below.
		accumulatedChanges = priorChanges.Clone()
		_ = accumulatedChanges.Merge(rawChanges)
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
		e.metricStepExecuted(ctx, taskName, workflow.StatusInterrupted, shardNum)
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
		if lerr == nil && e.seams.IsFault(FaultLoadGraph, subgraphURL) {
			lerr = errors.New("injected fault: "+FaultLoadGraph+" "+subgraphURL, http.StatusInternalServerError)
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
			changesJSON, parkedSubgraph, stepID, leaseSeq,
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
		e.seams.Checkpoint(ctx, CheckpointAfterCallerPark)
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
		e.metricStepExecuted(ctx, taskName, "subgraph", shardNum)
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
		e.seams.Checkpoint(ctx, CheckpointBeforeRetryRewind)
		var rewound bool
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			res, execErr := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, changes=?, attempt=?, not_before=DATE_ADD_MILLIS(NOW_UTC(), ?), lease_expires=NOW_UTC(), updated_at=NOW_UTC(), interrupt_done=0, resume_data=?, subgraph_done=0, subgraph_result=?, subgraph_error='' WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND lease_seq=?",
				workflow.StatusPending, changesJSON, attempt+1, retrySleepMs, emptyJSON, emptyJSON, stepID, leaseSeq,
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
		// The step was rewound in place for re-dispatch; drop this replica's reservation so the immediate
		// re-offer below (enqueueStepDue on a zero-backoff retry) is not skipped by its own still-live entry.
		e.claims.RelinquishClaim(shardNum, stepID)
		e.metricStateWriteBytes(ctx, workflowURL, "changes", len(changesJSON))
		if retrySleepMs == 0 {
			// The step was reset in place (same row, same denormalized priority) due now. A BACKED-OFF
			// retry needs nothing here: its not_before is in the future, so the piston's scan picks it up
			// within a cycle of coming due, and offering it now would only cache a hint no worker may claim.
			e.enqueueStepDue(ctx, shardNum, stepID, flowPriority)
		}
		e.metricStepExecuted(ctx, taskName, "retried", shardNum)
		return nil
	}

	// Complete the step
	if errorRouted {
		e.logger.DebugContext(ctx, "Task error routed", "task", taskName, "workflow", workflowURL)
		e.metricStepExecuted(ctx, taskName, "error_routed", shardNum)
	} else {
		e.logger.DebugContext(ctx, "Task completed", "task", taskName, "workflow", workflowURL)
		e.metricStepExecuted(ctx, taskName, workflow.StatusCompleted, shardNum)
	}
	// FaultLeaseStaleWrite makes this completion write carry a stale lease generation, exactly as a zombie
	// worker (whose lease was re-granted to a peer) would. The fence must reject it (zero rows -> benign
	// no-op below), so the step stays claimable and lease recovery re-runs it cleanly - the test proves a
	// late/slow worker's write can never corrupt or terminalize a flow a peer is healthily re-executing.
	writeSeq := leaseSeq
	if e.seams.IsFault(FaultLeaseStaleWrite, taskName) {
		writeSeq = leaseSeq - 1
	}
	// THE task has already run - its side effects have fired - so this write is the only record that it did.
	// A database error here must therefore retry the WRITE, never the task (see persist): an ephemeral blip is
	// absorbed with zero re-execution, and a write that will never land is classified and terminalized rather
	// than left to lease recovery, which would re-execute the task every `budget + leaseMargin`, forever.
	var stepRowsAffected int64
	err = e.persist(ctx, db, shardNum, stepID, leaseSeq, func() error {
		// FaultPersistErr makes this write fail with a synthetic NON-contention error, consumed per attempt -
		// so InjectN(1) is a transient blip the retry must absorb with NO re-execution, and InjectN(large) is a
		// permanent failure the classifier must terminalize rather than loop on.
		if e.seams.IsFault(FaultPersistErr, taskName) {
			return errors.New("injected fault: " + FaultPersistErr + " " + taskName)
		}
		res, werr := db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, changes=?, updated_at=NOW_UTC() WHERE step_id=? AND status!='"+workflow.StatusCancelled+"' AND lease_seq=?",
			workflow.StatusCompleted, changesJSON, stepID, writeSeq,
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
			return e.fireFanInDirect(ctx, shardNum, db, flowID, stepID, stepDepth, lineageID, fanOutOrdinal, fanInOfSource, dispatchURLOf(graph, fanInOfSource), workflowURL, graph, sleepDur, flowPriority, flowFairnessKey, flowFairnessWeight, flowTimeBudgetMs)
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

	// The successors' base state is state + accumulated changes, materialized: Merge accumulates, then
	// DelNils enacts the changes' delete tombstones so they are absent downstream. Clone leaves `state`
	// pristine; a forEach branch re-mints per element below.
	childInputState := state.Clone()
	_ = childInputState.Merge(accumulatedChanges)
	childInputState.DelNils()
	// Mint state refs for the successors: a field big enough to be worth an anchor is omitted from their
	// `state` and recorded in `state_refs` instead, pointing at THIS step (which holds the bytes in its
	// `changes` if the task just wrote them, or in its `state` if it merely carried them). A field that
	// arrived here as a ref keeps that ref. The bar scales with the fan-out width - see mintStateRefs.
	//
	// The linear/static-fan-out successors all share this one snapshot, so it is minted once; a forEach
	// branch re-mints because its state carries the injected element (below).
	linearStateJSON, linearRefsJSON, err := e.linkerFor(shardNum).Mint(childInputState, accumulatedChanges, inheritedRefs, stepID, len(normalNexts), nil)
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

	// Test checkpoint: a breakpoint here freezes the worker after the step is marked completed but before
	// the transition transaction, so a test can Cancel the flow in exactly the window the transition's
	// write-first terminal-status guard exists to survive (the transition must become a no-op, inserting no
	// successors into the cancelled flow).
	e.seams.Checkpoint(ctx, CheckpointBeforeTransitionTx)

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
	//
	// Serialize this cohort's arrivals through a per-peer stripe (e.cohortLocks) BEFORE the transition tx
	// opens - the point a losing sibling would otherwise take its connection and then queue on the spawn
	// row's write lock, tying that connection up until the holder commits. cohortSpawnID (the dispatched
	// step's lineage_id) is already in hand from the claim, so the key costs no read. It is released the
	// instant persist returns (below), NOT at function exit: a non-contention persist error runs
	// failOnPersistError -> failStep on this SAME stripe, and sync.Mutex is not reentrant. The deferred
	// unlock is only the panic backstop; the normal release is the explicit one before the switch.
	var cohortMu *sync.Mutex
	if fanInArrivals > 0 && cohortSpawnID != 0 {
		cohortMu = &e.cohortLocks[cohortLockStripe(shardNum, cohortSpawnID)]
		cohortMu.Lock()
		defer func() {
			if cohortMu != nil {
				cohortMu.Unlock()
			}
		}()
	}
	err = e.persist(ctx, db, shardNum, stepID, leaseSeq, func() error {
		return db.Transact(ctx, func(tx *sequel.Tx) error {
			newStepIDs = newStepIDs[:0]
			flowFailed = false
			txBytes = stateByteCount{}
			flowFailedParentStepID, flowFailedReDispatchParent = 0, false

			// FaultContention returns a retryable lock-contention error (consumed on the first attempt), so the
			// test proves Transact rolls back and re-runs the closure to a clean commit. FaultTransitionCommit
			// returns a non-retryable error, so the tx fails after the step was already marked completed and the
			// processStep recovery defer must reset it (completed->pending) and re-dispatch. Both are scoped to
			// the completing task and checked before the flow-row write, so a fired fault rolls back nothing.
			if e.seams.IsFault(FaultContention, taskName) {
				return errors.New("database is locked (injected fault: " + FaultContention + " " + taskName + ")")
			}
			if e.seams.IsFault(FaultTransitionCommit, taskName) {
				return errors.New("injected fault: " + FaultTransitionCommit + " " + taskName)
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
			//
			// Reports whether the flow is still non-terminal; a false return means the caller must bail (nil).
			flowRowLocked := false
			lockFlowRow := func() (bool, error) {
				if e.seams.Enabled() { // counting checkpoint; Enabled gates the throwaway scope string in production
					e.seams.Checkpoint(ctx, CheckpointFlowRowWrite, strconv.Itoa(flowID))
				}
				flowRes, flowErr := tx.ExecContext(ctx,
					"UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=? AND status NOT IN ('"+workflow.StatusCompleted+"', '"+workflow.StatusFailed+"', '"+workflow.StatusCancelled+"')",
					flowID,
				)
				if flowErr != nil {
					return false, errors.Trace(flowErr)
				}
				n, _ := flowRes.RowsAffected()
				flowRowLocked = n > 0
				return flowRowLocked, nil
			}

			// A NON-FINAL cohort arrival is the one disposition that neither extends nor terminalizes the flow:
			// it inserts no successor, advances no step_id, and its entire durable effect is the cohort_arrivals
			// bump on the SPAWN step below. Grabbing the flow row for it serializes EVERY sibling of a cohort on
			// one row - measured at fan-out width 64: 20 of 43 active backends queued on this exact statement,
			// each holding a pool connection - to perform two writes that carry no information: the grab itself,
			// and a step_id rewrite of 0 over 0 (step_id is already 0 throughout a fan-out). So the grab is
			// DEFERRED to the arrival that actually resolves the cohort, which is the only one that inserts a
			// fan-in step or fails the flow. This mirrors failStep/propagateCohortFailure - the failure-side twin
			// of this path - which already bumps the same counters steps-first with no flow-row lock at all.
			//
			// Three properties are preserved, not traded:
			//   - Write-first still holds: the bump IS a write, so it is the transaction's first statement and
			//     the SQLite SHARED->EXCLUSIVE upgrade deadlock stays closed (the same argument that makes
			//     handleInterrupt legitimately steps-first).
			//   - The terminal-status guard is MOVED, never weakened: it still runs, unchanged, in front of
			//     every write that can extend or terminalize the flow.
			//   - Exactly-one-resolver is unaffected: it comes from the spawn row's write lock, held across the
			//     bump and the re-read below within one transaction - never from the flow row.
			// isPushTransition is excluded because a fan-out SOURCE writes cohort_size to its own step row and
			// may carry successors, so it is not a pure arrival.
			pureArrival := fanInArrivals > 0 && len(normalNexts) == 0 && !isPushTransition
			if !pureArrival {
				ok, err := lockFlowRow()
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}

			for i, next := range normalNexts {
				// fan_out_ordinal identifies the BRANCH within a cohort so the fan-in folds order-sensitive
				// reducers (append/concat/union) in input-array order. A fan-out SOURCE stamps each new branch
				// with its spawn-loop position i. A step CONTINUING an existing branch (a single linear/goto
				// successor, i is always 0 here) must instead INHERIT its own ordinal, or every second-or-later
				// step of the branch lands in the ordinal-0 bucket and folds by completion order. A trunk step's
				// ordinal is 0 and irrelevant.
				successorOrdinal := i
				if !isPushTransition {
					successorOrdinal = fanOutOrdinal
				}
				stepStateJSON, stepRefsJSON := linearStateJSON, linearRefsJSON
				if next.itemKey != "" {
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
					stepStateJSON, stepRefsJSON, mintErr = e.linkerFor(shardNum).Mint(perStepState, accumulatedChanges, inheritedRefs, stepID, len(normalNexts), noRef)
					if mintErr != nil {
						return errors.Trace(mintErr)
					}
				}
				nextURL := dispatchURLOf(graph, next.taskName)
				newStepID, err := tx.InsertReturnID(ctx, "step_id",
					"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_refs, status, parked, time_budget_ms, lineage_id, fan_out_ordinal, predecessor_id, not_before, priority, fairness_key, fairness_weight, engine_id)"+
						" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?)",
					flowID, nextStepDepth, keys.RandomIdentifier(16), next.taskName, nextURL, stepStateJSON, stepRefsJSON, workflow.StatusPending, parkedNone, flowTimeBudgetMs, childLineageID, successorOrdinal, stepID, sleepMs, flowPriority, flowFairnessKey, flowFairnessWeight, e.engineID,
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
				// Bump the cohort counter and read the post-bump values in ONE round-trip where the driver
				// supports it (the same RETURNING/OUTPUT split as the claim CAS above).
				//
				// This is a CONTENTION fix, not merely a round-trip saving. The bump takes the spawn row's
				// write lock and holds it until COMMIT, so every sibling of a cohort queues on it - and the
				// queue drains at the rate of the LOCK HOLD, i.e. the work still outstanding after the bump
				// returns. Dropping this read from the hold shortens it by roughly a third, and each sibling
				// waiting behind the lock is paid that saving. Measured on PostgreSQL at fan-out width 64:
				// the cohort-row statement went 10.1ms -> 6.7ms and the whole arrival transaction 11.3ms ->
				// 7.9ms, for +7-13% fan-out throughput and a halved p99 on the statement.
				//
				// The two forms are equivalent: RETURNING/OUTPUT INSERTED yields the row AS UPDATED, which is
				// exactly what the follow-up SELECT read inside this transaction. cohort_size/cohort_failures/
				// lineage_id are untouched by this statement, and cohort_arrivals is read post-increment either
				// way. A zero-row match yields sql.ErrNoRows from both forms, preserving the existing error path
				// (the trunk-step-into-a-fan-in case, guarded above).
				var arrivals, size, failures, spawnLineageID int
				var err error
				switch db.DriverName() {
				case "pgx", "sqlite":
					err = tx.QueryRowContext(ctx,
						"UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals + ? WHERE step_id=?"+
							" RETURNING cohort_arrivals, cohort_size, cohort_failures, lineage_id",
						fanInArrivals, cohortSpawnID,
					).Scan(&arrivals, &size, &failures, &spawnLineageID)
				case "mssql":
					err = tx.QueryRowContext(ctx,
						"UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals + ?"+
							" OUTPUT INSERTED.cohort_arrivals, INSERTED.cohort_size, INSERTED.cohort_failures, INSERTED.lineage_id"+
							" WHERE step_id=?",
						fanInArrivals, cohortSpawnID,
					).Scan(&arrivals, &size, &failures, &spawnLineageID)
				default:
					// MySQL lacks RETURNING, so the bump and the read stay two statements. They run in one
					// transaction on one connection, so they are already serial and the read still sees the
					// post-bump value; only the shorter lock hold is unavailable there.
					tx.ExecContext(ctx, "UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals + ? WHERE step_id=?", fanInArrivals, cohortSpawnID)
					err = tx.QueryRowContext(ctx,
						"SELECT cohort_arrivals, cohort_size, cohort_failures, lineage_id FROM dwarf_steps WHERE step_id=?",
						cohortSpawnID,
					).Scan(&arrivals, &size, &failures, &spawnLineageID)
				}
				if err != nil {
					return errors.Trace(err)
				}
				fullyResolved := size > 0 && arrivals >= size
				// The cohort resolves here, so this arrival is about to extend (fan-in step) or terminalize
				// (cohort failure) the flow - exactly the writes the terminal guard protects. A deferred grab is
				// taken now, before either, so the guard sits in front of them just as it did when every arrival
				// grabbed the row. A zero-row match means Cancel/failStep won the race: bail as a clean no-op.
				if fullyResolved && !flowRowLocked {
					ok, lerr := lockFlowRow()
					if lerr != nil {
						return lerr
					}
					if !ok {
						return nil
					}
				}
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
			// Gated on holding the flow row: an unresolved pure arrival never took the grab, and its write here
			// would have been step_id=0 over the 0 a fan-out already carries - pure WAL amplification on the
			// flow's hottest row (two extra tuple versions per sibling; 126 per 64-wide cohort) plus the lock
			// hold that serializes the cohort. Every path that DOES advance the flow holds the row by here.
			if !flowFailed && flowRowLocked {
				if e.seams.Enabled() { // counting checkpoint; Enabled gates the throwaway scope string in production
					e.seams.Checkpoint(ctx, CheckpointFlowRowWrite, strconv.Itoa(flowID))
				}
				tx.ExecContext(ctx, "UPDATE dwarf_flows SET step_id=?, touch=1-touch WHERE flow_id=?", nextFlowStepID, flowID)
			}
			return nil
		})
	})
	if cohortMu != nil {
		// Release before the post-commit switch: a non-contention error routes to failOnPersistError ->
		// failStep, which re-locks this same stripe, and sync.Mutex is not reentrant. The arrival's DB work
		// is committed by here, so no sibling needs us to hold it any longer.
		cohortMu.Unlock()
		cohortMu = nil
	}
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
			// completing last arriver (this path) would wait on the latch detector to read the stop instead.
			e.signalStop(ctx, keys.New(shardNum, flowID, flowToken), workflow.StatusFailed)
			if flowFailedReDispatchParent {
				e.enqueueStep(ctx, shardNum, flowFailedParentStepID)
			}
			return nil
		}
		compositeID := keys.New(shardNum, flowID, flowToken)
		e.signalStop(ctx, compositeID, workflow.StatusFailed)
		return nil
	}

	// A sleeping successor is left to the piston: its not_before is in the future, so it is invisible to
	// selection until it comes due and then visible to the very next cycle.
	if sleepDur == 0 && len(newStepIDs) > 0 {
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
		stepArgs := []any{stepID, changesJSON, stepID, workflow.StatusInterrupted, parkedNone}
		stepArgs = append(stepArgs, allStepIDs...)
		chainSQL := "UPDATE dwarf_steps SET changes=CASE WHEN step_id=? THEN ? ELSE changes END, interrupt_done=CASE WHEN step_id=? THEN 1 ELSE interrupt_done END, status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN (" + stepPlaceholders + ") AND status IN ('" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')"
		// FaultInterruptChainWrite reproduces a deadlock victim dialect-independently: the statement errors and
		// applies NOTHING, so the leaf stays `running` - the shape that used to read back as a genuine fence.
		if e.seams.IsFault(FaultInterruptChainWrite) {
			chainSQL, stepArgs = "UPDATE dwarf_steps SET no_such_column=1 WHERE step_id=?", []any{stepID}
		}
		tx.ExecContext(ctx, chainSQL, stepArgs...)

		// Lease fence - on BOTH the generation AND the leaf's own transition. The combined UPDATE above locked
		// and re-parked the whole chain (in PK order, before any flow row - the ordering the comment guards),
		// but its leaf write is conditional on status IN ('running','interrupted'). There are two ways the leaf
		// can fail to be ours, and committing the chain re-park WITHOUT the leaf strands the parent - the ancestor
		// callers are flipped out of parkedSubgraph, so deliverFlowFailureToParent (which needs status='running'
		// AND parked=parkedSubgraph) can no longer reach them:
		//   - a PEER re-claimed the leaf: the combined UPDATE leaves lease_seq untouched, so the re-claim shows up
		//     here as a bumped generation on the leaf;
		//   - lease recovery reset the leaf to `pending`: recovery does NOT bump lease_seq, so the generation
		//     still matches - but the combined UPDATE matched the leaf ZERO rows (pending is not in its status
		//     list), leaving it pending/claimable with no interrupt_done/changes while the chain around it was
		//     re-parked and the chain flows marked interrupted.
		// So require that our UPDATE actually moved the leaf to `interrupted`, not merely that the generation
		// matches. On either failure roll the entire interrupt back (undoing the ancestor re-park) and abandon;
		// the peer that (re-)claims the pending leaf then performs the interrupt in full. The check reads the leaf
		// inside the tx (not a same-table subquery, which MySQL rejects on an UPDATE target) after the lock.
		// The fence read's error check is load-bearing in a way it is nowhere else, because this is the one
		// site that turns a READ into a control decision the caller reports as SUCCESS. A failed chain UPDATE
		// leaves the leaf `running` - EXACTLY what a genuine fence looks like - so a read that returned the
		// pre-UPDATE row instead of the transaction's error would make the fence below trip on a merely-
		// contended write and `return nil`: a leaf we still own, abandoned with no interrupt, no error and no
		// retry, its flow stuck `running` until the lease lapses (minutes) - permanently if a Resume lands in
		// that window, since the lost branch never marked its caller step interrupted. That was the live bug
		// while sequel's QueryRow uniquely neither short-circuited nor latched; it now reports the
		// transaction's error like every other statement method, so the check below catches it and Transact
		// retries. Observed on SQL Server, where two parallel subgraph callers interrupting at once deadlock:
		// moving a step running->interrupted deletes keys from the three status-filtered dwarf_steps indexes,
		// so even disjoint row sets cycle. Never let this error go unchecked, and never decide the fence ahead
		// of it.
		var curLeaseSeq int
		var curStatus string
		if serr := tx.QueryRowContext(ctx, "SELECT lease_seq, status FROM dwarf_steps WHERE step_id=?", stepID).Scan(&curLeaseSeq, &curStatus); serr != nil {
			return errors.Trace(serr)
		}
		// FaultInterruptStaleWrite forces the generation to look re-granted (as a real zombie would, after a peer
		// re-claimed the leaf), so the fence trips and the WHOLE transaction rolls back - undoing the ancestor
		// re-park - and the worker abandons. The only fence in the engine that rolls back rather than no-ops.
		if e.seams.IsFault(FaultInterruptStaleWrite) {
			curLeaseSeq = leaseSeq + 1
		}
		if curLeaseSeq != leaseSeq || curStatus != workflow.StatusInterrupted {
			fenced = true
			return errLeaseFenced
		}

		if len(interruptPayload) > 0 {
			payloadJSON, _ := json.Marshal(interruptPayload)
			payloadLen = len(payloadJSON)
			payloadArgs := []any{payloadJSON}
			payloadArgs = append(payloadArgs, allStepIDs...)
			// Guard: write the payload only to chain steps still at the default empty object, so a
			// concurrent fan-out interrupt does not clobber a payload already set on a shared ancestor
			// (first-writer-wins). Three dialect forms, and each is load-bearing:
			//   - MySQL's JSON column does not match a bare string literal with '=', so
			//     interrupt_payload='{}' silently matches nothing there; compare its textual form.
			//   - SQL Server's column is VARBINARY (the payload columns are binary there so a Go []byte
			//     binds as the matching type - see the migrations), so the literal must be the BYTE form:
			//     0x7B7D is "{}". A varchar '{}' literal is accepted only in a comparison like this one;
			//     SQL Server rejects the same implicit conversion outright in a DEFAULT or a SET
			//     assignment, which is why every payload RESET binds emptyJSON instead of inlining '{}'.
			//   - TEXT/JSONB match the literal directly.
			emptyGuard := "interrupt_payload='{}'"
			switch db.DriverName() {
			case "mysql":
				emptyGuard = "CAST(interrupt_payload AS CHAR)='{}'"
			case "mssql":
				emptyGuard = "interrupt_payload=0x7B7D"
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
	for _, compositeID := range chainCompositeIDs {
		e.signalStop(ctx, compositeID, workflow.StatusInterrupted)
	}
	return nil
}

// fireFanInDirect creates the fan-in step immediately for an empty-cohort case.
func (e *Engine) fireFanInDirect(ctx context.Context, shardNum int, db *sequel.DB, flowID int, stepID int, stepDepth int, lineageID int, fanOutOrdinal int, fanInTarget, fanInURL string, workflowURL string, graph *workflow.Graph, sleepDur time.Duration, priority int, fairnessKey string, fairnessWeight float64, timeBudgetMs int) error {
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

		var ourStateJSON, ourChangesJSON, ourRefsJSON []byte
		tx.QueryRowContext(ctx, "SELECT state, changes, state_refs FROM dwarf_steps WHERE step_id=?", stepID).Scan(&ourStateJSON, &ourChangesJSON, &ourRefsJSON)
		txBytes.stateRead = len(ourStateJSON)
		txBytes.changesRead = len(ourChangesJSON)
		ourState, _ := workflow.NewState(ourStateJSON)
		ourChanges, _ := workflow.NewState(ourChangesJSON)
		// Resolve only what the reducers actually fold; a merely-carried ref rides through the (empty) cohort
		// untouched, still pointing at its original anchor - see resolveReducedRefs. resolveReducedRefs mutates
		// the state map in place, so it gets the live map.
		ourRefs := staterefs.Parse(ourRefsJSON)
		rerr := e.resolveReducedRefs(ctx, tx, shardNum, ourState, ourRefs, graph.Reducers(), workflowURL)
		if rerr != nil {
			return errors.Trace(rerr)
		}
		// Fold our own delta via the reducers, then materialize: DelNils enacts replace-field deletes, and a
		// cleared reduced field was ignored during the fold. An empty forEach spawns no members, so this is the
		// whole merge - agreeing with insertFanInStep's populated path on the same primitive.
		mergedState := ourState.Clone()
		_ = mergedState.MergeReduce(ourChanges, graph.Reducers())
		mergedState.DelNils()
		// This step IS the cohort spawn (an empty forEach spawns no branches), so it anchors exactly as a
		// populated cohort's spawn does in insertFanInStep. There are no members, but a field this step wrote
		// through a COMBINING reducer still has a merged value (reduce(ourState[k], ourChanges[k])) that exists
		// in no row, so it must be inlined rather than anchored - the same exclusion insertFanInStep applies.
		inlineOnly := staterefs.CombinedReducerFields(ourState, ourChanges, graph.Reducers())
		mergedJSON, refsJSON, merr := e.linkerFor(shardNum).Mint(mergedState, ourChanges, ourRefs, stepID, staterefs.Linear, inlineOnly)
		if merr != nil {
			return errors.Trace(merr)
		}
		txBytes.stateWritten = len(mergedJSON)

		nextStepDepth := stepDepth + 1
		sleepMs := sleepDur.Milliseconds()
		var err error
		fanInStepID, err = tx.InsertReturnID(ctx, "step_id",
			"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_refs, status, parked, time_budget_ms, lineage_id, fan_out_ordinal, predecessor_id, not_before, priority, fairness_key, fairness_weight, engine_id)"+
				" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?)",
			flowID, nextStepDepth, keys.RandomIdentifier(16), fanInTarget, fanInURL, mergedJSON, refsJSON, workflow.StatusPending, parkedNone, timeBudgetMs, lineageID, fanOutOrdinal, stepID, sleepMs, priority, fairnessKey, fairnessWeight, e.engineID,
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

	// A sleeping fan-in step is left to the piston; see the sibling site above.
	if sleepDur == 0 {
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
	var spawnTaskName string
	var spawnStateJSON, spawnChangesJSON, spawnRefsJSON []byte
	var spawnLineageID, spawnFanOutOrdinal int
	tx.QueryRowContext(ctx,
		"SELECT state, changes, state_refs, lineage_id, task_name, fan_out_ordinal FROM dwarf_steps WHERE step_id=?",
		cohortSpawnID,
	).Scan(&spawnStateJSON, &spawnChangesJSON, &spawnRefsJSON, &spawnLineageID, &spawnTaskName, &spawnFanOutOrdinal)
	bytes := stateByteCount{stateRead: len(spawnStateJSON), changesRead: len(spawnChangesJSON)}
	spawnState, _ := workflow.NewState(spawnStateJSON)
	spawnChanges, _ := workflow.NewState(spawnChangesJSON)
	// Materialize only the ref'd fields a reducer will FOLD - a combining reducer (append/add/union/...) needs
	// its accumulated base, and folding a delta onto an absent base would silently lose everything so far. A
	// merely-carried ref is left alone and re-emitted onto the fan-in step below, still pointing at its
	// original anchor: resolving it here would materialize the payload and re-anchor it at EVERY fan-in,
	// giving back the win in exactly the fan-out graphs this design exists for. resolveReducedRefs mutates
	// the state map in place, so it gets the live map.
	spawnRefs := staterefs.Parse(spawnRefsJSON)
	err := e.resolveReducedRefs(ctx, tx, shardNum, spawnState, spawnRefs, graph.Reducers(), workflowURL)
	if err != nil {
		return 0, stateByteCount{}, errors.Trace(err)
	}
	// The fan-in accumulator's base: the spawn's state with its own delta folded in via the reducers. A
	// cleared reduced field is ignored (reducer identity); a cleared replace field rides as a tombstone that
	// DelNils enacts once all members are folded.
	merged := spawnState.Clone()
	_ = merged.MergeReduce(spawnChanges, graph.Reducers())
	// Fields a cohort MEMBER contributed. Their bytes are in that member's `changes` - not in the spawn's row -
	// and a reducer's COMBINED output exists in no row at all, so neither can be anchored at the spawn. They are
	// inlined into the fan-in step's own state, which becomes their anchor for everything downstream (the third
	// of the three places an anchor's bytes can sit). A stale spawn ref for such a field is dropped with them.
	memberWrites := map[string]bool{}

	// The cohort-exit steps whose successor_id must point at the fan-in step. Collected from this same
	// cohort scan so the successor_id write can target them by primary key (step_id IN ...) below,
	// rather than re-scanning dwarf_steps by (flow_id, lineage_id, task_name) - that unindexed predicate
	// took an Update lock across the whole table and deadlocked two concurrent fan-ins on SQL Server.
	//
	// A member qualifies as an exit only if it BOTH bears a fan-in-predecessor task name AND has not already
	// recorded a forward edge (successor_id=0). The task-name test alone is not enough: a branch that LOOPS
	// through the exit task via flow.Goto (e.g. `Work -Goto-> Work -> Join`) leaves several `Work` steps in the
	// cohort, but only the last one transitioned to the fan-in - the earlier iterations already point at their
	// next loop step. A step that transitions to a fan-in inserts no successor of its own (the fan-in step is
	// created here, or its arrival is only a counter bump), so its successor_id is still 0; a loop iteration or a
	// multi-step branch's interior step wrote its successor_id when it created that successor. Overwriting the
	// latter corrupts the execution DAG (spurious `step -> fan-in` edges in History/HistoryMermaid).
	exitTaskSet := make(map[string]bool)
	for _, t := range fanInPredecessorTasks(graph, fanInTaskName) {
		exitTaskSet[t] = true
	}
	var exitStepIDs []int

	rows, err := tx.QueryContext(ctx,
		"SELECT step_id, task_name, status, changes, step_depth, successor_id FROM dwarf_steps WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_depth, step_id",
		flowID, cohortSpawnID,
	)
	if err != nil {
		return 0, stateByteCount{}, errors.Trace(err)
	}
	defer rows.Close()
	maxCohortDepth := 0
	for rows.Next() {
		var memberStepID, memberSuccessorID int
		var memberTaskName, status string
		var changesJSON []byte
		var depth int
		rows.Scan(&memberStepID, &memberTaskName, &status, &changesJSON, &depth, &memberSuccessorID)
		bytes.changesRead += len(changesJSON)
		if depth > maxCohortDepth {
			maxCohortDepth = depth
		}
		// A cohort member with an exit task name that has not yet recorded a successor is one that transitioned
		// into the fan-in (see above); a member with the same task name that already has a successor looped or
		// continued elsewhere and must keep its own forward edge.
		if exitTaskSet[strings.TrimSpace(memberTaskName)] && memberSuccessorID == 0 {
			exitStepIDs = append(exitStepIDs, memberStepID)
		}
		if status != workflow.StatusCompleted {
			continue
		}
		changes, _ := workflow.NewState(changesJSON)
		for k := range changes {
			memberWrites[k] = true
		}
		_ = merged.MergeReduce(changes, graph.Reducers())
	}
	rows.Close()

	// Materialize the fan-in snapshot: the folds preserved replace-field delete tombstones (accumulation);
	// DelNils enacts them now, before the state is minted onto the fan-in step. Reduced fields are
	// unaffected - a cleared reduced field was ignored during the fold, never stored as a tombstone.
	merged.DelNils()

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
	for k := range staterefs.CombinedReducerFields(spawnState, spawnChanges, graph.Reducers()) {
		memberWrites[k] = true
	}

	// Mint against the SPAWN step, not this one: a field the cohort merely carried has its bytes in the spawn's
	// row (its `state` if the spawn received it - e.g. the entry step holding the flow's initial input - or its
	// `changes` if the spawn's task wrote it), so the spawn is a legitimate anchor and the fan-in must not
	// re-copy the payload into its own row. A field that arrived at the spawn as a ref keeps that ref, so the
	// chain stays one hop. Member-contributed and reduced fields are excluded and therefore inline (above).
	mergedJSON, refsJSON, err := e.linkerFor(shardNum).Mint(merged, spawnChanges, spawnRefs, cohortSpawnID, staterefs.Linear, memberWrites)
	if err != nil {
		return 0, stateByteCount{}, errors.Trace(err)
	}
	bytes.stateWritten = len(mergedJSON)
	fanInURL := dispatchURLOf(graph, fanInTaskName)
	fanInStepID, err := tx.InsertReturnID(ctx, "step_id",
		"INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url, state, state_refs, status, parked, time_budget_ms, lineage_id, fan_out_ordinal, predecessor_id, not_before, priority, fairness_key, fairness_weight, engine_id)"+
			" VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?)",
		flowID, fanInDepth, keys.RandomIdentifier(16), fanInTaskName, fanInURL, mergedJSON, refsJSON, workflow.StatusPending, parkedNone, timeBudgetMs, spawnLineageID, spawnFanOutOrdinal, predecessorStepID, sleepMs, priority, fairnessKey, fairnessWeight, e.engineID,
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
