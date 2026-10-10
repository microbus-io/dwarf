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
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/cancelmarker"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	// cohortLockStripesLog2 sizes e.cohortLocks at 2^13 = 8192 stripes (~320KB of cohortStripe). Fixed, never
	// resized: a stripe array's mutual exclusion rests on a key always mapping to the same lock instance,
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
// THE CTX CARRIES THIS JOB'S PLACE IN LINE, stamped by the crew's gate at the moment the candidate was
// picked up, and every database call below takes its turn on it (turnstile.WaitTurn). Because the stamp is
// the pickup time and never moves, a step already under way outranks one just starting - which is what
// makes a step that has begun finish rather than queue behind newly-admitted work.
//
// returnGateTurn hands back the turn the crew took on this worker's behalf. It is released as soon as the
// claim CAS is done, because that is the only database work it was ever covering: the calls after it take
// their own turns, and holding one turn across the whole entry phase would bound admission by phases
// rather than by round trips. The crew wraps it in sync.OnceFunc and calls it again on the way out, so
// every early return below is covered and a double call is a no-op.
func (e *Engine) processStep(ctx context.Context, shardNum int, stepID int, returnGateTurn func()) (err error) {
	// The ENTRY phase runs from here to the host call. Closed through the returned closure rather than at
	// that one line, because every early return in between - a lost claim, a terminal flow, a failed load -
	// must close it too, and a missed close leaks the count upward for the life of the process. Idempotent,
	// so closing it explicitly at the boundary AND deferring costs nothing.
	closeEntryPhase := e.enterDBPhase(phaseEnter)
	defer closeEntryPhase()
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
		//                       to commit. Re-dispatch replays the step (advancing=1: the task does not run
		//                       again) and re-evaluates its transitions.
		// Both are guarded on the from-status for idempotency and retried via Transact since this
		// recovery can itself race a contention storm; the reset UPDATE losing that race leaves the step
		// `completed` with advancing=1, which recoverOrphanedFlows rewinds once its lease lapses.
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
			// A parked step is not this worker's to rewind: it is waiting on its child, and a rewind would leave
			// it pending AND parked, which nothing selects or recovers.
			resetSQL := "UPDATE dwarf_steps SET status=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status=? AND parked=?"
			resetArgs := []any{workflow.StatusPending, stepID, fromStatus, parkedNone}
			if leaseSeq > 0 {
				resetSQL += " AND lease_seq=?"
				resetArgs = append(resetArgs, leaseSeq)
			}
			// Test checkpoint: a breakpoint here freezes a zombie between capturing its (stale) reset and
			// running it, so a test can let a peer re-claim+bump the step first and prove the lease_seq fence
			// makes the zombie's reset a zero-row no-op (never rewinding the peer's freshly-claimed step).
			e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeRecoveryReset)
			db.Transact(ctx, func(tx *sequel.Tx) error {
				// engineseams.FaultRecoveryResetErr makes this last-resort reset itself fail (a non-retryable error, as if
				// it lost to a contention storm), so the step stays `completed` and the flow strands `running`
				// with every step terminal until recoverOrphanedFlows rewinds the step.
				// Process-wide (no scope): taskName is declared after this defer, so it is not in scope here.
				if e.seams.IsFault(engineseams.FaultRecoveryResetErr) {
					return errors.New("injected fault: " + engineseams.FaultRecoveryResetErr)
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
	// stepCancelling is this step's own Cancel mark as of the claim.
	var stepCancelling bool
	// replaying is the step's advancing flag as of the claim: its completion write landed and its transition never
	// committed, so the task has already run and is not run again. The step is REPLAYED instead - its recorded
	// output and disposition (the Goto target, the Sleep and whether it was error-routed, stored by that same
	// completion write) stand in for the task's return, and dispatch resumes at the completion write. The
	// transition is a pure function of the frozen graph, the step's state and those recorded values, so a replay
	// commits exactly what the original worker would have.
	var replaying, replayOnError bool
	var replayGoto string
	// replaySleepLeftMs is what remains of the recorded Sleep: the completion write stored a deadline, so a replay
	// keeps the original wake time instead of restarting the sleep. Fractional on SQLite; negative once it passed.
	var replaySleepLeftMs float64

	// The flow row's columns. On Postgres the claim returns them too, joined in: one round trip instead of
	// two on every step. The other drivers read them after the claim (below).
	var flowToken, flowStatus, workflowURL, traceParent string
	var baggageJSON []byte
	var flowCreatedAt, flowUpdatedAt time.Time
	var flowPriority int
	var flowFairnessKey string
	var flowFairnessWeight float64
	var flowTimeBudgetMs int
	var flowCancelReason string
	flowDest := []any{&flowToken, &flowStatus, &workflowURL, &baggageJSON, &traceParent, &flowCreatedAt, &flowUpdatedAt, &flowPriority, &flowFairnessKey, &flowFairnessWeight, &flowTimeBudgetMs, &flowCancelReason}
	flowRead := true
	// The step columns every dialect's claim reads back, in claimCols order.
	stepDest := []any{&stepDepth, &taskName, &stepToken, &stateJSON, &priorChangesJSON, &stateRefsJSON, &attempt, &lineageID, &flowID, &timeBudgetMs, &interruptDone, &resumeDataJSON, &subgraphDone, &subgraphResultJSON, &subgraphErrorStr, &stepCreatedAt, &leaseSeq, &fanOutOrdinal, &stepCancelling, &replaying, &replayGoto, &replaySleepLeftMs, &replayOnError}

	switch db.DriverName() {
	case "pgx":
		// The joined flow row is READ, not locked: Postgres locks only an UPDATE's target rows, so the claim
		// takes no lock on dwarf_flows and cannot deadlock with a transaction that locks the flow row before
		// its steps (Cancel, Terminate). Do not extend this to SQL Server without READ_COMMITTED_SNAPSHOT:
		// there the join takes a shared lock on the flow row while holding the step row - the reverse order.
		// Every claim-side column is qualified, because dwarf_flows shares several names with dwarf_steps
		// (status, created_at, updated_at, time_budget_ms).
		err = db.QueryRowContext(ctx,
			claimSQLPostgres,
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		).Scan(append(stepDest, flowDest...)...)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n, flowRead = 1, false
		}
	case "sqlite":
		err = db.QueryRowContext(ctx,
			claimSQLSQLite,
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		).Scan(stepDest...)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	case "mssql":
		err = db.QueryRowContext(ctx,
			claimSQLMSSQL,
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		).Scan(stepDest...)
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
			claimSQLMySQL,
			workflow.StatusRunning, leaseMarginMs, e.engineID, stepID, parkedNone,
		)
		if claimErr != nil {
			err = claimErr
			break
		}
		n, _ = res.RowsAffected()
		if n == 1 {
			readErr := db.QueryRowContext(ctx,
				claimReadMySQL,
				stepID,
			).Scan(stepDest...)
			if readErr != nil && readErr != sql.ErrNoRows {
				err = readErr
			}
		}
	}
	// The gate's turn covered the claim CAS and nothing after it. Handing it back here rather than at the
	// host call keeps a turn worth one round trip rather than a whole phase, so the turn count means
	// concurrent round trips: everything below takes its own turn on this job's own place in line.
	returnGateTurn()
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
		// On PostgreSQL the claim joins the flow row, so a step whose flow row is gone matches nothing and
		// does count here; the reaper and Fork delete a flow's steps before the flow, so such a row should not
		// exist.
		e.metricStepClaimLost(ctx)
		return nil
	}
	if flowID == 0 {
		return nil
	}
	if e.seams.Enabled() { // Enabled gates the assembled name in production
		defer func() {
			e.seams.Checkpoint(ctx, engineseams.CheckpointClaimedDispatchDone)
			e.seams.Checkpoint(ctx, engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shardNum), strconv.Itoa(stepID)))
		}()
	}

	// The graph is frozen at flow creation, so a cached parse makes its JSON - the largest payload on the flow
	// row - dead weight; it is selected only on a miss, which is why the Postgres claim leaves it out.
	graphKey := graphCacheKey{shard: shardNum, flowID: flowID} // scopes the cache by shard, since flow_id is only unique within a shard
	cg, cached := e.graphCache.Load(graphKey)
	var graphJSON []byte
	readFlow := func(cols string, dest ...any) error {
		flowPass := turnstile.WaitTurn(ctx)
		defer flowPass.Return()
		return db.QueryRowContext(ctx, "SELECT "+cols+" FROM dwarf_flows WHERE flow_id=?", flowID).Scan(dest...)
	}
	switch {
	case flowRead:
		cols := "flow_token, status, workflow_url, baggage, trace_parent, created_at, updated_at, priority, fairness_key, fairness_weight, time_budget_ms, cancel_reason"
		if !cached {
			cols += ", graph"
			flowDest = append(flowDest, &graphJSON)
		}
		err = readFlow(cols, flowDest...)
	case !cached:
		err = readFlow("graph", &graphJSON)
	}
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

	if workflow.IsTerminalStatus(flowStatus) {
		reapPass := turnstile.WaitTurn(ctx)
		_, err = db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
			flowStatus, parkedNone, stepID,
		)
		reapPass.Return()
		return errors.Trace(err)
	}

	if !cached {
		cg, err = parseCachedGraph(graphJSON, workflowURL)
		if err != nil {
			// Its own turn: this is the entry phase, so the persist turn that covers every LATER failStep
			// does not exist yet, and failStep runs a whole transaction. Without it the two failure paths
			// would disagree about whether failing a step is ordered work.
			fctx, doneTurn := e.dbTurn(ctx, shardNum)
			err = e.failAndReturn(fctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
			doneTurn()
			return errors.Trace(err)
		}
		e.graphCache.Store(graphKey, cg)
	}
	graph := cg.graph

	// Build the Flow carrier. A State built from JSON holds each field as JSON and decodes it only if
	// something reads it - a step's state is a full input snapshot, so most fields on most steps are carried
	// rather than read, and decoding them would cost several times their byte length for the whole duration
	// of the host call.
	state, _ := workflow.NewState(stateJSON)
	// Materialize any field this step carries BY REFERENCE (see staterefs.go). Resolving here, once, is what
	// keeps the ref encoding out of everything downstream: the carrier, `when` evaluation, forEach expansion,
	// the transport to a remote task, and the transition machinery all work on literals and never learn refs
	// exist. The refs the step inherited are kept, because minting the successors' state needs to know which
	// fields arrived as refs (and so must be carried, never re-anchored against this step). resolveStateRefs
	// mutates the state map in place, so it gets the live backing map.
	//
	// Refs this replica cannot read fail the step, like a ref it cannot resolve: running the task without the
	// fields they point at would lose them silently.
	inheritedRefs, err := staterefs.Parse(stateRefsJSON)
	var refBytes int
	if err == nil {
		// A turn of its own, and only when this step actually carries a ref: resolving reads anchor rows, but
		// with nothing to resolve it touches no connection, so a step with no refs must not queue for a turn it
		// would not use. Most steps carry none.
		var refsPass turnstile.Pass
		if len(inheritedRefs) > 0 {
			refsPass = turnstile.WaitTurn(ctx)
		}
		refBytes, err = e.resolveDispatchRefs(ctx, db, shardNum, state, inheritedRefs, workflowURL)
		refsPass.Return()
	}
	if err != nil {
		// Its own turn, for the reason above: still the entry phase, so nothing covers this transaction.
		fctx, doneTurn := e.dbTurn(ctx, shardNum)
		err = e.failAndReturn(fctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
		doneTurn()
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

	// A replay's carrier already holds the task's output: the claim read the step's changes as priorChanges, so
	// they are the carrier's changes and are merged into its state exactly as the task left them. Only the control
	// signals were not, and the completion write recorded them.
	if replaying {
		flow.Goto(replayGoto)
		flow.Sleep(time.Duration(max(replaySleepLeftMs, 0)) * time.Millisecond)
	}

	// Execute the task. The step's time_budget_ms bounds the executor call's context deadline; the
	// surrounding DB work keeps using the undeadlined ctx so persistence is never cut short.
	if !replaying {
		e.logger.DebugContext(ctx, "Executing task", "task", taskName, "workflow", workflowURL)
	}
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
			attribute.Bool("workflow.replay", replaying),
		),
	)
	defer taskSpan.End()

	if timeBudgetMs > 0 {
		var cancel context.CancelFunc
		taskCtx, cancel = context.WithTimeout(taskCtx, time.Duration(timeBudgetMs)*time.Millisecond)
		defer cancel()
	}
	// Belt and braces on the gate's turn, which was handed back at the claim CAS: this is the last line
	// before the worker stops competing for a connection at all, so an early return between the two that
	// somehow skipped it is caught here. The crew wraps the release in sync.OnceFunc, so the repeat costs
	// nothing.
	returnGateTurn()
	closeEntryPhase()
	// Count what this worker holds for the duration of the host call. The window is the CALL, not this
	// function: the carrier is live for the whole of processStep, but only here is the worker parked on
	// something unbounded while holding no connection - which is what makes held state a memory question
	// rather than a throughput one. Bracketed rather than deferred for the same reason returnGateTurn is
	// not deferred; the release is safe unbracketed because CatchPanic converts a host panic into a return
	// rather than unwinding past it, and a runtime throw takes the process with it either way.
	//
	// refBytes, not the read bytes reported to dwarf_state_read_bytes: those are whole anchor ROWS and are
	// zero on a resolve-cache hit, which is the common case across a fan-out's branches - so a large
	// carried document would read as nothing on all but the first branch, in the one measurement it exists
	// to make.
	heldBytes := int64(len(stateJSON) + len(priorChangesJSON) + refBytes)
	e.inFlightStateBytes.Add(heldBytes)
	e.inFlightStateSteps.Add(1)
	// A step marked by Cancel before it started is preempted: the task is not run, and the cancellation
	// takes the path a task error would. The one exemption is a subgraph caller resuming after its child
	// returned - it runs once more so the child's result reaches its code, and its own mark is then applied
	// at the completion write below. An interrupted step resumed after Cancel is NOT exempt: its resume
	// data is new input arriving after the cancellation, and acting on it would keep a cancelled flow alive.
	//
	// A replay runs neither the task nor the preempt: the task already ran to completion, so a mark that landed
	// since is applied at the completion write below, as for a step that was running when Cancel landed.
	var execErr error
	if replaying {
		// No host call.
	} else if stepCancelling && !subgraphDone {
		// Re-read the reason rather than trust the claim's: on PostgreSQL the claim returns the flow row as of
		// the claim statement's snapshot, which can predate a Cancel whose reason commits before its mark
		// reaches this step - the claim's re-check then sees the mark, but the reason it returned is empty.
		// Cancel writes the reason before the mark, so a read after the claim finds it. A rare path.
		reasonPass := turnstile.WaitTurn(ctx)
		var rerr error
		flowCancelReason, rerr = readCancelReason(ctx, db, flowID)
		reasonPass.Return()
		if rerr != nil {
			return errors.Trace(rerr)
		}
		execErr = cancelmarker.New(flowCancelReason)
	} else {
		// A panic in the in-process host is caught here so it flows through the normal error disposition
		// rather than wedging this leased step until lease expiry.
		execErr = errors.CatchPanic(func() error {
			// engineseams.FaultPanicExecuteTask panics inside the wrapper so it exercises the host-call panic isolation
			// (caught here, routed as a normal task error), scoped to this task name.
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultPanicExecuteTask, taskName)) {
				panic("injected fault: " + engineseams.FaultPanicExecuteTask + " " + taskName)
			}
			return e.host.ExecuteTask(taskCtx, dispatchURL, &flow.Flow)
		})
	}
	e.inFlightStateBytes.Add(-heldBytes)
	e.inFlightStateSteps.Add(-1)
	// The task has now RUN. Everything below records that fact, and it holds a connection to do so, so it
	// takes a turn like every other database call - but on THIS job's claim, stamped when the candidate was
	// picked up and unchanged since. That is what keeps a finishing step ahead of a starting one: this
	// job is by now older than anything admitted while its task ran, so it outranks them without any rule
	// saying so.
	//
	// Do not add a dedicated reservation for completions; the age ordering is why none is needed. A shared
	// pool that must CHOOSE who wins fails both ways, measured: served evenly, completions lose at random and
	// queue behind admission (286 of them waiting out a full 1s budget); served with completions preferred,
	// dispatch starves and short-task throughput collapses 3x (4,416 vs 7,964 steps/s). Ordering by age
	// answers it without a rule, and without either population being able to starve - every claim
	// eventually becomes the oldest.
	//
	// A turn is never refused, so a drain records the work anyway: the outcome exists nowhere but in this
	// goroutine. The return is still mandatory - a lost one shrinks this shard's ceiling permanently - and
	// has no crew-side backstop, the gate's turn having been handed back long ago.
	turnWaitStart := time.Now()
	persistPass := turnstile.WaitTurn(ctx)
	// Through a CLOSURE, not `defer persistPass.Return()`: the pass is handed back and re-taken around every
	// wait below that is not the database (yieldTurn), so the deferred call has to see whichever pass is
	// current at the end, not the one that existed when the defer was written.
	defer func() { persistPass.Return() }()
	// Publish it, so a callee that must park on something other than the database - failStep on the cohort
	// stripe - can hand it back without it being threaded through every signature in between.
	ctx = withHeldTurn(ctx, &persistPass)
	// The EXIT phase runs from here to the end of the step. One line rather than the two-sided guard entry
	// needs, because there is no early return between this point and the function's end.
	closeExitPhase := e.enterDBPhase(phaseExit)
	defer closeExitPhase()
	// Timed HERE rather than inside the turnstile, which holds no metrics. Recorded on EVERY completion,
	// not only the ones that waited: count is then completions and sum/count the mean, so the exit side
	// becoming the constraint shows up as a shift in the distribution - where the free-turn gauge, being
	// instantaneous, can miss a reservation that was saturated all window without being sampled empty.
	e.metrics.turnWait.Record(ctx, time.Since(turnWaitStart).Seconds(),
		metric.WithAttributes(attribute.String("shard", strconv.Itoa(shardNum))))

	if execErr == nil && !replaying && e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultExecuteTask, taskName)) {
		execErr = errors.New("injected fault: "+engineseams.FaultExecuteTask+" "+taskName, http.StatusInternalServerError)
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
			// A FRESH flow, deliberately: an error voids the task's changes. Whatever it wrote before
			// returning the error is dropped rather than carried to the handler - execution is at-least-once,
			// so a lease-lost re-run recomputes those changes and a half-written attempt is not a fact
			// anything can be built on. failStep does the same (it writes status/error only), so the contract
			// does not turn on whether the author declared a handler. Do not "restore" rawChanges here.
			resultFlow = workflow.NewRawFlow()
			resultFlow.SetRawState(state)
			resultFlow.Set("onErr", onErrValue(execErr))
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
		// An error-routed step's recorded changes already carry its onErr; only the route is re-derived. The
		// graph is frozen on the flow, so the handler is the one the original worker routed to.
		if replaying && replayOnError {
			if tr, ok := graph.ErrorTransition(taskName); ok {
				errorRouted = true
				errorTarget = tr.To
			}
		}
	}

	// Accumulate changes
	var accumulatedChanges workflow.State
	var changesJSON []byte
	rawChanges := resultFlow.RawChanges()
	if rawChanges.Len() == 0 {
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
		landed, err := e.settlePersist(ctx, e.persistYielding(ctx, db, shardNum, stepID, leaseSeq, &persistPass, func() error {
			return e.handleInterrupt(ctx, shardNum, db, stepID, leaseSeq, flowID, flowToken, workflowURL, changesJSON, interruptPayload)
		}), shardNum, stepID, leaseSeq, flowID, flowToken, taskName)
		if landed {
			e.metricStepExecuted(ctx, taskName, workflow.StatusInterrupted, shardNum)
		}
		return err
	}

	// Handle subgraph
	if subgraphURL, subgraphInput, subgraphRequested := resultFlow.SubgraphRequested(); subgraphRequested {
		e.logger.DebugContext(ctx, "Task requested subgraph", "task", taskName, "workflow", workflowURL, "subgraph", subgraphURL)
		// Bound the subgraph LoadGraph by the caller flow's budget (the create-time LoadGraph uses the caller's ctx).
		loadCtx, loadCancel := context.WithTimeout(workflow.ContextWithBaggage(ctx, baggage), time.Duration(flowTimeBudgetMs)*time.Millisecond)
		var subgraphGraph *workflow.Graph
		lerr := errors.CatchPanic(func() error {
			var e2 error
			// The host call holds no connection and is unbounded, so the turn goes back for its duration -
			// otherwise one slow LoadGraph per worker parks the shard's turns on something that is not the
			// database at all.
			yieldTurn(ctx, &persistPass, func() {
				subgraphGraph, e2 = e.host.LoadGraph(loadCtx, subgraphURL)
			})
			return e2
		})
		if lerr == nil && e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultLoadGraph, subgraphURL)) {
			lerr = errors.New("injected fault: "+engineseams.FaultLoadGraph+" "+subgraphURL, http.StatusInternalServerError)
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
		// Persist the task's changes and park the caller step in the SAME transaction that inserts the child.
		// Two orderings are load-bearing here:
		//
		//   - The park must be visible before the child can complete: completeSurgraphFlow revives this caller
		//     only WHERE parked=parkedSubgraph, and a parkedSubgraph step is excluded from lease recovery, so a
		//     revive that ran before the park landed would lose the wakeup and strand the caller - its fan-in
		//     then never fires and the flow hangs. One transaction makes the two visible together.
		//   - A Terminate must not miss the child. Terminate locks every live step of the tree, this caller
		//     included, before scanning for children inside its own transaction (terminateSubtree). So this
		//     spawn either commits first, and Terminate's scan sees the child, or it waits on that lock and its
		//     park then matches no row, rolling back so no child is created.
		//
		// The park is guarded on status=running and the lease, so a terminated or re-claimed caller rolls the
		// spawn back too, and this worker returns quietly.
		parkCaller := func(tx *sequel.Tx) error {
			res, err := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET changes=?, parked=?, updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND parked=? AND lease_seq=?",
				changesJSON, parkedSubgraph, stepID, parkedNone, leaseSeq,
			)
			if err != nil {
				return errors.Trace(err)
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return errCallerGone
			}
			return nil
		}
		childInputState := subgraphInput
		if childInputState.IsZero() {
			childInputState, _ = workflow.NewState()
		}
		// The caller step's span is still live on taskCtx; parent the subgraph's "workflow" span under it
		// so the subgraph subtree nests beneath this task in the trace.
		callerTraceParent := extractTraceParent(taskCtx)
		e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeSubgraphSpawn)
		// createSubgraphFlow inserts the child already surgraph-linked and running, so no separate start. The
		// spawn is one transaction, so persist may re-run it whole.
		callerGone := false
		landed, err := e.settlePersist(ctx, e.persistYielding(ctx, db, shardNum, stepID, leaseSeq, &persistPass, func() error {
			_, err := e.createSubgraphFlow(ctx, shardNum, flowID, stepDepth, stepID, subgraphURL, subgraphGraph, childInputState, baggageJSON, callerTraceParent, parkCaller)
			callerGone = errors.Is(err, errCallerGone)
			if callerGone {
				return nil
			}
			return err
		}), shardNum, stepID, leaseSeq, flowID, flowToken, taskName)
		if !landed {
			return err
		}
		if callerGone {
			e.seams.Checkpoint(ctx, engineseams.CheckpointSubgraphSpawnAbandoned)
			return nil
		}
		e.metricStateWriteBytes(ctx, workflowURL, "changes", len(changesJSON))
		e.metricStepExecuted(ctx, taskName, "subgraph", shardNum)
		return nil
	}

	sleepDur := resultFlow.SleepRequested()

	// Handle retry
	if _, _, _, retryRequested := resultFlow.RetryRequested(); retryRequested {
		e.logger.DebugContext(ctx, "Task retried", "task", taskName, "workflow", workflowURL, "step", stepID, "attempt", attempt)
		// Sleep is the floor and the backoff adds on top: total = Sleep + min(backoff, maxDelay). This lets a
		// task set a precise wait (e.g. a downstream's Retry-After via Sleep) and still get exponential backoff
		// on repeated attempts. maxDelay caps the backoff component, not the total.
		retrySleepMs := sleepDur.Milliseconds() + resultFlow.RetryDelay(attempt).Milliseconds()
		// A retry rewinds this step in place and clears its subgraph park slot, so on
		// re-dispatch flow.Subgraph re-arms and spawns a *fresh* child. The prior attempt's
		// child (always terminal by now - the park only resolves on a terminal child) must be
		// reaped, recursively, in the same transaction as the rewind: leaving it dangling makes
		// the execution DAG claim two paths (X -> iter1 -> iter2 -> Y) when the model is
		// single-path, and lets history attach the discarded child's subtree to this caller.
		// The reap is step-scoped (only this caller's children), so a retrying fan-out sibling's
		// cohort is untouched.
		// The rewind is guarded to status='running': a Terminate landing mid-task flips this step terminal
		// (terminated), and an unguarded rewind would both revive an immutable terminal step and, via the reap,
		// delete the now-terminal tree's subgraph children. So rewind first under the guard and reap (and
		// re-dispatch) only when it actually rewound a still-running step; a lost guard (n==0) leaves the step
		// terminal - Terminate already terminated its children - and returns without reaping or re-dispatching.
		// Test checkpoint: a breakpoint here freezes the worker before the rewind, so a test can Terminate the
		// flow (terminalizing this running step) in exactly the window the status='running' rewind guard protects.
		e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeRetryRewind)
		var rewound bool
		landed, err := e.settlePersist(ctx, e.persistYielding(ctx, db, shardNum, stepID, leaseSeq, &persistPass, func() error {
			rewound = false
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultRetryRewindErr, taskName)) {
				return errors.New("injected fault: " + engineseams.FaultRetryRewindErr + " " + taskName)
			}
			return db.Transact(ctx, func(tx *sequel.Tx) error {
				rewound = false
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
		}), shardNum, stepID, leaseSeq, flowID, flowToken, taskName)
		if !landed || !rewound {
			return err
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

	// The completion write records the Goto target in advance_goto, a column as wide as task_name. A longer target
	// names no node, so it fails here with the error the transition would have given it, rather than as a column
	// overflow on the completion write (retried, then classified as a payload fault).
	if gotoTarget := resultFlow.GotoRequested(); len(gotoTarget) > maxGotoTargetLen {
		err = errors.New("task '%s' requested goto to '%s' but no goto transition exists from this task", stripProto(taskName), stripProto(gotoTarget))
		return errors.Trace(e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName))
	}

	// Complete the step.
	//
	// engineseams.FaultLeaseStaleWrite makes this completion write carry a stale lease generation, exactly as a zombie
	// worker (whose lease was re-granted to a peer) would. The fence must reject it (zero rows -> benign
	// no-op below), so the step stays claimable and lease recovery re-runs it cleanly - the test proves a
	// late/slow worker's write can never corrupt or terminalize a flow a peer is healthily re-executing.
	writeSeq := leaseSeq
	if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultLeaseStaleWrite, taskName)) {
		writeSeq = leaseSeq - 1
	}
	// THE task has already run - its side effects have fired - so this write is the only record that it did.
	// A database error here must therefore retry the WRITE, never the task (see persist): an ephemeral blip is
	// absorbed with zero re-execution, and a write that will never land is classified and terminalized rather
	// than left to lease recovery, which would re-execute the task every `budget + leaseMargin`, forever.
	//
	// The write also reads back the step's Cancel mark - the one post-dispatch cancellation check. It rides a
	// write every completion makes anyway, via RETURNING/OUTPUT, so on those dialects an uncancelled flow pays
	// nothing for it. MySQL lacks both and reads the mark with a follow-up SELECT: one extra round trip per
	// completed step, accepted as it is at the claim, rather than returning it through LAST_INSERT_ID(expr).
	// That SELECT is race-free only because the row is already terminal when it runs - Cancel never marks a
	// terminal row, so the value it reads is the one this UPDATE saw. The status always moves running ->
	// completed, which keeps RowsAffected an honest fence on MySQL (it counts CHANGED rows): a covered step is
	// settled by a write of its own below, never by a variant of this one that could leave the row unchanged.
	//
	// advancing=1 marks the step as completed-but-not-yet-transitioned, and the advance_* columns record what the
	// transition needs beyond the step's state and changes: the task's Goto target, its Sleep and whether it was
	// error-routed. The transition transaction clears the flag in a statement that already writes this row, so
	// neither side costs a round trip, and a worker that dies between here and that commit leaves a step the orphan
	// sweep can rewind and dispatch can replay without running the task again - see recoverOrphanedFlows and
	// `replaying` above.
	const advanceSet = "advancing=1, advance_goto=?, advance_sleep_until=DATE_ADD_MILLIS(NOW_UTC(), ?), advance_on_error=?"
	advanceGoto, advanceSleepMs, advanceOnError := resultFlow.GotoRequested(), sleepDur.Milliseconds(), 0
	if errorRouted {
		advanceOnError = 1
	}
	var stepRowsAffected int64
	var completedCancelling bool
	err = e.persistYielding(ctx, db, shardNum, stepID, leaseSeq, &persistPass, func() error {
		stepRowsAffected, completedCancelling = 0, false
		// engineseams.FaultPersistErr makes this write fail with a synthetic NON-contention error, consumed per attempt -
		// so InjectN(1) is a transient blip the retry must absorb with NO re-execution, and InjectN(large) is a
		// permanent failure the classifier must terminalize rather than loop on.
		if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultPersistErr, taskName)) {
			return errors.New("injected fault: " + engineseams.FaultPersistErr + " " + taskName)
		}
		const where = " WHERE step_id=? AND status NOT IN ('" + workflow.StatusTerminated + "', '" + workflow.StatusCancelled + "') AND lease_seq=?"
		switch db.DriverName() {
		case "pgx", "sqlite":
			werr := db.QueryRowContext(ctx,
				"UPDATE dwarf_steps SET status=?, changes=?, "+advanceSet+", updated_at=NOW_UTC()"+where+" RETURNING cancelling",
				workflow.StatusCompleted, changesJSON, advanceGoto, advanceSleepMs, advanceOnError, stepID, writeSeq,
			).Scan(&completedCancelling)
			if werr == sql.ErrNoRows {
				return nil
			}
			if werr != nil {
				return errors.Trace(werr)
			}
			stepRowsAffected = 1
		case "mssql":
			werr := db.QueryRowContext(ctx,
				"UPDATE dwarf_steps SET status=?, changes=?, "+advanceSet+", updated_at=NOW_UTC() OUTPUT INSERTED.cancelling"+where,
				workflow.StatusCompleted, changesJSON, advanceGoto, advanceSleepMs, advanceOnError, stepID, writeSeq,
			).Scan(&completedCancelling)
			if werr == sql.ErrNoRows {
				return nil
			}
			if werr != nil {
				return errors.Trace(werr)
			}
			stepRowsAffected = 1
		default:
			res, werr := db.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, changes=?, "+advanceSet+", updated_at=NOW_UTC()"+where,
				workflow.StatusCompleted, changesJSON, advanceGoto, advanceSleepMs, advanceOnError, stepID, writeSeq,
			)
			if werr != nil {
				return errors.Trace(werr)
			}
			stepRowsAffected, _ = res.RowsAffected()
			if stepRowsAffected > 0 {
				if rerr := db.QueryRowContext(ctx, "SELECT cancelling FROM dwarf_steps WHERE step_id=?", stepID).Scan(&completedCancelling); rerr != nil {
					return errors.Trace(rerr)
				}
			}
		}
		return nil
	})
	if landed, err := e.settlePersist(ctx, err, shardNum, stepID, leaseSeq, flowID, flowToken, taskName); !landed {
		return err
	}
	if stepRowsAffected == 0 {
		return nil
	}
	stepMarkedComplete = true
	e.metricStateWriteBytes(ctx, workflowURL, "changes", len(changesJSON))

	// A step Cancel marked while its task was running: the task genuinely ran, so its changes stand (the write
	// above persisted them), but what it asked for next is not honored. An errorRouted step is already on its
	// way to its handler and needs nothing more.
	if completedCancelling && !errorRouted {
		// The flow row was read at claim time, which for a step already running when Cancel landed predates
		// the reason. Cancel writes the reason before the mark, so a step that sees the mark finds the reason
		// on a fresh read; this path is rare, so the extra round trip is too.
		var rerr error
		if flowCancelReason, rerr = readCancelReason(ctx, db, flowID); rerr != nil {
			return errors.Trace(rerr)
		}
		tr, ok := graph.ErrorTransition(taskName)
		if !ok {
			// No handler: fail the step as cancelled. failStep flips it completed -> cancelled, the same
			// completed-to-terminal escape failOnPersistError uses, so the recovery defer - which rewinds only
			// a `completed` row - has nothing to rewind once it lands, and rewinds it for a re-dispatch (where
			// the claim preempts it) if it does not.
			return errors.Trace(e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, cancelmarker.New(flowCancelReason), taskName))
		}
		// Route to the handler exactly as a task error would, except that the task's own changes are kept -
		// it ran to completion, so they are a fact, not a half-written attempt - and onErr is added to them.
		// The extra write puts onErr into the step's persisted changes, so the step reads afterwards the way an
		// error-routed step does; it runs only on this rare path, so the completion write above never carries
		// a second copy of the changes.
		accumulatedChanges = accumulatedChanges.Clone()
		_ = accumulatedChanges.Set("onErr", onErrValue(cancelmarker.New(flowCancelReason)))
		changesJSON, _ = json.Marshal(accumulatedChanges)
		// The route is recorded with the changes, so a replay of this step routes to the handler too.
		if _, uerr := db.ExecContext(ctx,
			"UPDATE dwarf_steps SET changes=?, advance_sleep_until=NOW_UTC(), advance_on_error=1 WHERE step_id=? AND lease_seq=?",
			changesJSON, stepID, leaseSeq,
		); uerr != nil {
			return errors.Trace(uerr)
		}
		errorRouted = true
		errorTarget = tr.To
		sleepDur = 0
	}
	if replaying {
		// The task ran once, on the worker that recorded this completion, and dwarf_steps_executed counted it there.
		e.logger.DebugContext(ctx, "Task replayed", "task", taskName, "workflow", workflowURL)
	} else if errorRouted {
		e.logger.DebugContext(ctx, "Task error routed", "task", taskName, "workflow", workflowURL)
		e.metricStepExecuted(ctx, taskName, "error_routed", shardNum)
	} else {
		e.logger.DebugContext(ctx, "Task completed", "task", taskName, "workflow", workflowURL)
		e.metricStepExecuted(ctx, taskName, workflow.StatusCompleted, shardNum)
	}
	// A successor inherits a Cancel mark only from a source that was not itself covered: a covered source has
	// already had its cancellation delivered (or overridden by its own error), and what it routes to next is
	// the author's recovery. See successorRowSQL.
	inheritCancel := 1
	if stepCancelling || completedCancelling {
		inheritCancel = 0
	}

	// Test checkpoint: a breakpoint here freezes the worker after the step is marked completed but before
	// whichever transaction transitions it - the transition transaction, fireFanInDirect, or the flow's
	// completion - so a test can Terminate the flow in exactly the window the write-first terminal-status guard
	// exists to survive (the transition must become a no-op, inserting no successors into the terminated flow), or
	// take the step back from under a worker that has stalled there.
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeTransitionTx)

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
	routedToEnd := false
	for _, t := range nextTasks {
		if t.taskName == workflow.End {
			routedToEnd = true
		}
		if t.taskName != "" && t.taskName != workflow.End {
			realTasks = append(realTasks, t)
		}
	}

	isFanOutSource := graph.IsFanOutSource(taskName)
	isPushTransition := isFanOutSource && !errorRouted && resultFlow.GotoRequested() == ""
	cohortSize := len(realTasks)

	// The remaining post-completion writes. Like the transition transaction, these run with the step already
	// `completed`, so a database error here would drive the recovery defer to rewind and replay the step - forever,
	// if the error is permanent. Same class, same treatment: retry the write in place, and classify it if it will not
	// land (see persistStep).

	// A fan-out source can route STRAIGHT to its own fan-in with no branches - its forEach array was empty, or
	// the task steered there directly (Goto/onError/a `when` or `switch` edge), e.g. "if the array to fan out on
	// is empty, Goto the fan-in". In every such case the source IS the spawn, and the fan-in converges
	// immediately on the source's own state, exactly as an empty cohort would (fireFanInDirect). The source's
	// lineage_id carries through, so a NESTED source's direct fan-in stays a member of its outer cohort. This
	// must be detected regardless of edge kind: a Goto/onError makes isPushTransition false and a `when`/`switch`
	// leaves cohortSize at 1, so the plain cohort path below would otherwise mis-attribute the fan-in arrival -
	// the trunk case fails the not-in-a-cohort guard, and a nested source bumps arrivals on its OUTER spawn and
	// silently drops the branch. The shape is legal, not rejected.
	//
	// routedToEnd is what keeps that convenience from swallowing a deliberate termination. An explicit route to
	// END and "no candidates at all" both leave cohortSize at 0, but only the second is the empty-cohort shape:
	// a task on a fan-out source that calls Goto(workflow.End) - "fan out over the array, or end the flow when there is
	// nothing to do" - has asked to terminate, and converging on the fan-in instead sends the flow back around
	// the loop that reaches the source again, writing a step per lap until something outside kills it.
	fanInOfSource := ""
	if isFanOutSource {
		fanInOfSource = cg.fanIn.For(taskName)
	}
	if isFanOutSource && fanInOfSource != "" &&
		((cohortSize == 0 && !routedToEnd) || (cohortSize == 1 && realTasks[0].taskName == fanInOfSource)) {
		return e.persistStepYielding(ctx, db, shardNum, stepID, leaseSeq, flowID, flowToken, taskName, &persistPass, func() error {
			return e.fireFanInDirect(ctx, shardNum, db, flowID, stepID, leaseSeq, stepDepth, lineageID, fanOutOrdinal, fanInOfSource, dispatchURLOf(graph, fanInOfSource), workflowURL, graph, sleepDur, flowPriority, flowFairnessKey, flowFairnessWeight, flowTimeBudgetMs, inheritCancel)
		})
	}
	if isFanOutSource && cohortSize == 0 {
		// Either a fan-out source that routed to END, or an empty forEach with NO fan-in to converge on
		// (fanInOfSource == ""). Only a TRUNK source (lineage_id == 0) may complete the flow; a fan-out source
		// that is itself a cohort MEMBER (a nested fan-out) would terminalize the flow while its outer siblings
		// are still running. (Unreachable for a validated graph - Validate requires a fan-out source to converge
		// on a SetFanIn node, and rejects a route to END that leaves the outer frame unpopped - so this is
		// defense in depth for a graph frozen before those checks.)
		if lineageID != 0 {
			reason := "has no fan-in task to converge on"
			if routedToEnd {
				reason = "routed to END without converging on its fan-in task"
			}
			return e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken,
				errors.New("fan-out source '%s' is inside a cohort but %s", taskName, reason),
				taskName)
		}
		return e.persistStepYielding(ctx, db, shardNum, stepID, leaseSeq, flowID, flowToken, taskName, &persistPass, func() error {
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
				errors.New("task '%s' is inside a fan-out cohort but matched no onward transition; every branch must reach a fan-in task", taskName),
				taskName)
		}
		return e.persistStepYielding(ctx, db, shardNum, stepID, leaseSeq, flowID, flowToken, taskName, &persistPass, func() error {
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
	// pending, it is replayed, and it fails identically. An unbounded hot loop that hammers the database and never
	// advances the flow.
	//
	// A fan-out source routing to its OWN fan-in is legal and was already handled above (fireFanInDirect) -
	// it never reaches here. What remains is a step with no cohort of its own reaching a fan-in: a trunk
	// non-source step routed into a fan-in node (validateLineage rejects it, so a graph validated today
	// cannot produce it; the guard covers a graph frozen onto a flow BEFORE that check, replayed unvalidated
	// on every dispatch). Failing the step is the honest outcome - the flow terminates with a clear error
	// instead of hot-looping forever.
	if fanInArrivals > 0 && cohortSpawnID == 0 {
		return e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken,
			errors.New("task '%s' is routed to fan-in task '%s' but is not part of a fan-out cohort", taskName, fanInTaskName),
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
	linearStateJSON, linearRefsJSON, err := e.linkerFor(shardNum).Mint(childInputState, accumulatedChanges, inheritedRefs, stepID, len(normalNexts), nil, nil)
	if err != nil {
		err = e.failAndReturn(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, err, taskName)
		return errors.Trace(err)
	}
	nextStepDepth := stepDepth + 1
	sleepMs := sleepDur.Milliseconds()

	var newStepIDs []int
	// A cohort resolving as a loss fails the flow in this transaction; its post-commit tail runs after it.
	var loss flowLoss
	// State bytes moved by this transition (successor snapshots, fan-in merge), accumulated in the
	// closure and emitted only after commit (see stateByteCount).
	var txBytes stateByteCount

	// The transition (insert next steps, then advance or fail the flow) runs as one retryable
	// transaction. Under pessimistic locking it can deadlock with a concurrent worker, and a deadlocked
	// attempt MUST re-run rather than leave the just-completed step with no successor — which would wedge
	// the flow, since a completed step is not lease-recoverable. Transact rolls back and re-runs the
	// closure (re-reading the fan-in counts so the decision stays correct), and its Tx records any
	// statement error so a partial transition can never commit.
	// The step is already `completed` at this point, so a database error here is the OTHER half of the eternal
	// loop: the recovery defer rewinds it to pending and it is replayed, failing the same way. Retry the transaction in place
	// first (Transact re-runs its own closure, which re-derives every value it writes), and if it will not land,
	// classify it rather than spin - see persist / failOnPersistError.
	//
	// Serialize this cohort's arrivals through a per-peer stripe (e.cohortLocks) BEFORE the transition tx
	// opens - the point a losing sibling would otherwise take its connection and then queue on the spawn
	// row's write lock, tying that connection up until the holder commits. cohortSpawnID (the dispatched
	// step's lineage_id) is already in hand from the claim, so the key costs no read. It is released the
	// instant persist returns (below), NOT at function exit: a non-contention persist error runs
	// failOnPersistError -> failStep on this SAME stripe, and the stripe is not reentrant. The deferred
	// unlock is only the panic backstop; the normal release is the explicit one before the switch.
	var cohortMu *cohortStripe
	if fanInArrivals > 0 && cohortSpawnID != 0 {
		stripe := &e.cohortLocks[cohortLockStripe(shardNum, cohortSpawnID)]
		// Park on the stripe holding NO turn. The stripe exists precisely so a losing sibling waits without
		// occupying a connection, and a turn is that occupancy one level up - holding one here would put a
		// whole cohort's losers into the turnstile, which is the queue the stripe was built to keep them
		// out of. Yielding restores the property the stripe is for.
		held := false
		yieldTurn(ctx, &persistPass, func() {
			held = e.lockCohortStripe(ctx, stripe, db, shardNum, stepID, leaseSeq)
		})
		if !held {
			return nil // the step was taken back while it waited; its new owner replays it
		}
		cohortMu = stripe
		defer func() {
			if cohortMu != nil {
				cohortMu.unlock()
			}
		}()
	}
	err = e.persistYielding(ctx, db, shardNum, stepID, leaseSeq, &persistPass, func() error {
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			newStepIDs = newStepIDs[:0]
			loss = flowLoss{}
			txBytes = stateByteCount{}

			// engineseams.FaultContention returns a retryable lock-contention error (consumed on the first attempt), so the
			// test proves Transact rolls back and re-runs the closure to a clean commit. engineseams.FaultTransitionCommit
			// returns a non-retryable error, so the tx fails after the step was already marked completed and the
			// processStep recovery defer must reset it (completed->pending) and re-dispatch. Both are scoped to
			// the completing task and checked before the flow-row write, so a fired fault rolls back nothing.
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultContention, taskName)) {
				return errors.New("database is locked (injected fault: " + engineseams.FaultContention + " " + taskName + ")")
			}
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultTransitionCommit, taskName)) {
				return errors.New("injected fault: " + engineseams.FaultTransitionCommit + " " + taskName)
			}

			flowRowLocked := false

			// A NON-FINAL cohort arrival is the one disposition that neither extends nor terminalizes the flow:
			// it inserts no successor, advances no step_id, and its entire durable effect is the cohort_arrivals
			// bump on the SPAWN step below. Grabbing the flow row for it serializes EVERY sibling of a cohort on
			// one row - measured at fan-out width 64: 20 of 43 active backends queued on this exact statement,
			// each holding a pool connection - to perform two writes that carry no information: the grab itself,
			// and a step_id rewrite of 0 over 0 (step_id is 0 throughout a fan-out). So the grab is DEFERRED to
			// the arrival that resolves the cohort, which is the only one that inserts a fan-in step or fails the
			// flow. failStep/propagateCohortFailure, the failure-side twin of this path, bumps the same counters
			// steps-first with no flow-row lock at all.
			//
			// Three properties hold with the grab deferred:
			//   - Write-first: the bump IS a write, so it is the transaction's first statement and the SQLite
			//     SHARED->EXCLUSIVE upgrade deadlock stays closed (the same argument that makes handleInterrupt
			//     legitimately steps-first).
			//   - The terminal-status guard sits in front of every write that can extend or terminalize the flow.
			//     A pure arrival on a flow Terminate already stopped is not guarded: its bump commits, and it is
			//     the arrival's only write. Pinned by TestTerminateVsNonFinalCohortArrival_Deterministic and
			//     TestTerminateVsLastCohortArrival_Deterministic.
			//   - Exactly-one-resolver comes from the spawn row's write lock, held across the bump and the re-read
			//     below within one transaction - never from the flow row.
			// isPushTransition is excluded because a fan-out SOURCE writes cohort_size to its own step row and
			// may carry successors, so it is not a pure arrival.
			pureArrival := fanInArrivals > 0 && len(normalNexts) == 0 && !isPushTransition

			// The transition's statements go out as statement batches - one round trip each where the driver
			// pipelines.
			batch := newTxBatch(ctx, tx)

			// Write-first: queueGrab takes the flow row's lock before any step is written (except the deferred grab
			// of a cohort arrival, below), guarded on non-terminal status, so a transition racing a Terminate or
			// failStep inserts nothing into a terminal flow. The grab flips the non-indexed `touch` column, which
			// always changes value, so RowsAffected reflects the WHERE match on every driver. It is batched with what
			// follows it, so its guard is read only after those statements ran: a terminal flow matches no row, and
			// its reader returns bail and leaves flowRowLocked false.
			queueGrab := func(bail error) {
				if e.seams.Enabled() { // counting checkpoint; Enabled gates the assembled name in production
					e.seams.Checkpoint(ctx, engineseams.Join(engineseams.CheckpointFlowRowWrite, strconv.Itoa(flowID)))
				}
				batch.queue(func(res *sequel.BatchResults) error {
					grab, err := res.Exec()
					if err != nil {
						return errors.Trace(err)
					}
					if n, _ := grab.RowsAffected(); n == 0 {
						return bail
					}
					flowRowLocked = true
					return nil
				}, lockFlowRowSQL(), flowID)
			}
			// A transition on a terminal flow rolls back what was batched with the grab, which leaves it the same
			// clean no-op as a bail before any write.
			if !pureArrival {
				queueGrab(errTransitionFlowTerminal)
			}

			succ := successorBatch{
				flowID: flowID, stepDepth: nextStepDepth, timeBudgetMs: flowTimeBudgetMs, lineageID: childLineageID,
				predecessorID: stepID, sleepMs: sleepMs, priority: flowPriority, fairnessKey: flowFairnessKey,
				fairnessWeight: flowFairnessWeight, engineID: e.engineID, inheritCancel: inheritCancel,
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
					// A forEach branch's state is the flow state plus its element and ordinal context. Do not strip
					// the source array from it to save N copies: that is a byte optimization for the one field the
					// engine happens to know the name of, while every other carried field pays the same cost; it
					// makes a branch's state a LIE - the branch cannot see the array its own element came from -
					// and it makes a failed fan-out's final_state come back missing the array (a completed
					// sibling's branch-local snapshot is the merge base there). The general mechanism that owns the
					// cost is state refs: the array is REF'd, so the branches share one copy of it and each still
					// sees it whole.
					perStepState := childInputState.Clone()
					_ = perStepState.Set(next.itemKey, next.item)
					_ = perStepState.Set(next.itemKey+"Index", next.cohortIndex)
					_ = perStepState.Set(next.itemKey+"Count", next.cohortCount)
					// The ordinal context is SYNTHESIZED per branch - its bytes are in no step row at all - so it
					// can never be ref'd; a ref to it would dangle. The element is one element of the source
					// array, so Mint may point it into whichever anchor holds the array. Everything else in the
					// branch's state comes from this step and re-mints exactly as the linear case.
					noRef := map[string]bool{next.itemKey + "Index": true, next.itemKey + "Count": true}
					item := &staterefs.Item{Key: next.itemKey, Source: next.forEachKey, Index: next.cohortIndex}
					var mintErr error
					stepStateJSON, stepRefsJSON, mintErr = e.linkerFor(shardNum).Mint(perStepState, accumulatedChanges, inheritedRefs, stepID, len(normalNexts), noRef, item)
					if mintErr != nil {
						return errors.Trace(mintErr)
					}
				}
				succ.rows = append(succ.rows, successorRow{
					taskName: next.taskName, taskURL: dispatchURLOf(graph, next.taskName),
					stateJSON: stepStateJSON, refsJSON: stepRefsJSON, ordinal: successorOrdinal,
				})
				txBytes.stateWritten += len(stepStateJSON)
				if len(succ.rows) == successorBatchMax {
					// A wide fan-out goes successorBatchMax rows to a batch, so it never holds every branch's
					// state at once.
					succ.queue(batch, &newStepIDs)
					if err := batch.flush(); err != nil {
						return err
					}
				}
			}
			succ.queue(batch, &newStepIDs)

			// The own-row write and the flow advance take the new step's id. successor_id is the FIRST successor,
			// which History's step navigation follows, and no last-id function names the first row of a multi-row
			// INSERT, so only a lone successor can be named in SQL.
			successor, successorArgs := "", []any(nil)
			switch {
			case len(normalNexts) == 1:
				var err error
				successor, successorArgs, err = batch.insertedID(lastStepIDSQL, func() any { return newStepIDs[0] })
				if err != nil {
					return err
				}
			case len(normalNexts) > 1:
				if err := batch.flush(); err != nil {
					return err
				}
				successor, successorArgs = "?", []any{newStepIDs[0]}
			}

			// The dispatched row's own write - its successor link, its cohort size, and the advancing clear - is ONE
			// statement fenced on this dispatch's lease generation. The fence is what makes the orphan sweep's rewind
			// safe: the rewind bumps lease_seq, so a worker that was merely slow (not dead) rolls this transaction
			// back here instead of committing a second set of successors next to the re-dispatch's. Requiring
			// advancing=1 fences one more case: persist re-running this transaction after a COMMIT that landed but
			// reported an error finds the flag already cleared, and abandons rather than advancing the flow twice. A
			// fan-in arrival clears the flag in its cohort bump below instead, so every transaction that commits while
			// the flow stays live clears it.
			if len(normalNexts) > 0 || isPushTransition {
				set, args := "", append([]any{}, successorArgs...)
				if successor != "" {
					set += "successor_id=" + successor + ", "
				}
				if isPushTransition {
					set += "cohort_size=?, "
					args = append(args, cohortSize)
				}
				args = append(args, stepID, leaseSeq)
				batch.queue(func(res *sequel.BatchResults) error {
					fence, err := res.Exec()
					if err != nil {
						return errors.Trace(err)
					}
					if n, _ := fence.RowsAffected(); n == 0 {
						return errLeaseFenced
					}
					if isPushTransition {
						e.logger.DebugContext(ctx, "Fan-out cohort spawned", "flow", keys.CorrelationID(shardNum, flowID), "spawnStep", stepID, "task", taskName, "cohortSize", cohortSize)
					}
					return nil
				}, "UPDATE dwarf_steps SET "+set+"advancing=0 WHERE step_id=? AND lease_seq=? AND advancing=1", args...)
			}
			fanInID, fanInArgs := "", []any(nil) // the fan-in step's id in SQL, when this arrival inserts one
			if fanInArrivals > 0 {
				// The bump runs alone, on no open batch: whether the cohort resolved is decided from it.
				if err := batch.flush(); err != nil {
					return err
				}
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
				// way. A missing spawn row yields sql.ErrNoRows, preserving the existing error path (the
				// trunk-step-into-a-fan-in case, guarded above).
				//
				// The same statement clears this step's own advancing flag, fenced on its lease generation, so a
				// pure arrival - which writes nothing else to its own row - still clears it without a round trip.
				arrivals, size, failures, cancellations, spawnLineageID, err := bumpCohortArrivals(ctx, tx, cohortSpawnID, fanInArrivals, stepID, leaseSeq)
				if err != nil {
					return errors.Trace(err)
				}
				fullyResolved := size > 0 && arrivals >= size
				// The cohort resolves here, so this arrival is about to extend (fan-in step) or terminalize
				// (cohort failure) the flow - exactly the writes the terminal guard protects - so a deferred grab is
				// taken now, before either. A zero-row match means Terminate/failStep won the race: bail as a clean
				// no-op, which still commits the bump (arrivals then equals size, never exceeds it). A graceful Cancel
				// that landed while this step sat between its completion write and here marked nothing - every
				// member is already completed - so it reaches the flow only through the mark the fan-in insert
				// inherits (inheritCancel). Pinned by TestCancelVsLastCohortArrival_Deterministic.
				//
				// The cohort's reads go out in the same batch as the grab. They are reads, so a bail that commits the
				// bump commits nothing else.
				var cohort cohortRead
				if fullyResolved {
					if pureArrival {
						queueGrab(nil)
					}
					if failures == 0 {
						queueCohortRead(batch, flowID, cohortSpawnID, &cohort)
					}
					if err := batch.flush(); err != nil {
						return err
					}
					if !flowRowLocked {
						return nil
					}
				}
				if fullyResolved && failures == 0 {
					row, depth, exits, fanInBytes, err := e.fanInRow(ctx, tx, shardNum, nextStepDepth, cohortSpawnID, &cohort, fanInTaskName, graph, workflowURL)
					if err != nil {
						return errors.Trace(err)
					}
					txBytes.add(fanInBytes)
					fanIn := succ
					fanIn.stepDepth, fanIn.lineageID, fanIn.rows = depth, spawnLineageID, []successorRow{row}
					fanIn.queue(batch, &newStepIDs)
					// The cohort-exit -> fan-in edges and the flow advance below take the fan-in step's id.
					fanInID, fanInArgs, err = batch.insertedID(lastStepIDSQL, func() any { return newStepIDs[len(newStepIDs)-1] })
					if err != nil {
						return err
					}
					// The exit steps are targeted by primary key, collected from the cohort read, so the write locks
					// only those rows: a (flow_id, lineage_id, task_name IN ...) predicate has no supporting index
					// and scan-locked dwarf_steps, deadlocking concurrent fan-ins on SQL Server.
					if len(exits) > 0 {
						args := append([]any{}, fanInArgs...)
						for _, id := range exits {
							args = append(args, id)
						}
						batch.queue(execOnly, "UPDATE dwarf_steps SET successor_id="+fanInID+" WHERE step_id IN ("+strings.Repeat("?,", len(exits)-1)+"?)", args...)
					}
				} else if fullyResolved && failures > 0 {
					// The cohort resolves as a loss, and is a cancellation only if every failure in it was one.
					failFlow := spawnLineageID == 0
					flowCancelled := cancellations == failures
					if !failFlow {
						var pcfErr error
						failFlow, flowCancelled, pcfErr = e.propagateCohortFailure(ctx, tx, spawnLineageID, flowCancelled)
						if pcfErr != nil {
							return errors.Trace(pcfErr)
						}
					}
					if failFlow {
						// The cohort fully resolved here (this branch completed last), so every sibling has
						// settled - nothing is stranded.
						var parentStepID int
						tx.QueryRowContext(ctx, "SELECT surgraph_step_id FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&parentStepID)
						var lerr error
						loss, lerr = e.failFlowTx(ctx, tx, shardNum, flowID, flowCancelled, nil, parentStepID)
						if lerr != nil {
							return errors.Trace(lerr)
						}
					}
				}
			}

			// The flow advances to its only new step - the fan-in step, or a lone successor; with several, or none, no
			// one step is. Gated on holding the flow row (a queued grab either takes it or ends the transaction): an
			// unresolved pure arrival never takes the grab, and its write here would be step_id=0 over the 0 a fan-out
			// already carries - pure WAL amplification on the flow's hottest row (two extra tuple
			// versions per sibling; 126 per 64-wide cohort) plus the lock hold that serializes the cohort.
			if loss.status == "" && (!pureArrival || flowRowLocked) {
				if e.seams.Enabled() { // counting checkpoint; Enabled gates the assembled name in production
					e.seams.Checkpoint(ctx, engineseams.Join(engineseams.CheckpointFlowRowWrite, strconv.Itoa(flowID)))
				}
				target, targetArgs := "0", []any(nil)
				switch {
				case fanInID != "" && len(normalNexts) == 0:
					target, targetArgs = fanInID, fanInArgs
				case fanInID == "" && len(normalNexts) == 1:
					target, targetArgs = successor, successorArgs
				}
				batch.queue(execOnly, "UPDATE dwarf_flows SET step_id="+target+", touch=1-touch WHERE flow_id=?", append(append([]any{}, targetArgs...), flowID)...)
			}
			return batch.flush()
		})
		if errors.Is(err, errTransitionFlowTerminal) {
			txBytes = stateByteCount{} // nothing the transaction wrote survived the rollback
			return nil
		}
		return err
	})
	if cohortMu != nil {
		// Release before the post-commit switch: a non-contention error routes to failOnPersistError ->
		// failStep, which re-locks this same stripe, and the stripe is not reentrant. The arrival's DB work
		// is committed by here, so no sibling needs us to hold it any longer.
		cohortMu.unlock()
		cohortMu = nil
	}
	if landed, err := e.settlePersist(ctx, err, shardNum, stepID, leaseSeq, flowID, flowToken, taskName); !landed {
		return err
	}
	e.metricStateWriteBytes(ctx, workflowURL, "state", txBytes.stateWritten)
	e.metricStateReadBytes(ctx, workflowURL, "state", txBytes.stateRead)
	e.metricStateReadBytes(ctx, workflowURL, "changes", txBytes.changesRead)

	if loss.status != "" {
		e.endLostFlow(ctx, shardNum, flowID, flowToken, workflowURL, loss)
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

// onErrValue is the value an error-routed step records under onErr for its handler: the error WITHOUT its stack
// frames. onErr rides into changes -> final_state and is readable by any state-bearing reader (Snapshot's
// final_state, Step's state/changes), and internal stack traces are code-structure disclosure. The handler still
// gets the message, status code, trace id and properties for routing. It is a shallow copy, so the shared error
// keeps its stack for logs and a later failStep.
func onErrValue(err error) *errors.TracedError {
	redacted := *errors.Convert(err)
	redacted.Stack = nil
	return &redacted
}

// readCancelReason reads a flow's cancel reason fresh. A step that sees its cancel mark may hold a flow row read
// before the reason was written (Cancel writes the reason before the mark), so the reason is re-read rather than
// taken from that row.
func readCancelReason(ctx context.Context, db sequel.Executor, flowID int) (string, error) {
	var reason string
	err := db.QueryRowContext(ctx, "SELECT cancel_reason FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&reason)
	return reason, errors.Trace(err)
}

// errLeaseFenced is an in-transaction sentinel: a post-execution write found the dispatch's lease had
// been re-granted to a peer, so the transaction must roll back and the worker must abandon quietly. It is
// never surfaced - callers detect it via a captured `fenced` bool, or persist maps it to errPersistFenced,
// and return nil. See "Lease fencing".
var errLeaseFenced = errors.New("dispatch lease fenced")

// errTransitionFlowTerminal is an in-transaction sentinel: the transition's flow-row lock grab found the flow
// already terminal, after the successor INSERTs batched with it had run. Rolling them back leaves the transition
// the same clean no-op as a bail before any write, so the transaction's caller maps it to nil.
var errTransitionFlowTerminal = errors.New("flow terminal at transition")

// lockFlowRowSQL takes the flow row's lock, flipping the non-indexed touch column, and affects no row when the
// flow is terminal.
func lockFlowRowSQL() string {
	return "UPDATE dwarf_flows SET touch=1-touch WHERE flow_id=? AND status NOT IN (" + terminalStatusesSQL + ")"
}

// maxGotoTargetLen is the width of dwarf_steps.advance_goto, which matches task_name's.
const maxGotoTargetLen = 512

// bumpCohortArrivals adds n arrivals to the cohort spawned by spawnID and returns the spawn row's counters as
// updated. The same statement clears the arriving step's advancing flag, fenced on leaseSeq and on the flag still
// being set (see the successor write in processStep for why both); a fenced arrival returns errLeaseFenced (the
// caller's transaction must roll back, undoing the bump), and a missing spawn row returns sql.ErrNoRows.
//
// One statement covers two rows rather than adding a second one for the arriving step, because the bump holds
// the spawn row's lock until commit and every sibling of the cohort queues on it. The arriving row is its own
// worker's and uncontended, and it always sorts after its spawn (a later insert, a larger step_id), so the lock
// order is the one every other writer takes. A fan-out source arriving at its own fan-in is its own spawn
// (spawnID == stepID): the one row takes both writes, and the source's own fenced write has already run.
func bumpCohortArrivals(ctx context.Context, tx *sequel.Tx, spawnID, n, stepID, leaseSeq int) (arrivals, size, failures, cancellations, spawnLineageID int, err error) {
	const set = "SET cohort_arrivals = cohort_arrivals + CASE WHEN step_id=? THEN ? ELSE 0 END, advancing = CASE WHEN step_id=? THEN 0 ELSE advancing END"
	const where = " WHERE step_id IN (?, ?) AND (step_id=? OR (lease_seq=? AND advancing=1))"
	const cols = "step_id, cohort_arrivals, cohort_size, cohort_failures, cohort_cancellations, lineage_id"
	args := []any{spawnID, n, stepID, spawnID, stepID, spawnID, leaseSeq}
	var rows *sequel.Rows
	switch tx.DriverName() {
	case "pgx", "sqlite":
		rows, err = tx.QueryContext(ctx, "UPDATE dwarf_steps "+set+where+" RETURNING "+cols, args...)
	case "mssql":
		rows, err = tx.QueryContext(ctx, "UPDATE dwarf_steps "+set+
			" OUTPUT INSERTED.step_id, INSERTED.cohort_arrivals, INSERTED.cohort_size, INSERTED.cohort_failures, INSERTED.cohort_cancellations, INSERTED.lineage_id"+
			where, args...)
	default:
		// MySQL lacks RETURNING, so the bump and the read are two statements, serial on the transaction's one
		// connection. The fence is RowsAffected, which is honest here although MySQL counts CHANGED rows: every
		// row this statement matches changes - the spawn's arrivals rise by n (always at least one) and the arriving
		// step's flag goes 1 -> 0, the fence requiring it set. The read then needs only the spawn row, which this
		// transaction just wrote, so it sees the bump whatever snapshot the transaction holds.
		res, xerr := tx.ExecContext(ctx, "UPDATE dwarf_steps "+set+where, args...)
		if xerr != nil {
			return 0, 0, 0, 0, 0, errors.Trace(xerr)
		}
		want := int64(2)
		if spawnID == stepID {
			want = 1
		}
		changed, _ := res.RowsAffected()
		if xerr := tx.QueryRowContext(ctx,
			"SELECT cohort_arrivals, cohort_size, cohort_failures, cohort_cancellations, lineage_id FROM dwarf_steps WHERE step_id=?", spawnID,
		).Scan(&arrivals, &size, &failures, &cancellations, &spawnLineageID); xerr != nil {
			return 0, 0, 0, 0, 0, errors.Trace(xerr) // sql.ErrNoRows for a missing spawn, as from the other dialects
		}
		if changed < want {
			return 0, 0, 0, 0, 0, errLeaseFenced
		}
		return arrivals, size, failures, cancellations, spawnLineageID, nil
	}
	if err != nil {
		return 0, 0, 0, 0, 0, errors.Trace(err)
	}
	defer rows.Close()
	spawnSeen, stepSeen := false, false
	for rows.Next() {
		var id, a, s, f, c, l int
		if err := rows.Scan(&id, &a, &s, &f, &c, &l); err != nil {
			return 0, 0, 0, 0, 0, errors.Trace(err)
		}
		if id == spawnID {
			spawnSeen = true
			arrivals, size, failures, cancellations, spawnLineageID = a, s, f, c, l
		}
		if id == stepID {
			stepSeen = true
		}
	}
	if err := rows.Err(); err != nil {
		return 0, 0, 0, 0, 0, errors.Trace(err)
	}
	if !spawnSeen {
		return 0, 0, 0, 0, 0, sql.ErrNoRows
	}
	if !stepSeen {
		return 0, 0, 0, 0, 0, errLeaseFenced
	}
	return arrivals, size, failures, cancellations, spawnLineageID, nil
}

// handleInterrupt pauses a flow for external input.
func (e *Engine) handleInterrupt(ctx context.Context, shardNum int, db *sequel.DB, stepID, leaseSeq int, flowID int, flowToken string, workflowURL string, changesJSON []byte, interruptPayload workflow.State) error {
	chainFlowIDs, chainStepIDs, chainCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	fenced := false
	payloadLen := 0
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		fenced = false
		payloadLen = 0
		// Steps-first-then-flow lock ordering, matching resume and Terminate (which walk this same surgraph
		// chain). Interrupt is non-terminating, so it carries no write-first orphan obligation; the only
		// requirement is that the first statement be a write (satisfied here, keeping SQLite's shared-lock
		// upgrade deadlock closed). Ordering steps before flows removes the cycle with a concurrent
		// resume, which holds the chain step rows and wants the chain flow rows: acquiring in the same order
		// on both sides means one blocks rather than deadlocks. The flow-first transition/completion cluster
		// (the transition transaction, completeFlow) cannot cycle with this path either: the only step rows it
		// locks besides its own are a cohort's spawn and exit rows, and an interrupting leaf is a member that has
		// not arrived, so its cohort cannot be resolving while it interrupts.
		allStepIDs := append([]any{stepID}, chainStepIDs...)
		stepPlaceholders := strings.Repeat("?,", len(allStepIDs)-1) + "?"
		stepArgs := []any{stepID, changesJSON, stepID, workflow.StatusInterrupted, parkedNone}
		stepArgs = append(stepArgs, allStepIDs...)
		chainSQL := "UPDATE dwarf_steps SET changes=CASE WHEN step_id=? THEN ? ELSE changes END, interrupt_done=CASE WHEN step_id=? THEN 1 ELSE interrupt_done END, status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN (" + stepPlaceholders + ") AND status IN ('" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')"
		// engineseams.FaultInterruptChainWrite reproduces a deadlock victim dialect-independently: the statement errors and
		// applies NOTHING, so the leaf stays `running` - the shape that must not read back as a genuine fence.
		if e.seams.IsFault(engineseams.FaultInterruptChainWrite) {
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
		// that window, since the lost branch never marked its caller step interrupted. sequel's QueryRow
		// reports the transaction's error like every other statement method, so the check below catches it and
		// the write is retried. Observed on SQL Server, where two parallel subgraph callers interrupting at once deadlock:
		// moving a step running->interrupted deletes keys from the three status-filtered dwarf_steps indexes,
		// so even disjoint row sets cycle. Never let this error go unchecked, and never decide the fence ahead
		// of it.
		var curLeaseSeq int
		var curStatus string
		if serr := tx.QueryRowContext(ctx, "SELECT lease_seq, status FROM dwarf_steps WHERE step_id=?", stepID).Scan(&curLeaseSeq, &curStatus); serr != nil {
			return errors.Trace(serr)
		}
		// engineseams.FaultInterruptStaleWrite forces the generation to look re-granted (as a real zombie would, after a peer
		// re-claimed the leaf), so the fence trips and the WHOLE transaction rolls back - undoing the ancestor
		// re-park - and the worker abandons. The only fence in the engine that rolls back rather than no-ops.
		if e.seams.IsFault(engineseams.FaultInterruptStaleWrite) {
			curLeaseSeq = leaseSeq + 1
		}
		if curLeaseSeq != leaseSeq || curStatus != workflow.StatusInterrupted {
			fenced = true
			return errLeaseFenced
		}

		if interruptPayload.Len() > 0 {
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

// fireFanInDirect creates the fan-in step immediately for a fan-out source that converges with no branches:
// an empty forEach, or a goto/onError/when/switch route straight to the source's own fan-in. The source is the
// spawn, so the fan-in runs on the source's own state and changes.
func (e *Engine) fireFanInDirect(ctx context.Context, shardNum int, db *sequel.DB, flowID int, stepID, leaseSeq int, stepDepth int, lineageID int, fanOutOrdinal int, fanInTarget, fanInURL string, workflowURL string, graph *workflow.Graph, sleepDur time.Duration, priority int, fairnessKey string, fairnessWeight float64, timeBudgetMs int, inheritCancel int) error {
	var fanInStepID int64
	var txBytes stateByteCount
	err := db.Transact(ctx, func(tx *sequel.Tx) error {
		fanInStepID = 0
		txBytes = stateByteCount{}
		// Write-first, guarded on non-terminal status - the same lock-grab the transition tx uses. If a
		// concurrent Terminate/failStep terminalized this flow, the guard yields zero rows and this becomes a
		// clean no-op; without it we would insert a pending fan-in step and overwrite step_id on a terminal
		// flow (orphan work reaped only later by the claim-time terminal-flow guard). `touch` always changes
		// value, so RowsAffected reflects the WHERE match on every driver.
		flowRes, flowErr := tx.ExecContext(ctx, lockFlowRowSQL(), flowID)
		if flowErr != nil {
			return errors.Trace(flowErr)
		}
		if n, _ := flowRes.RowsAffected(); n == 0 {
			return nil
		}

		var ourStateJSON, ourChangesJSON, ourRefsJSON []byte
		tx.QueryRowContext(ctx, "SELECT state, changes, state_refs FROM dwarf_steps WHERE step_id=?", stepID).Scan(&ourStateJSON, &ourChangesJSON, &ourRefsJSON)
		txBytes.stateRead = len(ourStateJSON)
		txBytes.changesRead = len(ourChangesJSON)
		ourState, _ := workflow.NewState(ourStateJSON)
		ourChanges, _ := workflow.NewState(ourChangesJSON)
		// Resolve only what the reducers actually fold; a merely-carried ref rides through the (empty) cohort
		// untouched, still pointing at its original anchor - see resolveReducedRefs. resolveReducedRefs mutates
		// the state map in place, so it gets the live map.
		ourRefs, rerr := staterefs.Parse(ourRefsJSON)
		if rerr != nil {
			return errors.Trace(rerr)
		}
		rerr = e.resolveReducedRefs(ctx, tx, shardNum, ourState, ourRefs, graph.Reducers(), workflowURL)
		if rerr != nil {
			return errors.Trace(rerr)
		}
		// Fold our own delta via the reducers, then materialize: DelNils enacts replace-field deletes, and a
		// cleared reduced field was ignored during the fold. An empty forEach spawns no members, so this is the
		// whole merge - agreeing with fanInRow's populated path on the same primitive.
		mergedState := ourState.Clone()
		rerr = mergedState.MergeReduce(ourChanges, graph.Reducers())
		if rerr != nil {
			return errors.Trace(&permanentError{rerr})
		}
		mergedState.DelNils()
		// This step IS the cohort spawn (an empty forEach spawns no branches), so it anchors exactly as a
		// populated cohort's spawn does in fanInRow. There are no members, but a field this step wrote
		// through a COMBINING reducer still has a merged value (reduce(ourState[k], ourChanges[k])) that exists
		// in no row, so it must be inlined rather than anchored - the same exclusion fanInRow applies.
		inlineOnly := staterefs.CombinedReducerFields(ourState, ourChanges, graph.Reducers())
		mergedJSON, refsJSON, merr := e.linkerFor(shardNum).Mint(mergedState, ourChanges, ourRefs, stepID, staterefs.Linear, inlineOnly, nil)
		if merr != nil {
			return errors.Trace(merr)
		}
		txBytes.stateWritten = len(mergedJSON)

		nextStepDepth := stepDepth + 1
		sleepMs := sleepDur.Milliseconds()
		var err error
		fanInStepID, err = tx.InsertReturnID(ctx, "step_id", successorInsertSQL,
			flowID, nextStepDepth, keys.RandomIdentifier(16), fanInTarget, fanInURL, mergedJSON, refsJSON, workflow.StatusPending, parkedNone, timeBudgetMs, lineageID, fanOutOrdinal, stepID, sleepMs, priority, fairnessKey, fairnessWeight, e.engineID,
			inheritCancel, stepID, flowID,
		)
		if err != nil {
			return errors.Trace(err)
		}
		// The dispatched row's own write - its successor link, its (empty) cohort size and the advancing clear - is
		// ONE statement fenced exactly as in the transition transaction, and like it, it follows the INSERT: a
		// fenced attempt returns errLeaseFenced and the rollback takes the INSERT with it.
		stepRes, stepErr := tx.ExecContext(ctx, "UPDATE dwarf_steps SET successor_id=?, cohort_size=0, advancing=0 WHERE step_id=? AND lease_seq=? AND advancing=1", int(fanInStepID), stepID, leaseSeq)
		if stepErr != nil {
			return errors.Trace(stepErr)
		}
		if n, _ := stepRes.RowsAffected(); n == 0 {
			return errLeaseFenced
		}
		tx.ExecContext(ctx, "UPDATE dwarf_flows SET step_id=?, touch=1-touch WHERE flow_id=?", int(fanInStepID), flowID)
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	if fanInStepID == 0 {
		// The terminal-status guard bailed - the flow was terminated/failed/cancelled concurrently, no fan-in
		// step was inserted, nothing to dispatch.
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

// add adds o's counts to c.
func (c *stateByteCount) add(o stateByteCount) {
	c.stateWritten += o.stateWritten
	c.stateRead += o.stateRead
	c.changesRead += o.changesRead
}

// cohortRead is what a resolving arrival reads of its cohort: the spawn step's row and every member's.
type cohortRead struct {
	spawnTaskName      string
	spawnStateJSON     []byte
	spawnChangesJSON   []byte
	spawnRefsJSON      []byte
	spawnFanOutOrdinal int
	members            []cohortMember
	changesRead        int // bytes of the members' changes columns
}

// cohortMember is one row of a cohort, in fan_out_ordinal, step_depth, step_id order. changes is decoded as the
// row is read, and only for a completed member - the only kind whose changes the fold takes.
type cohortMember struct {
	stepID      int
	taskName    string
	completed   bool
	changes     workflow.State
	depth       int
	successorID int
}

// queueCohortRead queues the reads of a resolving arrival's cohort into c.
func queueCohortRead(b *txBatch, flowID, cohortSpawnID int, c *cohortRead) {
	b.queue(func(res *sequel.BatchResults) error {
		err := res.QueryRow().Scan(&c.spawnStateJSON, &c.spawnChangesJSON, &c.spawnRefsJSON, &c.spawnTaskName, &c.spawnFanOutOrdinal)
		return errors.Trace(err)
	}, "SELECT state, changes, state_refs, task_name, fan_out_ordinal FROM dwarf_steps WHERE step_id=?", cohortSpawnID)
	b.queue(func(res *sequel.BatchResults) error {
		rows, err := res.Query()
		if err != nil {
			return errors.Trace(err)
		}
		for rows.Next() {
			var m cohortMember
			var status string
			var changesJSON []byte
			if err := rows.Scan(&m.stepID, &m.taskName, &status, &changesJSON, &m.depth, &m.successorID); err != nil {
				return errors.Trace(err)
			}
			c.changesRead += len(changesJSON)
			if m.completed = status == workflow.StatusCompleted; m.completed {
				m.changes, _ = workflow.NewState(changesJSON)
			}
			c.members = append(c.members, m)
		}
		return errors.Trace(rows.Err())
	}, "SELECT step_id, task_name, status, changes, step_depth, successor_id FROM dwarf_steps WHERE flow_id=? AND lineage_id=? ORDER BY fan_out_ordinal, step_depth, step_id", flowID, cohortSpawnID)
}

// fanInRow folds a resolved cohort, as read by queueCohortRead, into its fan-in step's row. It returns the row,
// the fan-in's step_depth, the cohort-exit steps whose successor_id must point at the fan-in step, and the state
// bytes it moved (read: spawn snapshot + every cohort member's changes; written: the merged fan-in snapshot),
// which the caller emits after its transaction commits.
func (e *Engine) fanInRow(ctx context.Context, tx sequel.Executor, shardNum, nextStepDepth, cohortSpawnID int, c *cohortRead, fanInTaskName string, graph *workflow.Graph, workflowURL string) (row successorRow, depth int, exitStepIDs []int, bytes stateByteCount, err error) {
	bytes = stateByteCount{stateRead: len(c.spawnStateJSON), changesRead: len(c.spawnChangesJSON) + c.changesRead}
	spawnState, _ := workflow.NewState(c.spawnStateJSON)
	spawnChanges, _ := workflow.NewState(c.spawnChangesJSON)
	// Materialize only the ref'd fields a reducer will FOLD - a combining reducer (append/add/union/...) needs
	// its accumulated base, and folding a delta onto an absent base would silently lose everything so far. A
	// merely-carried ref is left alone and re-emitted onto the fan-in step below, still pointing at its
	// original anchor: resolving it here would materialize the payload and re-anchor it at EVERY fan-in,
	// giving back the win in exactly the fan-out graphs this design exists for. resolveReducedRefs mutates
	// the state map in place, so it gets the live map.
	spawnRefs, err := staterefs.Parse(c.spawnRefsJSON)
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(err)
	}
	err = e.resolveReducedRefs(ctx, tx, shardNum, spawnState, spawnRefs, graph.Reducers(), workflowURL)
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(err)
	}
	// The fan-in accumulator's base: the spawn's state with its own delta folded in via the reducers. A
	// cleared reduced field is ignored (reducer identity); a cleared replace field rides as a tombstone that
	// DelNils enacts once all members are folded.
	merged := spawnState.Clone()
	err = merged.MergeReduce(spawnChanges, graph.Reducers())
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(&permanentError{err})
	}
	// Fields a cohort MEMBER contributed. Their bytes are in that member's `changes` - not in the spawn's row -
	// and a reducer's COMBINED output exists in no row at all, so neither can be anchored at the spawn. They are
	// inlined into the fan-in step's own state, which becomes their anchor for everything downstream (the third
	// of the three places an anchor's bytes can sit). A stale spawn ref for such a field is dropped with them.
	memberWrites := map[string]bool{}
	var memberChanges []workflow.State

	// The cohort-exit steps whose successor_id must point at the fan-in step, collected from the cohort read so
	// the caller's write can target them by primary key.
	//
	// A member qualifies as an exit only if it BOTH bears a fan-in-predecessor task name AND has not already
	// recorded a forward edge (successor_id=0). The task-name test alone is not enough: a branch that LOOPS
	// through the exit task via flow.Goto (e.g. `Work -Goto-> Work -> Join`) leaves several `Work` steps in the
	// cohort, but only the last one transitioned to the fan-in - the earlier iterations already point at their
	// next loop step. A step that transitions to a fan-in inserts no successor of its own (the fan-in step is
	// created here, or its arrival is only a counter bump), so its successor_id is still 0; a loop iteration or a
	// multi-step branch's interior step wrote its successor_id when it created that successor. Overwriting the
	// latter corrupts the execution DAG (spurious `step -> fan-in` edges in History).
	exitTaskSet := make(map[string]bool)
	for _, t := range fanInPredecessorTasks(graph, fanInTaskName) {
		exitTaskSet[t] = true
	}
	maxCohortDepth := 0
	for _, m := range c.members {
		if m.depth > maxCohortDepth {
			maxCohortDepth = m.depth
		}
		// A cohort member with an exit task name that has not yet recorded a successor is one that transitioned
		// into the fan-in (see above); a member with the same task name that already has a successor looped or
		// continued elsewhere and must keep its own forward edge.
		if exitTaskSet[strings.TrimSpace(m.taskName)] && m.successorID == 0 {
			exitStepIDs = append(exitStepIDs, m.stepID)
		}
		if !m.completed {
			continue
		}
		for k := range m.changes.Names() {
			memberWrites[k] = true
		}
		memberChanges = append(memberChanges, m.changes)
	}
	// One fold for the whole cohort, not one per member: a combining reducer decodes its accumulator and
	// re-encodes the result, so folding per member re-encodes a value that grows as it goes - quadratic in
	// the accumulated bytes across the cohort's width, and a fan-out is exactly where width is large.
	err = merged.MergeReduceAll(memberChanges, graph.Reducers())
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(&permanentError{err})
	}

	// Materialize the fan-in snapshot: the folds preserved replace-field delete tombstones (accumulation);
	// DelNils enacts them now, before the state is minted onto the fan-in step. Reduced fields are
	// unaffected - a cleared reduced field was ignored during the fold, never stored as a tombstone.
	merged.DelNils()

	// The fan-in sits one level below the DEEPEST cohort branch, not merely below the last sibling to
	// complete - branch lengths can differ (loops, gotos, varying chains), so step_depth must reflect the
	// deepest path the flow took. nextStepDepth (last-completer+1) is the floor for the defensive empty case.
	depth = max(maxCohortDepth+1, nextStepDepth)

	// Drop the per-branch forEach bookkeeping of THIS cohort - the one spawnTaskName fanned out - and of no
	// other. An outer cohort's bookkeeping must survive an inner fan-in: a step converging out of the inner
	// cohort is still inside the outer branch and must still see which element that branch is working on. The
	// same scoped strip runs in computeFinalState for a fan-out that FAILED (and so never reached this
	// convergence), so the two paths agree on what a fan-out's state means once its cohort is behind it.
	//
	// A stripped name must also leave the spawn's refs, or the mint carries it forward from there: in a nested
	// fan-out whose levels share an `as`, the spawn of the inner cohort holds the OUTER element as an item ref,
	// and re-emitting it would bring that element back past the inner fan-in.
	for _, k := range stripForEachBookkeeping(merged, graph, strings.TrimSpace(c.spawnTaskName)) {
		delete(spawnRefs, k)
	}

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
	mergedJSON, refsJSON, err := e.linkerFor(shardNum).Mint(merged, spawnChanges, spawnRefs, cohortSpawnID, staterefs.Linear, memberWrites, nil)
	if err != nil {
		return successorRow{}, 0, nil, stateByteCount{}, errors.Trace(err)
	}
	bytes.stateWritten = len(mergedJSON)
	row = successorRow{
		taskName: fanInTaskName, taskURL: dispatchURLOf(graph, fanInTaskName),
		stateJSON: mergedJSON, refsJSON: refsJSON, ordinal: c.spawnFanOutOrdinal,
	}
	return row, depth, exitStepIDs, bytes, nil
}

// errCallerGone rolls back a subgraph spawn whose caller step is no longer running under this worker's lease -
// terminated, or re-claimed by a peer - so no child is created for it.
var errCallerGone = errors.New("subgraph caller is no longer running")

// dispatchURLOf resolves a graph node name to its dispatch URL.
func dispatchURLOf(graph *workflow.Graph, name string) string {
	if name == workflow.End {
		return name
	}
	if u := graph.URLOf(name); u != "" {
		return u
	}
	return name
}

// claimEligible is the claim's test that the step is still claimable - pending, unparked, due, lease lapsed -
// wrapped in CASE so that no index can serve it, leaving the primary key on step_id as the claim's only index
// path. Written as plain conjuncts, the test implies status='pending', which makes every partial index on
// pending rows (idx_dwarf_steps_due, _selection, _status) a candidate, and a scan of one of them filtered on
// step_id walks the whole backlog. Measured on Postgres: after an ANALYZE taken at a small backlog, both the
// generic and the custom plan claimed through a partial index on pending rows, every claim cost O(backlog),
// and a 5,000 steps/s run fell to ~800 steps/s with its backlog at the cap until an ANALYZE taken at a large
// backlog flipped the plan back. The status stays a literal (the inline-status rule); parked is the caller's
// bind. alias is the table alias with its dot ("s.") or "".
func claimEligible(alias string) string {
	return "CASE WHEN " + alias + "status='" + workflow.StatusPending + "' AND " + alias + "parked=? AND " +
		alias + "not_before<=NOW_UTC() AND " + alias + "lease_expires<=NOW_UTC() THEN 1 END=1"
}

// claimSet is the claim's SET clause: take the lease, bump its generation, and stamp started_at unless the
// dispatch continues an earlier one (a retry, a resumed park, a replay). alias qualifies the step table's columns
// ("s." or ""), since an assignment's right-hand side may need it. Binds: the running status, the lease margin in
// ms, the engine ID.
func claimSet(alias string) string {
	return "status=?, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), " + alias + "time_budget_ms + ?), lease_seq=" + alias + "lease_seq+1, engine_id=?, updated_at=NOW_UTC()," +
		" started_at=CASE WHEN " + alias + "attempt>0 OR " + alias + "subgraph_done=1 OR " + alias + "interrupt_done=1 OR " + alias + "advancing=1 THEN " + alias + "started_at ELSE NOW_UTC() END"
}

// claimReadCols is the step columns the claim reads back, in the order processStep scans them.
var claimReadCols = []string{"step_depth", "task_name", "step_token", "state", "changes", "state_refs", "attempt",
	"lineage_id", "flow_id", "time_budget_ms", "interrupt_done", "resume_data", "subgraph_done", "subgraph_result",
	"subgraph_error", "created_at", "lease_seq", "fan_out_ordinal", "cancelling", "advancing", "advance_goto",
	"advance_sleep_until", "advance_on_error"}

// claimCols is claimReadCols as a select list, each qualified by prefix ("s.", "INSERTED." or ""). A recorded
// sleep is read as what remains of it, on the database clock.
func claimCols(prefix string) string {
	cols := make([]string, len(claimReadCols))
	for i, c := range claimReadCols {
		cols[i] = prefix + c
		if c == "advance_sleep_until" {
			cols[i] = "DATE_DIFF_MILLIS(" + prefix + c + ", NOW_UTC())"
		}
	}
	return strings.Join(cols, ", ")
}

// The claim, per dialect, built once: it runs on every step. Binds: the running status, the lease margin in ms,
// the engine ID, the step ID, the unparked value. claimSQLPostgres is the CAS on the step row, its read, and the
// flow row joined in, in one round trip; SQLite and SQL Server read the step back with RETURNING/OUTPUT; MySQL has
// neither, so it claims and then reads (claimReadMySQL, bound to the step ID).
var (
	claimSQLSQLite   = "UPDATE dwarf_steps SET " + claimSet("") + " WHERE step_id=? AND " + claimEligible("") + " RETURNING " + claimCols("")
	claimSQLMSSQL    = "UPDATE dwarf_steps SET " + claimSet("") + " OUTPUT " + claimCols("INSERTED.") + " WHERE step_id=? AND " + claimEligible("")
	claimSQLMySQL    = "UPDATE dwarf_steps SET " + claimSet("") + " WHERE step_id=? AND " + claimEligible("")
	claimReadMySQL   = "SELECT " + claimCols("") + " FROM dwarf_steps WHERE step_id=?"
	claimSQLPostgres = "UPDATE dwarf_steps s SET " + claimSet("s.") +
		" FROM dwarf_flows f" +
		" WHERE s.step_id=? AND " + claimEligible("s.") + " AND f.flow_id=s.flow_id" +
		" RETURNING " + claimCols("s.") + "," +
		" f.flow_token, f.status, f.workflow_url, f.baggage, f.trace_parent, f.created_at, f.updated_at, f.priority, f.fairness_key, f.fairness_weight, f.time_budget_ms, f.cancel_reason"
)
