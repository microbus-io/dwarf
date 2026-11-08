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
	"database/sql/driver"
	"encoding/json"
	"log/slog"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/cancelmarker"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/faninmap"
	"github.com/microbus-io/dwarf/internal/graphcache"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// dispatch is one run of processStep over one step: claim, load, execute, record the outcome, transition. It is
// data only. Each stage is an Engine method taking it, and the fields are grouped by the stage that sets them: a
// later stage reads a group and does not rewrite it, except where a field says otherwise. It lives for one call
// and is never reused: the step's next claim takes a new lease generation.
type dispatch struct {
	// Set by processStep, before the claim. db is set by claim.
	shard           int
	stepID          int
	db              *sequel.DB
	returnGateTurn  func()
	closeEntryPhase func() // idempotent; execute closes the entry phase at the host call

	// Set by claim. row holds the claimed step's columns and its flow's (a value, so the claim scans into it
	// without a heap allocation per column). row.leaseSeq is this dispatch's lease generation, 0 until the claim
	// captures it; every post-execution write to the step is fenced on it. complete re-reads row.flowCancelReason.
	row     claimRow
	claimed bool

	// Set by load: the step's graph, its resolved input, and the carrier the task runs on.
	graph         *workflow.Graph
	fanIn         *faninmap.Map
	state         workflow.State // the resolved input, kept pristine: the successors are folded from it
	inheritedRefs staterefs.Refs
	refBytes      int // the bytes the dispatch's ref resolve put into the carrier
	priorChanges  workflow.Changes
	reducers      map[string]workflow.Reducer
	flow          *workflow.RawFlow
	baggage       workflow.State

	// Set by startTask: the step's span, and the context the host call runs on.
	taskCtx    context.Context
	taskSpan   trace.Span
	cancelTask context.CancelFunc // nil when the step has no budget

	// Set by processStep after the host call: the turn every outcome write runs under. yieldTurn swaps it, so it
	// is returned through a closure that reads it at the end.
	pass turnstile.Pass

	// Set by outcome; complete rewrites them for a Cancel-covered step it reroutes to its handler.
	result         *workflow.RawFlow
	errorRouted    bool
	errorTarget    string
	changes        workflow.Changes
	changesJSON    []byte
	successorBase  workflow.State
	sleep          time.Duration // set by processStep once no park was requested; complete zeroes it on a reroute
	markedComplete bool          // set by complete: the step is `completed` and its transition has not committed

	// Set by complete, read by the transition.
	completedCancelling bool
	inheritCancel       int

	// Set by transition, read by its transaction.
	normalNexts      []nextStep
	fanInTaskName    string
	fanInArrivals    int
	isPushTransition bool
	cohortSize       int
	cohortSpawnID    int
	childLineageID   int
	linearStateJSON  []byte
	linearRefsJSON   []byte
	inlineFolded     map[string]bool
	folds            map[string]staterefs.Fold

	// Set by the transition's transaction, and reset at the top of each attempt.
	newStepIDs    []int
	loss          flowLoss       // a cohort resolving as a loss fails the flow in the transaction
	txBytes       stateByteCount // emitted only after commit (see stateByteCount)
	flowRowLocked bool
}

// processStep acquires a step, executes its task, and enqueues the next step if applicable.
//
// THE CTX CARRIES THIS JOB'S PLACE IN LINE, stamped by the crew's gate at the moment the candidate was
// picked up, and every database call below takes its turn on it (turnstile.WaitTurn). The host call's
// duration is taken off it when the call returns (see there), so the step ranks by its time at the
// database: ahead of work that arrived after its own began, and never ahead of everything.
//
// returnGateTurn hands back the turn the crew took on this worker's behalf. It is released as soon as the
// claim CAS is done, because that is the only database work it was ever covering: the calls after it take
// their own turns, and holding one turn across the whole entry phase would bound admission by phases
// rather than by round trips. The crew wraps it in sync.OnceFunc and calls it again on the way out, so
// every early return below is covered and a double call is a no-op.
func (e *Engine) processStep(ctx context.Context, shardNum int, stepID int, returnGateTurn func()) (err error) {
	// The ENTRY phase runs from here to the host call. Closed through the returned closure rather than at
	// that one line, because every early return in between - a lost claim, a closed flow, a failed load -
	// must close it too, and a missed close leaks the count upward for the life of the process. Idempotent,
	// so closing it explicitly at the boundary AND deferring costs nothing.
	closeEntryPhase := e.enterDBPhase(phaseEnter)
	defer closeEntryPhase()
	d := &dispatch{shard: shardNum, stepID: stepID, returnGateTurn: returnGateTurn, closeEntryPhase: closeEntryPhase}
	// d.claimed is set once this dispatch wins its claim. The checkpoint's defer is registered before the
	// recovery defer so it runs after it: a test waiting for a dispatch to return sees its recovery reset too.
	if e.seams.Enabled() { // Enabled gates the assembled name in production
		defer func() {
			if d.claimed {
				e.seams.Checkpoint(ctx, engineseams.CheckpointClaimedDispatchDone)
				e.seams.Checkpoint(ctx, engineseams.Join(engineseams.CheckpointClaimedDispatchDone, strconv.Itoa(shardNum), strconv.Itoa(stepID)))
			}
		}()
	}
	// Through a closure, so the recovery reads the ctx this function holds at the end: by then it carries the
	// host call's duration and the persist turn.
	defer func() { e.recoverStep(ctx, d, err) }()

	if done, err := e.claim(ctx, d); done || err != nil {
		return err
	}
	if done, err := e.load(ctx, d); done || err != nil {
		return err
	}
	e.startTask(ctx, d)
	defer d.taskSpan.End()
	if d.cancelTask != nil {
		defer d.cancelTask()
	}
	execErr, hostCall, err := e.execute(ctx, d)
	if err != nil {
		return err
	}

	// The task has now RUN. Everything below records that fact, and it holds a connection to do so, so it
	// takes a turn like every other database call - on this job's pickup claim, moved forward by the host
	// call's duration. The job therefore ranks by the time it has spent at the database: ahead of entries that
	// arrived after its own entry did, which keeps finishing work ahead of starting work under saturation,
	// but never ahead of everything. Keeping the pickup stamp unmoved would age it by the whole task, so once
	// tasks outlast the queue every exit would outrank every entry and every Create - a strict precedence that
	// alternates exit and entry bursts with the pool idle in between.
	//
	// A turn is never refused, so a drain records the work anyway: the outcome exists nowhere but in this
	// goroutine. The return is still mandatory - a lost one shrinks this shard's ceiling permanently - and
	// has no crew-side backstop, the gate's turn having been handed back long ago.
	ctx = turnstile.Away(ctx, hostCall)
	turnWaitStart := time.Now()
	d.pass = turnstile.WaitTurn(ctx)
	// Through a CLOSURE, not `defer d.pass.Return()`: the pass is handed back and re-taken around every
	// wait below that is not the database (yieldTurn), so the deferred call has to see whichever pass is
	// current at the end, not the one that existed when the defer was written.
	defer func() { d.pass.Return() }()
	// Publish it, so a callee that must park on something other than the database - failStep on the cohort
	// stripe - can hand it back without it being threaded through every signature in between.
	ctx = withHeldTurn(ctx, &d.pass)
	// The EXIT phase runs from here to the end of the step. One line rather than the two-sided guard entry
	// needs, because there is no early return between this point and the function's end.
	closeExitPhase := e.enterDBPhase(phaseExit)
	defer closeExitPhase()
	// Timed HERE rather than inside the turnstile, which holds no metrics. Recorded on EVERY completion,
	// not only the ones that waited: count is then completions and sum/count the mean, so the exit side
	// becoming the constraint shows up as a shift in the distribution - where the free-turn gauge, being
	// instantaneous, can miss a reservation that was saturated all window without being sampled empty.
	e.metrics.turnWait.Record(ctx, time.Since(turnWaitStart).Seconds(), e.metrics.shardAttrOpts(shardNum).rec...)

	if execErr == nil && !d.row.replaying && e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultExecuteTask, d.row.taskName)) {
		execErr = errors.New("injected fault: "+engineseams.FaultExecuteTask+" "+d.row.taskName, http.StatusInternalServerError)
	}
	recordSpanError(d.taskSpan, execErr)

	if done, err := e.outcome(ctx, d, execErr); done || err != nil {
		return err
	}
	if interruptPayload, interrupted := d.result.InterruptRequested(); interrupted {
		return e.parkInterrupt(ctx, d, interruptPayload)
	}
	if subgraphURL, subgraphInput, subgraphRequested := d.result.SubgraphRequested(); subgraphRequested {
		return e.parkSubgraph(ctx, d, subgraphURL, subgraphInput)
	}
	d.sleep = d.result.SleepRequested()
	if _, _, _, retryRequested := d.result.RetryRequested(); retryRequested {
		return e.retry(ctx, d)
	}
	if done, err := e.complete(ctx, d); done || err != nil {
		return err
	}
	return e.transition(ctx, d)
}

// recoverStep runs as processStep's deferred recovery. On an error return it resets a step that processStep left in
// a non-pending state so recovery can re-dispatch it, then re-polls. Two mutually-exclusive cases per step, selected
// by from-status:
//
//	running→pending    a lock-contention error left the step leased mid-processStep.
//	completed→pending  the step was marked `completed` but the follow-up transaction that
//	                    inserts successors / bumps cohort_arrivals / completes the flow failed
//	                    to commit. Re-dispatch replays the step (advancing=1: the task does not run
//	                    again) and re-evaluates its transitions.
//
// Both are guarded on the from-status for idempotency and retried via Transact since this
// recovery can itself race a contention storm; the reset UPDATE losing that race leaves the step
// `completed` with advancing=1, which recoverOrphanedFlows rewinds once its lease lapses.
func (e *Engine) recoverStep(ctx context.Context, d *dispatch, err error) {
	// Without a captured generation we cannot prove ownership; lease expiry handles an uncertain claim.
	if err == nil || d.row.leaseSeq <= 0 {
		return
	}
	fromStatus := ""
	if d.markedComplete {
		fromStatus = workflow.StatusCompleted
	} else if sequel.IsLockContentionError(err) {
		fromStatus = workflow.StatusRunning
	}
	if fromStatus == "" {
		return
	}
	if db, derr := e.db.Shard(d.shard); derr == nil {
		// Its own turn: every turn this dispatch took has been handed back by now (the persist pass's
		// defer runs before this one), and the reset is a database call like any other.
		ctx, doneTurn := e.dbTurn(ctx, d.shard)
		defer doneTurn()
		// Fence every reset, including a claim-read failure, on the generation this dispatch acquired.
		// A parked step is not this worker's to rewind: it is waiting on its child, and a rewind would leave
		// it pending AND parked, which nothing selects or recovers.
		resetSQL := "UPDATE dwarf_steps SET status=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='" + fromStatus + "' AND parked=? AND lease_seq=?"
		resetArgs := []any{workflow.StatusPending, d.stepID, parkedNone, d.row.leaseSeq}
		// Test checkpoint: a breakpoint here freezes a zombie between capturing its (stale) reset and
		// running it, so a test can let a peer re-claim+bump the step first and prove the lease_seq fence
		// makes the zombie's reset a zero-row no-op (never rewinding the peer's freshly-claimed step).
		e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeRecoveryReset)
		db.Transact(ctx, func(tx *sequel.Tx) error {
			// engineseams.FaultRecoveryResetErr makes this last-resort reset itself fail (a non-retryable error, as if
			// it lost to a contention storm), so the step stays `completed` and the flow strands `running`
			// with every step terminal until recoverOrphanedFlows rewinds the step.
			// Process-wide (no scope): the reset may run before the claim read the task's name.
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
	e.claims.RelinquishClaim(d.shard, d.stepID)
}

// claim claims the step and reads its row and its flow's, then gates on the flow: a closed flow reaps the step,
// and a flow in a newer payload format defers it. done means the dispatch ends here.
func (e *Engine) claim(ctx context.Context, d *dispatch) (done bool, err error) {
	db, err := e.db.Shard(d.shard)
	if err != nil {
		return true, errors.Trace(err)
	}
	d.db = db
	// Lease = the step's own time_budget_ms (added in the claim UPDATE below) + a fixed margin, so it always
	// outlasts the ExecuteTask deadline. Only the margin is bound; the per-step budget comes from the column.
	leaseMarginMs := int(e.leaseMargin.Milliseconds())

	// Claim the step and read its data in one round-trip where the driver supports RETURNING. The flow row's
	// columns come back too on Postgres, joined in: one round trip instead of two on every step. The other
	// drivers read them after the claim (below).
	//
	// row.replaying is the step's advancing flag as of the claim: its completion write landed and its transition
	// never committed, so the task has already run and is not run again. The step is REPLAYED instead - its recorded
	// output and disposition (the Goto target, the Sleep and whether it was error-routed, stored by that same
	// completion write) stand in for the task's return, and dispatch resumes at the completion write. The
	// transition is a pure function of the frozen graph, the step's state and those recorded values, so a replay
	// commits exactly what the original worker would have. row.replaySleepLeftMs is what remains of the recorded
	// Sleep: the completion write stored a deadline, so a replay keeps the original wake time instead of restarting
	// the sleep. Fractional on SQLite; negative once it passed.
	var n int64
	flowRead := true
	stepDest, flowDest := d.row.destinations()
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
			workflow.StatusRunning, leaseMarginMs, e.engineID, d.stepID, parkedNone,
		).Scan(d.row.dest[:]...)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n, flowRead = 1, false
		}
	case "sqlite":
		err = db.QueryRowContext(ctx,
			claimSQLSQLite,
			workflow.StatusRunning, leaseMarginMs, e.engineID, d.stepID, parkedNone,
		).Scan(stepDest...)
		if err == sql.ErrNoRows {
			n, err = 0, nil
		} else if err == nil {
			n = 1
		}
	case "mssql":
		err = db.QueryRowContext(ctx,
			claimSQLMSSQL,
			workflow.StatusRunning, leaseMarginMs, e.engineID, d.stepID, parkedNone,
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
		// The read is fenced on the generation this claim took, so a later claim that slipped in between
		// leaves it no row and this dispatch abandons (flowID stays 0).
		res, claimErr := db.ExecContext(ctx,
			claimSQLMySQL,
			workflow.StatusRunning, leaseMarginMs, e.engineID, d.stepID, parkedNone,
		)
		if claimErr != nil {
			err = claimErr
			break
		}
		n, _ = res.RowsAffected()
		if n == 1 {
			leaseSeq, seqErr := res.LastInsertId()
			if seqErr != nil {
				err = seqErr
				break
			}
			if leaseSeq <= 0 {
				err = errors.New("claim of step %d returned lease generation %d", d.stepID, leaseSeq)
				break
			}
			// Retain ownership even if the read fails before scanning the generation.
			d.row.leaseSeq = int(leaseSeq)
			if e.seams.Enabled() { // Enabled gates the assembled name in production
				e.seams.Checkpoint(ctx, engineseams.Join(engineseams.CheckpointClaimBeforeRead, strconv.Itoa(d.shard), strconv.Itoa(d.stepID), strconv.FormatInt(leaseSeq, 10)))
			}
			var readErr error
			if e.seams.IsFault(engineseams.FaultClaimReadContention) {
				readErr = errors.New("database is locked (injected fault: " + engineseams.FaultClaimReadContention + ")")
			} else {
				readErr = db.QueryRowContext(ctx,
					claimReadMySQL,
					d.stepID, leaseSeq,
				).Scan(stepDest...)
			}
			if readErr != nil && readErr != sql.ErrNoRows {
				err = readErr
			}
		}
	}
	// The gate's turn covered the claim CAS and nothing after it. Handing it back here rather than at the
	// host call keeps a turn worth one round trip rather than a whole phase, so the turn count means
	// concurrent round trips: everything below takes its own turn on this job's own place in line.
	d.returnGateTurn()
	if err != nil {
		return true, errors.Trace(err)
	}
	if n == 0 {
		// A lost claim is routine, not an error: the candidate cache holds hints, not ownership, so the row
		// may have been claimed by a peer or left the claimable state between selection and claim. Counted
		// because the RATE is the lease-contention signal - it is the one cost of overlapping candidate
		// selection that no other instrument sees (refill waste counts candidates dropped before dispatch,
		// not round trips spent losing the CAS).
		//
		// Gated on n==0 ALONE, not on the flowID==0 arm below: on the MySQL path a successful claim (n==1)
		// whose follow-up read finds no row at its generation (a later claim took the step in between) leaves
		// flowID at 0, and counting that as a lost claim would attribute a superseded claim to candidate
		// contention - in the one instrument whose whole purpose is attribution.
		// On PostgreSQL the claim joins the flow row, so a step whose flow row is gone matches nothing and
		// does count here; the reaper and Fork delete a flow's steps before the flow, so such a row should not
		// exist.
		e.metricStepClaimLost(ctx)
		return true, nil
	}
	if d.row.flowID == 0 {
		return true, nil
	}
	d.claimed = true

	if flowRead {
		err = readFlowRow(ctx, d, "flow_token, status, workflow_url, baggage, trace_parent, created_at, updated_at, priority, fairness_key, fairness_weight, time_budget_ms, cancel_reason, graph_hash, payload_ver", flowDest...)
		if err != nil {
			return true, errors.Trace(err)
		}
	}
	// The flow's frozen budget seeds the steps this dispatch creates and bounds the subgraph LoadGraph. The
	// fallback is pure defense; createWithGraphTx always stores a concrete value.
	if d.row.flowTimeBudgetMs <= 0 {
		d.row.flowTimeBudgetMs = int(e.taskTimeBudget().Milliseconds())
	}
	e.metricStateReadBytes(ctx, d.row.workflowURL, "state", len(d.row.stateJSON))
	e.metricStateReadBytes(ctx, d.row.workflowURL, "changes", len(d.row.priorChangesJSON))
	e.metricStateReadBytes(ctx, d.row.workflowURL, "resume_data", len(d.row.resumeDataJSON))
	e.metricStateReadBytes(ctx, d.row.workflowURL, "subgraph_result", len(d.row.subgraphResultJSON))

	if workflow.IsTerminalStatus(d.row.flowStatus) {
		reapPass := turnstile.WaitTurn(ctx)
		_, err = db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=?",
			d.row.flowStatus, parkedNone, d.stepID,
		)
		reapPass.Return()
		return true, errors.Trace(err)
	}

	// A flow a newer build created is in a format this replica does not read, and so is every row of it. Defer the
	// step, before anything decodes it, for a replica that can. Past this check, every row the step reaches - its
	// own, its anchors, its cohort, the flow's tails - is readable here.
	if d.row.flowPayloadVer > payloadVer {
		return true, errors.Trace(e.deferNewerPayload(ctx, d))
	}
	return false, nil
}

// readFlowRow reads columns of the dispatch's flow row, on a turn of its own.
func readFlowRow(ctx context.Context, d *dispatch, cols string, dest ...any) error {
	flowPass := turnstile.WaitTurn(ctx)
	defer flowPass.Return()
	return d.db.QueryRowContext(ctx, "SELECT "+cols+" FROM dwarf_flows WHERE flow_id=?", d.row.flowID).Scan(dest...)
}

// failEntry fails the step from the entry phase, on a turn of its own: the persist turn that covers every LATER
// failStep does not exist yet, and failStep runs a whole transaction. Without it the two failure paths would
// disagree about whether failing a step is ordered work.
func (e *Engine) failEntry(ctx context.Context, d *dispatch, taskErr error) error {
	fctx, doneTurn := e.dbTurn(ctx, d.shard)
	err := e.failAndReturn(fctx, d, taskErr)
	doneTurn()
	return errors.Trace(err)
}

// load finds the step's graph, resolves its input and builds the carrier the task runs on. A graph, a state or a
// side column this replica cannot read fails the step. done means the dispatch ends here.
func (e *Engine) load(ctx context.Context, d *dispatch) (done bool, err error) {
	// The graph is frozen at flow creation, so a cached parse makes its JSON - the largest payload on the flow
	// row - dead weight. A flow this replica has not dispatched finds its parse by the graph_hash the flow row
	// carries, so the graph column is read only for a graph this replica has not parsed at all.
	graphKey := graphcache.Key{Shard: d.shard, FlowID: d.row.flowID, Token: d.row.flowToken}
	cg, perFlow := e.graphs.Flow(graphKey)
	cached := perFlow
	if !cached {
		cg, cached = e.graphs.Shared(d.row.workflowURL, d.row.graphHash)
	}
	if !cached {
		var graphJSON []byte
		err = readFlowRow(ctx, d, "graph", &graphJSON)
		if err != nil {
			return true, errors.Trace(err)
		}
		cg, err = e.internGraph(graphJSON, d.row.workflowURL, d.row.graphHash)
		if err != nil {
			return true, e.failEntry(ctx, d, err)
		}
	}
	if !perFlow {
		e.graphs.Point(graphKey, cg)
	}
	d.graph, d.fanIn = cg.Graph, cg.FanIn
	graph := d.graph

	// Build the Flow carrier. A State built from JSON holds each field as JSON and decodes it only if
	// something reads it - a step's state is a full input snapshot, so most fields on most steps are carried
	// rather than read, and decoding them would cost several times their byte length for the whole duration
	// of the host call.
	d.state, err = workflow.NewState(d.row.stateJSON)
	// Materialize any field this step carries BY REFERENCE (see staterefs.go). Resolving here, once, is what
	// keeps the ref encoding out of everything downstream: the carrier, `when` evaluation, forEach expansion,
	// the transport to a remote task, and the transition machinery all work on literals and never learn refs
	// exist. The refs the step inherited are kept, because minting the successors' state needs to know which
	// fields arrived as refs (and so must be carried, never re-anchored against this step). resolveStateRefs
	// mutates the state map in place, so it gets the live backing map.
	//
	// A state or refs column this replica cannot read fails the step, like a ref it cannot resolve: running the
	// task on empty input, or without the fields the refs point at, would lose them silently.
	if err == nil {
		d.inheritedRefs, err = staterefs.Parse(d.row.stateRefsJSON)
	}
	if err == nil {
		// No turn here: the resolve takes one only around an anchor read, so a step with no refs, a cache hit
		// and a wait on another dispatch's read touch no connection and queue for nothing.
		d.refBytes, err = e.resolveDispatchRefs(ctx, d.db, d.shard, d.row.flowToken, d.state, d.inheritedRefs, d.row.workflowURL)
	}
	// Unreadable prior changes fail the step too: running the task without its earlier runs' writes would
	// lose them silently.
	if err == nil {
		d.priorChanges, err = changesOf(d.row.priorChangesJSON)
	}
	if err != nil {
		// Its own turn, for the reason above: still the entry phase, so nothing covers this transaction.
		return true, e.failEntry(ctx, d, err)
	}
	// The carrier's view is the step's input with the changes of its earlier runs (a Retry, a resumed
	// Interrupt, a Subgraph re-entry) folded in through the reducers, and its changes are those runs' delta: a
	// step is one unit of work however many times its task runs, so this run's writes compose onto theirs.
	// Clone so `state` stays the pristine resolved input the successors are folded from below.
	d.reducers = graph.Reducers()
	mergedInputState := d.state.Clone()
	if err := mergedInputState.Apply(d.priorChanges, d.reducers); err != nil {
		return true, e.failEntry(ctx, d, err)
	}
	flow := workflow.NewRawFlow()
	d.flow = flow
	flow.SetRawState(mergedInputState)
	flow.SetRawChanges(d.priorChanges)
	// The graph's reducers travel with the carrier, so a task, local or remote, knows how each write combines.
	// Inside a fan-out branch a task may not overwrite a reduced field; the carrier refuses it at the call.
	flow.SetReducers(d.reducers)
	flow.SetInBranch(d.row.lineageID != 0)
	flow.SetAttempt(d.row.attempt)
	flow.SetCreatedAt(d.row.flowCreatedAt)
	flow.SetUpdatedAt(d.row.flowUpdatedAt)
	flow.SetStepCreatedAt(d.row.stepCreatedAt)
	// The task's own identity, so it can correlate logs/traces or call back into the engine
	// (e.g. History/Step) with its own keys. flow_token is loaded with the flow row, step_token
	// alongside the claim - both available here.
	flow.SetFlowKey(keys.NewFlowKey(d.shard, d.row.createdHour, d.row.flowID, d.row.flowToken))
	flow.SetStepKey(keys.NewStepKey(d.shard, d.row.createdHour, d.row.flowID, d.stepID, d.row.stepToken))

	// A side column this replica cannot read fails the step, as an unreadable state does: the task would
	// otherwise run on an empty resume payload, child result or baggage, and nothing would say so. A "null"
	// baggage column (from a nil FlowOptions.Baggage) and an empty one both decode to an empty State,
	// delivered as "no baggage".
	var sideErr error
	if d.row.interruptDone {
		resumeData, derr := workflow.NewState(d.row.resumeDataJSON)
		if derr != nil {
			sideErr = errors.New("unreadable resume data: %v", derr)
		}
		flow.SetInterruptResolution(resumeData)
	}
	if d.row.subgraphDone && sideErr == nil {
		subgraphResult, derr := workflow.NewState(d.row.subgraphResultJSON)
		if derr != nil {
			sideErr = errors.New("unreadable subgraph result: %v", derr)
		}
		flow.SetSubgraphResolution(subgraphResult, subgraphErrorText(d.row.subgraphErrorJSON))
	}
	var derr error
	d.baggage, derr = workflow.NewState(d.row.baggageJSON)
	if derr != nil && sideErr == nil {
		sideErr = errors.New("unreadable baggage: %v", derr)
	}
	if sideErr != nil {
		// Its own turn: still the entry phase, as for an unreadable state above.
		return true, e.failEntry(ctx, d, sideErr)
	}

	// A replay's carrier already holds the task's output: the claim read the step's changes as priorChanges, so
	// they are the carrier's changes and are merged into its state exactly as the task left them. Only the control
	// signals were not, and the completion write recorded them.
	if d.row.replaying {
		flow.Goto(d.row.replayGoto)
		flow.Sleep(time.Duration(max(d.row.replaySleepLeftMs, 0)) * time.Millisecond)
	}
	return false, nil
}

// startTask opens the step's span and bounds the host call by the step's budget. processStep defers the span's
// End and the budget's cancel, so both last until the dispatch returns.
func (e *Engine) startTask(ctx context.Context, d *dispatch) {
	// The Debug lines on the per-step path are guarded because their arguments are boxed into interfaces, and
	// so allocated, before the logger can decline them.
	if !d.row.replaying && e.logger.Enabled(ctx, slog.LevelDebug) {
		e.logger.DebugContext(ctx, "Executing task", "task", d.row.taskName, "workflow", d.row.workflowURL)
	}
	taskCtx := workflow.ContextWithBaggage(ctx, d.baggage)

	// Open a per-step span parented to the flow's root "workflow" span (reconstructed from the stored
	// trace_parent), and place it on the executor's context so the task's downstream spans nest under it.
	// The span is named by the task; no-op unless a TracerProvider is configured. workflow.id carries the
	// token-free correlation id, never the flowKey: a trace backend is typically readable far more broadly
	// than the workflow data, and the key is a bearer write-capability.
	taskCtx = injectTraceParent(taskCtx, d.row.traceParent)
	taskCtx, d.taskSpan = e.tracer.Start(taskCtx, d.row.taskName,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("workflow.id", keys.CorrelationID(d.shard, d.row.flowID)),
			attribute.String("workflow.name", d.row.workflowURL),
			attribute.Bool("workflow.replay", d.row.replaying),
		),
	)
	// The step's time_budget_ms bounds the executor call's context deadline; the surrounding DB work keeps using
	// the undeadlined ctx so persistence is never cut short.
	if d.row.timeBudgetMs > 0 {
		taskCtx, d.cancelTask = context.WithTimeout(taskCtx, time.Duration(d.row.timeBudgetMs)*time.Millisecond)
	}
	d.taskCtx = taskCtx
}

// execute runs the task on the host, or stands in for it: a replay runs nothing, and a step Cancel marked before it
// started is preempted with the cancellation. It returns the task's error, how long the host call took, and an
// error of its own only for a failed cancel-reason read.
func (e *Engine) execute(ctx context.Context, d *dispatch) (execErr error, hostCall time.Duration, err error) {
	dispatchURL := dispatchURLOf(d.graph, d.row.taskName)
	// Belt and braces on the gate's turn, which was handed back at the claim CAS: this is the last line
	// before the worker stops competing for a connection at all, so an early return between the two that
	// somehow skipped it is caught here. The crew wraps the release in sync.OnceFunc, so the repeat costs
	// nothing.
	d.returnGateTurn()
	d.closeEntryPhase()
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
	heldBytes := int64(len(d.row.stateJSON) + len(d.row.priorChangesJSON) + d.refBytes)
	e.inFlightStateBytes.Add(heldBytes)
	e.inFlightStateSteps.Add(1)
	hostCallStart := time.Now()
	// A step marked by Cancel before it started is preempted: the task is not run, and the cancellation
	// takes the path a task error would. The one exemption is a subgraph caller resuming after its child
	// returned - it runs once more so the child's result reaches its code, and its own mark is then applied
	// at the completion write. An interrupted step resumed after Cancel is NOT exempt: its resume
	// data is new input arriving after the cancellation, and acting on it would keep a cancelled flow alive.
	//
	// A replay runs neither the task nor the preempt: the task already ran to completion, so a mark that landed
	// since is applied at the completion write, as for a step that was running when Cancel landed.
	if d.row.replaying {
		// No host call.
	} else if d.row.stepCancelling && !d.row.subgraphDone {
		// Re-read the reason rather than trust the claim's: on PostgreSQL the claim returns the flow row as of
		// the claim statement's snapshot, which can predate a Cancel whose reason commits before its mark
		// reaches this step - the claim's re-check then sees the mark, but the reason it returned is empty.
		// Cancel writes the reason before the mark, so a read after the claim finds it. A rare path.
		reasonPass := turnstile.WaitTurn(ctx)
		var rerr error
		d.row.flowCancelReason, rerr = readCancelReason(ctx, d.db, d.row.flowID)
		if e.seams.IsFault(engineseams.FaultCancelReasonReadErr) {
			rerr = errors.New("injected fault: " + engineseams.FaultCancelReasonReadErr)
		}
		reasonPass.Return()
		if rerr != nil {
			e.inFlightStateBytes.Add(-heldBytes)
			e.inFlightStateSteps.Add(-1)
			return nil, 0, errors.Trace(rerr)
		}
		execErr = cancelmarker.New(d.row.flowCancelReason)
	} else {
		// A panic in the in-process host is caught here so it flows through the normal error disposition
		// rather than wedging this leased step until lease expiry.
		execErr = errors.CatchPanic(func() error {
			// engineseams.FaultPanicExecuteTask panics inside the wrapper so it exercises the host-call panic isolation
			// (caught here, routed as a normal task error), scoped to this task name.
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultPanicExecuteTask, d.row.taskName)) {
				panic("injected fault: " + engineseams.FaultPanicExecuteTask + " " + d.row.taskName)
			}
			return e.host.ExecuteTask(d.taskCtx, dispatchURL, &d.flow.Flow)
		})
	}
	e.inFlightStateBytes.Add(-heldBytes)
	e.inFlightStateSteps.Add(-1)
	return execErr, time.Since(hostCallStart), nil
}

// outcome turns the task's return into the step's outcome: an error is routed to the task's onError handler or
// fails the step, and the changes are checked, recorded as additions where they extend a list, sized and folded
// into the successors' base. A step that armed competing control signals, or a second park, fails. done means
// the dispatch ends here.
func (e *Engine) outcome(ctx context.Context, d *dispatch, execErr error) (done bool, err error) {
	taskName := d.row.taskName
	if execErr != nil {
		// The engine never inspects status codes or error text: a task that wants to back off (rate limit,
		// transient unavailability) reads its own signal and arms flow.Retry. Any error that reaches here is
		// terminal for this attempt - routed via the graph's onError transition if one exists, else it fails
		// the step.
		if tr, ok := d.graph.ErrorTransition(taskName); ok {
			e.logger.DebugContext(ctx, "Task error routed", "task", taskName, "workflow", d.row.workflowURL, "error", execErr)
			// A FRESH flow, deliberately: an error voids the task's changes. Whatever it wrote before
			// returning the error is dropped rather than carried to the handler - execution is at-least-once,
			// so a lease-lost re-run recomputes those changes and a half-written attempt is not a fact
			// anything can be built on. failStep does the same (it writes status/error only), so the contract
			// does not turn on whether the author declared a handler. Do not "restore" rawChanges here.
			// Its state is the carrier's input, rebuilt here rather than kept on the dispatch: kept, the clone would
			// escape on every step for the few that are error-routed. The fold succeeded in load on the same values.
			mergedInputState := d.state.Clone()
			_ = mergedInputState.Apply(d.priorChanges, d.reducers)
			d.result = workflow.NewRawFlow()
			d.result.SetRawState(mergedInputState)
			d.result.SetRawChanges(d.priorChanges)
			d.result.SetReducers(d.reducers)
			d.result.Set("onErr", onErrValue(execErr))
			d.errorRouted = true
			d.errorTarget = tr.To
		} else {
			return true, errors.Trace(e.failAndReturn(ctx, d, execErr))
		}
	} else {
		// Re-read the flow's changes after execution: the task executor wrote to the Flow
		// directly (it receives *workflow.Flow, which is the embedded Flow inside our RawFlow).
		d.result = d.flow
		// An error-routed step's recorded changes already carry its onErr; only the route is re-derived. The
		// graph is frozen on the flow, so the handler is the one the original worker routed to.
		if d.row.replaying && d.row.replayOnError {
			if tr, ok := d.graph.ErrorTransition(taskName); ok {
				d.errorRouted = true
				d.errorTarget = tr.To
			}
		}
	}

	// Accumulate changes
	// The carrier was seeded with the earlier runs' changes, so what it returns is the step's whole delta. It is
	// the task's output, so it is checked rather than trusted.
	d.changes = d.result.RawChanges()
	if err := checkChanges(d.changes, d.reducers, d.row.lineageID != 0); err != nil {
		return true, errors.Trace(e.failAndReturn(ctx, d, err))
	}
	dropIdentityNulls(d.changes, d.reducers)
	// A field with no reducer whose new value only extends the array it held is stored as what it gained, so a
	// task that rewrites a growing list each step stores each element once. Only outside a cohort: an addition is
	// relative to the step's own input, and the fan-in folds a member's changes onto the spawn's instead, where a
	// replace must win outright. The common case is a byte compare; decoding both arrays, when that misses, runs
	// with the turn handed back.
	if d.row.lineageID == 0 {
		recordAdditions(d.changes, d.state, d.reducers, func(decode func()) { yieldTurn(ctx, &d.pass, decode) })
	}
	d.changesJSON, _ = json.Marshal(d.changes)
	// Every outcome write below carries these changes (completion, retry, interrupt, subgraph park). A value the
	// database cannot store fails the step now, before hundreds of megabytes go out to be refused, and without the
	// write retries: the task has run, and running it again would produce the same value.
	if err := e.checkPayloadSize("changes of task '"+stripProto(taskName)+"'", d.changesJSON); err != nil {
		return true, errors.Trace(e.failAndReturn(ctx, d, err))
	}
	// What the successors are handed, and what routing reads: the input with the step's delta folded in. A value
	// the reducer cannot combine fails the step here, naming the field, before anything is written.
	d.successorBase = d.state.Clone()
	if err := d.successorBase.Apply(d.changes, d.reducers); err != nil {
		return true, errors.Trace(e.failAndReturn(ctx, d, err))
	}

	// Competing signals and a second park, like an invalid name in the changes above and a negative retry delay, are
	// the task breaking the carrier's contract - its own bug, which a retry repeats and a handler cannot repair - so
	// they fail the step outright rather than take onError. A failure outside the task's code that it can act on,
	// such as a subgraph that cannot be spawned, is delivered to the task instead (resolveFailedSpawn).
	signalCount := 0
	if _, interrupted := d.result.InterruptRequested(); interrupted {
		signalCount++
	}
	if _, _, _, retryRequested := d.result.RetryRequested(); retryRequested {
		signalCount++
	}
	if d.result.GotoRequested() != "" {
		signalCount++
	}
	if _, _, ok := d.result.SubgraphRequested(); ok {
		signalCount++
	}
	if signalCount > 1 {
		err = errors.New("task '%s' set multiple competing control signals", taskName)
		return true, errors.Trace(e.failAndReturn(ctx, d, err))
	}

	// Single-park guard
	_, interruptArmed := d.result.InterruptRequested()
	_, _, subgraphArmed := d.result.SubgraphRequested()
	if (interruptArmed || subgraphArmed) && (d.row.interruptDone || d.row.subgraphDone) {
		err = errors.New("task '%s' armed a second park on an already-resolved step", taskName)
		return true, errors.Trace(e.failAndReturn(ctx, d, err))
	}
	return false, nil
}

// parkInterrupt records the step's interrupt and parks the flow for external input.
func (e *Engine) parkInterrupt(ctx context.Context, d *dispatch, interruptPayload workflow.State) error {
	e.logger.DebugContext(ctx, "Task interrupted", "task", d.row.taskName, "workflow", d.row.workflowURL)
	landed, err := e.settlePersist(ctx, d, e.persistYielding(ctx, d, func() error {
		return e.handleInterrupt(ctx, d, interruptPayload)
	}), heldRunning)
	if landed {
		e.metricStepExecuted(ctx, d.row.taskName, workflow.StatusInterrupted, d.shard)
	}
	return err
}

// parkSubgraph spawns the subgraph the step requested and parks the step on it.
func (e *Engine) parkSubgraph(ctx context.Context, d *dispatch, subgraphURL string, subgraphInput workflow.State) error {
	taskName := d.row.taskName
	e.logger.DebugContext(ctx, "Task requested subgraph", "task", taskName, "workflow", d.row.workflowURL, "subgraph", subgraphURL)
	// The child's input is the caller's state handed across, checked as Create checks its caller's. A bad name is
	// delivered to the task as the subgraph's error, like a child that cannot be spawned.
	if err := checkFieldNames(subgraphInput); err != nil {
		return errors.Trace(e.resolveFailedSpawn(ctx, d, subgraphURL, err))
	}
	// Refused as Create refuses it. flow.Subgraph does already, but a carrier from a host that is not this
	// package's code arms the URL without it.
	if strings.TrimSpace(subgraphURL) != subgraphURL {
		return errors.Trace(e.resolveFailedSpawn(ctx, d, subgraphURL, errors.New("cannot start subgraph: workflow URL must not begin or end with whitespace", http.StatusBadRequest)))
	}
	// Bound the subgraph LoadGraph by the caller flow's budget (the create-time LoadGraph uses the caller's ctx).
	loadCtx, loadCancel := context.WithTimeout(workflow.ContextWithBaggage(ctx, d.baggage), time.Duration(d.row.flowTimeBudgetMs)*time.Millisecond)
	var subgraphGraph *workflow.Graph
	lerr := errors.CatchPanic(func() error {
		var e2 error
		// The host call holds no connection and is unbounded, so the turn goes back for its duration -
		// otherwise one slow LoadGraph per worker parks the shard's turns on something that is not the
		// database at all - and, like ExecuteTask's, its duration does not age the claim.
		yieldTurnAway(&ctx, &d.pass, func() {
			subgraphGraph, e2 = e.host.LoadGraph(loadCtx, subgraphURL)
		})
		return e2
	})
	if lerr == nil && e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultLoadGraph, subgraphURL)) {
		lerr = errors.New("injected fault: "+engineseams.FaultLoadGraph+" "+subgraphURL, http.StatusInternalServerError)
	}
	loadCancel()
	// A child that cannot be spawned is delivered to the caller as its subgraph's result, the way a failed child
	// is: LoadGraph's own error (a panic caught as one), a missing graph as a 404 and a structurally invalid one
	// as a 400, the same create-time guarantees Create gives. Validation is pure; the child's fan-in map is
	// derived once per graph per replica (internal/faninmap), not frozen into its JSON.
	if lerr != nil {
		return errors.Trace(e.resolveFailedSpawn(ctx, d, subgraphURL, lerr))
	}
	if subgraphGraph == nil {
		return errors.Trace(e.resolveFailedSpawn(ctx, d, subgraphURL, errors.New("subgraph graph not found: %s", subgraphURL, http.StatusNotFound)))
	}
	if verr := subgraphGraph.Validate(); verr != nil {
		return errors.Trace(e.resolveFailedSpawn(ctx, d, subgraphURL, errors.New("invalid subgraph graph %s: %v", subgraphURL, verr, http.StatusBadRequest)))
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
			d.changesJSON, parkedSubgraph, d.stepID, parkedNone, d.row.leaseSeq,
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
	callerTraceParent := extractTraceParent(d.taskCtx)
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeSubgraphSpawn)
	// createSubgraphFlow inserts the child already surgraph-linked and running, so no separate start. The
	// spawn is one transaction, so persist may re-run it whole.
	callerGone := false
	landed, err := e.settlePersist(ctx, d, e.persistYielding(ctx, d, func() error {
		_, err := e.createSubgraphFlow(ctx, d.shard, d.row.flowID, d.row.stepDepth, d.stepID, subgraphURL, subgraphGraph, childInputState, d.row.baggageJSON, callerTraceParent, parkCaller)
		callerGone = errors.Is(err, errCallerGone)
		if callerGone {
			return nil
		}
		return err
	}), heldRunning)
	if !landed {
		return err
	}
	if callerGone {
		e.seams.Checkpoint(ctx, engineseams.CheckpointSubgraphSpawnAbandoned)
		return nil
	}
	e.metricStateWriteBytes(ctx, d.row.workflowURL, "changes", len(d.changesJSON))
	e.metricStepExecuted(ctx, taskName, "subgraph", d.shard)
	return nil
}

// resolveFailedSpawn delivers a subgraph spawn that failed before any child existed as the subgraph's result: the
// step is resolved and re-pended with the task's changes and the error in subgraph_error, so the re-dispatch's
// flow.Subgraph returns it and the task decides - return it (onError included) or flow.Retry, which clears the
// slot and spawns again. Failing the step here instead would skip both, though a spawn failure is the same failure
// to the author as a failed child.
//
// The write is fenced on the lease and guarded to running and unparked, the state the task's outcome finds the
// step in; a zero-row match (a Terminate, or a peer's re-claim) is a benign no-op. It moves the status, so
// RowsAffected stays honest on MySQL.
func (e *Engine) resolveFailedSpawn(ctx context.Context, d *dispatch, subgraphURL string, spawnErr error) error {
	taskName := d.row.taskName
	e.logger.DebugContext(ctx, "Subgraph spawn failed", "task", taskName, "workflow", d.row.workflowURL, "subgraph", subgraphURL, "error", spawnErr)
	encoded, err := e.encodeSubgraphError(spawnErr)
	if err != nil {
		return errors.Trace(e.failAndReturn(ctx, d, err))
	}
	// Test checkpoint: freezes the worker before the write, so a test can Terminate the flow in the window the
	// status='running' guard protects.
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeSpawnResolve)
	var resolved bool
	landed, err := e.settlePersist(ctx, d, e.persistYielding(ctx, d, func() error {
		// Not a transaction, so it retries its own contention: one escaping deadlock would rewind the still-running
		// step and run the task again.
		return retryContention(ctx, func() error {
			resolved = false
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultSpawnResolveContention, taskName)) {
				return errors.New("database is locked (injected fault: " + engineseams.FaultSpawnResolveContention + " " + taskName + ")")
			}
			res, err := d.db.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, changes=?, subgraph_done=1, subgraph_result=?, subgraph_error=?, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND parked=? AND lease_seq=?",
				workflow.StatusPending, d.changesJSON, emptyJSON, encoded, d.stepID, parkedNone, d.row.leaseSeq,
			)
			if err != nil {
				return errors.Trace(err)
			}
			n, _ := res.RowsAffected()
			resolved = n > 0
			return nil
		})
	}), heldRunning)
	if !landed || !resolved {
		return err
	}
	// Re-pended in place for re-dispatch; drop this replica's reservation so the offer below is not skipped by its
	// own still-live entry.
	e.claims.RelinquishClaim(d.shard, d.stepID)
	e.metricStateWriteBytes(ctx, d.row.workflowURL, "changes", len(d.changesJSON))
	e.metricStepExecuted(ctx, taskName, "subgraph", d.shard)
	e.enqueueStepDue(ctx, d.shard, d.stepID, d.row.flowPriority)
	return nil
}

// retry rewinds the step in place for the retry the task requested, reaping the subgraph children of its earlier
// attempts.
func (e *Engine) retry(ctx context.Context, d *dispatch) error {
	taskName := d.row.taskName
	// Sleep is the floor and the backoff adds on top: total = Sleep + min(backoff, maxDelay). This lets a
	// task set a precise wait (e.g. a downstream's Retry-After via Sleep) and still get exponential backoff
	// on repeated attempts. maxDelay caps the backoff component, not the total.
	retrySleepMs := d.sleep.Milliseconds() + d.result.RetryDelay(d.row.attempt).Milliseconds()
	if retrySleepMs < 0 {
		// Retry refuses a negative initialDelay, but a remote host's carrier is not bound by it, and a past
		// not_before would re-dispatch the step at once on every attempt.
		err := errors.New("task '%s' requested a retry with a negative delay", stripProto(taskName))
		return errors.Trace(e.failAndReturn(ctx, d, err))
	}
	e.logger.DebugContext(ctx, "Task retried", "task", taskName, "workflow", d.row.workflowURL, "step", d.stepID, "attempt", d.row.attempt)
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
	// delete the now-closed tree's subgraph children. So rewind first under the guard and reap (and
	// re-dispatch) only when it actually rewound a still-running step; a lost guard (n==0) leaves the step
	// terminal - Terminate already terminated its children - and returns without reaping or re-dispatching.
	// Test checkpoint: a breakpoint here freezes the worker before the rewind, so a test can Terminate the
	// flow (terminalizing this running step) in exactly the window the status='running' rewind guard protects.
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeRetryRewind)
	var rewound bool
	landed, err := e.settlePersist(ctx, d, e.persistYielding(ctx, d, func() error {
		rewound = false
		if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultRetryRewindErr, taskName)) {
			return errors.New("injected fault: " + engineseams.FaultRetryRewindErr + " " + taskName)
		}
		return d.db.Transact(ctx, func(tx *sequel.Tx) error {
			rewound = false
			res, execErr := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, changes=?, attempt=?, not_before=DATE_ADD_MILLIS(NOW_UTC(), ?), lease_expires=NOW_UTC(), updated_at=NOW_UTC(), interrupt_done=0, resume_data=?, subgraph_done=0, subgraph_result=?, subgraph_error=?, parked_child_flow_id=0 WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND lease_seq=?",
				workflow.StatusPending, d.changesJSON, d.row.attempt+1, retrySleepMs, emptyJSON, emptyJSON, emptyJSON, d.stepID, d.row.leaseSeq,
			)
			if execErr != nil {
				return errors.Trace(execErr)
			}
			if n, _ := res.RowsAffected(); n == 0 {
				return nil
			}
			rewound = true
			return errors.Trace(e.deleteSubgraphFlowsRootedAt(ctx, tx, d.stepID))
		})
	}), heldRunning)
	if !landed || !rewound {
		return err
	}
	// The step was rewound in place for re-dispatch; drop this replica's reservation so the immediate
	// re-offer below (enqueueStepDue on a zero-backoff retry) is not skipped by its own still-live entry.
	e.claims.RelinquishClaim(d.shard, d.stepID)
	e.metricStateWriteBytes(ctx, d.row.workflowURL, "changes", len(d.changesJSON))
	if retrySleepMs == 0 {
		// The step was reset in place (same row, same denormalized priority) due now. A BACKED-OFF
		// retry needs nothing here: its not_before is in the future, so the piston's scan picks it up
		// within a cycle of coming due, and offering it now would only cache a hint no worker may claim.
		e.enqueueStepDue(ctx, d.shard, d.stepID, d.row.flowPriority)
	}
	e.metricStepExecuted(ctx, taskName, "retried", d.shard)
	return nil
}

// complete writes the step's completion, reading back its Cancel mark, and reroutes a step Cancel marked while it
// ran to its handler. done means the dispatch ends here.
func (e *Engine) complete(ctx context.Context, d *dispatch) (done bool, err error) {
	taskName := d.row.taskName
	// The completion write records the Goto target in advance_goto, a column as wide as task_name. A longer target
	// names no task, so it fails here with the error the transition would have given it, rather than as a column
	// overflow on the completion write (retried, then classified as a payload fault).
	if gotoTarget := d.result.GotoRequested(); len(gotoTarget) > maxGotoTargetLen {
		err = errors.New("task '%s' requested goto to '%s' but no goto transition exists from this task", stripProto(taskName), stripProto(gotoTarget))
		return true, errors.Trace(e.failAndReturn(ctx, d, err))
	}

	// Complete the step.
	//
	// engineseams.FaultLeaseStaleWrite makes this completion write carry a stale lease generation, exactly as a zombie
	// worker (whose lease was re-granted to a peer) would. The fence must reject it (zero rows -> benign
	// no-op below), so the step stays claimable and lease recovery re-runs it cleanly - the test proves a
	// late/slow worker's write can never corrupt or terminalize a flow a peer is healthily re-executing.
	writeSeq := d.row.leaseSeq
	if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultLeaseStaleWrite, taskName)) {
		writeSeq = d.row.leaseSeq - 1
	}
	// THE task has already run - its side effects have fired - so this write is the only record that it did.
	// A database error here must therefore retry the WRITE, never the task (see persist): an ephemeral blip is
	// absorbed with zero re-execution, and a write that will never land is classified and terminalized rather
	// than left to lease recovery, which would re-execute the task every `budget + leaseMargin`, forever.
	//
	// The write also reads back the step's Cancel mark - the one post-dispatch cancellation check. It rides a
	// write every completion makes anyway, via RETURNING/OUTPUT, so on those dialects an uncancelled flow pays
	// nothing for it. MySQL lacks both and reads the mark with a follow-up SELECT: one extra round trip per
	// completed step, accepted as it is at the claim, rather than returning it through LAST_INSERT_ID(expr). The
	// claim uses that idiom because its read would otherwise be racy; here the follow-up read is already race-free
	// (below), so the idiom would only save a round trip on a dialect that is not the recommended one.
	// That SELECT is race-free only because the row is already terminal when it runs - Cancel never marks a
	// terminal row, so the value it reads is the one this UPDATE saw. The status always moves running ->
	// completed, which keeps RowsAffected an honest fence on MySQL (it counts CHANGED rows): a covered step is
	// settled by a write of its own below, never by a variant of this one that could leave the row unchanged.
	//
	// advancing=1 marks the step completed but not yet transitioned, and the advance_* columns record what a replay
	// of the transition needs (the Goto target, the Sleep, whether it was error-routed), so a worker that dies before
	// the transition commits leaves a step replayed without re-running the task (row.replaying in claim). The
	// transition clears the flag in a statement that already writes this row.
	const advanceSet = "advancing=1, advance_goto=?, advance_sleep_until=DATE_ADD_MILLIS(NOW_UTC(), ?), advance_on_error=?"
	advanceGoto, advanceSleepMs, advanceOnError := d.result.GotoRequested(), d.sleep.Milliseconds(), 0
	if d.errorRouted {
		advanceOnError = 1
	}
	db := d.db
	var stepRowsAffected int64
	err = e.persistYielding(ctx, d, func() error {
		// Not a transaction, so it retries its own contention (see retryContention). On MySQL a retry after the
		// follow-up SELECT lost re-runs a landed UPDATE: the row still matches and is rewritten with the same values.
		return retryContention(ctx, func() error {
			stepRowsAffected, d.completedCancelling = 0, false
			// engineseams.FaultCompletionContention makes this write lose a lock-contention race, consumed per
			// attempt - so InjectN(1) is a deadlock the retry must absorb without re-running the task.
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultCompletionContention, taskName)) {
				return errors.New("database is locked (injected fault: " + engineseams.FaultCompletionContention + " " + taskName + ")")
			}
			// engineseams.FaultPersistErr makes this write fail with a synthetic NON-contention error, consumed per attempt -
			// so InjectN(1) is a transient blip the retry must absorb with NO re-execution, and InjectN(large) is a
			// permanent failure the classifier must terminalize rather than loop on.
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultPersistErr, taskName)) {
				return errors.New("injected fault: " + engineseams.FaultPersistErr + " " + taskName)
			}
			if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultPersistConnErr, taskName)) {
				return errors.Join(errors.New("injected fault: "+engineseams.FaultPersistConnErr+" "+taskName), driver.ErrBadConn)
			}
			const where = " WHERE step_id=? AND status NOT IN ('" + workflow.StatusTerminated + "', '" + workflow.StatusCancelled + "') AND lease_seq=?"
			switch db.DriverName() {
			case "pgx", "sqlite":
				werr := db.QueryRowContext(ctx,
					"UPDATE dwarf_steps SET status=?, changes=?, "+advanceSet+", updated_at=NOW_UTC()"+where+" RETURNING cancelling",
					workflow.StatusCompleted, d.changesJSON, advanceGoto, advanceSleepMs, advanceOnError, d.stepID, writeSeq,
				).Scan(&d.completedCancelling)
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
					workflow.StatusCompleted, d.changesJSON, advanceGoto, advanceSleepMs, advanceOnError, d.stepID, writeSeq,
				).Scan(&d.completedCancelling)
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
					workflow.StatusCompleted, d.changesJSON, advanceGoto, advanceSleepMs, advanceOnError, d.stepID, writeSeq,
				)
				if werr != nil {
					return errors.Trace(werr)
				}
				stepRowsAffected, _ = res.RowsAffected()
				if stepRowsAffected > 0 {
					if rerr := db.QueryRowContext(ctx, "SELECT cancelling FROM dwarf_steps WHERE step_id=?", d.stepID).Scan(&d.completedCancelling); rerr != nil {
						return errors.Trace(rerr)
					}
				}
			}
			// engineseams.FaultPersistLandedErr reports an error for a write that landed, as a lost acknowledgement
			// of a commit does, consumed per attempt.
			if stepRowsAffected > 0 && e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultPersistLandedErr, taskName)) {
				return errors.New("injected fault: " + engineseams.FaultPersistLandedErr + " " + taskName)
			}
			return nil
		})
	})
	if landed, err := e.settlePersist(ctx, d, err, heldRunning); !landed {
		return true, err
	}
	if stepRowsAffected == 0 {
		return true, nil
	}
	d.markedComplete = true
	e.metricStateWriteBytes(ctx, d.row.workflowURL, "changes", len(d.changesJSON))

	// A step Cancel marked while its task was running: the task genuinely ran, so its changes stand (the write
	// above persisted them), but what it asked for next is not honored. An errorRouted step is already on its
	// way to its handler and needs nothing more.
	if d.completedCancelling && !d.errorRouted {
		// The flow row was read at claim time, which for a step already running when Cancel landed predates
		// the reason. Cancel writes the reason before the mark, so a step that sees the mark finds the reason
		// on a fresh read; this path is rare, so the extra round trip is too.
		var rerr error
		if d.row.flowCancelReason, rerr = readCancelReason(ctx, db, d.row.flowID); rerr != nil {
			return true, errors.Trace(rerr)
		}
		tr, ok := d.graph.ErrorTransition(taskName)
		if !ok {
			// No handler: fail the step as cancelled. failStep flips it completed -> cancelled, the same
			// completed-to-terminal escape failOnPersistError uses, so the recovery defer - which rewinds only
			// a `completed` row - has nothing to rewind once it lands, and rewinds it for a re-dispatch (where
			// the claim preempts it) if it does not.
			return true, errors.Trace(e.failAndReturn(ctx, d, cancelmarker.New(d.row.flowCancelReason)))
		}
		// Route to the handler exactly as a task error would, except that the task's own changes are kept -
		// it ran to completion, so they are a fact, not a half-written attempt - and onErr is added to them.
		// The extra write puts onErr into the step's persisted changes, so the step reads afterwards the way an
		// error-routed step does; it runs only on this rare path, so the completion write above never carries
		// a second copy of the changes.
		d.changes = d.changes.Clone()
		_ = d.changes.Set("onErr", onErrValue(cancelmarker.New(d.row.flowCancelReason)), d.reducers)
		d.changesJSON, _ = json.Marshal(d.changes)
		// onErr carries the caller's cancel reason, so it can push changes that passed the check above over the
		// limit. The step fails on the size, not as cancelled: a real error wins over a cancellation.
		if err := e.checkPayloadSize("changes of task '"+stripProto(taskName)+"'", d.changesJSON); err != nil {
			return true, errors.Trace(e.failAndReturn(ctx, d, err))
		}
		d.successorBase = d.state.Clone()
		if err := d.successorBase.Apply(d.changes, d.reducers); err != nil {
			return true, errors.Trace(e.failAndReturn(ctx, d, err))
		}
		// The route is recorded with the changes, so a replay of this step routes to the handler too. It is part
		// of the step's recorded outcome, so it goes through persist like the completion write above.
		err = e.persistYielding(ctx, d, func() error {
			return retryContention(ctx, func() error {
				// engineseams.FaultRerouteWriteErr makes this write fail with a NON-contention error, consumed per
				// attempt: InjectN(1) is a blip persist absorbs in place, InjectN(large) a failure it classifies.
				if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultRerouteWriteErr, taskName)) {
					return errors.New("injected fault: " + engineseams.FaultRerouteWriteErr + " " + taskName)
				}
				// engineseams.FaultRerouteStaleWrite makes it carry a stale lease generation, as a zombie's would.
				writeSeq := d.row.leaseSeq
				if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultRerouteStaleWrite, taskName)) {
					writeSeq = d.row.leaseSeq - 1
				}
				res, uerr := db.ExecContext(ctx,
					"UPDATE dwarf_steps SET changes=?, advance_sleep_until=NOW_UTC(), advance_on_error=1 WHERE step_id=? AND lease_seq=?",
					d.changesJSON, d.stepID, writeSeq,
				)
				if uerr != nil {
					return errors.Trace(uerr)
				}
				// A zero-row match is a peer's re-claim: abandon quietly, before the route is counted or built.
				// advance_on_error moves 0 -> 1 on every row it matches, so RowsAffected is honest on MySQL.
				if n, _ := res.RowsAffected(); n == 0 {
					return errLeaseFenced
				}
				return nil
			})
		})
		if landed, err := e.settlePersist(ctx, d, err, heldRerouting); !landed {
			return true, err
		}
		d.errorRouted = true
		d.errorTarget = tr.To
		d.sleep = 0
	}
	debugLog := e.logger.Enabled(ctx, slog.LevelDebug)
	if d.row.replaying {
		// The task ran once, on the worker that recorded this completion, and dwarf_steps_executed counted it there.
		if debugLog {
			e.logger.DebugContext(ctx, "Task replayed", "task", taskName, "workflow", d.row.workflowURL)
		}
	} else if d.errorRouted {
		if debugLog {
			e.logger.DebugContext(ctx, "Task error routed", "task", taskName, "workflow", d.row.workflowURL)
		}
		e.metricStepExecuted(ctx, taskName, "error_routed", d.shard)
	} else {
		if debugLog {
			e.logger.DebugContext(ctx, "Task completed", "task", taskName, "workflow", d.row.workflowURL)
		}
		e.metricStepExecuted(ctx, taskName, workflow.StatusCompleted, d.shard)
	}
	// A successor inherits a Cancel mark only from a source that was not itself covered: a covered source has
	// already had its cancellation delivered (or overridden by its own error), and what it routes to next is
	// the author's recovery. See successorRowSQL.
	d.inheritCancel = 1
	if d.row.stepCancelling || d.completedCancelling {
		d.inheritCancel = 0
	}
	return false, nil
}

// transition routes the completed step: a fan-out source straight to its own fan-in, a step with nowhere to go to
// the flow's completion, and everything else to its successors and cohort arrivals (advance).
func (e *Engine) transition(ctx context.Context, d *dispatch) error {
	taskName, lineageID, graph := d.row.taskName, d.row.lineageID, d.graph
	// Test checkpoint: a breakpoint here freezes the worker after the step is marked completed but before
	// whichever transaction transitions it - the transition transaction, fireFanInDirect, or the flow's
	// completion - so a test can Terminate the flow in exactly the window the write-first terminal-status guard
	// exists to survive (the transition must become a no-op, inserting no successors into the terminated flow), or
	// take the step back from under a worker that has stalled there.
	e.seams.Checkpoint(ctx, engineseams.CheckpointBeforeTransitionTx)

	// Evaluate transitions
	var nextTasks []nextStep
	if d.errorRouted {
		nextTasks = []nextStep{{taskName: d.errorTarget}}
	} else {
		var err error
		nextTasks, err = evaluateTransitions(graph, taskName, d.result, d.successorBase)
		if err != nil {
			return errors.Trace(e.failAndReturn(ctx, d, err))
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
	d.isPushTransition = isFanOutSource && !d.errorRouted && d.result.GotoRequested() == ""
	d.cohortSize = len(realTasks)

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
		fanInOfSource = d.fanIn.For(taskName)
	}
	if isFanOutSource && fanInOfSource != "" &&
		((d.cohortSize == 0 && !routedToEnd) || (d.cohortSize == 1 && realTasks[0].taskName == fanInOfSource)) {
		return e.persistStepYielding(ctx, d, heldAdvancing, func() error {
			return e.fireFanInDirect(ctx, d, fanInOfSource)
		})
	}
	if isFanOutSource && d.cohortSize == 0 {
		// Either a fan-out source that routed to END, or an empty forEach with NO fan-in to converge on
		// (fanInOfSource == ""). Only a TRUNK source (lineage_id == 0) may complete the flow; a fan-out source
		// that is itself a cohort MEMBER (a nested fan-out) would terminalize the flow while its outer siblings
		// are still running. (Unreachable for a validated graph - Validate requires a fan-out source to converge
		// on a SetFanIn task, and rejects a route to END that leaves the outer frame unpopped - so this is
		// defense in depth for a graph frozen before those checks.)
		if lineageID != 0 {
			reason := "has no fan-in task to converge on"
			if routedToEnd {
				reason = "routed to END without converging on its fan-in task"
			}
			return e.failAndReturn(ctx, d, errors.New("fan-out source '%s' is inside a cohort but %s", taskName, reason))
		}
		return e.persistStepYielding(ctx, d, heldAdvancing, func() error {
			return e.completeFlowSequential(ctx, d.shard, d.row.flowID, d.stepID, d.row.leaseSeq, d.row.createdHour, d.row.flowToken, d.row.workflowURL)
		})
	}

	if d.cohortSize == 0 {
		// No matching transition. Only a TRUNK step (lineage_id == 0) completes the flow. A cohort MEMBER
		// (lineage_id != 0) that matched no onward transition has dead-ended before reaching its fan-in -
		// whether the dead end came from a `when` guard that evaluated false or a `switch`/`goto` with no
		// live target. Completing the flow here would terminalize it while sibling branches are still running
		// and skip the fan-in entirely, silently dropping every downstream task and the siblings' work. Fail
		// the branch loudly instead: every branch of a fan-out must reach a fan-in task. This mirrors the
		// fan-in-from-outside-a-cohort guard below, and a fan-in step is always trunk (its lineage_id is its
		// spawn source's OWN lineage, 0 for a top-level cohort), so a legitimate `<fan-in> -> END` still
		// completes here.
		if lineageID != 0 {
			return e.failAndReturn(ctx, d,
				errors.New("task '%s' is inside a fan-out cohort but matched no onward transition; every branch must reach a fan-in task", taskName))
		}
		return e.persistStepYielding(ctx, d, heldAdvancing, func() error {
			return e.completeFlowSequential(ctx, d.shard, d.row.flowID, d.stepID, d.row.leaseSeq, d.row.createdHour, d.row.flowToken, d.row.workflowURL)
		})
	}

	d.cohortSpawnID = lineageID
	d.childLineageID = lineageID
	if d.isPushTransition {
		d.cohortSpawnID = d.stepID
		d.childLineageID = d.stepID
	}

	for _, next := range realTasks {
		if graph.IsFanIn(next.taskName) {
			d.fanInTaskName = next.taskName
			d.fanInArrivals++
		} else {
			d.normalNexts = append(d.normalNexts, next)
		}
	}

	// A fan-in arrival must have a cohort to arrive at. cohortSpawnID is the dispatched step's lineage_id,
	// which is 0 for any step OUTSIDE a fan-out cohort - so a trunk step routed into a fan-in task would
	// bump cohort_arrivals on step_id=0 (zero rows, no error) and then SELECT that step: sql.ErrNoRows,
	// which aborts the transition transaction. The recovery defer then rewinds the just-completed step to
	// pending, it is replayed, and it fails identically. An unbounded hot loop that hammers the database and never
	// advances the flow.
	//
	// A fan-out source routing to its OWN fan-in is legal and was already handled above (fireFanInDirect) -
	// it never reaches here. What remains is a step with no cohort of its own reaching a fan-in: a trunk
	// non-source step routed into a fan-in task (validateLineage rejects it, so a graph validated today
	// cannot produce it; the guard covers a graph frozen onto a flow BEFORE that check, replayed unvalidated
	// on every dispatch). Failing the step is the honest outcome - the flow terminates with a clear error
	// instead of hot-looping forever.
	if d.fanInArrivals > 0 && d.cohortSpawnID == 0 {
		return e.failAndReturn(ctx, d,
			errors.New("task '%s' is routed to fan-in task '%s' but is not part of a fan-out cohort", taskName, d.fanInTaskName))
	}

	// The successors' base state is the input with the step's delta folded in (successorBase); a forEach branch
	// re-mints per element below. A reduced field the step folded holds a value that exists in no row - not
	// in this step's state, which predates the fold, nor its changes, which hold only the delta - so it is
	// never ref'd at this step: an append or an addition is a list of the rows holding its elements (folds), and
	// any other fold is stored inline in the successors' state.
	folded := foldedFields(d.changes, d.reducers)
	d.folds = appendFolds(d.state, d.changes, d.reducers, d.stepID)
	d.inlineFolded = withoutKeys(folded, d.folds)
	// Mint state refs for the successors: a field big enough to be worth an anchor is omitted from their
	// `state` and recorded in `state_refs` instead, pointing at THIS step (which holds the bytes in its
	// `changes` if the task just wrote them, or in its `state` if it merely carried them). A field that
	// arrived here as a ref keeps that ref. The bar scales with the fan-out width - see mintStateRefs.
	//
	// The linear/static-fan-out successors all share this one snapshot, so it is minted once; a forEach
	// branch re-mints because its state carries the injected element (below).
	var err error
	d.linearStateJSON, d.linearRefsJSON, err = e.linkerFor(d.shard).Mint(d.successorBase, d.changes, d.inheritedRefs, d.stepID, len(d.normalNexts), d.inlineFolded, nil, d.folds)
	if err == nil {
		// Refs keep a carried field out of this column, but a reduced field's folded value, and fields over the
		// anchor cap, are stored inline - so a state can outgrow the column though no single step wrote that much.
		err = e.checkPayloadSize("successor state of task '"+stripProto(taskName)+"'", d.linearStateJSON)
	}
	if err != nil {
		return errors.Trace(e.failAndReturn(ctx, d, err))
	}
	return e.advance(ctx, d)
}

// advance commits the transition - the successors, the cohort arrival, a cohort resolving as a loss - in one
// retryable transaction (transitionTx), then rings the doorbell for the first successor.
func (e *Engine) advance(ctx context.Context, d *dispatch) error {
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
	if d.fanInArrivals > 0 && d.cohortSpawnID != 0 {
		stripe := &e.cohortLocks[cohortLockStripe(d.shard, d.cohortSpawnID)]
		// Park on the stripe holding NO turn. The stripe exists precisely so a losing sibling waits without
		// occupying a connection, and a turn is that occupancy one level up - holding one here would put a
		// whole cohort's losers into the turnstile, which is the queue the stripe was built to keep them
		// out of. Yielding restores the property the stripe is for.
		held := false
		yieldTurn(ctx, &d.pass, func() {
			held = e.lockCohortStripe(ctx, stripe, d.db, d.shard, d.stepID, d.row.leaseSeq)
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
	err := e.persistYielding(ctx, d, func() error {
		err := d.db.Transact(ctx, func(tx *sequel.Tx) error {
			return e.transitionTx(ctx, d, tx)
		})
		if errors.Is(err, errTransitionFlowTerminal) {
			d.txBytes = stateByteCount{} // nothing the transaction wrote survived the rollback
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
	if landed, err := e.settlePersist(ctx, d, err, heldAdvancing); !landed {
		return err
	}
	workflowURL := d.row.workflowURL
	e.metricStateWriteBytes(ctx, workflowURL, "state", d.txBytes.stateWritten)
	e.metricStateWriteBytes(ctx, workflowURL, "state_refs", d.txBytes.refsWritten)
	e.metricStateReadBytes(ctx, workflowURL, "state", d.txBytes.stateRead)
	e.metricStateReadBytes(ctx, workflowURL, "changes", d.txBytes.changesRead)

	if d.loss.status != "" {
		e.endLostFlow(ctx, d.shard, d.row.flowID, d.row.createdHour, d.row.flowToken, flowLabel{workflowURL: workflowURL, priority: d.row.flowPriority}, d.loss)
		return nil
	}

	// The doorbell below offers the first successor to this replica, so whatever it refs at this step is served
	// from the cache on its dispatch rather than read back. Seeding from the linear mint usually covers a forEach
	// branch's array ref too (both mint at the same width); where a branch anchors what the linear mint inlined,
	// the branch's resolve simply misses.
	if len(d.normalNexts) > 0 && len(d.newStepIDs) > 0 && !e.seams.IsFault(engineseams.FaultSkipRefSeed) {
		e.linkerFor(d.shard).Seed(d.row.flowToken, d.stepID, d.successorBase, d.changes, d.linearRefsJSON)
	}

	// A sleeping successor is left to the piston: its not_before is in the future, so it is invisible to
	// selection until it comes due and then visible to the very next cycle.
	if d.sleep == 0 && len(d.newStepIDs) > 0 {
		// Priority is the flow's, just bound into the successor INSERTs; the sleep branch diverged above.
		e.enqueueStepDue(ctx, d.shard, d.newStepIDs[0], d.row.flowPriority)
	}
	return nil
}

// transitionTx is the transition's transaction: the flow-row grab, the successor INSERTs, the dispatched row's own
// fenced write and, for a fan-in arrival, the cohort bump and what it resolves (arriveAtCohort). Transact may run it
// more than once, so it starts from a clean slate each time.
func (e *Engine) transitionTx(ctx context.Context, d *dispatch, tx *sequel.Tx) error {
	d.newStepIDs = d.newStepIDs[:0]
	d.loss = flowLoss{}
	d.txBytes = stateByteCount{}
	d.flowRowLocked = false
	taskName, graph := d.row.taskName, d.graph

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

	// A NON-FINAL cohort arrival is the one disposition that neither extends nor terminalizes the flow:
	// it inserts no successor, and its entire durable effect is the cohort_arrivals bump on the SPAWN step
	// below. Grabbing the flow row for it serializes EVERY sibling of a cohort on one row - measured at
	// fan-out width 64: 20 of 43 active backends queued on this exact statement, each holding a pool
	// connection - for a lock that guards nothing it writes. So the grab is DEFERRED to
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
	pureArrival := d.fanInArrivals > 0 && len(d.normalNexts) == 0 && !d.isPushTransition

	// The transition's statements go out as statement batches - one round trip each where the driver
	// pipelines.
	batch := newTxBatch(ctx, tx)

	// A transition on a closed flow rolls back what was batched with the grab, which leaves it the same
	// clean no-op as a bail before any write.
	if !pureArrival {
		e.queueFlowGrab(ctx, d, batch, errTransitionFlowTerminal)
	}

	succ := successorBatch{
		flowID: d.row.flowID, flowHour: d.row.createdHour, stepDepth: d.row.stepDepth + 1, timeBudgetMs: d.row.flowTimeBudgetMs, lineageID: d.childLineageID,
		predecessorID: d.stepID, sleepMs: d.sleep.Milliseconds(), priority: d.row.flowPriority, fairnessKey: d.row.flowFairnessKey,
		fairnessWeight: d.row.flowFairnessWeight, engineID: e.engineID, inheritCancel: d.inheritCancel,
		maxBytes: e.maxPayloadBytes,
	}
	for i, next := range d.normalNexts {
		// fan_out_ordinal identifies the BRANCH within a cohort so the fan-in folds order-sensitive
		// reducers (append/concat/union) in input-array order. A fan-out SOURCE stamps each new branch
		// with its spawn-loop position i. A step CONTINUING an existing branch (a single linear/goto
		// successor, i is always 0 here) must instead INHERIT its own ordinal, or every second-or-later
		// step of the branch lands in the ordinal-0 bucket and folds by completion order. A trunk step's
		// ordinal is 0 and irrelevant.
		successorOrdinal := i
		if !d.isPushTransition {
			successorOrdinal = d.row.fanOutOrdinal
		}
		stepStateJSON, stepRefsJSON := d.linearStateJSON, d.linearRefsJSON
		if next.itemKey != "" {
			// A forEach branch's state is the flow state plus its element and ordinal context. Do not strip
			// the source array from it to save N copies: that is a byte optimization for the one field the
			// engine happens to know the name of, while every other carried field pays the same cost; it
			// makes a branch's state a LIE - the branch cannot see the array its own element came from -
			// and it makes a failed fan-out's final_state come back missing the array (a completed
			// sibling's branch-local snapshot is the merge base there). The general mechanism that owns the
			// cost is state refs: the array is REF'd, so the branches share one copy of it and each still
			// sees it whole.
			perStepState := d.successorBase.Clone()
			_ = perStepState.Set(next.itemKey, next.item)
			_ = perStepState.Set(next.itemKey+"Index", next.cohortIndex)
			_ = perStepState.Set(next.itemKey+"Count", next.cohortCount)
			// The ordinal context is SYNTHESIZED per branch - its bytes are in no step row at all - so it
			// can never be ref'd; a ref to it would dangle. The element is one element of the source
			// array, so Mint may point it into whichever anchor holds the array. Everything else in the
			// branch's state comes from this step and re-mints exactly as the linear case.
			noRef := map[string]bool{next.itemKey + "Index": true, next.itemKey + "Count": true}
			for k := range d.inlineFolded {
				noRef[k] = true
			}
			item := &staterefs.Item{Key: next.itemKey, Source: next.forEachKey, Index: next.cohortIndex}
			var mintErr error
			stepStateJSON, stepRefsJSON, mintErr = e.linkerFor(d.shard).Mint(perStepState, d.changes, d.inheritedRefs, d.stepID, len(d.normalNexts), noRef, item, d.folds)
			if mintErr != nil {
				return errors.Trace(mintErr)
			}
			if err := e.checkPayloadSize("successor state of task '"+stripProto(taskName)+"'", stepStateJSON); err != nil {
				return err
			}
		}
		// A wide fan-out is sent a batch at a time, so it never holds every branch's state at once.
		err := succ.add(batch, &d.newStepIDs, successorRow{
			taskName: next.taskName, taskURL: dispatchURLOf(graph, next.taskName),
			stateJSON: stepStateJSON, refsJSON: stepRefsJSON, ordinal: successorOrdinal,
		})
		if err != nil {
			return err
		}
		d.txBytes.stateWritten += len(stepStateJSON)
		d.txBytes.refsWritten += len(stepRefsJSON)
	}
	succ.queue(batch, &d.newStepIDs)

	// The own-row write takes the new step's id. successor_id is the FIRST successor,
	// which History's step navigation follows, and no last-id function names the first row of a multi-row
	// INSERT, so only a lone successor can be named in SQL.
	successor, successorArgs := "", []any(nil)
	switch {
	case len(d.normalNexts) == 1:
		var err error
		successor, successorArgs, err = batch.insertedID(lastStepIDSQL, func() any { return d.newStepIDs[0] })
		if err != nil {
			return err
		}
	case len(d.normalNexts) > 1:
		if err := batch.flush(); err != nil {
			return err
		}
		successor, successorArgs = "?", []any{d.newStepIDs[0]}
	}

	// The dispatched row's own write - its successor link, its cohort size, and the advancing clear - is ONE
	// statement fenced on this dispatch's lease generation. The fence is what makes the orphan sweep's rewind
	// safe: the rewind bumps lease_seq, so a worker that was merely slow (not dead) rolls this transaction
	// back here instead of committing a second set of successors next to the re-dispatch's. Requiring
	// advancing=1 fences one more case: persist re-running this transaction after a COMMIT that landed but
	// reported an error finds the flag already cleared, and abandons rather than advancing the flow twice. A
	// fan-in arrival clears the flag in its cohort bump below instead, so every transaction that commits while
	// the flow stays open clears it.
	if len(d.normalNexts) > 0 || d.isPushTransition {
		set, args := "", append([]any{}, successorArgs...)
		if successor != "" {
			set += "successor_id=" + successor + ", "
		}
		if d.isPushTransition {
			set += "cohort_size=?, "
			args = append(args, d.cohortSize)
		}
		args = append(args, d.stepID, d.row.leaseSeq)
		batch.queue(func(res *sequel.BatchResults) error {
			fence, err := res.Exec()
			if err != nil {
				return errors.Trace(err)
			}
			if n, _ := fence.RowsAffected(); n == 0 {
				return errLeaseFenced
			}
			if d.isPushTransition {
				e.logger.DebugContext(ctx, "Fan-out cohort spawned", "flow", keys.CorrelationID(d.shard, d.row.flowID), "spawnStep", d.stepID, "task", taskName, "cohortSize", d.cohortSize)
			}
			return nil
		}, "UPDATE dwarf_steps SET "+set+"advancing=0 WHERE step_id=? AND lease_seq=? AND advancing=1", args...)
	}
	if d.fanInArrivals > 0 {
		if err := e.arriveAtCohort(ctx, d, tx, batch, &succ, pureArrival); err != nil {
			return err
		}
	}

	// The flow row is not written here: its stopped_step_id records only the step a flow STOPPED at, written by the
	// terminal and interrupt writes. A per-step advance is a row version on the flow's hottest row every step.
	return batch.flush()
}

// queueFlowGrab queues the transition's write-first grab of the flow row's lock, before any step is written (except
// the deferred grab of a cohort arrival), guarded on non-terminal status, so a transition racing a Terminate or
// failStep inserts nothing into a closed flow. It is batched with what follows it, so its guard is read only after
// those statements ran: a closed flow matches no row, and its reader returns bail and leaves d.flowRowLocked false.
func (e *Engine) queueFlowGrab(ctx context.Context, d *dispatch, batch *txBatch, bail error) {
	if e.seams.Enabled() { // counting checkpoint; Enabled gates the assembled name in production
		e.seams.Checkpoint(ctx, engineseams.Join(engineseams.CheckpointFlowRowWrite, strconv.Itoa(d.row.flowID)))
	}
	driver := batch.tx.DriverName() // the reader captures the name, not the batch, which then stays on the stack
	batch.queue(func(res *sequel.BatchResults) error {
		n, err := lockedFlowRows(driver, res)
		if err != nil {
			return err
		}
		if n == 0 {
			return bail
		}
		d.flowRowLocked = true
		return nil
	}, lockFlowRowsSQL(driver, liveFlowWhere), d.row.flowID)
}

// arriveAtCohort bumps the cohort's arrivals and, when this arrival resolves it, inserts the fan-in step or fails the
// flow. The bump runs alone, on no open batch: whether the cohort resolved is decided from it.
func (e *Engine) arriveAtCohort(ctx context.Context, d *dispatch, tx *sequel.Tx, batch *txBatch, succ *successorBatch, pureArrival bool) error {
	if err := batch.flush(); err != nil {
		return err
	}
	// Bump the cohort counter and read the post-bump values in ONE round-trip where the driver
	// supports it (the same RETURNING/OUTPUT split as the claim CAS).
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
	// trunk-step-into-a-fan-in case, guarded in transition).
	//
	// The same statement clears this step's own advancing flag, fenced on its lease generation, so a
	// pure arrival - which writes nothing else to its own row - still clears it without a round trip.
	arrivals, size, failures, cancellations, spawnLineageID, err := bumpCohortArrivals(ctx, tx, d.cohortSpawnID, d.fanInArrivals, d.stepID, d.row.leaseSeq)
	if err != nil {
		return errors.Trace(err)
	}
	fullyResolved := size > 0 && arrivals >= size
	if !fullyResolved {
		return nil
	}
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
	if pureArrival {
		e.queueFlowGrab(ctx, d, batch, nil)
	}
	if failures == 0 {
		queueCohortRead(batch, d.row.flowID, d.cohortSpawnID, &cohort)
	}
	if err := batch.flush(); err != nil {
		return err
	}
	if !d.flowRowLocked {
		return nil
	}
	if failures == 0 {
		row, depth, exits, fanInBytes, err := e.fanInRow(ctx, tx, d.shard, d.row.flowToken, d.row.stepDepth+1, d.cohortSpawnID, &cohort, d.fanInTaskName, d.graph, d.row.workflowURL)
		if err != nil {
			return errors.Trace(err)
		}
		d.txBytes.add(fanInBytes)
		fanIn := *succ
		fanIn.stepDepth, fanIn.lineageID, fanIn.rows = depth, spawnLineageID, []successorRow{row}
		fanIn.queue(batch, &d.newStepIDs)
		// The cohort-exit -> fan-in edges take the fan-in step's id.
		fanInID, fanInArgs, err := batch.insertedID(lastStepIDSQL, func() any { return d.newStepIDs[len(d.newStepIDs)-1] })
		if err != nil {
			return err
		}
		// The exit steps are targeted by primary key, collected from the cohort read, so the write locks
		// only those rows: a (flow_id, lineage_id, task_name IN ...) predicate has no supporting index
		// and scan-locked dwarf_steps, deadlocking concurrent fan-ins on SQL Server.
		// Chunked, since a forEach has as many exits as branches; the chunks share the batch.
		for chunk := range slices.Chunk(exits, inListChunk) {
			ph, ids := inList(chunk)
			batch.queue(execOnly, "UPDATE dwarf_steps SET successor_id="+fanInID+" WHERE step_id IN ("+ph+")", append(append([]any{}, fanInArgs...), ids...)...)
		}
		return nil
	}
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
		if rerr := tx.QueryRowContext(ctx, "SELECT surgraph_step_id FROM dwarf_flows WHERE flow_id=?", d.row.flowID).Scan(&parentStepID); rerr != nil {
			return errors.Trace(rerr)
		}
		var lerr error
		d.loss, lerr = e.failFlowTx(ctx, tx, d.shard, d.row.flowID, d.row.flowToken, flowCancelled, nil, parentStepID, 0)
		if lerr != nil {
			return errors.Trace(lerr)
		}
	}
	return nil
}
