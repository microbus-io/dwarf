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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/cancelmarker"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// addRecoveryJobs adds shard's janitor the engine's recovery repairs, each a job of its own that runs its pass on the
// shard under a turn, the next pass an interval after the last ended (janitor.NewRecovery):
//
//   - lease_recovery, every recoveryInterval: recoverExpiredLeases, the running steps whose lease has expired.
//   - orphan_repair, every recoveryInterval: recoverOrphanedFlows, the completed steps whose transition never
//     committed. A job apart from lease recovery, so its scan of every open flow - the costliest pass of the three -
//     never delays a lease reset (TestRecoveryJobs_AHeldOrphanRepairDelaysNoLeaseReset).
//   - wedge_sweep, every wedgeSweepEvery recovery intervals: wedgeSweep, the park detectors and the stranded-flow alarm.
//
// Between them, the first two recover every step a dead worker held, claimed or just completed, and their interval is
// the term a crash adds to either.
//
// These are repairs for states that are not supposed to occur - a dead worker's lease, a caller whose child can no
// longer revive it, a child whose parent went terminal, a flow whose every step is terminal - so none of them is on a
// latency path. None may be given a trigger or an early wake: if a repair ever needs to be prompt, that is evidence the
// state it repairs is reachable in normal operation, which is a bug to fix at the source rather than to sweep faster.
//
// A job runs a pass as soon as its janitor resumes - at Startup on every shard this replica dispatches - because a
// replica restarting after a crash inherits its own expired leases, and reclaiming those is exactly lease recovery's
// job. Only a dispatcher's janitor is active, so only a replica that DISPATCHES a shard repairs it: each repair is
// idempotent, so one sweeper per shard is enough, and every shard always has a dispatcher; a replica that only reads a
// shard would otherwise run these heavy scans through its two-connection reader pool, once per replica in the fleet.
//
// A pass in flight is abandoned when its janitor pauses or stops: each repair writes in one statement or one
// transaction, so an abandoned one lands whole or not at all, and what is left falls to a peer or the next start. The
// jobs are added once, with the shard's actors, and the test hook's rebuild of the removal jobs leaves them alone.
func (e *Engine) addRecoveryJobs(j *janitor.Janitor, shard int) {
	j.AddJob(janitor.JobLeaseRecovery, janitor.NewRecovery(e.recoveryInterval, e.recoveryPass(janitor.JobLeaseRecovery, shard, e.recoverExpiredLeases)))
	j.AddJob(janitor.JobOrphanRepair, janitor.NewRecovery(e.recoveryInterval, e.recoveryPass(janitor.JobOrphanRepair, shard, e.recoverOrphanedFlows)))
	j.AddJob(janitor.JobWedgeSweep, janitor.NewRecovery(wedgeSweepEvery*e.recoveryInterval, e.recoveryPass(janitor.JobWedgeSweep, shard, e.wedgeSweep)))
}

// wedgeSweepEvery is how many recovery intervals pass between wedge sweeps - every 5 minutes at the 1-minute interval,
// matching the 5-minute age guards its detectors carry, so a sweep more often finds nothing new.
const wedgeSweepEvery = 5

// recoveryPass is pass on shard's DB, as the recovery job named job runs it.
func (e *Engine) recoveryPass(job string, shard int, pass func(ctx context.Context, db *sequel.DB, shard int) error) janitor.RecoveryPass {
	return func(ctx context.Context) error {
		if e.diags.Seams().Enabled() {
			defer e.diags.Seams().Checkpoint(ctx, diagnostics.Join(diagnostics.CheckpointRecoveryPassDone, job, strconv.Itoa(shard)))
		}
		sh, err := e.db.Shard(shard)
		if err != nil {
			e.logSweepError(ctx, "Recovery pass: resolving the shard", "job", job, "shard", shard, "error", err)
			return errors.Trace(err)
		}
		return pass(ctx, sh.DB(), shard)
	}
}

// logSweepError logs a failed statement of a background pass - unless the pass's ctx is done, which means the
// engine is stopping and abandoned the pass on purpose.
func (e *Engine) logSweepError(ctx context.Context, msg string, args ...any) {
	if ctx.Err() != nil {
		return
	}
	e.diags.Logger().ErrorContext(ctx, msg, args...)
}

// wedgeSweep is the wedge sweep's pass: the two park detectors, then the stranded-flow alarm, each run whatever the
// others met, and it returns their errors.
//
// The two park detectors are mirror images and both must run: one finds a live CALLER whose child can no
// longer revive it, the other a live CHILD whose caller went terminal. Each carries a parkWedgeThreshold
// age guard, so steady-state operation never trips one, and each recovery re-invokes the normal release
// mechanism (itself CAS-guarded on the park state) rather than reimplementing it - which is what makes them
// idempotent, and harmless under a concurrent resolution, a false positive, or a peer sweeping the same
// shard. A nonzero dwarf_steps_unwedged means a latent bug let a step wedge: the sweep papered over the
// effect, and the cause is still worth finding.
func (e *Engine) wedgeSweep(ctx context.Context, db *sequel.DB, shard int) error {
	parks := e.recoverWedgedSubgraphParks(ctx, db, shard, e.parkWedgeThreshold)
	children := e.recoverOrphanedSubgraphChildren(ctx, db, shard, e.parkWedgeThreshold)
	stranded := e.alarmStrandedFlows(ctx, db, shard)
	return errors.Join(parks, children, stranded)
}

// recoverExpiredLeases resets every running step of shard whose lease has expired: lease recovery's pass. It is the
// ONLY thing that does - a piston never touches a `running` row - so it is what makes at-least-once execution
// actually deliver the "at least" half after a worker dies holding a lease.
//
// It is not a dispatch mechanism and rings no doorbell. A step it resets to `pending` is due, so its
// shard's next piston cycle (~67ms at the derived interval) selects it like any other, and the only
// latency this cadence governs is how long an abandoned step waits past its own lease.
//
// THE INTERVAL IS THE COST OF A CRASH: worst case is `budget + leaseMargin + recoveryInterval` - 2m + 30s + 1m at the
// defaults, and ~3x the lease at a 2s budget - plus a pass's own length, since the next begins an interval after the
// last ended. The path runs when a replica died without draining, and also on a
// live one: persist leaves a step for lease recovery when the database is unreachable for both the outcome write
// and failStep, and a release on drain can fail the same way. A sweep run only at startup, or triggered by a
// peer's stale registry row, never reaches those. The SELECT walks the selection index's whole `running` range
// (`lease_expires` is not a seekable key there), and that range is mostly DEAD entries: every claim inserts one,
// the step's later updates are non-HOT, and only a scan or vacuum clears it. So a pass costs a heap fetch per step
// that left `running` since the range was last cleaned - throughput x the gap between sweeps, not the in-flight
// count - and marks those entries dead for the next. Measured: 34 live rows behind 7,997 heap fetches locally,
// ~1s a pass after a minute at ~1,300 steps/s, ~0.8s mean on a 32 vCPU rig at ~22k. A held vacuum horizon stops
// the marking, and every pass then walks everything since the hold began. Do not add a `lease_expires` index on
// running rows for this: a completed step's entry keeps its claim-time lease, so it enters the expired prefix a
// lease later and a held horizon grows it just the same, while every claim of the hottest table pays the insert.
//
// Find the ids with a plain SELECT, then reset them by id. A single `UPDATE ... WHERE lease_expires<=NOW` must
// row-lock every row its WHERE re-checks, so it can queue behind every in-flight claim and completion; that form
// measured 1.2-2.9s per pass while matching nothing (PG 18, ~2,000 flows in flight). A plain SELECT reads past
// those locks (Postgres, MySQL, and SQL Server under READ_COMMITTED_SNAPSHOT), and the by-id UPDATE locks only
// the rows it resets. The UPDATE repeats
// the whole predicate, so a step completed or re-claimed between the two statements is left alone (pinned by
// TestLeaseRecovery_StepCompletedBetweenSelectAndResetIsLeftAlone). The SELECT
// carries no LIMIT: SQL Server's LIMIT_OFFSET needs an ORDER BY, which on step_id invites a primary-key walk
// over every step ever written; the expired set is bounded by the in-flight count anyway.
// If the interval must be shortened, three constraints hold:
//   - Derive it from leaseMargin, never from the engine's TimeBudget. FlowOptions.TimeBudget is per flow and each
//     lease comes from its own step row, so an engine-wide budget bounds nothing - a 2s flow on a 2m-default engine
//     waits the interval regardless. leaseMargin is the term every lease shares: a leaseMargin/2 interval bounds
//     recovery at lease + 15s, at most 1.5x the lease, for every budget.
//   - Size the bound for ONE surviving sweeper. Several dispatchers sweeping a shard lower the expected wait to
//     about interval/(dispatchers+1), but the crash being recovered removed one, and the floor is two. Do not stagger
//     or coordinate sweepers to win that average back: that is a peer signal.
//   - Measure the running-row scan first, at a realistic in-flight count and the full dispatcher count.
//
// Do not give this a trigger, an early wake, or a wake sized from a per-sweep `MIN(lease_expires)` query. Do
// not replace the interval with
// repairs triggered by a peer's stale registry row either: a stale row is weak evidence of a crash in both
// directions (a database outage stales live peers; the replicas taking over a dead dispatcher's slot do not yet
// know their role), and closing those gaps costs far more than the minute it saves.
//
// A failed statement is logged and returned, and left for the next pass. The pass is idempotent and unconditional, so
// a transient DB error costs one deferred pass and needs no retry or backoff of its own.
func (e *Engine) recoverExpiredLeases(ctx context.Context, db *sequel.DB, shard int) error {
	const expired = "status='" + workflow.StatusRunning + "' AND parked=0 AND lease_expires<=NOW_UTC()"
	rows, err := db.QueryContext(ctx, "SELECT step_id FROM dwarf_steps WHERE "+expired)
	if err != nil {
		e.logSweepError(ctx, "Lease recovery sweep: selecting expired steps", "shard", shard, "error", err)
		return errors.Trace(err)
	}
	var stepIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			e.logSweepError(ctx, "Lease recovery sweep: scanning expired step id", "shard", shard, "error", err)
			return errors.Trace(err)
		}
		stepIDs = append(stepIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		e.logSweepError(ctx, "Lease recovery sweep: iterating expired steps", "shard", shard, "error", err)
		return errors.Trace(err)
	}
	if len(stepIDs) > 0 {
		e.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointLeaseRecoverySelected)
	}
	// e.leaseRecoveryBatch caps each UPDATE's bound IN (...) list. One batch covers any realistic pass.
	for len(stepIDs) > 0 {
		batch := stepIDs[:min(len(stepIDs), e.leaseRecoveryBatch, database.MaxListIDs)]
		stepIDs = stepIDs[len(batch):]
		ph, args := database.IDList(batch)
		res, err := db.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, not_before=NOW_UTC(), updated_at=NOW_UTC()"+
				" SEEK_FROM(dwarf_steps, 1(step_id)) WHERE step_id IN ("+ph+") AND "+expired,
			append([]any{workflow.StatusPending}, args...)...,
		)
		if err != nil {
			e.logSweepError(ctx, "Lease recovery sweep", "shard", shard, "error", err)
			return errors.Trace(err)
		}
		if recovered, _ := res.RowsAffected(); recovered > 0 {
			e.metricStepsRecovered(ctx, int(recovered))
		}
	}
	return nil
}

// wedgedParksSQL selects the parked callers no open child can revive. The literal f.surgraph_step_id>0 lets the
// partial index serve the correlated lookup; without it SQLite scans dwarf_flows once per parked step.
const wedgedParksSQL = "SELECT s.step_id, s.flow_id, s.flow_created_hour, s.parked_child_flow_id FROM dwarf_steps s" +
	" WHERE s.parked=? AND s.status='" + workflow.StatusRunning + "' AND s.updated_at <= DATE_ADD_MILLIS(NOW_UTC(), ?)" +
	" AND NOT EXISTS (SELECT 1 FROM dwarf_flows f WHERE f.surgraph_step_id=s.step_id AND f.surgraph_step_id>0" +
	" AND f.status IN ('" + workflow.StatusCreated + "', '" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "'))"

// recoverWedgedSubgraphParks finds parkedSubgraph caller steps whose child flow can no longer revive them -
// the child reached terminal but completeSurgraphFlow's revive was lost, or the child was deleted - and
// re-drives the release. A healthy caller step has one non-terminal child (created/running/interrupted)
// bound to it by surgraph_step_id and named by its parked_child_flow_id; a fan-out has several such caller steps,
// each checked independently here. The release is fenced on the owner the scan read, so a caller re-parked on a
// newer child since the scan is left alone. completeSurgraphFlow
// runs within milliseconds of child completion in steady state, so a step older than parkWedgeThreshold with
// no non-terminal child is genuinely wedged.
//
// The age comparison is `<=`, not `<`, so that minAge=0 means "no age guard" - which is exactly what every
// caller passing it reads it as. NOW_UTC() is millisecond-precision, so a strict `<` silently excludes a
// step stamped inside the CURRENT millisecond: a genuinely wedged caller whose park happens to share a tick
// with the sweep is skipped, the sweep reports nothing to do, and the wedge it exists to clear survives.
// The boundary is immaterial at the production threshold (one millisecond either side of five minutes), so
// `<=` costs nothing there and makes the zero case honest.
func (e *Engine) recoverWedgedSubgraphParks(ctx context.Context, db *sequel.DB, shard int, minAge time.Duration) error {
	rows, err := db.QueryContext(ctx, wedgedParksSQL, parkedSubgraph, -minAge.Milliseconds())
	if err != nil {
		e.logSweepError(ctx, "Wedge sweep: querying parked subgraph steps", "shard", shard, "error", err)
		return errors.Trace(err)
	}
	type wedgedCaller struct{ stepID, flowID, flowHour, childFlowID int }
	var hits []wedgedCaller
	for rows.Next() {
		var w wedgedCaller
		err := rows.Scan(&w.stepID, &w.flowID, &w.flowHour, &w.childFlowID)
		if err != nil {
			rows.Close()
			e.logSweepError(ctx, "Wedge sweep: scanning parked subgraph step", "shard", shard, "error", err)
			return errors.Trace(err)
		}
		hits = append(hits, w)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		e.logSweepError(ctx, "Wedge sweep: iterating parked subgraph steps", "shard", shard, "error", err)
		return errors.Trace(err)
	}
	if len(hits) > 0 {
		e.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointWedgeSweepSelected)
	}

	var failed error
	for _, w := range hits {
		if ctx.Err() != nil {
			return ctx.Err() // abandoned by a pause or a stop; the next pass finds what is left
		}
		// The child that owns the park, as the scan read it off the caller, decides the disposition, and every
		// delivery below is fenced on it: a caller re-parked on a newer child since the scan is left alone.
		childFlowID := w.childFlowID
		var childStatus, childError, childCancelReason string
		var childHour int
		var childFinalState []byte
		err := db.QueryRowContext(ctx,
			"SELECT status, created_hour, final_state, error, cancel_reason FROM dwarf_flows WHERE flow_id=?",
			childFlowID,
		).Scan(&childStatus, &childHour, &childFinalState, &childError, &childCancelReason)
		switch {
		case err == sql.ErrNoRows:
			// The child flow is gone (e.g. deleted/purged): fail the caller so the flow can terminate.
			e.diags.Logger().ErrorContext(ctx, "Wedge sweep: parked subgraph caller has no child flow; failing it",
				"shard", shard, "step", w.stepID, "flow", keys.CorrelationID(shard, w.flowHour, w.flowID))
			if rerr := e.deliverSubgraphError(ctx, shard, w.stepID, childFlowID, errors.New("subgraph flow not found (wedge recovery)")); rerr != nil {
				e.logSweepError(ctx, "Wedge sweep: failing orphaned subgraph caller", "shard", shard, "step", w.stepID, "error", rerr)
				failed = errors.Join(failed, rerr)
				continue
			}
			e.metricStepUnwedged(ctx, "subgraph")
		case err != nil:
			e.logSweepError(ctx, "Wedge sweep: reading child flow", "shard", shard, "step", w.stepID, "error", err)
			failed = errors.Join(failed, err)
		case !workflow.IsTerminalStatus(childStatus):
			// The scan found no open child, so an open one now is a spawn that landed since - the caller was
			// revived and parked again. It is not wedged.
		default:
			e.diags.Logger().ErrorContext(ctx, "Wedge sweep: reviving wedged subgraph caller",
				"shard", shard, "step", w.stepID, "childFlow", keys.CorrelationID(shard, childHour, childFlowID), "childStatus", childStatus)
			var rerr error
			switch childStatus {
			case workflow.StatusCompleted:
				rerr = e.completeSurgraphFlow(ctx, shard, w.flowID, w.stepID, childFlowID, childFinalState)
			case workflow.StatusCancelled:
				// Delivered as the cancellation itself, as the live path does, so the caller can recognize it.
				rerr = e.deliverSubgraphError(ctx, shard, w.stepID, childFlowID, cancelmarker.New(strings.TrimSpace(childCancelReason)))
			default:
				// failed / terminated: deliver the child's error (or a synthesized one) to the caller. The stored
				// message is passed as an argument, never as the pattern, which would read any % in it as a verb.
				msg := strings.TrimSpace(childError)
				if msg == "" {
					msg = "subgraph " + childStatus
				}
				rerr = e.deliverSubgraphError(ctx, shard, w.stepID, childFlowID, errors.New("%s", msg))
			}
			if rerr != nil {
				e.logSweepError(ctx, "Wedge sweep: reviving subgraph caller", "shard", shard, "step", w.stepID, "error", rerr)
				failed = errors.Join(failed, rerr)
				continue
			}
			e.metricStepUnwedged(ctx, "subgraph")
		}
	}
	return failed
}

// recoverOrphanedSubgraphChildren finds non-terminal subgraph child flows whose parent flow has already gone
// terminal - the mirror image of recoverWedgedSubgraphParks (which handles a parked caller whose child is gone;
// this handles an open child whose caller/parent is gone). Such a child is orphaned: its parent can never process
// its completion or interrupt, and no lifecycle op reaches it - the root key 409s (the root is terminal), the
// child's own key is read-only (Resume/Terminate 400), and recoverWedgedSubgraphParks is blind to it (the caller
// step is terminal, not running+parked). Nothing in the engine leaves one: a spawn parks its caller and inserts
// the child in one transaction, and Terminate re-scans the tree inside its own after locking the tree's steps
// (terminateSubtree), so a Terminate racing a spawn either prevents the child or terminates it; and a subgraph
// child fails via cohort accounting after every branch settles, never eagerly while a sibling is live. This is
// defense in depth for a bug that would. The sweep tears the
// orphan down by terminating its subtree, sharing the parent's terminal fate. In steady state a parent goes
// terminal only after its live subgraphs resolve, so a non-terminal child under a parent terminal for longer
// than minAge is genuinely orphaned; the age guard excludes the sub-second window in which a just-terminalized
// parent's sibling child is still being cleaned up by the normal completion/error path. The guard reads the
// PARENT's updated_at, which its terminal write stamps: the child's own is frozen at its last status change,
// so a child running longer than minAge would pass it the instant its parent went terminal. An interrupted parent is
// deliberately excluded (not terminal): a Resume of the root revives the interrupted branch, and a sibling child
// running under it is healthy.
func (e *Engine) recoverOrphanedSubgraphChildren(ctx context.Context, db *sequel.DB, shard int, minAge time.Duration) error {
	rows, err := db.QueryContext(ctx,
		"SELECT c.flow_id, c.created_hour, c.flow_token FROM dwarf_flows c"+
			" JOIN dwarf_flows p ON p.flow_id=c.surgraph_flow_id"+
			" WHERE c.surgraph_flow_id>0 AND c.status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')"+
			// `<=`, so minAge=0 means "no age guard" - see recoverWedgedSubgraphParks for why a strict `<`
			// silently skips a row stamped inside the sweep's own millisecond.
			" AND p.updated_at <= DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND p.status IN ("+database.TerminalStatusesSQL+")",
		-minAge.Milliseconds(),
	)
	if err != nil {
		e.logSweepError(ctx, "Wedge sweep: querying orphaned subgraph children", "shard", shard, "error", err)
		return errors.Trace(err)
	}
	type orphan struct {
		flowID, hour int
		token        string
	}
	var hits []orphan
	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.flowID, &o.hour, &o.token); err != nil {
			rows.Close()
			e.logSweepError(ctx, "Wedge sweep: scanning orphaned subgraph child", "shard", shard, "error", err)
			return errors.Trace(err)
		}
		hits = append(hits, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		e.logSweepError(ctx, "Wedge sweep: iterating orphaned subgraph children", "shard", shard, "error", err)
		return errors.Trace(err)
	}

	var failed error
	for _, o := range hits {
		if ctx.Err() != nil {
			return ctx.Err() // abandoned by a pause or a stop; the next pass finds what is left
		}
		e.diags.Logger().ErrorContext(ctx, "Wedge sweep: terminating orphaned subgraph child whose parent is terminal",
			"shard", shard, "childFlow", keys.CorrelationID(shard, o.hour, o.flowID))
		if rerr := e.terminateOrphanedSubtree(ctx, shard, o.flowID, o.hour, o.token); rerr != nil {
			e.logSweepError(ctx, "Wedge sweep: terminating orphaned subgraph child", "shard", shard, "childFlow", keys.CorrelationID(shard, o.hour, o.flowID), "error", rerr)
			failed = errors.Join(failed, rerr)
			continue
		}
		e.metricStepUnwedged(ctx, "orphaned_child")
	}
	return failed
}

// terminateOrphanedSubtree terminates an orphaned subgraph child and its own non-terminal descendants, sharing
// the public Terminate's transaction (terminateSubtree). No surgraph up-walk is needed - and none exists: the
// ancestor chain is already terminal, which is precisely why the child is orphaned. A child that terminalized
// concurrently between the sweep's SELECT and this write is a no-op, the outcome the sweep wanted anyway.
func (e *Engine) terminateOrphanedSubtree(ctx context.Context, shard int, childFlowID, childFlowHour int, childFlowToken string) error {
	const reason = "parent flow terminated (orphan recovery)"
	_, err := e.terminateSubtree(ctx, shard, childFlowID, childFlowHour, childFlowToken, reason, "", false)
	return errors.Trace(err)
}

// recoverOrphanedFlows repairs the lost transitions of shard - a `completed` step of an open flow whose transition
// never committed and whose lease has lapsed - by rewinding that step, whatever the flow's other steps are doing:
// orphan repair's pass.
//
// That shape is what a worker leaves when it dies between a step's completion write and its transition
// transaction: the step is `completed`, so lease recovery (which resets `running` rows only) cannot see it, and
// nothing else will ever advance it. The completion write sets advancing=1, and every transition transaction
// that commits while the flow stays open clears it, so advancing=1 on a completed step of a running or interrupted
// flow means exactly "this step's transition never committed". The repair rewinds such a step to `pending` - the
// same rewind the processStep recovery defer performs for a worker that is still alive - and dispatch REPLAYS it:
// the claim sees the flag, skips the task, and commits the transition from the output and disposition the
// completion write recorded. The task does not run again, and the transition is the one the dead worker would have
// committed. No transition logic is duplicated here, and a flow with no flagged step is never touched, so a false
// positive cannot double-advance a flow.
//
// THE REPAIR IS PER STEP, NOT PER FLOW. Do not gate it on the rest of the flow being terminal: a fan-out branch
// whose transition was lost would then wait out its live siblings - a 24h retry backoff, a parked subgraph, a human
// on an interrupt elsewhere in the flow - instead of its own lease. An interrupted flow is repaired too, for the
// same reason: its other branches run on while it waits (a transition commits into an interrupted flow), and the
// rewind is a transition's lock order, flow row first.
//
// THE LEASE IS THE ONLY WAIT, and the repair runs every recoveryInterval, so a dead worker's completed step is
// recovered within `lease + recoveryInterval` - the bound lease recovery gives a claimed step. Do not add an age
// guard on step activity to the repair: a step mid-transition holds an unexpired lease, so the scan already skips it,
// and a worker that keeps its step past its lease without dying is fenced rather than raced - the rewind bumps
// lease_seq, so a worker that outlived its lease rolls its transition back at its fenced step-row write instead of
// committing successors beside the re-dispatch's. The step is then replayed, not re-run, so a false positive costs
// one redone transition and never a second run of the task. Reaching one takes a task that overran its budget, a
// transition stalled past the leaseMargin, or the database clock jumping forward by more than the leaseMargin: the
// completion write leaves the lease as the claim set it, so a task that returns within its budget reaches its
// transition with leaseMargin left, and the two waits that can outlast that - persist's retry backoff and the cohort
// stripe - extend the lease first. The rewind runs behind a write-first lock on the flow row, guarded on the flow
// being open, so a Terminate landing between the scan and the rewind wins and the closed flow is left as it is.
//
// A flow stranded some other way, with no step flagged, is not repaired: alarmStrandedFlows reports it.
//
// THE SCAN LISTS THE OPEN FLOWS, THEN CHECKS THEM inListChunk AT A TIME. The work depends on the retained
// steps of those flows, not just their count: proving no completed step is still advancing can read the entire
// history of an open flow. Chunking bounds the candidate flow IDs, not the step rows examined or the query plan;
// a planner can prefer a sequential scan when those flows account for a large share of the step table. Do not
// check every open flow in one statement, which makes that the likely plan: measured on PostgreSQL 16 at 240,000
// running flows over 2.1M step rows, the one statement ran two parallel sequential scans of dwarf_steps (1.5 s)
// where the chunks probed the primary key and idx_dwarf_steps_flow_id per flow (6-7 ms per chunk, ~3 s for the
// pass, ~13 us a flow); against a 190 GB dwarf_steps the one statement averaged 48 s a pass and peaked at 596 s.
// OPEN INCLUDES THE WAITERS: sleeping/retrying and subgraph-parked flows are running, and human waits are
// interrupted, so all are checked on every pass. This cost is paid once a minute on every dispatcher of the shard, and
// again every five minutes by the alarm, whose own scan runs the same code (sweepOpenFlows): six scans of the open
// flows every five minutes. Advancing remains deliberately unindexed: it is
// set and cleared on every step, so indexing it adds two index changes per step on the hottest table. A chunk that
// fails is skipped rather than ending the pass, so one bad chunk cannot hold back every repair on the shard.
// The flow row cannot shortcut the search: stopped_step_id is 0 while a flow runs.
func (e *Engine) recoverOrphanedFlows(ctx context.Context, db *sequel.DB, shard int) error {
	return e.sweepOpenFlows(ctx, db, shard, false)
}

// alarmStrandedFlows reports the stranded flows of shard: the wedge sweep's alarm. A stranded flow - running, every
// step terminal, none flagged - is a bug signal: some other path stranded it. It is logged at error level and counted
// by dwarf_flows_orphaned, and not repaired. That verdict is re-taken under the flow-row lock, so a flow that went
// terminal, was repaired by a peer's sweep, or moved on after the unlocked scan is not miscounted. The ALARM keeps an
// age guard, and only the alarm: it fires only once no step of the flow has been touched for orphanFlowThreshold, so
// a transient all-terminal state nobody has named is not reported as a bug. It runs every wedgeSweepEvery recovery
// intervals, not with every pass of the repair, so an unrepairable flow is counted every five minutes rather than every
// minute. The guard reads dwarf_steps.updated_at, which moves on every step transition; dwarf_flows.updated_at is
// frozen at the flow's go-`running` time and says nothing about progress. "All steps terminal" excludes every
// legitimate long-wait from the alarm, because each holds a non-terminal step: a task mid-execution, a sleep/retry
// backoff, a parked subgraph caller, a human, a live sibling.
func (e *Engine) alarmStrandedFlows(ctx context.Context, db *sequel.DB, shard int) error {
	return e.sweepOpenFlows(ctx, db, shard, true)
}

// sweepOpenFlows is the scan of shard's open flows that orphan repair and the stranded-flow alarm share: without
// alarm it rewinds the lost transitions (recoverOrphanedFlows), with alarm it reports the stranded flows
// (alarmStrandedFlows). It goes on past a chunk or a flow that fails, and returns their errors.
func (e *Engine) sweepOpenFlows(ctx context.Context, db *sequel.DB, shard int, alarm bool) error {
	var running []int
	rows, err := db.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')")
	if err != nil {
		e.logSweepError(ctx, "Orphan sweep: listing open flows", "shard", shard, "error", err)
		return errors.Trace(err)
	}
	for rows.Next() {
		var flowID int
		if err := rows.Scan(&flowID); err != nil {
			rows.Close()
			e.logSweepError(ctx, "Orphan sweep: scanning open flow", "shard", shard, "error", err)
			return errors.Trace(err)
		}
		running = append(running, flowID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		e.logSweepError(ctx, "Orphan sweep: iterating open flows", "shard", shard, "error", err)
		return errors.Trace(err)
	}
	// Ascending, so each chunk's primary-key probes stay in one region of the index.
	slices.Sort(running)

	type orphan struct {
		flowID, hour int
		workflowURL  string
	}
	// A flow is a hit for the repair when one of its steps lost its transition and its lease has lapsed - whatever its
	// other steps are doing - and for the alarm when it is running with every step terminal and none flagged.
	const lostTransition = "EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status='" + workflow.StatusCompleted + "' AND s.advancing=1 AND s.lease_expires<=NOW_UTC())"
	const stranded = "(f.status='" + workflow.StatusRunning + "'" +
		" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s2 WHERE s2.flow_id=f.flow_id AND (s2.status IN ('" + workflow.StatusCreated + "', '" + workflow.StatusPending + "', '" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')" +
		" OR (s2.status='" + workflow.StatusCompleted + "' AND s2.advancing=1))))"
	hit := lostTransition
	if alarm {
		hit = stranded
	}
	var failed error
	var hits []orphan
	for chunk := range slices.Chunk(running, inListChunk) {
		if ctx.Err() != nil {
			return ctx.Err() // abandoned by a pause or a stop; the next pass finds what is left
		}
		ph, ids := inList(chunk)
		rows, err := db.QueryContext(ctx,
			"SELECT f.flow_id, f.created_hour, f.workflow_url FROM dwarf_flows f"+
				" WHERE f.flow_id IN ("+ph+") AND f.status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"') AND "+hit,
			ids...,
		)
		if err != nil {
			e.logSweepError(ctx, "Orphan sweep: checking open flows", "shard", shard, "error", err)
			failed = errors.Join(failed, err)
			continue
		}
		for rows.Next() {
			var o orphan
			if err := rows.Scan(&o.flowID, &o.hour, &o.workflowURL); err != nil {
				e.logSweepError(ctx, "Orphan sweep: scanning stranded flow", "shard", shard, "error", err)
				failed = errors.Join(failed, err)
				break
			}
			hits = append(hits, o)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			e.logSweepError(ctx, "Orphan sweep: iterating stranded flows", "shard", shard, "error", err)
			failed = errors.Join(failed, err)
		}
	}

	for _, o := range hits {
		if ctx.Err() != nil {
			return ctx.Err() // abandoned by a pause or a stop; the next pass finds what is left
		}
		// The scan read without locks, so its verdict is re-taken under the flow-row lock. Since the scan the flow may
		// have gone terminal, a flagged step may have stopped qualifying (its worker's lease extended, or a peer's
		// sweep rewound it, or a slow worker's transition committed), or a stranded flow may have moved on. Only a
		// flow still open with every step terminal and none flagged is an unrepairable orphan - and an interrupted
		// flow never is, since it holds the interrupted step.
		var rewound, held int64
		var locked bool
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			rewound, held, locked = 0, 0, false
			n, err := lockFlowRows(ctx, tx, "flow_id=? AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')", o.flowID)
			if err != nil {
				return err
			}
			if n == 0 {
				return nil // terminalized since the scan
			}
			locked = true
			if alarm {
				// A flagged step still under lease is its worker's, and the repair's once that lapses; a non-terminal
				// step means the flow is moving again; a step touched within orphanFlowThreshold holds the alarm off
				// (see alarmStrandedFlows).
				return errors.Trace(tx.QueryRowContext(ctx,
					"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND (status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')"+
						" OR (status='"+workflow.StatusCompleted+"' AND advancing=1) OR updated_at >= DATE_ADD_MILLIS(NOW_UTC(), ?))",
					o.flowID, -e.orphanFlowThreshold.Milliseconds(),
				).Scan(&held))
			}
			res, err := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, lease_seq=lease_seq+1, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC()"+
					" SEEK_FROM(dwarf_steps, idx_dwarf_steps_flow_id(flow_id)) WHERE flow_id=? AND status='"+workflow.StatusCompleted+"' AND advancing=1 AND lease_expires<=NOW_UTC()",
				workflow.StatusPending, o.flowID,
			)
			if err != nil {
				return errors.Trace(err)
			}
			rewound, _ = res.RowsAffected()
			return nil
		})
		// Token-free correlation id: an operator alarm must not carry the flow key, which is a write capability.
		switch {
		case err != nil && alarm:
			e.logSweepError(ctx, "Orphan sweep: checking a stranded flow", "shard", shard, "flow", keys.CorrelationID(shard, o.hour, o.flowID), "error", err)
			failed = errors.Join(failed, err)
		case err != nil:
			e.logSweepError(ctx, "Orphan sweep: rewinding stranded steps", "shard", shard, "flow", keys.CorrelationID(shard, o.hour, o.flowID), "error", err)
			failed = errors.Join(failed, err)
		case rewound > 0:
			e.diags.Logger().WarnContext(ctx, "Orphan sweep: replaying steps whose transition never committed",
				"flow", keys.CorrelationID(shard, o.hour, o.flowID), "steps", rewound)
			e.metricStepsRecovered(ctx, int(rewound))
		case alarm && locked && held == 0:
			e.diags.Logger().ErrorContext(ctx, "Orphaned flow: running with all steps terminal and no successor",
				"flow", keys.CorrelationID(shard, o.hour, o.flowID))
			e.metricOrphanedFlow(ctx, o.workflowURL)
		}
	}
	return failed
}
