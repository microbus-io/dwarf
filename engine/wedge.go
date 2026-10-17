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
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/cancelmarker"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// recoveryLoop is THE recovery cadence - every background repair the engine performs runs here, on one
// plain ticker, and nowhere else.
//
// One ticker, two rates. Lease recovery and the orphan-flow repair run every tick: between them they recover
// every step a dead worker held, claimed or just completed, and the tick is the term a crash adds to either. The
// park detectors run every heavyRepairEvery-th tick: they carry 5-minute age guards, so running them more often
// finds nothing new.
//
// These are repairs for states that are not supposed to occur - a dead worker's lease, a caller whose child
// can no longer revive it, a child whose parent went terminal, a flow whose every step is terminal - so none
// of them is on a latency path. Nothing here may be given its own timer or an early wake: if a repair ever
// needs to be prompt, that is evidence the state it repairs is reachable in normal operation, which is a bug
// to fix at the source rather than to sweep faster.
//
// It sweeps on entry rather than after the first tick, because a replica restarting after a crash inherits
// its own expired leases and reclaiming those is exactly this loop's job. The entry pass is a heavy one, though
// the wedge detectors are no-ops that early: their age guards exclude everything younger than parkWedgeThreshold.
//
// Every repair runs only on the shards this replica DISPATCHES. Each repair is idempotent, so one sweeper
// per shard is enough, and every shard always has a dispatcher; a replica that only reads a shard would
// otherwise run these heavy scans through its two-connection reader pool, once per replica in the fleet.
func (e *Engine) recoveryLoop(ctx context.Context) {
	ticker := time.NewTicker(e.recoveryInterval)
	defer ticker.Stop()
	for tick := 0; ; tick++ {
		e.runRecoverySweep(ctx, tick%heavyRepairEvery == 0)
		select {
		case <-e.recoveryStop:
			return
		case <-ticker.C:
		}
	}
}

// heavyRepairEvery is how many recovery ticks pass between runs of the park detectors - every 5 minutes at the
// 1-minute tick, matching the 5-minute age guards they carry.
const heavyRepairEvery = 5

// runRecoverySweep is one recovery pass: lease recovery and the orphan-flow repair always, and the park detectors
// when heavy is set.
//
// The two park detectors are mirror images and both must run: one finds a live CALLER whose child can no
// longer revive it, the other a live CHILD whose caller went terminal. Each carries a parkWedgeThreshold
// age guard, so steady-state operation never trips one, and each recovery re-invokes the normal release
// mechanism (itself CAS-guarded on the park state) rather than reimplementing it - which is what makes them
// idempotent, and harmless under a concurrent resolution, a false positive, or a peer sweeping the same
// shard. A nonzero dwarf_steps_unwedged means a latent bug let a step wedge: the sweep papered over the
// effect, and the cause is still worth finding.
func (e *Engine) runRecoverySweep(ctx context.Context, heavy bool) {
	e.recoverExpiredLeases(ctx)
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.dispatchesOn(shard) {
			return nil // one sweeper per shard suffices, and a reader's pool must not carry the scans
		}
		// This pass takes a turn on the shard it is about to read, so a background sweep queues for its
		// connections like any other caller instead of taking them from work already under way.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		if heavy {
			e.recoverWedgedSubgraphParks(ctx, db, shard, e.parkWedgeThreshold)
			e.recoverOrphanedSubgraphChildren(ctx, db, shard, e.parkWedgeThreshold)
		}
		e.recoverOrphanedFlows(ctx, db, shard, heavy)
		return nil
	})
	// Test rendezvous: this pass is over, so every detector below has already read the database. A test that
	// FORGES a wedge/orphan shape and then drives one detector itself must wait for this, or the background
	// pass races its forge and the shape is counted twice (see engineseams.CheckpointRecoverySweepDone).
	e.seams.Checkpoint(ctx, engineseams.CheckpointRecoverySweepDone)
}

// recoverExpiredLeases resets every running step whose lease has expired, on every shard. It is the ONLY
// thing that does - a piston never touches a `running` row - so it is what makes at-least-once execution
// actually deliver the "at least" half after a worker dies holding a lease.
//
// It is not a dispatch mechanism and rings no doorbell. A step it resets to `pending` is due, so its
// shard's next piston cycle (~67ms at the derived interval) selects it like any other, and the only
// latency this cadence governs is how long an abandoned step waits past its own lease.
//
// THE TICK IS THE COST OF A CRASH: worst case is `budget + leaseMargin + recoveryInterval` - 2m + 30s + 1m at the
// defaults, and ~3x the lease at a 2s budget. The path runs when a replica died without draining, and also on a
// live one: persist leaves a step for lease recovery when the database is unreachable for both the outcome write
// and failStep, and a release on drain can fail the same way. A sweep run only at startup, or triggered by a
// peer's stale registry row, never reaches those. The SELECT reads every `running` non-parked row
// (`lease_expires` is not a seekable key in the selection index), and every dispatcher of the shard runs it, so
// its cost is O(in-flight) per dispatcher per tick - small at a minute. Making it O(expired) would need a
// `lease_expires` index on running rows, maintained on every claim and completion of the hottest table; not
// worth it at this rate.
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
// If the tick must be shortened, three constraints hold:
//   - Derive it from leaseMargin, never from the engine's TimeBudget. FlowOptions.TimeBudget is per flow and each
//     lease comes from its own step row, so an engine-wide budget bounds nothing - a 2s flow on a 2m-default engine
//     waits the tick regardless. leaseMargin is the term every lease shares: a leaseMargin/2 tick bounds recovery
//     at lease + 15s, at most 1.5x the lease, for every budget.
//   - Size the bound for ONE surviving sweeper. Several dispatchers sweeping a shard lower the expected wait to
//     about tick/(dispatchers+1), but the crash being recovered removed one, and the floor is two. Do not stagger
//     or coordinate sweepers to win that average back: that is a peer signal.
//   - Measure the running-row scan first, at a realistic in-flight count and the full dispatcher count.
//
// Do not give this its own timer, an early wake, or a wake sized from a per-sweep `MIN(lease_expires)` query. Do
// not replace the tick with
// repairs triggered by a peer's stale registry row either: a stale row is weak evidence of a crash in both
// directions (a database outage stales live peers; the replicas taking over a dead dispatcher's slot do not yet
// know their role), and closing those gaps costs far more than the minute it saves.
//
// A failed UPDATE is logged and left for the next sweep. The sweep is idempotent and unconditional, so a
// transient DB error costs one deferred pass and needs no retry or backoff of its own.
func (e *Engine) recoverExpiredLeases(ctx context.Context) {
	const expired = "status='" + workflow.StatusRunning + "' AND parked=0 AND lease_expires<=NOW_UTC()"
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if !e.dispatchesOn(shard) {
			return nil // one sweeper per shard suffices, and a reader's pool must not carry the scans
		}
		// This pass takes a turn on the shard it is about to read, so a background sweep queues for its
		// connections like any other caller instead of taking them from work already under way.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		rows, err := db.QueryContext(ctx, "SELECT step_id FROM dwarf_steps WHERE "+expired)
		if err != nil {
			e.logger.ErrorContext(ctx, "Lease recovery sweep: selecting expired steps", "shard", shard, "error", err)
			return nil
		}
		var stepIDs []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				e.logger.ErrorContext(ctx, "Lease recovery sweep: scanning expired step id", "shard", shard, "error", err)
				return nil
			}
			stepIDs = append(stepIDs, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			e.logger.ErrorContext(ctx, "Lease recovery sweep: iterating expired steps", "shard", shard, "error", err)
			return nil
		}
		if len(stepIDs) > 0 {
			e.seams.Checkpoint(ctx, engineseams.CheckpointLeaseRecoverySelected)
		}
		// e.leaseRecoveryBatch caps each UPDATE's literal `IN (...)` list. One batch covers any realistic pass.
		for len(stepIDs) > 0 {
			batch := stepIDs[:min(len(stepIDs), e.leaseRecoveryBatch)]
			stepIDs = stepIDs[len(batch):]
			// ids are trusted integers embedded as literals to dodge the per-driver bind-param ceiling.
			res, err := db.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, not_before=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN ("+intCSV(batch)+") AND "+expired,
				workflow.StatusPending,
			)
			if err != nil {
				e.logger.ErrorContext(ctx, "Lease recovery sweep", "shard", shard, "error", err)
				return nil
			}
			if recovered, _ := res.RowsAffected(); recovered > 0 {
				e.metricStepsRecovered(ctx, int(recovered))
			}
		}
		return nil
	})
}

// recoverWedgedSubgraphParks finds parkedSubgraph caller steps whose child flow can no longer revive them -
// the child reached terminal but completeSurgraphFlow's revive was lost, or the child was deleted - and
// re-drives the release. A healthy caller step has one non-terminal child (created/running/interrupted)
// bound to it by surgraph_step_id; a fan-out has several such caller steps, each checked independently
// here, and flow.Retry leaves older terminal children whose latest sibling is still active. completeSurgraphFlow
// runs within milliseconds of child completion in steady state, so a step older than parkWedgeThreshold with
// no non-terminal child is genuinely wedged.
//
// The age comparison is `<=`, not `<`, so that minAge=0 means "no age guard" - which is exactly what every
// caller passing it reads it as. NOW_UTC() is millisecond-precision, so a strict `<` silently excludes a
// step stamped inside the CURRENT millisecond: a genuinely wedged caller whose park happens to share a tick
// with the sweep is skipped, the sweep reports nothing to do, and the wedge it exists to clear survives.
// The boundary is immaterial at the production threshold (one millisecond either side of five minutes), so
// `<=` costs nothing there and makes the zero case honest.
func (e *Engine) recoverWedgedSubgraphParks(ctx context.Context, db *sequel.DB, shard int, minAge time.Duration) {
	rows, err := db.QueryContext(ctx,
		"SELECT s.step_id, s.flow_id FROM dwarf_steps s"+
			" WHERE s.parked=? AND s.status='"+workflow.StatusRunning+"' AND s.updated_at <= DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_flows f WHERE f.surgraph_step_id=s.step_id AND f.status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"'))",
		parkedSubgraph, -minAge.Milliseconds(),
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: querying parked subgraph steps", "shard", shard, "error", err)
		return
	}
	type wedgedCaller struct{ stepID, flowID int }
	var hits []wedgedCaller
	for rows.Next() {
		var w wedgedCaller
		err := rows.Scan(&w.stepID, &w.flowID)
		if err != nil {
			rows.Close()
			e.logger.ErrorContext(ctx, "Wedge sweep: scanning parked subgraph step", "shard", shard, "error", err)
			return
		}
		hits = append(hits, w)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: iterating parked subgraph steps", "shard", shard, "error", err)
		return
	}
	if len(hits) > 0 {
		e.seams.Checkpoint(ctx, engineseams.CheckpointWedgeSweepSelected)
	}

	for _, w := range hits {
		// The latest child for this caller step decides the disposition; older children are completed
		// retry attempts. flow_id DESC mirrors how the live completion path keys the surgraph.
		var childFlowID int
		var childStatus, childError, childCancelReason string
		var childFinalState []byte
		err := db.QueryRowContext(ctx,
			"SELECT flow_id, status, final_state, error, cancel_reason FROM dwarf_flows WHERE surgraph_step_id=? ORDER BY flow_id DESC LIMIT_OFFSET(1, 0)",
			w.stepID,
		).Scan(&childFlowID, &childStatus, &childFinalState, &childError, &childCancelReason)
		switch {
		case err == sql.ErrNoRows:
			// The child flow is gone (e.g. deleted/purged): fail the caller so the flow can terminate.
			e.logger.ErrorContext(ctx, "Wedge sweep: parked subgraph caller has no child flow; failing it",
				"shard", shard, "step", w.stepID, "flow", keys.CorrelationID(shard, w.flowID))
			if rerr := e.deliverSubgraphError(ctx, shard, w.stepID, errors.New("subgraph flow not found (wedge recovery)")); rerr != nil {
				e.logger.ErrorContext(ctx, "Wedge sweep: failing orphaned subgraph caller", "shard", shard, "step", w.stepID, "error", rerr)
				continue
			}
			e.metricStepUnwedged(ctx, "subgraph")
		case err != nil:
			e.logger.ErrorContext(ctx, "Wedge sweep: reading child flow", "shard", shard, "step", w.stepID, "error", err)
		case !workflow.IsTerminalStatus(childStatus):
			// The scan found no live child, so a live one now is a spawn that landed since - the caller was
			// revived and parked again. It is not wedged.
		default:
			e.logger.ErrorContext(ctx, "Wedge sweep: reviving wedged subgraph caller",
				"shard", shard, "step", w.stepID, "childFlow", keys.CorrelationID(shard, childFlowID), "childStatus", childStatus)
			var rerr error
			switch childStatus {
			case workflow.StatusCompleted:
				rerr = e.completeSurgraphFlow(ctx, shard, w.flowID, w.stepID, childFinalState)
			case workflow.StatusCancelled:
				// Delivered as the cancellation itself, as the live path does, so the caller can recognize it.
				rerr = e.deliverSubgraphError(ctx, shard, w.stepID, cancelmarker.New(strings.TrimSpace(childCancelReason)))
			default:
				// failed / terminated: deliver the child's error (or a synthesized one) to the caller. The stored
				// message is passed as an argument, never as the pattern, which would read any % in it as a verb.
				msg := strings.TrimSpace(childError)
				if msg == "" {
					msg = "subgraph " + childStatus
				}
				rerr = e.deliverSubgraphError(ctx, shard, w.stepID, errors.New("%s", msg))
			}
			if rerr != nil {
				e.logger.ErrorContext(ctx, "Wedge sweep: reviving subgraph caller", "shard", shard, "step", w.stepID, "error", rerr)
				continue
			}
			e.metricStepUnwedged(ctx, "subgraph")
		}
	}
}

// recoverOrphanedSubgraphChildren finds non-terminal subgraph child flows whose parent flow has already gone
// terminal - the mirror image of recoverWedgedSubgraphParks (which handles a parked caller whose child is gone;
// this handles a live child whose caller/parent is gone). Such a child is orphaned: its parent can never process
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
func (e *Engine) recoverOrphanedSubgraphChildren(ctx context.Context, db *sequel.DB, shard int, minAge time.Duration) {
	rows, err := db.QueryContext(ctx,
		"SELECT c.flow_id, c.flow_token FROM dwarf_flows c"+
			" JOIN dwarf_flows p ON p.flow_id=c.surgraph_flow_id"+
			" WHERE c.surgraph_flow_id>0 AND c.status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')"+
			// `<=`, so minAge=0 means "no age guard" - see recoverWedgedSubgraphParks for why a strict `<`
			// silently skips a row stamped inside the sweep's own millisecond.
			" AND p.updated_at <= DATE_ADD_MILLIS(NOW_UTC(), ?)"+
			" AND p.status IN ("+terminalStatusesSQL+")",
		-minAge.Milliseconds(),
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: querying orphaned subgraph children", "shard", shard, "error", err)
		return
	}
	type orphan struct {
		flowID int
		token  string
	}
	var hits []orphan
	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.flowID, &o.token); err != nil {
			rows.Close()
			e.logger.ErrorContext(ctx, "Wedge sweep: scanning orphaned subgraph child", "shard", shard, "error", err)
			return
		}
		hits = append(hits, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		e.logger.ErrorContext(ctx, "Wedge sweep: iterating orphaned subgraph children", "shard", shard, "error", err)
		return
	}

	for _, o := range hits {
		e.logger.ErrorContext(ctx, "Wedge sweep: terminating orphaned subgraph child whose parent is terminal",
			"shard", shard, "childFlow", keys.CorrelationID(shard, o.flowID))
		if rerr := e.terminateOrphanedSubtree(ctx, shard, o.flowID, o.token); rerr != nil {
			e.logger.ErrorContext(ctx, "Wedge sweep: terminating orphaned subgraph child", "shard", shard, "childFlow", keys.CorrelationID(shard, o.flowID), "error", rerr)
			continue
		}
		e.metricStepUnwedged(ctx, "orphaned_child")
	}
}

// terminateOrphanedSubtree terminates an orphaned subgraph child and its own non-terminal descendants, sharing
// the public Terminate's transaction (terminateSubtree). No surgraph up-walk is needed - and none exists: the
// ancestor chain is already terminal, which is precisely why the child is orphaned. A child that terminalized
// concurrently between the sweep's SELECT and this write is a no-op, the outcome the sweep wanted anyway.
func (e *Engine) terminateOrphanedSubtree(ctx context.Context, shard int, childFlowID int, childFlowToken string) error {
	const reason = "parent flow terminated (orphan recovery)"
	return errors.Trace(e.terminateSubtree(ctx, shard, childFlowID, childFlowToken, reason, false, ""))
}

// recoverOrphanedFlows repairs a `running` flow that is stranded - every step terminal, and a step whose transition
// never committed and whose lease has lapsed - by rewinding that step.
//
// That shape is what a worker leaves when it dies between a step's completion write and its transition
// transaction: the step is `completed`, so lease recovery (which resets `running` rows only) cannot see it, and
// nothing else will ever advance the flow. The completion write sets advancing=1, and every transition transaction
// that commits while the flow stays live clears it, so advancing=1 on a completed step of a running flow means
// exactly "this step's transition never committed". The repair rewinds such a step to `pending` - the same rewind
// the processStep recovery defer performs for a worker that is still alive - and dispatch REPLAYS it: the claim
// sees the flag, skips the task, and commits the transition from the output and disposition the completion write
// recorded. The task does not run again, and the transition is the one the dead worker would have committed. No
// transition logic is duplicated here, and a flow with no flagged step is never touched, so a false positive cannot
// double-advance a flow.
//
// THE LEASE IS THE ONLY WAIT, and the repair runs on every recovery tick, so a dead worker's completed step is
// recovered within `lease + recoveryInterval` - the bound lease recovery gives a claimed step. Do not add an age
// guard on step activity to the repair: a step mid-transition holds an unexpired lease, so the scan already skips it,
// and a worker that keeps its step past its lease without dying is fenced rather than raced - the scan skips a flow
// whose flagged step still holds an unexpired lease, and the rewind bumps lease_seq, so a worker that outlived its
// lease rolls its transition back at its fenced step-row write instead of committing successors beside the
// re-dispatch's. The step is then replayed, not re-run, so a false positive costs one redone transition and never a
// second run of the task. Reaching one takes a task that overran its budget, a transition stalled past the
// leaseMargin, or the database clock jumping forward by more than the leaseMargin: the completion write leaves the lease as the claim set it, so a task that returns within its budget
// reaches its transition with leaseMargin left, and the two waits that can outlast that - persist's retry backoff and
// the cohort stripe - extend the lease first. The rewind runs behind a write-first lock on the flow row, guarded on
// `running`, so a Terminate landing between the scan and the rewind wins and the terminal flow is left as it is.
//
// A stranded flow with no flagged step is a bug signal - some other path stranded it - logged at error level and
// counted by dwarf_flows_orphaned, and not repaired. That verdict is re-taken under the flow-row lock, so a flow
// that went terminal, was repaired by a peer's sweep, or moved on after the unlocked scan is not miscounted. The
// ALARM keeps an age guard, and only the alarm: it fires only once no step of the flow has been touched for
// orphanFlowThreshold, so a transient all-terminal state nobody has named is not reported as a bug. It is raised
// only when alarm is set - the heavy pass, every heavyRepairEvery ticks - so an unrepairable flow is counted at the
// cadence the alarm always had rather than on every tick. The guard reads
// dwarf_steps.updated_at, which moves on every step transition; dwarf_flows.updated_at is frozen at the flow's
// go-`running` time and says nothing about progress.
//
// "All steps terminal" (NOT EXISTS a non-terminal step) excludes every legitimate long-wait, because each holds a
// non-terminal step: a task mid-execution (`running`, even for 10 minutes with no DB activity), a sleep/retry
// backoff (`pending`), a parked subgraph caller (`running`+parked), a human (`interrupted`), and a live fan-out
// sibling.
//
// Where running flows are a small fraction of the table, the scan probes each of them through
// idx_dwarf_steps_flow_id and costs running flows x their steps: measured on PostgreSQL 18 at ~0.3s for 20,000
// running flows of 11 steps over 5.2M step rows. Where most flows are running, the planner hashes all of
// dwarf_steps instead (~4.7s at 2.2M rows, cold). If that ever matters, a stranded linear flow's step_id points at
// its flagged step, a single primary-key probe per running flow (~65ms on the first data set).
func (e *Engine) recoverOrphanedFlows(ctx context.Context, db *sequel.DB, shard int, alarm bool) {
	rows, err := db.QueryContext(ctx,
		"SELECT f.flow_id, f.workflow_url FROM dwarf_flows f"+
			" WHERE f.status='"+workflow.StatusRunning+"'"+
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"'))"+
			// A flagged step whose lease is unexpired is still its worker's; the flow is left until it lapses.
			" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s3 WHERE s3.flow_id=f.flow_id AND s3.status='"+workflow.StatusCompleted+"' AND s3.advancing=1 AND s3.lease_expires > NOW_UTC())",
	)
	if err != nil {
		e.logger.ErrorContext(ctx, "Orphan sweep: querying running flows", "shard", shard, "error", err)
		return
	}
	type orphan struct {
		flowID      int
		workflowURL string
	}
	var hits []orphan
	for rows.Next() {
		var o orphan
		if err := rows.Scan(&o.flowID, &o.workflowURL); err != nil {
			rows.Close()
			e.logger.ErrorContext(ctx, "Orphan sweep: scanning running flow", "shard", shard, "error", err)
			return
		}
		hits = append(hits, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		e.logger.ErrorContext(ctx, "Orphan sweep: iterating running flows", "shard", shard, "error", err)
		return
	}

	for _, o := range hits {
		// The scan read without locks, so its verdict is re-taken under the flow-row lock. Since the scan the flow may
		// have gone terminal, a flagged step may have stopped qualifying (its worker's lease extended), or the flow
		// may have stopped being stranded at all - a peer's sweep rewound the step, or a slow worker's transition
		// committed. Only a flow still running with every step terminal and none flagged is an unrepairable orphan.
		var rewound, held int64
		var locked bool
		err := db.Transact(ctx, func(tx *sequel.Tx) error {
			rewound, held, locked = 0, 0, false
			n, err := lockFlowRows(ctx, tx, "flow_id=? AND status='"+workflow.StatusRunning+"'", o.flowID)
			if err != nil {
				return err
			}
			if n == 0 {
				return nil // terminalized since the scan
			}
			locked = true
			res, err := tx.ExecContext(ctx,
				"UPDATE dwarf_steps SET status=?, lease_seq=lease_seq+1, not_before=NOW_UTC(), lease_expires=NOW_UTC(), updated_at=NOW_UTC()"+
					" WHERE flow_id=? AND status='"+workflow.StatusCompleted+"' AND advancing=1 AND lease_expires<=NOW_UTC()",
				workflow.StatusPending, o.flowID,
			)
			if err != nil {
				return errors.Trace(err)
			}
			rewound, _ = res.RowsAffected()
			if rewound > 0 || !alarm {
				return nil
			}
			// Rare path only. A flagged step still under lease is its worker's and is repaired once that lapses; a
			// non-terminal step means the flow is moving again; a step touched within orphanFlowThreshold holds the
			// alarm off (see above).
			return errors.Trace(tx.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND (status IN ('"+workflow.StatusCreated+"', '"+workflow.StatusPending+"', '"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')"+
					" OR (status='"+workflow.StatusCompleted+"' AND advancing=1) OR updated_at >= DATE_ADD_MILLIS(NOW_UTC(), ?))",
				o.flowID, -e.orphanFlowThreshold.Milliseconds(),
			).Scan(&held))
		})
		// Token-free correlation id: this is an operator alarm, not a capability. See "Tracing".
		switch {
		case err != nil:
			e.logger.ErrorContext(ctx, "Orphan sweep: rewinding stranded steps", "shard", shard, "flow", keys.CorrelationID(shard, o.flowID), "error", err)
		case rewound > 0:
			e.logger.WarnContext(ctx, "Orphan sweep: replaying steps whose transition never committed",
				"flow", keys.CorrelationID(shard, o.flowID), "steps", rewound)
			e.metricStepsRecovered(ctx, int(rewound))
		case alarm && locked && held == 0:
			e.logger.ErrorContext(ctx, "Orphaned flow: running with all steps terminal and no successor",
				"flow", keys.CorrelationID(shard, o.flowID))
			e.metricOrphanedFlow(ctx, o.workflowURL)
		}
	}
}
