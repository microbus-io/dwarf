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
	"strings"

	"github.com/microbus-io/dwarf/internal/cancelmarker"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// failAndReturn fails the step and returns the error processStep should surface. If failStep's own
// transaction errored - most plausibly its lock-contention retries exhausting under a deadlock storm,
// which leaves the step still leased-and-running because the fail UPDATE never committed - that error is
// returned so processStep's recovery defer sees a lock-contention error and rewinds the step
// (running→pending) for immediate re-dispatch, instead of leaving it stranded running until lease expiry
// (budget+leaseMargin, minutes past the poll cadence). When failStep succeeds the original failure reason
// is returned unchanged (the step is already terminal, so the defer's guarded reset is a no-op). When the
// dispatch's lease was re-granted to a peer (failStep's fenced step UPDATE matched zero rows), nil is
// returned so processStep abandons quietly - the peer owns the step and will settle it. All failStep call
// sites in processStep go through this so the defer covers every fail path uniformly.
func (e *Engine) failAndReturn(ctx context.Context, shardNum int, stepID int, leaseSeq int, flowID int, flowToken string, taskErr error, taskName string) error {
	fenced, ferr := e.failStep(ctx, shardNum, stepID, leaseSeq, flowID, flowToken, taskErr, taskName, heldEither)
	if ferr != nil {
		return errors.Trace(ferr)
	}
	if fenced {
		return nil
	}
	return errors.Trace(taskErr)
}

// stepHeld is the state a worker holds its dispatched step in, as the predicate failStep fences on. A persist site
// passes the state its write STARTS from, never the broad heldEither: a write that committed but reported an error
// has moved the step out of that state, so failStep matches nothing and abandons, and lease recovery settles the
// landed outcome without running the task again. With heldEither a landed completion (`completed`, advancing=1) or
// transition (`completed`, advancing=0) still matches, and failStep fails a step whose outcome is stored.
//
// Flow completion writes nothing to the step row, so the row cannot show it landed: those sites pass
// heldAdvancing, and an ambiguous flow completion still fails the step. The flow stays completed, and failFlowTx,
// finding it terminal, neither delivers a failure to a parent nor counts it again.
type stepHeld string

const (
	// heldEither is for a failure the worker detects itself, with no write in doubt.
	heldEither stepHeld = "status IN ('" + workflow.StatusRunning + "', '" + workflow.StatusCompleted + "')"
	// heldRunning: the completion write, an interrupt, a subgraph spawn (which parks the step) and a Retry rewind.
	heldRunning stepHeld = "status='" + workflow.StatusRunning + "' AND parked=0"
	// heldAdvancing: a transition and a fan-in arrival, which clear advancing.
	heldAdvancing stepHeld = "status='" + workflow.StatusCompleted + "' AND advancing=1"
	// heldRerouting: the Cancel-covered reroute, which sets advance_on_error.
	heldRerouting stepHeld = heldAdvancing + " AND advance_on_error=0"
)

// failStep handles a task failure. It returns fenced=true (with a nil error) when the dispatch's lease was
// re-granted to a peer, or the step is no longer in the held state - its fenced step UPDATE matched zero rows, so it
// wrote nothing and the caller must abandon quietly rather than fail a flow the peer is healthily re-executing.
func (e *Engine) failStep(ctx context.Context, shardNum int, stepID int, leaseSeq int, flowID int, flowToken string, taskErr error, taskName string, held stepHeld) (fenced bool, err error) {
	if e.seams.Enabled() && e.seams.IsFault(engineseams.Join(engineseams.FaultFailStepErr, taskName)) {
		return false, errors.New("injected fault: " + engineseams.FaultFailStepErr)
	}
	db, err := e.db.Shard(shardNum)
	if err != nil {
		return false, errors.Trace(err)
	}

	// A subgraph child's failure is surfaced to the parent's flow.Subgraph call (via the parked caller step,
	// carried in subgraph_error) rather than notified directly - but only when the child flow ACTUALLY fails,
	// which for a child with a fan-out is after its cohort fully resolves (the same cohort accounting below
	// that governs a top-level flow), never eagerly on the first branch error. Failing the child eagerly
	// while a sibling branch is still live would strand that sibling and any subgraph descendants it parked
	// on: the interrupt/resume/terminate tree walks all skip a terminal flow, so nothing could ever release
	// them. parentStepID>0 iff this flow is a subgraph child.
	parentStepID, lostLabel, err := e.dynamicSubgraphParent(ctx, db, flowID)
	if err != nil {
		return false, errors.Trace(err)
	}

	var stepLineageID int
	err = db.QueryRowContext(ctx,
		"SELECT lineage_id FROM dwarf_steps WHERE step_id=?",
		stepID,
	).Scan(&stepLineageID)
	if err != nil {
		return false, errors.Trace(err)
	}

	// The message lands in a text column, so it is sanitized: a control byte echoed out of a driver error (a
	// NUL, which Postgres rejects in `text` exactly as it does in `jsonb`) would kill the very write that is
	// supposed to be the clean one, and failOnPersistError would misread that as an unreachable database.
	errMsg := sanitizeErrorMessage(taskErr.Error())
	// A cancellation settles the step as cancelled. Either value differs from the running/completed row it
	// replaces, which is what keeps the RowsAffected fence below honest on MySQL.
	stepCancelled := workflow.IsCancelled(taskErr)
	stepStatus := workflow.StatusFailed
	if stepCancelled {
		stepStatus = workflow.StatusCancelled
	}
	failFlow := false
	var loss flowLoss
	// Serialize a failing BRANCH's arrival through the same per-peer stripe the success path uses (keyed on
	// its cohort's spawn, stepLineageID), so a failed sibling parks on the stripe holding no connection
	// rather than piling onto the spawn row's write lock. A trunk step (lineage 0) fails the flow directly,
	// with no cohort to contend, so it takes nothing. Only this bottom level is locked; propagateCohortFailure's
	// ancestor bumps walk UP and stay DB-serialized, which is what keeps the stripe leaf-only and cycle-free.
	// A simple defer suffices here: nothing in this transaction or its post-commit tail re-locks the stripe.
	//
	// Parked on holding NO TURN, the same as the success path: every caller of this reaches it holding one,
	// and a turn held across a lock is the occupancy the stripe exists to remove, one level up. The wait keeps
	// the step's lease alive; if it finds the step taken back, this worker no longer owns it - fenced.
	if stepLineageID != 0 {
		stripe := &e.cohortLocks[cohortLockStripe(shardNum, stepLineageID)]
		held := false
		yieldHeldTurn(ctx, func() {
			held = e.lockCohortStripe(ctx, stripe, db, shardNum, stepID, leaseSeq)
		})
		if !held {
			return true, nil
		}
		defer stripe.unlock()
	}
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		fenced = false
		// A trunk step (lineage_id==0) has no concurrent sibling, so its failure fails the flow at once.
		// A fan-out branch (lineage_id!=0) instead defers to cohort accounting below: siblings run to
		// completion and the flow fails only once the whole cohort arrives with cohort_failures>0.
		failFlow = stepLineageID == 0
		flowCancelled := stepCancelled
		loss = flowLoss{}
		// Fence the fail on BOTH our lease generation AND the state the worker holds the step in (held): `running` (a
		// task error, or the step-completion write itself failing) or `completed` (a transition/eval failure that
		// failOnPersistError fails the already-completed step for, to escape the re-execution loop). The lease fence
		// alone guards the zombie "late error → healthy-flow kill" hazard; the status guard - which every sibling post-
		// execution write carries and this one was missing - closes the rest. Without it a step a racing Terminate just
		// terminated (terminateSubtree does NOT bump lease_seq, so it still matches our generation) is rewritten
		// terminated→failed, violating step immutability and seeding a phantom branch failure a later Fork re-derives
		// from step status; and a step recovery reset to `pending` (also lease_seq-preserving) would be terminalized
		// out from under the peer re-claiming it. This is the first write in the transaction, so a zero-row match means
		// nothing was written - commit the empty tx and report fenced so the caller abandons without failing a flow
		// that is terminated or that a peer is re-running.
		res, uerr := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET status=?, parked=?, error=?, updated_at=NOW_UTC() WHERE step_id=? AND "+string(held)+" AND lease_seq=?",
			stepStatus, parkedNone, errMsg, stepID, leaseSeq,
		)
		if uerr != nil {
			return errors.Trace(uerr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			fenced = true
			return nil
		}
		if !failFlow {
			var err error
			failFlow, flowCancelled, err = e.propagateCohortFailure(ctx, tx, stepLineageID, stepCancelled)
			if err != nil {
				return errors.Trace(err)
			}
		}
		if failFlow {
			var err error
			loss, err = e.failFlowTx(ctx, tx, shardNum, flowID, flowToken, flowCancelled, taskErr, parentStepID, stepID)
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return false, errors.Trace(err)
	}
	if fenced {
		// Lease re-granted to a peer; we wrote nothing. The peer owns this step and settles it.
		return true, nil
	}
	// The step is now failed (or cancelled) regardless of whether the whole flow fails - count it.
	e.metricStepExecuted(ctx, taskName, stepStatus, shardNum)

	if !failFlow || loss.status == "" {
		return false, nil
	}
	e.endLostFlow(ctx, shardNum, flowID, flowToken, lostLabel, loss)
	return false, nil
}

// flowLoss is what failFlowTx decided inside its transaction, for endLostFlow to act on after the commit.
type flowLoss struct {
	status           string // failed or cancelled; empty when no flow was failed
	parentStepID     int    // the parked caller step of a subgraph child, else 0
	reDispatchParent bool   // whether that caller step must be offered for re-dispatch
}

// failFlowTx terminalizes a flow as a loss inside tx: it records the flow's final state, status and error and, for
// a subgraph child (parentStepID != 0), delivers the failure to the parked caller step. It is shared by failStep
// and by a completing last arrival that resolves a cohort carrying failures, so the two cannot disagree on what a
// lost flow looks like. lastLoss is the resolving step's own error, or nil for a completing arrival. stoppedStepID
// is the step recorded as the one the flow stopped at: the failing step, or 0 for a completing arrival, which is not
// the loss.
func (e *Engine) failFlowTx(ctx context.Context, tx sequel.Executor, shardNum, flowID int, flowToken string, cancelled bool, lastLoss error, parentStepID int, stoppedStepID int) (flowLoss, error) {
	loss := flowLoss{status: workflow.StatusFailed, parentStepID: parentStepID}
	if cancelled {
		loss.status = workflow.StatusCancelled
	}
	lossErr := e.flowLossError(ctx, tx, flowID, cancelled, lastLoss)
	finalStateJSON, _, err := e.computeFinalState(ctx, tx, shardNum, flowID, flowToken)
	if err == nil && e.seams.IsFault(engineseams.FaultFailFlowFinalState) {
		err = errors.New("injected fault: " + engineseams.FaultFailFlowFinalState)
	}
	if err != nil {
		// A fuse that can itself be poisoned is not a fuse. failStep is the ONLY way out of a step whose outcome
		// cannot be persisted (failOnPersistError uses it as the clean, payload-free write that tells a permanent
		// failure from an unreachable database) - and computeFinalState is the one part of it that reads other
		// steps' payloads. If a value that cannot be read back had already landed in a row, this merge would die
		// the same way as the write being failed, failStep would report an error, and the classifier would misread
		// it as "the database is down" - leaving the step to re-execute forever. So the flow still terminalizes,
		// with an empty final_state and the error logged.
		e.logger.ErrorContext(ctx, "Computing final_state while failing a flow; terminalizing with empty state",
			"flow", keys.CorrelationID(shardNum, flowID), "error", err)
		finalStateJSON = []byte("{}")
	}
	res, uerr := tx.ExecContext(ctx,
		"UPDATE dwarf_flows SET final_state=?, status=?, error=?, stopped_step_id=?, updated_at=NOW_UTC(), touch=1-touch WHERE flow_id=? AND status NOT IN ("+terminalStatusesSQL+")",
		finalStateJSON, loss.status, flowErrorText(cancelled, lossErr), stoppedStepID, flowID,
	)
	if uerr == nil {
		if n, _ := res.RowsAffected(); n == 0 {
			// The flow was already terminal, and whatever ended it settled its parent and counted it. A flow
			// completion whose commit landed with its acknowledgement lost reaches here through failStep: delivering
			// a failure would hand the parent a loss for a child that completed. The new status always differs from
			// a non-terminal one, so RowsAffected is honest on MySQL.
			return flowLoss{}, nil
		}
	}
	if parentStepID != 0 {
		loss.reDispatchParent, err = e.deliverFlowFailureToParent(ctx, tx, parentStepID, lossErr)
		if err != nil {
			return flowLoss{}, errors.Trace(err)
		}
	}
	return loss, nil
}

// endLostFlow is failFlowTx's post-commit tail. It counts the flow ended - a subgraph child included, since every
// flow was counted as started - and wakes any Await on it, including on a subgraph child's key (legal read-only
// introspection), whose failure otherwise travels only to its parent's flow.Subgraph call.
func (e *Engine) endLostFlow(ctx context.Context, shardNum, flowID int, flowToken string, label flowLabel, loss flowLoss) {
	e.metricFlowEnded(ctx, label.workflowURL, loss.status, label.priority, shardNum)
	e.logger.InfoContext(ctx, "Flow status transition", "flow", keys.CorrelationID(shardNum, flowID), "to", loss.status)
	e.signalStop(ctx, keys.New(shardNum, flowID, flowToken), loss.status)
	if loss.reDispatchParent {
		e.enqueueStep(ctx, shardNum, loss.parentStepID)
	}
}

// propagateCohortFailure bumps a spawn step's cohort_arrivals and cohort_failures.
// cancelled says whether the loss arriving is a cancellation, and is counted in cohort_cancellations. When a
// cohort resolves, its own loss - carried to the enclosing cohort, or to the flow - is a cancellation only if
// every failure in it was one, so a single real error anywhere below makes the whole resolution a failure.
// Returns whether the flow fails, and if so whether it resolves cancelled.
func (e *Engine) propagateCohortFailure(ctx context.Context, tx sequel.Executor, spawnStepID int, cancelled bool) (failFlow bool, flowCancelled bool, err error) {
	current := spawnStepID
	for {
		cancelledInc := 0
		if cancelled {
			cancelledInc = 1
		}
		_, err := tx.ExecContext(ctx,
			"UPDATE dwarf_steps SET cohort_arrivals = cohort_arrivals + 1, cohort_failures = cohort_failures + 1, cohort_cancellations = cohort_cancellations + ? WHERE step_id=?",
			cancelledInc, current,
		)
		if err != nil {
			return false, false, errors.Trace(err)
		}
		var arrivals, size, failures, cancellations, lineageID int
		err = tx.QueryRowContext(ctx,
			"SELECT cohort_arrivals, cohort_size, cohort_failures, cohort_cancellations, lineage_id FROM dwarf_steps WHERE step_id=?",
			current,
		).Scan(&arrivals, &size, &failures, &cancellations, &lineageID)
		if err != nil {
			return false, false, errors.Trace(err)
		}
		if arrivals < size {
			return false, false, nil
		}
		cancelled = cancellations == failures
		if lineageID == 0 {
			return true, cancelled, nil
		}
		current = lineageID
	}
}

// flowErrorText is the error column's text for a flow resolving as a loss: the error for a failed flow, and
// nothing for a cancelled one, whose explanation is its cancel reason - as a terminated flow's is. Every reader
// then agrees that a cancelled flow carries no error.
func flowErrorText(cancelled bool, lossErr error) string {
	if cancelled {
		return ""
	}
	return sanitizeErrorMessage(lossErr.Error())
}

// flowLossError picks the error a flow resolving as a loss records, and delivers to its surgraph caller. A
// cancelled flow reports the cancellation. A failed flow reports a real failure even when the loss that
// resolved it was a cancellation - a real error was mixed into its cohort, and a cancellation on a failed flow
// would have IsCancelled report true for it. lastLoss is the resolving step's own error, or nil when the
// resolver is a completing arrival that has none.
func (e *Engine) flowLossError(ctx context.Context, tx sequel.Executor, flowID int, cancelled bool, lastLoss error) error {
	if cancelled {
		if workflow.IsCancelled(lastLoss) {
			return lastLoss
		}
		var reason string
		tx.QueryRowContext(ctx, "SELECT cancel_reason FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&reason)
		return cancelmarker.New(strings.TrimSpace(reason))
	}
	if lastLoss != nil && !workflow.IsCancelled(lastLoss) {
		return lastLoss
	}
	var sample string
	tx.QueryRowContext(ctx,
		"SELECT error FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusFailed+"' AND error!='' ORDER BY step_id LIMIT_OFFSET(1, 0)",
		flowID,
	).Scan(&sample)
	sample = strings.TrimSpace(sample)
	if sample == "" {
		// No step failed. In a fork, a branch a Terminate stopped in the origin is a loss of its own (Fork's
		// recount), and a live flow never fails from one: its Terminate already ended it.
		var terminated int
		tx.QueryRowContext(ctx,
			"SELECT 1 FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusTerminated+"' ORDER BY step_id LIMIT_OFFSET(1, 0)", // ORDER BY: SQL Server's OFFSET/FETCH requires one
			flowID,
		).Scan(&terminated)
		if terminated == 1 {
			sample = "a fan-out branch was terminated before it completed; to re-run it, fork at or before a fan-out whose branches include every terminated one"
		} else {
			sample = "cohort failed"
		}
	}
	return errors.New("%s", sample)
}
