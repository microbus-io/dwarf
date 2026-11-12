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
	"encoding/json"
	"slices"
	"strings"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
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
// and return nil.
var errLeaseFenced = errors.New("dispatch lease fenced")

// errTransitionFlowTerminal is an in-transaction sentinel: the transition's flow-row lock grab found the flow
// already terminal, after the successor INSERTs batched with it had run. Rolling them back leaves the transition
// the same clean no-op as a bail before any write, so the transaction's caller maps it to nil.
var errTransitionFlowTerminal = errors.New("flow terminal at transition")

// maxGotoTargetLen is the width of dwarf_steps.advance_goto, which matches task_name's.
const maxGotoTargetLen = 512

// handleInterrupt pauses a flow for external input.
func (e *Engine) handleInterrupt(ctx context.Context, d *dispatch, interruptPayload workflow.State) error {
	shardNum, db, stepID, leaseSeq, changesJSON := d.shard, d.db, d.stepID, d.row.leaseSeq, d.changesJSON
	flowID, flowHour, flowToken, workflowURL := d.row.flowID, d.row.createdHour, d.row.flowToken, d.row.workflowURL
	chainFlowIDs, chainStepIDs, chainCompositeIDs, err := e.surgraphChain(ctx, shardNum, flowID, flowHour, flowToken)
	if err != nil {
		return errors.Trace(err)
	}

	payloadLen := 0
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
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
		// The chain is as deep as the subgraph nesting, which nothing bounds, so every chain-wide write goes out
		// inListChunk ids at a time, in one transaction. Ascending, so chunk after chunk takes its row locks in one
		// order (lockOrder); a single statement over the whole list would instead pass SQL Server's 2,100-parameter
		// ceiling at a chain of 700 flows.
		allStepIDs := lockOrder(append([]any{stepID}, chainStepIDs...))
		// diagnostics.FaultInterruptChainWrite reproduces a deadlock victim dialect-independently: the statement errors and
		// applies NOTHING, so the leaf stays `running` - the shape that must not read back as a genuine fence.
		if e.diags.Seams().IsFault(diagnostics.FaultInterruptChainWrite) {
			tx.ExecContext(ctx, "UPDATE dwarf_steps SET no_such_column=1 WHERE step_id=?", stepID)
		} else {
			for chunk := range slices.Chunk(allStepIDs, inListChunk) {
				ph, ids := inList(chunk)
				tx.ExecContext(ctx,
					"UPDATE dwarf_steps SET changes=CASE WHEN step_id=? THEN ? ELSE changes END, changes_bytes=CASE WHEN step_id=? THEN ? ELSE changes_bytes END, interrupt_done=CASE WHEN step_id=? THEN 1 ELSE interrupt_done END, status=?, parked=?, lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id IN ("+ph+") AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')",
					append([]any{stepID, changesJSON, stepID, len(changesJSON), stepID, workflow.StatusInterrupted, parkedNone}, ids...)...,
				)
			}
		}

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
		// contended write and abandon: a leaf we still own, abandoned with no interrupt, no error and no
		// retry, its flow stuck `running` until the lease lapses (minutes) - permanently if a Resume lands in
		// that window, since the lost branch never marked its caller step interrupted. sequel's QueryRow
		// reports the transaction's error like every other statement method, so the check below catches it and
		// the write is retried. Observed on SQL Server, where two parallel subgraph callers interrupting at once deadlock:
		// moving a step running->interrupted deletes keys from the status-filtered dwarf_steps indexes,
		// so even disjoint row sets cycle. Never let this error go unchecked, and never decide the fence ahead
		// of it.
		var curLeaseSeq int
		var curStatus string
		if serr := tx.QueryRowContext(ctx, "SELECT lease_seq, status FROM dwarf_steps WHERE step_id=?", stepID).Scan(&curLeaseSeq, &curStatus); serr != nil {
			return errors.Trace(serr)
		}
		// diagnostics.FaultInterruptStaleWrite forces the generation to look re-granted (as a real zombie would, after a peer
		// re-claimed the leaf), so the fence trips and the WHOLE transaction rolls back - undoing the ancestor
		// re-park - and the worker abandons. The only fence in the engine that rolls back rather than no-ops.
		if e.diags.Seams().IsFault(diagnostics.FaultInterruptStaleWrite) {
			curLeaseSeq = leaseSeq + 1
		}
		if curLeaseSeq != leaseSeq || curStatus != workflow.StatusInterrupted {
			return errLeaseFenced
		}

		if interruptPayload.Len() > 0 {
			payloadJSON, _ := json.Marshal(interruptPayload)
			if err := e.checkPayloadSize("interrupt payload", payloadJSON); err != nil {
				return err
			}
			payloadLen = len(payloadJSON)
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
			case "mysql", "mariadb":
				emptyGuard = "CAST(interrupt_payload AS CHAR)='{}'"
			case "mssql":
				emptyGuard = "interrupt_payload=0x7B7D"
			}
			for chunk := range slices.Chunk(allStepIDs, inListChunk) {
				ph, ids := inList(chunk)
				tx.ExecContext(ctx,
					"UPDATE dwarf_steps SET interrupt_payload=?, interrupt_payload_bytes=? WHERE step_id IN ("+ph+") AND "+emptyGuard,
					append([]any{payloadJSON, len(payloadJSON)}, ids...)...,
				)
			}
		}

		// Each chain flow records the step it stopped at - the leaf in the leaf's flow, the parked caller in each
		// ancestor (stoppedAt pairs each chain flow with its step) - unless it is already interrupted, where the first
		// interrupt keeps its record. stopped_step_id is assigned BEFORE status: MySQL evaluates SET assignments left to
		// right, so a CASE placed after status=? would read the new status. A chunk binds two values per flow for the
		// CASE and one for the IN list, about 1,500 at inListChunk flows.
		// chainFlowIDs[0] is the leaf's flow and chainFlowIDs[i] the flow of the caller chainStepIDs[i-1].
		stoppedAt := make(map[int]any, len(chainFlowIDs))
		for i, chainFlowID := range chainFlowIDs {
			if i == 0 {
				stoppedAt[chainFlowID.(int)] = stepID
			} else {
				stoppedAt[chainFlowID.(int)] = chainStepIDs[i-1]
			}
		}
		for chunk := range slices.Chunk(lockOrder(chainFlowIDs), inListChunk) {
			ph, ids := inList(chunk)
			args := make([]any, 0, 2*len(chunk)+1+len(ids))
			for _, chainFlowID := range chunk {
				args = append(args, chainFlowID, stoppedAt[chainFlowID.(int)])
			}
			args = append(args, workflow.StatusInterrupted)
			args = append(args, ids...)
			tx.ExecContext(ctx,
				"UPDATE dwarf_flows SET stopped_step_id=CASE WHEN status='"+workflow.StatusRunning+"' THEN CASE flow_id"+strings.Repeat(" WHEN ? THEN ?", len(chunk))+" ELSE stopped_step_id END ELSE stopped_step_id END, status=?, updated_at=NOW_UTC(), touch=1-touch"+
					" WHERE flow_id IN ("+ph+") AND status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')",
				args...,
			)
		}
		return nil
	})
	if err != nil {
		// A fence (errLeaseFenced) included: the peer that owns the step re-runs the interrupt, and persist
		// abandons this one quietly, so the caller neither counts nor signals an interrupt that did not land.
		return errors.Trace(err)
	}

	e.metricStateWriteBytes(ctx, workflowURL, "changes", len(changesJSON))
	e.metricStateWriteBytes(ctx, workflowURL, "interrupt_payload", payloadLen)
	for _, compositeID := range chainCompositeIDs {
		e.signalStop(ctx, compositeID, workflow.StatusInterrupted)
	}
	return nil
}

// errCallerGone rolls back a subgraph spawn whose caller step is no longer running under this worker's lease -
// terminated, or re-claimed by a peer - so no child is created for it.
var errCallerGone = errors.New("subgraph caller is no longer running")

// dispatchURLOf resolves a task name in the graph to its dispatch URL.
func dispatchURLOf(graph *workflow.Graph, name string) string {
	if name == workflow.End {
		return name
	}
	if u := graph.URLOf(name); u != "" {
		return u
	}
	return name
}
