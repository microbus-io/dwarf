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
	"strconv"
	"strings"

	"github.com/microbus-io/sequel"
)

// Fault and checkpoint names for the engine's test-only instrumentation seams. The mechanism (the registry,
// its locking, the production-inert gate, scoping, and the Wait/Break/Resume composition) lives in the
// seamster package and is documented there. The engine embeds one seamster.Seamster as e.seams (enabled under
// testing.Testing() in NewEngine) and consults it directly: e.seams.IsFault / Checkpoint / Inject / Wait /
// Break / Resume. The names live here, next to the code they mark, so the valid set is discoverable and a test
// cannot arm a fault or checkpoint no site consumes. deliverFailureLost is the one consult that needs a DB read.

// --- Fault injection ---
const (
	// Scoped by task name (the graph node name passed to ExecuteTask):
	faultExecuteTask      = "executeTask"      // ExecuteTask returns a synthetic error (as if the task failed)
	faultPanicExecuteTask = "panicExecuteTask" // ExecuteTask panics (exercises host-call panic isolation)

	// Scoped by workflow URL:
	faultLoadGraph = "loadGraph" // LoadGraph returns a synthetic error

	// Scoped by task name; force the persistence/transition steps of a dispatch to fail:
	faultTransitionCommit   = "transitionCommit"   // the post-completion transition transaction errors
	faultCompleteFlowCommit = "completeFlowCommit" // the flow-completion transaction errors
	faultContention         = "contention"         // a dispatch transaction returns a lock-contention error
	faultLeaseStaleWrite    = "leaseStaleWrite"    // the completion write carries a stale lease_seq (zombie)
	faultPersistErr         = "persistErr"         // the step-completion write returns a non-contention database error (consumed per attempt, so InjectN sets how many attempts fail)
	faultSubgraphSpawnErr   = "subgraphSpawnErr"   // createSubgraphFlow errors after the caller step parked

	// Scoped by workflow URL of the subgraph child:
	faultSubgraphReviveLost = "subgraphReviveLost" // completeSurgraphFlow skips reviving the parked caller

	// Process-wide, consumed per attempt (InjectN sets how many attempts fail):
	faultCompleteSurgraphErr = "completeSurgraphErr" // completeSurgraphFlow returns a non-contention database error

	// Scoped by signal op (enqueue / statusChange):
	faultSignalPeersPanic = "signalPeersPanic" // the host SignalPeers call panics (host-call panic isolation)

	// Process-wide (no scope):
	faultInterruptStaleWrite = "interruptStaleWrite" // handleInterrupt's in-tx leaf lease_seq read is forced to mismatch (zombie)
	faultInterruptChainWrite = "interruptChainWrite" // handleInterrupt's combined chain UPDATE fails and applies nothing (deadlock victim)
	faultDropSignalStop      = "dropSignalStop"      // signalStop delivers nothing (lost terminal wake)
	faultDropDoorbell        = "dropDoorbell"        // the enqueue doorbell is dropped (lost wake)
	faultRecoveryResetErr    = "recoveryResetErr"    // the processStep recovery defer's own reset UPDATE errors
	faultReapMidTree         = "reapMidTree"         // the reaper errors after deleting steps, before flows
	faultReapSelectErr       = "reapSelectErr"       // the reaper's due-root SELECT errors
	faultRefillScanErr       = "refillScanErr"       // the refiller's priority-band scan errors
	faultSlowPoolPush        = "slowPoolPush"        // recomputePools stalls between reading R and pushing the derived sizes
	faultPollSizingErr       = "pollSizingErr"       // the poll's pending-sizing query is treated as errored
	faultDeliverFailureErr   = "deliverFailureErr"   // deliverFlowFailureToParent drops the parked-caller re-dispatch (lost delivery); unscoped, or scoped by the parked caller's task name for per-level control
	faultCancelCommit        = "cancelCommit"        // the Cancel transaction errors
	faultResumeCommit        = "resumeCommit"        // the Resume transaction errors
	faultForkCommit          = "forkCommit"          // the Fork clone transaction errors
)

// deliverFailureLost consults faultDeliverFailureErr, the seam that simulates a subgraph child's
// failure-delivery to its parked caller being lost (wedging the caller for recoverWedgedSubgraphParks to
// backstop). It fires either unscoped (the single-level lost-delivery pin) OR scoped to the parked caller's task name,
// so a multi-level test drops the delivery at one chosen level while the other level's succeeds - each caller
// task's FIRST delivery is lost and its sweep-driven re-delivery goes through, letting a depth-N tree wedge
// (and unwedge) at every level. Inert in production: the enabled gate short-circuits before the scope
// SELECT, so a live binary runs neither the query nor any consult.
func (e *Engine) deliverFailureLost(ctx context.Context, tx sequel.Executor, parentStepID int) bool {
	if !e.seams.Enabled() {
		return false
	}
	if e.seams.IsFault(faultDeliverFailureErr) {
		return true
	}
	var taskName string
	if err := tx.QueryRowContext(ctx, "SELECT task_name FROM dwarf_steps WHERE step_id=?", parentStepID).Scan(&taskName); err != nil {
		return false
	}
	return e.seams.IsFault(faultDeliverFailureErr, strings.TrimSpace(taskName))
}

// --- Test-only statement counters ---

// The flow-row-write counter is a COUNTING checkpoint (seamster's Visits), not a rendezvous: countFlowRowWrite
// fires checkpointFlowRowWrite scoped to the flow at each dwarf_flows UPDATE the transition transaction issues,
// and flowRowWriteCount reads the accrued Visits count. No test arms Wait/Break on these flow-scoped names, so
// Checkpoint just increments and returns.
//
// It exists for a single pin, and that pin guards a performance property no correctness test can see: a
// NON-FINAL cohort arrival must issue ZERO flow-row statements. Grabbing the flow row for every arrival
// serializes an entire cohort on one row (measured at fan-out width 64: 20 of 43 active backends queued on
// that one statement), so the grab is deferred to the arrival that actually resolves the cohort. Nothing about
// the flow's OUTCOME changes if someone reintroduces a per-arrival flow-row write - the fan-in still fires and
// every existing fixture still passes - it just costs ~20% throughput silently. Hence a counter rather than a
// state assertion.
//
// Counts are per flow and cumulative across the flow's whole life (Visits never resets), so a test reads the
// delta it cares about. A Transact contention retry re-runs the closure and legitimately re-counts, so a test
// asserting an exact count must be single-worker and contention-free.

// flowRowWriteCheckpoint is the flow-scoped name of the flow-row-write counting checkpoint. Scoped by flow id
// so concurrent flows count independently, mirroring the old per-flow map key.
func flowRowWriteCheckpoint(flowID int) string {
	return "flowRowWrite:" + strconv.Itoa(flowID)
}

// countFlowRowWrite records one dwarf_flows UPDATE issued by the transition transaction for a flow. Inert in
// production: the Enabled gate short-circuits before the scoped checkpoint name is built, so a live binary
// allocates no throwaway string and consults nothing.
func (e *Engine) countFlowRowWrite(ctx context.Context, flowID int) {
	if !e.seams.Enabled() {
		return
	}
	e.seams.Checkpoint(ctx, flowRowWriteCheckpoint(flowID))
}

// flowRowWriteCount reports how many dwarf_flows UPDATEs the transition transaction has issued for a flow.
func (e *Engine) flowRowWriteCount(flowID int) int {
	return e.seams.Visits(flowRowWriteCheckpoint(flowID))
}

// --- Execution checkpoints ---
const (
	checkpointResumeBeforeFlowWrite   = "resumeBeforeFlowWrite"   // resume(), just before its transaction's flow-status gate write
	checkpointBeforeTransitionTx      = "beforeTransitionTx"      // processStep, after the step is marked completed, before the transition transaction
	checkpointAfterArrivalTx          = "afterArrivalTx"          // processStep, after a cohort arrival has committed, before the cohort resolve
	checkpointAfterCallerPark         = "afterCallerPark"         // processStep, after the subgraph caller step is parked, before createSubgraphFlow
	checkpointBeforeRetryRewind       = "beforeRetryRewind"       // processStep, before the flow.Retry rewind transaction
	checkpointBeforeCompleteFlowWrite = "beforeCompleteFlowWrite" // completeFlow(), just before its transaction's status-gate write
	checkpointBeforeDeleteWrite       = "beforeDeleteWrite"       // deleteFlow(), just before its transaction's delete-stamp/interrupted-CAS write
	checkpointBeforeReviveWrite       = "beforeReviveWrite"       // completeSurgraphFlow(), just before its transaction's caller-revive write
	checkpointBeforeRecoveryReset     = "beforeRecoveryReset"     // processStep recovery defer, just before its fenced step-reset transaction

	// Lifecycle rendezvous (fired at an event, used with Wait - not a freeze site): lets a test wait for
	// exact engine progress instead of polling status / sleeping.
	checkpointFlowStopped = "flowStopped" // signalStop(), a flow just reached a stop (completed/failed/cancelled/interrupted)
)
