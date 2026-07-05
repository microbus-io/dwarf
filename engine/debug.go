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

	"github.com/microbus-io/sequel"
)

// This file holds two test-only instrumentation seams a white-box test in this package uses to exercise
// recovery/race paths deterministically, without DB forging or timing hammers. Both are inert in production:
// every engine-side consult short-circuits on !underTest (cached from testing.Testing() in NewEngine), so a
// production engine pays a single bool read per site and neither seam can ever fire. They share faultsLock.
//
//   - Fault injection makes a site MISBEHAVE: a test arms a named fault, the engine consults it and simulates
//     an error/drop that is otherwise hard to trigger (a dropped signal, a lost revive, a commit that fails).
//   - Execution checkpoints make a site OBSERVABLE and PAUSABLE: a test rendezvouses with the engine's
//     progress (waitFor) or freezes the engine at an exact point (setBreakpoint/clearBreakpoint).

// --- Fault injection ---

// Faults are usually SCOPED: the consult site appends an identifier so a test targets one task, one workflow,
// or one flow rather than "the next thing that happens". Pass the scope straight to isFault, e.g.
// e.isFault(faultExecuteTask, taskName); a test arms the same key with faultKey,
// e.injectFault(faultKey(faultExecuteTask, "Charge")). isFault only builds the key when the fault seam is
// live (under test), so a scoped consult on a production engine allocates nothing - see isFault. A few faults
// are process-wide and take no scope: e.isFault(faultCompleteFlowCommit).
//
// Fault names live here, next to the primitives, so the valid set is discoverable and a test cannot arm a
// fault no site consumes. The corresponding consult site lives at the point it affects, calling isFault.
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
	faultSubgraphSpawnErr   = "subgraphSpawnErr"   // createSubgraphFlow errors after the caller step parked

	// Scoped by workflow URL of the subgraph child:
	faultSubgraphReviveLost = "subgraphReviveLost" // completeSurgraphFlow skips reviving the parked caller

	// Scoped by signal op (enqueue / statusChange):
	faultSignalPeersPanic = "signalPeersPanic" // the host SignalPeers call panics (host-call panic isolation)

	// Process-wide (no scope):
	faultInterruptStaleWrite = "interruptStaleWrite" // handleInterrupt's in-tx leaf lease_seq read is forced to mismatch (zombie)
	faultDropSignalStop      = "dropSignalStop"      // signalStop delivers nothing (lost terminal wake)
	faultDropDoorbell        = "dropDoorbell"        // the enqueue doorbell is dropped (lost wake)
	faultRecoveryResetErr    = "recoveryResetErr"    // the processStep recovery defer's own reset UPDATE errors
	faultReapMidTree         = "reapMidTree"         // the reaper errors after deleting steps, before flows
	faultReapSelectErr       = "reapSelectErr"       // the reaper's due-root SELECT errors
	faultRefillScanErr       = "refillScanErr"       // the refiller's priority-band scan errors
	faultPollSizingErr       = "pollSizingErr"       // the poll's pending-sizing query is treated as errored
	faultDeliverFailureErr   = "deliverFailureErr"   // deliverFlowFailureToParent drops the parked-caller re-dispatch (lost delivery); unscoped, or scoped by the parked caller's task name for per-level control
	faultCancelCommit        = "cancelCommit"        // the Cancel transaction errors
	faultResumeCommit        = "resumeCommit"        // the Resume transaction errors
	faultForkCommit          = "forkCommit"          // the Fork clone transaction errors
)

// faultKey builds a scoped fault key: "<fault>:<scope>[:<scope>...]". The test side uses it to arm a
// scoped fault; the consult side passes the same scope args to isFault, so the scoping is spelled one way.
func faultKey(fault string, scope ...string) string {
	for _, s := range scope {
		fault += ":" + s
	}
	return fault
}

// isFault reports whether the named fault is armed, consuming one fire. The only consult entry point; free
// in production (returns on the !underTest bool read before any lock OR key build). Optional scope args
// target the fault (task name, workflow URL, ...) and are joined into the key via faultKey - but only once
// underTest has passed, so a scoped consult on a production engine allocates no throwaway key. To arm a
// fault for the rest of a test (until clearFault), inject a large count, e.g. injectFaultN(name, math.MaxInt).
func (e *Engine) isFault(fault string, scope ...string) bool {
	if !e.underTest {
		return false
	}
	name := fault
	if len(scope) > 0 {
		name = faultKey(fault, scope...)
	}
	e.faultsLock.Lock()
	defer e.faultsLock.Unlock()
	n := e.faults[name]
	if n <= 0 {
		return false
	}
	if n == 1 {
		delete(e.faults, name)
	} else {
		e.faults[name] = n - 1
	}
	return true
}

// deliverFailureLost consults faultDeliverFailureErr, the seam that simulates a subgraph child's
// failure-delivery to its parked caller being lost (wedging the caller for recoverWedgedSubgraphParks to
// backstop). It fires either unscoped (the single-level lost-delivery pin) OR scoped to the parked caller's task name,
// so a multi-level test drops the delivery at one chosen level while the other level's succeeds - each caller
// task's FIRST delivery is lost and its sweep-driven re-delivery goes through, letting a depth-N tree wedge
// (and unwedge) at every level. Inert in production: the underTest gate short-circuits before the scope
// SELECT, so a live binary runs neither the query nor any consult.
func (e *Engine) deliverFailureLost(ctx context.Context, tx sequel.Executor, parentStepID int) bool {
	if !e.underTest {
		return false
	}
	if e.isFault(faultDeliverFailureErr) {
		return true
	}
	var taskName string
	if err := tx.QueryRowContext(ctx, "SELECT task_name FROM dwarf_steps WHERE step_id=?", parentStepID).Scan(&taskName); err != nil {
		return false
	}
	return e.isFault(faultDeliverFailureErr, strings.TrimSpace(taskName))
}

// injectFault arms the named fault to fire once (additive: calling twice fires twice).
func (e *Engine) injectFault(name string) { e.injectFaultN(name, 1) }

// injectFaultN arms the named fault to fire n more times (added to any current count).
func (e *Engine) injectFaultN(name string, n int) {
	if n <= 0 {
		return
	}
	e.faultsLock.Lock()
	defer e.faultsLock.Unlock()
	if e.faults == nil {
		e.faults = make(map[string]int)
	}
	e.faults[name] += n
}

// clearFault disarms the named fault regardless of its remaining count.
func (e *Engine) clearFault(name string) {
	e.faultsLock.Lock()
	defer e.faultsLock.Unlock()
	delete(e.faults, name)
}

// --- Execution checkpoints ---

// Where a fault makes a site misbehave, a checkpoint makes a site observable and pausable. The engine passes
// through a named checkpoint by calling checkpoint(); tests drive it two composable ways:
//
//   - waitFor(name): block the TEST until the engine reaches the checkpoint. A one-way rendezvous - "do the
//     next thing only once the engine has gotten to X". It blocks until the engine NEXT reaches name, so a
//     checkpoint the engine already passed does not re-fire; arm a breakpoint (below) when you need to catch
//     a point the engine may already have reached.
//   - setBreakpoint(name) / clearBreakpoint(name): freeze the ENGINE at the checkpoint until the test clears
//     it. A debugger breakpoint for the engine - arm it, let the engine run into it and block, do whatever
//     the test needs while the engine is frozen, then clear to release it.
//
// The two compose to drive a concurrent operation into a precise window deterministically, with no timing
// hammer: setBreakpoint(name); start the engine op in a goroutine; waitFor(name) (returns once the engine is
// frozen at the breakpoint); run the racing op while the engine is held; clearBreakpoint(name) to release.
// waitFor returns immediately if a breakpoint is already holding the engine at name, so the compose case is
// race-free regardless of whether waitFor or the engine's arrival happens first.
//
// Checkpoint site names live here, next to the primitives; the consult site lives at the point it marks,
// calling checkpoint().
const (
	checkpointResumeBeforeFlowWrite   = "resumeBeforeFlowWrite"   // resume(), just before its transaction's flow-status gate write
	checkpointBeforeTransitionTx      = "beforeTransitionTx"      // processStep, after the step is marked completed, before the transition transaction
	checkpointAfterCallerPark         = "afterCallerPark"         // processStep, after the subgraph caller step is parked, before createSubgraphFlow
	checkpointBeforeRetryRewind       = "beforeRetryRewind"       // processStep, before the flow.Retry rewind transaction
	checkpointBeforeCompleteFlowWrite = "beforeCompleteFlowWrite" // completeFlow(), just before its transaction's status-gate write
	checkpointBeforeDeleteWrite       = "beforeDeleteWrite"       // deleteFlow(), just before its transaction's delete-stamp/interrupted-CAS write
	checkpointBeforeReviveWrite       = "beforeReviveWrite"       // completeSurgraphFlow(), just before its transaction's caller-revive write
	checkpointBeforeRecoveryReset     = "beforeRecoveryReset"     // processStep recovery defer, just before its fenced step-reset transaction

	// Lifecycle rendezvous (fired at an event, used with waitFor - not a freeze site): lets a test wait for
	// exact engine progress instead of polling status / sleeping.
	checkpointFlowStopped = "flowStopped" // signalStop(), a flow just reached a stop (completed/failed/cancelled/interrupted)
)

// breakpoint is one armed breakpoint: release is closed by clearBreakpoint to let the frozen engine proceed;
// hit is closed by the engine (under faultsLock) when it reaches the checkpoint and is about to block, so a
// waitFor that arrives after the engine froze still observes the arrival instead of hanging.
type breakpoint struct {
	release chan struct{}
	hit     chan struct{}
}

// checkpoint is the engine-side consult at an instrumented site. Free in production (returns on the
// !underTest bool read). Under test it wakes any waitFor(name) waiters and, if a breakpoint is armed for
// name, blocks until clearBreakpoint(name) (or ctx is done, so a stuck test / shutdown can never wedge the
// goroutine forever). Waking waiters and marking the breakpoint hit happen under one lock hold, so a waitFor
// racing the engine's arrival is woken or sees hit - never lost between the two.
func (e *Engine) checkpoint(ctx context.Context, name string) {
	if !e.underTest {
		return
	}
	e.faultsLock.Lock()
	for _, ch := range e.waitFors[name] {
		close(ch)
	}
	delete(e.waitFors, name)
	bp := e.breakpoints[name]
	if bp != nil {
		close(bp.hit) // one-shot: clearBreakpoint deletes the entry, so a re-dispatch reaching here finds bp==nil
	}
	e.faultsLock.Unlock()

	if bp != nil {
		select {
		case <-bp.release:
		case <-ctx.Done():
		}
	}
}

// waitFor blocks until the engine reaches the named checkpoint. If a breakpoint is already holding the engine
// at name, it returns immediately; otherwise it registers a waiter woken by the engine's next arrival.
// Test-only.
func (e *Engine) waitFor(name string) {
	e.faultsLock.Lock()
	if bp := e.breakpoints[name]; bp != nil {
		select {
		case <-bp.hit:
			e.faultsLock.Unlock()
			return // engine already frozen at this breakpoint
		default:
		}
	}
	ch := make(chan struct{})
	if e.waitFors == nil {
		e.waitFors = make(map[string][]chan struct{})
	}
	e.waitFors[name] = append(e.waitFors[name], ch)
	e.faultsLock.Unlock()
	<-ch
}

// setBreakpoint arms a breakpoint so the engine blocks the next time it reaches the named checkpoint, until
// clearBreakpoint releases it. Test-only.
func (e *Engine) setBreakpoint(name string) {
	e.faultsLock.Lock()
	if e.breakpoints == nil {
		e.breakpoints = make(map[string]*breakpoint)
	}
	e.breakpoints[name] = &breakpoint{release: make(chan struct{}), hit: make(chan struct{})}
	e.faultsLock.Unlock()
}

// clearBreakpoint releases the engine frozen at the named breakpoint (and disarms it). A no-op if none is
// armed. Test-only.
func (e *Engine) clearBreakpoint(name string) {
	e.faultsLock.Lock()
	bp := e.breakpoints[name]
	delete(e.breakpoints, name)
	e.faultsLock.Unlock()
	if bp != nil {
		close(bp.release)
	}
}
