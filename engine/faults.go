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

// Fault injection is a test-only seam: a white-box test in this package arms a named fault, and the engine
// consults it at a strategic site to simulate an error or other behavior that is otherwise hard to trigger
// on demand (a dropped signal, a lost revive, a commit that fails). It is a lighter alternative to
// elaborate DB forging or timing races, and it is inert in production: every consult short-circuits on
// !underTest (cached from testing.Testing() in NewEngine) before touching the lock or map, so a production
// engine pays a single bool read per site and no fault can ever fire.
//
// Faults are usually SCOPED: the consult site appends an identifier so a test targets one task, one
// workflow, or one flow rather than "the next thing that happens". Build the key with faultKey, e.g.
// e.isFault(faultKey(faultExecuteTask, taskName)); a test arms the same key,
// e.injectFault(faultKey(faultExecuteTask, "Charge")). A few faults are process-wide and take no scope.
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

	// Scoped by workflow URL of the subgraph child:
	faultSubgraphReviveLost = "subgraphReviveLost" // completeSurgraphFlow skips reviving the parked caller

	// Process-wide (no scope):
	faultDropSignalStop = "dropSignalStop" // signalStop delivers nothing (lost terminal wake)
	faultDropDoorbell   = "dropDoorbell"   // the enqueue doorbell is dropped (lost wake)
	faultReapMidTree    = "reapMidTree"    // the reaper errors after deleting steps, before flows
	faultRefillScanErr  = "refillScanErr"  // the refiller's priority-band scan errors
	faultPollSizingErr  = "pollSizingErr"  // the poll's pending-sizing query is treated as errored
)

// faultKey builds a scoped fault key: "<fault>:<scope>". Both the consult site and the test use it so the
// scoping is spelled one way.
func faultKey(fault, scope string) string { return fault + ":" + scope }

// isFault reports whether the named fault is armed, consuming one fire. The only consult entry point; free
// in production (returns on the !underTest bool read before any lock). To arm a fault for the rest of a
// test (until clearFault), inject a large count, e.g. injectFaultN(name, math.MaxInt).
func (e *Engine) isFault(name string) bool {
	if !e.underTest {
		return false
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
