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
	"testing"
	"time"
)

const (
	// retryInterval is the cadence of driveLeaseRecovery, the one helper that still loops - it DRIVES the engine
	// (it calls recoverExpiredLeases) rather than waiting for it, so it is not a spin-wait on an observation.
	retryInterval = 20 * time.Millisecond

	// leaseRecoveryWait bounds a lease-recovery re-dispatch - see driveLeaseRecovery.
	leaseRecoveryWait = 2 * time.Second
)

// awaitStartupRecoverySweep blocks until the engine's Startup recovery pass has finished. Every test that
// FORGES a wedge or orphan shape and then drives one detector itself must call this before forging: that
// background pass runs the same detectors, so one still in flight sees the forged shape too and the test's
// "flagged exactly once" assertion reads two (see CheckpointRecoverySweepDone for the measured timing).
//
// Arm FIRST, then check Visits. That order is load-bearing: a sweep that finished before the call is caught by
// the count, one that finishes later by the channel, and one landing between the two lines by the channel - the
// waiter is already registered. Checking first would drop a sweep that lands in the gap.
func awaitStartupRecoverySweep(t testing.TB, e *Engine) {
	t.Helper()
	swept := e.seams.Waiter(CheckpointRecoverySweepDone)
	if e.seams.Visits(CheckpointRecoverySweepDone) > 0 {
		return
	}
	// A "did it hang" ceiling, not a timing contract - the sweep either completes or the engine is broken - so
	// it leaves generous room for a loaded server and for -race.
	select {
	case <-swept:
	case <-time.After(time.Minute):
		t.Fatal("engine never completed its startup recovery sweep")
	}
}
