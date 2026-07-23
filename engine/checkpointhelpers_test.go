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
	// retryInterval is the cadence of drivePollBackstop, the one helper that still loops - it DRIVES the engine
	// (it calls pollPendingSteps) rather than waiting for it, so it is not a spin-wait on an observation.
	retryInterval = 20 * time.Millisecond

	// pollBackstopWait bounds a lease-recovery re-dispatch - see drivePollBackstop.
	pollBackstopWait = 2 * time.Second
)

// awaitFlowStatus blocks until the given flow has reached the given stop status, failing the test on timeout. It
// replaced a helper that polled the flow row every 10ms until the status matched. The poll was two-sided-wrong:
// too short a timeout flakes on a loaded CI box, and a long one silently reports a genuine hang as a slow pass.
// The checkpoint fires post-commit inside signalStop, so a return here means the status is durable - the same
// fact the poll was sampling for, observed exactly once and at the moment it becomes true.
//
// This is the one wait that needs a helper rather than a bare e.seams.WaitTimeout: the flow key is not known
// until Create returns, so the flow can genuinely stop before the test can arm, and neither a plain wait (which
// would arm too late) nor a bare Visits check (which would race a stop landing just after it) covers that alone.
// So it does BOTH, and the order is load-bearing: arm FIRST, check Visits SECOND. A stop before the call is then
// caught by the count, one after by the wait, and one landing between the two lines by the wait - because the
// waiter is already registered. Reversing the two lines reintroduces exactly the race the split-arming Waiter
// exists to remove.
//
// Two limits. It only sees stops routed through signalStop (completed/failed/cancelled/interrupted) - a status
// reached any other way has no rendezvous. And because Visits is monotonic, a REPEATED status (a flow
// interrupted, resumed, then interrupted again) is satisfied by the earlier occurrence; wait on such a status
// with e.seams.Waiter armed around the specific trigger instead.
func awaitFlowStatus(t *testing.T, e *Engine, flowKey, want string, timeout time.Duration) {
	t.Helper()
	reached := e.seams.Waiter(checkpointFlowStopped, flowKey, want) // arm first...
	if e.seams.Visits(checkpointFlowStopped, flowKey, want) > 0 {
		return // ...then catch a stop that beat us to it
	}
	select {
	case <-reached:
	case <-time.After(timeout * testTimeoutScale):
		t.Fatalf("flow %s never reached status %q within %s", flowKey, want, timeout*testTimeoutScale)
	}
}
