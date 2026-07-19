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

// retryInterval is the cadence shared by the settle-and-retry helpers.
const (
	retryInterval = 20 * time.Millisecond

	// pollBackstopWait bounds a lease-recovery re-dispatch - see drivePollBackstop.
	pollBackstopWait = 2 * time.Second

	// invariantSettleWait bounds how long an invariant may stay violated before it counts as broken rather
	// than as work still in flight - see the check helper in invariants_test.go.
	invariantSettleWait = 3 * time.Second
)

// pollPredicate re-evaluates `pred` every retryInterval until it reports true or retryDuration elapses, and
// reports whether it did. It exists for one job: letting an assertion settle against a live engine, where a
// condition that is about to become true is not the same as one that is broken.
//
// `pred` must be SIDE-EFFECT FREE - it observes, it does not make the thing happen. A helper that needs to
// drive the engine to reach its condition is a different operation and says so in its own name; see
// drivePollBackstop, which deliberately does not route through here.
//
// The bound is a DURATION and the iteration count is derived from it, because that is the number a caller can
// reason about: "give it two seconds" survives a change to the cadence, a hardcoded count silently does not.
func pollPredicate(retryDuration time.Duration, pred func() bool) bool {
	for range max(1, int(retryDuration/retryInterval)) {
		if pred() {
			return true
		}
		time.Sleep(retryInterval)
	}
	return pred()
}

// cpWaitFor blocks until the engine reaches (or is already frozen at) the named checkpoint, failing the
// test on timeout rather than hanging to the suite deadline. Shared by every checkpoint-driven test.
func cpWaitFor(t *testing.T, e *Engine, name string, timeout time.Duration) {
	t.Helper()
	reached := make(chan struct{})
	go func() { e.seams.Wait(name); close(reached) }()
	select {
	case <-reached:
	case <-time.After(timeout):
		t.Fatalf("engine never reached checkpoint %q", name)
	}
}
