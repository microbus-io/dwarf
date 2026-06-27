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

package fixtures

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCompletionRaceflow is a regression test for the flow-completion lock-contention wedge. It drives a
// burst of flows that complete near-simultaneously on a shared SQLite database, which stresses completeFlow:
// before the fix that transaction was read-first (computeFinalState's SELECTs, then the flow-status UPDATE),
// so two concurrent completions could both hold SHARED locks and deadlock on the upgrade to write. Under
// that contention completeFlow exhausts its retries and errors; because the terminal step was already marked
// completed, the lease recovery (which only resets running rows) cannot re-dispatch it, and the flow is
// stranded forever 'running' with every step terminal - an orphan flow. The make-completeFlow-write-first fix
// removes the deadlock. Each flow is a subgraph caller with a trivial child purely to double the completion
// rate (caller flow plus child flow both finish in the burst); the bug is not subgraph-specific. The short
// drain bound (far below any lease) makes a stranded flow fail rather than eventually self-heal. Without the
// fix this strands a flow on roughly one run in five; with it, all drain in well under a second.
func TestCompletionRaceflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Parent graph: a single subgraph-caller task, then END.
	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("Caller", "completionrace.verify:428/caller")
	parent.AddTransition("Caller", workflow.END)
	proxy.HandleGraph("completionrace.verify:428/parent", parent)

	// Inner graph: a single no-op task, then END - completes as fast as possible to race the caller's park.
	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("InnerEntry", "completionrace.verify:428/inner-entry")
	inner.AddTransition("InnerEntry", workflow.END)
	proxy.HandleGraph("completionrace.verify:428/inner", inner)

	proxy.HandleTask("completionrace.verify:428/caller", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("completionrace.verify:428/inner", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("completionrace.verify:428/inner-entry", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.SetNumShards(2)
	eng.SetWorkers(4)
	eng.RunInTest(t)

	t.Run("every_flow_terminates", func(t *testing.T) {
		assert := testarossa.For(t)

		const n = 500
		keys := make([]string, 0, n)
		for range n {
			k, err := eng.Create(ctx, "completionrace.verify:428/parent", nil, nil)
			if !assert.NoError(err) {
				return
			}
			keys = append(keys, k)
		}

		// Drain with a bound far below the worker lease: a stranded caller never reaches a terminal status,
		// so it survives the bound. With the fix every caller completes in well under a second.
		stuck := keys
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) {
			var remaining []string
			for _, k := range stuck {
				o, _ := eng.Snapshot(ctx, k)
				if o != nil && o.Status == workflow.StatusCompleted {
					continue
				}
				remaining = append(remaining, k)
			}
			stuck = remaining
			if len(stuck) == 0 {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}

		if !assert.Equal(0, len(stuck), "stranded flows: %d of %d", len(stuck), n) {
			for i, k := range stuck {
				o, _ := eng.Snapshot(ctx, k)
				st := ""
				if o != nil {
					st = o.Status
				}
				t.Logf("stranded flow=%s status=%s", k, st)
				steps, _ := eng.History(ctx, k)
				for _, step := range steps {
					t.Logf("  step depth=%d task=%s status=%s parked=%v sub=%v", step.StepDepth, step.TaskName, step.Status, step.Parked, step.Subgraph)
					for _, ss := range step.SubHistory {
						t.Logf("    sub depth=%d task=%s status=%s parked=%v", ss.StepDepth, ss.TaskName, ss.Status, ss.Parked)
					}
				}
				if i >= 5 {
					break
				}
			}
		}
	})
}
