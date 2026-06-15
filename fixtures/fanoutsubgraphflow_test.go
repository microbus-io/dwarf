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

// TestFanoutSubgraphflow exercises a static fan-out where ONE sibling is a subgraph caller with a trivial
// child, all converging at a fan-in - the shape that wedged examples/creditflow's CreditApproval happy path
// before the subgraph park-before-start fix. The engine must park the caller step BEFORE making the child
// dispatchable: completeSurgraphFlow revives the caller only WHERE parked=parkedSubgraph (and parkedSubgraph
// steps are excluded from lease recovery), so if the child completes and runs that revive before a later
// park lands, the caller is stranded permanently and its fan-in never fires. This is a functional check that
// the fan-out-sibling subgraph path completes; note it does NOT deterministically reproduce the strand
// in-process (the TestProxy dispatch is too synchronous for the child to win the race) - the deterministic
// repro is bus-timing-specific (creditflow over the Microbus bus), tracked separately. The short drain bound
// makes a stranded flow fail rather than hang.
func TestFanoutSubgraphflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Parent: Entry fans out to {A, B, Sub}, all converging at the Join fan-in, then END.
	parent := workflow.NewGraph("Parent", "fanoutsub.verify:428/parent")
	parent.AddTask("Entry", "fanoutsub.verify:428/entry")
	parent.AddTask("A", "fanoutsub.verify:428/a")
	parent.AddTask("B", "fanoutsub.verify:428/b")
	parent.AddTask("Sub", "fanoutsub.verify:428/sub")
	parent.AddTask("Join", "fanoutsub.verify:428/join")
	parent.SetFanIn("Join")
	parent.AddTransition("Entry", "A")
	parent.AddTransition("Entry", "B")
	parent.AddTransition("Entry", "Sub")
	parent.AddTransition("A", "Join")
	parent.AddTransition("B", "Join")
	parent.AddTransition("Sub", "Join")
	parent.AddTransition("Join", workflow.END)
	proxy.HandleGraph("fanoutsub.verify:428/parent", parent)

	// Inner: a single trivial task, so the child completes as fast as possible to race the caller's park.
	inner := workflow.NewGraph("Inner", "fanoutsub.verify:428/inner")
	inner.AddTask("InnerEntry", "fanoutsub.verify:428/inner-entry")
	inner.AddTransition("InnerEntry", workflow.END)
	proxy.HandleGraph("fanoutsub.verify:428/inner", inner)

	noop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	proxy.HandleTask("fanoutsub.verify:428/entry", noop)
	proxy.HandleTask("fanoutsub.verify:428/a", noop)
	proxy.HandleTask("fanoutsub.verify:428/b", noop)
	proxy.HandleTask("fanoutsub.verify:428/join", noop)
	proxy.HandleTask("fanoutsub.verify:428/inner-entry", noop)
	proxy.HandleTask("fanoutsub.verify:428/sub", func(ctx context.Context, f *workflow.Flow) error {
		_, yield, err := f.Subgraph("fanoutsub.verify:428/inner", nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy).
		WithWorkers(4)
	eng.RunInTest(t)

	t.Run("every_flow_terminates", func(t *testing.T) {
		assert := testarossa.For(t)

		const n = 50
		keys := make([]string, 0, n)
		for range n {
			k, err := eng.Create(ctx, "fanoutsub.verify:428/parent", nil, nil)
			if !assert.NoError(err) {
				return
			}
			if err := eng.Start(ctx, k); !assert.NoError(err) {
				return
			}
			keys = append(keys, k)
		}

		stuck := keys
		deadline := time.Now().Add(20 * time.Second)
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
		assert.Equal(0, len(stuck), "stranded fan-out-subgraph flows: %d of %d", len(stuck), n)
	})
}
