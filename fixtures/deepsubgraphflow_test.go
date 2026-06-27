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

/*
Deeply nested subgraphs (5 levels: g0 -> g1 -> g2 -> g3 -> leaf) exercise the
root_flow_id-backed tree walks end to end. An interrupt raised in the deepest leaf
propagates up the whole chain (surgraphChain) so the caller awaiting the root sees
interrupted; Resume on the root descends back down (interruptedSubgraphChain +
surgraphChain) to the leaf and the chain runs to completion, with state threaded
correctly through every level. A second case cancels a deeply-interrupted tree from
the root (surgraphChain + the membership down-walk allSubgraphFlows). These are the
operations whose per-level recursion was collapsed to single tree scans; the test
pins that the collapse is behavior-preserving at depth.
*/
package fixtures

import (
	"context"
	"strconv"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestDeepsubgraphflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const depth = 4 // intermediate levels g0..g3; g4 is the interrupting leaf

	proxy := engine.NewTestProxy()

	// Intermediate levels: each is a single task that calls the next level as a subgraph and wraps the
	// returned v, so the root's final v reflects every level it passed through.
	for i := 0; i < depth; i++ {
		gURL := "deepnest.verify:0/g" + strconv.Itoa(i)
		taskURL := "deepnest.verify:0/call" + strconv.Itoa(i)
		g := workflow.NewGraph("G" + strconv.Itoa(i))
		g.SetEndpoint("Call", taskURL)
		g.AddTransition("Call", workflow.END)
		proxy.HandleGraph(gURL, g)

		level := i
		nextURL := "deepnest.verify:0/g" + strconv.Itoa(i+1)
		proxy.HandleTask(taskURL, func(ctx context.Context, f *workflow.Flow) error {
			var out map[string]any
			yield, err := f.Subgraph(nextURL, nil, &out)
			if yield || err != nil {
				return err
			}
			inner, _ := out["v"].(string)
			f.SetString("v", inner+"<"+strconv.Itoa(level)+">")
			return nil
		})
	}

	// Leaf level g4: interrupts, then bakes the resume answer into v.
	leafG := workflow.NewGraph("Leaf")
	leafG.SetEndpoint("Leaf", "deepnest.verify:0/leaf")
	leafG.AddTransition("Leaf", workflow.END)
	proxy.HandleGraph("deepnest.verify:0/g"+strconv.Itoa(depth), leafG)
	proxy.HandleTask("deepnest.verify:0/leaf", func(ctx context.Context, f *workflow.Flow) error {
		var rd map[string]any
		yield, err := f.Interrupt(map[string]any{"depth": depth}, &rd)
		if yield || err != nil {
			return err
		}
		ans, _ := rd["answer"].(string)
		f.SetString("v", "leaf("+ans+")")
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("deep_interrupt_propagates_up_and_resume_descends", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "deepnest.verify:0/g0", nil, nil)
		if !assert.NoError(err) {
			return
		}

		// The interrupt raised 5 levels deep surfaces at the root (surgraphChain up-walk).
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)
		assert.Equal(float64(depth), outcome.InterruptPayload["depth"])

		// Resume on the root descends to the leaf (interruptedSubgraphChain) and bubbles completion back up.
		if !assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"answer": "ok"})) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		// State threaded correctly through every level: leaf, then wrapped by g3,g2,g1,g0.
		assert.Equal("leaf(ok)<3><2><1><0>", outcome.State["v"])
	})

	t.Run("cancel_deeply_interrupted_tree_from_root", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "deepnest.verify:0/g0", nil, nil)
		if !assert.NoError(err) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)

		// Cancel from the root tears down the whole interrupted tree (surgraphChain + allSubgraphFlows).
		if !assert.NoError(eng.Cancel(ctx, flowKey, "stop")) {
			return
		}
		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal("stop", outcome.CancelReason)
	})
}
