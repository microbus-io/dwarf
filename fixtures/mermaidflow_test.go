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
	"strings"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestMermaidflow exercises the execution-history renderer (workflow.FlowRenderer) and the engine's
// HistoryMermaid wrapper over a flow that hits every structural branch of the renderer: a linear head,
// a dynamic fan-out + fan-in cohort, and a subgraph caller (whose SubHistory nests). It asserts robust
// structural invariants of the Mermaid output rather than exact bytes, so it pins that the renderer runs
// end-to-end and decomposes subgraphs/cohorts without pinning cosmetic formatting.
func TestMermaidflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// Parent: Start -> (forEach items -> Work) -> Collect (fan-in) -> CallSub (subgraph) -> Finish -> END
	parent := workflow.NewGraph("MermaidParent")
	parent.SetEndpoint("Start", "mermaidflow.verify:428/start")
	parent.SetEndpoint("Work", "mermaidflow.verify:428/work")
	parent.SetEndpoint("Collect", "mermaidflow.verify:428/collect")
	parent.SetEndpoint("CallSub", "mermaidflow.verify:428/call-sub")
	parent.SetEndpoint("Finish", "mermaidflow.verify:428/finish")
	parent.SetFanIn("Collect")
	parent.SetReducer("processed", workflow.ReducerAdd)
	parent.AddTransitionForEach("Start", "Work", "items", "item")
	parent.AddTransitionChain("Work", "Collect", "CallSub", "Finish", workflow.END)
	proxy.HandleGraph("mermaidflow.verify:428/parent", parent)

	// Inner subgraph: InnerA -> InnerB -> END
	inner := workflow.NewGraph("MermaidInner")
	inner.SetEndpoint("InnerA", "mermaidflow.verify:428/inner-a")
	inner.SetEndpoint("InnerB", "mermaidflow.verify:428/inner-b")
	inner.AddTransitionChain("InnerA", "InnerB", workflow.END)
	proxy.HandleGraph("mermaidflow.verify:428/inner", inner)

	proxy.HandleTask("mermaidflow.verify:428/start", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("mermaidflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("item") != "" {
			f.SetInt("processed", 1)
		}
		return nil
	})
	proxy.HandleTask("mermaidflow.verify:428/collect", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("processedCount", f.GetInt("processed"))
		return nil
	})
	proxy.HandleTask("mermaidflow.verify:428/call-sub", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("mermaidflow.verify:428/inner", map[string]any{"seed": "s"}, &out)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("mermaidflow.verify:428/finish", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})
	proxy.HandleTask("mermaidflow.verify:428/inner-a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("innerResult", "A")
		return nil
	})
	proxy.HandleTask("mermaidflow.verify:428/inner-b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("innerResult", "B")
		return nil
	})

	flowKey, outcome, err := eng.Run(ctx, "mermaidflow.verify:428/parent",
		map[string]any{"items": []string{"x", "y", "z"}}, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	t.Run("history_mermaid_structure", func(t *testing.T) {
		assert := testarossa.For(t)

		var sb strings.Builder
		err := eng.HistoryMermaid(ctx, flowKey, &sb)
		assert.NoError(err)
		mmd := sb.String()

		assert.True(strings.HasPrefix(strings.TrimSpace(mmd), "flowchart"), "output should be a Mermaid flowchart")
		// Every parent task node appears.
		for _, node := range []string{"Start", "Work", "Collect", "CallSub", "Finish"} {
			assert.True(strings.Contains(mmd, node), "expected parent node %q in diagram", node)
		}
		// The subgraph caller decomposes into a visible subgraph block containing the inner tasks.
		assert.True(strings.Contains(mmd, "subgraph"), "expected a Mermaid subgraph wrapper block")
		for _, node := range []string{"InnerA", "InnerB"} {
			assert.True(strings.Contains(mmd, node), "expected inner subgraph node %q in diagram", node)
		}
		// Chrome start/end sentinels are drawn.
		assert.True(strings.Contains(mmd, "_start"), "expected _start chrome node")
		assert.True(strings.Contains(mmd, "_end"), "expected _end chrome node")
		// HistoryMermaid renders with WithLinks("step"), so per-node click directives are emitted.
		assert.True(strings.Contains(mmd, "click "), "expected click directives from WithLinks")
	})

	t.Run("flow_renderer_options", func(t *testing.T) {
		assert := testarossa.For(t)

		steps, err := eng.History(ctx, flowKey)
		assert.NoError(err)
		assert.True(len(steps) > 0, "history should have steps")

		// Exercise the option setters (direction, title, custom palettes) and confirm a clean render.
		mmd := workflow.NewFlowRenderer(steps).
			WithLeftRight().
			WithTitle("Mermaid Test").
			WithPrimaryColors("#112233", "#ffffff").
			WithSecondaryColors("#445566", "#ffffff").
			WithErrorColors("#aa0000", "#ffffff").
			WithAttentionColors("#cc8800", "#000000").
			Render()
		assert.True(strings.Contains(mmd, "LR"), "WithLeftRight should orient the flowchart left-to-right")
		assert.True(strings.Contains(mmd, "Mermaid Test"), "WithTitle should embed the title")
		assert.True(strings.Contains(mmd, "#112233"), "custom primary fill should appear in a classDef/style")
	})
}
