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

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestSwitchflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Switch", "switchflow.verify:428/switch")
	graph.AddTask("Router", "switchflow.verify:428/router")
	graph.AddTask("HandleHigh", "switchflow.verify:428/handle-high")
	graph.AddTask("HandleMid", "switchflow.verify:428/handle-mid")
	graph.AddTask("HandleLow", "switchflow.verify:428/handle-low")
	graph.AddTransitionSwitch("Router", "HandleHigh", "amount >= 10000")
	graph.AddTransitionSwitch("Router", "HandleMid", "amount >= 1000")
	graph.AddTransitionSwitch("Router", "HandleLow", "true")
	graph.AddTransition("HandleHigh", workflow.END)
	graph.AddTransition("HandleMid", workflow.END)
	graph.AddTransition("HandleLow", workflow.END)
	proxy.HandleGraph("switchflow.verify:428/switch", graph)

	// No-match graph: same router but no default arm.
	noMatchGraph := workflow.NewGraph("SwitchNoMatch", "switchflow.verify:428/switch-no-match")
	noMatchGraph.AddTask("Router", "switchflow.verify:428/router")
	noMatchGraph.AddTask("HandleHigh", "switchflow.verify:428/handle-high")
	noMatchGraph.AddTask("HandleMid", "switchflow.verify:428/handle-mid")
	noMatchGraph.AddTransitionSwitch("Router", "HandleHigh", "amount >= 10000")
	noMatchGraph.AddTransitionSwitch("Router", "HandleMid", "amount >= 1000")
	noMatchGraph.AddTransition("HandleHigh", workflow.END)
	noMatchGraph.AddTransition("HandleMid", workflow.END)
	proxy.HandleGraph("switchflow.verify:428/switch-no-match", noMatchGraph)

	proxy.HandleTask("switchflow.verify:428/router", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("switchflow.verify:428/handle-high", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "high")
		return nil
	})
	proxy.HandleTask("switchflow.verify:428/handle-mid", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "mid")
		return nil
	})
	proxy.HandleTask("switchflow.verify:428/handle-low", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "low")
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("amount_above_high_threshold_takes_high_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "switchflow.verify:428/switch", map[string]any{"amount": 50000}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("high", outcome.State["branch"])
	})

	t.Run("amount_in_mid_band_takes_mid_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "switchflow.verify:428/switch", map[string]any{"amount": 5000}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("mid", outcome.State["branch"])
	})

	t.Run("amount_below_thresholds_takes_default_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "switchflow.verify:428/switch", map[string]any{"amount": 100}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("low", outcome.State["branch"])
	})

	t.Run("boundary_10000_takes_high_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "switchflow.verify:428/switch", map[string]any{"amount": 10000}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("high", outcome.State["branch"])
	})

	t.Run("boundary_1000_takes_mid_branch", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "switchflow.verify:428/switch", map[string]any{"amount": 1000}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("mid", outcome.State["branch"])
	})
}

func TestSwitchflow_NoMatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("SwitchNoMatch", "switchflow.verify:428/switch-no-match")
	graph.AddTask("Router", "switchflow.verify:428/router")
	graph.AddTask("HandleHigh", "switchflow.verify:428/handle-high")
	graph.AddTask("HandleMid", "switchflow.verify:428/handle-mid")
	graph.AddTransitionSwitch("Router", "HandleHigh", "amount >= 10000")
	graph.AddTransitionSwitch("Router", "HandleMid", "amount >= 1000")
	graph.AddTransition("HandleHigh", workflow.END)
	graph.AddTransition("HandleMid", workflow.END)
	proxy.HandleGraph("switchflow.verify:428/switch-no-match", graph)

	proxy.HandleTask("switchflow.verify:428/router", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("switchflow.verify:428/handle-high", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "high")
		return nil
	})
	proxy.HandleTask("switchflow.verify:428/handle-mid", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("branch", "mid")
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("no_match_completes_flow_without_branching", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "switchflow.verify:428/switch-no-match", map[string]any{"amount": 100}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(nil, outcome.State["branch"])
	})

	t.Run("matching_input_still_routes_normally", func(t *testing.T) {
		assert := testarossa.For(t)

		outcome, err := eng.Run(ctx, "switchflow.verify:428/switch-no-match", map[string]any{"amount": 5000}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("mid", outcome.State["branch"])
	})
}
