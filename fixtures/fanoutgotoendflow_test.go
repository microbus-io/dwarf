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

// A node that is both a forEach fan-out source and a Goto(END) exit - the "fan out over the array each
// round, or end the flow when the task decides there is nothing left to do" loop. The Goto must terminate
// the flow: routing it to the fan-in instead sends the flow back around the loop that reaches the source
// again, writing a step per lap until something outside kills it, so the pin is the step COUNT as much as
// the terminal status.
func TestFanoutgotoendflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("FanOutGotoEnd")
	graph.SetEndpoint("Init", "fanoutgotoendflow.verify:428/init")
	graph.SetEndpoint("Decide", "fanoutgotoendflow.verify:428/decide")
	graph.SetEndpoint("Work", "fanoutgotoendflow.verify:428/work")
	graph.SetEndpoint("Join", "fanoutgotoendflow.verify:428/join")
	graph.SetFanIn("Join")
	graph.AddTransition("Init", "Decide")
	graph.AddTransitionGoto("Decide", workflow.END)
	graph.AddTransitionForEach("Decide", "Work", "pending", "current")
	graph.AddTransition("Work", "Join")
	graph.AddTransition("Join", "Decide")
	assert.NoError(graph.Validate())
	proxy.HandleGraph("fanoutgotoendflow.verify:428/fan-out-goto-end", graph)

	proxy.HandleTask("fanoutgotoendflow.verify:428/init", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("pending", []string{"a", "b"})
		return nil
	})
	// Round 0 fans out over pending; round 1 ends the flow.
	proxy.HandleTask("fanoutgotoendflow.verify:428/decide", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("rounds") > 0 {
			f.Goto(workflow.END)
			return nil
		}
		f.SetInt("rounds", f.GetInt("rounds")+1)
		return nil
	})
	proxy.HandleTask("fanoutgotoendflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("fanoutgotoendflow.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("pending", nil)
		return nil
	})

	t.Run("goto_end_terminates_the_loop", func(t *testing.T) {
		assert := testarossa.For(t)

		runCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		flowKey, outcome, err := eng.Run(runCtx, "fanoutgotoendflow.verify:428/fan-out-goto-end", nil, nil)
		// Guarded, because the regression this pins is a flow that never terminates: Run then returns a NIL
		// outcome on its ctx deadline, and reading a field off it panics - which kills the whole package's
		// test binary, taking every co-running parallel fixture with it, instead of failing this one test.
		if !assert.NoError(err) || !assert.NotNil(outcome, "the flow never stopped: it looped instead of ending") {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)

		// Init, Decide, two Work branches, Join, Decide. A flow that looped instead of ending accumulates
		// a Decide/Join pair per lap, so any excess here is the regression.
		steps, err := eng.History(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(6, len(steps))
	})
}
