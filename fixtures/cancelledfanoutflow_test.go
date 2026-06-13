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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestCancelledfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("cancelledfanoutflow.verify:428/cancelled-fan-out")
	graph.AddTask("source", "cancelledfanoutflow.verify:428/source")
	graph.AddTask("a", "cancelledfanoutflow.verify:428/a")
	graph.AddTask("b", "cancelledfanoutflow.verify:428/b")
	graph.AddTask("c", "cancelledfanoutflow.verify:428/c")
	graph.AddTask("j", "cancelledfanoutflow.verify:428/j")
	graph.SetFanIn("j")
	graph.SetReducer("executed", workflow.ReducerAdd)
	graph.AddTransition("source", "a")
	graph.AddTransition("source", "b")
	graph.AddTransition("source", "c")
	graph.AddTransition("a", "j")
	graph.AddTransition("b", "j")
	graph.AddTransition("c", "j")
	graph.AddTransition("j", workflow.END)
	proxy.HandleGraph("cancelledfanoutflow.verify:428/cancelled-fan-out", graph)

	var executed atomic.Int32

	branch := func(ctx context.Context, f *workflow.Flow) error {
		executed.Add(1)
		select {
		case <-time.After(2 * time.Second):
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}
		f.SetInt("executed", 1)
		return nil
	}

	proxy.HandleTask("cancelledfanoutflow.verify:428/source", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("cancelledfanoutflow.verify:428/a", branch)
	proxy.HandleTask("cancelledfanoutflow.verify:428/b", branch)
	proxy.HandleTask("cancelledfanoutflow.verify:428/c", branch)
	proxy.HandleTask("cancelledfanoutflow.verify:428/j", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithWorkers(1)
	eng.RunInTest(t)

	t.Run("cancel_mid_fan_out", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "cancelledfanoutflow.verify:428/cancelled-fan-out", nil, nil)
		if !assert.NoError(err) {
			return
		}
		err = eng.Start(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		time.Sleep(1 * time.Second)
		err = eng.Cancel(ctx, flowKey, "")
		if !assert.NoError(err) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal(1, int(executed.Load()))
	})
}
