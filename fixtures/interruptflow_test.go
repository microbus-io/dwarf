/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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

func TestInterruptflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("interruptflow.verify:428/interrupt")
	graph.AddTask("taskA", "interruptflow.verify:428/task-a")
	graph.AddTask("awaitInput", "interruptflow.verify:428/await-input")
	graph.AddTask("compose", "interruptflow.verify:428/compose")
	graph.AddTransition("taskA", "awaitInput")
	graph.AddTransition("awaitInput", "compose")
	graph.AddTransition("compose", workflow.END)
	proxy.HandleGraph("interruptflow.verify:428/interrupt", graph)

	proxy.HandleTask("interruptflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		return nil
	})
	proxy.HandleTask("interruptflow.verify:428/await-input", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		resumeData, yield, err := f.Interrupt(map[string]any{"requestedInput": "userInput"})
		if yield || err != nil {
			return err
		}
		if ui, ok := resumeData["userInput"]; ok {
			f.Set("userInput", ui)
		}
		return nil
	})
	proxy.HandleTask("interruptflow.verify:428/compose", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		f.SetString("result", f.GetString("prompt")+", "+f.GetString("userInput"))
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("interrupt_then_resume_completes_flow", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "interruptflow.verify:428/interrupt", map[string]any{"prompt": "Hello"}, nil, nil)
		if !assert.NoError(err) {
			return
		}
		err = eng.Start(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, outcome.Status)

		err = eng.Resume(ctx, flowKey, map[string]any{"userInput": "world"})
		if !assert.NoError(err) {
			return
		}

		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("Hello, world", outcome.State["result"])
	})
}
