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

func TestInterruptflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Interrupt", "interruptflow.verify:428/interrupt")
	graph.AddTask("TaskA", "interruptflow.verify:428/task-a")
	graph.AddTask("AwaitInput", "interruptflow.verify:428/await-input")
	graph.AddTask("Compose", "interruptflow.verify:428/compose")
	graph.AddTransition("TaskA", "AwaitInput")
	graph.AddTransition("AwaitInput", "Compose")
	graph.AddTransition("Compose", workflow.END)
	proxy.HandleGraph("interruptflow.verify:428/interrupt", graph)

	proxy.HandleTask("interruptflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("interruptflow.verify:428/await-input", func(ctx context.Context, f *workflow.Flow) error {
		resumeData, yield, err := f.Interrupt(map[string]any{"requestedInput": "userInput"})
		if yield || err != nil {
			return err
		}
		if ui, ok := resumeData["userInput"]; ok {
			f.Set("userInput", ui)
		}
		return nil
	})
	proxy.HandleTask("interruptflow.verify:428/compose", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", f.GetString("prompt")+", "+f.GetString("userInput"))
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy)
	eng.RunInTest(t)

	t.Run("interrupt_then_resume_completes_flow", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "interruptflow.verify:428/interrupt", map[string]any{"prompt": "Hello"}, nil)
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
