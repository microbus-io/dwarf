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
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestErrorflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("errorflow.verify:428/error")
	graph.AddTask("taskA", "errorflow.verify:428/task-a")
	graph.AddTask("taskB", "errorflow.verify:428/task-b")
	graph.AddTask("handler", "errorflow.verify:428/handler")
	graph.AddTask("taskC", "errorflow.verify:428/task-c")
	graph.AddTransition("taskA", "taskB")
	graph.AddTransitionOnError("taskB", "handler")
	graph.AddTransition("taskB", "taskC")
	graph.AddTransition("handler", "taskC")
	graph.AddTransition("taskC", workflow.END)
	proxy.HandleGraph("errorflow.verify:428/error", graph)

	proxy.HandleTask("errorflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("errorflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("trigger") == "fail" {
			return errors.New("triggered failure")
		}
		f.SetString("result", "normal")
		return nil
	})
	proxy.HandleTask("errorflow.verify:428/handler", func(ctx context.Context, f *workflow.Flow) error {
		var onErr errors.TracedError
		err := f.Get("onErr", &onErr)
		if err != nil || onErr.Error() == "" {
			f.SetString("result", "recovered:no-error")
		} else {
			f.SetString("result", "recovered:"+onErr.Error())
		}
		return nil
	})
	proxy.HandleTask("errorflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("finalResult", "final:"+f.GetString("result"))
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("normal_path", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"trigger": "ok"}
		outcome, err := eng.Run(ctx, "errorflow.verify:428/error", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("final:normal", outcome.State["finalResult"])
	})

	t.Run("error_handled_path", func(t *testing.T) {
		assert := testarossa.For(t)

		initialState := map[string]any{"trigger": "fail"}
		outcome, err := eng.Run(ctx, "errorflow.verify:428/error", initialState, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		finalResult, _ := outcome.State["finalResult"].(string)
		assert.True(strings.HasPrefix(finalResult, "final:recovered:triggered failure"))
	})
}
