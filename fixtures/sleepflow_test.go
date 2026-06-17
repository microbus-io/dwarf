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

func TestSleepflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Delay")
	graph.SetEndpoint("TaskA", "sleepflow.verify:428/task-a")
	graph.SetEndpoint("TaskB", "sleepflow.verify:428/task-b")
	graph.SetEndpoint("TaskC", "sleepflow.verify:428/task-c")
	graph.AddTransitionChain("TaskA", "TaskB", "TaskC", workflow.END)
	proxy.HandleGraph("sleepflow.verify:428/delay", graph)

	proxy.HandleTask("sleepflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("sleepflow.verify:428/task-b", func(ctx context.Context, f *workflow.Flow) error {
		f.Sleep(f.GetDuration("sleepFor"))
		f.SetBool("marked", true)
		return nil
	})
	proxy.HandleTask("sleepflow.verify:428/task-c", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("flow_sleeps_for_configured_duration", func(t *testing.T) {
		assert := testarossa.For(t)

		sleepFor := 100 * time.Millisecond
		initialState := map[string]any{"sleepFor": sleepFor}
		start := time.Now()
		_, outcome, err := eng.Run(ctx, "sleepflow.verify:428/delay", initialState, nil)
		elapsed := time.Since(start)

		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(true, outcome.State["marked"])
		assert.True(elapsed >= sleepFor)
	})
}
