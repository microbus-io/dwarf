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

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestFailedfanoutflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	graph := workflow.NewGraph("FailedFanOut")
	graph.SetEndpoint("Src", "failedfanoutflow.verify:428/src")
	graph.SetEndpoint("A", "failedfanoutflow.verify:428/a")
	graph.SetEndpoint("B", "failedfanoutflow.verify:428/b")
	graph.SetEndpoint("C", "failedfanoutflow.verify:428/c")
	graph.SetEndpoint("J", "failedfanoutflow.verify:428/j")
	graph.SetFanIn("J")
	graph.SetReducer("executed", workflow.ReducerAdd)
	graph.AddTransition("Src", "A")
	graph.AddTransition("Src", "B")
	graph.AddTransition("Src", "C")
	graph.AddTransition("A", "J")
	graph.AddTransition("B", "J")
	graph.AddTransitionChain("C", "J", workflow.END)
	commonProxy.HandleGraph("failedfanoutflow.verify:428/failed-fan-out", graph)

	commonProxy.HandleTask("failedfanoutflow.verify:428/src", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	commonProxy.HandleTask("failedfanoutflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("executed", 1)
		return nil
	})
	commonProxy.HandleTask("failedfanoutflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("triggered failure in B")
	})
	commonProxy.HandleTask("failedfanoutflow.verify:428/c", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("executed", 1)
		return nil
	})
	commonProxy.HandleTask("failedfanoutflow.verify:428/j", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	t.Run("failing_branch_fails_the_flow", func(t *testing.T) {
		assert := testarossa.For(t)

		_, outcome, err := commonEngine.Run(ctx, "failedfanoutflow.verify:428/failed-fan-out", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, outcome.Status)
	})
}
