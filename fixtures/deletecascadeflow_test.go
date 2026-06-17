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
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestDeletecascadeflow verifies that Delete cascades into a flow's subgraph descendants, recursively.
// A subgraph child's only inbound reference is its parent's surgraph step, so deleting the parent
// without the child would strand the child. We run a parent -> subgraph -> nested-subgraph chain to
// completion, capture a step key from each child off the carrier, delete the root, and assert every
// descendant's step can no longer be loaded.
func TestDeletecascadeflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	var mu sync.Mutex
	childStepKeys := map[string]string{} // task name -> step key

	record := func(name string, f *workflow.Flow) {
		mu.Lock()
		childStepKeys[name] = f.StepKey()
		mu.Unlock()
	}

	// Root: TaskA -> RunChild -> TaskZ
	root := workflow.NewGraph("Root")
	root.SetEndpoint("TaskA", "deletecascadeflow.verify:428/task-a")
	root.SetEndpoint("RunChild", "deletecascadeflow.verify:428/run-child")
	root.SetEndpoint("TaskZ", "deletecascadeflow.verify:428/task-z")
	root.AddTransitionChain("TaskA", "RunChild", "TaskZ", workflow.END)
	proxy.HandleGraph("deletecascadeflow.verify:428/root", root)

	// Child: ChildWork -> RunGrandchild
	child := workflow.NewGraph("Child")
	child.SetEndpoint("ChildWork", "deletecascadeflow.verify:428/child-work")
	child.SetEndpoint("RunGrandchild", "deletecascadeflow.verify:428/run-grandchild")
	child.AddTransitionChain("ChildWork", "RunGrandchild", workflow.END)
	proxy.HandleGraph("deletecascadeflow.verify:428/child", child)

	// Grandchild: GrandchildWork
	grandchild := workflow.NewGraph("Grandchild")
	grandchild.SetEndpoint("GrandchildWork", "deletecascadeflow.verify:428/grandchild-work")
	grandchild.AddTransition("GrandchildWork", workflow.END)
	proxy.HandleGraph("deletecascadeflow.verify:428/grandchild", grandchild)

	proxy.HandleTask("deletecascadeflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("deletecascadeflow.verify:428/task-z", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("deletecascadeflow.verify:428/run-child", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("deletecascadeflow.verify:428/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("deletecascadeflow.verify:428/child-work", func(ctx context.Context, f *workflow.Flow) error {
		record("ChildWork", f)
		return nil
	})
	proxy.HandleTask("deletecascadeflow.verify:428/run-grandchild", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("deletecascadeflow.verify:428/grandchild", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("deletecascadeflow.verify:428/grandchild-work", func(ctx context.Context, f *workflow.Flow) error {
		record("GrandchildWork", f)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	flowKey, outcome, err := eng.Run(ctx, "deletecascadeflow.verify:428/root", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	mu.Lock()
	childKey := childStepKeys["ChildWork"]
	grandchildKey := childStepKeys["GrandchildWork"]
	mu.Unlock()
	assert.NotEqual("", childKey)
	assert.NotEqual("", grandchildKey)

	// Both descendant steps are loadable before the delete.
	_, err = eng.Step(ctx, childKey)
	assert.NoError(err)
	_, err = eng.Step(ctx, grandchildKey)
	assert.NoError(err)

	// Delete the root flow.
	err = eng.Delete(ctx, flowKey)
	assert.NoError(err)

	// The root and every subgraph descendant are gone.
	_, err = eng.Snapshot(ctx, flowKey)
	assert.Error(err)
	_, err = eng.Step(ctx, childKey)
	assert.Error(err)
	_, err = eng.Step(ctx, grandchildKey)
	assert.Error(err)
}
