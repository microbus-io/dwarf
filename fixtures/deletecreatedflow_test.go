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

// TestDeletecreatedflow verifies that Delete refuses only a running flow, not a created (never-started)
// one. A flow sitting in created has no in-flight work, so deleting it is safe and must be permitted;
// only running flows are protected.
func TestDeletecreatedflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("deletecreatedflow.verify:428/flow")
	graph.AddTask("work", "deletecreatedflow.verify:428/work")
	graph.AddTransition("work", workflow.END)
	proxy.HandleGraph("deletecreatedflow.verify:428/flow", graph)
	proxy.HandleTask("deletecreatedflow.verify:428/work", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	// Create but never Start: the flow is in created status.
	flowKey, err := eng.Create(ctx, "deletecreatedflow.verify:428/flow", nil, nil, nil)
	if !assert.NoError(err) {
		return
	}

	// Delete must succeed on a created flow.
	err = eng.Delete(ctx, flowKey)
	assert.NoError(err)

	// The flow must be gone.
	_, err = eng.Snapshot(ctx, flowKey)
	assert.Error(err)
}
