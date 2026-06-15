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
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestBreakerstartparkflow verifies that Start parks the entry step of a flow whose task already has a
// tripped breaker, so it joins the held-back backlog instead of being dispatched straight into the
// known-bad endpoint. A flow started after the trip must not have its task executed while the breaker
// stays tripped (its turn comes only when it becomes the oldest probe).
func TestBreakerstartparkflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Flow", "breakerstartparkflow.verify:428/flow")
	graph.AddTask("Work", "breakerstartparkflow.verify:428/work")
	graph.AddTransition("Work", workflow.END)
	proxy.HandleGraph("breakerstartparkflow.verify:428/flow", graph)

	// The task records each flow's marker, then always fails with an ack-timeout 404 so the breaker
	// trips and stays tripped.
	var mu sync.Mutex
	seen := map[string]bool{}
	proxy.HandleTask("breakerstartparkflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		mu.Lock()
		seen[f.GetString("marker")] = true
		mu.Unlock()
		return workflow.ErrUnavailable(errors.New("ack timeout: breakerstartparkflow.verify:428/work", http.StatusNotFound), "ack_timeout")
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.SetWorkers(4)
	eng.RunInTest(t)

	// Trip the breaker with a batch of older flows.
	for range 5 {
		k, err := eng.Create(ctx, "breakerstartparkflow.verify:428/flow", map[string]any{"marker": "pre"}, nil)
		assert.NoError(err)
		eng.Start(ctx, k)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if eng.BreakerTripped("breakerstartparkflow.verify:428/work") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !assert.True(eng.BreakerTripped("breakerstartparkflow.verify:428/work")) {
		return
	}

	// Start a fresh flow AFTER the trip. Its entry step must be born parked, not dispatched.
	postKey, err := eng.Create(ctx, "breakerstartparkflow.verify:428/flow", map[string]any{"marker": "post"}, nil)
	if !assert.NoError(err) {
		return
	}
	if !assert.NoError(eng.Start(ctx, postKey)) {
		return
	}

	// Give the refiller ample time to (wrongly) dispatch the post flow if it were left selectable.
	time.Sleep(2 * time.Second)

	mu.Lock()
	postExecuted := seen["post"]
	mu.Unlock()
	assert.False(postExecuted)
}
