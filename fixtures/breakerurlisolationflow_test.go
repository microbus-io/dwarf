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

/*
The per-task breaker must key on the dispatch URL (the real downstream), not the graph node name. Two
graphs can use the SAME node name for tasks pointing at DIFFERENT URLs; a breaker tripped for the down
endpoint must NOT park flows whose same-named node points at a healthy endpoint.

This fixture builds exactly that collision: both graphs name their node "Shared", but one dispatches to a
down URL (always trips the breaker) and the other to a healthy URL. We trip the down breaker, then run a
flow on the healthy graph and assert it completes. With a name-keyed breaker (the bug) the healthy flow's
"Shared" step is parked under the down endpoint's breaker and never runs; with a URL-keyed breaker it has
its own untripped breaker and completes.
*/
package fixtures

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestBreakerurlisolationflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := engine.NewTestProxy()

	// Two graphs, SAME node name "Shared", DIFFERENT dispatch URLs.
	downGraph := workflow.NewGraph("DownFlow", "breakerurlisolationflow.verify:428/down-flow")
	downGraph.AddTask("Shared", "breakerurlisolationflow.verify:428/down")
	downGraph.AddTransition("Shared", workflow.END)
	proxy.HandleGraph("breakerurlisolationflow.verify:428/down-flow", downGraph)

	upGraph := workflow.NewGraph("UpFlow", "breakerurlisolationflow.verify:428/up-flow")
	upGraph.AddTask("Shared", "breakerurlisolationflow.verify:428/up")
	upGraph.AddTransition("Shared", workflow.END)
	proxy.HandleGraph("breakerurlisolationflow.verify:428/up-flow", upGraph)

	var downCalled atomic.Bool
	proxy.HandleTask("breakerurlisolationflow.verify:428/down", func(ctx context.Context, f *workflow.Flow) error {
		downCalled.Store(true)
		// "I cannot serve right now" - trips the breaker for this URL.
		return workflow.ErrBreakerTrip(errors.New("down", http.StatusServiceUnavailable), "unavailable")
	})
	proxy.HandleTask("breakerurlisolationflow.verify:428/up", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("result", "ok")
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	// 1. Start a flow on the down graph. Its task trips the breaker; the step parks (and, being created
	//    first, stays the oldest parked step for node name "Shared", so a name-keyed breaker keeps electing
	//    it as the probe).
	downKey, err := eng.Create(ctx, "breakerurlisolationflow.verify:428/down-flow", nil, nil)
	if !assert.NoError(err) {
		return
	}
	if !assert.NoError(eng.Start(ctx, downKey)) {
		return
	}

	// 2. Wait until the breaker has tripped. The key differs by implementation (node name "Shared" in the
	//    buggy version, URL "...down" once fixed), so accept either.
	tripped := waitUntil(2*time.Second, func() bool {
		return downCalled.Load() &&
			(eng.BreakerTripped("Shared") || eng.BreakerTripped("breakerurlisolationflow.verify:428/down"))
	})
	if !assert.True(tripped, "down breaker should have tripped") {
		return
	}

	// 3. Run a flow on the healthy graph. Its node is also named "Shared" but dispatches to the healthy
	//    URL. With a URL-keyed breaker it completes immediately; with a name-keyed breaker it is parked
	//    behind the down endpoint's breaker and never runs.
	upKey, err := eng.Create(ctx, "breakerurlisolationflow.verify:428/up-flow", nil, nil)
	if !assert.NoError(err) {
		return
	}
	if !assert.NoError(eng.Start(ctx, upKey)) {
		return
	}

	awaitCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := eng.Await(awaitCtx, upKey)
	if !assert.NoError(err, "healthy flow should not be blocked by an unrelated endpoint's breaker") {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	assert.Equal("ok", out.State["result"])
}

// waitUntil polls cond until it returns true or the timeout elapses.
func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
