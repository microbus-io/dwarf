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

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestRefillPace_LightLoadUnpaced pins the pacing gate's light-load-inert property: pacing applies only
// after a refill that filled the cache to capacity, so a sequential flow - whose refill batches are far
// below capacity - never waits out a pace interval between steps. The pace is set absurdly high (2s);
// if the gate ever paced a partial batch, a 5-step flow would take >10s and blow the deadline.
func TestRefillPace_LightLoadUnpaced(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Chain")
	names := []string{"A", "B", "C", "D", "E", workflow.END}
	for _, n := range names[:5] {
		g.SetEndpoint(n, "refillpace.verify:428/nop")
	}
	g.AddTransitionChain(names...)
	proxy.HandleGraph("refillpace.verify:428/chain", g)
	proxy.HandleTask("refillpace.verify:428/nop", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.refillPace = 2 * time.Second // before Startup; would dominate the runtime if the gate misfired
	e.RunInTest(t)

	start := time.Now()
	_, outcome, err := e.Run(ctx, "refillpace.verify:428/chain", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.True(time.Since(start) < 2*time.Second, "light-load flow must not absorb a pace interval (took %v)", time.Since(start))
}

// TestRefillPace_DeepBacklogLiveness pins that pacing cannot wedge a deep backlog: with a single worker
// (cache capacity 2, so every refill is full and every cycle is paced) and a backlog of flows several
// times the capacity, everything still completes - the armed trigger resumes the refiller after each
// pause, and the pause only bounds scan frequency, never delivery.
func TestRefillPace_DeepBacklogLiveness(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("One")
	g.SetEndpoint("Do", "refillpaceliveness.verify:428/nop")
	g.AddTransition("Do", workflow.END)
	proxy.HandleGraph("refillpaceliveness.verify:428/one", g)
	proxy.HandleTask("refillpaceliveness.verify:428/nop", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngine()
	e.SetHost(proxy)
	e.SetWorkers(1)                      // capacity 2: a backlog of 10 keeps every refill full
	e.refillPace = 50 * time.Millisecond // the over-pacing regime; must still drain, just slower
	e.RunInTest(t)

	keys := make([]string, 10)
	for i := range keys {
		k, err := e.Create(ctx, "refillpaceliveness.verify:428/one", nil, nil)
		assert.NoError(err)
		keys[i] = k
	}
	deadline, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for _, k := range keys {
		outcome, err := e.Await(deadline, k)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}
}
