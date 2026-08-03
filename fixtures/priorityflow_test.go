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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestPriorityflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Priority")
	graph.SetEndpoint("Record", "priorityflow.verify:428/record")
	graph.AddTransition("Record", workflow.END)
	proxy.HandleGraph("priorityflow.verify:428/priority", graph)

	var mu sync.Mutex
	var order []string

	// The holder occupies the single worker until the test lets go, and reports the moment it has it. Both
	// halves are rendezvous rather than durations, because both are waits on ENGINE progress: "the worker is
	// mine" is a dispatch, and a fixed hold long enough to cover everything the test does before releasing is
	// a bet that reads as an ordering failure when it is lost, not as a timeout.
	var holdOnce, releaseOnce sync.Once
	holderRunning := make(chan struct{})
	releaseHolder := make(chan struct{})
	release := func() { releaseOnce.Do(func() { close(releaseHolder) }) }

	proxy.HandleTask("priorityflow.verify:428/record", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetBool("hold") {
			holdOnce.Do(func() { close(holderRunning) })
			<-releaseHolder
		}
		delayMs := f.GetInt("delayMs")
		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
		mu.Lock()
		order = append(order, f.GetString("tag"))
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	// Released here, after the Shutdown defer, so it unwinds FIRST: Shutdown drains the workers, and
	// one still holding this would never return.
	defer release()
	eng.SetHost(proxy)
	eng.SetWorkers(1)
	assert.NoError(eng.Startup(t.Context()))

	t.Run("strict_priority_ordering", func(t *testing.T) {
		assert := testarossa.For(t)
		mu.Lock()
		order = nil
		mu.Unlock()

		// The holder takes the one worker and keeps it until released, so every test flow below is created
		// while nothing can be dispatched.
		holderKey, err := eng.Create(ctx, "priorityflow.verify:428/priority",
			map[string]any{"hold": true, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1})
		assert.NoError(err)

		select {
		case <-holderRunning:
		case <-time.After(time.Minute):
			t.Fatal("the holder flow never reached its task, so the worker was never occupied")
		}

		// Create test flows with varying priorities. Each tag is its creation index so the
		// expected order can be derived by a stable sort on priority.
		type flow struct {
			tag      string
			priority int
		}
		flows := []flow{
			{"f0", 5}, {"f1", 2}, {"f2", 9}, {"f3", 2}, {"f4", 5}, {"f5", 3},
		}
		var keys []string
		for _, fl := range flows {
			k, err := eng.Create(ctx, "priorityflow.verify:428/priority",
				map[string]any{"delayMs": 50, "tag": fl.tag},
				&workflow.FlowOptions{Priority: fl.priority})
			assert.NoError(err)
			assert.NoError(err)
			keys = append(keys, k)
		}

		// Every test flow is committed pending; now let the candidate cache converge to strict order before
		// the holder frees the lone worker. Two pushing cycles, not a duration: each flow was doorbell-
		// admitted at creation and `Cache.Pop` ranks partitions by the band the FIRST arrival stamped, so
		// until a cycle has reconciled the partition against the plan the cache holds hints in creation
		// order that no plan chose. A cycle already in flight when these Creates committed may have scanned
		// before they existed, so the second is the one whose scan is guaranteed to have seen them.
		enginetest.AwaitShardCycles(t, eng, 1, 2)
		release()

		// Wait for all to complete.
		eng.Await(ctx, holderKey)
		for _, k := range keys {
			eng.Await(ctx, k)
		}

		mu.Lock()
		got := make([]string, len(order))
		copy(got, order)
		mu.Unlock()

		// Strict, with no allowance. The order was best-effort while the release was a duration: the
		// first-created flow could still be sitting in the cache as the "pioneer" the doorbell admitted into
		// an empty partition, ahead of anything a plan chose, and the test had to accept that as a second
		// valid ordering. Waiting for the partition to be RECONCILED removes the case rather than tolerating
		// it - a pushing cycle wholesale-replaces the partition with the plan, and no further doorbell
		// arrival can disturb it because these flows are single-task and spawn no successors. Intra-band
		// order is FIFO by step_id.
		stable := make([]flow, len(flows))
		copy(stable, flows)
		sort.SliceStable(stable, func(i, j int) bool { return stable[i].priority < stable[j].priority })
		strictOrder := []string{"holder"}
		for _, fl := range stable {
			strictOrder = append(strictOrder, fl.tag)
		}
		assert.Equal(strictOrder, got, "dispatch order")
	})
}
