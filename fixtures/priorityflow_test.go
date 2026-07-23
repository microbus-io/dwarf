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
	"slices"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestPriorityflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Priority")
	graph.SetEndpoint("Record", "priorityflow.verify:428/record")
	graph.AddTransition("Record", workflow.END)
	proxy.HandleGraph("priorityflow.verify:428/priority", graph)

	var mu sync.Mutex
	var order []string

	proxy.HandleTask("priorityflow.verify:428/record", func(ctx context.Context, f *workflow.Flow) error {
		delayMs := f.GetInt("delayMs")
		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
		mu.Lock()
		order = append(order, f.GetString("tag"))
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.SetWorkers(1)
	eng.RunInTest(t)

	t.Run("strict_priority_ordering", func(t *testing.T) {
		assert := testarossa.For(t)
		mu.Lock()
		order = nil
		mu.Unlock()

		// Holder flow at priority 1 with long delay to fill the single worker.
		holderKey, err := eng.Create(ctx, "priorityflow.verify:428/priority",
			map[string]any{"delayMs": 1500, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1})
		assert.NoError(err)
		assert.NoError(err)

		time.Sleep(100 * time.Millisecond)

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

		// Let every test flow commit pending and the candidate cache converge to strict order before the
		// holder frees the lone worker. Creating in a tight burst (no inter-create spacing) keeps the
		// refiller from caching the first flow alone as a pioneer; intra-band order is FIFO by step_id.
		time.Sleep(300 * time.Millisecond)

		// Wait for all to complete.
		eng.Await(ctx, holderKey)
		for _, k := range keys {
			eng.Await(ctx, k)
		}

		mu.Lock()
		got := make([]string, len(order))
		copy(got, order)
		mu.Unlock()

		// Priority is best-effort, not a structural guarantee: the dispatcher trades exact ordering for
		// throughput. The first-created test flow (f0) is the one flow that can briefly be the sole
		// pending candidate while the holder occupies the worker, so the refiller may cache it as a
		// "pioneer" and dispatch it ahead of the strict band order. Every other flow drains in strict
		// priority, FIFO (by step_id) within a band. So exactly two orderings are valid: strict, or
		// strict with f0 pioneered to the front of the test set.
		stable := make([]flow, len(flows))
		copy(stable, flows)
		sort.SliceStable(stable, func(i, j int) bool { return stable[i].priority < stable[j].priority })
		strictOrder := []string{"holder"}
		for _, fl := range stable {
			strictOrder = append(strictOrder, fl.tag)
		}
		pioneerOrder := []string{"holder", flows[0].tag}
		for _, fl := range stable {
			if fl.tag != flows[0].tag {
				pioneerOrder = append(pioneerOrder, fl.tag)
			}
		}
		assert.Equal("holder", got[0])
		assert.True(slices.Equal(got, strictOrder) || slices.Equal(got, pioneerOrder),
			"got %v, want strict %v or pioneer %v", got, strictOrder, pioneerOrder)
	})
}
