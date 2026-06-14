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
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestFairnessflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Fairness", "fairnessflow.verify:428/fairness")
	graph.AddTask("Tally", "fairnessflow.verify:428/tally")
	graph.AddTransition("Tally", workflow.END)
	proxy.HandleGraph("fairnessflow.verify:428/fairness", graph)

	var mu sync.Mutex
	var order []string

	proxy.HandleTask("fairnessflow.verify:428/tally", func(ctx context.Context, f *workflow.Flow) error {
		delayMs := f.GetInt("delayMs")
		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
		mu.Lock()
		order = append(order, f.GetString("tag"))
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy).
		WithWorkers(1)
	eng.RunInTest(t)

	t.Run("weighted_share_and_liveness", func(t *testing.T) {
		assert := testarossa.For(t)

		// Holder flow blocks the single worker so test flows queue up.
		holderKey, err := eng.Create(ctx, "fairnessflow.verify:428/fairness",
			map[string]any{"delayMs": 1500, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1, FairnessKey: "_holder"})
		assert.NoError(err)
		eng.Start(ctx, holderKey)

		time.Sleep(100 * time.Millisecond)

		// 40 heavy (weight 4) and 40 light (weight 1), interleaved, same priority.
		var keys []string
		for i := range 40 {
			hk, _ := eng.Create(ctx, "fairnessflow.verify:428/fairness",
				map[string]any{"delayMs": 8, "tag": "heavy"},
				&workflow.FlowOptions{Priority: 5, FairnessKey: "heavy", FairnessWeight: 4})
			eng.Start(ctx, hk)
			keys = append(keys, hk)

			lk, _ := eng.Create(ctx, "fairnessflow.verify:428/fairness",
				map[string]any{"delayMs": 8, "tag": "light"},
				&workflow.FlowOptions{Priority: 5, FairnessKey: "light", FairnessWeight: 1})
			eng.Start(ctx, lk)
			keys = append(keys, lk)

			_ = i
		}

		time.Sleep(400 * time.Millisecond)

		eng.Await(ctx, holderKey)
		for _, k := range keys {
			eng.Await(ctx, k)
		}

		mu.Lock()
		got := make([]string, len(order))
		copy(got, order)
		mu.Unlock()

		// Count heavy and light.
		var heavyCount, lightCount int
		for _, tag := range got {
			switch tag {
			case "heavy":
				heavyCount++
			case "light":
				lightCount++
			}
		}
		assert.Equal(40, heavyCount)
		assert.Equal(40, lightCount)

		// Light key makes progress before heavy key exhausted.
		firstLight := -1
		lastHeavy := -1
		for i, tag := range got {
			if tag == "light" && firstLight < 0 {
				firstLight = i
			}
			if tag == "heavy" {
				lastHeavy = i
			}
		}
		assert.True(firstLight < lastHeavy)
	})
}
