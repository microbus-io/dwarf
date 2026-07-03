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
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Fairness")
	graph.SetEndpoint("Tally", "fairnessflow.verify:428/tally")
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

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.SetWorkers(1)
	eng.RunInTest(t)

	t.Run("weighted_share_and_liveness", func(t *testing.T) {
		assert := testarossa.For(t)

		// Holder flow blocks the single worker so test flows queue up.
		holderKey, err := eng.Create(ctx, "fairnessflow.verify:428/fairness",
			map[string]any{"delayMs": 1500, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1, FairnessKey: "_holder"})
		assert.NoError(err)

		time.Sleep(100 * time.Millisecond)

		// 40 heavy (weight 4) and 40 light (weight 1), interleaved, same priority.
		var keys []string
		for i := range 40 {
			hk, _ := eng.Create(ctx, "fairnessflow.verify:428/fairness",
				map[string]any{"delayMs": 8, "tag": "heavy"},
				&workflow.FlowOptions{Priority: 5, FairnessKey: "heavy", FairnessWeight: 4})
			keys = append(keys, hk)

			lk, _ := eng.Create(ctx, "fairnessflow.verify:428/fairness",
				map[string]any{"delayMs": 8, "tag": "light"},
				&workflow.FlowOptions{Priority: 5, FairnessKey: "light", FairnessWeight: 1})
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

		// Weighted 4:1 share (the point of FairnessWeight): heavy (weight 4) is dispatched preferentially
		// over light (weight 1), so heavy work clusters earlier in the dispatch order. The earlier assertions
		// (all 80 complete, some interleaving) would still pass if weights were ignored and the two keys
		// interleaved evenly - these two checks are what actually pin the ratio. If the 4:1 semantics
		// regressed to 1:1, mean dispatch indices would converge (~39.5 each) and the early window would be
		// ~even, failing both. The thresholds are loose relative to the observed signal (meanHeavy~26.5 vs
		// meanLight~54.5; first-25 heavy:light ~20:4) so ordinary scheduler jitter never trips them.
		var sumHeavy, sumLight int
		var earlyHeavy, earlyLight int
		const earlyWindow = 25
		for i, tag := range got {
			switch tag {
			case "heavy":
				sumHeavy += i
				if i < earlyWindow {
					earlyHeavy++
				}
			case "light":
				sumLight += i
				if i < earlyWindow {
					earlyLight++
				}
			}
		}
		meanHeavy := float64(sumHeavy) / float64(heavyCount)
		meanLight := float64(sumLight) / float64(lightCount)
		t.Logf("meanHeavyIdx=%.1f meanLightIdx=%.1f earlyHeavy=%d earlyLight=%d", meanHeavy, meanLight, earlyHeavy, earlyLight)
		assert.True(meanLight-meanHeavy >= 10,
			"weighted 4:1 share too weak (may have regressed toward 1:1): meanHeavyIdx=%.1f meanLightIdx=%.1f", meanHeavy, meanLight)
		assert.True(earlyHeavy >= 2*earlyLight,
			"heavy (weight 4) should dominate the first %d dispatches ~4:1: earlyHeavy=%d earlyLight=%d", earlyWindow, earlyHeavy, earlyLight)
	})
}
