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
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestFairnessflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Fairness")
	graph.SetEndpoint("Tally", "fairnessflow.verify:428/tally")
	graph.AddTransition("Tally", workflow.END)
	proxy.HandleGraph("fairnessflow.verify:428/fairness", graph)

	var mu sync.Mutex
	var order []string

	// The holder occupies the single worker until the test lets go, and reports the moment it has it. Both
	// halves are rendezvous rather than durations, because both are waits on ENGINE progress: "the worker is
	// mine" is a dispatch, and a fixed hold long enough to cover 80 Creates plus the cache converging is a
	// bet that reads as a share imbalance when it is lost, not as a timeout.
	var holdOnce, releaseOnce sync.Once
	holderRunning := make(chan struct{})
	releaseHolder := make(chan struct{})
	release := func() { releaseOnce.Do(func() { close(releaseHolder) }) }

	proxy.HandleTask("fairnessflow.verify:428/tally", func(ctx context.Context, f *workflow.Flow) error {
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

	t.Run("weighted_share_and_liveness", func(t *testing.T) {
		assert := testarossa.For(t)

		// The holder takes the one worker and keeps it until released, so all 80 test flows below are
		// created while nothing can be dispatched - which is what makes the drain a single weighted lottery
		// over a settled backlog rather than a race between arrival and dispatch.
		holderKey, err := eng.Create(ctx, "fairnessflow.verify:428/fairness",
			map[string]any{"hold": true, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1, FairnessKey: "_holder"})
		assert.NoError(err)

		select {
		case <-holderRunning:
		case <-time.After(time.Minute):
			t.Fatal("the holder flow never reached its task, so the worker was never occupied")
		}

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

		// Both keys are committed pending; now let the cache be reconciled against the plan before the
		// holder frees the lone worker, so the share below is decided by the weighted pick and not by which
		// flows the doorbell happened to admit first. Two pushing cycles, not a duration - a cycle already
		// in flight when these Creates committed may have scanned before they existed.
		enginetest.AwaitShardCycles(t, eng, 1, 2)
		release()

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
		// interleaved evenly. The mean-dispatch-index gap is the primary ratio guard: if the 4:1 semantics
		// regressed to 1:1 the means would converge (~39.5 each, gap ~0), far below the >=10 threshold
		// (observed gap ~28, with 25-run range ~20..37, so ordinary jitter never trips it). The early-window
		// check is a secondary, non-brittle confirmation that heavy *leads* early: with weighted sampling the
		// first-25 counts are noisy (heavy 14..24, light 0..10 across runs), so a fixed 2:1 ratio there flaked
		// when heavy dipped to ~14-15; only the direction (heavy > light early, observed gap always >=4) is
		// asserted, leaving the ratio magnitude to the robust mean gap above.
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
		assert.True(earlyHeavy > earlyLight,
			"heavy (weight 4) should lead light in the first %d dispatches: earlyHeavy=%d earlyLight=%d", earlyWindow, earlyHeavy, earlyLight)
	})
}
