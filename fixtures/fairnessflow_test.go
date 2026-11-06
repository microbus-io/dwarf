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
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestFairnessflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()

	graph := workflow.NewGraph("Fairness")
	graph.SetURL("Tally", "fairnessflow.verify:428/tally")
	graph.AddTransition("Tally", workflow.End)
	mux.HandleGraph("fairnessflow.verify:428/fairness", graph)

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

	mux.HandleTask("fairnessflow.verify:428/tally", func(ctx context.Context, f *workflow.Flow) error {
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
	eng.SetHost(mux)
	eng.SetWorkers(1)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	t.Run("weighted_share_and_liveness", func(t *testing.T) {
		assert := testarossa.For(t)

		// The holder takes the one worker and keeps it until released, so all the test flows below are
		// created while nothing can be dispatched - which is what makes the drain a single weighted lottery
		// over a settled backlog rather than a race between arrival and dispatch.
		holderKey, err := eng.Create(ctx, "fairnessflow.verify:428/fairness",
			map[string]any{"hold": true, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1, FairnessKey: "_holder"})
		assert.NoError(err)

		select {
		case <-holderRunning:
		case <-time.After(time.Minute * enginetest.TimeoutScale()):
			t.Fatal("the holder flow never reached its task, so the worker was never occupied")
		}

		// 60 heavy (weight 4) and 60 light (weight 1), interleaved, same priority. The count is sized for the
		// share assertion below, not for speed - see there.
		const perKey = 60
		var keys []string
		for range perKey {
			hk, err := eng.Create(ctx, "fairnessflow.verify:428/fairness",
				map[string]any{"delayMs": 8, "tag": "heavy"},
				&workflow.FlowOptions{Priority: 5, FairnessKey: "heavy", FairnessWeight: 4})
			if !assert.NoError(err) {
				return
			}
			keys = append(keys, hk)

			lk, err := eng.Create(ctx, "fairnessflow.verify:428/fairness",
				map[string]any{"delayMs": 8, "tag": "light"},
				&workflow.FlowOptions{Priority: 5, FairnessKey: "light", FairnessWeight: 1})
			if !assert.NoError(err) {
				return
			}
			keys = append(keys, lk)
		}

		// Both keys are committed pending; now let the cache be reconciled against the plan before the
		// holder frees the lone worker, so the share below is decided by the weighted pick and not by which
		// flows the doorbell happened to admit first. Two pushing cycles, not a duration - a cycle already
		// in flight when these Creates committed may have scanned before they existed.
		enginetest.AwaitShardCycles(t, eng, 2)
		release()

		enginetest.AwaitAndAssertComplete(t, eng, holderKey)
		for _, k := range keys {
			enginetest.AwaitAndAssertComplete(t, eng, k)
		}

		mu.Lock()
		got := make([]string, len(order))
		copy(got, order)
		mu.Unlock()

		var heavyCount, lightCount int
		for _, tag := range got {
			switch tag {
			case "heavy":
				heavyCount++
			case "light":
				lightCount++
			}
		}
		assert.Equal(perKey, heavyCount)
		assert.Equal(perKey, lightCount)

		// The share and the liveness are both read off ONE statistic: how many light flows were dispatched
		// before heavy's last. While both keys have backlog every dispatch is an independent weighted draw
		// (heavy with p=4/5), so that count is negative-binomial - heavy's 60 successes against light's 1-in-5
		// - with mean 15. A key whose weight were ignored (1:1) would put it near 56; weight acting as strict
		// precedence would make it 0, light starved until heavy ran dry. The lottery is genuinely random, so
		// the bounds are chosen from its exact distribution, not from observed runs: at 4:1 each side fails
		// spuriously ~1.5e-6 of the time, while a 1:1 regression still passes only ~3% of runs and starvation
		// never. Do not tighten them toward the observed spread, and do not add a check on an early window or
		// a mean-index gap: over a backlog this size either one fails ~1 run in 1,000 by chance alone, and
		// still misses a 1:1 regression more often than this one. More flows is the only lever that narrows
		// both errors at once. Counting only BEFORE heavy's last dispatch also keeps the tail out of it, where a
		// stale tally can plan a slot for an exhausted key.
		lastHeavy := -1
		for i, tag := range got {
			if tag == "heavy" {
				lastHeavy = i
			}
		}
		var lightBeforeLastHeavy int
		for _, tag := range got[:lastHeavy+1] {
			if tag == "light" {
				lightBeforeLastHeavy++
			}
		}
		t.Logf("light dispatched before heavy's last: %d (expected ~15 at 4:1)", lightBeforeLastHeavy)
		assert.True(lightBeforeLastHeavy >= 1,
			"light (weight 1) was starved until heavy (weight 4) ran out: weight is acting as precedence, not share")
		assert.True(lightBeforeLastHeavy <= 40,
			"weighted 4:1 share too weak (may have regressed toward 1:1): %d light dispatched before heavy's last, expected ~15", lightBeforeLastHeavy)
	})
}
