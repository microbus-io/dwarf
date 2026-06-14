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
	"math/rand/v2"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestSoakflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	soakGraph := workflow.NewGraph("Soak", "soakflow.verify:428/soak")
	soakGraph.AddTask("Seed", "soakflow.verify:428/seed")
	soakGraph.AddTask("FanA", "soakflow.verify:428/fan-a")
	soakGraph.AddTask("Work", "soakflow.verify:428/work")
	soakGraph.AddTask("Collect", "soakflow.verify:428/collect")
	soakGraph.AddTask("Loop", "soakflow.verify:428/loop")
	soakGraph.AddTask("Sub", "soakflow.verify:428/sub")
	soakGraph.AddTask("BoomR", "soakflow.verify:428/boom-r")
	soakGraph.AddTask("Recover", "soakflow.verify:428/recover")
	soakGraph.AddTask("BoomF", "soakflow.verify:428/boom-f")
	soakGraph.AddTask("Join", "soakflow.verify:428/join")
	soakGraph.SetFanIn("Collect")
	soakGraph.SetFanIn("Join")
	soakGraph.SetReducer("work", workflow.ReducerAdd)
	soakGraph.AddTransitionWhen("Seed", "FanA", "branch == 0")
	soakGraph.AddTransitionWhen("Seed", "Loop", "branch == 1")
	soakGraph.AddTransitionWhen("Seed", "Sub", "branch == 2")
	soakGraph.AddTransitionWhen("Seed", "BoomR", "branch == 3")
	soakGraph.AddTransitionWhen("Seed", "BoomF", "branch == 4")
	soakGraph.AddTransitionForEach("FanA", "Work", "items", "item")
	soakGraph.AddTransition("Work", "Collect")
	soakGraph.AddTransition("Collect", "Join")
	soakGraph.AddTransitionGoto("Loop", "Loop")
	soakGraph.AddTransition("Loop", "Join")
	soakGraph.AddTransition("Sub", "Join")
	soakGraph.AddTransitionOnError("BoomR", "Recover")
	soakGraph.AddTransition("BoomR", "Join")
	soakGraph.AddTransition("Recover", "Join")
	soakGraph.AddTransition("BoomF", "Join")
	soakGraph.AddTransition("Join", workflow.END)
	proxy.HandleGraph("soakflow.verify:428/soak", soakGraph)

	innerGraph := workflow.NewGraph("Inner", "soakflow.verify:428/inner")
	innerGraph.AddTask("InnerEntry", "soakflow.verify:428/inner-entry")
	innerGraph.AddTransition("InnerEntry", workflow.END)
	proxy.HandleGraph("soakflow.verify:428/inner", innerGraph)

	proxy.HandleTask("soakflow.verify:428/seed", func(ctx context.Context, f *workflow.Flow) error {
		branch := f.GetInt("branch") % 5
		f.SetInt("branch", branch)
		fanWidth := f.GetInt("fanWidth")
		if fanWidth < 1 {
			fanWidth = 1
		}
		if fanWidth > 6 {
			fanWidth = 6
		}
		items := make([]int, fanWidth)
		for i := range items {
			items[i] = i
		}
		f.Set("items", items)
		loops := f.GetInt("loops")
		if loops < 0 {
			loops = 0
		}
		if loops > 5 {
			loops = 5
		}
		f.SetInt("loopsLeft", loops)
		return nil
	})
	proxy.HandleTask("soakflow.verify:428/fan-a", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("soakflow.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetInt("work", 1)
		return nil
	})
	proxy.HandleTask("soakflow.verify:428/collect", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("soakflow.verify:428/loop", func(ctx context.Context, f *workflow.Flow) error {
		left := f.GetInt("loopsLeft") - 1
		f.SetInt("loopsLeft", left)
		if left > 0 {
			f.Goto("Loop")
		}
		return nil
	})
	proxy.HandleTask("soakflow.verify:428/sub", func(ctx context.Context, f *workflow.Flow) error {
		_, yield, err := f.Subgraph("soakflow.verify:428/inner", nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("soakflow.verify:428/boom-r", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("soak boom (recoverable)")
	})
	proxy.HandleTask("soakflow.verify:428/recover", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("soakflow.verify:428/boom-f", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("soak boom (fatal)")
	})
	proxy.HandleTask("soakflow.verify:428/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("soakflow.verify:428/inner-entry", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := engine.NewEngine().
		WithHost(proxy).
		WithNumShards(2).
		WithWorkers(4)
	eng.RunInTest(t)

	t.Run("all_flows_terminate", func(t *testing.T) {
		assert := testarossa.For(t)

		rng := rand.New(rand.NewPCG(12345, 67890))

		type started struct {
			key    string
			branch int
		}
		var active []started
		soakWindow := 5 * time.Second
		deadline := time.Now().Add(soakWindow)
		total := 0

		for time.Now().Before(deadline) {
			branch := rng.IntN(5)
			fanWidth := rng.IntN(7)
			loops := rng.IntN(6)
			state := map[string]any{"branch": branch, "fanWidth": fanWidth, "loops": loops}
			k, err := eng.Create(ctx, "soakflow.verify:428/soak", state, nil)
			if !assert.NoError(err) {
				return
			}
			eng.Start(ctx, k)
			active = append(active, started{key: k, branch: branch})
			total++

			if len(active) > 128 {
				var remaining []started
				for _, s := range active {
					outcome, _ := eng.Snapshot(ctx, s.key)
					if outcome != nil && (outcome.Status == workflow.StatusCompleted || outcome.Status == workflow.StatusFailed || outcome.Status == workflow.StatusCancelled) {
						continue
					}
					remaining = append(remaining, s)
				}
				active = remaining
			}
		}

		// Drain: wait for all to finish. The wait is bounded by lack of *progress*, not a fixed clock, so
		// a slow backend (the backlog drains slower) keeps waiting as long as flows keep terminating;
		// only a sustained stall (genuinely stuck flows) fails the test.
		lastActive := len(active)
		lastProgress := time.Now()
		for len(active) > 0 {
			var remaining []started
			for _, s := range active {
				outcome, _ := eng.Snapshot(ctx, s.key)
				if outcome != nil && (outcome.Status == workflow.StatusCompleted || outcome.Status == workflow.StatusFailed || outcome.Status == workflow.StatusCancelled) {
					continue
				}
				remaining = append(remaining, s)
			}
			active = remaining
			if len(active) < lastActive {
				lastActive = len(active)
				lastProgress = time.Now()
			} else if time.Since(lastProgress) > 60*time.Second {
				break // no flow has terminated in 60s: a real stall, not just a slow server
			}
			if len(active) > 0 {
				time.Sleep(50 * time.Millisecond)
			}
		}

		assert.Equal(0, len(active), "stuck flows: %d of %d", len(active), total)
	})
}
