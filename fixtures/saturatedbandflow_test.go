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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestSaturatedbandflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// Two workflows: SaturatedBand (bounded) and OpenBand (open).
	satGraph := workflow.NewGraph("saturatedbandflow.verify:428/saturated-band")
	satGraph.AddTask("bounded", "saturatedbandflow.verify:428/bounded")
	satGraph.AddTransition("bounded", workflow.END)
	proxy.HandleGraph("saturatedbandflow.verify:428/saturated-band", satGraph)

	openGraph := workflow.NewGraph("saturatedbandflow.verify:428/open-band")
	openGraph.AddTask("open", "saturatedbandflow.verify:428/open")
	openGraph.AddTransition("open", workflow.END)
	proxy.HandleGraph("saturatedbandflow.verify:428/open-band", openGraph)

	const boundedCap = 2
	var mu sync.Mutex
	var boundedInFlight int
	var rejections atomic.Int32

	type completion struct {
		tag string
		at  time.Time
	}
	var completions []completion

	proxy.HandleTask("saturatedbandflow.verify:428/bounded", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		mu.Lock()
		boundedInFlight++
		over := boundedInFlight > boundedCap
		if over {
			boundedInFlight--
			rejections.Add(1)
		}
		mu.Unlock()
		if over {
			return errors.New("saturated", http.StatusTooManyRequests)
		}
		time.Sleep(80 * time.Millisecond)
		mu.Lock()
		boundedInFlight--
		completions = append(completions, completion{tag: "sat", at: time.Now()})
		mu.Unlock()
		return nil
	})
	proxy.HandleTask("saturatedbandflow.verify:428/open", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		time.Sleep(40 * time.Millisecond)
		mu.Lock()
		completions = append(completions, completion{tag: "open", at: time.Now()})
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithWorkers(6)
	eng.RunInTest(t)

	t.Run("low_priority_advances_while_high_priority_saturated", func(t *testing.T) {
		assert := testarossa.For(t)

		var keys []string
		for range 8 {
			k, _ := eng.Create(ctx, "saturatedbandflow.verify:428/saturated-band",
				nil, nil, &workflow.FlowOptions{Priority: 1})
			eng.Start(ctx, k)
			keys = append(keys, k)
		}
		for range 8 {
			k, _ := eng.Create(ctx, "saturatedbandflow.verify:428/open-band",
				nil, nil, &workflow.FlowOptions{Priority: 5})
			eng.Start(ctx, k)
			keys = append(keys, k)
		}

		for _, k := range keys {
			outcome, err := eng.Await(ctx, k)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}

		assert.True(rejections.Load() >= 1)

		mu.Lock()
		cs := make([]completion, len(completions))
		copy(cs, completions)
		mu.Unlock()

		assert.Equal(16, len(cs))

		var firstOpenAt, lastSatAt time.Time
		for _, c := range cs {
			if c.tag == "open" && (firstOpenAt.IsZero() || c.at.Before(firstOpenAt)) {
				firstOpenAt = c.at
			}
			if c.tag == "sat" && c.at.After(lastSatAt) {
				lastSatAt = c.at
			}
		}
		assert.True(firstOpenAt.Before(lastSatAt))
	})
}
