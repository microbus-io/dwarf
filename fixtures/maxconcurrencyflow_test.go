/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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

func TestMaxconcurrencyflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("maxconcurrencyflow.verify:428/max-concurrency")
	graph.AddTask("bounded", "maxconcurrencyflow.verify:428/bounded")
	graph.AddTransition("bounded", workflow.END)
	proxy.HandleGraph("maxconcurrencyflow.verify:428/max-concurrency", graph)

	const cap = 3
	const dwell = 100 * time.Millisecond
	var mu sync.Mutex
	var inFlight, peak int
	var rejections atomic.Int32

	proxy.HandleTask("maxconcurrencyflow.verify:428/bounded", func(ctx context.Context, f *workflow.Flow, metadata map[string]any) error {
		mu.Lock()
		inFlight++
		if inFlight > peak {
			peak = inFlight
		}
		over := inFlight > cap
		if over {
			inFlight--
			rejections.Add(1)
		}
		mu.Unlock()
		if over {
			return errors.New("saturated", http.StatusTooManyRequests)
		}
		time.Sleep(dwell)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithWorkers(6)
	eng.RunInTest(t)

	t.Run("backpressure_bounds_max_concurrency", func(t *testing.T) {
		assert := testarossa.For(t)

		var keys []string
		for i := range 24 {
			k, err := eng.Create(ctx, "maxconcurrencyflow.verify:428/max-concurrency",
				map[string]any{"tag": i}, nil, nil)
			assert.NoError(err)
			err = eng.Start(ctx, k)
			assert.NoError(err)
			keys = append(keys, k)
		}

		for _, k := range keys {
			outcome, err := eng.Await(ctx, k)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}

		assert.True(rejections.Load() >= 1)
		mu.Lock()
		p := peak
		mu.Unlock()
		assert.True(p <= 6)
	})
}
