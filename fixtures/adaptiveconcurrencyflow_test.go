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
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	"github.com/microbus-io/throttle"
)

func TestAdaptiveconcurrencyflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("AdaptiveConcurrency", "adaptiveconcurrencyflow.verify:428/adaptive-concurrency")
	graph.AddTask("Adaptive", "adaptiveconcurrencyflow.verify:428/adaptive")
	graph.AddTransition("Adaptive", workflow.END)
	proxy.HandleGraph("adaptiveconcurrencyflow.verify:428/adaptive-concurrency", graph)

	rate := throttle.New(time.Second, 7)
	var rejections atomic.Int32
	var completions atomic.Int32

	proxy.HandleTask("adaptiveconcurrencyflow.verify:428/adaptive", func(ctx context.Context, f *workflow.Flow) error {
		if admit, _ := rate.Allow(); !admit {
			rejections.Add(1)
			// Wrap the transport's 429 as backpressure; the engine bounces the step and cuts the rate.
			return workflow.ErrBackpressure(errors.New("rate limited", http.StatusTooManyRequests), "")
		}
		time.Sleep(10 * time.Millisecond)
		completions.Add(1)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.RunInTest(t)

	t.Run("rate_convergence", func(t *testing.T) {
		assert := testarossa.For(t)

		const flows = 49
		var keys []string
		for range flows {
			k, err := eng.Create(ctx, "adaptiveconcurrencyflow.verify:428/adaptive-concurrency", nil, nil)
			assert.NoError(err)
			eng.Start(ctx, k)
			keys = append(keys, k)
		}

		for _, k := range keys {
			outcome, err := eng.Await(ctx, k)
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}

		assert.Equal(int32(flows), completions.Load())
		assert.True(rejections.Load() >= 1)
	})
}
