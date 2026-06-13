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

/*
The WHERE status='running' predicate in handleBackpressure is the only protection
against a 429 reviving a cancelled step.
*/
package fixtures

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestCancelbackpressureflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("cancelbackpressureflow.verify:428/cancel-backpressure")
	graph.AddTask("bounceAndCancel", "cancelbackpressureflow.verify:428/bounce-and-cancel")
	graph.AddTransition("bounceAndCancel", workflow.END)
	proxy.HandleGraph("cancelbackpressureflow.verify:428/cancel-backpressure", graph)

	var readyOnce sync.Once
	ready := make(chan struct{})
	release := make(chan struct{})

	proxy.HandleTask("cancelbackpressureflow.verify:428/bounce-and-cancel", func(ctx context.Context, f *workflow.Flow) error {
		readyOnce.Do(func() { close(ready) })
		select {
		case <-release:
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}
		return errors.New("saturated", http.StatusTooManyRequests)
	})

	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask)
	eng.RunInTest(t)

	t.Run("status_guard_race", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "cancelbackpressureflow.verify:428/cancel-backpressure",
			map[string]any{"tag": "race"}, nil)
		if !assert.NoError(err) {
			return
		}
		err = eng.Start(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}

		<-ready

		err = eng.Cancel(ctx, flowKey, "")
		if !assert.NoError(err) {
			return
		}

		close(release)

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)

		time.Sleep(200 * time.Millisecond)

		steps, err := eng.History(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		for _, s := range steps {
			if s.TaskName == "bounceAndCancel" {
				assert.Equal(workflow.StatusCancelled, s.Status)
			}
		}
	})
}
