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
)

func TestAckdroppedflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	ackGraph := workflow.NewGraph("AckDropped", "ackdroppedflow.verify:428/ack-dropped")
	ackGraph.AddTask("Park", "ackdroppedflow.verify:428/park")
	ackGraph.AddTransition("Park", workflow.END)
	proxy.HandleGraph("ackdroppedflow.verify:428/ack-dropped", ackGraph)

	echoGraph := workflow.NewGraph("Echo", "ackdroppedflow.verify:428/echo")
	echoGraph.AddTask("Ping", "ackdroppedflow.verify:428/ping")
	echoGraph.AddTransition("Ping", workflow.END)
	proxy.HandleGraph("ackdroppedflow.verify:428/echo", echoGraph)

	var parkHits atomic.Int64
	var pingHits atomic.Int64
	var parkDisabled atomic.Bool
	parkDisabled.Store(true)

	proxy.HandleTask("ackdroppedflow.verify:428/park", func(ctx context.Context, f *workflow.Flow) error {
		if parkDisabled.Load() {
			// Wrap the transport's unreachable signal as a breaker trip with an ack_timeout cause.
			return workflow.ErrUnavailable(errors.New("ack timeout: ackdroppedflow.verify:428/park", http.StatusNotFound), "ack_timeout")
		}
		parkHits.Add(1)
		return nil
	})
	proxy.HandleTask("ackdroppedflow.verify:428/ping", func(ctx context.Context, f *workflow.Flow) error {
		pingHits.Add(1)
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.SetWorkers(4)
	eng.RunInTest(t)

	var parkKeys []string
	for range 20 {
		k, err := eng.Create(ctx, "ackdroppedflow.verify:428/ack-dropped", nil, nil)
		testarossa.For(t).NoError(err)
		eng.Start(ctx, k)
		parkKeys = append(parkKeys, k)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if eng.BreakerTripped("ackdroppedflow.verify:428/park") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Run("park_flows_park_in_pending_and_breaker_trips", func(t *testing.T) {
		assert := testarossa.For(t)
		assert.True(eng.BreakerTripped("ackdroppedflow.verify:428/park"))
		assert.Equal(int64(0), parkHits.Load())
	})

	t.Run("echo_flows_drain_unimpeded_while_park_is_tripped", func(t *testing.T) {
		assert := testarossa.For(t)

		for range 5 {
			k, err := eng.Create(ctx, "ackdroppedflow.verify:428/echo", nil, nil)
			assert.NoError(err)
			eng.Start(ctx, k)
			timeoutCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			outcome, err := eng.Await(timeoutCtx, k)
			cancel()
			assert.NoError(err)
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}
		assert.True(pingHits.Load() >= 5)
		assert.True(eng.BreakerTripped("ackdroppedflow.verify:428/park"))
	})

	t.Run("reactivating_park_drains_blocked_flows", func(t *testing.T) {
		assert := testarossa.For(t)

		parkDisabled.Store(false)

		timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		for _, k := range parkKeys {
			outcome, err := eng.Await(timeoutCtx, k)
			if !assert.NoError(err) {
				return
			}
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}
		assert.True(parkHits.Load() >= 20)
		assert.False(eng.BreakerTripped("ackdroppedflow.verify:428/park"))
	})
}
