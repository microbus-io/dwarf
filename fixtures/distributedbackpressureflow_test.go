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
Multi-replica testing: two Engine instances sharing the same in-memory SQLite databases. Each replica has
its own TestProxy with the other engine added via AddPeer, so each engine's SignalPeers relays to the
other's DeliverSignal, standing in for the bus — a doorbell, valve cut, or breaker trip on one replica
reaches the other. SUM(running) aggregation across every shard produces cluster-wide saturation; a 429 on
one replica propagates via valve gossip so both converge.
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

func TestDistributedbackpressureflow(t *testing.T) {
	ctx := context.Background()

	proxy1 := engine.NewTestProxy()
	proxy2 := engine.NewTestProxy()

	graph := workflow.NewGraph("DistributedBackpressure")
	graph.SetEndpoint("Bounded", "distributedbackpressureflow.verify:428/bounded")
	graph.AddTransition("Bounded", workflow.END)
	proxy1.HandleGraph("distributedbackpressureflow.verify:428/distributed-backpressure", graph)
	proxy2.HandleGraph("distributedbackpressureflow.verify:428/distributed-backpressure", graph)

	const cap = 2
	var mu sync.Mutex
	var inFlight, peak int
	var rejections, completions atomic.Int32

	boundedTask := func(ctx context.Context, f *workflow.Flow) error {
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
			return workflow.ErrRateLimited(errors.New("saturated", http.StatusTooManyRequests), "")
		}
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		completions.Add(1)
		return nil
	}
	// Both replicas share the same handler closure (and its cluster-wide counters).
	proxy1.HandleTask("distributedbackpressureflow.verify:428/bounded", boundedTask)
	proxy2.HandleTask("distributedbackpressureflow.verify:428/bounded", boundedTask)

	// Two replicas sharing the same shards. cache=shared keeps the named in-memory DB alive and visible
	// to both engines' connection pools.
	dsn := "file:dbpf%d?mode=memory&cache=shared"
	eng1 := engine.NewEngine()
	eng1.SetHost(proxy1)
	eng1.SetDSN(dsn)
	eng1.SetNumShards(2)
	eng1.SetWorkers(2)
	eng2 := engine.NewEngine()
	eng2.SetHost(proxy2)
	eng2.SetDSN(dsn)
	eng2.SetNumShards(2)
	eng2.SetWorkers(2)
	proxy1.AddPeer(eng2)
	proxy2.AddPeer(eng1)

	err := eng1.Startup(ctx)
	testarossa.For(t).NoError(err)
	t.Cleanup(func() { eng1.Shutdown(ctx) })

	err = eng2.Startup(ctx)
	testarossa.For(t).NoError(err)
	t.Cleanup(func() { eng2.Shutdown(ctx) })

	t.Run("multi_replica_multi_shard_backpressure", func(t *testing.T) {
		assert := testarossa.For(t)

		const totalFlows = 16
		var keys []string
		for range totalFlows {
			k, err := eng1.Create(ctx, "distributedbackpressureflow.verify:428/distributed-backpressure", nil, nil)
			assert.NoError(err)
			eng1.Start(ctx, k)
			keys = append(keys, k)
		}

		// Await on eng1 even for flows eng2 ran: proxy2's SignalPeers relays the status-change signal to
		// eng1's DeliverSignal so eng1's waiters wake when a peer completes the flow.
		for _, k := range keys {
			timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			outcome, err := eng1.Await(timeoutCtx, k)
			cancel()
			if !assert.NoError(err) {
				return
			}
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}

		assert.Equal(int32(totalFlows), completions.Load())
		assert.True(rejections.Load() >= 1)
		mu.Lock()
		p := peak
		mu.Unlock()
		assert.True(p <= 4)
	})
}
