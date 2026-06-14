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
Multi-replica testing: two Engine instances sharing the same in-memory SQLite databases. A peerBridge
relays each engine's SignalPeers to the other's DeliverSignal, standing in for the bus so a doorbell,
valve cut, or breaker trip on one replica reaches the other. SUM(running) aggregation across every shard
produces cluster-wide saturation; a 429 on one replica propagates via valve gossip so both converge.
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

// peerBridge wraps a TestProxy (for LoadGraph/ExecuteTask) and relays the engine's cross-replica signals
// to a peer engine's DeliverSignal, standing in for the bus. Its SignalPeers shadows the embedded
// proxy's single-replica no-op, so the bridge is a complete Host. The relay is async to mirror bus
// semantics and avoid synchronous reentrancy into a peer mid-processStep.
type peerBridge struct {
	*engine.TestProxy
	peer *engine.Engine
}

func (b *peerBridge) SignalPeers(ctx context.Context, op string, payload []byte) {
	p := b.peer
	if p != nil {
		go p.DeliverSignal(context.WithoutCancel(ctx), op, payload)
	}
}

func TestDistributedbackpressureflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("distributedbackpressureflow.verify:428/distributed-backpressure")
	graph.AddTask("bounded", "distributedbackpressureflow.verify:428/bounded")
	graph.AddTransition("bounded", workflow.END)
	proxy.HandleGraph("distributedbackpressureflow.verify:428/distributed-backpressure", graph)

	const cap = 2
	var mu sync.Mutex
	var inFlight, peak int
	var rejections, completions atomic.Int32

	proxy.HandleTask("distributedbackpressureflow.verify:428/bounded", func(ctx context.Context, f *workflow.Flow) error {
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
			return workflow.ErrBackpressure(errors.New("saturated", http.StatusTooManyRequests), "")
		}
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		completions.Add(1)
		return nil
	})

	// Two replicas sharing the same shards. cache=shared keeps the named in-memory DB alive and visible
	// to both engines' connection pools.
	dsn := "file:dbpf%d?mode=memory&cache=shared"
	bridge1 := &peerBridge{TestProxy: proxy}
	bridge2 := &peerBridge{TestProxy: proxy}
	eng1 := engine.NewEngine().
		WithHost(bridge1).
		WithDSN(dsn).
		WithNumShards(2).
		WithWorkers(2)
	eng2 := engine.NewEngine().
		WithHost(bridge2).
		WithDSN(dsn).
		WithNumShards(2).
		WithWorkers(2)
	bridge1.peer = eng2
	bridge2.peer = eng1

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

		// Await on eng1 even for flows eng2 ran: the peerBridge relays NotifyStatusChange so eng1's
		// waiters wake when a peer completes the flow.
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
