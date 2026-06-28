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
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestShardedflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Sharded")
	graph.SetEndpoint("Record", "shardedflow.verify:428/record")
	graph.AddTransition("Record", workflow.END)
	proxy.HandleGraph("shardedflow.verify:428/sharded", graph)

	var mu sync.Mutex
	var order []string

	proxy.HandleTask("shardedflow.verify:428/record", func(ctx context.Context, f *workflow.Flow) error {
		delayMs := f.GetInt("delayMs")
		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
		mu.Lock()
		order = append(order, f.GetString("tag"))
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngine()
	eng.SetHost(proxy)
	eng.SetWorkers(1)
	eng.SetNumShards(8)
	eng.RunInTest(t)

	t.Run("strict_priority_across_shards", func(t *testing.T) {
		assert := testarossa.For(t)
		mu.Lock()
		order = nil
		mu.Unlock()

		holderKey, _ := eng.Create(ctx, "shardedflow.verify:428/sharded",
			map[string]any{"delayMs": 1500, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1})
		time.Sleep(100 * time.Millisecond)

		var keys []string
		for i := range 8 {
			p := i + 2
			tag := fmt.Sprintf("p%d", p)
			k, _ := eng.Create(ctx, "shardedflow.verify:428/sharded",
				map[string]any{"delayMs": 50, "tag": tag},
				&workflow.FlowOptions{Priority: p})
			keys = append(keys, k)
		}

		eng.Await(ctx, holderKey)
		for _, k := range keys {
			eng.Await(ctx, k)
		}

		mu.Lock()
		got := make([]string, len(order))
		copy(got, order)
		mu.Unlock()

		assert.Equal("holder", got[0])
		expected := []string{"holder", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"}
		assert.Equal(expected, got)
	})

	t.Run("random_shard_distribution", func(t *testing.T) {
		assert := testarossa.For(t)

		shards := map[int]int{}
		for range 400 {
			k, err := eng.Create(ctx, "shardedflow.verify:428/sharded",
				map[string]any{"delayMs": 0, "tag": "dist"}, nil)
			assert.NoError(err)
			parts := strings.SplitN(k, "-", 2)
			shard, _ := strconv.Atoi(parts[0])
			shards[shard]++
		}
		assert.Equal(8, len(shards))
		mean := 400 / 8
		for _, count := range shards {
			assert.True(count > mean/3)
			assert.True(count < mean*3)
		}
	})
}
