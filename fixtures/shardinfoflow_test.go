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
ShardInfo returns a per-shard operational summary (one entry per shard, 1-indexed,
each reporting reachability plus flow/step counts). This covers the admin endpoint
across a multi-shard engine: every shard is reachable, and the flow counts sum to
the number created and distribute across more than one shard.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestShardinfoflow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("shardinfoflow.verify:428/flow")
	graph.AddTask("only", "shardinfoflow.verify:428/only")
	graph.AddTransition("only", workflow.END)
	proxy.HandleGraph("shardinfoflow.verify:428/flow", graph)

	proxy.HandleTask("shardinfoflow.verify:428/only", func(ctx context.Context, f *workflow.Flow, baggage any) error {
		f.SetString("done", "yes")
		return nil
	})

	const numShards = 3
	eng := engine.NewEngine().
		WithGraphLoader(proxy.LoadGraph).
		WithTaskExecutor(proxy.ExecuteTask).
		WithNumShards(numShards)
	eng.RunInTest(t)

	const total = 30
	for range total {
		outcome, err := eng.Run(ctx, "shardinfoflow.verify:428/flow", nil, nil, nil)
		testarossa.NoError(t, err)
		testarossa.Equal(t, workflow.StatusCompleted, outcome.Status)
	}

	t.Run("reports_every_shard_and_counts_sum", func(t *testing.T) {
		assert := testarossa.For(t)

		shards, err := eng.ShardInfo(ctx)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(numShards, len(shards))

		totalFlows := 0
		nonEmptyShards := 0
		for i, s := range shards {
			// Shards are 1-indexed and returned in order.
			assert.Equal(i+1, s.Shard)
			assert.Equal("", s.Error, "shard %d unreachable", s.Shard)
			totalFlows += s.Flows
			if s.Flows > 0 {
				nonEmptyShards++
				// A shard holding flows must also hold their steps.
				assert.True(s.Steps > 0, "shard %d has flows but no steps", s.Shard)
			}
		}
		// Every created flow is accounted for across the shards.
		assert.Equal(total, totalFlows)
		// Random shard assignment should spread 30 flows over more than one shard.
		assert.True(nonEmptyShards > 1, "flows landed on a single shard (%d)", nonEmptyShards)
	})
}
