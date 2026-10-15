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
ShardInfo returns a per-shard health summary (one entry per shard, 1-indexed, each
reporting whether the shard answered). This covers it across a multi-shard engine -
every shard is reachable - and, through List with Query.Shard, that the flows created
are all accounted for and spread over more than one shard.
*/
package fixtures

import (
	"context"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestShardinfoflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()

	graph := workflow.NewGraph("Flow")
	graph.SetURL("Only", "shardinfoflow.verify:428/only")
	graph.AddTransition("Only", workflow.End)
	mux.HandleGraph("shardinfoflow.verify:428/flow", graph)

	mux.HandleTask("shardinfoflow.verify:428/only", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("done", "yes")
		return nil
	})

	const numShards = 3
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	for i := 1; i <= numShards; i++ {
		eng.SetShard(engine.ShardSpec{Index: i})
	}
	assert.NoError(eng.Startup(t.Context()))

	const total = 30
	for range total {
		_, outcome, err := eng.Run(ctx, "shardinfoflow.verify:428/flow", nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
	}

	t.Run("reports_every_shard", func(t *testing.T) {
		assert := testarossa.For(t)

		shards, err := eng.ShardInfo(ctx)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(numShards, len(shards))
		for i, s := range shards {
			// Shards are 1-indexed and returned in order.
			assert.Equal(i+1, s.Index)
			assert.Equal("", s.Error, "shard %d unreachable", s.Index)
		}
	})

	t.Run("flows_spread_across_shards", func(t *testing.T) {
		assert := testarossa.For(t)

		totalFlows := 0
		nonEmptyShards := 0
		for i := 1; i <= numShards; i++ {
			flows, _, err := eng.List(ctx, workflow.Query{Shard: i, Limit: total})
			if !assert.NoError(err) {
				return
			}
			totalFlows += len(flows)
			if len(flows) > 0 {
				nonEmptyShards++
			}
		}
		// Every created flow is accounted for across the shards.
		assert.Equal(total, totalFlows)
		// Random shard assignment should spread 30 flows over more than one shard.
		assert.True(nonEmptyShards > 1, "flows landed on a single shard (%d)", nonEmptyShards)
	})
}
