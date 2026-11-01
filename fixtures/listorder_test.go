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
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestList_NewestFirstAcrossShards pins List's top-N: at most Limit flows, the newest by creation time across every
// shard. Each shard reads its own newest Limit and the union is cut to Limit, so a page is neither grouped by shard
// nor a per-shard share of Limit: with Limit 10 over two shards, a shard holding all ten newest flows supplies all
// ten.
func TestList_NewestFirstAcrossShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "listorder/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("listorder/g", g)
	mux.HandleTask("listorder/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.SetShard(engine.ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(engine.ShardSpec{Index: 2}))
	assert.NoError(e.Startup(ctx))

	// Enough flows that placement (a weighted-random pick) lands some on each shard with overwhelming odds.
	const total = 30
	for range total {
		_, err := e.Create(ctx, "listorder/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
	}

	all, err := e.List(ctx, workflow.Query{Limit: 100})
	if !assert.NoError(err) || !assert.Len(all, total) {
		return
	}
	shards := map[int]bool{}
	for i, f := range all {
		shard, _, _, _, err := keys.ParseFlowKey(f.FlowKey)
		assert.NoError(err)
		shards[shard] = true
		if i > 0 {
			assert.False(f.CreatedAt.After(all[i-1].CreatedAt), "row %d is newer than the one before it", i)
		}
	}
	assert.Len(shards, 2, "both shards must contribute (placement is random; 30 flows over 2 shards)")

	// A smaller Limit is a hard cap, and it keeps the newest: exactly the first ten of the whole list.
	newest, err := e.List(ctx, workflow.Query{Limit: 10})
	if !assert.NoError(err) || !assert.Len(newest, 10) {
		return
	}
	for i, f := range newest {
		assert.Equal(all[i].FlowKey, f.FlowKey, "row %d", i)
	}
}
