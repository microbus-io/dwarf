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
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestListOpen_NewestFirstAcrossShards pins ListOpen's top-N: at most Limit flows, the most recently updated
// across every shard, ties by shard then newest flow. Each shard reads its own newest Limit and the union is cut to
// Limit, so a page is neither grouped by shard nor a per-shard share of Limit: a shard holding all of the newest flows
// supplies all of them. The flows rest interrupted, so they stay open and their order stays put.
func TestListOpen_NewestFirstAcrossShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Solo")
	g.SetURL("A", "listorder/a")
	g.AddTransition("A", workflow.End)
	mux.HandleGraph("listorder/g", g)
	mux.HandleTask("listorder/a", func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(nil, nil)
		return err
	})

	e := engine.NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.SetShard(engine.ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(engine.ShardSpec{Index: 2}))
	assert.NoError(e.Startup(ctx))

	// Enough flows that placement (a weighted-random pick) lands some on each shard with overwhelming odds.
	const total = 30
	for range total {
		fk, err := e.Create(ctx, "listorder/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusInterrupted, time.Minute)
	}

	all, err := e.ListOpen(ctx, enginetest.Recent(workflow.Query{Limit: 100}))
	if !assert.NoError(err) || !assert.Len(all, total) {
		return
	}
	shards := map[int]bool{}
	for i, f := range all {
		shard, _, _, _, err := keys.ParseFlowKey(f.FlowKey)
		assert.NoError(err)
		shards[shard] = true
		if i > 0 {
			prev := all[i-1]
			assert.False(f.UpdatedAt.After(prev.UpdatedAt), "row %d is newer than the one before it", i)
			if f.UpdatedAt.Equal(prev.UpdatedAt) {
				prevShard, _, prevID, _, _ := keys.ParseFlowKey(prev.FlowKey)
				_, _, id, _, _ := keys.ParseFlowKey(f.FlowKey)
				assert.True(prevShard < shard || (prevShard == shard && prevID > id), "row %d breaks the tie by shard, then newest flow", i)
			}
		}
	}
	assert.Len(shards, 2, "both shards must contribute (placement is random; 30 flows over 2 shards)")

	// A smaller Limit is a hard cap, and it keeps the newest: every limit returns exactly that prefix of the whole
	// list. Checking every prefix, not one, is what catches a Limit divided across shards: that passes a single
	// limit whenever the newest flows happen to split evenly between the shards, but not every prefix.
	for limit := 1; limit <= total; limit++ {
		newest, err := e.ListOpen(ctx, enginetest.Recent(workflow.Query{Limit: limit}))
		if !assert.NoError(err) || !assert.Len(newest, limit) {
			return
		}
		for i, f := range newest {
			assert.Equal(all[i].FlowKey, f.FlowKey, "limit %d, row %d", limit, i)
		}
	}

	// Query.Shard with a Limit is that shard's newest Limit, in the same order.
	for shard := range shards {
		var onShard []string
		for _, f := range all {
			if s, _, _, _, _ := keys.ParseFlowKey(f.FlowKey); s == shard {
				onShard = append(onShard, f.FlowKey)
			}
		}
		limit := min(5, len(onShard))
		got, err := e.ListOpen(ctx, enginetest.Recent(workflow.Query{Shard: shard, Limit: limit}))
		if !assert.NoError(err) || !assert.Len(got, limit) {
			return
		}
		for i, f := range got {
			assert.Equal(onShard[i], f.FlowKey, "shard %d, row %d", shard, i)
		}
	}
}
