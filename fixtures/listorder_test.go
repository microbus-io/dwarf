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
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestList_NewestFirstIsPerShardNotGlobal pins what List's ordering actually IS, because the docs long
// promised something it is not ("newest first", flat) and the tempting "fix" is a real regression.
//
// A page is newest-first WITHIN a shard and shard-GROUPED across them: on two shards, List returns shard 1's
// newest flows, then shard 2's newest - so shard 2's newest flow follows shard 1's oldest returned one, and
// the concatenation is not one descending time order. There is no cross-shard order to give: flow ids are
// per-shard sequences (a shard with fewer flows has lower ids, so they do not compare), and merging by
// created_at - the obvious repair - would compare two different database servers' CLOCKS. The doc was the
// bug; this test is what stops someone "fixing" the code to match the old promise.
func TestList_NewestFirstIsPerShardNotGlobal(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	g := workflow.NewGraph("Solo")
	g.SetEndpoint("A", "listorder/a")
	g.AddTransition("A", workflow.END)
	proxy.HandleGraph("listorder/g", g)
	proxy.HandleTask("listorder/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := engine.NewEngineUnderTest(t)
	e.SetHost(proxy)
	assert.NoError(e.SetShard(engine.ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(engine.ShardSpec{Index: 2}))
	assert.NoError(e.Startup(ctx))

	// Enough flows that placement (a weighted-random pick) lands some on each shard with overwhelming odds.
	for range 30 {
		_, err := e.Create(ctx, "listorder/g", nil, nil)
		if !assert.NoError(err) {
			return
		}
	}

	flows, _, err := e.List(ctx, workflow.Query{Limit: 100})
	if !assert.NoError(err) {
		return
	}
	assert.Equal(30, len(flows))

	// The page is grouped by shard: each shard's rows are contiguous, so a shard index is never revisited
	// after the page has moved on from it.
	var shardRuns []int
	seen := map[int]bool{}
	prev := 0
	perShard := map[int][]workflow.FlowSummary{}
	for _, f := range flows {
		shard, _, _, err := keys.ParseFlowKey(f.FlowKey)
		if !assert.NoError(err) {
			return
		}
		perShard[shard] = append(perShard[shard], f)
		if shard != prev {
			assert.False(seen[shard], "shard %d's rows are not contiguous: List must be shard-grouped", shard)
			seen[shard] = true
			shardRuns = append(shardRuns, shard)
			prev = shard
		}
	}
	assert.Equal(2, len(shardRuns), "both shards must contribute (placement is random; 30 flows over 2 shards)")

	// Within a shard, newest first holds exactly.
	for shard, rows := range perShard {
		for i := 1; i < len(rows); i++ {
			assert.False(rows[i].CreatedAt.After(rows[i-1].CreatedAt),
				"shard %d row %d is newer than the one before it: within a shard, List is newest-first", shard, i)
		}
	}

	// And the flat concatenation is NOT globally newest-first: at the shard boundary the page jumps back to
	// the next shard's newest flow. Asserting the violation is present is the honest way to pin "no global
	// order is promised" - if a future change ever did sort the page globally, this fails loudly and the docs
	// get updated on purpose rather than drifting back into a lie. (Stated as an existential over the page,
	// so a millisecond tie between two adjacent rows cannot flake it.)
	globallyDescending := true
	for i := 1; i < len(flows); i++ {
		if flows[i].CreatedAt.After(flows[i-1].CreatedAt) {
			globallyDescending = false
			break
		}
	}
	assert.False(globallyDescending,
		"the page came back globally newest-first; List's contract is per-shard order + shard grouping, so either the ordering changed (update the docs deliberately) or the two shards did not both contribute")
}
