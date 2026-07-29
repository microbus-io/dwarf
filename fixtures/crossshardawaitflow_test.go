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

// TestCrossShardReplicaAwait covers both axes of Await at once: the flows are spread over several SHARDS,
// and every one of them is completed by a REPLICA other than the one waiting on it. Each axis alone is
// already covered - crossreplicaawait_test.go is single-shard, and the sharded fixtures do not await
// across replicas - and it is their combination that exercises how a waiting replica discovers stops it
// did not make: the keys are grouped by shard and each shard is asked separately, so a fixture on one
// shard cannot tell a correct grouping from a hardcoded one.
//
// Nothing crosses between the replicas to announce a stop - no op exists that could - so the awaiting
// replica can only learn of one by reading the shared flow rows. That is what makes this a test of the
// status detector rather than of any delivery path.
//
// TWO shards, not more. Two is what proves the per-shard grouping - two groups, two queries, one merged
// answer - and a third proves nothing further. It is also what this costs: against a real server the
// runtime is dominated by DROPPING the test databases at cleanup (~5s each on Postgres, and the engine
// work in between measures ~0.5s), so every extra shard is another database and another few seconds.
func TestCrossShardReplicaAwait(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	const shards = 2

	graph := workflow.NewGraph("Flow")
	graph.SetEndpoint("Work", "crossshardawait.verify:428/work")
	graph.AddTransition("Work", workflow.END)

	// eng1: pure awaiter, zero workers. Its handler must never run.
	proxy1 := engine.NewTestProxy()
	proxy1.HandleGraph("crossshardawait.verify:428/flow", graph)
	proxy1.HandleTask("crossshardawait.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		t.Error("eng1 has zero workers and must never execute the task")
		return nil
	})

	// eng2: the executor, stamping which flow it ran so the outcome cannot come from anywhere else.
	proxy2 := engine.NewTestProxy()
	proxy2.HandleGraph("crossshardawait.verify:428/flow", graph)
	proxy2.HandleTask("crossshardawait.verify:428/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("ranOn", "eng2")
		return nil
	})

	// Both replicas key their test databases off the same t.Name(), so they share one isolated database
	// per shard - which is what makes them peers rather than two independent engines.
	eng1 := engine.NewEngineUnderTest(t)
	assert.NoError(eng1.SetHost(proxy1))
	assert.NoError(eng1.SetWorkers(0))
	eng2 := engine.NewEngineUnderTest(t)
	assert.NoError(eng2.SetHost(proxy2))
	assert.NoError(eng2.SetWorkers(4))
	for shard := 1; shard <= shards; shard++ {
		assert.NoError(eng1.SetShard(engine.ShardSpec{Index: shard}))
		assert.NoError(eng2.SetShard(engine.ShardSpec{Index: shard}))
	}
	assert.NoError(eng1.Startup(ctx))
	assert.NoError(eng2.Startup(ctx))
	// Shut BOTH engines down before anything else unwinds. Startup registers a cleanup per engine and
	// t.Cleanup is LIFO, so without this a peer's databases are dropped while the other engine still holds
	// connections to them, and the DROP waits on those live sessions.
	t.Cleanup(func() {
		eng1.Shutdown(context.Background())
		eng2.Shutdown(context.Background())
	})

	// Placement is a weighted random pick, so create until every shard holds at least one flow rather than
	// assuming a spread. The bound fails loudly instead of looping if placement ever stops covering them.
	flowKeys := map[int][]string{}
	created := 0
	for len(flowKeys) < shards && created < 200 {
		flowKey, err := eng1.Create(ctx, "crossshardawait.verify:428/flow", nil, nil)
		if !assert.NoError(err) {
			return
		}
		created++
		shard, err := shardOfFlowKey(flowKey)
		if !assert.NoError(err) {
			return
		}
		flowKeys[shard] = append(flowKeys[shard], flowKey)
	}
	if !assert.Equal(shards, len(flowKeys), "every shard must hold at least one flow, or this is not a cross-shard test") {
		return
	}

	// Await them all at once from the replica that ran none of them. Concurrent by design: one sweep has
	// to resolve keys belonging to several shards, which is the case a serial await would never produce.
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var group sync.WaitGroup
	var lock sync.Mutex
	stopped := 0
	for shard, keysOnShard := range flowKeys {
		for _, flowKey := range keysOnShard {
			group.Go(func() {
				outcome, err := eng1.Await(awaitCtx, flowKey)
				lock.Lock()
				defer lock.Unlock()
				if !assert.NoError(err, "awaiting %s on shard %d", flowKey, shard) {
					return
				}
				if !assert.Equal(workflow.StatusCompleted, outcome.Status, "flow %s", flowKey) {
					return
				}
				assert.Equal("eng2", outcome.State.Value("ranOn"), "the peer replica is the only one that can run it")
				stopped++
			})
		}
	}
	group.Wait()
	assert.Equal(created, stopped, "every flow must be awaited to completion")
}

// shardOfFlowKey reads the shard out of a "{shard}-{id}-{token}" flow key. The format is public (doc.go),
// so a fixture may parse it; nothing here reaches into the engine's own key package.
func shardOfFlowKey(flowKey string) (int, error) {
	prefix, _, ok := strings.Cut(flowKey, "-")
	if !ok {
		return 0, fmt.Errorf("malformed flow key %q", flowKey)
	}
	return strconv.Atoi(prefix)
}
