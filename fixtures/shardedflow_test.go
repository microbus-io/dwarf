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

// awaitShardCycles blocks until every shard's piston has completed `extra` further pushing cycles, counted
// from the moment of the call. A pushing cycle is the point at which that shard's cache partition has been
// reconciled against the plan (see engine.CheckpointRefillCycleDone), so this is how a test waits for the
// fleet's cached hints to agree with what the planner actually chose.
//
// There is no wall-clock stand-in for it, and that is the whole reason the seam exists: each piston turns on
// its own cadence, so one starved or slow shard holds an unreconciled partition for as long as it likes while
// its peers turn normally - the asymmetric case, which no uniform delay reproduces or waits out.
//
// Arm the waiter FIRST, then read Visits, per shard: a cycle that lands before the read is caught by the
// count, a later one by the channel, and one landing between the two lines by the channel. Waiter is
// one-shot, hence the inner loop. The minute is a "did it hang" ceiling, not a timing contract.
func awaitShardCycles(t *testing.T, eng *engine.Engine, shards, extra int) {
	t.Helper()
	seams := eng.Seams()
	for shard := 1; shard <= shards; shard++ {
		key := strconv.Itoa(shard)
		want := seams.Visits(engine.CheckpointRefillCycleDone, key) + extra
		for {
			cycled := seams.Waiter(engine.CheckpointRefillCycleDone, key)
			got := seams.Visits(engine.CheckpointRefillCycleDone, key)
			if got >= want {
				break
			}
			select {
			case <-cycled:
			case <-time.After(time.Minute):
				t.Fatalf("shard %d completed %d of the %d cycles awaited", shard, got, want)
			}
		}
	}
}

func TestShardedflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("Sharded")
	graph.SetEndpoint("Record", "shardedflow.verify:428/record")
	graph.AddTransition("Record", workflow.END)
	proxy.HandleGraph("shardedflow.verify:428/sharded", graph)

	var mu sync.Mutex
	var order []string

	// A `hold` flow occupies the single worker until the test releases it, and reports the moment it has it.
	// Both halves are rendezvous rather than sleeps because both are waits on engine progress: "the worker is
	// mine" is a dispatch, and the release must not happen until the fleet's cache partitions have been
	// reconciled - a condition that is not a duration at all (see awaitShardCycles).
	var holdOnce sync.Once
	holderRunning := make(chan struct{})
	releaseHolder := make(chan struct{})

	proxy.HandleTask("shardedflow.verify:428/record", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetBool("hold") {
			holdOnce.Do(func() { close(holderRunning) })
			<-releaseHolder
		}
		delayMs := f.GetInt("delayMs")
		if delayMs > 0 {
			time.Sleep(time.Duration(delayMs) * time.Millisecond)
		}
		mu.Lock()
		order = append(order, f.GetString("tag"))
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	eng.SetWorkers(1)
	for i := 1; i <= 8; i++ {
		eng.SetShard(engine.ShardSpec{Index: i})
	}
	assert.NoError(eng.Startup(t.Context()))

	t.Run("strict_priority_across_shards", func(t *testing.T) {
		assert := testarossa.For(t)
		mu.Lock()
		order = nil
		mu.Unlock()

		// The holder takes the one worker and keeps it until released, so every p-flow below is created while
		// nothing can be dispatched.
		holderKey, _ := eng.Create(ctx, "shardedflow.verify:428/sharded",
			map[string]any{"hold": true, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1})
		select {
		case <-holderRunning:
		case <-time.After(time.Minute):
			t.Fatal("the holder flow never reached its task, so the worker was never occupied")
		}

		var keys []string
		placement := map[string]string{}
		for i := range 8 {
			p := i + 2
			tag := fmt.Sprintf("p%d", p)
			k, _ := eng.Create(ctx, "shardedflow.verify:428/sharded",
				map[string]any{"delayMs": 50, "tag": tag},
				&workflow.FlowOptions{Priority: p})
			keys = append(keys, k)
			placement[tag] = strings.SplitN(k, "-", 2)[0]
		}

		// Every p-flow was doorbell-admitted into its shard's EMPTY partition on creation, which stamps that
		// partition's band from the arrival itself - and `Cache.Pop` ranks partitions by that frozen band
		// without ever consulting the current global minimum. Until each shard's piston has run a cycle, the
		// cache therefore holds eight hints that no plan chose, and which one Pop takes depends on which
		// pistons have got round to reconciling. Releasing the worker into that state is what makes the
		// ordering below a race: it is won by whichever shards happen to have cycled, not by priority.
		//
		// Two cycles per shard, not one: a cycle already in flight when these Creates committed may have
		// scanned before they existed, so its push proves nothing about them. The second is the one whose
		// scan is guaranteed to have seen them.
		awaitShardCycles(t, eng, 8, 2)
		close(releaseHolder)

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
		// Report SHARD PLACEMENT alongside the order. Placement is capacity-weighted random, so the eight
		// flows do not land one per shard and which pair shares one differs every run - yet it is the first
		// thing an out-of-order dispatch has to be read against, and it is unrecoverable once the test
		// database is dropped. Two flows competing ACROSS shards is a tally/plan question (the planner holds
		// each shard's LAST report, and a shard publishes a band only by scanning); two on the SAME shard is
		// a partition-ordering question (`Offer` sets the band from the first arrival and declines anything
		// worse). An observed inversion was a single adjacent swap (p5 before p4) with no refill-scan error
		// in the run's log, so neither the publish gap nor a planner Clear explains it.
		assert.Equal(expected, got, "dispatch order; shard placement: %v", placement)
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
