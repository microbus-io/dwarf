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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestShardedflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()

	graph := workflow.NewGraph("Sharded")
	graph.SetURL("Record", "shardedflow.verify:428/record")
	graph.AddTransition("Record", workflow.End)
	mux.HandleGraph("shardedflow.verify:428/sharded", graph)

	// Every flow of the ordering subtest is GATED: its task reports that it has the single worker, then holds
	// it until the test lets go. Both halves are rendezvous rather than durations, because both are waits on
	// engine progress - "the worker is mine" is a dispatch, and the release must wait until every shard's
	// cache partition has been reconciled against a plan that already knows about that dispatch (see the
	// subtest).
	started := make(chan string, 16)
	proceed := make(chan struct{})
	done := make(chan struct{})

	mux.HandleTask("shardedflow.verify:428/record", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetBool("gated") {
			started <- f.GetString("tag")
			select {
			case <-proceed:
			case <-done:
			}
		}
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	// Closed here, after the Shutdown defer, so it unwinds FIRST: Shutdown drains the workers, and one still
	// holding a gated task would never return.
	defer close(done)
	eng.SetHost(mux)
	eng.SetWorkers(1)
	eng.SetMaxPriority(10) // the ordering below spans more bands than the default five
	for i := 1; i <= 8; i++ {
		eng.SetShard(engine.ShardSpec{Index: i})
	}
	assert.NoError(eng.Startup(t.Context()))

	t.Run("strict_priority_across_shards", func(t *testing.T) {
		assert := testarossa.For(t)

		next := func() string {
			select {
			case tag := <-started:
				return tag
			case <-time.After(time.Minute * enginetest.TimeoutScale()):
				t.Fatal("no gated flow reached its task, so the worker was never occupied")
				return ""
			}
		}

		// The holder takes the one worker and keeps it until released, so every p-flow below is created while
		// nothing can be dispatched.
		holderKey, err := eng.Create(ctx, "shardedflow.verify:428/sharded",
			map[string]any{"gated": true, "tag": "holder"},
			&workflow.FlowOptions{Priority: 1})
		if !assert.NoError(err) {
			return
		}
		got := []string{next()}

		keys := []string{holderKey}
		placement := map[string]string{}
		for i := range 8 {
			p := i + 2
			tag := fmt.Sprintf("p%d", p)
			k, err := eng.Create(ctx, "shardedflow.verify:428/sharded",
				map[string]any{"gated": true, "tag": tag},
				&workflow.FlowOptions{Priority: p})
			if !assert.NoError(err) {
				return
			}
			keys = append(keys, k)
			placement[tag] = strings.SplitN(k, "-", 2)[0]
		}

		// Strict priority across shards is a promise about a SETTLED fleet, not about every instant of a
		// drain. Each shard's Supplier keeps only its own slice of the global plan, re-planned on its own
		// cadence from the planner's last tally of every shard, and Pop takes the best band any partition
		// holds without consulting a plan. So in the moment after a dispatch, the shard holding the next band
		// may still be holding the empty slice of a plan built from tallies that predate the dispatch - it
		// saw a better band still due and planned its own step out - while another shard has already
		// re-planned and holds the band after it. A worker freed in that moment runs the worse band. Holding
		// each flow for a fixed time instead (50ms, under one cycle) failed ~1 run in 75 under six parallel
		// PostgreSQL processes with no engine error logged: an adjacent swap across two shards.
		//
		// So every release, the holder's included, waits for each shard to scan and then push twice since
		// the dispatch it frees - not once: a cycle already in flight when the claim committed may have
		// scanned before it. Every partition then holds a slice of a plan that already knows the released
		// flow is running, and the next pop is decided by priority alone.
		for i := range keys {
			enginetest.AwaitShardCycles(t, eng, 2)
			proceed <- struct{}{}
			if i < len(keys)-1 {
				got = append(got, next())
			}
		}
		for _, k := range keys {
			enginetest.AwaitAndAssertComplete(t, eng, k)
		}

		expected := []string{"holder", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"}
		// Report SHARD PLACEMENT alongside the order. Placement is capacity-weighted random, so the eight
		// flows do not land one per shard and which pair shares one differs every run - yet it is the first
		// thing an out-of-order dispatch has to be read against, and it is unrecoverable once the test
		// database is dropped. Two flows competing ACROSS shards is a tally/plan question; two on the SAME
		// shard is a partition-ordering question (`Offer` sets the band from the first arrival and declines
		// anything worse).
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
