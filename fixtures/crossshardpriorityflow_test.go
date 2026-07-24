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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCrossShardPriorityflow pins that strict priority still holds ACROSS SHARDS once each shard has
// its own independent refiller, and that the refillers' scan floor does not break it.
//
// Why this shape, and why the existing priorityflow fixture does not cover it. With one refiller the
// global minimum band came from a single barriered pass that read every shard at once, so cross-shard
// priority was true by construction. Now each shard scans on its own cadence and publishes to a shared
// census, the global band is merged from those (possibly one pass stale) entries, and a rate floor sits
// between a band change and the refiller noticing it. Every one of those is a way for a high-priority
// arrival on one shard to be ignored while other shards keep draining lower-priority work - a silent
// semantic break that no throughput benchmark can see. (The rig campaigns ran a single band end to end,
// so this path was entirely unmeasured there; ordering is a correctness property, not a timing one,
// which is what makes it a fixture rather than a benchmark arm.)
//
// The workload deliberately holds a DEEP low-priority backlog in the caches when the urgent work
// arrives: that is the case where the caches are full of band-100 candidates and a band-1 arrival must
// preempt them, rather than the trivial case of an idle engine.
// NOT t.Parallel: this test asserts the fleet's urgent-burst REACTION LATENCY (< 3s against an expected
// ~350ms), which CPU oversubscription from co-running parallel tests inflates past the bound. It measures
// timing, not just an outcome, so it must run without competition.
func TestCrossShardPriorityflow(t *testing.T) {
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	graph := workflow.NewGraph("CrossShardPriority")
	graph.SetEndpoint("Record", "crossshardpriority.verify:428/record")
	graph.AddTransition("Record", workflow.END)
	proxy.HandleGraph("crossshardpriority.verify:428/priority", graph)

	var mu sync.Mutex
	var order []string

	proxy.HandleTask("crossshardpriority.verify:428/record", func(ctx context.Context, f *workflow.Flow) error {
		if d := f.GetInt("delayMs"); d > 0 {
			time.Sleep(time.Duration(d) * time.Millisecond)
		}
		mu.Lock()
		order = append(order, f.GetString("tag"))
		mu.Unlock()
		return nil
	})

	eng := engine.NewEngineUnderTest(t)
	assertSetup := testarossa.For(t)
	assertSetup.NoError(eng.SetHost(proxy))
	// Three shards, so placement spreads the flows and the global band must be merged from three
	// independently-scanned census entries rather than read in one pass.
	assertSetup.NoError(eng.SetShard(engine.ShardSpec{Index: 1}))
	assertSetup.NoError(eng.SetShard(engine.ShardSpec{Index: 2}))
	assertSetup.NoError(eng.SetShard(engine.ShardSpec{Index: 3}))
	// One worker: dispatch order is then observable directly as completion order, with no interleaving
	// to reason around.
	assertSetup.NoError(eng.SetWorkers(1))
	if err := eng.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}

	assert := testarossa.For(t)

	// A deep low-priority backlog, created first and spread across all three shards.
	const lowCount = 24
	lowKeys := make([]string, 0, lowCount)
	for range lowCount {
		k, err := eng.Create(ctx, "crossshardpriority.verify:428/priority",
			map[string]any{"delayMs": 20, "tag": "low"},
			&workflow.FlowOptions{Priority: 100})
		assert.NoError(err)
		lowKeys = append(lowKeys, k)
	}

	// Let the refillers cache band-100 candidates and start draining them, so the urgent work below
	// arrives into caches that are already full of lower-priority hints - the preemption case.
	time.Sleep(250 * time.Millisecond)

	// The urgent burst. More than one, deliberately: Offer head-inserts at most ONE pioneer per
	// band-opening per partition, so everything past the pioneers can only be served by a refill scan
	// noticing the new band. That is exactly what the scan floor delays.
	const highCount = 6
	highKeys := make([]string, 0, highCount)
	urgentAt := time.Now()
	for range highCount {
		k, err := eng.Create(ctx, "crossshardpriority.verify:428/priority",
			map[string]any{"delayMs": 20, "tag": "high"},
			&workflow.FlowOptions{Priority: 1})
		assert.NoError(err)
		highKeys = append(highKeys, k)
	}

	deadline, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for _, k := range highKeys {
		outcome, err := eng.Await(deadline, k)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}
	}
	urgentLatency := time.Since(urgentAt)

	// THE SENSITIVE ASSERTION, and the reason it is latency rather than ordering. A refill pass always
	// plans the global MINIMUM band, so however slowly the refillers scan, the urgent band is still
	// selected first whenever a scan does happen - the floor cannot invert the order, it can only make
	// everything wait. Verified: a build with a 5s floor and the urgent bypass removed still produced
	// correct ordering, and simply took 22x as long (50.7s vs 2.25s). An ordering-only fixture would
	// have passed it silently.
	//
	// So what is pinned here is how fast the fleet REACTS to a band change: the urgent burst must clear
	// in roughly a scan plus its own work (~6 x 20ms plus one pass), not in multiples of the scan
	// floor. The bound is an order of magnitude above the expected ~350ms, so it is inert against CI
	// jitter while still failing loudly if the urgent bypass is lost or the floor is raised into
	// dispatch latency.
	assert.True(urgentLatency < 3*time.Second,
		"urgent band-1 burst took %v to clear; it must be served on a band change, not held for scan floors", urgentLatency)

	for _, k := range lowKeys {
		outcome, err := eng.Await(deadline, k)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status)
		}
	}

	mu.Lock()
	got := append([]string{}, order...)
	mu.Unlock()

	// Where the urgent work landed in the completion order. The assertion is deliberately not "all six
	// high before every low": a worker already executing a low-priority step finishes it (priority is
	// not preemptive), the refiller serves the new band on its next pass rather than instantly, and
	// each shard may pioneer one candidate - so a bounded number of low-priority steps legitimately
	// interleave. What must NOT happen is the urgent band waiting out the low-priority backlog.
	lastHigh, lowsAfterHigh := -1, 0
	for i, tag := range got {
		if tag == "high" {
			lastHigh = i
		}
	}
	for _, tag := range got[lastHigh+1:] {
		if tag == "low" {
			lowsAfterHigh++
		}
	}
	assert.Equal(highCount, strings.Count(strings.Join(got, ","), "high"))

	// The ordering claim, kept as the semantic contract even though it is the LESS sensitive of the two
	// (see above): once the band-1 work was observable it went ahead of the great majority of the
	// still-pending band-100 backlog, across all three shards. Half the backlog is a wide margin
	// against dispatch jitter - a worker mid-step finishes it, priority is not preemptive, and each
	// partition may pioneer one candidate - while still failing if a shard's census entry stopped
	// contributing to the global band at all.
	assert.True(lowsAfterHigh >= lowCount/2,
		"expected the urgent band to preempt most of the %d-step low backlog, but only %d low steps ran after the last high step (order: %v)",
		lowCount, lowsAfterHigh, got)
}
