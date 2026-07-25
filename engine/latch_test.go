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

package engine

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

func TestLatchPadBinds_RoundsUpToABucket(t *testing.T) {
	assert := testarossa.For(t)

	// The arity is part of the statement text, so what matters is that many different chunk sizes collapse
	// onto few distinct lengths - and that the ids themselves all survive the padding.
	sizes := map[int]bool{}
	for n := 1; n <= latchResolveChunk; n++ {
		ids := make([]int, n)
		for i := range ids {
			ids[i] = i + 1
		}
		binds := latchPadBinds(ids)
		sizes[len(binds)] = true
		assert.True(len(binds) >= n, "padding must never drop an id")
		for i := range ids {
			assert.Equal(any(ids[i]), binds[i], "the real ids stay in place, ahead of the padding")
		}
		// Padding repeats an id already in the chunk, so it can never widen what the query matches.
		for _, b := range binds[n:] {
			assert.Equal(any(ids[n-1]), b)
		}
	}
	// Every chunk size from 1 to the cap collapses onto this handful of arities, which is the whole point:
	// the set of query texts a shard ever sees stays small.
	assert.Equal([]int{1, 2, 4, 8, 16, 32, 64, 128, 256, latchResolveChunk}, slices.Sorted(maps.Keys(sizes)))
}

// TestLatchResolve_ReportsStoppedFlowsOnly drives the resolver directly: it must report a stopped flow,
// stay silent about a running one, and - the load-bearing case - refuse a key whose token does not match
// the row, since a flow key is a capability and resolving on flow_id alone would answer a caller that
// never held it.
func TestLatchResolve_ReportsStoppedFlowsOnly(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	release := make(chan struct{})
	defer close(release)

	done := workflow.NewGraph("Done")
	done.SetEndpoint("Done", "latchresolve.verify:0/done")
	done.AddTransition("Done", workflow.END)
	proxy.HandleGraph("latchresolve.verify:0/done", done)
	proxy.HandleTask("latchresolve.verify:0/done", func(ctx context.Context, f *workflow.Flow) error { return nil })

	blocked := workflow.NewGraph("Blocked")
	blocked.SetEndpoint("Block", "latchresolve.verify:0/block")
	blocked.AddTransition("Block", workflow.END)
	proxy.HandleGraph("latchresolve.verify:0/blocked", blocked)
	proxy.HandleTask("latchresolve.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	// Two shards, so the resolver's per-shard grouping is exercised rather than assumed.
	assert.NoError(e.SetShard(ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2}))
	assert.NoError(e.Startup(t.Context()))

	stoppedKey, _, err := e.Run(ctx, "latchresolve.verify:0/done", nil, nil)
	if !assert.NoError(err) {
		return
	}
	runningKey, err := e.Create(ctx, "latchresolve.verify:0/blocked", nil, nil)
	if !assert.NoError(err) {
		return
	}

	shard, flowID, _, err := keys.ParseFlowKey(stoppedKey)
	if !assert.NoError(err) {
		return
	}
	forgedKey := fmt.Sprintf("%d-%d-%s", shard, flowID, "0123456789abcdef")

	resolved, err := e.resolveStoppedFlows(ctx, []string{stoppedKey, runningKey, forgedKey, "not-a-key"})
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, resolved[stoppedKey])
	_, running := resolved[runningKey]
	assert.False(running, "a running flow is not done waiting")
	_, forged := resolved[forgedKey]
	assert.False(forged, "the token is the capability: a wrong one must resolve nothing")
	_, garbage := resolved["not-a-key"]
	assert.False(garbage, "an unparseable key names no row")
}

// TestLatchResolve_ChunksWithoutDroppingKeys pins that a key set well past latchResolveChunk still
// resolves every stopped flow in it. A chunking bug is silent - the caller simply waits out its context -
// so the assertion is on the real flow surviving among synthetic keys that span several chunks.
func TestLatchResolve_ChunksWithoutDroppingKeys(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := NewTestProxy()
	g := workflow.NewGraph("Done")
	g.SetEndpoint("Done", "latchchunk.verify:0/done")
	g.AddTransition("Done", workflow.END)
	proxy.HandleGraph("latchchunk.verify:0/done", g)
	proxy.HandleTask("latchchunk.verify:0/done", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t)
	assert.NoError(e.SetHost(proxy))
	assert.NoError(e.Startup(t.Context()))

	stoppedKey, _, err := e.Run(ctx, "latchchunk.verify:0/done", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shard, flowID, _, err := keys.ParseFlowKey(stoppedKey)
	if !assert.NoError(err) {
		return
	}

	// Enough synthetic keys to span three chunks, with the real one buried in the middle of the sort order
	// the resolver imposes rather than at either end.
	askFor := make([]string, 0, 3*latchResolveChunk)
	for i := range 3 * latchResolveChunk {
		if i == flowID {
			continue
		}
		askFor = append(askFor, fmt.Sprintf("%d-%d-%s", shard, i+1, "0123456789abcdef"))
	}
	askFor = append(askFor, stoppedKey)

	resolved, err := e.resolveStoppedFlows(ctx, askFor)
	assert.NoError(err)
	assert.Equal(1, len(resolved), "only the one real, stopped flow resolves")
	assert.Equal(workflow.StatusCompleted, resolved[stoppedKey])
}
