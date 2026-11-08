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
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
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

// TestLatchResolve_ReportsStoppedFlowsOnly drives the resolver directly across the four cases it has to
// tell apart: a stopped flow resolves to its status, a running one stays silent (its caller keeps waiting),
// and a key naming no row settles as unresolved rather than waiting out a row that will never change. The
// load-bearing case is the fourth - a key whose token does not match the row must land in the LAST group,
// never the first, since a flow key is a capability and resolving on flow_id alone would hand a caller an
// outcome it never held the key for.
func TestLatchResolve_ReportsStoppedFlowsOnly(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	release := make(chan struct{})

	done := workflow.NewGraph("Done")
	done.SetURL("Done", "latchresolve.verify:0/done")
	done.AddTransition("Done", workflow.End)
	mux.HandleGraph("latchresolve.verify:0/done", done)
	mux.HandleTask("latchresolve.verify:0/done", func(ctx context.Context, f *workflow.Flow) error { return nil })

	blocked := workflow.NewGraph("Blocked")
	blocked.SetURL("Block", "latchresolve.verify:0/block")
	blocked.AddTransition("Block", workflow.End)
	mux.HandleGraph("latchresolve.verify:0/blocked", blocked)
	mux.HandleTask("latchresolve.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	// Released here, after the Shutdown defer, so it unwinds FIRST: Shutdown drains the workers, and
	// one still holding this would never return.
	defer close(release)
	assert.NoError(e.SetHost(mux))
	// Two shards, so a board resolving only its own shard's keys is exercised rather than assumed.
	assert.NoError(e.SetShard(ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2}))
	assert.Must(assert.NoError(e.Startup(t.Context())))

	stoppedKey, _, err := e.Run(ctx, "latchresolve.verify:0/done", nil, nil)
	if !assert.NoError(err) {
		return
	}
	runningKey, err := e.Create(ctx, "latchresolve.verify:0/blocked", nil, nil)
	if !assert.NoError(err) {
		return
	}

	shard, hour, flowID, _, err := keys.ParseFlowKey(stoppedKey)
	if !assert.NoError(err) {
		return
	}
	forgedKey := keys.NewFlowKey(shard, hour, flowID, "0123456789abcdef")

	resolved, err := resolveOnEachShard(ctx, e, []string{stoppedKey, runningKey, forgedKey, "not-a-key"})
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, resolved[stoppedKey])
	_, running := resolved[runningKey]
	assert.False(running, "a running flow has not settled; its caller stays parked")
	// A key that names no row settles as flowUnresolved - the caller is woken to read the flow and get the
	// not-found. What must NEVER happen is the forged key resolving to the real flow's status: the token is
	// the capability, and answering on flow_id alone would hand the outcome to a caller who never held it.
	assert.Equal(flowUnresolved, resolved[forgedKey], "a wrong token must not resolve to the real status")
	assert.Equal(flowUnresolved, resolved["not-a-key"], "an unparseable key names no row")

	// A board asks only its own shard, so a key naming another shard names no row there.
	otherShard := 3 - shard
	resolved, err = e.resolveStoppedFlows(ctx, otherShard, []string{stoppedKey})
	assert.NoError(err)
	assert.Equal(flowUnresolved, resolved[stoppedKey], "a key is resolved only by its own shard's board")
}

// resolveOnEachShard asks each key's own shard's resolver, as the per-shard detectors do, and merges the
// answers. A key that does not parse is asked of shard 1, which settles it as naming no row.
func resolveOnEachShard(ctx context.Context, e *Engine, flowKeys []string) (map[string]string, error) {
	byShard := map[int][]string{}
	for _, flowKey := range flowKeys {
		shard, _, _, _, err := keys.ParseFlowKey(flowKey)
		if err != nil {
			shard = 1
		}
		byShard[shard] = append(byShard[shard], flowKey)
	}
	merged := map[string]string{}
	var errs []error
	for shard, shardKeys := range byShard {
		resolved, err := e.resolveStoppedFlows(ctx, shard, shardKeys)
		maps.Copy(merged, resolved)
		errs = append(errs, err)
	}
	return merged, errors.Join(errs...)
}

// TestLatchResolve_ChunksWithoutDroppingKeys pins that a key set well past latchResolveChunk still
// resolves every stopped flow in it. A chunking bug is silent - the caller simply waits out its context -
// so the assertion is on the real flow surviving among synthetic keys that span several chunks.
func TestLatchResolve_ChunksWithoutDroppingKeys(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Done")
	g.SetURL("Done", "latchchunk.verify:0/done")
	g.AddTransition("Done", workflow.End)
	mux.HandleGraph("latchchunk.verify:0/done", g)
	mux.HandleTask("latchchunk.verify:0/done", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(mux))
	assert.Must(assert.NoError(e.Startup(t.Context())))

	stoppedKey, _, err := e.Run(ctx, "latchchunk.verify:0/done", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shard, hour, flowID, _, err := keys.ParseFlowKey(stoppedKey)
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
		askFor = append(askFor, keys.NewFlowKey(shard, hour, i+1, "0123456789abcdef"))
	}
	askFor = append(askFor, stoppedKey)

	resolved, err := e.resolveStoppedFlows(ctx, shard, askFor)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, resolved[stoppedKey])
	// Every key is settled - the real one by its status, the synthetic ones as naming no row - and that
	// total is what catches a chunking bug: a dropped chunk leaves its keys absent from the map entirely,
	// which a check on the real key alone would miss whenever the real key landed in a surviving chunk.
	assert.Equal(len(askFor), len(resolved), "every key asked about must be settled")
	for _, k := range askFor {
		if k != stoppedKey {
			assert.Equal(flowUnresolved, resolved[k], "synthetic key %s names no row", k)
		}
	}
}

// TestLatchResolve_RecentStopScanIsOnlyAnOptimization pins the one property that keeps the recent-stop
// pre-scan safe: a flow whose stop fell out of the scan's window still resolves, because the IN lookup
// answers every key the scan did not. Trusting the scan alone would be SILENT - the key is simply never
// reported, and its caller waits out its whole deadline - so the stale flow is aged past the window
// deliberately rather than left to timing.
func TestLatchResolve_RecentStopScanIsOnlyAnOptimization(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Done")
	g.SetURL("Done", "latchrecent.verify:0/done")
	g.AddTransition("Done", workflow.End)
	mux.HandleGraph("latchrecent.verify:0/done", g)
	mux.HandleTask("latchrecent.verify:0/done", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(mux))
	assert.Must(assert.NoError(e.Startup(t.Context())))

	freshKey, _, err := e.Run(ctx, "latchrecent.verify:0/done", nil, nil)
	if !assert.NoError(err) {
		return
	}
	staleKey, _, err := e.Run(ctx, "latchrecent.verify:0/done", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shard, _, freshID, _, err := keys.ParseFlowKey(freshKey)
	if !assert.NoError(err) {
		return
	}
	_, _, staleID, _, err := keys.ParseFlowKey(staleKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}
	// BOTH timestamps are stated outright rather than inherited from how fast the harness ran. The window
	// is two sweep intervals - a hundred milliseconds - which is less than the setup above can take on a
	// loaded box, so a "fresh" row left to real time drifts out of the scan and the test silently stops
	// covering the scan at all (it still passes, on the lookup). Written with NOW_UTC()/DATE_ADD_MILLIS
	// rather than a bound Go time, so both land on the shard's own clock like every other timestamp write.
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET updated_at=NOW_UTC() WHERE flow_id=?", freshID)
	if !assert.NoError(err) {
		return
	}
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE flow_id=?", staleID)
	if !assert.NoError(err) {
		return
	}

	// One pass, both paths: the fresh stop is settled by the scan, the stale one only by the lookup.
	resolved, err := e.resolveStoppedFlows(ctx, shard, []string{freshKey, staleKey})
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, resolved[freshKey],
		"a stop inside the scan's window is settled without the lookup naming it")
	assert.Equal(workflow.StatusCompleted, resolved[staleKey],
		"a stop older than the scan's window must still be reported by the lookup")
}

// TestLatch_StalledShardHoldsOnlyItsOwnAwaiters pins that the detector has no barrier across shards. A caller
// is parked on a running flow on each shard; shard 2's turnstile is then closed off, so any read of shard 2
// waits until its context ends. Shard 1's flow completes with its local wake dropped, which leaves the
// detector as its only path - and its Await must return while shard 2 is still stalled. A pass that gathers
// every shard before releasing anything holds it for as long as shard 2 does.
func TestLatch_StalledShardHoldsOnlyItsOwnAwaiters(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()

	release := make(chan struct{})
	started := make(chan struct{}, 1)
	unblock := sync.OnceFunc(func() { close(release) })
	mux := hosts.NewMux()
	g := workflow.NewGraph("Stall")
	g.SetURL("Work", "stall/work")
	g.SetURL("Then", "stall/then")
	g.AddTransition("Work", "Then")
	g.AddTransition("Then", workflow.End)
	mux.HandleGraph("stall/g", g)
	mux.HandleTask("stall/then", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("stall/work", func(ctx context.Context, f *workflow.Flow) error {
		shard, _, _, _, _ := keys.ParseFlowKey(f.FlowKey())
		switch shard {
		case 1:
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
		case 2:
			f.Sleep(time.Hour) // delays Then, keeping the flow running and its key parked for the whole test
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	t.Cleanup(unblock) // runs first, so a failed test frees the worker
	assert.NoError(e.SetShard(ShardSpec{Index: 1}))
	assert.NoError(e.SetShard(ShardSpec{Index: 2}))
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(ctx)))

	// Placement is random, so create until each shard holds a flow. The extras finish at once (shard 2's
	// sleep, shard 1's gate only for the first).
	var onShard [3]string
	for i := 0; i < 200 && (onShard[1] == "" || onShard[2] == ""); i++ {
		fk, err := e.Create(ctx, "stall/g", nil, nil)
		assert.Must(assert.NoError(err))
		shard, _, _, _, _ := keys.ParseFlowKey(fk)
		if onShard[shard] == "" {
			onShard[shard] = fk
		}
	}
	assert.Must(assert.True(onShard[1] != "" && onShard[2] != "", "placement never reached both shards"))
	<-started

	type result struct {
		outcome *workflow.FlowOutcome
		err     error
	}
	awaited := map[int]chan result{}
	for _, shard := range []int{1, 2} {
		parked := e.seams.Waiter(engineseams.Join(engineseams.CheckpointAwaitParked, onShard[shard]))
		awaited[shard] = make(chan result, 1)
		go func() {
			out, err := e.Await(ctx, onShard[shard])
			awaited[shard] <- result{out, err}
		}()
		select {
		case <-parked:
		case r := <-awaited[shard]:
			t.Fatalf("Await on shard %d returned instead of parking: %+v, %v", shard, r.outcome, r.err)
		case <-time.After(10 * time.Second):
			t.Fatalf("Await on shard %d never parked", shard)
		}
	}

	size := e.turnstiles.Turnstile(2).Size()
	e.turnstiles.Resize(2, 0)
	t.Cleanup(func() { e.turnstiles.Resize(2, size) }) // before Shutdown, which runs later
	e.seams.InjectN(engineseams.FaultDropSignalStop, 1<<20)
	unblock()

	select {
	case r := <-awaited[1]:
		if assert.NoError(r.err) {
			assert.Equal(workflow.StatusCompleted, r.outcome.Status)
		}
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		t.Fatal("a stalled shard 2 held shard 1's Await")
	}
	select {
	case r := <-awaited[2]:
		t.Fatalf("shard 2's Await returned while its flow sleeps: %v, %v", r.outcome, r.err)
	default:
	}
}
