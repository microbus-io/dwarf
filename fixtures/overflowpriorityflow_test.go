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
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestOverflowPriorityflow pins, end to end, that work stored at a priority past the dispatching replica's
// maximum runs, after every band in its range, and that Continue and Fork carry such a priority on rather than
// refusing it.
//
// The work comes from a replica configured with a larger maximum, one of the real ways a step gets there. The
// dispatcher's own Create would refuse the priority, so nothing below a fixture shows the whole path: the scan
// counting the overflow, the fetch matching it, the claim, the successor offered at that priority, and the
// derived operations inheriting it on a replica whose range does not hold it.
//
// The dispatcher's refill loops are held at their checkpoints and turned one cycle at a time, as in
// bandspillflow, so which band a push carried is decided by the plan and not by timing. The creator has no
// workers, so its doorbell dispatches nothing.
func TestOverflowPriorityflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const name = "overflowpriority.verify:428"
	var mu sync.Mutex
	ran := map[string]int{}
	buildMux := func() *hosts.Mux {
		mux := hosts.NewMux()
		g := workflow.NewGraph("OverflowPriority")
		g.SetURL("First", name+"/first")
		g.SetURL("Second", name+"/second")
		g.AddTransition("First", "Second")
		g.AddTransition("Second", workflow.End)
		mux.HandleGraph(name+"/g", g)
		record := func(ctx context.Context, f *workflow.Flow) error {
			mu.Lock()
			ran[f.GetString("band")]++
			mu.Unlock()
			return nil
		}
		mux.HandleTask(name+"/first", record)
		mux.HandleTask(name+"/second", record)
		return mux
	}
	ranAt := func(band string) int {
		mu.Lock()
		defer mu.Unlock()
		return ran[band]
	}

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetHost(buildMux()))
	assert.NoError(eng.SetWorkers(2)) // a cache of 4; the default maximum, 5
	creator := engine.NewEngineUnderTest(t.Name())
	defer creator.Shutdown(ctx)
	assert.NoError(creator.SetHost(buildMux()))
	assert.NoError(creator.SetWorkers(0))
	assert.NoError(creator.SetMaxPriority(10))
	assert.NoError(eng.Startup(ctx))
	assert.NoError(creator.Startup(ctx))

	seams := engineseams.Seams(eng)
	loops := []string{engineseams.CheckpointRefillTallyDone, engineseams.CheckpointRefillCycleDone}
	hold := func(cp string) {
		seams.Break(cp)
		assert.True(seams.WaitTimeout(ctx, cp, 10*time.Second*enginetest.TimeoutScale()), "the loop never came round to %s", cp)
	}
	// One more cycle of each loop, the scan first so the push plans from a tally that saw the work.
	turn := func() {
		for _, cp := range loops {
			seams.Resume(cp)
			hold(cp)
		}
	}
	// Declared after the Shutdown defers so it unwinds first: a held loop would wedge the drain.
	defer func() {
		for _, cp := range loops {
			seams.Resume(cp)
		}
	}()

	create := func(band string, priority, n int) []string {
		keys := make([]string, 0, n)
		for range n {
			k, err := creator.Create(ctx, name+"/g", map[string]any{"band": band}, &workflow.FlowOptions{Priority: priority})
			if assert.NoError(err) {
				keys = append(keys, k)
			}
		}
		return keys
	}
	await := func(keys ...string) bool {
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
		defer cancel()
		for _, k := range keys {
			out, err := eng.Await(awaitCtx, k)
			if err != nil {
				return false
			}
			assert.Equal(workflow.StatusCompleted, out.Status)
		}
		return true
	}

	for _, cp := range loops {
		hold(cp)
	}
	ins := create("in", 5, 4) // the dispatcher's worst band, deep enough to fill its cache
	outs := create("out", 8, 2)

	// The range fills the cache, so the plan stops at band 5 and the overflow is not even tallied. Each
	// successor is offered at its own band, so the in-range flows finish on this one push.
	turn()
	if !assert.True(await(ins...), "the in-range band was not served") {
		return
	}
	assert.Equal(0, ranAt("out"), "the overflow ran before the band in the range that filled the cache")

	// With the range drained, the overflow is the best band due: the next push carries it, and its successors.
	turn()
	if !assert.True(await(outs...), "work past the maximum was stranded") {
		return
	}
	assert.Equal(4, ranAt("out"))

	// Continue and Fork on the dispatcher, whose own Create would refuse priority 8, inherit it.
	cont, err := eng.Continue(ctx, outs[0], nil)
	assert.NoError(err)
	history, err := eng.History(ctx, outs[1])
	assert.NoError(err)
	var fork string
	if assert.True(len(history) > 0) {
		fork, err = eng.Fork(ctx, history[0].StepKey, nil)
		assert.NoError(err)
	}
	turn() // in case a doorbell found no room; the derived flows' steps are then the plan's
	assert.True(await(cont, fork), "a derived flow at the inherited overflow priority was stranded")
	assert.Equal(4, enginetest.CountRows(t, eng, 1, "SELECT COUNT(*) FROM dwarf_flows WHERE priority=8"),
		"the two created flows, the continuation and the fork all keep priority 8")
}
