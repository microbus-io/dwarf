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
	"encoding/json"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestForkOfForkflow walks the failed-fan-out partial-recovery pattern that is *the* use of Fork:
// fork one failed branch at a time, each fork re-failing cleanly via cohort
// accounting until every failed branch is fixed. Graph: A -> {X, Y} -> J (fan-in). Both X and Y fail (no
// onError) on the first run, so the flow fails. Fixing X and forking from X's step re-runs X but the fork
// still fails because Y's cloned failure keeps cohort_failures>0. Fixing Y and forking again from the fork's
// cloned Y step finally completes, with both branches' outputs merged at J. Neither origin is ever mutated.
func TestForkOfForkflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	// pass gates each branch: a branch errors until its flag is set. Guarded because the engine's workers
	// dispatch the branches concurrently.
	var mu sync.Mutex
	pass := map[string]bool{}
	setPass := func(k string) { mu.Lock(); pass[k] = true; mu.Unlock() }
	getPass := func(k string) bool { mu.Lock(); defer mu.Unlock(); return pass[k] }

	proxy := engine.NewTestProxy()
	g := workflow.NewGraph("ForkOfFork")
	g.SetEndpoint("A", "forkoffork.verify:428/a")
	g.SetEndpoint("X", "forkoffork.verify:428/x")
	g.SetEndpoint("Y", "forkoffork.verify:428/y")
	g.SetEndpoint("J", "forkoffork.verify:428/j")
	g.SetFanIn("J")
	g.AddTransition("A", "X")
	g.AddTransition("A", "Y")
	g.AddTransition("X", "J")
	g.AddTransition("Y", "J")
	g.AddTransition("J", workflow.END)
	assert.NoError(g.Validate())
	proxy.HandleGraph("forkoffork.verify:428/g", g)

	proxy.HandleTask("forkoffork.verify:428/a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("forkoffork.verify:428/x", func(ctx context.Context, f *workflow.Flow) error {
		if !getPass("X") {
			return errors.New("X not fixed yet")
		}
		f.SetString("xOut", "x-done")
		return nil
	})
	proxy.HandleTask("forkoffork.verify:428/y", func(ctx context.Context, f *workflow.Flow) error {
		if !getPass("Y") {
			return errors.New("Y not fixed yet")
		}
		f.SetString("yOut", "y-done")
		return nil
	})
	proxy.HandleTask("forkoffork.verify:428/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// Initial run: both branches fail -> the flow fails.
	originKey, out0, err := eng.Run(ctx, "forkoffork.verify:428/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, out0.Status)

	// Fix X, fork from X's step. X now completes, but Y's cloned failure keeps the fork's cohort_failures>0,
	// so the first fork re-fails cleanly (no wedge).
	setPass("X")
	xKey := stepKeyByTask(t, eng, originKey, "X")
	if !assert.NotEqual("", xKey) {
		return
	}
	fork1Key, err := eng.Fork(ctx, xKey, nil)
	if !assert.NoError(err) {
		return
	}
	fork1Out, err := eng.Await(ctx, fork1Key)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusFailed, fork1Out.Status) // re-fails, not wedged

	// Snapshot both origins' histories now, to prove the second fork never mutates them.
	originHistBefore := historyJSON(t, eng, originKey)
	fork1HistBefore := historyJSON(t, eng, fork1Key)

	// Fix Y, fork again from the *fork's* cloned Y step. Now X is already completed (kept from fork1) and Y
	// re-runs and completes -> the cohort resolves with no failures -> J fires -> the second fork completes.
	setPass("Y")
	yKey := stepKeyByTask(t, eng, fork1Key, "Y")
	if !assert.NotEqual("", yKey) {
		return
	}
	fork2Key, err := eng.Fork(ctx, yKey, nil)
	if !assert.NoError(err) {
		return
	}
	fork2Out, err := eng.Await(ctx, fork2Key)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, fork2Out.Status)
	assert.Equal("x-done", fork2Out.State.Value("xOut")) // X's output survived from the kept (cloned) branch
	assert.Equal("y-done", fork2Out.State.Value("yOut")) // Y's output from the re-run branch, merged at J

	// Neither origin was mutated by the second fork.
	assert.Equal(workflow.StatusFailed, snapshotStatus(t, eng, originKey))
	assert.Equal(workflow.StatusFailed, snapshotStatus(t, eng, fork1Key))
	assert.Equal(originHistBefore, historyJSON(t, eng, originKey))
	assert.Equal(fork1HistBefore, historyJSON(t, eng, fork1Key))
}

// historyJSON returns a flow's History serialized to JSON, for byte-for-byte stability comparison.
func historyJSON(t *testing.T, eng *engine.Engine, flowKey string) string {
	t.Helper()
	assert := testarossa.For(t)
	hist, err := eng.History(context.Background(), flowKey)
	assert.NoError(err)
	b, err := json.Marshal(hist)
	assert.NoError(err)
	return string(b)
}

// snapshotStatus returns a flow's current status.
func snapshotStatus(t *testing.T, eng *engine.Engine, flowKey string) string {
	t.Helper()
	assert := testarossa.For(t)
	out, err := eng.Snapshot(context.Background(), flowKey)
	assert.NoError(err)
	if out == nil {
		return ""
	}
	return out.Status
}
