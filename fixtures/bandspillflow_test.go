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

// TestBandSpillflow pins that a better band which cannot fill the cache does not starve a worse one: the worse
// band gets the capacity the better band leaves unused.
//
// The shape is a sparse but CONTINUOUS better band over a backlog at a worse band. A free-running trickle
// cannot pin it: on a fast local database the better band's steps are claimed so quickly that many scans find
// none of them due, and every such scan lets the backlog through. So the refill loops are HELD at their checkpoints while each round of the better band is created, then stepped one
// turn each - a scan, then a push planned from it: every scan the dispatcher makes sees better-band work due,
// and never enough to fill the cache.
//
// Every flow is created by a replica with no workers, so its doorbell cannot dispatch it and the only way to
// any of this work is the dispatcher's own plan.
func TestBandSpillflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	const name = "bandspill.verify:428"
	var mu sync.Mutex
	ranLow := 0
	buildMux := func() *hosts.Mux {
		mux := hosts.NewMux()
		g := workflow.NewGraph("BandSpill")
		g.SetURL("Work", name+"/work")
		g.AddTransition("Work", workflow.End)
		mux.HandleGraph(name+"/g", g)
		mux.HandleTask(name+"/work", func(ctx context.Context, f *workflow.Flow) error {
			if f.GetString("band") == "low" {
				mu.Lock()
				ranLow++
				mu.Unlock()
			}
			return nil
		})
		return mux
	}

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetHost(buildMux()))
	assert.NoError(eng.SetWorkers(8)) // a cache of 16
	creator := engine.NewEngineUnderTest(t.Name())
	defer creator.Shutdown(ctx)
	assert.NoError(creator.SetHost(buildMux()))
	assert.NoError(creator.SetWorkers(0))
	assert.Must(assert.NoError(eng.Startup(ctx)))
	assert.Must(assert.NoError(creator.Startup(ctx)))

	seams := engineseams.Seams(eng)
	loops := []string{engineseams.CheckpointRefillTallyDone, engineseams.CheckpointRefillCycleDone}
	hold := func(cp string) {
		seams.Break(cp)
		assert.True(seams.WaitTimeout(ctx, cp, 10*time.Second*enginetest.TimeoutScale()), "the loop never came round to %s", cp)
	}
	// turn lets one held loop run exactly one more cycle. The SCAN goes first: a push released alongside it can
	// plan from the tally before, which predates this round's better band.
	turn := func(cp string) {
		seams.Resume(cp)
		hold(cp)
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
	// await reports, on a miss, why each unfinished flow missed: the Await's error and its one step's status.
	// The step tells the causes apart - pending at attempt 0 was never claimed (the held loops supplied no
	// hint for it, or the hint was lost), running was claimed but did not finish, completed was finished but
	// not observed.
	await := func(keys []string) bool {
		awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second*enginetest.TimeoutScale())
		defer cancel()
		ok := true
		for _, k := range keys {
			out, err := eng.Await(awaitCtx, k)
			if err == nil {
				assert.Equal(workflow.StatusCompleted, out.Status)
				continue
			}
			ok = false
			steps, herr := eng.History(ctx, k)
			if herr != nil {
				t.Logf("flow %s: await: %v; history: %v", k, err, herr)
				continue
			}
			for _, s := range steps {
				t.Logf("flow %s: await: %v; step %s status=%s attempt=%d", k, err, s.TaskName, s.Status, s.Attempt)
			}
		}
		return ok
	}

	for _, cp := range loops {
		hold(cp)
	}
	const lowCount = 48
	lows := create("low", 5, lowCount)
	for round := range 30 {
		highs := create("high", 1, 4)
		for _, cp := range loops {
			turn(cp)
		}
		// The push just planned from a scan that saw this round's better band, which fits, so it carried all of
		// it; the workers drain the push while the loops are held.
		if !assert.True(await(highs), "the better band itself was not served in round %d", round) {
			return
		}
	}

	mu.Lock()
	got := ranLow
	mu.Unlock()
	assert.Equal(lowCount, got, "the worse band starved behind a better band that never fills the cache")
	assert.True(await(lows), "the worse band starved behind a better band that never fills the cache")
}
