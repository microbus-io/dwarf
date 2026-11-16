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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestFanInHold_AQueuedFanInStopsTheCrew pins what the fan-in hold is for: a cohort whose arrivals cannot land
// does not grow the crew past the hold's depth. The test stands in for a slow fan-in by holding the cohort's
// stripe itself, so every branch that finishes queues at the fan-in holding its candidate and no turn - the state
// in which, without the hold, every take finds nobody idle and spawns another worker, one per branch claimed.
//
// It measures only once the fan-in has the hold's depth queued, so a slow run (-race) cannot pass on a crew that never
// had a reason to grow. Without the hold the crew then runs to the cohort's width; with it, the crew settles under its
// floor plus the depth plus one cache of candidates already handed out when the hold engaged, and the flow still
// completes once the stripe is let go - the hold releases as its arrivals land.
func TestFanInHold_AQueuedFanInStopsTheCrew(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	var width atomic.Int64

	// The stripe the source task takes, and whether the test is done with it. Every branch queues behind it and
	// Shutdown waits for them, so it is let go however the test ends, and never taken after.
	var stripeMu sync.Mutex
	var held *cohortStripe
	var key arrivalKey
	finished := false
	release := func() {
		stripeMu.Lock()
		defer stripeMu.Unlock()
		finished = true
		if held != nil {
			held.unlock()
			held = nil
		}
	}
	defer release()
	// atFanIn reads the cohort's count at its fan-in and whether its lineage is held.
	atFanIn := func() (c fanInCount, seeded bool) {
		stripeMu.Lock()
		s, k := held, key
		stripeMu.Unlock()
		if s == nil {
			return c, false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.atFanIn[k], true
	}

	mux := hosts.NewMux()
	g := workflow.NewGraph("FanInHold")
	g.SetURL("Seed", "fih/seed")
	g.SetURL("Branch", "fih/branch")
	g.SetURL("Join", "fih/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Seed", "Branch", "items", "item")
	g.AddTransitionChain("Branch", "Join", workflow.End)
	mux.HandleGraph("fih/g", g)
	mux.HandleTask("fih/seed", func(ctx context.Context, f *workflow.Flow) error {
		// The forEach source is its cohort's spawn, so its own step id names the stripe every arrival takes.
		shard, _, _, stepID, _, err := keys.ParseStepKey(f.StepKey())
		if err != nil {
			return err
		}
		stripeMu.Lock()
		if !finished && held == nil {
			key = arrivalKey{shard: shard, spawn: stepID}
			held = &e.cohortLocks[cohortLockStripe(shard, stepID)]
			held.lock(time.Hour, func() (time.Duration, bool) { return time.Hour, true })
		}
		stripeMu.Unlock()
		items := make([]int, width.Load())
		for i := range items {
			items[i] = i
		}
		f.Set("items", items)
		return nil
	})
	mux.HandleTask("fih/branch", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("fih/join", func(ctx context.Context, f *workflow.Flow) error { return nil })
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	floor, depth, capacity := e.crew.Min(), int(e.fanInHoldAt.Load()), e.cache.Capacity()
	bound := floor + depth + capacity
	width.Store(int64(3 * bound))
	assert.Must(assert.True(int64(bound) < int64(e.crew.Max()), "the crew's maximum must leave room to see it grow"))

	fk, err := e.Create(ctx, "fih/g", nil, nil)
	assert.Must(assert.NoError(err))

	// Measure only once the fan-in has as many queued as the hold's depth - the state the hold is for. Before it,
	// the crew has not had a reason to grow, and a quiet crew says nothing.
	deadline := time.Now().Add(time.Minute)
	for {
		if c, _ := atFanIn(); c.workers >= depth {
			break
		}
		if !assert.True(time.Now().Before(deadline), "the fan-in never had %d workers queued", depth) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	// Then wait for the crew to stop growing: no new peak for half a second, with the stripe still held.
	peak, quietSince := e.crew.Resident(), time.Now()
	for time.Since(quietSince) < 500*time.Millisecond && time.Now().Before(deadline) {
		if r := e.crew.Resident(); r > peak {
			peak, quietSince = r, time.Now()
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.True(peak <= bound, "the crew grew to %d behind a held fan-in, past floor %d + depth %d + cache %d", peak, floor, depth, capacity)
	c, _ := atFanIn()
	assert.True(c.held, "the lineage is held while %d workers wait at its fan-in", c.workers)

	release()
	awaitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status, "the hold must release as the arrivals land")
	}
}
