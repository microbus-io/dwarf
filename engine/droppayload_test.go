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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestDropPayload_AQueuedArrivalHoldsNoPayload pins that a worker queued at its cohort's fan-in holds none of its
// step's payload: neither the input it was claimed with, nor the output its task returned, nor the states minted from
// them. The test holds the cohort's stripe from its source task, so every branch's arrival queues in the cohort's
// batch, and reads each member's dispatch there. Each branch carries a large field in and writes one out, so a
// dispatch that kept either shows it.
func TestDropPayload_AQueuedArrivalHoldsNoPayload(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	const width = 8
	doc := strings.Repeat("x", 64<<10)

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
	// queued reports what each arrival queued in the cohort's batch still holds of its payload. The members wrote
	// their dispatches before joining under the stripe's mutex, and write none of these while they wait.
	queued := func() (holding [][]string, ok bool) {
		stripeMu.Lock()
		s, k := held, key
		stripeMu.Unlock()
		if s == nil {
			return nil, false
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		b := s.batches[k]
		if b == nil {
			return nil, false
		}
		for _, d := range b.members {
			holding = append(holding, payloadHeld(d))
		}
		return holding, true
	}

	mux := hosts.NewMux()
	g := workflow.NewGraph("DropPayload")
	g.SetURL("Seed", "dp/seed")
	g.SetURL("Branch", "dp/branch")
	g.SetURL("Join", "dp/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Seed", "Branch", "items", "item")
	g.AddTransitionChain("Branch", "Join", workflow.End)
	mux.HandleGraph("dp/g", g)
	mux.HandleTask("dp/seed", func(ctx context.Context, f *workflow.Flow) error {
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
		items := make([]int, width)
		for i := range items {
			items[i] = i
		}
		f.Set("items", items)
		f.Set("doc", doc)
		return nil
	})
	mux.HandleTask("dp/branch", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("out", doc)
		return nil
	})
	mux.HandleTask("dp/join", func(ctx context.Context, f *workflow.Flow) error { return nil })
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk, err := e.Create(ctx, "dp/g", nil, nil)
	assert.Must(assert.NoError(err))

	deadline := time.Now().Add(time.Minute)
	for {
		if holding, _ := queued(); len(holding) == width {
			for i, fields := range holding {
				assert.Len(fields, 0, "arrival %d holds %v while it waits at the fan-in", i, fields)
			}
			break
		}
		if !assert.True(time.Now().Before(deadline), "the cohort's %d arrivals never queued at its fan-in", width) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}

	release()
	awaitCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status, "the arrivals must land once the stripe is let go")
	}
}

// payloadHeld names the payload fields a dispatch still holds.
func payloadHeld(d *dispatch) (fields []string) {
	held := map[string]bool{
		"row.stateJSON":        d.row.stateJSON != nil,
		"row.priorChangesJSON": d.row.priorChangesJSON != nil,
		"row.baggageJSON":      d.row.baggageJSON != nil,
		"state":                d.state.Len() > 0,
		"priorChanges":         d.priorChanges.Len() > 0,
		"flow":                 d.flow != nil,
		"result":               d.result != nil,
		"changes":              d.changes.Len() > 0,
		"changesJSON":          d.changesJSON != nil,
		"successorBase":        d.successorBase.Len() > 0,
		"linearStateJSON":      d.linearStateJSON != nil,
		"linearRefsJSON":       d.linearRefsJSON != nil,
	}
	for name, ok := range held {
		if ok {
			fields = append(fields, name)
		}
	}
	return fields
}
