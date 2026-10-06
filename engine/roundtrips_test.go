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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestRoundTrips_LinearStep measures k, the round trips one linear step makes, by charging every round trip a
// simulated delay and timing linear flows of two lengths: the difference divided by the extra steps cancels the
// fixed costs of creating, completing and awaiting a flow. Opt-in (DWARF_MEASURE_K=1), since it is slow by
// design; run it against the database whose k is wanted (SEQUEL_TESTING_DSN).
func TestRoundTrips_LinearStep(t *testing.T) {
	if os.Getenv("DWARF_MEASURE_K") == "" {
		t.Skip("set DWARF_MEASURE_K=1 to measure")
	}
	assert := testarossa.For(t)
	ctx := context.Background()
	const rtt = 20 * time.Millisecond

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.SetWorkers(1))
	assert.NoError(e.Startup(t.Context()))

	run := func(steps int) time.Duration {
		name := "rt/linear" + strconv.Itoa(steps)
		g := workflow.NewGraph("Linear" + strconv.Itoa(steps))
		for i := range steps {
			task := fmt.Sprintf("S%d", i)
			g.SetURL(task, "rt/s")
			if i+1 < steps {
				g.AddTransition(task, fmt.Sprintf("S%d", i+1))
			} else {
				g.AddTransition(task, workflow.End)
			}
		}
		assert.NoError(g.Validate())
		mux.HandleGraph(name, g)
		start := time.Now()
		_, outcome, err := e.Run(ctx, name, nil, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		return time.Since(start)
	}
	mux.HandleTask("rt/s", func(ctx context.Context, f *workflow.Flow) error { return nil })

	run(5) // warm the statement caches on the connections before timing
	db, err := e.db.Shard(1)
	assert.NoError(err)
	db.SimulateRTT(rtt)
	defer db.SimulateRTT(0)
	// What one simulated round trip really costs: the timer oversleeps by an amount that varies by platform, and
	// dividing by the nominal delay would count the oversleep as extra round trips.
	const probes = 30
	start := time.Now()
	for range probes {
		_, err := db.ExecContext(ctx, "SELECT 1")
		assert.NoError(err)
	}
	perRT := time.Since(start) / probes
	short, long := run(10), run(30)
	k := float64(long-short) / float64(20*perRT)
	t.Logf("driver=%s rtt=%v per-round-trip=%v short=%v long=%v k=%.2f round trips per linear step",
		db.DriverName(), rtt, perRT, short, long, k)
}

// TestRoundTrips_FanInCycle measures the round trips of one fan-out cycle - a source fanning out over two
// elements, the two branch steps (one a plain arrival, one the arrival that resolves the cohort) and the fan-in
// step - the same way TestRoundTrips_LinearStep measures a linear step: chains of cycles of two lengths, the
// difference divided by the extra cycles. Opt-in (DWARF_MEASURE_K=1).
func TestRoundTrips_FanInCycle(t *testing.T) {
	if os.Getenv("DWARF_MEASURE_K") == "" {
		t.Skip("set DWARF_MEASURE_K=1 to measure")
	}
	assert := testarossa.For(t)
	ctx := context.Background()
	const rtt = 20 * time.Millisecond

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	mux := NewHostMux()
	e.SetHost(mux)
	assert.NoError(e.SetWorkers(1))
	assert.NoError(e.Startup(t.Context()))

	// Cycle i: Src<i> -forEach-> Work<i> -> Join<i>, and Join<i> is Src<i+1>.
	run := func(cycles int) time.Duration {
		name := "rt/fanin" + strconv.Itoa(cycles)
		g := workflow.NewGraph("FanIn" + strconv.Itoa(cycles))
		for i := range cycles {
			src, work, join := fmt.Sprintf("J%d", i), fmt.Sprintf("W%d", i), fmt.Sprintf("J%d", i+1)
			g.SetURL(src, "rt/s")
			g.SetURL(work, "rt/s")
			g.SetFanIn(join)
			g.AddTransitionForEach(src, work, "items", "n")
			g.AddTransition(work, join)
		}
		g.SetURL(fmt.Sprintf("J%d", cycles), "rt/s")
		g.AddTransition(fmt.Sprintf("J%d", cycles), workflow.End)
		assert.NoError(g.Validate())
		mux.HandleGraph(name, g)
		start := time.Now()
		_, outcome, err := e.Run(ctx, name, map[string]any{"items": []int{1, 2}}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		return time.Since(start)
	}
	mux.HandleTask("rt/s", func(ctx context.Context, f *workflow.Flow) error { return nil })

	run(3) // warm the statement caches on the connections before timing
	run(9)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	db.SimulateRTT(rtt)
	defer db.SimulateRTT(0)
	const probes = 30
	start := time.Now()
	for range probes {
		_, err := db.ExecContext(ctx, "SELECT 1")
		assert.NoError(err)
	}
	perRT := time.Since(start) / probes
	short, long := run(3), run(9)
	k := float64(long-short) / float64(6*perRT)
	t.Logf("driver=%s rtt=%v per-round-trip=%v short=%v long=%v k=%.2f round trips per fan-out cycle",
		db.DriverName(), rtt, perRT, short, long, k)
}
