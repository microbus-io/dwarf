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

package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/microbus-io/testarossa"
)

// TestEndedByStatus_SplitsTheAttributedSeries pins that the drain reads the per-status series and not the bare
// total: the total counts every terminal status, so reading it as "completed" would pass a run whose flows
// failed.
func TestEndedByStatus_SplitsTheAttributedSeries(t *testing.T) {
	assert := testarossa.For(t)

	completed, other := endedByStatus(map[string]int64{
		"dwarf_flows_ended": 110,
		"dwarf_flows_ended|priority=100|shard=1|status=completed|workflow=bench/linear": 60,
		"dwarf_flows_ended|priority=100|shard=2|status=completed|workflow=bench/linear": 40,
		"dwarf_flows_ended|priority=100|shard=1|status=failed|workflow=bench/linear":    7,
		"dwarf_flows_ended|priority=100|shard=1|status=cancelled|workflow=bench/linear": 3,
		"dwarf_flows_started|priority=100|shard=1|workflow=bench/linear":                110,
	})
	assert.Equal(int64(100), completed)
	assert.Equal(map[string]int64{"failed": 7, "cancelled": 3}, other)
}

// TestWindowInvalidity_DrainAndStall pins what the drain and the watchdog invalidate: a stall always, a drain
// only when a flow is unfinished, ended otherwise, or missing from the arithmetic.
func TestWindowInvalidity_DrainAndStall(t *testing.T) {
	assert := testarossa.For(t)
	healthy := func() stepResult {
		return stepResult{
			EngineCounters: map[string]int64{"dwarf_steps_executed": 1},
			GaugesBefore:   map[string]float64{"g": 1},
			GaugesAfter:    map[string]float64{"g": 1},
		}
	}

	res := healthy()
	res.Drain = &drainResult{Created: 50, Completed: 50}
	assert.Equal("", windowInvalidity(res, 0, 0))

	// A failed collection at either window edge leaves its snapshot empty.
	res = healthy()
	res.GaugesBefore = nil
	assert.Contains(windowInvalidity(res, 0, 0), "collection failed")
	res = healthy()
	res.EngineCounters = map[string]int64{}
	assert.Contains(windowInvalidity(res, 0, 0), "collection failed")

	res = healthy()
	res.Stalled = "no task started"
	assert.Contains(windowInvalidity(res, 0, 0), "stalled")

	for _, d := range []drainResult{
		{Created: 50, Completed: 48, Unfinished: 2},                             // gave up with flows in flight
		{Created: 50, Completed: 48, EndedOther: map[string]int64{"failed": 2}}, // ended, not completed
		{Created: 50, Completed: 48},                                            // two flows vanished
	} {
		res = healthy()
		res.Drain = &d
		assert.Contains(windowInvalidity(res, 0, 0), "drain", "%+v", d)
	}
}

// TestDrainOutstanding_GivesUpOnSilenceNotOnTime pins that the drain waits out a slow backlog for as long as it
// keeps draining, counts a task starting as progress, and counts as unfinished only what is left once nothing
// has moved for the quiet period.
func TestDrainOutstanding_GivesUpOnSilenceNotOnTime(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Ten flows ending one every 250ms take ~2.5s, five times the quiet period: all of them are waited out.
	var left atomic.Int64
	left.Store(10)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for left.Load() > 0 {
			select {
			case <-stop:
				return
			case <-time.After(250 * time.Millisecond):
				left.Add(-1)
			}
		}
	}()
	noTasks := func() int64 { return 0 }
	d := drainOutstanding(nil, map[string]int64{}, 10, left.Load, noTasks, 500*time.Millisecond)
	assert.Equal(int64(0), d.Unfinished)
	assert.True(d.Sec > 2, "the drain must outlast the quiet period while flows keep ending, took %.1fs", d.Sec)
	assert.True(d.FlowsPerSec > 0)

	// Flows that end only after a long silence, with tasks starting throughout - long multi-step flows - are
	// waited out too: no flow ends for 1.5s, three quiet periods, and then all of them do.
	var tasks atomic.Int64
	held := time.Now()
	go func() {
		for time.Since(held) < 1500*time.Millisecond {
			select {
			case <-stop:
				return
			case <-time.After(100 * time.Millisecond):
				tasks.Add(1)
			}
		}
	}()
	outstanding := func() int64 {
		if time.Since(held) < 1500*time.Millisecond {
			return 4
		}
		return 0
	}
	d = drainOutstanding(nil, map[string]int64{}, 4, outstanding, tasks.Load, 500*time.Millisecond)
	assert.Equal(int64(0), d.Unfinished, "a task starting is progress")

	// A backlog that stops draining is given up on after the quiet period, with what is left unfinished.
	start := time.Now()
	d = drainOutstanding(nil, map[string]int64{}, 3, func() int64 { return 3 }, noTasks, 500*time.Millisecond)
	assert.Equal(int64(3), d.Unfinished)
	assert.True(time.Since(start) < 2*time.Second)
}

// TestWatchProgress_FiresOnlyWhenNothingMoves pins the watchdog every mode runs: it stays quiet while tasks
// start, or while nothing is in flight, and fires - ending the run - once work is in flight and nothing moves.
func TestWatchProgress_FiresOnlyWhenNothingMoves(t *testing.T) {
	// Not parallel: guard and stalledRun are the process's own run guards.
	assert := testarossa.For(t)
	defer func(stall time.Duration) { guard.stall = stall; stalledRun.Store(false) }(guard.stall)
	guard.stall = 1500 * time.Millisecond

	// Nothing in flight: silence is not a stall.
	w := watchProgress(context.Background(), nil, func() int64 { return 0 })
	w.pause(3 * time.Second)
	assert.Equal("", w.reason())
	w.stop()

	// Work in flight and tasks starting: not a stall either.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
				tasksStarted.Add(1)
			}
		}
	}()
	w = watchProgress(context.Background(), nil, func() int64 { return 5 })
	w.pause(3 * time.Second)
	close(stop)
	assert.Equal("", w.reason())
	w.stop()
	assert.False(stalledRun.Load())

	// Work in flight and nothing moving: pause returns early, and the run is marked stalled.
	start := time.Now()
	w = watchProgress(context.Background(), nil, func() int64 { return 5 })
	w.pause(time.Minute)
	assert.True(time.Since(start) < 10*time.Second, "pause must end at the stall, took %s", time.Since(start))
	assert.Contains(w.reason(), "5 flows in flight")
	assert.True(stalledRun.Load())
	w.stop()
}

// TestCollectAll_OneFailedReplicaFailsTheCollection pins that a collection missing one replica reads as failed,
// not as the other replicas' sum: an under-count, or a negative delta against a full opening total, would pass
// every check as a real measurement.
func TestCollectAll_OneFailedReplicaFailsTheCollection(t *testing.T) {
	assert := testarossa.For(t)

	healthy := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(healthy))
	defer mp.Shutdown(context.Background())
	c, err := mp.Meter("test").Int64Counter("dwarf_steps_executed")
	assert.NoError(err)
	c.Add(context.Background(), 5)
	failed := sdkmetric.NewManualReader() // never registered, so its Collect errors

	assert.Equal(int64(5), collectAllCounters([]*sdkmetric.ManualReader{healthy})["dwarf_steps_executed"])
	assert.Nil(collectAllCounters([]*sdkmetric.ManualReader{healthy, failed}))
	assert.Nil(collectAllGauges([]*sdkmetric.ManualReader{healthy, failed}))
	_, ok := collectAllHistograms([]*sdkmetric.ManualReader{healthy})
	assert.True(ok, "no histograms recorded is a reading")
	_, ok = collectAllHistograms([]*sdkmetric.ManualReader{healthy, failed})
	assert.False(ok)

	// A window whose histograms alone failed is not a measurement.
	res := stepResult{
		EngineCounters: map[string]int64{"dwarf_steps_executed": 1},
		GaugesBefore:   map[string]float64{"g": 1},
		GaugesAfter:    map[string]float64{"g": 1},
		HistsMissing:   true,
	}
	assert.Contains(windowInvalidity(res, 0, 0), "collection failed")

	// A gauge tick whose collection failed is left out of the window's means rather than averaged in as zeros.
	s := startGaugeSampler([]*sdkmetric.ManualReader{failed})
	time.Sleep(4 * gaugeSampleInterval)
	g := s.close()
	assert.Equal(0, g.Samples)
	assert.True(g.Failed > 0)

	// A delta against a failed snapshot is no deltas, never the other snapshot's raw totals.
	assert.Len(counterDeltas(nil, map[string]int64{"x": 7}), 0)
	assert.Len(counterDeltas(map[string]int64{"x": 7}, nil), 0)
}
