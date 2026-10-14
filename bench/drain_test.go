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
	"testing"

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
			GaugesAfter:    map[string]float64{"g": 1},
		}
	}

	res := healthy()
	res.Drain = &drainResult{Created: 50, Completed: 50}
	assert.Equal("", windowInvalidity(res, 0, 0))

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
