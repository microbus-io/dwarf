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
	"testing"

	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

func TestDBStats_WindowDeltaCountsTheArchive(t *testing.T) {
	assert := testarossa.For(t)

	s := &dbStatsSampler{}
	before := dbStats{StepsBytes: 100, FlowsBytes: 10, ArchiveStepsBytes: 1000, ArchiveFlowsBytes: 200,
		WALBytes: 5000, WALFPI: 7, WALBuffersFull: 1, WALSyncs: 3, Commits: 50, ok: true}
	after := dbStats{StepsBytes: 104, FlowsBytes: 11, ArchiveStepsBytes: 9000, ArchiveFlowsBytes: 1700,
		WALBytes: 25000, WALFPI: 17, WALBuffersFull: 1, WALSyncs: 13, Commits: 450, ok: true}
	d := s.windowDelta(before, after)
	if assert.NotNil(d) {
		assert.Equal(dbDelta{StepsBytes: 4, FlowsBytes: 1, ArchiveStepsBytes: 8000, ArchiveFlowsBytes: 1500,
			WALBytes: 20000, WALFPI: 10, WALSyncs: 10, Commits: 400}, *d)
	}

	// The flush figures, from a 10 s window: 10 flushes of 2,000 bytes carrying 40 commits each.
	d = d.withFlush(10)
	assert.Equal(1.0, d.FlushesPerSec)
	assert.Equal(1000.0, d.FlushMs)
	assert.Equal(2000.0/1024, d.FlushKB)
	assert.Equal(40.0, d.CommitsPerFlush)
	var none *dbDelta
	assert.Nil(none.withFlush(10))
	unknown := &dbDelta{WALSyncs: -1}
	assert.Zero(unknown.withFlush(10).FlushMs)

	// A failed reading on either side yields no delta.
	before.ok = false
	assert.Nil(s.windowDelta(before, after))
}

// TestGaugePoints_CarryTheFloatSums pins that the gauge snapshot carries a float64 sum's points - sequel's cumulative
// pool-wait seconds, which the counter snapshot (int64 sums only) leaves out - so a window's pool wait is gaugesAfter
// less gaugesBefore.
func TestGaugePoints_CarryTheFloatSums(t *testing.T) {
	t.Parallel()
	reader := sdkmetric.NewManualReader()
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	_, err := meter.Float64ObservableCounter("sequel_pool_wait_duration_seconds", metric.WithFloat64Callback(
		func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(1.5, metric.WithAttributes(attribute.String("database", "d")))
			return nil
		}))
	if err != nil {
		t.Fatal(err)
	}
	if got := collectGauges(reader)["sequel_pool_wait_duration_seconds|database=d"]; got != 1.5 {
		t.Fatalf("pool wait %v, want 1.5", got)
	}
}
