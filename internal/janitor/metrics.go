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

package janitor

import (
	"context"
	"strconv"
	"time"

	"github.com/microbus-io/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The parts of a batch's time, the part attribute of dwarf_janitor_batch_seconds.
const (
	partTurn = "turn" // waiting for a turn on the shard's connections
	partWork = "work" // the batch's statements
)

// Instruments are the janitors' metrics, one set for every Janitor of an engine. A nil *Instruments records nothing.
type Instruments struct {
	batchSeconds metric.Float64Histogram
}

// NewInstruments creates the janitors' instruments on meter: dwarf_janitor_batch_seconds. It returns every instrument
// it could create, and the errors of those it could not.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	batchSeconds, err := meter.Float64Histogram("dwarf_janitor_batch_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time one batch of a janitor's job took, by shard, job and part: turn is the wait for a turn on the shard's connections, which the batch shares with dispatch; work is the batch's statements, including the waits for a connection from the pool. A job falling behind with most of its time in turn is held back by the shard's load; with most in work, by its statements or the pool."),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60))
	return &Instruments{batchSeconds: batchSeconds}, errors.Trace(err)
}

// batchAttrs is the attribute set of one job's batch timings for one part.
func batchAttrs(shard int, job, part string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("shard", strconv.Itoa(shard)), attribute.String("job", job),
		attribute.String("part", part))
}

// batch records one batch's time in its parts.
func (m *Instruments) batch(ctx context.Context, jb *job, turn, work time.Duration) {
	if m == nil || m.batchSeconds == nil {
		return
	}
	m.batchSeconds.Record(ctx, turn.Seconds(), jb.turnAttrs)
	m.batchSeconds.Record(ctx, work.Seconds(), jb.workAttrs)
}
