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
	partTurn    = "turn"    // waiting for a turn on the shard's connections
	partAcquire = "acquire" // waiting for the batch's connection from the pool
	partWork    = "work"    // the batch's statements
)

// Instruments are the janitors' metrics, one set for every Janitor of an engine. A nil *Instruments records nothing.
type Instruments struct {
	batchSeconds metric.Float64Histogram
	archived     metric.Int64Counter
	moveFailures metric.Int64Counter
	reaped       metric.Int64Counter
}

// The descriptions of the counters internal/reaper creates too, on the same meter, from these: two instruments of one
// name and different descriptions are exported as two streams of that name.
const (
	ArchivedDescription     = "Counts flows moved from the live tables into the archive, by shard: each root and every subgraph flow of its tree. Its rate plus dwarf_flows_reaped's, against dwarf_flows_closed, is archiving keeping up - a closed root waits the archive grace (a minute) in the live tables, so the two track each other a minute apart."
	MoveFailuresDescription = "Failed archive attempts, including sizing, contention and deadlines; counts attempts, not distinct trees."
	ReapedDescription       = "Counts flows deleted from the live tables WITHOUT archiving them, by shard and by reason: deleted (a tree Delete or DeleteOnCompletion scheduled for deletion, or a tree a retention of 0 stamped for deletion as too large for one transaction), no_retention (a retention of 0 keeps no archive), and archive_failed (an unarchived tree reached its retention cutoff). archive_failed is records LOST from the archive and must stay zero; dwarf_archive_failing_seconds says since when it has been failing, and the logs say why."
)

// NewInstruments creates the janitors' instruments on meter: dwarf_janitor_batch_seconds, dwarf_flows_archived,
// dwarf_archive_move_failures and dwarf_flows_reaped. It returns every instrument it could create, and the errors of
// those it could not.
func NewInstruments(meter metric.Meter) (*Instruments, error) {
	var errs []error
	ctr := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return c
	}
	m := &Instruments{}
	m.archived = ctr("dwarf_flows_archived", ArchivedDescription)
	m.moveFailures = ctr("dwarf_archive_move_failures", MoveFailuresDescription)
	m.reaped = ctr("dwarf_flows_reaped", ReapedDescription)
	batchSeconds, err := meter.Float64Histogram("dwarf_janitor_batch_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time one batch of a janitor's job took, by shard, job and part: turn is the wait for a turn on the shard's connections, which the batch shares with dispatch; acquire is the wait for its one connection from the pool; work is its statements, every one on that connection. A job falling behind with most of its time in turn or acquire is held back by the shard's load; with most in work, by its statements."),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60))
	if err != nil {
		errs = append(errs, errors.Trace(err))
	}
	m.batchSeconds = batchSeconds
	return m, errors.Join(errs...)
}

func shardAttr(shard int) attribute.KeyValue {
	return attribute.String("shard", strconv.Itoa(shard))
}

// flowsArchived counts the flows one batch moved into the archive.
func (m *Instruments) flowsArchived(ctx context.Context, shard, n int) {
	if m == nil || m.archived == nil || n <= 0 {
		return
	}
	m.archived.Add(ctx, int64(n), metric.WithAttributes(shardAttr(shard)))
}

// flowsReaped counts the flows one batch deleted without archiving, for reason.
func (m *Instruments) flowsReaped(ctx context.Context, shard int, reason string, n int) {
	if m == nil || m.reaped == nil || n <= 0 {
		return
	}
	m.reaped.Add(ctx, int64(n), metric.WithAttributes(shardAttr(shard), attribute.String("reason", reason)))
}

// archiveMoveFailure counts one failed archive attempt.
func (m *Instruments) archiveMoveFailure(ctx context.Context, shard int) {
	if m == nil || m.moveFailures == nil {
		return
	}
	m.moveFailures.Add(ctx, 1, metric.WithAttributes(shardAttr(shard)))
}

// batchAttrs is the attribute set of one job's batch timings for one part.
func batchAttrs(shard int, job, part string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("shard", strconv.Itoa(shard)), attribute.String("job", job),
		attribute.String("part", part))
}

// batch records one batch's time in its parts; a negative work, for a batch that could not take its connection,
// records none.
func (m *Instruments) batch(ctx context.Context, jb *job, turn, acquire, work time.Duration) {
	if m == nil || m.batchSeconds == nil {
		return
	}
	m.batchSeconds.Record(ctx, turn.Seconds(), jb.turnAttrs)
	m.batchSeconds.Record(ctx, acquire.Seconds(), jb.acquireAttrs)
	if work >= 0 {
		m.batchSeconds.Record(ctx, work.Seconds(), jb.workAttrs)
	}
}
