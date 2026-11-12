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

// The counters the jobs share - created on one meter, every job that creates one gets the same instrument - and that
// internal/reaper creates too: keep each description word for word the same as the reaper's, since two instruments of
// one name and different descriptions are exported as two streams of that name.
const (
	archivedName        = "dwarf_flows_archived"
	archivedDescription = "Counts flows moved from the live tables into the archive, by shard: each root and every subgraph flow of its tree. Its rate plus dwarf_flows_reaped's, against dwarf_flows_closed, is archiving keeping up - a closed root waits the archive grace (a minute) in the live tables, so the two track each other a minute apart."
	moveFailuresName    = "dwarf_archive_move_failures"
	moveFailuresDesc    = "Failed archive attempts, including sizing, contention and deadlines; counts attempts, not distinct trees."
	reapedName          = "dwarf_flows_reaped"
	reapedDescription   = "Counts flows deleted from the live tables WITHOUT archiving them, by shard and by reason: deleted (a tree Delete or DeleteOnCompletion scheduled for deletion, or a tree a retention of 0 stamped for deletion as too large for one transaction), no_retention (a retention of 0 keeps no archive), and archive_failed (an unarchived tree reached its retention cutoff). archive_failed is records LOST from the archive and must stay zero; dwarf_archive_failing_seconds says since when it has been failing, and the logs say why."
)

// counter creates the counter name on meter, appending to errs if it cannot.
func counter(meter metric.Meter, name, desc string, errs *[]error) metric.Int64Counter {
	c, err := meter.Int64Counter(name, metric.WithDescription(desc))
	if err != nil {
		*errs = append(*errs, errors.Trace(err))
	}
	return c
}

// add adds n to c for attrs, unless c could not be created or n is not positive.
func add(ctx context.Context, c metric.Int64Counter, n int, attrs ...attribute.KeyValue) {
	if c == nil || n <= 0 {
		return
	}
	c.Add(ctx, int64(n), metric.WithAttributes(attrs...))
}

func shardAttr(shard int) attribute.KeyValue {
	return attribute.String("shard", strconv.Itoa(shard))
}

// newBatchSeconds creates dwarf_janitor_batch_seconds on meter.
func newBatchSeconds(meter metric.Meter) (metric.Float64Histogram, error) {
	h, err := meter.Float64Histogram("dwarf_janitor_batch_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time one batch of a janitor's job took, by shard, job and part: turn is the wait for a turn on the shard's connections, which the batch shares with dispatch; acquire is the wait for its one connection from the pool; work is its statements, every one on that connection. A job falling behind with most of its time in turn or acquire is held back by the shard's load; with most in work, by its statements."),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60))
	return h, errors.Trace(err)
}

// batchAttrs is the attribute set of one job's batch timings for one part.
func batchAttrs(shard int, job, part string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("shard", strconv.Itoa(shard)), attribute.String("job", job),
		attribute.String("part", part))
}

// recordBatch records one batch's time in its parts; a negative work, for a batch that could not take its connection,
// records none.
func (j *Janitor) recordBatch(ctx context.Context, job string, turn, acquire, work time.Duration) {
	if j.batchSeconds == nil {
		return
	}
	shard := j.shard.Index()
	j.batchSeconds.Record(ctx, turn.Seconds(), batchAttrs(shard, job, partTurn))
	j.batchSeconds.Record(ctx, acquire.Seconds(), batchAttrs(shard, job, partAcquire))
	if work >= 0 {
		j.batchSeconds.Record(ctx, work.Seconds(), batchAttrs(shard, job, partWork))
	}
}
