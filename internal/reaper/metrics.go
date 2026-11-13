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

package reaper

import (
	"context"
	"strconv"
	"time"

	"github.com/microbus-io/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instruments are the Reaper's metrics. A nil *instruments records nothing.
type instruments struct {
	moveFailures metric.Int64Counter
	archived     metric.Int64Counter
	staged       metric.Int64Counter
	reaped       metric.Int64Counter
	passSeconds  metric.Float64Histogram
}

// newInstruments creates the instruments on meter. It returns every instrument it could create, and the errors of
// those it could not.
func newInstruments(meter metric.Meter) (*instruments, error) {
	var errs []error
	ctr := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return c
	}
	m := &instruments{}
	// Janitor steps 2-3: internal/janitor creates dwarf_archive_move_failures, dwarf_flows_archived and
	// dwarf_flows_reaped on the same meter, and their descriptions are its, word for word.
	m.moveFailures = ctr("dwarf_archive_move_failures", "Failed archive attempts, including sizing, contention and deadlines; counts attempts, not distinct trees.")
	m.archived = ctr("dwarf_flows_archived", "Counts flows moved from the live tables into the archive, by shard: each root and every subgraph flow of its tree. Its rate plus dwarf_flows_reaped's, against dwarf_flows_closed, is archiving keeping up - a closed root waits the archive grace (a minute) in the live tables, so the two track each other a minute apart.")
	m.staged = ctr("dwarf_archive_staged_rows", "Counts rows a staged archive copied into the archive, by shard: the flows and steps of a tree too large to archive in one transaction, written chunk by chunk before it is published. A chunk run again counts only what was not there. Read beside dwarf_archive_oldest_attempt_seconds: an age that grows while this moves is a large tree copying; one that grows while this is flat is a copy that is stuck, and the logs say why.")
	m.reaped = ctr("dwarf_flows_reaped", "Counts flows deleted from the live tables WITHOUT archiving them, by shard and by reason: deleted (a tree Delete or DeleteOnCompletion scheduled for deletion, a tree a retention of 0 stamped for deletion as too large for one transaction, or a tree past its retention, counted under archive_failed too when it was stamped), no_retention (a retention of 0 keeps no archive), and archive_failed (the flows of an unarchived tree past its retention cutoff, counted when it is stamped for deletion). archive_failed is records LOST from the archive and must stay zero; dwarf_archive_failing_seconds says since when it has been failing, and the logs say why.")
	passSeconds, err := meter.Float64Histogram("dwarf_reap_pass_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time one reaper pass spent on a shard, by shard: from its turn at the shard to the end of its work - the trees scheduled for deletion removed, then the closed trees archived, each batch by batch until none is due. A pass runs every 15 seconds on each dispatcher of the shard. Passes that grow toward that interval mean the reaper is falling behind the rate flows close; read dwarf_archive_lag_seconds for how far."),
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60, 300))
	if err != nil {
		errs = append(errs, errors.Trace(err))
	}
	m.passSeconds = passSeconds
	return m, errors.Join(errs...)
}

func shardAttr(shard int) attribute.KeyValue {
	return attribute.String("shard", strconv.Itoa(shard))
}

// flowsArchived counts the flows one batch moved into the archive.
func (m *instruments) flowsArchived(ctx context.Context, shard, n int) {
	if m == nil || m.archived == nil || n <= 0 {
		return
	}
	m.archived.Add(ctx, int64(n), metric.WithAttributes(shardAttr(shard)))
}

// rowsStaged counts the rows one staging chunk copied.
func (m *instruments) rowsStaged(ctx context.Context, shard, n int) {
	if m == nil || m.staged == nil || n <= 0 {
		return
	}
	m.staged.Add(ctx, int64(n), metric.WithAttributes(shardAttr(shard)))
}

// flowsReaped counts the flows one batch deleted without archiving, for reason.
func (m *instruments) flowsReaped(ctx context.Context, shard int, reason string, n int) {
	if m == nil || m.reaped == nil || n <= 0 {
		return
	}
	m.reaped.Add(ctx, int64(n), metric.WithAttributes(shardAttr(shard), attribute.String("reason", reason)))
}

// archiveMoveFailure counts one failed archive attempt.
func (m *instruments) archiveMoveFailure(ctx context.Context, shard int) {
	if m == nil || m.moveFailures == nil {
		return
	}
	m.moveFailures.Add(ctx, 1, metric.WithAttributes(shardAttr(shard)))
}

// reapPass records one pass's time on a shard.
func (m *instruments) reapPass(ctx context.Context, shard int, d time.Duration) {
	if m == nil || m.passSeconds == nil {
		return
	}
	m.passSeconds.Record(ctx, d.Seconds(), metric.WithAttributes(shardAttr(shard)))
}
