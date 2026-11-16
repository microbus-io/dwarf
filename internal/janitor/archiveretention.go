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

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// JobArchiveRetention is the name the engine adds its archive retention job under.
const JobArchiveRetention = "archive_retention"

// maintainFailedDescription is archive retention's counter's description.
const maintainFailedDescription = "Counts archive maintenance passes that failed, by shard: the pass that makes the days ahead and drops the expired days, run hourly by a dispatcher of the shard. One failure is retried by the next pass; a run of them leaves the days ahead unmade, and one to two days of them stop archiving (dwarf_archive_failing_seconds). A pass cut short by Shutdown is not counted."

// ArchiveRetention is the job that keeps a shard's archive days: each round makes the days ahead - archiving into today
// waits for tomorrow's tables - and drops the days past the retention, with their directory rows, through the archiver
// (Maintain). A round every pass interval: hourly, or a twenty-fourth of a day shorter than a day. A round that made
// a new day's tables or dropped a day made progress, so the job's progress age reads how long since its days last
// moved: up to a day and a pass interval while it works.
type ArchiveRetention struct {
	archive       *archiver.Archive
	retentionDays int

	// j is the janitor the job is added to; the counter is created on its meter, and records under shardAttr.
	j         *Janitor
	shardAttr attribute.KeyValue
	failed    metric.Int64Counter
}

// NewArchiveRetention returns an archive retention job over the shard's archive, keeping retentionDays of days.
func NewArchiveRetention(archive *archiver.Archive, retentionDays int) *ArchiveRetention {
	return &ArchiveRetention{archive: archive, retentionDays: retentionDays}
}

// attach creates the job's counter on j's meter.
func (a *ArchiveRetention) attach(j *Janitor, _ string) {
	a.j = j
	a.shardAttr = attribute.String("shard", strconv.Itoa(j.shard.Index()))
	var err error
	a.failed, err = j.diags.Meter().Int64Counter("dwarf_archive_maintain_failed", metric.WithDescription(maintainFailedDescription))
	if err != nil {
		j.diags.Logger().Error("Creating the archive retention job's instruments", "shard", j.shard.Index(), "error", err)
	}
}

// detach has nothing to release.
func (a *ArchiveRetention) detach() {}

// idleWait is the pass interval: an hour, or a twenty-fourth of a day shorter than a day, at least a second.
func (a *ArchiveRetention) idleWait() time.Duration {
	if a.archive == nil {
		return time.Hour
	}
	return PassInterval(a.archive.DayLength())
}

// PassInterval is how often the archive's days are kept, for a day of dayLength: hourly for a day of a day or longer,
// a twenty-fourth of a shorter one - a test's day of seconds - and at least a second.
func PassInterval(dayLength time.Duration) time.Duration {
	return min(max(dayLength/24, time.Second), time.Hour)
}

// reset has nothing to forget: each round reads the archive's days afresh.
func (a *ArchiveRetention) reset() {}

// round runs one maintenance pass as one batch, the archiver's migrations and drops on the shard's DB, and reports
// progress when it saw a new day's tables whole or dropped a day; the round after, at once, finds neither, and the
// idle wait paces the next.
func (a *ArchiveRetention) round(ctx context.Context, batch batchFunc) (progress bool) {
	if a.archive == nil {
		return false
	}
	_ = batch(ctx, func(ctx context.Context, _ handle) error { // one that did not run made no progress
		newDay, dropped, err := a.archive.Maintain(ctx, a.retentionDays)
		// diagnostics.FaultArchiveMaintainErr fails the pass after its work, as a failed drop or make does.
		if err == nil && a.j.diags.Seams().IsFault(diagnostics.FaultArchiveMaintainErr) {
			err = errors.New("injected fault: " + diagnostics.FaultArchiveMaintainErr)
		}
		if err != nil && ctx.Err() == nil { // a pass cut short by a stop is no failure
			a.failed.Add(ctx, 1, metric.WithAttributes(a.shardAttr))
			a.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointJanitorFailed)
			a.j.diags.Logger().ErrorContext(ctx, "Janitor: maintaining the archive tables; the next pass retries",
				"shard", a.j.shard.Index(), "error", err)
		}
		a.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointArchiveMaintained)
		progress = newDay || dropped > 0
		return nil
	})
	return progress
}
