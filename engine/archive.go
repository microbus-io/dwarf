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
	"net/http"
	"os"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// defaultRetentionDays is how many days the archive keeps a day when SetRetention is not called.
const defaultRetentionDays = 30

// SetRetention sets how many days the archive keeps each UTC day's records, counted from the end of that day: a day is
// dropped as a whole once it ended at least days days ago. The default is 30. 0 keeps no archive: an ended flow is
// deleted rather than archived. To keep the archive indefinitely, pass a large number of days.
//
// Use the same value on every replica sharing a database: any of them may drop an expired day. Construction-time
// only.
func (e *Engine) SetRetention(days int) error {
	if e.started.Load() {
		return errSetAfterStartup("retention")
	}
	if days < 0 {
		return errors.New("retention must not be negative", http.StatusBadRequest)
	}
	e.retentionDays = days
	return nil
}

// PurgeBefore drops the archive's records of every UTC day before the one containing before, on every shard, whatever
// the retention; a before within today keeps only today's. It returns a 400 when before falls after today on the
// database's clock, since today's records are still being written.
func (e *Engine) PurgeBefore(ctx context.Context, before time.Time) error {
	if err := e.ensureStarted(); err != nil {
		return errors.Trace(err)
	}
	set := e.archives.Load()
	day := archiver.DayOf(before, set.dayLength)
	return errors.Trace(e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		ctx, doneTurn := e.opTurn(ctx, shard, "purge_before")
		defer doneTurn()
		a := set.byShard[shard]
		if a == nil {
			return errors.New("shard %d has no archive in this run; the engine is restarting", shard, http.StatusServiceUnavailable)
		}
		_, err := a.DropBefore(ctx, day)
		return errors.Trace(err)
	}))
}

// archiveSet is one run's archives: an Archive per open shard and the day's length they count in. Published whole, so
// a call straddling a Shutdown/Startup restart reads one run's set or the other's, never a map being assigned.
type archiveSet struct {
	byShard   map[int]*archiver.Archive
	dayLength time.Duration
}

// archiveDayEnv names the environment variable that shortens the archive's day for the benchmark rig, a whole number
// of seconds as a Go duration. A rig cannot reach a test-only override, since the engine's test seams are inert in a
// bench binary, and a day of minutes is what shows day rollover and retention under load in one run. Production
// leaves it unset.
const archiveDayEnv = "DWARF_ARCHIVE_DAY"

// archiveDayLength is the length of this run's day: DefaultDayLength, a test's testArchiveDay, or archiveDayEnv.
func (e *Engine) archiveDayLength(ctx context.Context) (time.Duration, error) {
	dayLength := archiver.DefaultDayLength
	if e.testArchiveDay > 0 {
		dayLength = e.testArchiveDay
	}
	if v := os.Getenv(archiveDayEnv); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second || d%time.Second != 0 {
			return 0, errors.New("%s must be a whole number of seconds, at least 1s, not %q", archiveDayEnv, v, http.StatusBadRequest)
		}
		// Loud, because a short day under the default retention drops a day's records within hours.
		e.logger.WarnContext(ctx, "The archive's day is shortened by "+archiveDayEnv+"; it is for benchmarks only", "day", d)
		dayLength = d
	}
	return dayLength, nil
}

// startArchives builds an Archive per open shard and makes sure each shard has the tables of today and of the two days
// after it before the replica serves, whatever the retention, then brings the existing days within the retention up to
// the archive migrations - right after the shards' schema migrations, before anything reads a day.
func (e *Engine) startArchives(ctx context.Context, dayLength time.Duration) error {
	set := &archiveSet{byShard: make(map[int]*archiver.Archive, len(e.db.Indices())), dayLength: dayLength}
	for _, idx := range e.db.Indices() {
		db, err := e.db.Shard(idx)
		if err != nil {
			return errors.Trace(err)
		}
		set.byShard[idx] = archiver.New(db, dayLength, migrations.Archive())
	}
	e.archives.Store(set)
	e.archiveFailures.reset()
	return errors.Trace(e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		a := set.byShard[shard]
		err := a.EnsureToday(ctx)
		if err == nil {
			err = a.MigrateDays(ctx, e.retentionDays)
		}
		if err != nil {
			return errors.New("shard %d", shard, err)
		}
		return nil
	}))
}

// archivePassInterval is how often the archive loop passes: 24 times a day, at least a second apart.
func archivePassInterval(dayLength time.Duration) time.Duration {
	return min(max(dayLength/24, time.Second), time.Hour)
}

// archiveLoop keeps the archive of every shard this replica dispatches: one pass when it starts, then one per tick.
func (e *Engine) archiveLoop(ctx context.Context) {
	set := e.archives.Load()
	ticker := time.NewTicker(archivePassInterval(set.dayLength))
	defer ticker.Stop()
	// A pass at once, not an interval in: Startup makes days but drops none, so a fleet whose replicas live shorter
	// than an interval would never drop an expired day.
	e.maintainArchives(ctx, set)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.maintainArchives(ctx, set)
		}
	}
}

// maintainArchives is one pass of the archive loop. A reader replica keeps no archive: one replica per shard
// suffices, and every pass is idempotent.
func (e *Engine) maintainArchives(ctx context.Context, set *archiveSet) {
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		// A turn on the shard, so the pass queues for its connections like any other caller.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		if !e.dispatchesOn(shard) {
			// A reader maintains nothing, but its locates still need the days that exist: one catalog read a pass.
			err := set.byShard[shard].Refresh(ctx)
			if err != nil {
				e.logSweepError(ctx, "Reading the archive's days; the next pass retries", "shard", shard, "error", err)
			}
			return nil
		}
		err := set.byShard[shard].Maintain(ctx, e.retentionDays)
		if err != nil {
			e.logSweepError(ctx, "Maintaining the archive tables; the next pass retries", "shard", shard, "error", err)
		}
		return nil
	})
	e.seams.Checkpoint(ctx, engineseams.CheckpointArchivePassDone)
}
