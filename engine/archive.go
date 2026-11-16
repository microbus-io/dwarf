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
	"cmp"
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// defaultRetentionDays is how many days the archive keeps a day when SetRetention is not called.
const defaultRetentionDays = 30

// SetRetention sets how many days the archive keeps each UTC day's records, counted from the end of that day: a day is
// dropped as a whole once it ended at least days days ago. The default is 30. 0 keeps no archive: a closed flow is
// deleted rather than archived. To keep the archive indefinitely, pass a large number of days.
//
// An archive failure leaves a closed flow live until it can be archived or its retention expires. For an unarchived
// flow, retention counts from the day it became eligible for archival, normally a minute after completion. A late
// successful archival instead receives retention from its actual archive day. Deletion can be delayed by database
// failures, and a large tree is deleted over several passes.
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
// database's clock, since today's records are still being written. Closed flows still awaiting archival are unaffected.
func (e *Engine) PurgeBefore(ctx context.Context, before time.Time) (err error) {
	if err := e.ensureStarted(); err != nil {
		return errors.Trace(err)
	}
	defer e.metricIngressOperation(ctx, "purge_before", &err)
	day := archiver.DayOf(before, e.dayLength())
	return errors.Trace(e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		ctx, doneTurn := e.opTurn(ctx, shard, "purge_before")
		defer doneTurn()
		a := e.archiveOn(shard)
		if a == nil {
			return errors.New("shard %d has no archive in this run; the engine is restarting", shard, http.StatusServiceUnavailable)
		}
		_, err := a.DropBefore(ctx, day)
		return errors.Trace(err)
	}))
}

// archiveOn is shard's archive in the current run, or nil before the first Startup and for a shard the run did not
// open.
func (e *Engine) archiveOn(shard int) *archiver.Archive {
	if a := e.actorsOn(shard); a != nil {
		return a.archive
	}
	return nil
}

// flowTables names the pair of tables a read runs against: the live tables, or one archive day's. The two have the
// same columns, so a read is written once and run against either.
type flowTables struct {
	flows, steps string
}

// liveTables are the live tables, which a read tries first.
var liveTables = flowTables{flows: "dwarf_flows", steps: "dwarf_steps"}

// queryLiveOrArchive runs read on the live tables, and when the flow with flowID is not there, on the archive day it
// is in. read reports whether it found the flow; it must have no side effect and tolerate rows vanishing, since it may
// run on both sides.
//
// A read of more than one statement is closed (closing) by a primary-key read of the flow's row. A tree moves in one
// transaction, and each plain statement sees only what had committed when it began, so the row still there means
// every statement of the pass saw the tree on that side; the row gone means the pass's result is discarded, an error
// included, since the error may be the move itself (an anchor read after its tree left). A read of one statement
// (closeNone) sees the tree on one side by itself. Neither pass runs in a transaction, which would break that argument
// on MySQL, whose transactions read one snapshot, and lock tables on SQLite's shared cache.
//
// A live tree is deleted in chunks once its root's stamp is due, so a read that must not return part of a tree
// (closeTree) closes on the root too, and a tree stamped and due is discarded as if it had left. A read that only
// tracks change (closeFlow) closes on its own row, and serves a tree in deletion until its row goes, as Snapshot does
// - but a tree published to the archive is read there, since its live copy is being deleted in chunks.
//
// found is false when neither side holds the flow: never archived, archived on a day dropped since, or removed from
// the archive mid-read. The caller names the 404.
func (e *Engine) queryLiveOrArchive(ctx context.Context, db *sequel.DB, shard, flowID, hour int, closing readClosing,
	read func(t flowTables) (found bool, err error)) (found bool, err error) {
	found, err = read(liveTables)
	if found || err != nil {
		if closing == closeNone {
			return found, errors.Trace(err)
		}
		var there bool
		var cerr error
		if closing == closeTree {
			there, cerr = liveFlowStanding(ctx, db, flowID)
		} else {
			there, cerr = liveFlowUnpublished(ctx, db, flowID)
		}
		if cerr != nil {
			return false, errors.Trace(cmp.Or(err, cerr))
		}
		if there {
			return found, errors.Trace(err)
		}
	}
	a := e.archiveOn(shard)
	if a == nil {
		return false, nil
	}
	day, err := e.locateRetained(ctx, a, flowID, hour)
	if errors.StatusCode(err) == http.StatusNotFound {
		return false, nil
	}
	if err != nil {
		return false, errors.Trace(err)
	}
	// A day dropped since the locate - or a directory row outliving its day - fails a statement on its missing table:
	// the flow is not there.
	t := flowTables{flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day)}
	found, err = read(t)
	if sequel.IsUndefinedTableError(err) {
		return false, nil
	}
	// Every read of the archive closes on the visibility rule, one of one statement included: an archived flow is there
	// only if its root is in the same day and unmarked. A copy not published, or abandoned, has children in a day
	// without their root; a tree marked for deletion is hidden from the mark on.
	if found || err != nil {
		there, cerr := archiveFlowVisible(ctx, db, t, flowID)
		if sequel.IsUndefinedTableError(cerr) || (cerr == nil && !there) {
			return false, nil
		}
		if cerr != nil {
			return false, errors.Trace(cmp.Or(err, cerr))
		}
	}
	return found, errors.Trace(err)
}

// locateRetained returns the archive day the flow with flowID is in, as LocateFlow does, or a 404 for a day past the
// retention. Such a day is waiting to be dropped, and nothing reads it: Startup brings only the retained days up to the
// archive's migrations (MigrateDays), so an expired day's tables can be a migration behind, and a read of them would
// fail on the schema rather than miss. The lists cut their window the same way. The day is today on this process's
// clock, as the lists' cut is; a skew of seconds at midnight affects only the oldest day, which is about to drop.
func (e *Engine) locateRetained(ctx context.Context, a *archiver.Archive, flowID, hour int) (int, error) {
	day, err := a.LocateFlow(ctx, flowID, hour)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if day < archiver.DayOf(time.Now(), e.dayLength())-max(e.retentionDays, 0) {
		return 0, errors.New("flow not found", http.StatusNotFound)
	}
	return day, nil
}

// readClosing is how queryLiveOrArchive closes a read.
type readClosing int

const (
	closeNone readClosing = iota // a read of one statement: no closing read
	closeFlow                    // the flow's row is still there
	closeTree                    // on the live side, the flow's row and its root are there, the root not stamped and due
)

// liveFlowStanding reports whether the live flow with flowID is there and its root neither stamped for deletion and due
// nor gone: the closing read of a pass on the live side. A DeleteOnCompletion root inside its grace is standing.
func liveFlowStanding(ctx context.Context, db *sequel.DB, flowID int) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM dwarf_flows f JOIN dwarf_flows r ON r.flow_id=f.root_flow_id"+
		" WHERE f.flow_id=? AND NOT (r.delete_after_ms>0 AND DATE_ADD_MILLIS(r.updated_at, r.delete_after_ms)<=NOW_UTC())",
		flowID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, errors.Trace(err)
}

// liveFlowUnpublished reports whether the live flows table holds the flow with flowID, its root not published to the
// archive: the closing read of a live read that tracks change (closeFlow). A published tree's live copy is deleted in
// chunks, so a read of its steps would change as they go and change back once the archive's copy answers; the
// archive's copy, whole from publication on, answers instead.
func liveFlowUnpublished(ctx context.Context, db *sequel.DB, flowID int) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM dwarf_flows f JOIN dwarf_flows r ON r.flow_id=f.root_flow_id"+
		" WHERE f.flow_id=? AND NOT (r.delete_after_ms>0 AND r."+janitor.AttemptedSQL(db.DriverName())+")", flowID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, errors.Trace(err)
}

// publicationColumn reads a live root's publication disposition with its status and stamp. Archive tables have no
// attempt column; their visibility is checked separately.
func publicationColumn(driver string, t flowTables) (string, []any) {
	if t != liveTables {
		return "0", nil
	}
	return "CASE WHEN delete_after_ms>0 AND " + janitor.AttemptedSQL(driver) + " THEN 1 ELSE 0 END", nil
}

// archiveFlowVisible reports whether t's flows table holds the flow with flowID, visible: its root in the same table and
// not marked for deletion (archiver.VisibleSQL). It is the closing read of every read of the archive.
func archiveFlowVisible(ctx context.Context, db *sequel.DB, t flowTables, flowID int) (bool, error) {
	var one int
	err := db.QueryRowContext(ctx, "SELECT 1 FROM "+t.flows+" f LEFT JOIN "+t.flows+
		" r ON r.flow_id=NULLIF(f.root_flow_id, f.flow_id) WHERE f.flow_id=? AND "+archiver.VisibleSQL("f", "r"), flowID).Scan(&one)
	if err == sql.ErrNoRows {
		return false, nil
	}
	return err == nil, errors.Trace(err)
}

// setArchiveDay replaces the length of the archive's day, a whole number of seconds of at least 1s: for a benchmark,
// where a day of minutes shows day rollover and retention under load in one run, and reached through
// accessors.SetArchiveDay, since the engine's test seams are inert in a bench binary. Construction-time only.
func (e *Engine) setArchiveDay(d time.Duration) error {
	if e.started.Load() {
		return errSetAfterStartup("the archive day")
	}
	if d < time.Second || d%time.Second != 0 {
		return errors.New("the archive day must be a whole number of seconds, at least 1s, not %s", d, http.StatusBadRequest)
	}
	e.archiveDayOverride = d
	return nil
}

// setArchiveGrace sets how long a closed root waits in the live tables before it is archived, at least 0: for a
// benchmark that measures the archive's cost, which needs every closed flow archived within its window, or one that
// measures the live tables, which needs them held longer. Construction-time only; production keeps the minute.
func (e *Engine) setArchiveGrace(d time.Duration) error {
	if e.started.Load() {
		return errSetAfterStartup("the archive grace")
	}
	if d < 0 {
		return errors.New("the archive grace must be at least 0, not %s", d, http.StatusBadRequest)
	}
	e.archiveGrace = d
	return nil
}

// setStageRows and setStageBytes set the sizes above which a closed tree is archived in stages rather than in one
// transaction: in rows (0, never staged) and in payload bytes beside it (0, no byte threshold), each at least 0. For a
// benchmark, and for a test run that stages every tree. Construction-time only; production keeps the defaults.
func (e *Engine) setStageRows(n int) error {
	return e.setStageThreshold("the staging rows", &e.reapStageRows, n)
}

func (e *Engine) setStageBytes(n int) error {
	return e.setStageThreshold("the staging bytes", &e.reapStageBytes, n)
}

func (e *Engine) setStageThreshold(what string, field *int, n int) error {
	if e.started.Load() {
		return errSetAfterStartup(what)
	}
	if n < 0 {
		return errors.New("%s must be at least 0, not %d", what, n, http.StatusBadRequest)
	}
	*field = n
	return nil
}

// archiveDayLength is the length of this run's day: DefaultDayLength, or the one setArchiveDay or a test set.
func (e *Engine) archiveDayLength() time.Duration {
	if e.archiveDayOverride > 0 {
		return e.archiveDayOverride
	}
	return archiver.DefaultDayLength
}

// startArchives makes sure each shard has the tables of today and of the days ahead of it before the replica serves,
// whatever the retention, then brings the existing days within the retention up to the archive migrations - right
// after the shards' schema migrations, before anything reads a day.
func (e *Engine) startArchives(ctx context.Context) error {
	return errors.Trace(e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		a := e.archiveOn(shard)
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

// archiveLoop keeps the catalog of the archive's days fresh on the shards this replica does not dispatch, for their
// reads' locates: one pass when it starts, then one per archive pass interval. On a shard it dispatches, the janitor's
// archive retention keeps the days, and its passes read the catalog.
func (e *Engine) archiveLoop(ctx context.Context) {
	ticker := time.NewTicker(janitor.PassInterval(e.dayLength()))
	defer ticker.Stop()
	e.refreshArchives(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.refreshArchives(ctx)
		}
	}
}

// refreshArchives reads the archive's days on every shard this replica does not dispatch: a reader keeps no archive,
// but its locates need the days that exist - one catalog read a pass.
func (e *Engine) refreshArchives(ctx context.Context) {
	e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		if e.dispatchesOn(shard) {
			return nil // the janitor's archive retention
		}
		// A turn on the shard, so the pass queues for its connections like any other caller.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		if err := e.archiveOn(shard).Refresh(ctx); err != nil {
			e.logSweepError(ctx, "Reading the archive's days; the next pass retries", "shard", shard, "error", err)
		}
		return nil
	})
	e.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointArchivePassDone)
}
