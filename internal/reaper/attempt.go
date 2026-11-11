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
	"database/sql"
	"strconv"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// noAttempt is archive_attempt_at's value for a live root with no archive attempt: the column's default.
var noAttempt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// NoAttemptSQL returns the SQL of archive_attempt_at's "no attempt" value on driver, holding one placeholder, and the
// argument to bind to it. Every stamp of a live root but a publication writes it in the same UPDATE, so a root stamped
// with an attempt is one published to the archive.
func NoAttemptSQL(driver string) (expr string, arg string) {
	return database.SQLTime(driver, noAttempt)
}

// AttemptedSQL returns the condition, on a live flows row, that the root has an archive attempt, and its arguments.
func AttemptedSQL(driver string) (cond string, args []any) {
	expr, arg := NoAttemptSQL(driver)
	return "archive_attempt_at>" + expr, []any{arg}
}

// DeletePublished deletes the tree of root, published to the archive while its live copy is still being deleted: in one
// transaction it locks the live root, then marks the archive root in the day of its attempt, which hides the archived
// tree, and clears the live root's attempt, so the live copy's deletion goes on as an ordinary stamped one. A crash
// therefore never leaves the archive copy standing after an accepted Delete. An archive root already gone - removed by
// Purge, or its day dropped - is the archive's deletion done already, and only the attempt is cleared. done is false
// when root is not stamped with an attempt any more: another Delete got there first. marked is true when this call
// marked the archive's copy, and false when the copy was gone or marked already. The archive's tree is deleted in the
// background, by the discovery of marked trees.
func (r *Reaper) DeletePublished(ctx context.Context, db *sequel.DB, dayLength time.Duration, root int) (done, marked bool, err error) {
	driver := db.DriverName()
	attempted, attemptedArgs := AttemptedSQL(driver)
	none, noneArg := NoAttemptSQL(driver)
	id := strconv.Itoa(root)
	var at time.Time
	var dropped bool
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		done, marked, dropped = false, false, false
		locked, err := lockRoots(ctx, tx, live.flows, false, "flow_id="+id+" AND delete_after_ms>0 AND "+attempted, attemptedArgs...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		at, err = attemptOf(ctx, tx, root)
		if err != nil {
			return errors.Trace(err)
		}
		// The archive root is locked by the UPDATE that marks it: the live root, then the archive root, the one lock
		// order across the two.
		res, err := tx.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(archiver.DayOf(at, dayLength))+
			" SET delete_after_ms=1 WHERE flow_id="+id+" AND delete_after_ms=0")
		if sequel.IsUndefinedTableError(err) {
			dropped = true
			return errors.Trace(err)
		}
		if err != nil {
			return errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		marked = n == 1
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+none+" WHERE flow_id="+id, noneArg); err != nil {
			return errors.Trace(err)
		}
		done = true
		return nil
	})
	if dropped {
		// The statement on the dropped day ended its transaction. The error is the proof the archive's copy is gone,
		// since a dropped day is never made again: a fresh transaction clears the attempt it read, if it still holds.
		expr, arg := database.SQLTime(driver, at)
		res, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+none+" WHERE flow_id="+id+
			" AND delete_after_ms>0 AND archive_attempt_at="+expr, noneArg, arg)
		if err != nil {
			return false, false, errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		return n == 1, false, nil
	}
	return done, marked, errors.Trace(err)
}

// attemptOf reads the live root's archive_attempt_at. A column declared as a time scans into a time.Time on every
// dialect, SQLite's included; database.StoredTime takes its instant, since on MySQL the driver puts it in the DSN's
// location.
func attemptOf(ctx context.Context, tx *sequel.Tx, root int) (time.Time, error) {
	var at time.Time
	err := tx.QueryRowContext(ctx, "SELECT archive_attempt_at FROM dwarf_flows WHERE flow_id=?", root).Scan(&at)
	if err == sql.ErrNoRows {
		return at, errors.New("flow %d is gone", root)
	}
	return database.StoredTime(at), errors.Trace(err)
}
