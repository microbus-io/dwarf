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

// Deletion reports whether the live root was found, and how many roots this call newly marked on each side.
// A found root already due for deletion has both counts zero; one in DeleteOnCompletion's grace is made due at once,
// and counted as a live removal.
type Deletion struct {
	Found         bool
	Live, Archive int
}

// DeleteLive schedules a closed live root for deletion - at once, cutting DeleteOnCompletion's grace short - or marks
// its published archive copy and clears the live attempt. The caller must authenticate the key and check that it names a closed root; closed roots cannot reopen.
// A result with Found false asks the caller to locate the tree in the archive. dayLength is the archive's day length.
func (r *Reaper) DeleteLive(ctx context.Context, db *sequel.DB, dayLength time.Duration, root int) (result Deletion, err error) {
	driver := db.DriverName()
	none, noneArg := NoAttemptSQL(driver)
	id := strconv.Itoa(root)
	var at time.Time
	var dropped bool
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		result, dropped = Deletion{}, false
		// Wait for the root: skipping a held row would mistake another owner's lock for a move to the archive.
		locked, err := lockRoots(ctx, tx, live.flows, false, "flow_id="+id+" AND surgraph_flow_id=0 AND status IN ("+terminalStatusesSQL+")")
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		var stamp int
		if err := tx.QueryRowContext(ctx, "SELECT delete_after_ms, archive_attempt_at FROM dwarf_flows WHERE flow_id="+id).Scan(&stamp, &at); err != nil {
			return errors.Trace(err)
		}
		at = database.StoredTime(at)
		result.Found = true
		if stamp == 0 {
			_, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, archive_attempt_at="+none+" WHERE flow_id="+id, noneArg)
			result.Live = 1
			return errors.Trace(err)
		}
		if !at.After(noAttempt) {
			if stamp > 1 {
				// Stamped by DeleteOnCompletion, due when its grace ends: a Delete takes precedence, due at once.
				_, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1 WHERE flow_id="+id)
				result.Live = 1
				return errors.Trace(err)
			}
			return nil // being deleted already
		}
		// Publication and Delete share this order: live root, then archive root.
		res, err := tx.ExecContext(ctx, "UPDATE "+migrations.FlowsTable(archiver.DayOf(at, dayLength))+
			" SET delete_after_ms=1 WHERE flow_id="+id+" AND delete_after_ms=0")
		if sequel.IsUndefinedTableError(err) {
			dropped = true
			return errors.Trace(err)
		}
		if err != nil {
			return errors.Trace(err)
		}
		n, err := res.RowsAffected()
		if err != nil {
			return errors.Trace(err)
		}
		result.Archive = int(n)
		_, err = tx.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+none+" WHERE flow_id="+id, noneArg)
		return errors.Trace(err)
	})
	if dropped {
		// A missing table ends the transaction. A dropped day is never recreated, so its copy is gone even if
		// another Delete or live cleanup wins before this guarded follow-up clears the attempt.
		expr, arg := database.SQLTime(driver, at)
		_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+none+" WHERE flow_id="+id+
			" AND delete_after_ms>0 AND archive_attempt_at="+expr, noneArg, arg)
		result = Deletion{Found: true}
	}
	if err != nil {
		return Deletion{}, errors.Trace(err)
	}
	return result, nil
}

// publishedSQL identifies published roots for cleanup accounting. The archive time survives Delete clearing
// the attempt; a stamped root with its attempt still set also establishes publication.
func publishedSQL(driver string) (string, []any) {
	cond, args := AttemptedSQL(driver)
	sentinel, arg := database.SQLTime(driver, database.MaxSQLTime().Truncate(time.Second))
	return "((delete_after_ms>0 AND " + cond + ") OR archived_at<" + sentinel + ")", append(args, arg)
}
