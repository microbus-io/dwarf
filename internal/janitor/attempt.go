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
	"github.com/microbus-io/dwarf/internal/database"
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

// AttemptedSQL is the condition, on a live flows row, that it has an archive attempt, with the no-attempt value
// inlined as the column's default spells it: idx_dwarf_flows_attempt is partial on exactly this, and a planner uses a
// partial index only for a query whose own predicate implies the index's - which a bound value cannot show.
func AttemptedSQL(driver string) string {
	if driver == "sqlite" {
		return "archive_attempt_at>'2000-01-01 00:00:00.000'" // text, compared as NOW_UTC() spells it
	}
	return "archive_attempt_at>'2000-01-01 00:00:00'"
}

// attemptsAmong returns the roots among roots that have an archive attempt: one primary-key read of the roots.
func attemptsAmong(ctx context.Context, q sequel.Executor, roots []int) (map[int]bool, error) {
	none, arg := NoAttemptSQL(q.DriverName())
	rows, err := q.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE flow_id IN ("+database.IntList(roots)+")"+
		" AND archive_attempt_at>"+none, arg)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	attempted := map[int]bool{}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, errors.Trace(err)
		}
		attempted[id] = true
	}
	return attempted, errors.Trace(rows.Err())
}

// errNotToday rolls back an attempt whose new time fell outside the filing day: past midnight, which the quiet margin
// keeps rare.
var errNotToday = errors.New("the attempt's time is not in today's archive day")

// claimAttempt starts or joins root's archive attempt, under the live root's lock, while root is still a closed root
// matching due (and its arguments): an attempt there is joined, and none there starts one at the database's now. stale,
// when not zero, is an attempt whose day was dropped: found still there, it is replaced by a new one. A new attempt
// starts only while d is startable and in d's day, so its stamp groups have the quiet margin's room. ok is false when no
// attempt holds: root not due, held, or no room to start one. The attempt is the tree's identity and its day
// (archiver.DayOf), so every copier of one attempt writes the same rows.
func claimAttempt(ctx context.Context, h handle, root int, due string, dueArgs []any, d *filingDay, stale time.Time) (at time.Time, ok bool, err error) {
	id := strconv.Itoa(root)
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		at, ok = time.Time{}, false
		locked, err := lockRoots(ctx, tx, "", true, "flow_id="+id+" AND "+due, dueArgs...)
		if err != nil || len(locked) == 0 {
			return errors.Trace(err)
		}
		if at, err = attemptOf(ctx, tx, root); err != nil {
			return errors.Trace(err)
		}
		if at.After(noAttempt) && !at.Equal(stale) {
			ok = true // join
			return nil
		}
		if !d.startable() {
			return nil // no room left in the day, or tomorrow not whole: a later round starts it
		}
		if _, err := tx.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at=NOW_UTC() WHERE flow_id="+id); err != nil {
			return errors.Trace(err)
		}
		if at, err = attemptOf(ctx, tx, root); err != nil {
			return errors.Trace(err)
		}
		if archiver.DayOf(at, d.archive.DayLength()) != d.day {
			return errNotToday
		}
		ok = true
		return nil
	})
	if errors.Is(err, errNotToday) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, errors.Trace(err)
	}
	return at, ok, nil
}

// attemptOf reads the live root's archive attempt, as the instant it holds.
func attemptOf(ctx context.Context, tx *sequel.Tx, root int) (time.Time, error) {
	var at time.Time
	err := tx.QueryRowContext(ctx, "SELECT archive_attempt_at FROM dwarf_flows WHERE flow_id=?", root).Scan(&at)
	return database.StoredTime(at), errors.Trace(err)
}
