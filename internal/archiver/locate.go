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

package archiver

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// DirectoryTable is the live table naming the flows a locate would not find on its own - those archived on a day other
// than their creation day and the day after it - with the day each was archived on: (flow_id, day).
const DirectoryTable = "dwarf_flow_days"

// VisibleSQL is the condition that the flow aliased f is visible, given its root aliased r, joined on
// r.flow_id=NULLIF(f.root_flow_id, f.flow_id): a root unmarked itself, or a child whose root is in the same tables and
// unmarked. It is the one rule every reader of the flows tables applies, live and archived: a root is marked for
// deletion by delete_after_ms>0, and its whole tree is hidden with it; an archived child whose root is not in its day
// is a copy not published, and hidden too. The NULLIF keeps a root from probing itself.
func VisibleSQL(f, r string) string {
	return "((" + f + ".root_flow_id=" + f + ".flow_id AND " + f + ".delete_after_ms=0) OR " + r + ".delete_after_ms=0)"
}

// forgetBatch is how many of the directory's expired rows one statement deletes: one bound list.
const forgetBatch = database.MaxListIDs

// LocateFlow returns the day whose archive tables hold the flow with the given id and creation hour, if it is
// archived, and a 404 if no day does. It does not check the flow's token: the caller's read of the day's tables, with
// the flow's token and hour, says whether the flow is the one asked for. A whole tree is archived on one day, so the day
// holds the flow's steps and its subgraph flows too. It reads no catalog of its own.
//
// A day the directory names is returned without checking that it still has its tables: the caller's read of a day
// dropped since fails with an error sequel.IsUndefinedTableError recognizes, and the caller reads it as not found.
func (a *Archive) LocateFlow(ctx context.Context, flowID, hour int) (day int, err error) {
	created := a.creationDay(hour)
	// The database mints the hour, so a key created more than a day past this replica's clock - a day of skew between
	// the two being far beyond any real one - names a flow that does not exist, and is answered without a statement,
	// whose probes of days not made yet would each fail.
	if created > DayOf(time.Now(), a.dayLength)+1 {
		return 0, errors.New("flow not found", http.StatusNotFound)
	}
	probed := a.worthProbing(created, created+1)
	// A day known may have been dropped since by a peer, and one not seen yet may not exist: on a missing table the
	// oldest day probed is left out and the statement run again, down to the directory alone, which is a live table.
	// Days go oldest first, and a row goes into a day only once the day after it is whole, so the day left out holds
	// nothing of the flow. What the locate learns is not kept: the next catalog read says it.
	for {
		found, err := a.locateIn(ctx, flowID, probed)
		if err != nil {
			if len(probed) > 0 && sequel.IsUndefinedTableError(err) {
				probed = probed[1:]
				continue
			}
			return 0, errors.Trace(err)
		}
		if found == nil {
			return 0, errors.New("flow not found", http.StatusNotFound)
		}
		return *found, nil
	}
}

// worthProbing returns the days that may have tables, of those given in ascending order: the days the archive knows,
// and any later than every day it knows - made since its last catalog read. A day it lacks below its newest was
// dropped or never made, and nothing makes it later.
func (a *Archive) worthProbing(days ...int) []int {
	a.mu.Lock()
	defer a.mu.Unlock()
	var probed []int
	for _, d := range days {
		_, ok := slices.BinarySearch(a.known, d)
		if ok || len(a.known) == 0 || d > a.known[len(a.known)-1] {
			probed = append(probed, d)
		}
	}
	return probed
}

// forget takes a day the archive dropped out of the days it knows. A catalog read begun before it may list the day,
// so it counts as begun after every such read.
func (a *Archive) forget(day int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.begun++
	a.knownAt = a.begun
	if i, ok := slices.BinarySearch(a.known, day); ok {
		a.known = slices.Delete(slices.Clone(a.known), i, i+1)
	}
}

// locateIn runs the locate's statement: the directory's row for the flow, and a primary-key probe of the flows table of
// each probed day. It returns the day it found the flow in, if any.
//
// A probe finds a flow only where it is visible: its root in the same day and not marked for deletion. A root is its
// own root, and the NULLIF keeps it from probing itself; a child costs one primary-key probe of its root. A child whose
// root is not in the day - a copy not published yet, or abandoned - is not found there. The directory's branch cannot
// join a day chosen at runtime, so the day it names is checked by the reader's own read of that day.
func (a *Archive) locateIn(ctx context.Context, flowID int, probed []int) (found *int, err error) {
	var stmt strings.Builder
	args := []any{flowID}
	stmt.WriteString("SELECT day FROM " + DirectoryTable + " WHERE flow_id=?")
	for _, d := range probed {
		flows := migrations.FlowsTable(d)
		stmt.WriteString(" UNION ALL SELECT " + strconv.Itoa(d) + " FROM " + flows + " f LEFT JOIN " + flows +
			" r ON r.flow_id=NULLIF(f.root_flow_id, f.flow_id) WHERE f.flow_id=? AND " + VisibleSQL("f", "r"))
		args = append(args, flowID)
	}
	rows, err := a.db.QueryContext(ctx, stmt.String(), args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	// At most one branch matches: the directory names only flows archived outside the days probed. Were two to match
	// anyway, a probed day is preferred, since its table answered.
	for rows.Next() {
		var d int
		if err := rows.Scan(&d); err != nil {
			return nil, errors.Trace(err)
		}
		if found == nil || slices.Contains(probed, d) {
			found = &d
		}
	}
	return found, errors.Trace(rows.Err())
}

// DirectoryHours returns the creation hours whose flows, archived on day, the directory must name: those created in an
// hour below before (a creation day earlier than day-1) or at or above from (a creation day after day). Every other
// flow archived on day was created on day or the day before, and a locate finds it by its creation hour alone.
func (a *Archive) DirectoryHours(day int) (before, from int) {
	return a.dayStartHour(day - 1), a.dayStartHour(day + 1)
}

// dayStartHour returns the first creation hour whose creation day is day or later.
func (a *Archive) dayStartHour(day int) int {
	// creationDay(h) >= day exactly when h*3600 >= day*length, so the bound is day*length/3600 rounded up.
	secs := int64(day) * dayLengthSeconds(a.dayLength)
	return int(-floorDiv(-secs, 3600))
}

// creationDay returns the day a row created in hour was created on. The hour counts from the days' origin and rounds
// down, on the same clock as the day, so the two agree for any day length.
func (a *Archive) creationDay(hour int) int {
	return floorDiv(int64(hour)*3600, dayLengthSeconds(a.dayLength))
}

// forgetDaysBefore deletes the directory's rows for every day before day. It runs on every pass rather than per day
// dropped, so a row a failed pass left behind goes with the next; a day that could not be dropped yet loses its rows a
// little early, which costs only its flows archived far from their creation day, expired anyway. The rows go
// forgetBatch at a time - a select of their ids by the day index, then a delete by those ids - so a day that named many
// flows is not one long delete.
func (a *Archive) forgetDaysBefore(ctx context.Context, day int) error {
	if day <= 0 {
		return nil // no day before it has tables; and a retention near the largest int would not bind as an INT
	}
	for ctx.Err() == nil {
		rows, err := a.db.QueryContext(ctx,
			"SELECT flow_id FROM "+DirectoryTable+" WHERE day<? ORDER BY day LIMIT_OFFSET(?, 0)", day, forgetBatch)
		if err != nil {
			return errors.Trace(err)
		}
		var ids []int
		for rows.Next() {
			var id int
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return errors.Trace(err)
			}
			ids = append(ids, id)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
		if len(ids) == 0 {
			return nil
		}
		if a.forgetSelected != nil {
			a.forgetSelected()
		}
		// The day bound is repeated: a flow's row can move to a later day between the select and the delete (an
		// archive attempt replaced after its day dropped), and deleting by flow_id alone would take the new row.
		ph, args := database.IDList(ids)
		_, err = a.db.ExecContext(ctx, "DELETE FROM "+DirectoryTable+
			" SEEK_FROM("+DirectoryTable+", 1(flow_id)) WHERE flow_id IN ("+ph+") AND day<?", append(args, day)...)
		if err != nil {
			return errors.Trace(err)
		}
		if len(ids) < forgetBatch {
			return nil
		}
	}
	return errors.Trace(ctx.Err())
}
