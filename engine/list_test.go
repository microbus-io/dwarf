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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// idsOf returns the flow ids of summaries, in order.
func idsOf(t *testing.T, summaries []workflow.FlowSummary) []int {
	t.Helper()
	ids := make([]int, len(summaries))
	for i, s := range summaries {
		_, _, id, _, err := keys.ParseFlowKey(s.FlowKey)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	return ids
}

// stampArchived sets the archive time of the tree rooted at rootID, archived in day.
func stampArchived(t *testing.T, e *Engine, day, rootID int, at time.Time) {
	t.Helper()
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	expr, arg := sqlTime(db.DriverName(), at)
	_, err = db.ExecContext(t.Context(), "UPDATE "+migrations.FlowsTable(day)+" SET archived_at="+expr+" WHERE root_flow_id=?", arg, rootID)
	if err != nil {
		t.Fatal(err)
	}
}

// TestList_EachListReadsItsOwnTables pins the split between the two lists over the live tables and three archive days:
// ListArchived reads the archive alone, newest archived first, windowed on the archive time from inclusive to
// exclusive, with ArchivedAt on every flow, a Limit that keeps the order's prefix, the subgraph children with
// IncludeSubgraphs, and Purge's ascending order the same list reversed; ListLive reads the live tables alone, the
// ended flows not archived yet included, with a zero ArchivedAt.
func TestList_EachListReadsItsOwnTables(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	a := e.archives.Load().byShard[1]
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-2))
	assert.NoError(makeArchiveDay(t, e, today-1))
	assert.NoError(a.Refresh(ctx))
	roots := endedRoots(t, e, 6)
	moveInto(t, e, roots[0], today-2)
	moveInto(t, e, roots[1], today-1)
	moveInto(t, e, roots[5], today-1) // a second in that day, archived before roots[1]
	moveInto(t, e, roots[2], today)
	// roots[3] and roots[4] stay live: ended, not archived yet.
	oldest := a.DayEnds(today - 2).Add(-time.Hour)
	stampArchived(t, e, today-2, roots[0], oldest)
	stampArchived(t, e, today-1, roots[1], a.DayEnds(today-1).Add(-time.Hour))
	stampArchived(t, e, today-1, roots[5], a.DayEnds(today-1).Add(-2*time.Hour))
	if now, _ := a.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}

	window := workflow.Query{FromTime: time.Now().Add(-(3 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 100}
	all, err := e.ListArchived(ctx, window)
	assert.NoError(err)
	assert.Equal([]int{roots[2], roots[1], roots[5], roots[0]}, idsOf(t, all), "the archive alone, newest archived first")
	if assert.Len(all, 4) {
		for _, f := range all {
			assert.False(f.ArchivedAt.IsZero(), "archived")
		}
		assert.True(all[3].ArchivedAt.Equal(oldest), "archived at %v, not %v", all[3].ArchivedAt, oldest)
		assert.False(all[0].Subgraph)
	}
	live, err := e.ListLive(ctx, workflow.Query{Limit: 100})
	assert.NoError(err)
	assert.Equal([]int{roots[4], roots[3]}, idsOf(t, live), "the live tables alone: the ended flows not archived yet")
	for _, f := range live {
		assert.True(f.ArchivedAt.IsZero(), "not archived")
		assert.Equal(workflow.StatusCompleted, f.Status)
	}
	// The older flow updated last comes first, and is the one a Limit of 1 keeps: the order is by update, not by id.
	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), 1000) WHERE flow_id=?", roots[3])
	assert.NoError(err)
	live, err = e.ListLive(ctx, workflow.Query{Limit: 1})
	assert.NoError(err)
	assert.Equal([]int{roots[3]}, idsOf(t, live), "the most recently updated first")

	for limit := 1; limit <= 4; limit++ {
		q := window
		q.Limit = limit
		got, err := e.ListArchived(ctx, q)
		assert.NoError(err)
		assert.Equal(idsOf(t, all)[:limit], idsOf(t, got), "limit %d", limit)
	}

	q := window
	q.IncludeSubgraphs = true
	withChildren, err := e.ListArchived(ctx, q)
	assert.NoError(err)
	assert.Len(withChildren, 8, "every archived root and its subgraph child")

	q = window
	q.ToTime = time.Now().Add(-time.Minute)
	past, err := e.ListArchived(ctx, q)
	assert.NoError(err)
	assert.Equal([]int{roots[1], roots[5], roots[0]}, idsOf(t, past), "a window ending a minute ago: not the one archived since")
	// A limit below a day's rows keeps that day's newest: each day's own read is ordered and limited too.
	q.Limit = 1
	past, err = e.ListArchived(ctx, q)
	assert.NoError(err)
	assert.Equal([]int{roots[1]}, idsOf(t, past))

	q = window
	q.FromTime, q.ToTime = time.Now().Add(time.Hour), time.Now().Add(2*time.Hour)
	future, err := e.ListArchived(ctx, q)
	assert.NoError(err)
	assert.Len(future, 0, "a window wholly after now")

	q = window
	q.FromTime, q.ToTime = oldest, oldest.Add(time.Millisecond)
	at, err := e.ListArchived(ctx, q)
	assert.NoError(err)
	assert.Equal([]int{roots[0]}, idsOf(t, at), "FromTime is inclusive")
	q.ToTime = oldest
	q.FromTime = oldest.Add(-time.Hour)
	before, err := e.ListArchived(ctx, q)
	assert.NoError(err)
	assert.Len(before, 0, "ToTime is exclusive")

	rows, err := e.listArchived(ctx, window, 100, true)
	assert.NoError(err)
	ascending := make([]int, len(rows))
	for i, lr := range rows {
		ascending[i] = lr.flowID
	}
	assert.Equal([]int{roots[0], roots[5], roots[1], roots[2]}, ascending, "oldest archived first")
	rows, err = e.listArchived(ctx, window, 1, true)
	assert.NoError(err)
	if assert.Len(rows, 1) {
		assert.Equal(roots[0], rows[0].flowID, "ascending, each day's read keeps its oldest")
	}
}

// TestList_MergeOrdersAcrossShards pins the cross-shard merge of both lists: by time, then shard ascending, then flow
// id - descending newest first, ascending oldest first.
func TestList_MergeOrdersAcrossShards(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	rows := []listRow{
		{shard: 2, flowID: 7, at: at},
		{shard: 1, flowID: 3, at: at},
		{shard: 1, flowID: 9, at: at},
		{shard: 1, flowID: 4, at: at.Add(time.Second)},
		{shard: 2, flowID: 2, at: at.Add(-time.Second)},
	}
	key := func(rows []listRow) [][2]int {
		out := make([][2]int, len(rows))
		for i, lr := range rows {
			out[i] = [2]int{lr.shard, lr.flowID}
		}
		return out
	}
	assert.Equal([][2]int{{1, 4}, {1, 9}, {1, 3}, {2, 7}, {2, 2}}, key(mergeByTime(append([]listRow(nil), rows...), false)))
	assert.Equal([][2]int{{2, 2}, {1, 3}, {1, 9}, {2, 7}, {1, 4}}, key(mergeByTime(append([]listRow(nil), rows...), true)))
}

// TestListArchived_ADayAPeerDroppedIsReadAgain pins the list over a stale record of the days: a day a peer dropped since
// this replica's last catalog read fails the shard's statement on its missing table, and the list reads the catalog
// and runs it again, without the day.
func TestListArchived_ADayAPeerDroppedIsReadAgain(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	a := e.archives.Load().byShard[1]
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-2))
	assert.NoError(makeArchiveDay(t, e, today-1))
	assert.NoError(a.Refresh(ctx))
	root := endedRoots(t, e, 1)[0]
	moveInto(t, e, root, today-1)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	peer := archiver.New(db, e.archives.Load().dayLength, migrations.Archive())
	_, err = peer.DropBefore(ctx, today-1)
	assert.NoError(err)
	assert.True(a.Whole(today-2), "this replica's record is stale")

	got, err := e.ListArchived(ctx, workflow.Query{FromTime: time.Now().Add(-(3 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 100})
	assert.NoError(err)
	assert.Equal([]int{root}, idsOf(t, got))
	assert.False(a.Whole(today-2), "the record was read again")
}

// TestListArchived_TheWindowIsClampedToTheArchive pins the window's cut: a lower bound before the retention reads from
// the retention's first day, a window wholly before it lists nothing, and what is left after the cut may reach at most
// 31 days - today and the 30 before it - or the call is a 400.
func TestListArchived_TheWindowIsClampedToTheArchive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	root := endedRoots(t, e, 1)[0]
	moveInto(t, e, root, today)
	got, err := e.ListArchived(ctx, workflow.Query{FromTime: time.Now().Add(-(400 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 10})
	assert.NoError(err, "a window past the retention is cut to it")
	assert.Equal([]int{root}, idsOf(t, got))
	got, err = e.ListArchived(ctx, workflow.Query{FromTime: time.Now().Add(-(400 * 24 * time.Hour)), ToTime: time.Now().Add(-(40 * 24 * time.Hour)), Limit: 10})
	assert.NoError(err)
	assert.Len(got, 0, "a window wholly before the retention")

	// A retention past the cap, so the cut leaves the window as long as asked. Its own engine, under its own name.
	t.Run("retention_100", func(t *testing.T) {
		assert := testarossa.For(t)
		long := archiveEngine(t, treeMux(), 100)
		_, err := long.ListArchived(ctx, workflow.Query{FromTime: time.Now().Add(-(30 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 10})
		assert.NoError(err, "31 days, today included")
		_, err = long.ListArchived(ctx, workflow.Query{FromTime: time.Now().Add(-(31 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 10})
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "32 days")
		_, err = long.ListArchived(ctx, workflow.Query{FromTime: time.Now().Add(-(60 * 24 * time.Hour)), ToTime: time.Now().Add(-(30 * 24 * time.Hour)), Limit: 10})
		assert.NoError(err, "31 days, all in the past")
		_, err = long.ListArchived(ctx, workflow.Query{FromTime: time.Now().Add(-(20 * 24 * time.Hour)), ToTime: time.Now().Add(60 * 24 * time.Hour), Limit: 10})
		assert.NoError(err, "a window reaching 60 days ahead is cut to today")
	})
}

// TestList_RequiredInputsAndStatuses pins what the two lists refuse with a 400: no Limit, a Limit over the most a call
// returns, an empty window, a negative duration, and an unknown status or shard; ListArchived a missing bound on either
// side, which ListLive takes as no bound there, and a status that has not ended, which ListLive takes like any other.
func TestList_RequiredInputsAndStatuses(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	now := time.Now()
	ok := workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: now.Add(time.Hour), Limit: 10}
	bad := map[string]workflow.Query{
		"no limit":        {FromTime: time.Now().Add(-time.Hour), ToTime: now.Add(time.Hour)},
		"limit over":      {FromTime: time.Now().Add(-time.Hour), ToTime: now.Add(time.Hour), Limit: maxListLimit + 1},
		"empty window":    {FromTime: now, ToTime: now, Limit: 10},
		"inverted window": {FromTime: time.Now().Add(-time.Hour), ToTime: time.Now().Add(-(2 * time.Hour)), Limit: 10},
		"unknown status":  {FromTime: time.Now().Add(-time.Hour), ToTime: now.Add(time.Hour), Limit: 10, Status: "bogus"},
		"invalid shard":   {FromTime: time.Now().Add(-time.Hour), ToTime: now.Add(time.Hour), Limit: 10, Shard: 99},
	}
	for name, q := range bad {
		_, err := e.ListLive(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListLive: %s", name)
		_, err = e.ListArchived(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListArchived: %s", name)
	}
	for name, q := range map[string]workflow.Query{
		"no lower bound": {ToTime: now.Add(time.Hour), Limit: 10},
		"no upper bound": {FromTime: time.Now().Add(-time.Hour), Limit: 10},
		"no bound":       {Limit: 10},
	} {
		_, err := e.ListArchived(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListArchived: %s", name)
		_, err = e.ListLive(ctx, q)
		assert.NoError(err, "ListLive: %s", name)
	}
	max := ok
	max.Limit = maxListLimit
	_, err := e.ListLive(ctx, max)
	assert.NoError(err)
	_, err = e.ListArchived(ctx, max)
	assert.NoError(err)

	for _, status := range []string{workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled} {
		q := ok
		q.Status = status
		_, err := e.ListLive(ctx, q)
		assert.NoError(err, "ListLive of %s: an ended flow is live until it is archived", status)
		_, err = e.ListArchived(ctx, q)
		assert.NoError(err, "ListArchived of %s", status)
	}
	for _, status := range []string{workflow.StatusRunning, workflow.StatusInterrupted} {
		q := ok
		q.Status = status
		_, err := e.ListArchived(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListArchived of %s", status)
		_, err = e.ListLive(ctx, q)
		assert.NoError(err, "ListLive of %s", status)
	}
}

// TestList_TheLimitIsAboveAReaperBatch pins that the lists' maximum Limit stays above the reaper's batch: a batch's
// trees share one archive time, and a caller paging ListArchived by window cannot page across a tie larger than its Limit.
func TestList_TheLimitIsAboveAReaperBatch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngine()
	assert.True(maxListLimit > e.reapBatchRows, "the most a list returns (%d) must exceed a reaper batch (%d rows)", maxListLimit, e.reapBatchRows)
	assert.True(maxListLimit > e.reapBatch, "and its roots (%d)", e.reapBatch)
}

// TestSQLTime_ReadsAsTheInstantItNames pins the bound a list's FromTime and ToTime are written as: compared to the
// database's NOW_UTC(), the current instant reads within a second of it on every dialect, and on PostgreSQL in a
// session whose time zone is not UTC.
func TestSQLTime_ReadsAsTheInstantItNames(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	e := archiveEngine(t, treeMux(), 30)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	diff := func(x sequel.Executor) float64 {
		expr, arg := sqlTime(db.DriverName(), time.Now())
		var ms float64
		assert.NoError(x.QueryRowContext(ctx, "SELECT DATE_DIFF_MILLIS(NOW_UTC(), "+expr+")", arg).Scan(&ms))
		return ms
	}
	ms := diff(db)
	assert.True(ms > -1000 && ms < 1000, "%vms off", ms)
	if db.DriverName() != "pgx" {
		return
	}
	errRollback := errors.New("rollback")
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if _, err := tx.ExecContext(ctx, "SET LOCAL TIME ZONE 'Asia/Tokyo'"); err != nil {
			return err
		}
		ms = diff(tx)
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		assert.NoError(err)
	}
	assert.True(ms > -1000 && ms < 1000, "%vms off on a session in Tokyo", ms)
}

// TestListArchived_TheDayAfterTodayIsRead pins the one day past today a window reaching past now reads, for a database
// clock ahead of this process's across midnight: a tree there is listed.
func TestListArchived_TheDayAfterTodayIsRead(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	root := endedRoots(t, e, 1)[0]
	moveInto(t, e, root, today+1) // as a pass on a database clock already past midnight would
	got, err := e.ListArchived(ctx, workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: time.Now().Add(48 * time.Hour), Limit: 10})
	assert.NoError(err)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal([]int{root}, idsOf(t, got))
}

// TestSQLTime_RoundsUpToTheColumnsPrecision pins that a bound between two of the column's units - microseconds on
// PostgreSQL, milliseconds elsewhere - compares as the instant it names: half a unit after a stored value, an
// exclusive upper bound includes it and an inclusive lower bound excludes it. Truncated, both would do the opposite.
func TestSQLTime_RoundsUpToTheColumnsPrecision(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	root := endedRoots(t, e, 1)[0]
	moveInto(t, e, root, today)
	unit := time.Millisecond
	if db, _ := e.db.Shard(1); db.DriverName() == "pgx" {
		unit = time.Microsecond
	}
	stored := time.Now().Add(-time.Minute).Truncate(unit)
	stampArchived(t, e, today, root, stored)
	if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today || archiver.DayOf(stored, e.archives.Load().dayLength) != today {
		t.Skip("midnight is too close")
	}
	half := unit / 2
	got, err := e.ListArchived(ctx, workflow.Query{FromTime: stored.Add(-time.Hour), ToTime: stored.Add(half), Limit: 10})
	assert.NoError(err)
	assert.Equal([]int{root}, idsOf(t, got), "before the upper bound")
	got, err = e.ListArchived(ctx, workflow.Query{FromTime: stored.Add(half), ToTime: stored.Add(time.Hour), Limit: 10})
	assert.NoError(err)
	assert.Len(got, 0, "before the lower bound")

	expr, arg := sqlTime("mysql", time.Date(2026, 11, 4, 1, 2, 3, 123400000, time.UTC))
	assert.Equal("CAST(? AS DATETIME(3))", expr)
	assert.Equal("2026-11-04 01:02:03.124", arg)
	_, arg = sqlTime("sqlite", time.Date(2026, 11, 4, 1, 2, 3, 123000000, time.UTC))
	assert.Equal("2026-11-04 01:02:03.123", arg, "an exact millisecond is kept")
	_, arg = sqlTime("pgx", time.Date(2026, 11, 4, 1, 2, 3, 123456700, time.UTC))
	assert.Equal("2026-11-04 01:02:03.123457", arg)
}
