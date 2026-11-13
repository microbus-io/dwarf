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

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/accessors"
	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/janitor"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
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
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	expr, arg := database.SQLTime(db.DriverName(), at)
	_, err = db.ExecContext(t.Context(), "UPDATE "+migrations.FlowsTable(day)+" SET archived_at="+expr+" WHERE root_flow_id=?", arg, rootID)
	if err != nil {
		t.Fatal(err)
	}
}

// TestList_EachListReadsItsOwnTables pins the split between the two lists over the live tables and three archive days:
// ListClosed reads the archive alone, newest archived first, windowed on the archive time from inclusive to exclusive,
// with ArchivedAt on every flow, a Limit that keeps the order's prefix, the subgraph children with IncludeSubgraphs,
// and Purge's ascending order the same list reversed; a flow closed but not archived yet is in ListRecentlyClosed alone,
// with its subgraph children by IncludeSubgraphs, by status, and not once it is marked for deletion.
func TestList_EachListReadsItsOwnTables(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	a := e.archiveOn(1)
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-2))
	assert.NoError(makeArchiveDay(t, e, today-1))
	assert.NoError(a.Refresh(ctx))
	roots := closedRoots(t, e, 6)
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
	all, err := e.ListClosed(ctx, window)
	assert.NoError(err)
	assert.Equal([]int{roots[2], roots[1], roots[5], roots[0]}, idsOf(t, all), "the archive alone, newest archived first")
	if assert.Len(all, 4) {
		for _, f := range all {
			assert.False(f.ArchivedAt.IsZero(), "archived")
		}
		assert.True(all[3].ArchivedAt.Equal(oldest), "archived at %v, not %v", all[3].ArchivedAt, oldest)
		assert.False(all[0].Subgraph)
	}
	open, err := e.ListOpen(ctx, workflow.Query{Limit: 100})
	assert.NoError(err)
	assert.Len(open, 0, "the flows closed but not archived yet are not open")
	live, err := accessors.ListLive(e, ctx, workflow.Query{Limit: 100})
	assert.NoError(err)
	assert.Equal([]int{roots[4], roots[3]}, idsOf(t, live), "they are in the live tables")
	recent, err := e.ListRecentlyClosed(ctx, workflow.Query{Limit: 100})
	assert.NoError(err)
	assert.Equal([]int{roots[4], roots[3]}, idsOf(t, recent), "ListRecentlyClosed lists them, and nothing archived")
	for _, f := range recent {
		assert.True(f.ArchivedAt.IsZero(), "not archived")
		assert.False(f.UpdatedAt.IsZero(), "closed at")
	}
	recent, err = e.ListRecentlyClosed(ctx, workflow.Query{Limit: 1})
	assert.NoError(err)
	assert.Equal([]int{roots[4]}, idsOf(t, recent), "the most recently closed first")
	recent, err = e.ListRecentlyClosed(ctx, workflow.Query{Limit: 100, IncludeSubgraphs: true})
	assert.NoError(err)
	assert.Len(recent, 4, "each live root and its subgraph child")
	if assert.Len(live, 2) {
		recent, err = e.ListRecentlyClosed(ctx, workflow.Query{Limit: 100, Status: live[0].Status})
		assert.NoError(err)
		assert.Contains(idsOf(t, recent), roots[4], "by its status")
	}
	recent, err = e.ListRecentlyClosed(ctx, workflow.Query{Limit: 100, Status: workflow.StatusCancelled})
	assert.NoError(err)
	assert.Len(recent, 0, "no flow was cancelled")

	for limit := 1; limit <= 4; limit++ {
		q := window
		q.Limit = limit
		got, err := e.ListClosed(ctx, q)
		assert.NoError(err)
		assert.Equal(idsOf(t, all)[:limit], idsOf(t, got), "limit %d", limit)
	}

	q := window
	q.IncludeSubgraphs = true
	withChildren, err := e.ListClosed(ctx, q)
	assert.NoError(err)
	assert.Len(withChildren, 8, "every archived root and its subgraph child")

	q = window
	q.ToTime = time.Now().Add(-time.Minute)
	past, err := e.ListClosed(ctx, q)
	assert.NoError(err)
	assert.Equal([]int{roots[1], roots[5], roots[0]}, idsOf(t, past), "a window ending a minute ago: not the one archived since")
	// A limit below a day's rows keeps that day's newest: each day's own read is ordered and limited too.
	q.Limit = 1
	past, err = e.ListClosed(ctx, q)
	assert.NoError(err)
	assert.Equal([]int{roots[1]}, idsOf(t, past))

	q = window
	q.FromTime, q.ToTime = time.Now().Add(time.Hour), time.Now().Add(2*time.Hour)
	future, err := e.ListClosed(ctx, q)
	assert.NoError(err)
	assert.Len(future, 0, "a window wholly after now")

	q = window
	q.FromTime, q.ToTime = oldest, oldest.Add(time.Millisecond)
	at, err := e.ListClosed(ctx, q)
	assert.NoError(err)
	assert.Equal([]int{roots[0]}, idsOf(t, at), "FromTime is inclusive")
	q.ToTime = oldest
	q.FromTime = oldest.Add(-time.Hour)
	before, err := e.ListClosed(ctx, q)
	assert.NoError(err)
	assert.Len(before, 0, "ToTime is exclusive")

	rows, err := e.listArchived(ctx, window, 100, true, "purge")
	assert.NoError(err)
	ascending := make([]int, len(rows))
	for i, lr := range rows {
		ascending[i] = lr.flowID
	}
	assert.Equal([]int{roots[0], roots[5], roots[1], roots[2]}, ascending, "oldest archived first")
	rows, err = e.listArchived(ctx, window, 1, true, "purge")
	assert.NoError(err)
	if assert.Len(rows, 1) {
		assert.Equal(roots[0], rows[0].flowID, "ascending, each day's read keeps its oldest")
	}

	assert.NoError(e.Delete(ctx, live[1].FlowKey))
	recent, err = e.ListRecentlyClosed(ctx, workflow.Query{Limit: 100})
	assert.NoError(err)
	assert.Equal([]int{roots[4]}, idsOf(t, recent), "a flow marked for deletion is not listed")
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

// TestListClosed_ADayAPeerDroppedIsReadAgain pins the list over a stale record of the days: a day a peer dropped since
// this replica's last catalog read fails the shard's statement on its missing table, and the list reads the catalog
// and runs it again, without the day.
func TestListClosed_ADayAPeerDroppedIsReadAgain(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	a := e.archiveOn(1)
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-2))
	assert.NoError(makeArchiveDay(t, e, today-1))
	assert.NoError(a.Refresh(ctx))
	root := closedRoots(t, e, 1)[0]
	moveInto(t, e, root, today-1)
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	peer := archiver.New(db, e.dayLength(), migrations.Archive())
	_, err = peer.DropBefore(ctx, today-1)
	assert.NoError(err)
	assert.True(a.Whole(today-2), "this replica's record is stale")

	got, err := e.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-(3 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 100})
	assert.NoError(err)
	assert.Equal([]int{root}, idsOf(t, got))
	assert.False(a.Whole(today-2), "the record was read again")
}

// TestListClosed_TheWindowIsClampedToTheArchive pins the window's cut: a lower bound before the retention reads from
// the retention's first day, a window wholly before it lists nothing, and what is left after the cut may reach at most
// 31 days - today and the 30 before it - or the call is a 400.
func TestListClosed_TheWindowIsClampedToTheArchive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	root := closedRoots(t, e, 1)[0]
	moveInto(t, e, root, today)
	got, err := e.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-(400 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 10})
	assert.NoError(err, "a window past the retention is cut to it")
	assert.Equal([]int{root}, idsOf(t, got))
	got, err = e.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-(400 * 24 * time.Hour)), ToTime: time.Now().Add(-(40 * 24 * time.Hour)), Limit: 10})
	assert.NoError(err)
	assert.Len(got, 0, "a window wholly before the retention")

	// A retention past the cap, so the cut leaves the window as long as asked. Its own engine, under its own name.
	t.Run("retention_100", func(t *testing.T) {
		assert := testarossa.For(t)
		long := archiveEngine(t, treeMux(), 100)
		_, err := long.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-(30 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 10})
		assert.NoError(err, "31 days, today included")
		_, err = long.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-(31 * 24 * time.Hour)), ToTime: time.Now().Add(time.Hour), Limit: 10})
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "32 days")
		_, err = long.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-(60 * 24 * time.Hour)), ToTime: time.Now().Add(-(30 * 24 * time.Hour)), Limit: 10})
		assert.NoError(err, "31 days, all in the past")
		_, err = long.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-(20 * 24 * time.Hour)), ToTime: time.Now().Add(60 * 24 * time.Hour), Limit: 10})
		assert.NoError(err, "a window reaching 60 days ahead is cut to today")
		_, err = long.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-(30 * 24 * time.Hour)), ToTime: time.Now().Add(48 * time.Hour), Limit: 10})
		assert.NoError(err, "31 days, and the day past today it reads beside them uncounted")
	})
}

// TestList_RequiredInputsAndStatuses pins what the two lists refuse with a 400: no Limit, a Limit over the most a call
// returns, an empty window, a negative duration, and an unknown status or shard; ListClosed a missing bound on either
// side, which ListOpen takes as no bound there; and each list a status of the other's.
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
		_, err := e.ListOpen(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListOpen: %s", name)
		_, err = e.ListClosed(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListClosed: %s", name)
		_, err = e.ListRecentlyClosed(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListRecentlyClosed: %s", name)
	}
	for name, q := range map[string]workflow.Query{
		"no lower bound": {ToTime: now.Add(time.Hour), Limit: 10},
		"no upper bound": {FromTime: time.Now().Add(-time.Hour), Limit: 10},
		"no bound":       {Limit: 10},
	} {
		_, err := e.ListClosed(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListClosed: %s", name)
		_, err = e.ListOpen(ctx, q)
		assert.NoError(err, "ListOpen: %s", name)
		_, err = e.ListRecentlyClosed(ctx, q)
		assert.NoError(err, "ListRecentlyClosed: %s", name)
	}
	max := ok
	max.Limit = maxListLimit
	_, err := e.ListOpen(ctx, max)
	assert.NoError(err)
	_, err = e.ListClosed(ctx, max)
	assert.NoError(err)
	_, err = e.ListRecentlyClosed(ctx, max)
	assert.NoError(err)

	for _, status := range []string{workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusTerminated, workflow.StatusCancelled} {
		q := ok
		q.Status = status
		_, err := e.ListOpen(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListOpen of %s", status)
		_, err = e.ListClosed(ctx, q)
		assert.NoError(err, "ListClosed of %s", status)
		_, err = e.ListRecentlyClosed(ctx, q)
		assert.NoError(err, "ListRecentlyClosed of %s", status)
	}
	for _, status := range []string{workflow.StatusRunning, workflow.StatusInterrupted} {
		q := ok
		q.Status = status
		_, err := e.ListClosed(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListClosed of %s", status)
		_, err = e.ListOpen(ctx, q)
		assert.NoError(err, "ListOpen of %s", status)
		_, err = e.ListRecentlyClosed(ctx, q)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "ListRecentlyClosed of %s", status)
	}
}

// TestList_TheLimitIsAboveAReaperBatch pins that the lists' maximum Limit stays above the reaper's batch: a batch's
// trees share one archive time, and a caller paging ListClosed by window cannot page across a tie larger than its Limit.
func TestList_TheLimitIsAboveAReaperBatch(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngine()
	assert.True(maxListLimit > e.reapBatchRows, "the most a list returns (%d) must exceed a reaper batch (%d rows)", maxListLimit, e.reapBatchRows)
	assert.True(maxListLimit > e.reapBatch, "and its roots (%d)", e.reapBatch)
	assert.True(maxListLimit > janitor.MaxBatchFlows, "and an archiving batch's flows (%d)", janitor.MaxBatchFlows)
}

// TestListClosed_TheDayAfterTodayIsRead pins the one day past today a window reaching past now reads, for a database
// clock ahead of this process's across midnight: a tree there is listed.
func TestListClosed_TheDayAfterTodayIsRead(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	root := closedRoots(t, e, 1)[0]
	moveInto(t, e, root, today+1) // as a pass on a database clock already past midnight would
	got, err := e.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: time.Now().Add(48 * time.Hour), Limit: 10})
	assert.NoError(err)
	if now, _ := e.archiveOn(1).Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal([]int{root}, idsOf(t, got))
}

// TestListClosed_TheWindowRoundsUpToTheColumnsPrecision pins that a bound between two of the column's units compares
// as the instant it names: half a unit after a stored value, an exclusive upper bound includes it and an inclusive
// lower bound excludes it. Truncated, both would do the opposite.
func TestListClosed_TheWindowRoundsUpToTheColumnsPrecision(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	root := closedRoots(t, e, 1)[0]
	moveInto(t, e, root, today)
	unit := time.Millisecond
	stored := time.Now().Add(-time.Minute).Truncate(unit)
	stampArchived(t, e, today, root, stored)
	if now, _ := e.archiveOn(1).Today(ctx); now != today || archiver.DayOf(stored, e.dayLength()) != today {
		t.Skip("midnight is too close")
	}
	half := unit / 2
	got, err := e.ListClosed(ctx, workflow.Query{FromTime: stored.Add(-time.Hour), ToTime: stored.Add(half), Limit: 10})
	assert.NoError(err)
	assert.Equal([]int{root}, idsOf(t, got), "before the upper bound")
	got, err = e.ListClosed(ctx, workflow.Query{FromTime: stored.Add(half), ToTime: stored.Add(time.Hour), Limit: 10})
	assert.NoError(err)
	assert.Len(got, 0, "before the lower bound")
}

// TestListClosed_RetentionZeroStillReadsToday pins the archived list at a retention of 0: today's tables are still
// read, holding what a run with a retention archived before a restart.
func TestListClosed_RetentionZeroStillReadsToday(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 0)
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	root := closedRoots(t, e, 1)[0]
	moveInto(t, e, root, today)
	got, err := e.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: time.Now().Add(time.Hour), Limit: 10})
	assert.NoError(err)
	if now, _ := e.archiveOn(1).Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.Equal([]int{root}, idsOf(t, got))
}

// TestListOpen_NewestUpdatedFirstAcrossStatuses pins ListOpen: the running and interrupted flows, not a closed one, the
// most recently updated first across statuses - each status is its own branch of the statement, so the ordered union
// of the branches is what a Limit cuts - a Limit keeping that order's prefix, and a status filter.
func TestListOpen_NewestUpdatedFirstAcrossStatuses(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, _, _ := expiryEngine(t, 0)
	first := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	napping := startOpen(t, e, "exp/nap", workflow.StatusRunning)
	last := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	closed := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	assert.NoError(e.Terminate(ctx, closed, "closed"))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	// The two newest are both interrupted, the second branch: an outer limit that did not order the branches' union
	// would keep the running flow, which comes first in it.
	for fk, ahead := range map[string]int{first: 3000, last: 2000, napping: 1000} {
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		_, err = db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?", ahead, id)
		assert.NoError(err)
	}
	keysOf := func(flows []workflow.FlowSummary) []string {
		var got []string
		for _, f := range flows {
			got = append(got, f.FlowKey)
		}
		return got
	}

	all, err := e.ListOpen(ctx, workflow.Query{Limit: 100})
	assert.NoError(err)
	assert.Equal([]string{first, last, napping}, keysOf(all), "newest updated first, across statuses; not the closed flow")
	two, err := e.ListOpen(ctx, workflow.Query{Limit: 2})
	assert.NoError(err)
	assert.Equal([]string{first, last}, keysOf(two))
	running, err := e.ListOpen(ctx, workflow.Query{Status: workflow.StatusRunning, Limit: 100})
	assert.NoError(err)
	assert.Equal([]string{napping}, keysOf(running))
	interrupted, err := e.ListOpen(ctx, workflow.Query{Status: workflow.StatusInterrupted, Limit: 100})
	assert.NoError(err)
	assert.Equal([]string{first, last}, keysOf(interrupted))
}

// TestListClosed_AFarFutureBoundMeansNoBound pins the clamp at the year 9999: a ToTime past it - a "no upper bound"
// sentinel - lists what a ToTime of the last instant would, on every dialect, rather than nothing (SQLite compares a
// five-digit year as text, MySQL casts it to NULL) or an error (SQL Server).
func TestListClosed_AFarFutureBoundMeansNoBound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	roots := closedRoots(t, e, 1)
	a := e.archiveOn(1)
	today, err := a.Today(ctx)
	assert.NoError(err)
	moveInto(t, e, roots[0], today)
	far := time.Date(20000, 1, 1, 0, 0, 0, 0, time.UTC)
	got, err := e.ListClosed(ctx, workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: far, Limit: 10})
	assert.NoError(err)
	assert.Equal([]int{roots[0]}, idsOf(t, got))
}

// TestList_SearchPatternIsLiteral pins the LIKE pattern a Search builds per dialect: every metacharacter of that
// dialect is escaped, so the term matches as a literal substring. SQL Server alone reads `[` as opening a character
// set, so only its pattern escapes it; unescaped, Search "[x]" matched every value holding an x, and Purge deleted them.
func TestList_SearchPatternIsLiteral(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, driver := range []string{"sqlite", "pgx", "mysql", "mariadb"} {
		assert.Equal("%a~_b~%c~~d[x]%", likePattern(driver, "A_B%C~D[X]"), driver)
	}
	assert.Equal("%a~_b~%c~~d~[x]%", likePattern("mssql", "A_B%C~D[X]"))
	assert.Equal("%~[a-c]~[^z]]%", likePattern("mssql", "[a-c][^z]]"))
}

// TestList_SearchFindsTheFlowOfACorrelationID pins that the correlation id telemetry carries finds its flow through
// the lists: searched for followed by "-", within its shard, it matches that flow's key and no other flow's.
func TestList_SearchFindsTheFlowOfACorrelationID(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()

	mux := hosts.NewMux()
	g := workflow.NewGraph("Corr")
	g.SetURL("Work", "corrsearch.verify:0/work")
	g.AddTransition("Work", workflow.End)
	mux.HandleGraph("corrsearch.verify:0/corr", g)
	mux.HandleTask("corrsearch.verify:0/work", func(ctx context.Context, f *workflow.Flow) error { return nil })
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(context.Background())
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(ctx)))

	var flowKeys []string
	for range 2 {
		key, _, err := e.Run(ctx, "corrsearch.verify:0/corr", nil, nil)
		assert.Must(assert.NoError(err))
		flowKeys = append(flowKeys, key)
	}
	shard, hour, flowID, _, err := keys.ParseFlowKey(flowKeys[0])
	assert.Must(assert.NoError(err))
	found, err := enginetest.ListAll(ctx, e, workflow.Query{Shard: shard, Search: keys.CorrelationID(shard, hour, flowID) + "-"})
	assert.NoError(err)
	if assert.Len(found, 1) {
		assert.Equal(flowKeys[0], found[0].FlowKey)
	}
}
