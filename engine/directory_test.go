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
	"net/http"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// moveInto archives the tree rooted at rootID as a reaper pass would: into the day its root ended.
func moveInto(t *testing.T, e *Engine, rootID int) {
	t.Helper()
	ctx := t.Context()
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	day := &reapDay{}
	if err := e.refreshReapDay(ctx, e.archives.Load().byShard[1], day); err != nil {
		t.Fatal(err)
	}
	cut, ended, err := e.rootsWithinRows(ctx, db, []int{rootID})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := e.planBatch(day, cut, ended)
	if err != nil {
		t.Fatal(err)
	}
	moved, err := e.reapBatchOf(ctx, db, cut, e.endedDue(0), day, plan)
	if err != nil {
		t.Fatal(err)
	}
	if moved != 2 {
		t.Fatalf("moved %d flows of tree %d, want the root and its child", moved, rootID)
	}
}

// shiftTree moves the tree rooted at rootID back in time, as if it had been created createdDaysAgo and its root had
// ended endedDaysAgo: every flow's creation hour, and the root's updated_at, on the database clock.
func shiftTree(t *testing.T, e *Engine, rootID, createdDaysAgo, endedDaysAgo int) {
	t.Helper()
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET created_hour=created_hour-? WHERE root_flow_id=?", createdDaysAgo*24, rootID)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(updated_at, ?) WHERE flow_id=?",
		-endedDaysAgo*24*int(time.Hour/time.Millisecond), rootID)
	if err != nil {
		t.Fatal(err)
	}
}

// located returns the day LocateFlow finds a flow key's flow in.
func located(t *testing.T, e *Engine, flowKey string) (int, error) {
	t.Helper()
	_, hour, id, _, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		t.Fatal(err)
	}
	return e.archives.Load().byShard[1].LocateFlow(t.Context(), id, hour)
}

// TestDirectory_NamesOnlyWhatTheLocateCannotFind pins the directory's rule, through the reaper's own move: a tree
// archived - in the day its root ended - on its creation day or the day after gets no row, the locate finding it by
// its key's hour, while one that ended two or more days after its creation day has every flow named. The locate finds
// each in the day it ended on, a past one included.
func TestDirectory_NamesOnlyWhatTheLocateCannotFind(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	madeDay(t, e, today-1)
	roots := endedRoots(t, e, 4)
	db, err := e.db.Shard(1)
	assert.NoError(err)

	for i, c := range []struct {
		createdDaysAgo, endedDaysAgo int
		named                        bool
	}{
		{0, 0, false}, // ended on its creation day
		{1, 0, false}, // the day after
		{2, 0, true},  // two days after
		{3, 1, true},  // two days after, filed in yesterday's table
	} {
		shiftTree(t, e, roots[i], c.createdDaysAgo, c.endedDaysAgo)
		if now, _ := e.archives.Load().byShard[1].Today(ctx); now != today {
			t.Skip("midnight passed during the test")
		}
		var token string
		var hour int
		assert.NoError(db.QueryRowContext(ctx, "SELECT flow_token, created_hour FROM dwarf_flows WHERE flow_id=?", roots[i]).Scan(&token, &hour))
		moveInto(t, e, roots[i])
		endDay := today - c.endedDaysAgo
		var named int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable+" d JOIN "+
			migrations.FlowsTable(endDay)+" f ON f.flow_id=d.flow_id WHERE f.root_flow_id=?", roots[i]).Scan(&named))
		if c.named {
			assert.Equal(2, named, "case %d: the root and its child are named", i)
		} else {
			assert.Equal(0, named, "case %d: nothing is named", i)
		}
		day, err := located(t, e, keys.NewFlowKey(1, hour, roots[i], token))
		assert.NoError(err)
		assert.Equal(endDay, day, "case %d: located in the day it ended", i)
	}
}

// TestDirectory_ShortDaysNameMovesFarFromTheHour pins the directory with days shorter than the key's hour, as tests and
// the rig use: the creation day the hour gives is the day its hour began, so a move more than a day after that - all
// but the first seconds of the hour - is named, and the locate finds the flow either way.
func TestDirectory_ShortDaysNameMovesFarFromTheHour(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(ctx) })
	e.SetHost(treeMux())
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	e.testArchiveDay = 2 * time.Second
	assert.NoError(e.SetRetention(100))
	assert.NoError(e.Startup(ctx))

	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	_, hour, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	e.reapDueFlows(ctx)
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Zero(flows, "archived")
	day, err := located(t, e, fk)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	var named int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable+" WHERE day=?", day).Scan(&named))
	before, from := e.archives.Load().byShard[1].DirectoryHours(day)
	if hour < before || hour >= from {
		assert.Equal(2, named, "the root and its child are named")
	} else {
		assert.Equal(0, named, "moved within a day of its hour's start: found without a row")
	}
}

// TestDirectory_RetentionZeroNamesNothing pins that a tree deleted rather than archived - at a retention of 0 - leaves
// no directory row, and is not located.
func TestDirectory_RetentionZeroNamesNothing(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 0)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	e.reapDueFlows(ctx)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	var named int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable).Scan(&named))
	assert.Zero(named)
	_, err = located(t, e, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}

// TestDirectory_TheReapersBounds pins the reaper's comparisons at the edges of DirectoryHours: a tree whose creation
// hour is one below the lower bound or at the upper bound is named, one at the lower bound or one below the upper is
// not.
func TestDirectory_TheReapersBounds(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	today, err := e.archives.Load().byShard[1].Today(ctx)
	assert.NoError(err)
	before, from := e.archives.Load().byShard[1].DirectoryHours(today)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	roots := endedRoots(t, e, 4)
	for i, c := range []struct {
		hour  int
		named bool
	}{
		{before - 1, true},
		{before, false},
		{from - 1, false},
		{from, true},
	} {
		_, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET created_hour=? WHERE root_flow_id=?", c.hour, roots[i])
		assert.NoError(err)
		moveInto(t, e, roots[i])
		var named int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable+" d JOIN "+
			migrations.FlowsTable(today)+" f ON f.flow_id=d.flow_id WHERE f.root_flow_id=?", roots[i]).Scan(&named))
		if c.named {
			assert.Equal(2, named, "hour %d (bounds %d, %d) is named", c.hour, before, from)
		} else {
			assert.Equal(0, named, "hour %d (bounds %d, %d) is not named", c.hour, before, from)
		}
	}
}

// TestDirectory_AReaderRefreshesTheDaysItLocates pins that a replica that only reads a shard, and so maintains nothing
// there, still reads which days exist on every archive pass: a day made since its last read is probed from then on.
func TestDirectory_AReaderRefreshesTheDaysItLocates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	a := e.archives.Load().byShard[1]
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-3))
	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "INSERT INTO "+migrations.FlowsTable(today-3)+
		" (flow_id, flow_token, workflow_url, workflow_name, graph, graph_hash, status) VALUES (999999, 'tok', 'u', 'W', ?, 'h', 'completed')",
		[]byte("{}")) // bound: graph is binary on SQL Server
	assert.NoError(err)
	hour := (today - 3) * 24
	_, err = a.LocateFlow(ctx, 999999, hour)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "not probed before the read")

	e.dispatchFlag(1).Store(false)
	e.maintainArchives(ctx, e.archives.Load())
	day, err := a.LocateFlow(ctx, 999999, hour)
	assert.NoError(err)
	assert.Equal(today-3, day)
}
