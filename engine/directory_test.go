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

// moveInto archives the tree rooted at rootID into day, as a reaper pass whose day is day would.
func moveInto(t *testing.T, e *Engine, rootID, day int) {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	moved, err := e.reaper.Load().MoveTrees(t.Context(), db, e.archiveOn(1), day, []int{rootID})
	if err != nil {
		t.Fatal(err)
	}
	if moved != 2 {
		t.Fatalf("moved %d flows of tree %d, want the root and its child", moved, rootID)
	}
}

// located returns the day LocateFlow finds a flow key's flow in.
func located(t *testing.T, e *Engine, flowKey string) (int, error) {
	t.Helper()
	_, hour, id, _, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		t.Fatal(err)
	}
	return e.archiveOn(1).LocateFlow(t.Context(), id, hour)
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
	assert.Must(assert.NoError(e.Startup(ctx)))

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
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var named int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable+" WHERE day=?", day).Scan(&named))
	before, from := e.archiveOn(1).DirectoryHours(day)
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
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	var named int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+archiver.DirectoryTable).Scan(&named))
	assert.Zero(named)
	_, err = located(t, e, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}

// TestDirectory_AReaderRefreshesTheDaysItLocates pins that a replica that only reads a shard, and so maintains nothing
// there, still reads which days exist on every archive pass: a day made since its last read is probed from then on.
func TestDirectory_AReaderRefreshesTheDaysItLocates(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	a := e.archiveOn(1)
	today, err := a.Today(ctx)
	assert.NoError(err)
	assert.NoError(makeArchiveDay(t, e, today-3))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	_, err = db.ExecContext(ctx, "INSERT INTO "+migrations.FlowsTable(today-3)+
		" (flow_id, flow_token, workflow_url, workflow_name, graph, graph_hash, status, root_flow_id) VALUES (999999, 'tok', 'u', 'W', ?, 'h', 'completed', 999999)",
		[]byte("{}")) // bound: graph is binary on SQL Server
	assert.NoError(err)
	hour := (today - 3) * 24
	_, err = a.LocateFlow(ctx, 999999, hour)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err), "not probed before the read")

	e.dispatchFlag(1).Store(false)
	e.maintainArchives(ctx)
	day, err := a.LocateFlow(ctx, 999999, hour)
	assert.NoError(err)
	assert.Equal(today-3, day)
}
