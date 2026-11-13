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
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// treeCount returns how many flows and steps of the tree rooted at rootID the two tables hold.
func treeCount(t *testing.T, e *Engine, flowsTable, stepsTable string, rootID int) (flows, steps int) {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	ctx := t.Context()
	flows, steps = -1, -1
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+flowsTable+" WHERE root_flow_id=?", rootID).Scan(&flows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+stepsTable+" WHERE flow_id IN (SELECT flow_id FROM "+
		flowsTable+" WHERE root_flow_id=?)", rootID).Scan(&steps); err != nil {
		t.Fatal(err)
	}
	return flows, steps
}

// archiveEngine starts an engine whose closed roots are due at once and whose own reaper stays out of the way, so a
// test drives each pass itself.
func archiveEngine(t *testing.T, mux *hosts.Mux, retention int) *Engine {
	t.Helper()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(mux)
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	if err := e.SetRetention(retention); err != nil {
		t.Fatal(err)
	}
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	// The archive loop passes once as it starts, on its own goroutine; waited out here, it cannot land in the middle of
	// a test and read the catalog - learning a day the test made - at a moment the test does not choose.
	enginetest.AwaitVisitCount(t, e.diags.Seams(), 1, 10*time.Second, diagnostics.CheckpointArchivePassDone)
	return e
}

// treeMux serves a parent graph whose one task calls a one-task child graph, so a root carries a subgraph descendant.
func treeMux() *hosts.Mux {
	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Call", "arch/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("arch/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("Work", "arch/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("arch/child", child)
	mux.HandleTask("arch/call", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph("arch/child", map[string]any{"n": 1}, &out)
		if yield || err != nil {
			return err
		}
		f.Set("got", out["done"])
		return nil
	})
	mux.HandleTask("arch/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetBool("done", true)
		return nil
	})
	// arch/solo is the child's graph run as a root: a tree of one flow, so consecutive roots take consecutive ids.
	mux.HandleGraph("arch/solo", child)
	return mux
}

// TestArchiving_OnlyClosedRootsPastTheGraceLeave pins what the archiver leaves alone: a flow that has not closed (an
// interrupted one waits for a human), and a closed one inside its grace. A DeleteOnCompletion root is deleted, not
// archived.
func TestArchiving_OnlyClosedRootsPastTheGraceLeave(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	mux := treeMux()
	ask := workflow.NewGraph("Ask")
	ask.SetURL("Ask", "arch/ask")
	ask.AddTransition("Ask", workflow.End)
	mux.HandleGraph("arch/ask", ask)
	mux.HandleTask("arch/ask", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"q": "ok?"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	e := archiveEngine(t, mux, 30)
	e.deletionGrace = time.Millisecond // 0 would stamp "keep"

	asked, err := e.Create(ctx, "arch/ask", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, asked, workflow.StatusInterrupted, time.Minute)
	disposable, err := e.Create(ctx, "arch/parent", nil, &workflow.FlowOptions{DeleteOnCompletion: true})
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, disposable, workflow.StatusCompleted, time.Minute)
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)

	// The stamp is due a millisecond after completion on the database clock, so a pass may come too soon.
	_, _, disposableID, _, _ := keys.ParseFlowKey(disposable)
	deadline := time.Now().Add(5 * time.Second * enginetest.TimeoutScale())
	flows := -1
	for flows != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond) // the grace is wall-clock time
		e.reapDueFlows(ctx)
		flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", disposableID)
	}
	assert.Equal(0, flows, "a DeleteOnCompletion root is deleted")
	_, _, askedID, _, _ := keys.ParseFlowKey(asked)
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", askedID)
	assert.Equal(1, flows, "an interrupted flow has not closed")
	if now, _ := e.archiveOn(1).Today(ctx); now == today {
		archFlows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), disposableID)
		assert.Equal(0, archFlows, "and not archived")
	}

	e.archiveGrace = time.Hour
	fresh, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fresh, workflow.StatusCompleted, time.Minute)
	e.reapDueFlows(ctx)
	_, _, freshID, _, _ := keys.ParseFlowKey(fresh)
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", freshID)
	assert.Equal(2, flows, "a flow inside its grace stays live")
}

// TestArchiving_DeleteOfATreeArchivedMeanwhileDeletesItThere pins Delete against the reaper: a tree archived between
// Delete's read and its stamp is deleted from the archive instead, never reported deleted while it stays there.
func TestArchiving_DeleteOfATreeArchivedMeanwhileDeletesItThere(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)

	e.diags.Seams().Break(diagnostics.CheckpointBeforeDeleteStamp)
	deleted := make(chan error, 1)
	go func() { deleted <- e.Delete(ctx, fk) }()
	assert.True(e.diags.Seams().WaitTimeout(ctx, diagnostics.CheckpointBeforeDeleteStamp, 10*time.Second))
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	e.reapDueFlows(ctx)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	flows, _ := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), rootID)
	if now, _ := e.archiveOn(1).Today(ctx); flows == 0 && now != today {
		e.diags.Seams().Resume(diagnostics.CheckpointBeforeDeleteStamp)
		t.Skip("midnight passed during the pass")
	}
	assert.Equal(2, flows, "the reaper archived the tree while the Delete was held")
	e.diags.Seams().Resume(diagnostics.CheckpointBeforeDeleteStamp)
	assert.NoError(<-deleted)
	e.reapDueFlows(ctx) // the archive deletion sweep deletes what the Delete marked
	flows, steps := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), rootID)
	assert.Zero(flows, "deleted from the archive")
	assert.Zero(steps)
	_, err = e.Snapshot(ctx, fk)
	assert.Equal(http.StatusNotFound, errors.StatusCode(err))
}

// closedRoots creates n closed trees and returns their roots.
func closedRoots(t *testing.T, e *Engine, n int) []int {
	t.Helper()
	assert := testarossa.For(t)
	var roots []int
	for range n {
		fk, err := e.Create(t.Context(), "arch/parent", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		roots = append(roots, id)
	}
	return roots
}

// logCounter is a slog.Handler that counts the records carrying each message.
type logCounter struct {
	mu   sync.Mutex
	seen map[string]int
}

func (l *logCounter) Enabled(context.Context, slog.Level) bool { return true }
func (l *logCounter) WithAttrs([]slog.Attr) slog.Handler       { return l }
func (l *logCounter) WithGroup(string) slog.Handler            { return l }
func (l *logCounter) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	l.seen[r.Message]++
	l.mu.Unlock()
	return nil
}

// count returns how many records carried a message starting with prefix.
func (l *logCounter) count(prefix string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for msg, c := range l.seen {
		if strings.HasPrefix(msg, prefix) {
			n += c
		}
	}
	return n
}

// TestArchiving_ARemovalOfADroppedDayIsNoError pins the archive-side deletion across days: a day dropped since the list
// read it removes nothing and is no error, and the roots of a day still there are removed and counted.
func TestArchiving_ARemovalOfADroppedDayIsNoError(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, hosts.NewMux(), 30)
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
	today, err := e.archiveOn(1).Today(ctx)
	assert.NoError(err)
	roots := seedTrees(t, db, 2, callShape)
	moveInto(t, e, roots[0], today)
	moveInto(t, e, roots[1], today)
	dropped := today - 5 // never made: its tables are undefined, as a dropped day's are
	removed, err := e.deleteArchivedRoots(ctx, []listRow{
		{shard: 1, flowID: roots[0], day: today},
		{shard: 1, flowID: roots[1], day: dropped},
	})
	assert.NoError(err)
	assert.Equal(1, removed, "the day still there")
	e.reapDueFlows(ctx) // the archive deletion sweep deletes what the call marked
	flows, steps := treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), roots[0])
	assert.Zero(flows + steps)
	flows, _ = treeCount(t, e, migrations.FlowsTable(today), migrations.StepsTable(today), roots[1])
	assert.Equal(2, flows, "not named in a day it is in")
}
