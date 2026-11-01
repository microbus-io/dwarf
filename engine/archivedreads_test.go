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
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	merrors "github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// endingMux serves one graph per way a flow ends, each a parent calling a one-task child, so every tree carries a
// subgraph descendant. The child of read/block waits for release before it returns.
func endingMux(release <-chan struct{}, started chan<- struct{}) *hosts.Mux {
	mux := hosts.NewMux()
	for _, name := range []string{"ok", "fail", "block"} {
		parent := workflow.NewGraph("Parent")
		parent.SetURL("Call", "read/call/"+name)
		parent.AddTransition("Call", workflow.End)
		mux.HandleGraph("read/"+name, parent)
		child := workflow.NewGraph("Child")
		child.SetURL("Work", "read/work/"+name)
		child.AddTransition("Work", workflow.End)
		mux.HandleGraph("read/child/"+name, child)
		mux.HandleTask("read/call/"+name, func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph("read/child/"+name, nil, nil)
			if yield || err != nil {
				return err
			}
			return f.Set("called", name)
		})
	}
	mux.HandleTask("read/work/ok", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("done", true)
	})
	mux.HandleTask("read/work/fail", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("no luck")
	})
	mux.HandleTask("read/work/block", func(ctx context.Context, f *workflow.Flow) error {
		started <- struct{}{}
		<-release
		return f.Set("done", true)
	})
	return mux
}

// childKey returns the key of the root's subgraph child.
func childKey(t *testing.T, e *Engine, rootKey string) string {
	t.Helper()
	_, _, rootID, _, err := keys.ParseFlowKey(rootKey)
	if err != nil {
		t.Fatal(err)
	}
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	var id, hour int
	var token string
	err = db.QueryRowContext(t.Context(), "SELECT flow_id, flow_token, created_hour FROM dwarf_flows WHERE surgraph_flow_id=?",
		rootID).Scan(&id, &token, &hour)
	if err != nil {
		t.Fatal(err)
	}
	return keys.NewFlowKey(1, hour, id, token)
}

// readsOf is what the read paths return for one flow key.
type readsOf struct {
	outcome     *workflow.FlowOutcome
	fingerprint string
	status      string
}

func readAll(t *testing.T, e *Engine, flowKey string) readsOf {
	t.Helper()
	assert := testarossa.For(t)
	var r readsOf
	var err error
	r.outcome, err = e.Snapshot(t.Context(), flowKey)
	assert.NoError(err, flowKey)
	r.fingerprint, r.status, err = e.Fingerprint(t.Context(), flowKey)
	assert.NoError(err, flowKey)
	return r
}

// TestArchivedReads_ArchivingChangesNoRead pins that Snapshot and Fingerprint read an archived flow as they read it
// live, for every way a flow ends and for a subgraph child's key as well as the root's: a poller's fingerprint must
// not move because its flow was archived.
func TestArchivedReads_ArchivingChangesNoRead(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	e := archiveEngine(t, endingMux(release, started), 30)

	completed, err := e.Create(ctx, "read/ok", nil, nil)
	assert.NoError(err)
	failed, err := e.Create(ctx, "read/fail", nil, nil)
	assert.NoError(err)
	terminated, err := e.Create(ctx, "read/block", nil, nil)
	assert.NoError(err)
	cancelled, err := e.Create(ctx, "read/block", nil, nil)
	assert.NoError(err)
	<-started
	<-started
	assert.NoError(e.Terminate(ctx, terminated, "stopped"))
	assert.NoError(e.Cancel(ctx, cancelled, "called off"))
	close(release)
	ended := map[string]string{
		completed:  workflow.StatusCompleted,
		failed:     workflow.StatusFailed,
		terminated: workflow.StatusTerminated,
		cancelled:  workflow.StatusCancelled,
	}
	for fk, status := range ended {
		enginetest.AwaitFlowStatus(t, e, fk, status, time.Minute)
	}

	before := map[string]readsOf{}
	var flowKeys []string
	for fk, status := range ended {
		for _, k := range []string{fk, childKey(t, e, fk)} {
			before[k] = readAll(t, e, k)
			flowKeys = append(flowKeys, k)
		}
		assert.Equal(status, before[fk].outcome.Status)
	}

	e.reapDueFlows(ctx)
	for fk := range ended {
		_, _, rootID, _, err := keys.ParseFlowKey(fk)
		assert.NoError(err)
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
		assert.Zero(flows, "tree of %s left in the live tables", fk)
	}
	for _, k := range flowKeys {
		after := readAll(t, e, k)
		assert.Equal(before[k].outcome, after.outcome, k)
		assert.Equal(before[k].fingerprint, after.fingerprint, k)
		assert.Equal(before[k].status, after.status, k)
	}
}

// readAcrossTheMove runs read held at checkpoint cp, after its first statement on the live tables, while the reaper
// moves the tree, and returns its result, failing the test unless the flow named by flowKey left the live tables. The
// read's statements hold no locks between them, so the move commits while it is held, and its later statements run
// after the move.
func readAcrossTheMove(t *testing.T, e *Engine, flowKey, cp string, read func() (any, error)) (any, error) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := t.Context()
	db, err := e.db.Shard(1)
	assert.NoError(err)
	e.seams.Break(cp)
	type result struct {
		v   any
		err error
	}
	done := make(chan result, 1)
	go func() {
		v, err := read()
		done <- result{v, err}
	}()
	assert.True(e.seams.WaitTimeout(ctx, cp, 10*time.Second))
	reaped := make(chan struct{})
	go func() {
		e.reapDueFlows(ctx)
		close(reaped)
	}()
	<-reaped
	e.seams.Resume(cp)
	r := <-done
	_, _, flowID, _, err := keys.ParseFlowKey(flowKey)
	assert.NoError(err)
	var live int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id=?", flowID).Scan(&live))
	assert.Zero(live, "the tree did not move")
	return r.v, r.err
}

// TestArchivedReads_FingerprintStraddlingTheMoveReadsOneSide pins the closing read: a Fingerprint whose tree is
// archived between its first statement and the rest returns the digest it would have returned without the move. For a
// root and for a subgraph child, whose subtree is found by a walk of its own.
func TestArchivedReads_FingerprintStraddlingTheMoveReadsOneSide(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	for _, child := range []bool{false, true} {
		fk, err := e.Create(ctx, "arch/parent", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		if child {
			fk = childKey(t, e, fk)
		}
		want, _, err := e.Fingerprint(ctx, fk)
		assert.NoError(err)

		got, err := readAcrossTheMove(t, e, fk, engineseams.CheckpointFingerprintFlowRead, func() (any, error) {
			fp, _, err := e.Fingerprint(ctx, fk)
			return fp, err
		})
		assert.NoError(err, "child %v", child)
		assert.Equal(want, got, "child %v", child)
	}
}

// TestArchivedReads_NotFound pins that an archived flow is found only by its whole key, as a live one is, and that a
// tree deleted rather than archived - at a retention of 0 - is not found.
func TestArchivedReads_NotFound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	for _, retention := range []int{30, 0} {
		e := archiveEngine(t, treeMux(), retention)
		fk, err := e.Create(ctx, "arch/parent", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		e.reapDueFlows(ctx)
		shard, hour, id, _, err := keys.ParseFlowKey(fk)
		assert.NoError(err)

		keysToMiss := []string{
			keys.NewFlowKey(shard, hour, id, "fedcba9876543210"),
			keys.NewFlowKey(shard, hour+1, id, "fedcba9876543210"),
		}
		if retention == 0 {
			keysToMiss = append(keysToMiss, fk)
		} else {
			_, err = e.Snapshot(ctx, fk)
			assert.NoError(err)
		}
		for _, k := range keysToMiss {
			_, err = e.Snapshot(ctx, k)
			assert.Equal(http.StatusNotFound, merrors.StatusCode(err), "retention %d, %s: %v", retention, k, err)
			_, _, err = e.Fingerprint(ctx, k)
			assert.Equal(http.StatusNotFound, merrors.StatusCode(err), "retention %d, %s: %v", retention, k, err)
		}
	}
}

// TestArchivedReads_AwaitParkedAcrossTheMoveGetsTheOutcome pins Await against the archive: a caller parked on a
// running flow that stops and is archived before the detector sees the stop - its wake dropped, the sweep held off -
// is released by the sweep as unresolved, the flow being gone from the live tables, and its own read then finds the
// outcome in the archive rather than reporting the flow not found. Poll and Run wait the same way.
func TestArchivedReads_AwaitParkedAcrossTheMoveGetsTheOutcome(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock) // runs first, so a failed test frees the worker
	e.SetHost(endingMux(release, started))
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	e.latchSweepInterval = time.Hour // the test sweeps
	assert.NoError(e.SetRetention(30))
	assert.NoError(e.Startup(ctx))

	fk, err := e.Create(ctx, "read/block", nil, nil)
	assert.NoError(err)
	<-started
	type result struct {
		outcome *workflow.FlowOutcome
		err     error
	}
	awaited := make(chan result, 1)
	go func() {
		out, err := e.Await(ctx, fk)
		awaited <- result{out, err}
	}()
	for deadline := time.Now().Add(10 * time.Second); e.latches.Waiting(fk) == 0; {
		if time.Now().After(deadline) {
			t.Fatal("Await never parked")
		}
		time.Sleep(10 * time.Millisecond)
	}

	e.seams.InjectN(engineseams.FaultDropSignalStop, 1<<20)
	unblock()
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	e.reapDueFlows(ctx)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Zero(flows)
	select {
	case r := <-awaited:
		t.Fatalf("Await returned before the sweep: %v, %v", r.outcome, r.err)
	default:
	}

	e.latches.Sweep(ctx)
	r := <-awaited
	if assert.NoError(r.err) {
		assert.Equal(workflow.StatusCompleted, r.outcome.Status)
		assert.Equal("block", r.outcome.State.GetString("called"))
	}
}

// refTreeMux serves a root that seeds a document large enough to be carried by reference, uses it in a later step,
// and calls a one-task child: a tree whose steps resolve refs and whose navigation crosses into a subgraph.
func refTreeMux() *hosts.Mux {
	mux := hosts.NewMux()
	parent := workflow.NewGraph("Parent")
	parent.SetURL("Seed", "refs/seed")
	parent.SetURL("Use", "refs/use")
	parent.SetURL("Call", "refs/call")
	parent.SetURL("After", "refs/after")
	parent.AddTransitionChain("Seed", "Use", "Call", "After", workflow.End)
	mux.HandleGraph("refs/parent", parent)
	child := workflow.NewGraph("Child")
	child.SetURL("Work", "refs/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph("refs/child", child)
	mux.HandleTask("refs/seed", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("doc", blob(200_000))
	})
	mux.HandleTask("refs/use", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("docLen", len(f.GetString("doc")))
	})
	mux.HandleTask("refs/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("refs/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("refs/work", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("done", true)
	})
	mux.HandleTask("refs/after", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("after", true)
	})
	return mux
}

// refEngine starts an archiving engine that keeps no resolved field in memory and seeds none on write, so every
// ref a read resolves is read from the table its anchor is in.
func refEngine(t *testing.T) *Engine {
	t.Helper()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(refTreeMux())
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	if err := e.SetRetention(30); err != nil {
		t.Fatal(err)
	}
	if err := e.SetStateCacheSize(1); err != nil {
		t.Fatal(err)
	}
	unseeded(e)
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	return e
}

// stepKeysOf returns every step key in a history, its subhistories included.
func stepKeysOf(history []workflow.FlowStep) []string {
	var out []string
	for _, s := range history {
		out = append(out, s.StepKey)
		out = append(out, stepKeysOf(s.SubHistory)...)
	}
	return out
}

// TestArchivedReads_HistoryAndStepReadTheArchive pins History and Step on an archived tree: the same history,
// subgraph included, and every step with its refs resolved from the day's table and its navigation across the
// subgraph seam, as they read live.
func TestArchivedReads_HistoryAndStepReadTheArchive(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := refEngine(t)
	fk, err := e.Create(ctx, "refs/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	child := childKey(t, e, fk)

	history, err := e.History(ctx, fk)
	assert.NoError(err)
	childHistory, err := e.History(ctx, child)
	assert.NoError(err)
	stepKeys := stepKeysOf(history)
	assert.Len(stepKeys, 5)
	after, err := e.Step(ctx, history[3].StepKey)
	assert.NoError(err)
	assert.Equal(childHistory[0].StepKey, after.PrevKey, "After navigates back to the subgraph's exit")
	steps := map[string]*workflow.FlowStep{}
	for _, k := range stepKeys {
		steps[k], err = e.Step(ctx, k)
		assert.NoError(err, k)
	}
	use := steps[history[1].StepKey]
	assert.Len(use.State.GetString("doc"), 200_000)
	_, refs, _ := stepRefsOf(t, e, keyID(t, history[1].StepKey))
	assert.True(len(refs) > 0, "Use carries the document by ref")

	e.reapDueFlows(ctx)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Zero(flows)

	got, err := e.History(ctx, fk)
	assert.NoError(err)
	assert.Equal(history, got)
	got, err = e.History(ctx, child)
	assert.NoError(err)
	assert.Equal(childHistory, got)
	for _, k := range stepKeys {
		s, err := e.Step(ctx, k)
		assert.NoError(err, k)
		assert.Equal(steps[k], s, k)
	}
}

// keyID returns the id a key names.
func keyID(t *testing.T, key string) int {
	t.Helper()
	_, _, _, id, _, err := keys.ParseStepKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestArchivedReads_HistoryAndStepStraddlingTheMoveReadOneSide pins the closing read: a History, or a Step, whose tree
// is archived between its first statement and the rest returns what it would have returned without the move. For Step,
// a step whose refs resolve after the move, and one without refs whose navigation reads follow it.
func TestArchivedReads_HistoryAndStepStraddlingTheMoveReadOneSide(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := refEngine(t)

	newTree := func() (string, []workflow.FlowStep) {
		fk, err := e.Create(ctx, "refs/parent", nil, nil)
		assert.NoError(err)
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		history, err := e.History(ctx, fk)
		assert.NoError(err)
		return fk, history
	}

	fk, history := newTree()
	got, err := readAcrossTheMove(t, e, fk, engineseams.CheckpointHistoryFlowRead, func() (any, error) { return e.History(ctx, fk) })
	assert.NoError(err)
	assert.Equal(history, got)

	for _, i := range []int{1, 0} { // Use resolves a ref; Seed holds the document itself
		fk, history := newTree()
		k := history[i].StepKey
		want, err := e.Step(ctx, k)
		assert.NoError(err)
		got, err := readAcrossTheMove(t, e, fk, engineseams.CheckpointStepRead, func() (any, error) { return e.Step(ctx, k) })
		assert.NoError(err, history[i].TaskName)
		assert.Equal(want, got, history[i].TaskName)
	}
}

// archivedDay returns the archive day holding the tree of the flow named by key.
func archivedDay(t *testing.T, e *Engine, flowKey string) flowTables {
	t.Helper()
	shard, hour, id, _, err := keys.ParseFlowKey(flowKey)
	if err != nil {
		t.Fatal(err)
	}
	day, err := e.archives.Load().byShard[shard].LocateFlow(t.Context(), id, hour)
	if err != nil {
		t.Fatal(err)
	}
	return flowTables{flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day)}
}

// TestArchivedReads_AnUnstoppedArchivedFlowIsAnError pins Snapshot on a flow the archive holds unstopped - an orphan
// the move took, which only a bug leaves: nothing will change it again, so it is an error, and an Await on it returns
// that error at once rather than waiting out its deadline. History still reads it.
func TestArchivedReads_AnUnstoppedArchivedFlowIsAnError(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	child := childKey(t, e, fk)
	e.reapDueFlows(ctx)
	tables := archivedDay(t, e, fk)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	_, _, childID, _, err := keys.ParseFlowKey(child)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "UPDATE "+tables.flows+" SET status='"+workflow.StatusRunning+"' WHERE flow_id=?", childID)
	assert.NoError(err)

	_, err = e.Snapshot(ctx, child)
	assert.Equal(http.StatusInternalServerError, merrors.StatusCode(err), err)
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	start := time.Now()
	_, err = e.Await(awaitCtx, child)
	assert.Equal(http.StatusInternalServerError, merrors.StatusCode(err), err)
	assert.True(time.Since(start) < 5*time.Second, "Await waited")
	_, err = e.History(ctx, child)
	assert.NoError(err)
	_, err = e.Snapshot(ctx, fk)
	assert.NoError(err, "the root stopped")
}

// TestArchivedReads_ADayDroppedMidReadIsNotFound pins a read whose archive day goes between its locate and its later
// statements, as a retention drop does: the read fails on the missing table, and is reported not found - the flow
// went with its day - rather than as the database's error. The day's tables are renamed away, as a drop would leave
// them. The read is held on its archive pass: the live pass meets the checkpoint first, on a tree already moved.
func TestArchivedReads_ADayDroppedMidReadIsNotFound(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	reads := map[string]func(e *Engine, flowKey, stepKey string) error{
		engineseams.CheckpointFingerprintFlowRead: func(e *Engine, fk, _ string) error { _, _, err := e.Fingerprint(ctx, fk); return err },
		engineseams.CheckpointHistoryFlowRead:     func(e *Engine, fk, _ string) error { _, err := e.History(ctx, fk); return err },
		engineseams.CheckpointStepRead:            func(e *Engine, _, sk string) error { _, err := e.Step(ctx, sk); return err },
	}
	for cp, read := range reads {
		t.Run(cp, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			e := archiveEngine(t, treeMux(), 30)
			db, err := e.db.Shard(1)
			assert.NoError(err)
			fk, err := e.Create(ctx, "arch/parent", nil, nil)
			assert.NoError(err)
			enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
			history, err := e.History(ctx, fk)
			assert.NoError(err)
			e.reapDueFlows(ctx)
			tables := archivedDay(t, e, fk)

			// Fingerprint and History meet their checkpoint only on the side that holds the flow row, and Step only
			// where the step is: here the archive.
			e.seams.Break(cp)
			done := make(chan error, 1)
			go func() { done <- read(e, fk, history[0].StepKey) }()
			assert.True(e.seams.WaitTimeout(ctx, cp, 10*time.Second), cp)
			for _, table := range []string{tables.flows, tables.steps} {
				_, err = db.ExecContext(ctx, "ALTER TABLE "+table+" RENAME TO "+table+"_away")
				assert.NoError(err)
			}
			e.seams.Resume(cp)
			err = <-done
			assert.Equal(http.StatusNotFound, merrors.StatusCode(err), "%s: %v", cp, err)
		})
	}
}

// TestArchivedReads_ContinueFromAnArchivedFlow pins that Continue reads its source from the archive as from the live
// tables: the new flow starts from the archived flow's final state, and runs in the live tables.
func TestArchivedReads_ContinueFromAnArchivedFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, endingMux(nil, nil), 30)
	fk, err := e.Create(ctx, "read/ok", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	e.reapDueFlows(ctx)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", rootID)
	assert.Zero(flows, "archived")

	next, err := e.Continue(ctx, fk, map[string]any{"more": 1})
	assert.NoError(err)
	out, err := e.Await(ctx, next)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal("ok", out.State.GetString("called"), "the archived flow's final state")
		assert.Equal(1, out.State.GetInt("more"))
	}
}

// TestArchivedReads_ADirectoryDayDroppedIsNotFound pins a read led to a day with no tables - a directory row naming a
// day dropped before retention deleted the row - for a read of one statement and one of several alike: the missing
// table is a 404, not the database's error.
func TestArchivedReads_ADirectoryDayDroppedIsNotFound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	e.reapDueFlows(ctx)
	tables := archivedDay(t, e, fk)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	// The tree out of the day its key names, and the directory pointing at a day long gone.
	_, err = db.ExecContext(ctx, "DELETE FROM "+tables.steps+" WHERE flow_id IN (SELECT flow_id FROM "+tables.flows+" WHERE root_flow_id=?)", rootID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "DELETE FROM "+tables.flows+" WHERE root_flow_id=?", rootID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "INSERT INTO "+archiver.DirectoryTable+" (flow_id, day) VALUES (?, 1)", rootID)
	assert.NoError(err)

	_, err = e.Snapshot(ctx, fk)
	assert.Equal(http.StatusNotFound, merrors.StatusCode(err), "Snapshot: %v", err)
	_, err = e.Continue(ctx, fk, nil)
	assert.Equal(http.StatusNotFound, merrors.StatusCode(err), "Continue: %v", err)
	_, err = e.History(ctx, fk)
	assert.Equal(http.StatusNotFound, merrors.StatusCode(err), "History: %v", err)
	_, _, err = e.Fingerprint(ctx, fk)
	assert.Equal(http.StatusNotFound, merrors.StatusCode(err), "Fingerprint: %v", err)
}

// TestArchivedReads_AnArchivedTreeRemovedMidReadIsNotFound pins the archive pass's closing read: a tree removed from
// its day between a read's statements is a 404, not what the read had gathered by then. Nothing removes a single
// archived tree yet; the rows are deleted by hand, as a delete of an archived tree will.
func TestArchivedReads_AnArchivedTreeRemovedMidReadIsNotFound(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 30)
	fk, err := e.Create(ctx, "arch/parent", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	e.reapDueFlows(ctx)
	tables := archivedDay(t, e, fk)
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	db, err := e.db.Shard(1)
	assert.NoError(err)

	e.seams.Break(engineseams.CheckpointHistoryFlowRead)
	done := make(chan error, 1)
	go func() {
		_, err := e.History(ctx, fk)
		done <- err
	}()
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointHistoryFlowRead, 10*time.Second))
	_, err = db.ExecContext(ctx, "DELETE FROM "+tables.steps+" WHERE flow_id IN (SELECT flow_id FROM "+tables.flows+" WHERE root_flow_id=?)", rootID)
	assert.NoError(err)
	_, err = db.ExecContext(ctx, "DELETE FROM "+tables.flows+" WHERE root_flow_id=?", rootID)
	assert.NoError(err)
	e.seams.Resume(engineseams.CheckpointHistoryFlowRead)
	err = <-done
	assert.Equal(http.StatusNotFound, merrors.StatusCode(err), err)
}

// nestedCallMux serves a root calling a child that calls a grandchild, each one task: a tree in which a subgraph
// child's subtree is more than the child.
func nestedCallMux() *hosts.Mux {
	mux := hosts.NewMux()
	for _, level := range []struct{ name, calls string }{{"top", "mid"}, {"mid", "leaf"}, {"leaf", ""}} {
		g := workflow.NewGraph(level.name)
		g.SetURL("Work", "nest/work/"+level.name)
		g.AddTransition("Work", workflow.End)
		mux.HandleGraph("nest/"+level.name, g)
		calls := level.calls
		mux.HandleTask("nest/work/"+level.name, func(ctx context.Context, f *workflow.Flow) error {
			if calls == "" {
				return f.Set("leaf", true)
			}
			yield, err := f.Subgraph("nest/"+calls, nil, nil)
			if yield || err != nil {
				return err
			}
			return nil
		})
	}
	return mux
}

// TestArchivedReads_AChildsFingerprintCoversItsSubtree pins Fingerprint of a subgraph child whose subtree has a
// grandchild: the same digest before and after the move, the descendant walk reading the day's flows table.
func TestArchivedReads_AChildsFingerprintCoversItsSubtree(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, nestedCallMux(), 30)
	fk, err := e.Create(ctx, "nest/top", nil, nil)
	assert.NoError(err)
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	child := childKey(t, e, fk)
	db, err := e.db.Shard(1)
	assert.NoError(err)
	// Without the grandchild's step the child's digest would count one step, as with no descendant walk at all.
	_, _, childID, _, err := keys.ParseFlowKey(child)
	assert.NoError(err)
	var grandchildren int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE surgraph_flow_id=?", childID).Scan(&grandchildren))
	assert.Equal(1, grandchildren)
	want, _, err := e.Fingerprint(ctx, child)
	assert.NoError(err)

	e.reapDueFlows(ctx)
	got, _, err := e.Fingerprint(ctx, child)
	assert.NoError(err)
	assert.Equal(want, got)
}
