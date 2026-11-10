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
	"github.com/microbus-io/dwarf/internal/migrations"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// chunkMux serves a root of five steps whose last calls a child of two: a tree of two flows and seven step rows.
func chunkMux() *hosts.Mux {
	mux := hosts.NewMux()
	root := workflow.NewGraph("ChunkRoot")
	for _, task := range []string{"A", "B", "C", "D", "Call"} {
		root.SetURL(task, "chunk/"+task)
	}
	root.AddTransition("A", "B")
	root.AddTransition("B", "C")
	root.AddTransition("C", "D")
	root.AddTransition("D", "Call")
	root.AddTransition("Call", workflow.End)
	mux.HandleGraph("chunk/root", root)
	child := workflow.NewGraph("ChunkChild")
	child.SetURL("X", "chunk/X")
	child.SetURL("Y", "chunk/Y")
	child.AddTransition("X", "Y")
	child.AddTransition("Y", workflow.End)
	mux.HandleGraph("chunk/child", child)
	for _, task := range []string{"A", "B", "C", "D", "X", "Y"} {
		mux.HandleTask("chunk/"+task, func(ctx context.Context, f *workflow.Flow) error { return nil })
	}
	mux.HandleTask("chunk/Call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("chunk/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	return mux
}

// chunkTree runs a chunkMux root to completion, moves its close a minute into the past, and returns its key and flow
// id. A stamp is due delete_after_ms after the close, and a warm run reaches its next pass within that millisecond.
func chunkTree(t *testing.T, e *Engine) (string, int) {
	t.Helper()
	fk, err := e.Create(t.Context(), "chunk/root", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
	_, _, id, _, _ := keys.ParseFlowKey(fk)
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(updated_at, ?) WHERE root_flow_id=?",
		-time.Minute.Milliseconds(), id); err != nil {
		t.Fatal(err)
	}
	return fk, id
}

// TestReapWalk_TheArchiveDayLookupKeepsToTheAttempt pins the attempt deadline on the archive-day lookup: with every
// connection held, the lookup gives up at the attempt, not at the caller's deadline.
func TestReapWalk_TheArchiveDayLookupKeepsToTheAttempt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := archiveEngine(t, chunkMux(), 30)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db.DB.SetMaxOpenConns(1)
	held, err := db.DB.Conn(t.Context())
	assert.Must(assert.NoError(err))
	defer held.Close()
	e.reapAttempt = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = e.refreshReapDay(ctx, e.archives.Load().byShard[1], &reapDay{})
	assert.Error(err)
	assert.True(time.Since(start) < 2*time.Second, "bounded by the attempt: %v", time.Since(start))
}

// TestReapWalk_ABlockedAttemptIsAFailureAndAStopIsNot pins the attempt deadline on a bounded transaction, a tree's
// delete at a retention of 0: blocked inside its transaction - here held at a checkpoint, as a lock wait on SQL
// Server or SQLite holds it - it is abandoned at the deadline as a failure, and the tree stays whole; the same delete
// stopped by the engine's ctx is no failure.
func TestReapWalk_ABlockedAttemptIsAFailureAndAStopIsNot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	logs := &logCounter{seen: map[string]int{}}
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(chunkMux())
	assert.Must(assert.NoError(e.SetLogger(slog.New(logs))))
	e.archiveGrace = 0
	e.reapInterval = time.Hour
	assert.Must(assert.NoError(e.SetRetention(0)))
	assert.Must(assert.NoError(e.Startup(ctx)))
	enginetest.AwaitVisitCount(t, e.seams, 1, 10*time.Second, engineseams.CheckpointArchivePassDone)
	e.reapAttempt = 200 * time.Millisecond
	_, root := chunkTree(t, e)

	e.seams.Break(engineseams.CheckpointReapBatchLocked)
	defer e.seams.Resume(engineseams.CheckpointReapBatchLocked)
	start := time.Now()
	e.reapDueFlows(ctx)
	assert.True(time.Since(start) < 10*time.Second*enginetest.TimeoutScale(), "the pass is bounded by the attempt")
	assert.Equal(1, logs.count("Reaper: removing due trees"), "an attempt that ran out is a failure")
	flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps}, "rolled back whole")

	// A fresh breakpoint, so the wait below is for this pass's arrival, not the last one's.
	e.seams.Resume(engineseams.CheckpointReapBatchLocked)
	e.seams.Break(engineseams.CheckpointReapBatchLocked)
	e.reapAttempt = time.Hour
	stop, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		e.reapDueFlows(stop)
	}()
	assert.True(e.seams.WaitTimeout(ctx, engineseams.CheckpointReapBatchLocked, 10*time.Second*enginetest.TimeoutScale()))
	cancel()
	<-done
	assert.Equal(1, logs.count("Reaper: removing due trees"), "a stop is not a failure")
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
	assert.Equal(2, flows)
}

// Poisoned deletes and failed sizing must not trap their healthy batch-mates in either population.
func TestReapWalk_PassesFailedTrees(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"closed", "deleted", "retention", "sizing"} {
		t.Run(kind, func(t *testing.T) {
			assert := testarossa.For(t)
			e := archiveEngine(t, treeMux(), 30)
			roots := closedRoots(t, e, 7)
			db, err := e.db.Shard(1)
			assert.Must(assert.NoError(err))
			if kind == "deleted" {
				_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET delete_after_ms=1, updated_at=DATE_ADD_MILLIS(updated_at,-60000) WHERE flow_id IN ("+intCSV(roots)+")")
				assert.NoError(err)
			}
			if kind == "retention" {
				for _, id := range roots {
					backdate(t, e, id, 40*24*time.Hour)
				}
				// No usable archive set: retention cleanup has to work independently.
				e.archives.Store(nil)
			}
			fault := engineseams.FaultReapMidTree
			if kind == "sizing" {
				fault = engineseams.FaultReapSizeErr
			}
			targeted := engineseams.Join(fault, strconv.Itoa(roots[2]))
			e.seams.InjectN(targeted, 10000)
			e.reapBatch = 4
			for range 3 {
				e.reapDueFlows(t.Context())
			}
			for i, id := range roots {
				flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
				if i == 2 {
					assert.Equal(2, flows)
					assert.True(steps > 0)
				} else {
					assert.Zero(flows + steps)
				}
			}
			e.seams.Withdraw(targeted)
			e.reapDueFlows(t.Context())
			flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", roots[2])
			assert.Zero(flows + steps)
		})
	}
}

func TestReapWalk_RepairWrapsAFrozenEndAndFindsLateRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := archiveEngine(t, treeMux(), 0)
	roots := closedRoots(t, e, 3)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	// Equal timestamps exercise the id tie-break across pages on every dialect.
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(),-60000) WHERE flow_id IN ("+intCSV(roots)+")")
	assert.NoError(err)
	// Keep an already-created root out of the first scan, then make its old close visible behind the position.
	late := closedRoots(t, e, 1)[0]
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(),3600000) WHERE flow_id=?", late)
	assert.NoError(err)
	e.reapSlice = 0
	e.reapBatch = 1
	e.seams.InjectN(engineseams.FaultReapMidTree, 10000)
	kind := e.closedDue(0)
	day := func() (*reapDay, bool) { return nil, true }
	key := reapWalkKey{1, "closed", true}
	e.reapWalk(t.Context(), db, 1, kind, day, reapReasonNoRetention, true)
	frozen := e.reapWalks.get(key).end
	later := closedRoots(t, e, 1)[0]
	// A new id is outside the frozen endpoint, even if the database clock moved backwards.
	backdate(t, e, later, 2*time.Minute)
	backdate(t, e, late, 2*time.Minute)
	for range 3 {
		e.reapWalk(t.Context(), db, 1, kind, day, reapReasonNoRetention, true)
	}
	w := e.reapWalks.get(key)
	assert.True(w.started.IsZero(), "the sweep reaches its frozen end despite the late root")
	assert.Equal(frozen, w.end)
	e.seams.Withdraw(engineseams.FaultReapMidTree)
	for range 12 {
		e.reapWalk(t.Context(), db, 1, kind, day, reapReasonNoRetention, true)
	}
	for _, id := range append(roots, late, later) {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows)
	}
}

func TestReapWalk_RetentionBoundaryAndHugeRetention(t *testing.T) {
	t.Parallel()
	for _, retention := range []int{1, math.MaxInt} {
		t.Run(strconv.Itoa(retention), func(t *testing.T) {
			assert := testarossa.For(t)
			e := archiveEngine(t, treeMux(), retention)
			roots := closedRoots(t, e, 2)
			a := e.archives.Load().byShard[1]
			today, err := a.Today(t.Context())
			assert.NoError(err)
			db, err := e.db.Shard(1)
			assert.Must(assert.NoError(err))
			edge := a.DayEnds(today - 2)
			for i, id := range roots {
				ts, arg := sqlTime(db.DriverName(), edge.Add(time.Duration(i-1)*time.Millisecond))
				_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at="+ts+" WHERE flow_id=?", arg, id)
				assert.NoError(err)
			}
			e.reapRetained(t.Context(), db, 1, e.archives.Load(), 0, 0)
			for i, id := range roots {
				flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
				if retention == 1 && i == 0 {
					assert.Zero(flows)
				} else {
					assert.Equal(2, flows)
				}
			}
		})
	}
}

func TestReapWalk_AtomicTreeAboveBatchBudget(t *testing.T) {
	t.Parallel()
	for _, retention := range []int{0, 30} {
		t.Run(strconv.Itoa(retention), func(t *testing.T) {
			assert := testarossa.For(t)
			e := archiveEngine(t, chunkMux(), retention)
			_, root := chunkTree(t, e)
			e.reapBatchRows = 2
			db, err := e.db.Shard(1)
			assert.Must(assert.NoError(err))
			// A tree larger than a batch is still one atomic unit, including every descendant.
			e.seams.InjectN(engineseams.FaultReapMidTree, 1000)
			e.reapDueFlows(t.Context())
			flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
			assert.Equal([2]int{2, 7}, [2]int{flows, steps})
			var stamp int
			assert.NoError(db.QueryRowContext(t.Context(), "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp))
			assert.Zero(stamp)
			e.seams.Withdraw(engineseams.FaultReapMidTree)
			e.reapDueFlows(t.Context())
			flows, steps = treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
			assert.Zero(flows + steps)
			if retention > 0 {
				day, _ := e.archives.Load().byShard[1].Today(t.Context())
				flows, steps = treeCount(t, e, migrations.FlowsTable(day), migrations.StepsTable(day), root)
				assert.Equal([2]int{2, 7}, [2]int{flows, steps})
			}
		})
	}
}

// A representative qualification workload: 10,000 flow+step rows and about 16 MB of JSON, moved/deleted whole.
// This is a regression workload, not a hardware-independent latency guarantee or a substitute for load testing.
func TestReapWalk_TreeEnvelope(t *testing.T) {
	if testing.Short() {
		t.Skip("tree envelope workload")
	}
	for _, retention := range []int{0, 30} {
		t.Run(strconv.Itoa(retention), func(t *testing.T) {
			assert := testarossa.For(t)
			e := archiveEngine(t, chunkMux(), retention)
			_, root := chunkTree(t, e)
			db, err := e.db.Shard(1)
			assert.Must(assert.NoError(err))
			payload := `'{"p":"` + strings.Repeat("x", 1600) + `"}'`
			if db.DriverName() == "mssql" {
				payload = "CONVERT(VARBINARY(MAX)," + payload + ")"
			}
			row := "(" + strconv.Itoa(root) + ",1,'envelope','Envelope','envelope','completed',0,1," + payload + ")"
			for left := 9991; left > 0; {
				n := min(left, 200)
				_, err = db.ExecContext(t.Context(), "INSERT INTO dwarf_steps (flow_id,step_depth,step_token,task_name,task_url,status,time_budget_ms,flow_created_hour,state) VALUES "+strings.TrimSuffix(strings.Repeat(row+",", n), ","))
				assert.Must(assert.NoError(err))
				left -= n
			}
			var day *reapDay
			if retention > 0 {
				day = &reapDay{}
				assert.Must(assert.NoError(e.refreshReapDay(t.Context(), e.archives.Load().byShard[1], day)))
			}
			start := time.Now()
			cut, err := e.rootsWithinRows(t.Context(), db, []int{root})
			assert.Must(assert.NoError(err))
			sized := time.Since(start)
			start = time.Now()
			n, err := e.reapBatchOf(t.Context(), db, cut, e.closedDueAfter(0, 0), day)
			assert.NoError(err)
			assert.Equal(2, n)
			t.Logf("%s retention=%d: size=%s atomic transaction=%s, 10000 rows/~16MB JSON", db.DriverName(), retention, sized, time.Since(start))
			flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
			assert.Zero(flows + steps)
			if day != nil {
				flows, steps = treeCount(t, e, migrations.FlowsTable(day.day), migrations.StepsTable(day.day), root)
				assert.Equal(10000, flows+steps)
			}
		})
	}
}

func TestReapWalk_ArchiveMoveAlsoHasAnAttemptDeadline(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := archiveEngine(t, chunkMux(), 30)
	_, root := chunkTree(t, e)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	day := &reapDay{}
	assert.Must(assert.NoError(e.refreshReapDay(t.Context(), e.archives.Load().byShard[1], day)))
	e.reapAttempt = 50 * time.Millisecond
	e.seams.Break(engineseams.CheckpointReapBatchLocked)
	defer e.seams.Resume(engineseams.CheckpointReapBatchLocked)
	start := time.Now()
	_, err = e.reapBatchOf(t.Context(), db, []int{root}, e.closedDueAfter(0, 0), day)
	assert.Error(err)
	assert.True(time.Since(start) < 5*time.Second)
	flows, steps := treeCount(t, e, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps})
	flows, steps = treeCount(t, e, migrations.FlowsTable(day.day), migrations.StepsTable(day.day), root)
	assert.Zero(flows + steps)
}

// ascendingCloses gives roots closes in their order, a minute apart, all past any grace, so a walk meets them in order.
func ascendingCloses(t *testing.T, e *Engine, roots []int) {
	t.Helper()
	for i, id := range roots {
		backdate(t, e, id, time.Duration(len(roots)+10-i)*time.Minute)
	}
}

// TestReapWalk_FastPassesTheBatchItAttempted pins the fast walker's advance: a batch whose move fails, or whose size
// cannot be counted, is passed - left to repair - and the batches behind it are reached in the same slice. A fast
// walker that held its position would retry the failed batch until the slice gave up, and reach nothing behind it.
func TestReapWalk_FastPassesTheBatchItAttempted(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{engineseams.FaultReapMidTree, engineseams.FaultReapSizeErr} {
		t.Run(fault, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			e := archiveEngine(t, treeMux(), 0)
			roots := closedRoots(t, e, 4)
			ascendingCloses(t, e, roots)
			db, err := e.db.Shard(1)
			assert.Must(assert.NoError(err))
			e.reapBatch = 2
			e.reapSlice = time.Hour
			targeted := engineseams.Join(fault, strconv.Itoa(roots[0]))
			e.seams.InjectN(targeted, 10000)
			defer e.seams.Withdraw(targeted)

			e.reapWalk(ctx, db, 1, e.closedDue(0), func() (*reapDay, bool) { return nil, true }, reapReasonNoRetention, false)
			for i, id := range roots {
				flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
				if i < 2 {
					assert.Equal(2, flows, "the failed batch is left to repair: root %d", i)
				} else {
					assert.Zero(flows, "the batch behind it is reached: root %d", i)
				}
			}
			assert.Equal(roots[3], e.reapWalks.get(reapWalkKey{1, "closed", false}).after.flowID)
		})
	}
}

// TestReapWalk_RepairRetriesAFailedBatchFromItsFirstRoot pins repair's step sequence, one attempt per slice: a removal
// doubles the batch; a failed batch of several does NOT advance and drops to one, so its first root is retried alone;
// a failed single advances, counted as held.
func TestReapWalk_RepairRetriesAFailedBatchFromItsFirstRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 0)
	roots := closedRoots(t, e, 4)
	ascendingCloses(t, e, roots)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	e.reapSlice = 0 // one attempt per slice
	targeted := engineseams.Join(engineseams.FaultReapMidTree, strconv.Itoa(roots[2]))
	e.seams.InjectN(targeted, 10000)
	defer e.seams.Withdraw(targeted)
	key := reapWalkKey{1, "closed", true}
	step := func() reapWalk {
		e.reapWalk(ctx, db, 1, e.closedDue(0), func() (*reapDay, bool) { return nil, true }, reapReasonNoRetention, true)
		return e.reapWalks.get(key)
	}

	w := step() // [r0] removed
	assert.Equal([2]int{roots[0], 2}, [2]int{w.after.flowID, w.batch})
	w = step() // [r1 r2] fails
	assert.Equal([2]int{roots[0], 1}, [2]int{w.after.flowID, w.batch}, "a failed batch of several does not advance")
	w = step() // [r1] removed
	assert.Equal([2]int{roots[1], 2}, [2]int{w.after.flowID, w.batch})
	w = step() // [r2 r3] fails
	assert.Equal([2]int{roots[1], 1}, [2]int{w.after.flowID, w.batch})
	w = step() // [r2] fails alone
	assert.Equal([2]int{roots[2], 1}, [2]int{w.after.flowID, w.failed}, "a failed single advances, held")
	step() // [r3] removed
	for i, id := range roots {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		if i == 2 {
			assert.Equal(2, flows)
		} else {
			assert.Zero(flows, "root %d", i)
		}
	}
}

// TestReapWalk_RetentionCountsFromTheCloseAndTheGrace pins the retention cutoff's grace term and its recheck under
// the root lock: an unarchived root expires with the day it would have been filed in, its close plus the archive
// grace, so a root whose grace runs past the edge of the expiring day is kept; and the lock refuses a root that is
// not past the cutoff whatever a scan saw.
func TestReapWalk_RetentionCountsFromTheCloseAndTheGrace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 1)
	roots := closedRoots(t, e, 2)
	set := e.archives.Load()
	a := set.byShard[1]
	today, err := a.Today(ctx)
	assert.Must(assert.NoError(err))
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	grace := a.DayLength() / 4
	edge := a.DayEnds(today - 2) // the last day a retention of 1 drops
	closeAt := func(id int, at time.Time) {
		ts, arg := sqlTime(db.DriverName(), at)
		_, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at="+ts+" WHERE flow_id=?", arg, id)
		assert.NoError(err)
	}
	closeAt(roots[0], edge.Add(-grace-grace/2)) // filed within the expiring day
	closeAt(roots[1], edge.Add(-grace/2))       // closed within it, filed in the next

	e.reapRetained(ctx, db, 1, set, 0, grace)
	flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", roots[0])
	assert.Zero(flows, "past the retention counted from its filing day")
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", roots[1])
	assert.Equal(2, flows, "its grace runs past the edge: not yet")

	removed, err := e.reapBatchOf(ctx, db, []int{roots[1]}, e.retentionDue(db.DriverName(), edge.Add(-grace), 0, grace), nil)
	assert.NoError(err)
	assert.Zero(removed, "the lock rechecks the cutoff")
	flows, _ = treeCount(t, e, "dwarf_flows", "dwarf_steps", roots[1])
	assert.Equal(2, flows)
}

// TestReapWalk_TheSweepEndIncludesTheNewestRoot pins the inclusive sweep end: when the newest flow is itself a root (a
// tree of one flow), the sweep's frozen end names it, and repair reaches it. An exclusive end would skip it on every
// sweep for as long as no newer flow is created.
func TestReapWalk_TheSweepEndIncludesTheNewestRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e := archiveEngine(t, treeMux(), 0)
	var roots []int
	for range 2 {
		fk, err := e.Create(ctx, "arch/solo", nil, nil)
		assert.Must(assert.NoError(err))
		enginetest.AwaitFlowStatus(t, e, fk, workflow.StatusCompleted, time.Minute)
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		roots = append(roots, id)
	}
	ascendingCloses(t, e, roots)
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	var newest int
	assert.NoError(db.QueryRowContext(ctx, "SELECT MAX(flow_id) FROM dwarf_flows").Scan(&newest))
	assert.Equal(roots[1], newest, "the newest flow is a root")

	e.reapSlice = time.Hour
	e.reapWalk(ctx, db, 1, e.closedDue(0), func() (*reapDay, bool) { return nil, true }, reapReasonNoRetention, true)
	assert.True(e.reapWalks.get(reapWalkKey{1, "closed", true}).started.IsZero(), "the sweep completed")
	for _, id := range roots {
		flows, _ := treeCount(t, e, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows)
	}
}
