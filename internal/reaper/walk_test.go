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
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// chunkTree seeds a closed tree of two flows and seven steps on db, closed a minute ago, and returns its root. A stamp
// is due delete_after_ms after the close, and a warm run reaches its next pass within that millisecond.
func chunkTree(t *testing.T, db *sequel.DB) int {
	t.Helper()
	root := seedTrees(t, db, 1, chunkShape)[0]
	closeTreesAgo(t, db, time.Minute, root)
	return root
}

// TestWalk_TheArchiveDayLookupKeepsToTheAttempt pins the attempt deadline on the archive-day lookup: with every
// connection held, the lookup gives up at the attempt, not at the caller's deadline.
func TestWalk_TheArchiveDayLookupKeepsToTheAttempt(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	db := r.db
	var err error
	db.DB.SetMaxOpenConns(1)
	held, err := db.DB.Conn(t.Context())
	assert.Must(assert.NoError(err))
	defer held.Close()
	r.cfg.Attempt = 20 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	start := time.Now()
	err = r.p().refreshDay(ctx, &filingDay{})
	assert.Error(err)
	assert.True(time.Since(start) < 2*time.Second, "bounded by the attempt: %v", time.Since(start))
}

// TestWalk_ABlockedAttemptIsAFailureAndAStopIsNot pins the attempt deadline on a bounded transaction, a tree's
// delete at a retention of 0: blocked inside its transaction - here held at a checkpoint, as a lock wait on SQL
// Server or SQLite holds it - it is abandoned at the deadline as a failure, and the tree stays whole; the same delete
// stopped by its caller's ctx is no failure.
func TestWalk_ABlockedAttemptIsAFailureAndAStopIsNot(t *testing.T) {
	t.Skip("janitor step 2: the fast walk of closed trees runs in the janitor; see internal/janitor TestWalk_AStopIsNotAFailure; the janitor has no attempt deadline")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	logs := r.logs
	r.cfg.Attempt = 200 * time.Millisecond
	root := chunkTree(t, r.db)

	r.seams.Break(CheckpointBatchLocked)
	defer r.seams.Resume(CheckpointBatchLocked)
	start := time.Now()
	r.pass(ctx)
	assert.True(time.Since(start) < 10*time.Second*timeoutScale, "the pass is bounded by the attempt")
	assert.Equal(1, logs.count("Reaper: removing due trees"), "an attempt that ran out is a failure")
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps}, "rolled back whole")

	// A fresh breakpoint, so the wait below is for this pass's arrival, not the last one's.
	r.seams.Resume(CheckpointBatchLocked)
	r.seams.Break(CheckpointBatchLocked)
	r.cfg.Attempt = time.Hour
	stop, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.pass(stop)
	}()
	assert.True(r.seams.WaitTimeout(ctx, CheckpointBatchLocked, 10*time.Second*timeoutScale))
	cancel()
	<-done
	assert.Equal(1, logs.count("Reaper: removing due trees"), "a stop is not a failure")
	flows, _ = treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal(2, flows)
}

// Poisoned deletes and failed sizing must not trap their healthy batch-mates in either population.
func TestWalk_PassesFailedTrees(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"closed", "deleted", "retention", "sizing"} {
		t.Run(kind, func(t *testing.T) {
			assert := testarossa.For(t)
			r := newRig(t, 30)
			db := r.db
			var err error
			roots := seedTrees(t, db, 7, callShape)
			if kind == "deleted" {
				_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET delete_after_ms=1, updated_at=DATE_ADD_MILLIS(updated_at,-60000) WHERE flow_id IN ("+intCSV(roots)+")")
				assert.NoError(err)
			}
			if kind == "retention" {
				for _, id := range roots {
					backdate(t, r, id, 40*24*time.Hour)
				}
				// No usable archive set: retention cleanup has to work independently.
				r.archive = nil
			}
			fault := FaultMidTree
			if kind == "sizing" {
				fault = FaultSizeErr
			}
			targeted := join(fault, strconv.Itoa(roots[2]))
			r.seams.InjectN(targeted, 10000)
			r.cfg.Batch = 4
			for range 3 {
				r.pass(t.Context())
			}
			for i, id := range roots {
				flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
				if i == 2 {
					assert.Equal(2, flows)
					assert.True(steps > 0)
				} else {
					assert.Zero(flows + steps)
				}
			}
			r.seams.Withdraw(targeted)
			r.pass(t.Context())
			flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", roots[2])
			assert.Zero(flows + steps)
		})
	}
}

func TestWalk_RepairWrapsAFrozenEndAndFindsLateRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 0)
	db := r.db
	var err error
	roots := seedTrees(t, db, 3, callShape)
	// Equal timestamps exercise the id tie-break across pages on every dialect.
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(),-60000) WHERE flow_id IN ("+intCSV(roots)+")")
	assert.NoError(err)
	// Keep an already-created root out of the first scan, then make its old close visible behind the position.
	late := seedTrees(t, db, 1, callShape)[0]
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(),3600000) WHERE flow_id=?", late)
	assert.NoError(err)
	r.cfg.Slice = 0
	r.cfg.Batch = 1
	r.seams.InjectN(FaultMidTree, 10000)
	kind := closedDue(0, r.cfg.Grace)
	day := func() (*filingDay, bool) { return nil, true }
	key := walkKey{1, "closed", true}
	r.p().walk(t.Context(), kind, day, ReasonNoRetention, true)
	frozen := r.reaper.walks.get(key).end
	later := seedTrees(t, db, 1, callShape)[0]
	// A new id is outside the frozen endpoint, even if the database clock moved backwards.
	backdate(t, r, later, 2*time.Minute)
	backdate(t, r, late, 2*time.Minute)
	for range 3 {
		r.p().walk(t.Context(), kind, day, ReasonNoRetention, true)
	}
	w := r.reaper.walks.get(key)
	assert.True(w.started.IsZero(), "the sweep reaches its frozen end despite the late root")
	assert.Equal(frozen, w.end)
	r.seams.Withdraw(FaultMidTree)
	for range 12 {
		r.p().walk(t.Context(), kind, day, ReasonNoRetention, true)
	}
	for _, id := range append(roots, late, later) {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows)
	}
}

func TestWalk_RetentionBoundaryAndHugeRetention(t *testing.T) {
	t.Parallel()
	for _, retention := range []int{1, math.MaxInt} {
		t.Run(strconv.Itoa(retention), func(t *testing.T) {
			assert := testarossa.For(t)
			r := newRig(t, retention)
			db := r.db
			var err error
			roots := seedTrees(t, db, 2, callShape)
			a := r.archive
			today, err := a.Today(t.Context())
			assert.NoError(err)
			edge := a.DayEnds(today - 2)
			for i, id := range roots {
				ts, arg := database.SQLTime(db.DriverName(), edge.Add(time.Duration(i-1)*time.Millisecond))
				_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at="+ts+" WHERE flow_id=?", arg, id)
				assert.NoError(err)
			}
			r.cfg.StealGrace = 0
			r.p().retained(t.Context())
			for i, id := range roots {
				flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
				if retention == 1 && i == 0 {
					assert.Zero(flows)
				} else {
					assert.Equal(2, flows)
				}
			}
		})
	}
}

// TestWalk_AnArchiveMoveAboveTheBatchBudgetIsAtomic pins that a tree to archive larger than a batch is still one atomic
// move, every descendant included: a fault mid-move leaves it whole and unstamped, and the next pass moves it.
func TestWalk_AnArchiveMoveAboveTheBatchBudgetIsAtomic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	db := r.db
	root := chunkTree(t, db)
	r.cfg.BatchRows = 2
	r.seams.InjectN(FaultMidTree, 1000)
	r.pass(t.Context())
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps})
	var stamp int
	assert.NoError(db.QueryRowContext(t.Context(), "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp))
	assert.Zero(stamp)
	r.seams.Withdraw(FaultMidTree)
	r.pass(t.Context())
	flows, steps = treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Zero(flows + steps)
	day, _ := r.archive.Today(t.Context())
	flows, steps = treeCount(t, r, migrations.FlowsTable(day), migrations.StepsTable(day), root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps})
}

// A representative qualification workload: 10,000 flow+step rows and about 16 MB of JSON, moved/deleted whole.
// This is a regression workload, not a hardware-independent latency guarantee or a substitute for load testing.
func TestWalk_TreeEnvelope(t *testing.T) {
	if testing.Short() {
		t.Skip("tree envelope workload")
	}
	for _, retention := range []int{0, 30} {
		t.Run(strconv.Itoa(retention), func(t *testing.T) {
			assert := testarossa.For(t)
			r := newRig(t, retention)
			db := r.db
			var err error
			root := chunkTree(t, db)
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
			var day *filingDay
			if retention > 0 {
				day = &filingDay{}
				assert.Must(assert.NoError(r.p().refreshDay(t.Context(), day)))
			}
			start := time.Now()
			cut, _, _, err := r.p().rootsWithin(t.Context(), []int{root}, false)
			assert.Must(assert.NoError(err))
			sized := time.Since(start)
			start = time.Now()
			n, err := r.p().batchOf(t.Context(), cut, closedDue(0, 0), day)
			assert.NoError(err)
			assert.Equal(2, n.flows)
			t.Logf("%s retention=%d: size=%s atomic transaction=%s, 10000 rows/~16MB JSON", db.DriverName(), retention, sized, time.Since(start))
			flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
			assert.Zero(flows + steps)
			if day != nil {
				flows, steps = treeCount(t, r, migrations.FlowsTable(day.day), migrations.StepsTable(day.day), root)
				assert.Equal(10000, flows+steps)
			}
		})
	}
}

func TestWalk_ArchiveMoveAlsoHasAnAttemptDeadline(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	db := r.db
	var err error
	root := chunkTree(t, db)
	day := &filingDay{}
	assert.Must(assert.NoError(r.p().refreshDay(t.Context(), day)))
	r.cfg.Attempt = 50 * time.Millisecond
	r.seams.Break(CheckpointBatchLocked)
	defer r.seams.Resume(CheckpointBatchLocked)
	start := time.Now()
	_, err = r.p().batchOf(t.Context(), []int{root}, closedDue(0, 0), day)
	assert.Error(err)
	assert.True(time.Since(start) < 5*time.Second)
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps})
	flows, steps = treeCount(t, r, migrations.FlowsTable(day.day), migrations.StepsTable(day.day), root)
	assert.Zero(flows + steps)
}

// TestWalk_FastPassesTheBatchItAttempted pins the fast walker's advance: a batch whose move fails, or whose size
// cannot be counted, is passed - left to repair - and the batches behind it are reached in the same slice. A fast
// walker that held its position would retry the failed batch until the slice gave up, and reach nothing behind it.
func TestWalk_FastPassesTheBatchItAttempted(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{FaultMidTree, FaultSizeErr} {
		t.Run(fault, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 0)
			db := r.db
			roots := seedTrees(t, db, 4, callShape)
			ascendingCloses(t, r, roots)
			r.cfg.Batch = 2
			r.cfg.Slice = time.Hour
			targeted := join(fault, strconv.Itoa(roots[0]))
			r.seams.InjectN(targeted, 10000)
			defer r.seams.Withdraw(targeted)

			r.p().walk(ctx, closedDue(0, r.cfg.Grace), func() (*filingDay, bool) { return nil, true }, ReasonNoRetention, false)
			for i, id := range roots {
				flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
				if i < 2 {
					assert.Equal(2, flows, "the failed batch is left to repair: root %d", i)
				} else {
					assert.Zero(flows, "the batch behind it is reached: root %d", i)
				}
			}
			assert.Equal(roots[3], r.reaper.walks.get(walkKey{1, "closed", false}).after.flowID)
		})
	}
}

// TestWalk_RepairRetriesAFailedBatchFromItsFirstRoot pins repair's step sequence, one attempt per slice: a removal
// doubles the batch; a failed batch of several does NOT advance and drops to one, so its first root is retried alone;
// a failed single advances, counted as held.
func TestWalk_RepairRetriesAFailedBatchFromItsFirstRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	db := r.db
	roots := seedTrees(t, db, 4, callShape)
	ascendingCloses(t, r, roots)
	r.cfg.Slice = 0 // one attempt per slice
	targeted := join(FaultMidTree, strconv.Itoa(roots[2]))
	r.seams.InjectN(targeted, 10000)
	defer r.seams.Withdraw(targeted)
	key := walkKey{1, "closed", true}
	step := func() walk {
		r.p().walk(ctx, closedDue(0, r.cfg.Grace), func() (*filingDay, bool) { return nil, true }, ReasonNoRetention, true)
		return r.reaper.walks.get(key)
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
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		if i == 2 {
			assert.Equal(2, flows)
		} else {
			assert.Zero(flows, "root %d", i)
		}
	}
}

// TestWalk_RetentionCountsFromTheCloseAndTheGrace pins the retention cutoff's grace term and its recheck under
// the root lock: an unarchived root expires with the day it would have been filed in, its close plus the archive
// grace, so a root whose grace runs past the edge of the expiring day is kept; and the lock refuses a root that is
// not past the cutoff whatever a scan saw.
func TestWalk_RetentionCountsFromTheCloseAndTheGrace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 1)
	db := r.db
	var err error
	roots := seedTrees(t, db, 2, callShape)
	a := r.archive
	today, err := a.Today(ctx)
	assert.Must(assert.NoError(err))
	grace := a.DayLength() / 4
	edge := a.DayEnds(today - 2) // the last day a retention of 1 drops
	closeAt := func(id int, at time.Time) {
		ts, arg := database.SQLTime(db.DriverName(), at)
		_, err := db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at="+ts+" WHERE flow_id=?", arg, id)
		assert.NoError(err)
	}
	closeAt(roots[0], edge.Add(-grace-grace/2)) // filed within the expiring day
	closeAt(roots[1], edge.Add(-grace/2))       // closed within it, filed in the next

	r.cfg.StealGrace, r.cfg.Grace = 0, grace
	r.p().retained(ctx)
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", roots[0])
	assert.Zero(flows, "past the retention counted from its filing day")
	flows, _ = treeCount(t, r, "dwarf_flows", "dwarf_steps", roots[1])
	assert.Equal(2, flows, "its grace runs past the edge: not yet")

	removed, err := r.p().batchOf(ctx, []int{roots[1]}, retentionDue(db.DriverName(), edge.Add(-grace), 0, grace), nil)
	assert.NoError(err)
	assert.Zero(removed, "the lock rechecks the cutoff")
	flows, _ = treeCount(t, r, "dwarf_flows", "dwarf_steps", roots[1])
	assert.Equal(2, flows)
}

// TestWalk_TheSweepEndIncludesTheNewestRoot pins the inclusive sweep end: when the newest flow is itself a root (a
// tree of one flow), the sweep's frozen end names it, and repair reaches it. An exclusive end would skip it on every
// sweep for as long as no newer flow is created.
func TestWalk_TheSweepEndIncludesTheNewestRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	db := r.db
	roots := seedTrees(t, db, 2, soloShape)
	ascendingCloses(t, r, roots)
	var newest int
	assert.NoError(db.QueryRowContext(ctx, "SELECT MAX(flow_id) FROM dwarf_flows").Scan(&newest))
	assert.Equal(roots[1], newest, "the newest flow is a root")

	r.cfg.Slice = time.Hour
	r.p().walk(ctx, closedDue(0, r.cfg.Grace), func() (*filingDay, bool) { return nil, true }, ReasonNoRetention, true)
	assert.True(r.reaper.walks.get(walkKey{1, "closed", true}).started.IsZero(), "the sweep completed")
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows)
	}
}

// TestWalk_TheStampedScanWalksTheStampedIndex pins the stamped scan's plan to idx_dwarf_flows_delete_after - keyed
// on flow_id over the stamped roots alone on PostgreSQL and SQLite, named with FORCE INDEX on MySQL and MariaDB.
// Without it the scan walks the primary key past every unstamped root ahead of the first stamped one: an archive
// outage's whole held backlog, on every sweep from the front.
func TestWalk_TheStampedScanWalksTheStampedIndex(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	var err error
	var explain, priceOutScans string
	switch db.DriverName() {
	case "pgx":
		// An empty table's estimates favour a scan or a bitmap read and sort whatever the index; priced out, the plan
		// shows whether the index itself is in the scan's order.
		explain, priceOutScans = "EXPLAIN ", "SET LOCAL enable_seqscan = off; SET LOCAL enable_bitmapscan = off"
	case "sqlite":
		explain = "EXPLAIN QUERY PLAN "
	case "mysql", "mariadb":
		explain = "EXPLAIN "
	default:
		t.Skip("the plan shapes pinned here are PostgreSQL's, SQLite's, MySQL's and MariaDB's")
	}
	stmt, args := deletionDue(0).selectDue(db.DriverName(), "", nil, 512)
	var plan strings.Builder
	errRollback := errors.New("rollback")
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		for _, set := range strings.Split(priceOutScans, "; ") {
			if set == "" {
				continue
			}
			if _, err := tx.ExecContext(ctx, set); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, explain+stmt, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return err
			}
			for _, v := range vals {
				if b, ok := v.([]byte); ok {
					v = string(b)
				}
				fmt.Fprint(&plan, v, " ")
			}
			plan.WriteString("\n")
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return errRollback
	})
	assert.True(errors.Is(err, errRollback), "%v", err)
	assert.Contains(plan.String(), "idx_dwarf_flows_delete_after", "%s", plan.String())
	if d := db.DriverName(); d == "pgx" || d == "sqlite" {
		// Keyed on flow_id the index is already in the scan's order; keyed on anything else it needs a sort of every
		// stamped root on each scan, which is what an empty table's plan shows.
		assert.False(strings.Contains(plan.String(), "Sort") || strings.Contains(plan.String(), "TEMP B-TREE"),
			"no sort: %s", plan.String())
	}
}

// TestWalk_EveryFlowAndStepIsARow pins the sizing of a tree: every flow row and every step row counts as one, a flow of
// no steps included, and the move takes the tree whole.
func TestWalk_EveryFlowAndStepIsARow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	db := r.db
	var err error
	roots := seedTrees(t, db, 2, treeShape{rootSteps: 2, childSteps: 0})
	r.cfg.BatchRows = 7
	cut, _, _, err := r.p().rootsWithin(ctx, roots, false)
	assert.NoError(err)
	assert.Equal(roots[:1], cut, "four rows a tree: the root, its two steps and the child of none")
	r.cfg.BatchRows = 8
	cut, _, _, err = r.p().rootsWithin(ctx, roots, false)
	assert.NoError(err)
	assert.Equal(roots, cut)

	day := &filingDay{}
	assert.Must(assert.NoError(r.p().refreshDay(ctx, day)))
	moved, err := r.p().batchOf(ctx, cut, closedDue(0, r.cfg.Grace), day)
	assert.NoError(err)
	assert.Equal(4, moved.flows)
	for _, root := range roots {
		flows, steps := treeCount(t, r, migrations.FlowsTable(day.day), migrations.StepsTable(day.day), root)
		assert.Equal([2]int{2, 2}, [2]int{flows, steps})
	}
}

// TestWalk_AStopIsNotAFailure pins an edge of the failure timer: a failure met while the pass is being stopped is not
// recorded. A restart forgets the failures because the engine builds a new Reaper, which TestReaperContract_
// ARestartForgetsTheWalksAndTheLag pins in the engine.
func TestWalk_AStopIsNotAFailure(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	stopped, cancel := context.WithCancel(t.Context())
	cancel()
	r.p().archiveFailed(stopped, "archiving closed trees", errors.New("stopped"))
	assert.Len(r.reaper.failures.byID, 0, "a stop records no failure")
	r.p().archiveFailed(t.Context(), "archiving closed trees", errors.New("failed"))
	assert.Len(r.reaper.failures.byID, 1)
}

// TestWalk_TheResidencyCallbackRestrictsAPass pins that a pass reads the shard's Residency: with two dispatchers, a
// pass takes only its own residue class of the fresh roots, and the other class's once nothing restricts it.
func TestWalk_TheResidencyCallbackRestrictsAPass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	roots := seedTrees(t, r.db, 4, soloShape)
	calls := 0
	shard := r.shard()
	shard.Residency = func() (count, ordinal int, ok bool) {
		calls++
		return 2, 0, true
	}
	r.reaper.Pass(ctx, r.cfg, shard)
	assert.True(calls > 0, "the walks read the residency")
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		if id%2 == 0 {
			assert.Zero(flows, "root %d is in this dispatcher's class", id)
		} else {
			assert.Equal(1, flows, "root %d is the other's, and fresh", id)
		}
	}
	r.pass(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows, "root %d", id)
	}
}
