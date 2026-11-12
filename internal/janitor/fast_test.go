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
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestWalk_AStopIsNotAFailure pins that a round stopped by its context records no failure: a delete at a retention of
// 0 held inside its transaction - as a lock wait on SQL Server or SQLite holds it - and stopped there is rolled back
// whole and logs nothing; and the failure timer ignores a failure met while stopping.
func TestWalk_AStopIsNotAFailure(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	logs := r.logs
	root := chunkTree(t, r.db)

	r.seams.Break(diagnostics.CheckpointReapBatchLocked)
	defer r.seams.Resume(diagnostics.CheckpointReapBatchLocked)
	stop, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.pass(stop)
	}()
	assert.True(r.seams.WaitTimeout(ctx, diagnostics.CheckpointReapBatchLocked, 10*time.Second*timeoutScale))
	cancel()
	<-done
	assert.Zero(logs.count("Janitor: removing closed trees"), "a stop is not a failure")
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps}, "rolled back whole")

	r2 := newRig(t, 30)
	stopped, cancel2 := context.WithCancel(t.Context())
	cancel2()
	r2.fast().failed(stopped, "archiving closed trees", errors.New("stopped"))
	assert.Zero(r2.failing(time.Now().Add(time.Second)), "a stop records no failure")
	r2.fast().failed(t.Context(), "archiving closed trees", errors.New("failed"))
	assert.True(r2.failing(time.Now().Add(time.Second)) > 0)
}

// TestWalk_AnArchiveMoveAboveTheBatchBudgetIsAtomic pins that a tree to archive larger than a batch is still one atomic
// move, every descendant included: a fault mid-move leaves it whole and unstamped, and a janitor starting over moves
// it.
func TestWalk_AnArchiveMoveAboveTheBatchBudgetIsAtomic(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	db := r.db
	root := chunkTree(t, db)
	r.settings.batchRows = 2
	r.seams.InjectN(diagnostics.FaultReapMidTree, 1000)
	r.pass(t.Context())
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps})
	var stamp int
	assert.NoError(db.QueryRowContext(t.Context(), "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp))
	assert.Zero(stamp)
	r.seams.Withdraw(diagnostics.FaultReapMidTree)
	r.restart()
	r.pass(t.Context())
	flows, steps = treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Zero(flows + steps)
	day, _ := r.archive.Today(t.Context())
	flows, steps = treeCount(t, r, migrations.FlowsTable(day), migrations.StepsTable(day), root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps})
}

// TestWalk_TreeEnvelope is a representative qualification workload: 10,000 flow and step rows and about 16 MB of JSON,
// moved or deleted whole. It is a regression workload, not a hardware-independent latency guarantee or a substitute
// for load testing.
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
				assert.Must(assert.NoError(refreshDay(t.Context(), db, r.archive, true, day)))
			}
			start := time.Now()
			cut, _, err := r.fast().cut(t.Context(), db, r.settings, []int{root}, retention > 0)
			assert.Must(assert.NoError(err))
			sized := time.Since(start)
			start = time.Now()
			due, dueArgs := closedDueSQL(0)
			n, err := move(t.Context(), db, r.seams, cut, due, dueArgs, day)
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

// TestWalk_FastPassesTheBatchItAttempted pins fast archiving's advance: a batch whose move fails, or whose size cannot
// be counted, is passed - left behind the position - and the batches after it are reached in the next round. A job that
// held its position would retry the failed batch round after round, and reach nothing behind it.
func TestWalk_FastPassesTheBatchItAttempted(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{diagnostics.FaultReapMidTree, diagnostics.FaultReapSizeErr} {
		t.Run(fault, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 0)
			db := r.db
			roots := seedTrees(t, db, 4, callShape)
			ascendingCloses(t, r, roots)
			r.settings.batchRoots = 2
			targeted := join(fault, strconv.Itoa(roots[0]))
			r.seams.InjectN(targeted, 10000)
			defer r.seams.Withdraw(targeted)

			r.pass(ctx) // the failed batch: a round without progress
			r.pass(ctx)
			for i, id := range roots {
				flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
				if i < 2 {
					assert.Equal(2, flows, "the failed batch is passed: root %d", i)
				} else {
					assert.Zero(flows, "the batch behind it is reached: root %d", i)
				}
			}
			assert.Equal(roots[3], r.fast().after.flowID)
		})
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
	roots := seedTrees(t, db, 2, treeShape{rootSteps: 2, childSteps: 0})
	r.settings.batchRows = 7
	cut, _, err := r.fast().cut(ctx, db, r.settings, roots, false)
	assert.NoError(err)
	assert.Equal(roots[:1], cut, "four rows a tree: the root, its two steps and the child of none")
	r.settings.batchRows = 8
	cut, _, err = r.fast().cut(ctx, db, r.settings, roots, false)
	assert.NoError(err)
	assert.Equal(roots, cut)

	day := &filingDay{}
	assert.Must(assert.NoError(refreshDay(ctx, db, r.archive, true, day)))
	due, dueArgs := closedDueSQL(0)
	moved, err := move(ctx, db, r.seams, cut, due, dueArgs, day)
	assert.NoError(err)
	assert.Equal(4, moved.flows)
	for _, root := range roots {
		flows, steps := treeCount(t, r, migrations.FlowsTable(day.day), migrations.StepsTable(day.day), root)
		assert.Equal([2]int{2, 2}, [2]int{flows, steps})
	}
}

// TestWalk_TheResidencyRestrictsAPass pins that the job keeps to the residue class set on the janitor: with two
// dispatchers, a round takes only its own class of the fresh roots, and the other class's once nothing restricts it -
// a residency change that starts the walk over, so the roots behind the position are reached too.
func TestWalk_TheResidencyRestrictsAPass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	roots := seedTrees(t, r.db, 4, soloShape)
	r.residency = residency{2, 0, true}
	r.pass(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		if id%2 == 0 {
			assert.Zero(flows, "root %d is in this dispatcher's class", id)
		} else {
			assert.Equal(1, flows, "root %d is the other's, and fresh", id)
		}
	}
	r.residency = residency{}
	r.pass(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows, "root %d", id)
	}
}

// TestFast_AResidencyChangeStartsOver pins the start over: when a replica's residue class changes - a peer left - the
// roots of the class it did not have, behind its position, are reached at once rather than left to a sweep.
func TestFast_AResidencyChangeStartsOver(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	roots := seedTrees(t, r.db, 12, soloShape) // fresh: no root is overdue by the steal grace
	r.residency = residency{2, 0, true}
	r.pass(ctx)
	behind := 0
	for _, id := range roots {
		if id%2 != 0 && id%3 == 0 && id < r.fast().after.flowID {
			behind++
		}
	}
	assert.True(behind > 0, "a root of the next class is behind the position")
	r.residency = residency{3, 0, true} // a peer joined, and this replica's class is another
	r.pass(ctx)
	for _, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(id%2 != 0 && id%3 != 0, flows == 1, "root %d", id)
	}
}

// TestFast_ARootWithAnAttemptIsPassed pins that a root whose staged archive has started is not fast archiving's: it
// is passed, left to whoever copies it, and the roots around it are archived.
func TestFast_ARootWithAnAttemptIsPassed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	roots := seedTrees(t, r.db, 3, callShape)
	ascendingCloses(t, r, roots)
	at, arg := database.SQLTime(r.db.DriverName(), time.Now().Add(-time.Minute))
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+at+" WHERE flow_id=?", arg, roots[1])
	assert.NoError(err)
	r.pass(ctx)
	for i, id := range roots {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Equal(i == 1, flows == 2, "root %d", i)
	}
	assert.Equal(roots[2], r.fast().after.flowID)

	// Every candidate with an attempt: passed, and no failure.
	more := seedTrees(t, r.db, 2, callShape)
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_flows SET archive_attempt_at="+at+" WHERE flow_id IN ("+intCSV(more)+")", arg)
	assert.NoError(err)
	r.pass(ctx)
	assert.Equal(more[1], r.fast().after.flowID)
	assert.Zero(r.failing(time.Now()))
}

// TestFast_ATreeOverTheStagingThresholdIsPassed pins that, with staging on, a tree over the staging threshold is
// passed - alone in its batch, left to the staged archive - and the trees under it are archived.
func TestFast_ATreeOverTheStagingThresholdIsPassed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.stageRows = 5
	small := seedTrees(t, r.db, 2, callShape)  // 4 rows each
	large := seedTrees(t, r.db, 1, chunkShape) // 9 rows
	after := seedTrees(t, r.db, 1, callShape)
	ascendingCloses(t, r, []int{small[0], large[0], small[1], after[0]})
	r.pass(ctx)
	for _, id := range append(small, after...) {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows, "root %d archived", id)
	}
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", large[0])
	assert.Equal(2, flows, "the large tree is passed")
	var attempted int
	none, arg := noAttemptSQL(r.db.DriverName())
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE archive_attempt_at>"+none, arg).Scan(&attempted))
	assert.Zero(attempted, "no attempt started")
}

// TestFast_AtRetentionZeroATreeTooLargeForABatchIsStamped pins a retention of 0: a tree that fits a batch is deleted
// in it, and one that does not is stamped for deletion, due at once, and left to the deletion in chunks.
func TestFast_AtRetentionZeroATreeTooLargeForABatchIsStamped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	r.settings.batchRows = 5
	small := seedTrees(t, r.db, 1, callShape)[0] // 4 rows
	large := chunkTree(t, r.db)                  // 9 rows
	ascendingCloses(t, r, []int{large, small})
	r.pass(ctx)
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", small)
	assert.Zero(flows)
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", large)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps}, "left whole")
	var stamp int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", large).Scan(&stamp))
	assert.Equal(1, stamp, "stamped, due at once")
}

// TestFast_TheLagIsTheOldestDueRootPastTheGrace pins dwarf_archive_lag_seconds' reading: how long past the grace the
// oldest due root after the position had waited at the last scan, and 0 once none is due.
func TestFast_TheLagIsTheOldestDueRootPastTheGrace(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.grace = 30 * time.Second
	r.settings.batchRoots = 1
	_, ok := r.janitor().ArchiveLag()
	assert.False(ok, "no reading before a scan")
	roots := seedTrees(t, r.db, 2, callShape)
	backdate(t, r, roots[0], 150*time.Second)
	backdate(t, r, roots[1], 90*time.Second)
	assert.True(r.fast().round(ctx, r.janitor().batch(r.janitor().jobs[0])))
	lag, ok := r.janitor().ArchiveLag()
	assert.True(ok)
	assert.True(lag >= 119*time.Second && lag < 140*time.Second, "the oldest root's wait past the grace: %v", lag)
	r.pass(ctx)
	lag, _ = r.janitor().ArchiveLag()
	assert.Zero(lag, "nothing due")
}

// TestSize_TreesAreSizedInRowsAndBytes pins a tree's size: its flow rows and step rows, the sum of their byte counts,
// and how many of the rows are flows, each read capped at its limit of rows.
func TestSize_TreesAreSizedInRowsAndBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedWideTree(t, r.db, 2, 3, 4) // a root of 2 steps, 3 children of 4: 4 flows, 14 steps
	setStepBytes(t, r, root, 100, 1000)
	got, err := treesSizesUpTo(ctx, r.db, live, []int{root}, []int{1000})
	assert.Must(assert.NoError(err))
	assert.Equal(size{rows: 18, bytes: 14*100 + 4*1000, flows: 4}, got[0])
	got, err = treesSizesUpTo(ctx, r.db, live, []int{root}, []int{5})
	assert.Must(assert.NoError(err))
	assert.True(got[0].rows >= 5, "read to its limit: %d rows", got[0].rows)
}

// TestSize_SmallTreesMoveTogether pins the path of ordinary trees, with staged archival off and on at realistic
// thresholds: a pass moves them all in one transaction - one archive time for every flow of every tree - with no
// attempt started.
func TestSize_SmallTreesMoveTogether(t *testing.T) {
	t.Parallel()
	for _, staging := range []bool{false, true} {
		t.Run(map[bool]string{false: "staging_off", true: "staging_on"}[staging], func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 30)
			r.settings.batchRoots, r.settings.batchRows, r.settings.batchBytes = 512, 4096, 16<<20
			if staging {
				r.settings.stageRows, r.settings.stageBytes = 131072, 64<<20
			}
			reader := sdkmetric.NewManualReader()
			r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
			today, err := r.archive.Today(ctx)
			assert.Must(assert.NoError(err))
			roots := seedTrees(t, r.db, 50, treeShape{rootSteps: 2, childSteps: 3})
			for _, root := range roots {
				setStepBytes(t, r, root, 500, 2000)
			}
			r.pass(ctx)
			if now, _ := r.archive.Today(ctx); now != today {
				t.Skip("midnight passed during the pass")
			}
			for _, root := range roots {
				_, _, there := rowsOf(t, r, root)
				assert.False(there, "tree %d archived", root)
			}
			var flows, stamps int
			err = r.db.QueryRowContext(ctx, "SELECT COUNT(*), COUNT(DISTINCT archived_at) FROM "+migrations.FlowsTable(today)+
				" WHERE root_flow_id IN ("+intCSV(roots)+")").Scan(&flows, &stamps)
			assert.Must(assert.NoError(err))
			assert.Equal(100, flows, "every flow of every tree")
			assert.Equal(1, stamps, "one transaction: one archive time")
			var rm metricdata.ResourceMetrics
			assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
			assert.Equal(int64(100), counterSum(rm, "dwarf_flows_archived"))
		})
	}
}

// counterSum returns the total of an int64 counter's points.
func counterSum(rm metricdata.ResourceMetrics, name string) int64 {
	var total int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == name {
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
			}
		}
	}
	return total
}

// TestJanitor_NoBatchTakesASecondConnection pins the one-connection rule end to end: with a pool of one connection,
// every job archives and deletes what is due. A statement that took a second connection while its batch holds the
// only one - on the shard's DB, or through the archive - would wait for it until the deadline, and nothing would move.
func TestJanitor_NoBatchTakesASecondConnection(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	closed := seedTrees(t, r.db, 3, callShape)
	stamped := seedTrees(t, r.db, 2, callShape)
	for _, root := range stamped {
		stampTree(t, r.db, root)
	}
	r.db.DB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second*timeoutScale)
	defer cancel()
	r.pass(ctx)
	assert.NoError(ctx.Err(), "no batch waited for a second connection")
	for _, id := range append(closed, stamped...) {
		flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
		assert.Zero(flows, "root %d removed", id)
	}
}

// TestFast_TheFailureEndsAtAMoveOrACleanSweep pins the end of a failure episode: an empty round ends nothing - a root
// a failure left behind the position is not in its scan - while a sweep of every due root begun after the failure and
// failing on none ends it, and so does a later move that archives something.
func TestFast_TheFailureEndsAtAMoveOrACleanSweep(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	before := time.Now()
	r.seams.InjectN(diagnostics.FaultReapSelectErr, 1)
	r.pass(t.Context())
	assert.True(r.failing(time.Now().Add(time.Second)) > 0, "a failed scan begins an episode")
	r.pass(t.Context())
	assert.True(r.failing(time.Now().Add(time.Second)) > 0, "an empty round ends nothing")
	r.janitor().SweptClean(before)
	assert.True(r.failing(time.Now().Add(time.Second)) > 0, "a sweep begun before the failure ends nothing")
	r.janitor().SweptClean(time.Now())
	assert.Zero(r.failing(time.Now().Add(time.Second)), "a clean sweep begun after it ends it")

	r.seams.InjectN(diagnostics.FaultReapSelectErr, 1)
	r.pass(t.Context())
	assert.True(r.failing(time.Now().Add(time.Second)) > 0)
	seedTrees(t, r.db, 1, callShape)
	r.pass(t.Context())
	assert.Zero(r.failing(time.Now().Add(time.Second)), "a move ends it")
}

// TestFast_ABatchThatCannotTakeItsConnectionIsAFailure pins a batch whose connection cannot be taken: it is an archive
// failure, logged, and not a silent idle round.
func TestFast_ABatchThatCannotTakeItsConnectionIsAFailure(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := newRig(t, 30)
	seedTrees(t, r.db, 1, callShape)
	r.seams.InjectN(diagnostics.FaultJanitorConnErr, 1)
	r.pass(t.Context())
	assert.True(r.failing(time.Now().Add(time.Second)) > 0)
	assert.Equal(1, r.logs.count("Janitor: taking a connection failed"))
}

// TestFast_AHeldRootIsNotWaitedForWhenStamped pins the stamp at a retention of 0: a tree too large for a batch whose
// root another transaction holds - a Fork cloning it - is skipped, not waited for, on the dialects with SKIP LOCKED,
// so the trees after it are deleted.
func TestFast_AHeldRootIsNotWaitedForWhenStamped(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	db := r.db
	if d := db.DriverName(); d != "pgx" && d != "mysql" && d != "mariadb" {
		t.Skip("a held root is waited for on " + d)
	}
	r.settings.batchRows = 5
	large := chunkTree(t, db)                  // 9 rows
	small := seedTrees(t, db, 1, callShape)[0] // 4 rows
	ascendingCloses(t, r, []int{large, small})
	locked := make(chan error, 1)
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- db.Transact(ctx, func(tx *sequel.Tx) error {
			_, err := lockRoots(ctx, tx, live.flows, false, "flow_id=?", large)
			locked <- err
			<-release
			return err
		})
	}()
	assert.Must(assert.NoError(<-locked))
	bounded, cancel := context.WithTimeout(ctx, 10*time.Second*timeoutScale)
	defer cancel()
	r.pass(bounded)
	assert.NoError(bounded.Err(), "the stamp did not wait on the held root")
	close(release)
	assert.NoError(<-holderDone)
	flows, _ := treeCount(t, r, "dwarf_flows", "dwarf_steps", small)
	assert.Zero(flows, "the tree after it is deleted")
	var stamp int
	assert.NoError(db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", large).Scan(&stamp))
	assert.Zero(stamp, "the held root was skipped, not stamped")
}
