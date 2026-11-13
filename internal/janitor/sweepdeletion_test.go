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
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// stepsOf counts the live steps of flows, whether or not their flow rows are still there.
func stepsOf(t *testing.T, r *rig, flows []int) int {
	t.Helper()
	var n int
	if err := r.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id IN ("+database.IntList(flows)+")").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// deletionRounds runs fast deletion's rounds until one makes no progress - every stamped tree over a batch passed - and
// then sweep deletion's, and returns how many of the sweep's made progress.
func (r *rig) deletionRounds(ctx context.Context) (n int) {
	for r.roundOf(ctx, JobFastDeletion) {
	}
	for r.roundOf(ctx, JobSweepDeletion) {
		n++
	}
	return n
}

// stampOf returns the delete_after_ms of root.
func stampOf(t *testing.T, r *rig, root int) int {
	t.Helper()
	var stamp int
	if err := r.db.QueryRowContext(t.Context(), "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp); err != nil {
		t.Fatal(err)
	}
	return stamp
}

// TestChunk_ALargeTreeIsDeletedInChunksRootLast pins chunked deletion for every population that deletes: a tree larger
// than a batch is deleted over several transactions, each about a batch of rows, its root stamped from the first and
// deleted last, and no step outlives its flow. A stamped tree is sweep deletion's once fast deletion passed it; a closed
// tree at a retention of 0 is stamped by fast archiving, and a tree past its retention by live retention, then the
// same.
func TestChunk_ALargeTreeIsDeletedInChunksRootLast(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{kindDeleted, kindClosed, kindRetention} {
		t.Run(kind, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			retention := 0
			if kind == kindRetention {
				retention = 1
			}
			r := newRig(t, retention)
			r.settings.overdue = 0
			root := seedWideTree(t, r.db, 2, 6, 3) // 7 flows, 20 steps
			flows := treeIDs(t, r, root)
			switch kind {
			case kindDeleted:
				stampTree(t, r.db, root)
			case kindClosed:
				closeTreesAgo(t, r.db, time.Minute, root)
			case kindRetention:
				closeTreesAgo(t, r.db, 4*24*time.Hour, root)
			}
			r.settings.batchRows = 4
			switch kind {
			case kindClosed:
				r.fastRounds(ctx) // at a retention of 0, fast archiving stamps it
			case kindRetention:
				r.retentionRounds(ctx) // archiving never moved it: past its retention, live retention stamps it
				stampsDue(t, r, root)
			}
			for r.roundOf(ctx, JobFastDeletion) {
			}
			rounds := 0
			for ; rounds < 40; rounds++ {
				f, s, rootThere := rowsOf(t, r, root)
				if f+s == 0 {
					break
				}
				assert.True(rootThere, "the root goes last: %d flows and %d steps left without it", f, s)
				assert.Zero(stepsOf(t, r, flows)-s, "no step outlives its flow")
				if rounds > 0 {
					assert.True(stampOf(t, r, root) > 0, "stamped from the first chunk")
				}
				r.roundOf(ctx, JobSweepDeletion) // a chunk a round
			}
			f, s, _ := rowsOf(t, r, root)
			assert.Zero(f + s)
			assert.Zero(stepsOf(t, r, flows))
			assert.True(rounds >= 3, "deleted over several rounds, not one: %d", rounds)
		})
	}
}

// TestChunk_AFaultedChunkRollsBackWhole pins that a chunk is one transaction: a fault after its steps rolls them back
// with it, leaving the tree stamped and whole, and the next chunk deletes it.
func TestChunk_AFaultedChunkRollsBackWhole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	root := chunkTree(t, r.db)
	stampTree(t, r.db, root)
	r.seams.InjectN(diagnostics.FaultReapMidTree, 1000)
	var pos chunkPos
	_, _, err := deleteChunk(ctx, r.db, r.seams, root, capOf(2, 0), &pos)
	assert.Error(err)
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps}, "rolled back whole")
	assert.Equal(1, stampOf(t, r, root), "still stamped")
	assert.Equal(chunkPos{}, pos, "the position moves only when a chunk commits")
	r.seams.Withdraw(diagnostics.FaultReapMidTree)
	for range 20 {
		if _, done, err := deleteChunk(ctx, r.db, r.seams, root, capOf(2, 0), &pos); err != nil || done {
			assert.NoError(err)
			break
		}
	}
	flows, steps = treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Zero(flows + steps)
}

// TestChunk_ARestartResumesTheDeletion pins that a chunked deletion needs nothing in memory to finish: a fresh janitor -
// a restart, or another replica - finds the stamped tree and deletes the rest.
func TestChunk_ARestartResumesTheDeletion(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	r.settings.overdue = 0
	r.settings.batchRows = 6
	root := seedWideTree(t, r.db, 3, 10, 4)
	stampTree(t, r.db, root)
	flows := treeIDs(t, r, root)
	for r.roundOf(ctx, JobFastDeletion) {
	}
	r.roundOf(ctx, JobSweepDeletion) // the tree found too large: taken from the next batch on
	r.roundOf(ctx, JobSweepDeletion)
	f, s, rootThere := rowsOf(t, r, root)
	assert.True(rootThere && f+s > 0 && f+s < 11+43, "part of the tree is gone: %d flows, %d steps", f, s)

	r.restart()
	r.pass(ctx)
	f, s, _ = rowsOf(t, r, root)
	assert.Zero(f + s)
	assert.Zero(stepsOf(t, r, flows))
}

// TestChunk_TwoJanitorsShareATree pins concurrent chunked deletion of one tree, as two dispatchers of a shard can run:
// the root lock serializes their chunks, and the tree is deleted once, whole.
func TestChunk_TwoJanitorsShareATree(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	root := seedWideTree(t, r.db, 2, 40, 3)
	flows := treeIDs(t, r, root)
	stampTree(t, r.db, root)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			var pos chunkPos // each its own, as each replica's job keeps its own
			for range 200 {
				_, done, err := deleteChunk(ctx, r.db, r.seams, root, capOf(8, 0), &pos)
				if err == nil && done {
					return
				}
			}
		})
	}
	wg.Wait()
	f, s, _ := rowsOf(t, r, root)
	assert.Zero(f + s)
	assert.Zero(stepsOf(t, r, flows))
}

// TestChunk_StaleWorkIsANoOp pins a chunk that finds nothing to do: a root already gone, or one not stamped and due - a
// candidate a peer deleted, or a selection gone stale - deletes nothing and reports the tree done for this chunk.
func TestChunk_StaleWorkIsANoOp(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedWideTree(t, r.db, 1, 2, 2)
	n, done, err := deleteChunk(ctx, r.db, r.seams, root, capOf(4096, 0), &chunkPos{})
	assert.NoError(err)
	assert.True(done)
	assert.Zero(n.flows)
	f, s, _ := rowsOf(t, r, root)
	assert.Equal([2]int{3, 5}, [2]int{f, s}, "an unstamped tree is left whole")
	n, done, err = deleteChunk(ctx, r.db, r.seams, root+1000, capOf(4096, 0), &chunkPos{})
	assert.NoError(err)
	assert.True(done)
	assert.Zero(n.flows)
}

// TestChunk_ALateDescendantKeepsTheRoot pins the membership recheck before a root is deleted: a descendant that appears
// after the chunk walked the tree keeps the root, and the next chunk deletes both. A closed tree gains no flow, so this
// guards against a bug, not a path. SQLite has one writer, so there the late insert waits for the chunk to commit.
func TestChunk_ALateDescendantKeepsTheRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	if r.db.DriverName() == "sqlite" {
		t.Skip("SQLite's one writer holds a late insert until the chunk commits")
	}
	root := seedWideTree(t, r.db, 1, 1, 1)
	stampTree(t, r.db, root)
	r.seams.Break(diagnostics.CheckpointReapBeforeRootDelete)
	type result struct {
		n    int
		done bool
		err  error
	}
	got := make(chan result, 1)
	go func() {
		n, done, err := deleteChunk(ctx, r.db, r.seams, root, capOf(4096, 0), &chunkPos{})
		got <- result{n.flows, done, err}
	}()
	assert.Must(assert.True(r.seams.WaitTimeout(ctx, diagnostics.CheckpointReapBeforeRootDelete, 10*time.Second*timeoutScale)))
	late, err := r.db.InsertReturnID(ctx, "flow_id", "INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name,"+
		" graph, graph_hash, status, surgraph_flow_id, surgraph_step_id, root_flow_id) VALUES (?,?,?,?,?,'"+
		workflow.StatusCompleted+"',?,0,?)", "0123456789abcdef", "seed/late", "Late", []byte("{}"), "", root, root)
	r.seams.Resume(diagnostics.CheckpointReapBeforeRootDelete)
	assert.Must(assert.NoError(err))
	res := <-got
	assert.NoError(res.err)
	assert.False(res.done, "the late descendant keeps the root")
	_, _, rootThere := rowsOf(t, r, root)
	assert.True(rootThere)

	n, done, err := deleteChunk(ctx, r.db, r.seams, root, capOf(4096, 0), &chunkPos{})
	assert.NoError(err)
	assert.True(done)
	assert.Equal(2, n.flows, "the late descendant and the root")
	var left int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id IN (?, ?)", root, late).Scan(&left))
	assert.Zero(left)
}

// TestChunk_ALateDescendantWithAKeptPosition pins the late-descendant recheck against a position kept from earlier
// chunks: a descendant newer than the position walked keeps the root, and the next chunk finds and deletes it rather than
// ending at the recheck forever.
func TestChunk_ALateDescendantWithAKeptPosition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	root := seedWideTree(t, r.db, 1, 3, 1)
	stampTree(t, r.db, root)
	var pos chunkPos
	_, done, err := deleteChunk(ctx, r.db, r.seams, root, capOf(2, 0), &pos)
	assert.Must(assert.NoError(err))
	assert.Must(assert.False(done))
	assert.Must(assert.True(pos.top > 0, "the chunks have a position: %+v", pos))
	late, err := r.db.InsertReturnID(ctx, "flow_id", "INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name,"+
		" graph, graph_hash, status, surgraph_flow_id, surgraph_step_id, root_flow_id) VALUES (?,?,?,?,?,'"+
		workflow.StatusCompleted+"',?,0,?)", "0123456789abcdef", "seed/late", "Late", []byte("{}"), "", root, root)
	assert.Must(assert.NoError(err))
	for range 20 {
		if _, done, err = deleteChunk(ctx, r.db, r.seams, root, capOf(2, 0), &pos); err != nil || done {
			break
		}
	}
	assert.NoError(err)
	assert.True(done, "finished")
	var left int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id IN (?, ?)", root, late).Scan(&left))
	assert.Zero(left, "the late descendant and the root")
}

// TestChunk_AFailingLargeTreeDoesNotHoldBackTheRest pins that a large tree whose chunks keep failing is passed: the
// trees behind it are deleted, the sweep counts it held, and it is deleted once the failure clears.
func TestChunk_AFailingLargeTreeDoesNotHoldBackTheRest(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	r.settings.overdue = 0
	r.settings.batchRows = 4
	big := seedWideTree(t, r.db, 2, 6, 3) // 27 rows
	small := seedTrees(t, r.db, 3, callShape)
	for _, root := range append([]int{big}, small...) {
		stampTree(t, r.db, root)
	}
	fault := diagnostics.Join(diagnostics.FaultReapMidTree, strconv.Itoa(big))
	r.seams.InjectN(fault, 100000)
	defer r.seams.Withdraw(fault)
	for range 3 {
		r.pass(ctx)
	}
	for _, root := range small {
		f, s, _ := rowsOf(t, r, root)
		assert.Zero(f+s, "tree %d behind the failing one is deleted", root)
	}
	f, s, _ := rowsOf(t, r, big)
	assert.Equal([2]int{7, 20}, [2]int{f, s}, "the failing tree is whole")
	assert.Equal(1, r.sweepDel().w.reading(time.Now()).held, "and held, once a sweep")

	r.seams.Withdraw(fault)
	r.pass(ctx)
	f, s, _ = rowsOf(t, r, big)
	assert.Zero(f + s)
}

// TestChunk_CommittedChunksAreCountedWhenALaterOneFails pins dwarf_flows_reaped for a chunked deletion: each chunk's
// flows are counted as it commits, so a later chunk's failure does not lose them.
func TestChunk_CommittedChunksAreCountedWhenALaterOneFails(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	r.settings.overdue = 0
	r.settings.batchRows = 2               // a child a chunk
	root := seedWideTree(t, r.db, 1, 4, 1) // children of two rows each
	stampTree(t, r.db, root)
	for r.roundOf(ctx, JobFastDeletion) {
	}
	r.roundOf(ctx, JobSweepDeletion) // the first chunk
	fault := diagnostics.Join(diagnostics.FaultReapMidTree, strconv.Itoa(root))
	r.seams.InjectN(fault, 100000)
	defer r.seams.Withdraw(fault)
	r.roundOf(ctx, JobSweepDeletion) // the next fails
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	assert.Equal(int64(1), counterSum(rm, "dwarf_flows_reaped"), "the first chunk's child, counted though the next failed")
	f, _, _ := rowsOf(t, r, root)
	assert.Equal(4, f, "the root and three children left")
}

// TestChunk_AChunkKeepsToItsBytes pins a chunk's byte budget: it takes steps until the next would pass its bytes, and
// a step over the whole budget goes alone.
func TestChunk_AChunkKeepsToItsBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedWideTree(t, r.db, 6, 0, 0) // one flow of 6 steps
	setStepBytes(t, r, root, 3000, 0)
	stampTree(t, r.db, root)
	steps := func() int {
		_, n, _ := rowsOf(t, r, root)
		return n
	}
	var pos chunkPos
	_, done, err := deleteChunk(ctx, r.db, r.seams, root, capOf(4096, 7000), &pos)
	assert.Must(assert.NoError(err))
	assert.False(done)
	assert.Equal(4, steps(), "two steps of 3,000 bytes fit 7,000")
	_, done, err = deleteChunk(ctx, r.db, r.seams, root, capOf(4096, 1000), &pos)
	assert.Must(assert.NoError(err))
	assert.False(done)
	assert.Equal(3, steps(), "one step over the budget goes alone")
	for range 10 {
		if _, done, err = deleteChunk(ctx, r.db, r.seams, root, capOf(4096, 7000), &pos); err != nil || done {
			break
		}
	}
	assert.NoError(err)
	assert.True(done)
	_, _, there := rowsOf(t, r, root)
	assert.False(there, "the tree is gone")
}

// TestChunk_AFlowRowIsChargedItsBytes pins a flow row's own bytes against a chunk's budget: it goes with its last
// steps only when both fit, and otherwise first in the next chunk, so no chunk takes more than the budget's bytes
// unless it took one row alone.
func TestChunk_AFlowRowIsChargedItsBytes(t *testing.T) {
	t.Parallel()
	const budget = 7000
	for _, shape := range []struct {
		name                    string
		rootSteps, kids, kidSts int
	}{
		{"one_flow", 2, 0, 0},
		{"a_child", 1, 1, 3}, // the child resumed part done: its last step and its row together pass the budget
	} {
		t.Run(shape.name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 30)
			root := seedWideTree(t, r.db, shape.rootSteps, shape.kids, shape.kidSts)
			setStepBytes(t, r, root, 3000, 6000)
			stampTree(t, r.db, root)
			ids := treeIDs(t, r, root)
			left := func() size { return treeSize(t, r, "dwarf_flows", "dwarf_steps", ids) }
			before := left()
			var pos chunkPos
			for range 20 {
				_, done, err := deleteChunk(ctx, r.db, r.seams, root, capOf(4096, budget), &pos)
				assert.Must(assert.NoError(err))
				now := left()
				took := size{rows: before.rows - now.rows, bytes: before.bytes - now.bytes}
				assert.True(took.bytes <= budget || took.rows == 1, "a chunk took %d rows of %d bytes", took.rows, took.bytes)
				before = now
				if done {
					_, _, there := rowsOf(t, r, root)
					assert.False(there, "the tree is gone")
					return
				}
			}
			t.Fatal("not finished in 20 chunks")
		})
	}
}

// TestSweepDeletion_TakesWhatFastDeletionPassed pins the sweep's population: a stamp that came due behind fast
// deletion's position - a DeleteOnCompletion tree still in its grace when fast deletion passed it - is the sweep's once
// overdue by its window, and not before.
func TestSweepDeletion_TakesWhatFastDeletionPassed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	roots := seedTrees(t, r.db, 2, callShape)
	// The first in its grace when fast deletion passes the second.
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=3600000, updated_at=NOW_UTC() WHERE flow_id=?", roots[0])
	assert.NoError(err)
	stampTree(t, r.db, roots[1])
	r.deletionRounds(ctx)
	assert.Equal(roots[1], r.del().after, "fast deletion is past the first")
	f, _, _ := rowsOf(t, r, roots[0])
	assert.Equal(2, f, "in its grace")

	// Its stamp comes due, behind fast deletion's position: within the window, nothing takes it.
	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), -3600000) WHERE flow_id=?", roots[0])
	assert.NoError(err)
	r.pass(ctx)
	f, _, _ = rowsOf(t, r, roots[0])
	assert.Equal(2, f, "due within the window: not yet the sweep's")

	_, err = r.db.ExecContext(ctx, "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE flow_id=?",
		-(time.Hour + DefaultSweepOverdue + time.Second).Milliseconds(), roots[0])
	assert.NoError(err)
	r.pass(ctx)
	f, _, _ = rowsOf(t, r, roots[0])
	assert.Zero(f, "overdue past the window: swept")
}

// TestSweepDeletion_ALargeTreeIsFinishedBeforeTheNext pins one large tree at a time: the sweep deletes a large stamped
// tree chunk after chunk until it is gone, and only then touches the next.
func TestSweepDeletion_ALargeTreeIsFinishedBeforeTheNext(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	r.settings.batchRows = 4
	first := seedWideTree(t, r.db, 2, 6, 3)  // 27 rows
	second := seedWideTree(t, r.db, 2, 6, 3) // 27 rows
	stampTree(t, r.db, first)
	stampTree(t, r.db, second)
	for r.roundOf(ctx, JobFastDeletion) {
	}
	whole := [2]int{7, 20}
	for range 40 {
		f, s, _ := rowsOf(t, r, first)
		if f+s == 0 {
			break
		}
		f2, s2, _ := rowsOf(t, r, second)
		assert.Equal(whole, [2]int{f2, s2}, "the second untouched while the first has %d rows", f+s)
		r.roundOf(ctx, JobSweepDeletion)
	}
	f, s, _ := rowsOf(t, r, first)
	assert.Zero(f+s, "the first finished")
	r.pass(ctx)
	f, s, _ = rowsOf(t, r, second)
	assert.Zero(f+s, "then the second")
}

// TestSweepDeletion_PassesFailedTrees pins that a stamped root whose deletion or sizing fails does not trap its
// batch-mates or the roots after it, and is deleted once the fault clears.
func TestSweepDeletion_PassesFailedTrees(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{diagnostics.FaultReapMidTree, diagnostics.FaultReapSizeErr} {
		t.Run(fault, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 30)
			r.settings.overdue = 0
			r.settings.batchRoots = 4
			roots := seedTrees(t, r.db, 7, callShape)
			for _, id := range roots {
				stampTree(t, r.db, id)
			}
			targeted := diagnostics.Join(fault, strconv.Itoa(roots[2]))
			r.seams.InjectN(targeted, 10000)
			for range 3 {
				r.pass(ctx)
			}
			for i, id := range roots {
				flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", id)
				if i == 2 {
					assert.Equal(2, flows)
					assert.True(steps > 0)
				} else {
					assert.Zero(flows+steps, "root %d", i)
				}
			}
			assert.True(r.sweepDel().w.reading(time.Now()).held > 0, "held")
			r.seams.Withdraw(targeted)
			r.pass(ctx)
			flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", roots[2])
			assert.Zero(flows + steps)
		})
	}
}

// TestSweepDeletion_TheEndIsFixed pins that a sweep's end is fast deletion's position when it began: a stamped root that
// fast deletion passes while the sweep runs is the next sweep's.
func TestSweepDeletion_TheEndIsFixed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	r.settings.batchRows = 4
	big := seedWideTree(t, r.db, 2, 6, 3) // deleted in chunks: the sweep runs over several rounds
	stampTree(t, r.db, big)
	for r.roundOf(ctx, JobFastDeletion) {
	}
	assert.True(r.roundOf(ctx, JobSweepDeletion), "the sweep began")
	arrival := seedTrees(t, r.db, 1, callShape)[0]
	stampTree(t, r.db, arrival)
	withdraw := failFast(r, arrival)
	for r.roundOf(ctx, JobFastDeletion) {
	}
	withdraw()
	assert.Equal(arrival, r.del().after, "fast deletion passed the arrival")
	for r.roundOf(ctx, JobSweepDeletion) {
	}
	f, _, _ := rowsOf(t, r, big)
	assert.Zero(f, "the sweep finished the tree behind its end")
	f, _, _ = rowsOf(t, r, arrival)
	assert.Equal(2, f, "and left the arrival past its end to the next sweep")
	r.pass(ctx)
	f, _, _ = rowsOf(t, r, arrival)
	assert.Zero(f, "which takes it")
}

// TestSweepDeletion_FollowsFastDeletionsResidueClass pins that a sweep takes the residue class of the boundary it began
// at: another class's stamped root is the sweep's only once overdue by the steal grace past its window.
func TestSweepDeletion_FollowsFastDeletionsResidueClass(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	r.setResidency(2, 0, true)
	roots := seedTrees(t, r.db, 4, soloShape) // consecutive ids
	var own, other []int
	for _, id := range roots {
		// Due a second ago: well within the steal grace, so another class's root is not stolen.
		_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, updated_at=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE flow_id=?", id)
		assert.NoError(err)
		if id%2 == 0 {
			own = append(own, id)
		} else {
			other = append(other, id)
		}
	}
	withdraw := failFast(r, own...)
	r.deletionRounds(ctx) // fast deletion fails and passes its own class's; the sweep fails them too
	withdraw()
	r.deletionRounds(ctx)
	for _, id := range own {
		f, _, _ := rowsOf(t, r, id)
		assert.Zero(f, "its own class's swept: root %d", id)
	}
	for _, id := range other {
		f, _, _ := rowsOf(t, r, id)
		assert.Equal(1, f, "another class's, fresh: root %d", id)
	}
	assert.Equal(0, r.sweepDel().end.ordinal)
}

// TestSweepDeletion_AClassChangeDropsTheSweep pins that a sweep under way is dropped when fast deletion's residue class
// changes, the large tree it was deleting included, and the next sweep takes the new class's boundary.
func TestSweepDeletion_AClassChangeDropsTheSweep(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.settings.overdue = 0
	r.settings.batchRows = 4
	r.setResidency(2, 0, true)
	var big int
	for big == 0 || big%2 != 0 { // a tree of class 0
		big = seedWideTree(t, r.db, 2, 6, 3)
	}
	stampTree(t, r.db, big) // due a second ago: not another class's to steal
	for r.roundOf(ctx, JobFastDeletion) {
	}
	assert.True(r.roundOf(ctx, JobSweepDeletion), "the sweep began on the large tree")
	assert.Equal(big, r.sweepDel().current.flowID)

	r.setResidency(2, 1, true)
	r.roundOf(ctx, JobFastDeletion) // starts over in class 1
	r.roundOf(ctx, JobSweepDeletion)
	assert.Equal(1, r.sweepDel().end.ordinal, "the next sweep is class 1's")
	assert.NotEqual(big, r.sweepDel().current.flowID, "the class 0 tree was dropped")
}

// TestSweepDeletion_TheGauges pins the sweep's gauges, under kind="deleted": a root that fails alone is held, and the
// oldest root at the sweep's front is reported.
func TestSweepDeletion_TheGauges(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	reader := sdkmetric.NewManualReader()
	r.meter = sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test")
	r.settings.overdue = 0
	root := seedTrees(t, r.db, 1, callShape)[0]
	_, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, updated_at=DATE_ADD_MILLIS(NOW_UTC(), -120000) WHERE flow_id=?", root)
	assert.NoError(err)
	fault := diagnostics.Join(diagnostics.FaultReapMidTree, strconv.Itoa(root))
	r.seams.InjectN(fault, 10000)
	defer r.seams.Withdraw(fault)
	r.pass(ctx)
	var rm metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &rm))
	attrs := map[string]string{"shard": "1", "kind": kindDeleted}
	held, ok := gaugeOf(rm, "dwarf_archive_held_roots", attrs)
	assert.True(ok && held == 1, "held: %v %v", held, ok)
	oldest, ok := gaugeOf(rm, "dwarf_archive_oldest_held_seconds", attrs)
	assert.True(ok && oldest >= 119, "the front's oldest: %v %v", oldest, ok)
}
