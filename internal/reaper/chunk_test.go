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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// treeFlows returns the flow ids of the tree rooted at root.
func treeFlows(t *testing.T, r *rig, root int) []int {
	t.Helper()
	rows, err := r.db.QueryContext(t.Context(), "SELECT flow_id FROM dwarf_flows WHERE root_flow_id=? ORDER BY flow_id", root)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

// stepsOf counts the steps of flows, whether or not their flow rows are still there.
func stepsOf(t *testing.T, r *rig, flows []int) int {
	t.Helper()
	var n int
	if err := r.db.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id IN ("+intCSV(flows)+")").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestChunk_ALargeTreeIsDeletedInChunksRootLast pins chunked deletion for every kind that deletes: a tree larger than a
// batch is deleted over several transactions, each about a batch of rows, its root stamped from the first and deleted
// last, and no step outlives its flow.
func TestChunk_ALargeTreeIsDeletedInChunksRootLast(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{KindDeleted, KindClosed, KindRetention} {
		t.Run(kind, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			retention := 0
			if kind == KindRetention {
				retention = 1
			}
			r := newRig(t, retention)
			root := seedWideTree(t, r.db, 2, 6, 3) // 7 flows, 20 steps
			flows := treeFlows(t, r, root)
			switch kind {
			case KindDeleted:
				stampTree(t, r.db, root)
			case KindRetention:
				closeTreesAgo(t, r.db, 4*24*time.Hour, root)
				r.archive = nil // retention needs no archive; without one nothing is archived either
			}
			r.cfg.BatchRows = 4
			r.cfg.Slice, r.cfg.Budget = 0, 0 // a chunk a walk, one round of walks a pass
			passes := 0
			for ; passes < 40; passes++ {
				f, s, rootThere := rowsOf(t, r, root)
				if f+s == 0 {
					break
				}
				assert.True(rootThere, "the root goes last: %d flows and %d steps left without it", f, s)
				assert.Zero(stepsOf(t, r, flows)-s, "no step outlives its flow")
				if passes > 0 {
					var stamp int
					assert.NoError(r.db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp))
					assert.True(stamp > 0, "stamped from the first chunk")
				}
				r.pass(ctx)
			}
			f, s, _ := rowsOf(t, r, root)
			assert.Zero(f + s)
			assert.Zero(stepsOf(t, r, flows))
			assert.True(passes >= 3, "deleted over several passes, not one: %d", passes)
			assert.Zero(r.logs.count("Reaper: removing due trees"))
		})
	}
}

// TestChunk_AFaultedChunkRollsBackWhole pins that a chunk is one transaction: a fault after its steps rolls them back
// with it, leaving the tree stamped and whole, and the next pass deletes it.
func TestChunk_AFaultedChunkRollsBackWhole(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	root := chunkTree(t, r.db)
	r.cfg.BatchRows = 2
	r.seams.InjectN(FaultMidTree, 1000)
	r.pass(ctx)
	flows, steps := treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Equal([2]int{2, 7}, [2]int{flows, steps}, "rolled back whole")
	var stamp int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT delete_after_ms FROM dwarf_flows WHERE flow_id=?", root).Scan(&stamp))
	assert.Equal(1, stamp, "stamped before the first chunk")
	r.seams.Withdraw(FaultMidTree)
	r.pass(ctx)
	flows, steps = treeCount(t, r, "dwarf_flows", "dwarf_steps", root)
	assert.Zero(flows + steps)
}

// TestChunk_ARestartResumesTheDeletion pins that a chunked deletion needs nothing in memory to finish: a fresh Reaper
// - a restart, or another replica - finds the stamped tree and deletes the rest.
func TestChunk_ARestartResumesTheDeletion(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	root := seedWideTree(t, r.db, 3, 10, 4)
	flows := treeFlows(t, r, root)
	r.cfg.BatchRows = 6
	r.cfg.Slice, r.cfg.Budget = 0, 0
	r.pass(ctx)
	f, s, rootThere := rowsOf(t, r, root)
	assert.True(rootThere && f+s > 0 && f+s < 11+43, "part of the tree is gone: %d flows, %d steps", f, s)

	r.reaper = New()
	r.reaper.SetSeams(r.seams)
	r.cfg.Slice, r.cfg.Budget = time.Hour, time.Hour
	r.pass(ctx)
	f, s, _ = rowsOf(t, r, root)
	assert.Zero(f + s)
	assert.Zero(stepsOf(t, r, flows))
}

// TestChunk_TwoReapersShareATree pins concurrent chunked deletion of one tree, as two dispatchers of a shard can run:
// the root lock serializes their chunks, and the tree is deleted once, whole.
func TestChunk_TwoReapersShareATree(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	root := seedWideTree(t, r.db, 2, 40, 3)
	flows := treeFlows(t, r, root)
	stampTree(t, r.db, root)
	r.cfg.BatchRows = 8
	r.cfg.Slice, r.cfg.Budget = 0, 0
	other := New()
	other.SetSeams(r.seams)
	var wg sync.WaitGroup
	for _, reaper := range []*Reaper{r.reaper, other} {
		wg.Go(func() {
			for range 100 {
				reaper.Pass(ctx, r.cfg, r.shard())
				if f, s, _ := rowsOf(t, r, root); f+s == 0 {
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

// TestChunk_StaleWorkIsANoOp pins a chunk that finds nothing to do: a root already gone, or one not stamped and due -
// a candidate a peer deleted, or a selection gone stale - deletes nothing and reports the tree done for this walk.
func TestChunk_StaleWorkIsANoOp(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedWideTree(t, r.db, 1, 2, 2)
	n, done, err := r.p().deleteChunk(ctx, root, &chunkPos{})
	assert.NoError(err)
	assert.True(done)
	assert.Zero(n)
	f, s, _ := rowsOf(t, r, root)
	assert.Equal([2]int{3, 5}, [2]int{f, s}, "an unstamped tree is left whole")
	n, done, err = r.p().deleteChunk(ctx, root+1000, &chunkPos{})
	assert.NoError(err)
	assert.True(done)
	assert.Zero(n)
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
	r.seams.Break(CheckpointBeforeRootDelete)
	type result struct {
		n    int
		done bool
		err  error
	}
	got := make(chan result, 1)
	go func() {
		n, done, err := r.p().deleteChunk(ctx, root, &chunkPos{})
		got <- result{n, done, err}
	}()
	assert.Must(assert.True(r.seams.WaitTimeout(ctx, CheckpointBeforeRootDelete, 10*time.Second*timeoutScale)))
	late, err := r.db.InsertReturnID(ctx, "flow_id", "INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name,"+
		" graph, graph_hash, status, surgraph_flow_id, surgraph_step_id, root_flow_id) VALUES (?,?,?,?,?,'"+
		workflow.StatusCompleted+"',?,0,?)", "0123456789abcdef", "seed/late", "Late", []byte("{}"), "", root, root)
	r.seams.Resume(CheckpointBeforeRootDelete)
	assert.Must(assert.NoError(err))
	res := <-got
	assert.NoError(res.err)
	assert.False(res.done, "the late descendant keeps the root")
	_, _, rootThere := rowsOf(t, r, root)
	assert.True(rootThere)

	n, done, err := r.p().deleteChunk(ctx, root, &chunkPos{})
	assert.NoError(err)
	assert.True(done)
	assert.Equal(2, n, "the late descendant and the root")
	var left int
	assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE flow_id IN (?, ?)", root, late).Scan(&left))
	assert.Zero(left)
}

// TestChunk_TheStepBoundaryWalksTheFlowsIndex pins the plan of a chunk's step read to idx_dwarf_steps_flow_id in step_id
// order, with no sort: keyed on (flow_id, step_id) it is bounded by the flow's own steps, however few of the table's
// they are. Keyed on (flow_id) alone, PostgreSQL walks the primary key instead, past every other flow's
// steps (3.6k-6.9k buffers a chunk at 4% density, against about 20 with the pair). Do not key it on flow_id alone.
func TestChunk_TheStepBoundaryWalksTheFlowsIndex(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	var explain, priceOut string
	switch r.db.DriverName() {
	case "pgx":
		explain, priceOut = "EXPLAIN ", "SET LOCAL enable_seqscan = off"
	case "sqlite":
		explain = "EXPLAIN QUERY PLAN "
	case "mysql", "mariadb":
		explain = "EXPLAIN "
	default:
		t.Skip("the plan shapes pinned here are PostgreSQL's, SQLite's, MySQL's and MariaDB's")
	}
	var plan string
	errRollback := errors.New("rollback")
	err := r.db.Transact(ctx, func(tx *sequel.Tx) error {
		if priceOut != "" {
			if _, err := tx.ExecContext(ctx, priceOut); err != nil {
				return err
			}
		}
		rows, err := tx.QueryContext(ctx, explain+"SELECT step_id FROM dwarf_steps WHERE flow_id=7 ORDER BY step_id LIMIT_OFFSET(?, 0)", 4096)
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
				plan += " " + toString(v)
			}
			plan += "\n"
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return errRollback
	})
	assert.True(errors.Is(err, errRollback), "%v", err)
	assert.Contains(plan, "idx_dwarf_steps_flow_id", "%s", plan)
	if r.db.DriverName() != "mysql" && r.db.DriverName() != "mariadb" {
		assert.False(strings.Contains(plan, "Sort") || strings.Contains(plan, "TEMP B-TREE"), "no sort: %s", plan)
	}
}

// toString renders a plan cell.
func toString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return strconv.Quote(fmt.Sprint(v))
}

// TestChunk_AFailingLargeTreeDoesNotHoldBackTheRest pins that a large tree whose chunks keep failing is passed like any
// failed batch: the trees behind it are deleted, repair counts it held, and it is deleted once the failure clears.
func TestChunk_AFailingLargeTreeDoesNotHoldBackTheRest(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 0)
	big := seedWideTree(t, r.db, 2, 6, 3) // 27 rows
	small := seedTrees(t, r.db, 3, callShape)
	for _, root := range append([]int{big}, small...) {
		stampTree(t, r.db, root)
	}
	r.cfg.BatchRows = 4
	fault := join(FaultMidTree, strconv.Itoa(big))
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
	assert.Equal(1, r.reaper.Observe(1, time.Now()).Kinds[1].Held, "and held, once")

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
	assert.Must(assert.NoError(r.reaper.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))))
	root := seedWideTree(t, r.db, 1, 4, 1) // children of two rows each
	stampTree(t, r.db, root)
	r.cfg.BatchRows = 2 // a child a chunk
	r.seams.Break(CheckpointChunkDone)
	type result struct {
		removed int
		err     error
	}
	got := make(chan result, 1)
	go func() {
		removed, _, err := r.p().deleteLarge(ctx, deletionDue(0), root, ReasonDeleted, time.Now())
		got <- result{removed, err}
	}()
	assert.Must(assert.True(r.seams.WaitTimeout(ctx, CheckpointChunkDone, 10*time.Second*timeoutScale)))
	fault := join(FaultMidTree, strconv.Itoa(root))
	r.seams.InjectN(fault, 100000)
	defer r.seams.Withdraw(fault)
	r.seams.Resume(CheckpointChunkDone)
	res := <-got
	assert.Error(res.err)
	assert.Equal(1, res.removed, "the first chunk's child")
	var rm metricdata.ResourceMetrics
	assert.Must(assert.NoError(reader.Collect(ctx, &rm)))
	var reaped int64
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok && m.Name == "dwarf_flows_reaped" {
				for _, dp := range sum.DataPoints {
					reaped += dp.Value
				}
			}
		}
	}
	assert.Equal(int64(1), reaped, "counted though the next chunk failed")
}

// TestChunk_ChunkEnvelope is a qualification workload for chunk selection: a wide tree half of whose descendants are
// already deleted, a tree of one child of 100,000 steps, and a root of 100,000 steps with no child, each beside a flow
// of 50,000 unrelated steps. Every chunk stays well within the attempt - the sizing reads at most a budget's worth of
// steps, however large the flow, and a part-deleted flow is read from its position - and the tree is deleted whole. It logs the slowest chunk; it is not a hardware-independent guarantee.
func TestChunk_ChunkEnvelope(t *testing.T) {
	if testing.Short() {
		t.Skip("chunk selection envelope")
	}
	t.Parallel()
	for _, layout := range []string{"drained", "one_large_child", "one_large_root"} {
		t.Run(layout, func(t *testing.T) {
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 0)
			seedWideTree(t, r.db, 0, 0, 0)                  // an unrelated flow ahead of the tree
			unrelated := seedWideTree(t, r.db, 50000, 0, 0) // 50,000 unrelated steps
			var root int
			if layout == "drained" {
				root = seedWideTree(t, r.db, 10, 2000, 15) // 2,001 flows, 30,010 steps
				drained := treeFlows(t, r, root)[1:1001]
				_, err := r.db.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+intCSV(drained)+")")
				assert.Must(assert.NoError(err))
				_, err = r.db.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id IN ("+intCSV(drained)+")")
				assert.Must(assert.NoError(err))
			} else if layout == "one_large_child" {
				root = seedWideTree(t, r.db, 1, 1, 100000)
			} else {
				root = seedWideTree(t, r.db, 100000, 0, 0) // one flow: a forEach's branches all run in it
			}
			stampTree(t, r.db, root)
			r.cfg.BatchRows = 4096
			var slowest, first, last time.Duration
			var pos chunkPos
			chunks := 0
			for {
				start := time.Now()
				_, done, err := r.p().deleteChunk(ctx, root, &pos)
				last = time.Since(start)
				slowest = max(slowest, last)
				if chunks == 0 {
					first = last
				}
				assert.Must(assert.NoError(err))
				chunks++
				if done {
					break
				}
			}
			t.Logf("%s %s: %d chunks, slowest %v, first %v, last %v", r.db.DriverName(), layout, chunks, slowest, first, last)
			assert.True(slowest < r.cfg.Attempt/3, "slowest chunk %v", slowest)
			f, s, _ := rowsOf(t, r, root)
			assert.Zero(f + s)
			f, s, _ = rowsOf(t, r, unrelated)
			assert.Equal([2]int{1, 50000}, [2]int{f, s}, "the unrelated flow is untouched")
		})
	}
}
