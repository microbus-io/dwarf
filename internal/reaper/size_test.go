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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// setStepBytes gives every step of root's tree a payload of n bytes, as its byte counts say, and its flows one of
// flowBytes.
func setStepBytes(t *testing.T, r *rig, root, n, flowBytes int) {
	t.Helper()
	ctx := t.Context()
	if _, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET state_bytes=? WHERE flow_id IN"+
		" (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?)", n, root); err != nil {
		t.Fatal(err)
	}
	if _, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET graph_bytes=? WHERE root_flow_id=?", flowBytes, root); err != nil {
		t.Fatal(err)
	}
}

// TestSize_TreesAreSizedInRowsAndBytes pins a tree's size: its flow rows and step rows, and the sum of their byte
// counts, each read capped at its limit of rows.
func TestSize_TreesAreSizedInRowsAndBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedWideTree(t, r.db, 2, 3, 4) // a root of 2 steps, 3 children of 4: 4 flows, 14 steps
	setStepBytes(t, r, root, 100, 1000)
	got, err := treesSizesUpTo(ctx, r.db, live, []int{root}, []int{1000})
	assert.Must(assert.NoError(err))
	assert.Equal(size{18, 14*100 + 4*1000}, got[0])
	got, err = treesSizesUpTo(ctx, r.db, live, []int{root}, []int{5})
	assert.Must(assert.NoError(err))
	assert.True(got[0].rows >= 5, "read to its limit: %d rows", got[0].rows)
}

// TestSize_AFewHeavyRowsAreSizedByTheirBytes pins the byte caps of a pass: a tree of few rows whose bytes pass the
// batch's is deleted in chunks rather than in one transaction, and one whose bytes pass StageBytes is staged though
// its rows are far under StageRows.
func TestSize_AFewHeavyRowsAreSizedByTheirBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.cfg.BatchRows, r.cfg.BatchBytes = 4096, 10000
	heavy := seedWideTree(t, r.db, 1, 2, 3) // 3 flows, 7 steps
	setStepBytes(t, r, heavy, 3000, 0)      // 21,000 bytes
	light := seedWideTree(t, r.db, 1, 2, 3)

	cut, via, _, err := r.p().rootsWithin(ctx, []int{heavy, light}, false)
	assert.Must(assert.NoError(err))
	assert.Equal([]int{heavy}, cut, "alone: over the batch's bytes")
	assert.Equal(inChunks, via, "deleted in chunks")
	cut, via, _, err = r.p().rootsWithin(ctx, []int{light, heavy}, false)
	assert.Must(assert.NoError(err))
	assert.Equal([]int{light}, cut, "the light tree's batch ends before the heavy one")
	assert.Equal(inBatch, via)

	r.cfg.StageRows, r.cfg.StageBytes = 1000, 15000
	cut, via, _, err = r.p().rootsWithin(ctx, []int{heavy, light}, true)
	assert.Must(assert.NoError(err))
	assert.Equal([]int{heavy}, cut)
	assert.Equal(inStages, via, "staged by its bytes, 10 rows under 1,000")
	r.cfg.StageBytes = 0
	_, via, _, err = r.p().rootsWithin(ctx, []int{heavy, light}, true)
	assert.Must(assert.NoError(err))
	assert.Equal(inBatch, via, "with no byte threshold, moved in one transaction")
}

// TestSize_AChunkKeepsToItsBytes pins a chunk's byte budget: each chunk of a chunked deletion takes the steps whose
// bytes fit it, at least one, so a tree of heavy steps goes over several chunks however few its rows, and a step over
// the budget on its own still goes, alone.
func TestSize_AChunkKeepsToItsBytes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	root := seedWideTree(t, r.db, 6, 0, 0) // one flow of 6 steps
	setStepBytes(t, r, root, 3000, 0)
	stampTree(t, r.db, root) // backdated: a stamp in the seed's millisecond is not due, and its root is not deleted

	steps := func() int {
		_, n, _ := rowsOf(t, r, root)
		return n
	}
	var pos chunkPos
	_, done, err := r.reaper.chunk(ctx, r.db, liveTarget, root, capOf(4096, 7000), &pos)
	assert.Must(assert.NoError(err))
	assert.False(done)
	assert.Equal(4, steps(), "two steps of 3,000 bytes fit 7,000")
	_, done, err = r.reaper.chunk(ctx, r.db, liveTarget, root, capOf(4096, 1000), &pos)
	assert.Must(assert.NoError(err))
	assert.False(done)
	assert.Equal(3, steps(), "one step over the budget goes alone")
	for range 10 {
		if _, done, err = r.reaper.chunk(ctx, r.db, liveTarget, root, capOf(4096, 7000), &pos); err != nil || done {
			break
		}
	}
	assert.NoError(err)
	assert.True(done)
	_, _, there := rowsOf(t, r, root)
	assert.False(there, "the tree is gone")
}

// TestSize_AStagingChunkKeepsToItsBytes pins the staged copy's byte budget: a tree staged by its bytes is copied over
// several chunks, each within the batch's bytes, and published whole.
func TestSize_AStagingChunkKeepsToItsBytes(t *testing.T) {
	t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestSize_AStagingChunkKeepsToItsBytes")
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	r := newRig(t, 30)
	r.cfg.StageRows, r.cfg.StageBytes = 1000, 5000
	r.cfg.BatchRows, r.cfg.BatchBytes = 4096, 7000
	oneChunk(r)
	today, err := r.archive.Today(ctx)
	assert.Must(assert.NoError(err))
	root := seedWideTree(t, r.db, 2, 3, 2) // 4 flows, 8 steps
	setStepBytes(t, r, root, 3000, 0)      // 24,000 bytes
	var copied []int
	for range 40 {
		if _, _, there := rowsOf(t, r, root); !there {
			break
		}
		r.pass(ctx)
		var n int
		assert.Must(assert.NoError(r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+migrations.StepsTable(today)).Scan(&n)))
		copied = append(copied, n)
	}
	if now, _ := r.archive.Today(ctx); now != today {
		t.Skip("midnight passed during the test")
	}
	assert.True(archivedByStages(t, r, today, root), "staged")
	assert.Equal(4, visibleInDay(t, r, today, root), "published whole: its four flows")
	last := 0
	for _, n := range copied {
		assert.True(n-last <= 2*2, "at most two steps of 3,000 bytes a chunk, two chunks a pass: %v", copied)
		last = n
	}
}

// TestSize_SmallTreesMoveTogether pins the path of ordinary trees, with staged archival off and on at realistic
// thresholds: a pass moves them all in one transaction - one archive time for every flow of every tree - with no
// attempt started and nothing counted as staged.
func TestSize_SmallTreesMoveTogether(t *testing.T) {
	t.Skip("janitor step 2: the fast walk of closed trees runs in the janitor; see internal/janitor TestSize_SmallTreesMoveTogether")
	t.Parallel()
	for _, staging := range []bool{false, true} {
		t.Run(map[bool]string{false: "staging_off", true: "staging_on"}[staging], func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 30)
			r.cfg.Batch, r.cfg.BatchRows, r.cfg.BatchBytes = 512, 4096, 16<<20
			if staging {
				r.cfg.StageRows, r.cfg.StageBytes = 131072, 64<<20
			}
			reader := sdkmetric.NewManualReader()
			assert.Must(assert.NoError(r.reaper.SetMeter(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))))
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
			assert.Zero(stagedRows(t, reader), "nothing staged")
		})
	}
}

// treeIDs returns the flow ids of root's live tree.
func treeIDs(t *testing.T, r *rig, root int) []int {
	t.Helper()
	rows, err := r.db.QueryContext(t.Context(), "SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?", root)
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

// treeSize sizes the rows of the flows ids in t, and their steps, by their byte counts: by id, since a staged copy
// writes a flow's steps before its row.
func treeSize(t *testing.T, r *rig, flows, steps string, ids []int) size {
	t.Helper()
	var f, s size
	ctx := t.Context()
	list := intCSV(ids)
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM("+flowBytesSQL("")+"), 0) FROM "+flows+
		" WHERE flow_id IN ("+list+")").Scan(&f.rows, &f.bytes); err != nil {
		t.Fatal(err)
	}
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*), COALESCE(SUM("+stepBytesSQL("")+"), 0) FROM "+steps+
		" WHERE flow_id IN ("+list+")").Scan(&s.rows, &s.bytes); err != nil {
		t.Fatal(err)
	}
	return f.plus(s)
}

// TestSize_AFlowRowIsChargedItsBytes pins a flow row's bytes in a chunk's budget: a flow row goes with the last of
// its steps only when both fit what is left, and otherwise first in the next chunk, so no chunk - of a chunked deletion
// or of a staged copy - passes its bytes unless it took one row alone. Each tree has heavy flow rows: a flow of two
// 3,000-byte steps and a 6,000-byte row, and a root over a child of three such steps, resumed with one left.
func TestSize_AFlowRowIsChargedItsBytes(t *testing.T) {
	t.Parallel()
	const budget = 7000
	check := func(t *testing.T, assert *testarossa.Asserter, step func() (done bool), moved func() size) {
		t.Helper()
		before := moved()
		for range 20 {
			done := step()
			now := moved()
			took := size{now.rows - before.rows, now.bytes - before.bytes}
			assert.True(took.bytes <= budget || took.rows == 1, "a chunk took %d rows of %d bytes", took.rows, took.bytes)
			before = now
			if done {
				return
			}
		}
		t.Fatal("not finished in 20 chunks")
	}
	shapes := []struct {
		name                    string
		rootSteps, kids, kidSts int
	}{
		{"one_flow", 2, 0, 0},
		{"a_child", 1, 1, 3}, // the child resumed part done: its last step and its row together pass the budget
	}
	for _, shape := range shapes {
		t.Run("delete_"+shape.name, func(t *testing.T) {
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 30)
			root := seedWideTree(t, r.db, shape.rootSteps, shape.kids, shape.kidSts)
			setStepBytes(t, r, root, 3000, 6000)
			stampTree(t, r.db, root) // backdated: a stamp in the seed's millisecond is not due, and its root is not deleted
			ids := treeIDs(t, r, root)
			whole := treeSize(t, r, "dwarf_flows", "dwarf_steps", ids)
			var pos chunkPos
			check(t, assert, func() bool {
				_, done, err := r.reaper.chunk(ctx, r.db, liveTarget, root, capOf(4096, budget), &pos)
				assert.Must(assert.NoError(err))
				return done
			}, func() size {
				left := treeSize(t, r, "dwarf_flows", "dwarf_steps", ids)
				return size{whole.rows - left.rows, whole.bytes - left.bytes}
			})
			_, _, there := rowsOf(t, r, root)
			assert.False(there, "the tree is gone")
		})
		t.Run("stage_"+shape.name, func(t *testing.T) {
			t.Skip("janitor step 5: the staged copy runs in the janitor's large-tree archiving; see internal/janitor TestSize_AStagedFlowRowIsChargedItsBytes")
			t.Parallel()
			assert := testarossa.For(t)
			ctx := t.Context()
			r := newRig(t, 30)
			r.cfg.StageRows, r.cfg.BatchRows, r.cfg.BatchBytes = 1, 4096, budget
			today, err := r.archive.Today(ctx)
			assert.Must(assert.NoError(err))
			root := seedWideTree(t, r.db, shape.rootSteps, shape.kids, shape.kidSts)
			setStepBytes(t, r, root, 3000, 6000)
			ids := treeIDs(t, r, root)
			d := &filingDay{}
			assert.Must(assert.NoError(r.p().refreshDay(ctx, d)))
			at, ok, err := r.p().claimAttempt(ctx, closedDue(0, 0), root, d, time.Time{})
			assert.Must(assert.NoError(err))
			assert.Must(assert.True(ok, "claimed"))
			pos := stagePos{at: at}
			check(t, assert, func() bool {
				n, _, done, err := r.p().stageChunk(ctx, root, r.archive, &pos)
				assert.Must(assert.NoError(err))
				return n > 0 || done
			}, func() size {
				return treeSize(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), ids)
			})
			if now, _ := r.archive.Today(ctx); now != today {
				t.Skip("midnight passed during the test")
			}
			assert.Equal(treeSize(t, r, "dwarf_flows", "dwarf_steps", ids),
				treeSize(t, r, migrations.FlowsTable(today), migrations.StepsTable(today), ids), "copied whole")
		})
	}
}
