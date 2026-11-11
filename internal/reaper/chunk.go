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
	"strconv"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// removal separates physical live cleanup from removal without an archive copy.
type removal struct{ flows, unarchived int }

// treeChunk is how many descendant flows one statement of a chunked deletion names: a literal IN list of a size every
// dialect plans.
const treeChunk = 256

// deleteLarge deletes the live tree of root in transactions of about a batch each, until it is gone, the walk's
// slice (begun at sliceStart) ends, or ctx is done, and counts each committed chunk's flows under reason. A root that
// is not stamped yet - the closed kind at a retention of 0, the retention kind - is stamped first, in a transaction of
// its own, so every chunk after it is authorized by the stamp, and a pass that stops part-way leaves a stamped tree the
// deletion kind finishes. finished is true when the tree is gone or is not this walk's to delete: no longer due, or
// held by another transaction. removed counts the flows of the chunks that committed, an error after them included.
func (p *pass) deleteLarge(ctx context.Context, k kind, root int, reason string, sliceStart time.Time) (removed int, finished bool, err error) {
	if k.name != KindDeleted {
		stamped, err := p.stamp(ctx, k, root)
		if err != nil || !stamped {
			return 0, err == nil, errors.Trace(err)
		}
	}
	var pos chunkPos
	for {
		n, done, err := p.deleteChunk(ctx, root, &pos)
		removed += n.flows
		p.r.metrics.flowsReaped(ctx, p.shard.Index, reason, n.unarchived)
		if err != nil {
			return removed, false, errors.Trace(err)
		}
		p.r.seams.Checkpoint(ctx, CheckpointChunkDone)
		if done {
			return removed, true, nil
		}
		if ctx.Err() != nil || time.Since(sliceStart) >= p.cfg.Slice {
			return removed, false, nil
		}
	}
}

// stamp marks root for deletion, due at once, if it is still due for k: the durable authorization every later chunk
// rechecks. It reports whether it stamped it.
func (p *pass) stamp(ctx context.Context, k kind, root int) (bool, error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	// The stamp clears any archive attempt, as every stamp but a publication does: a tree stamped with an attempt is a
	// published one.
	none, noneArg := NoAttemptSQL(p.shard.DB.DriverName())
	res, err := p.shard.DB.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1, archive_attempt_at="+none+
		" WHERE flow_id=? AND "+k.due, append([]any{noneArg, root}, k.dueArgs...)...)
	if err != nil {
		return false, errors.Trace(err)
	}
	n, err := res.RowsAffected()
	return n == 1, errors.Trace(err)
}

// chunkPos is where the chunks of one deleteLarge stand on a tree: every descendant from below up to top is deleted,
// and of stepFlow, a flow part of whose steps are deleted, every step up to stepAfter. It lives in memory only, for one
// call: a fresh call starts again from the newest descendant and the first step. Keeping it lets each chunk's reads
// seek past the entries the earlier chunks deleted, which stay in the indexes until vacuum: read from the start, a
// flow of a million steps would cost each chunk every step deleted before it.
type chunkPos struct {
	top, below          int
	stepFlow, stepAfter int
}

// stepsFrom is where a chunk resumes reading flow's steps: after the position when flow is the one part deleted.
func (c chunkPos) stepsFrom(flow int) int {
	if c.stepFlow == flow {
		return c.stepAfter
	}
	return 0
}

// chunkTarget is what a chunked deletion deletes from: the tables, the condition the root must still meet under its
// lock, and whether the directory names the tables' flows (an archive day's).
type chunkTarget struct {
	t         tables
	due       string
	directory bool
}

// liveTarget is the live tables, their roots stamped for deletion and due.
var liveTarget = chunkTarget{t: live, due: deletionDue(0).due}

// archiveTarget is day's archive tables, their roots marked for deletion.
func archiveTarget(day int) chunkTarget {
	return chunkTarget{
		t:         tables{flows: migrations.FlowsTable(day), steps: migrations.StepsTable(day)},
		due:       "delete_after_ms>0 AND surgraph_flow_id=0",
		directory: true,
	}
}

// deleteChunk is chunk on the live tables, under the attempt deadline, with the pass's batch as its budget.
func (p *pass) deleteChunk(ctx context.Context, root int, pos *chunkPos) (deleted removal, done bool, err error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	return p.r.chunk(ctx, p.shard.DB, liveTarget, root, p.batchLimit(), pos)
}

// chunk deletes, in one transaction under the root's lock, up to about budget's rows and bytes of the tree of root in
// tgt: its descendant flows newest first, each flow's steps and then the flow, and once none is left the root's
// steps, then the root itself after a recheck that the tree holds nothing else. It returns how many flows it deleted,
// and done when the tree is gone or is not this call's: the root no longer meets tgt.due, or is held by another
// transaction. On an archive day the directory's rows for the deleted flows go with them.
// pos advances only when the chunk commits. Its result separates physical removals from unarchived live removals.
//
// Every read is bounded by the budget, not by the tree: which descendants fit is decided by sizing at most the
// budget's worth of their steps, halving the run of descendants until it fits, so a flow of a million steps costs a
// chunk no more than one of a thousand. A chunk always takes at least one row, so one row over the byte budget still
// goes, alone.
func (r *Reaper) chunk(ctx context.Context, db *sequel.DB, tgt chunkTarget, root int, budget limit, pos *chunkPos) (deleted removal, done bool, err error) {
	id := strconv.Itoa(root)
	flowsTable, stepsTable := tgt.t.flows, tgt.t.steps
	// dropFlows deletes the flows in list, and the directory's rows for them on an archive day.
	dropFlows := func(tx *sequel.Tx, list string) (int, error) {
		if tgt.directory {
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+archiver.DirectoryTable+" WHERE flow_id IN ("+list+")"); err != nil {
				return 0, errors.Trace(err)
			}
		}
		res, err := tx.ExecContext(ctx, "DELETE FROM "+flowsTable+" WHERE flow_id IN ("+list+")")
		if err != nil {
			return 0, errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		return int(n), nil
	}
	var at chunkPos
	var published int
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		deleted, done, at = removal{}, false, *pos
		published = 0
		locked, err := lockRoots(ctx, tx, flowsTable, true, "flow_id="+id+" AND "+tgt.due)
		if err != nil {
			return errors.Trace(err)
		}
		if len(locked) == 0 {
			done = true
			return nil
		}
		if !tgt.directory {
			cond, args := publishedSQL(tx.DriverName())
			if err := tx.QueryRowContext(ctx, "SELECT CASE WHEN "+cond+" THEN 1 ELSE 0 END FROM dwarf_flows WHERE flow_id="+id, args...).Scan(&published); err != nil {
				return errors.Trace(err)
			}
		}
		left := budget
		for !left.spent() {
			below := ""
			if at.below > 0 {
				below = " AND flow_id<" + strconv.Itoa(at.below)
			}
			children, childBytes, err := intPairs(ctx, tx, "SELECT flow_id, "+flowBytesSQL("")+" FROM "+flowsTable+
				" WHERE root_flow_id="+id+" AND flow_id<>"+id+below+" ORDER BY flow_id DESC LIMIT_OFFSET(?, 0)", treeChunk)
			if err != nil {
				return errors.Trace(err)
			}
			if len(children) == 0 {
				break
			}
			at.top = max(at.top, children[0])
			if at.stepFlow == children[0] {
				// A child part deleted by an earlier chunk: go on from its position, without sizing it again.
				fresh := left == budget
				sz, last, more, err := deleteStepsFrom(ctx, tx, stepsTable, children[0], at.stepAfter, left, fresh)
				if err != nil {
					return errors.Trace(err)
				}
				if more || !rowFits(left, fresh, sz, childBytes[0]) {
					at.stepAfter = last
					return errors.Trace(r.midTree(root))
				}
				if err := r.midTree(root); err != nil {
					return errors.Trace(err)
				}
				flows, err := dropFlows(tx, strconv.Itoa(children[0]))
				if err != nil {
					return errors.Trace(err)
				}
				deleted.flows += flows
				left = left.minus(sz.plus(size{1, childBytes[0]}))
				at.below, at.stepFlow, at.stepAfter = children[0], 0, 0
				continue
			}
			run, sz, err := fitRun(ctx, tx, stepsTable, children, childBytes, left)
			if err != nil {
				return errors.Trace(err)
			}
			if run == 0 {
				// The newest descendant alone is over what is left: part of its steps now, the rest - and its flow row -
				// in the next chunk.
				_, last, _, err := deleteStepsFrom(ctx, tx, stepsTable, children[0], 0, left, left == budget)
				if err != nil {
					return errors.Trace(err)
				}
				at.stepFlow, at.stepAfter = children[0], last
				return errors.Trace(r.midTree(root))
			}
			list := intCSV(children[:run])
			if _, err := tx.ExecContext(ctx, "DELETE FROM "+stepsTable+" WHERE flow_id IN ("+list+")"); err != nil {
				return errors.Trace(err)
			}
			if err := r.midTree(root); err != nil {
				return errors.Trace(err)
			}
			flows, err := dropFlows(tx, list)
			if err != nil {
				return errors.Trace(err)
			}
			deleted.flows += flows
			left = left.minus(sz)
			at.below = children[run-1]
		}
		if left.spent() {
			return nil
		}
		from := at.stepsFrom(root)
		fresh := left == budget
		sz, last, more, err := deleteStepsFrom(ctx, tx, stepsTable, root, from, left, fresh)
		if err != nil {
			return errors.Trace(err)
		}
		if more {
			at.stepFlow, at.stepAfter = root, last // the root may hold more steps than the budget left
			return nil
		}
		if sz.rows > 0 {
			from = last
		}
		rootBytes, err := flowBytes(ctx, tx, flowsTable, root)
		if err != nil {
			return errors.Trace(err)
		}
		if !rowFits(left, fresh, sz, rootBytes) {
			at.stepFlow, at.stepAfter = root, from
			return nil
		}
		if err := r.midTree(root); err != nil {
			return errors.Trace(err)
		}
		r.seams.Checkpoint(ctx, CheckpointBeforeRootDelete)
		// The root goes last, and only once the tree holds nothing else: two probes, not an EXISTS over the tree,
		// which MariaDB materializes over the whole steps index. The walk just found no descendant below its position,
		// and deleted those between it and top, so a descendant left can only be one newer than top: the probe seeks
		// there rather than through the entries the chunks deleted. On MySQL and MariaDB the probes are locking reads:
		// a plain read there sees the snapshot the chunk's first plain read took, so it would miss a row committed since.
		locking := ""
		if d := tx.DriverName(); d == "mysql" || d == "mariadb" {
			locking = " FOR UPDATE"
		}
		rest, err := ints(ctx, tx, "SELECT flow_id FROM "+flowsTable+" WHERE root_flow_id="+id+" AND flow_id>"+
			strconv.Itoa(max(at.top, root))+" ORDER BY flow_id LIMIT_OFFSET(?, 0)"+locking, 1)
		if err != nil || len(rest) > 0 {
			return errors.Trace(err)
		}
		rest, err = ints(ctx, tx, "SELECT step_id FROM "+stepsTable+" WHERE flow_id="+id+" AND step_id>"+strconv.Itoa(from)+
			" ORDER BY step_id LIMIT_OFFSET(?, 0)"+locking, 1)
		if err != nil || len(rest) > 0 {
			return errors.Trace(err)
		}
		rows, err := dropFlows(tx, id)
		if err != nil {
			return errors.Trace(err)
		}
		deleted.flows += rows
		done = true
		return nil
	})
	if err != nil {
		return removal{}, false, errors.Trace(err)
	}
	if !tgt.directory && published == 0 {
		deleted.unarchived = deleted.flows
	}
	*pos = at
	return deleted, done, nil
}

// fitRun returns the longest leading run of flows, halving from all of them, whose steps and flow rows fit budget, and
// their size; run 0 when the first flow alone is over it. flowBytes are the flows' own payload bytes. Each probe reads
// at most the budget's worth of steps.
func fitRun(ctx context.Context, tx *sequel.Tx, steps string, flows, flowBytes []int, budget limit) (run int, sz size, err error) {
	for run = len(flows); run > 0; run /= 2 {
		own := size{rows: run}
		for _, b := range flowBytes[:run] {
			own.bytes += b
		}
		room := budget.minus(own)
		if room.rows < 0 || room.bytes < 0 {
			continue
		}
		st, err := stepsSizeUpTo(ctx, tx, steps, flows[:run], room.rows+1)
		if err != nil {
			return 0, size{}, errors.Trace(err)
		}
		if !room.over(st) {
			return run, st.plus(own), nil
		}
	}
	return 0, size{}, nil
}

// stepsSizeUpTo sizes the steps of flows, reading at most limit of them: enough to tell whether they fit a budget,
// whatever their number.
func stepsSizeUpTo(ctx context.Context, q sequel.Executor, steps string, flows []int, limit int) (sz size, err error) {
	driver := q.DriverName()
	inner := capped(driver, stepBytesSQL("")+" AS b FROM "+steps+" WHERE flow_id IN ("+intCSV(flows)+")", limit)
	err = q.QueryRowContext(ctx, "SELECT COUNT(*), "+sumSQL(driver, "b")+" FROM ("+inner+") c").Scan(&sz.rows, &sz.bytes)
	return sz, errors.Trace(err)
}

// treesSizesUpTo sizes each tree rooted at roots in t - its flow rows and its step rows, and their payload bytes - each
// read up to its own limit of rows, in one statement (treesSizesUpToSQL), so a group of trees costs one round trip and
// no tree is read past its limit. A tree read up to its limit reports at least that many rows.
func treesSizesUpTo(ctx context.Context, q sequel.Executor, t tables, roots, limits []int) ([]size, error) {
	rows, err := q.QueryContext(ctx, treesSizesUpToSQL(q.DriverName(), t, roots, limits))
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	sizes := make([]size, len(roots))
	for rows.Next() {
		var k int
		var sz size
		if err := rows.Scan(&k, &sz.rows, &sz.bytes); err != nil {
			return nil, errors.Trace(err)
		}
		sizes[k] = sizes[k].plus(sz)
	}
	return sizes, errors.Trace(rows.Err())
}

// treesSizesUpToSQL is treesSizesUpTo's statement: a UNION ALL of two capped reads a tree, its flows by root and their
// steps by flow. On PostgreSQL the steps are read through a LATERAL join and on SQL Server a CROSS APPLY, each flow's
// steps a seek into its own: a plain join there may be planned as a hash join over a scan of every step in the table,
// which the LIMIT stops early only for a tree larger than it, so a small tree costs the whole table. MySQL, MariaDB and
// SQLite join an indexed equality by nested loops. Each read is capped on its own, since a LIMIT over a UNION ALL is
// applied after MySQL materializes it whole.
func treesSizesUpToSQL(driver string, t tables, roots, limits []int) string {
	branches := make([]string, 0, 2*len(roots))
	for k, root := range roots {
		n, key, id := strconv.Itoa(limits[k]), strconv.Itoa(k), strconv.Itoa(root)
		steps := stepBytesSQL("s.") + " AS b FROM " + t.flows + " f JOIN " + t.steps + " s ON s.flow_id=f.flow_id"
		switch driver {
		case "pgx":
			steps = "s.b FROM " + t.flows + " f CROSS JOIN LATERAL (SELECT " + stepBytesSQL("s.") + " AS b FROM " + t.steps +
				" s WHERE s.flow_id=f.flow_id LIMIT " + n + ") s"
		case "mssql":
			steps = "s.b FROM " + t.flows + " f CROSS APPLY (SELECT TOP (" + n + ") " + stepBytesSQL("s.") + " AS b FROM " +
				t.steps + " s WHERE s.flow_id=f.flow_id) s"
		}
		flows := capped(driver, flowBytesSQL("")+" AS b FROM "+t.flows+" WHERE root_flow_id="+id, limits[k])
		stepRows := capped(driver, steps+" WHERE f.root_flow_id="+id, limits[k])
		agg := "SELECT " + key + " AS k, COUNT(*) AS n, " + sumSQL(driver, "b") + " AS b FROM ("
		branches = append(branches, agg+flows+") cf"+key, agg+stepRows+") cs"+key)
	}
	return strings.Join(branches, " UNION ALL ")
}

// capped is "SELECT "+selectFrom reading at most limit rows: a LIMIT, or a TOP on SQL Server.
func capped(driver, selectFrom string, limit int) string {
	if driver == "mssql" {
		return "SELECT TOP (" + strconv.Itoa(limit) + ") " + selectFrom
	}
	return "SELECT " + selectFrom + " LIMIT " + strconv.Itoa(limit)
}

// deleteStepsFrom deletes steps of flow after the step after, in step_id order, up to budget - at least one, whatever
// its bytes, when fresh - and returns their size, the last one's id, and more when the budget, not the flow, ended
// them. The ids are read first and deleted by a literal list: a range or an ORDER BY with a limit on the DELETE is not
// portable, and the read is a seek into (flow_id, step_id) whatever else the table holds.
func deleteStepsFrom(ctx context.Context, tx *sequel.Tx, steps string, flow, after int, budget limit, fresh bool) (sz size, last int, more bool, err error) {
	ids, sz, more, err := stepsWithin(ctx, tx, steps, flow, after, budget, fresh)
	if err != nil || len(ids) == 0 {
		return size{}, after, more, errors.Trace(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+steps+" WHERE step_id IN ("+intCSV(ids)+")"); err != nil {
		return size{}, after, false, errors.Trace(err)
	}
	return sz, ids[len(ids)-1], more, nil
}

// stepsWithin returns the leading steps of flow in steps after the step after, in step_id order, that fit budget; their
// size; and more when the budget, not the flow, ended them. When fresh - nothing else in the chunk yet - it takes at
// least one, so a step over the whole budget goes alone; otherwise it may take none, and the chunk ends there. It reads
// up to budget's rows, a seek into (flow_id, step_id) whatever else the table holds.
func stepsWithin(ctx context.Context, tx *sequel.Tx, steps string, flow, after int, budget limit, fresh bool) (ids []int, sz size, more bool, err error) {
	read, bytes, err := intPairs(ctx, tx, "SELECT step_id, "+stepBytesSQL("")+" FROM "+steps+" WHERE flow_id="+strconv.Itoa(flow)+
		" AND step_id>"+strconv.Itoa(after)+" ORDER BY step_id LIMIT_OFFSET(?, 0)", max(budget.rows, 1))
	if err != nil {
		return nil, size{}, false, errors.Trace(err)
	}
	for k := range read {
		if (k > 0 || !fresh) && sz.bytes+bytes[k] > budget.bytes {
			return read[:k], sz, true, nil
		}
		sz = sz.plus(size{1, bytes[k]})
	}
	return read, sz, len(read) >= max(budget.rows, 1), nil
}

// intPairs runs a query of two integer columns and returns their values.
func intPairs(ctx context.Context, tx *sequel.Tx, query string, args ...any) (a, b []int, err error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var x, y int
		if err := rows.Scan(&x, &y); err != nil {
			return nil, nil, errors.Trace(err)
		}
		a, b = append(a, x), append(b, y)
	}
	return a, b, errors.Trace(rows.Err())
}

// ints runs a query of one integer column and returns its values.
func ints(ctx context.Context, tx *sequel.Tx, query string, args ...any) ([]int, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	var out []int
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, errors.Trace(err)
		}
		out = append(out, v)
	}
	return out, errors.Trace(rows.Err())
}

// midTree is FaultMidTree's consult: after a tree's steps, before its flows, plain or targeted at one of roots.
func (r *Reaper) midTree(roots ...int) error {
	if r.seams.IsFault(FaultMidTree) {
		return errors.New("injected fault: " + FaultMidTree)
	}
	return r.faultOn(FaultMidTree, roots...)
}

// faultOn fails when fault is injected targeted at any of roots.
func (r *Reaper) faultOn(fault string, roots ...int) error {
	if !r.seams.Enabled() {
		return nil
	}
	for _, root := range roots {
		if r.seams.IsFault(join(fault, strconv.Itoa(root))) {
			return errors.New("injected fault: " + fault)
		}
	}
	return nil
}
