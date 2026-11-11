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

// deleteLarge deletes the live tree of root in transactions of about BatchRows rows each, until it is gone, the walk's
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
	return p.r.chunk(ctx, p.shard.DB, liveTarget, root, p.cfg.BatchRows, pos)
}

// chunk deletes, in one transaction under the root's lock, up to about budget rows of the tree of root in tgt: its descendant flows newest first, each flow's steps and then the flow, and once none is left the root's
// steps, then the root itself after a recheck that the tree holds nothing else. It returns how many flows it deleted,
// and done when the tree is gone or is not this call's: the root no longer meets tgt.due, or is held by another
// transaction. On an archive day the directory's rows for the deleted flows go with them.
// pos advances only when the chunk commits. Its result separates physical removals from unarchived live removals.
//
// Every read is bounded by the budget, not by the tree: which descendants fit is decided by counting at most the
// budget's worth of their steps, halving the run of descendants until it fits, so a flow of a million steps costs a
// chunk no more than one of a thousand.
func (r *Reaper) chunk(ctx context.Context, db *sequel.DB, tgt chunkTarget, root, budget int, pos *chunkPos) (deleted removal, done bool, err error) {
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
		budget := max(budget, 1)
		for budget > 0 {
			below := ""
			if at.below > 0 {
				below = " AND flow_id<" + strconv.Itoa(at.below)
			}
			children, err := ints(ctx, tx, "SELECT flow_id FROM "+flowsTable+" WHERE root_flow_id="+id+" AND flow_id<>"+id+
				below+" ORDER BY flow_id DESC LIMIT_OFFSET(?, 0)", treeChunk)
			if err != nil {
				return errors.Trace(err)
			}
			if len(children) == 0 {
				break
			}
			at.top = max(at.top, children[0])
			if at.stepFlow == children[0] {
				// A child part deleted by an earlier chunk: go on from its position, without sizing it again.
				n, last, err := deleteStepsFrom(ctx, tx, stepsTable, children[0], at.stepAfter, budget)
				if err != nil {
					return errors.Trace(err)
				}
				if n == budget {
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
				budget -= n + 1
				at.below, at.stepFlow, at.stepAfter = children[0], 0, 0
				continue
			}
			// The longest leading run whose steps and flow rows fit what is left of the budget.
			run := len(children)
			for run > 0 {
				room := budget - run
				if room >= 0 {
					n, err := stepsUpTo(ctx, tx, stepsTable, children[:run], room+1)
					if err != nil {
						return errors.Trace(err)
					}
					if n <= room {
						break
					}
				}
				run /= 2
			}
			if run == 0 {
				// The newest descendant alone is over what is left: part of its steps now, the rest in the next chunk.
				_, last, err := deleteStepsFrom(ctx, tx, stepsTable, children[0], 0, budget)
				if err != nil {
					return errors.Trace(err)
				}
				at.stepFlow, at.stepAfter = children[0], last
				return errors.Trace(r.midTree(root))
			}
			list := intCSV(children[:run])
			res, err := tx.ExecContext(ctx, "DELETE FROM "+stepsTable+" WHERE flow_id IN ("+list+")")
			if err != nil {
				return errors.Trace(err)
			}
			steps, _ := res.RowsAffected()
			if err := r.midTree(root); err != nil {
				return errors.Trace(err)
			}
			flows, err := dropFlows(tx, list)
			if err != nil {
				return errors.Trace(err)
			}
			deleted.flows += flows
			budget -= int(steps) + run
			at.below = children[run-1]
		}
		if budget <= 0 {
			return nil
		}
		from := at.stepsFrom(root)
		n, last, err := deleteStepsFrom(ctx, tx, stepsTable, root, from, budget)
		if err != nil {
			return errors.Trace(err)
		}
		if n == budget {
			at.stepFlow, at.stepAfter = root, last // the root may hold more steps than the budget left
			return nil
		}
		if n > 0 {
			from = last
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

// stepsUpTo counts the steps of flows, reading at most limit of them: enough to tell whether they fit a budget,
// whatever their number.
func stepsUpTo(ctx context.Context, q sequel.Executor, steps string, flows []int, limit int) (int, error) {
	return countUpTo(ctx, q, "step_id FROM "+steps+" WHERE flow_id IN ("+intCSV(flows)+")", limit)
}

// treeRowsUpTo counts the rows of the tree of root in t, as rootsWithinRowsIn sizes a tree (a flow with no steps counts
// one), up to limit.
func treeRowsUpTo(ctx context.Context, q sequel.Executor, t tables, root, limit int) (int, error) {
	return countUpTo(ctx, q, "f.flow_id FROM "+t.flows+" f LEFT JOIN "+t.steps+" s ON s.flow_id=f.flow_id WHERE f.root_flow_id="+
		strconv.Itoa(root), limit)
}

// treesRowsUpTo counts the rows of each tree rooted at roots in t, as treeRowsUpTo does, each up to its own limit, in
// one statement: a UNION ALL of capped counts, so a group of trees costs one round trip and no tree is read past its
// limit.
func treesRowsUpTo(ctx context.Context, q sequel.Executor, t tables, roots, limits []int) ([]int, error) {
	branches := make([]string, len(roots))
	for k, root := range roots {
		inner := "SELECT f.flow_id FROM " + t.flows + " f LEFT JOIN " + t.steps + " s ON s.flow_id=f.flow_id WHERE f.root_flow_id=" +
			strconv.Itoa(root) + " LIMIT " + strconv.Itoa(limits[k])
		if q.DriverName() == "mssql" {
			inner = "SELECT TOP (" + strconv.Itoa(limits[k]) + ") f.flow_id FROM " + t.flows + " f LEFT JOIN " + t.steps +
				" s ON s.flow_id=f.flow_id WHERE f.root_flow_id=" + strconv.Itoa(root)
		}
		branches[k] = "SELECT " + strconv.Itoa(k) + " AS k, COUNT(*) AS n FROM (" + inner + ") c" + strconv.Itoa(k)
	}
	rows, err := q.QueryContext(ctx, strings.Join(branches, " UNION ALL "))
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	counts := make([]int, len(roots))
	for rows.Next() {
		var k, n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, errors.Trace(err)
		}
		counts[k] = n
	}
	return counts, errors.Trace(rows.Err())
}

// countUpTo counts the rows of "SELECT "+selectFrom, up to limit: a COUNT over a LIMIT or TOP, so it reads at most limit
// rows however many match.
func countUpTo(ctx context.Context, q sequel.Executor, selectFrom string, limit int) (int, error) {
	inner := "SELECT " + selectFrom + " LIMIT " + strconv.Itoa(limit)
	if q.DriverName() == "mssql" {
		inner = "SELECT TOP (" + strconv.Itoa(limit) + ") " + selectFrom
	}
	var n int
	err := q.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+inner+") c").Scan(&n)
	return n, errors.Trace(err)
}

// deleteStepsFrom deletes up to limit steps of flow after the step after, in step_id order, and returns how many it
// read and deleted, and the last one's id. The ids are read first and deleted by a literal list: a range or an ORDER
// BY with a limit on the DELETE is not portable, and the read is a seek into (flow_id, step_id) whatever else the table
// holds.
func deleteStepsFrom(ctx context.Context, tx *sequel.Tx, steps string, flow, after, limit int) (n, last int, err error) {
	ids, err := ints(ctx, tx, "SELECT step_id FROM "+steps+" WHERE flow_id="+strconv.Itoa(flow)+" AND step_id>"+
		strconv.Itoa(after)+" ORDER BY step_id LIMIT_OFFSET(?, 0)", limit)
	if err != nil || len(ids) == 0 {
		return 0, after, errors.Trace(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM "+steps+" WHERE step_id IN ("+intCSV(ids)+")"); err != nil {
		return 0, after, errors.Trace(err)
	}
	return len(ids), ids[len(ids)-1], nil
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

// midTree is FaultMidTree's consult in a chunk: after its steps, before its flows, plain or targeted at root.
func (r *Reaper) midTree(root int) error {
	if r.seams.Enabled() && (r.seams.IsFault(FaultMidTree) || r.seams.IsFault(join(FaultMidTree, strconv.Itoa(root)))) {
		return errors.New("injected fault: " + FaultMidTree)
	}
	return nil
}
