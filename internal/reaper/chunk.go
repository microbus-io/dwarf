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
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

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
		removed += n
		p.r.metrics.flowsReaped(ctx, p.shard.Index, reason, n)
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
	res, err := p.shard.DB.ExecContext(ctx, "UPDATE dwarf_flows SET delete_after_ms=1 WHERE flow_id=? AND "+k.due,
		append([]any{root}, k.dueArgs...)...)
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

// deleteChunk deletes, in one transaction under the root's lock, up to about BatchRows rows of the stamped tree of
// root: its descendant flows newest first, each flow's steps and then the flow, and once none is left the root's
// steps, then the root itself after a recheck that the tree holds nothing else. It returns how many flows it deleted,
// and done when the tree is gone or is not this walk's: the root not stamped and due, or held by another transaction.
// pos advances only when the chunk commits.
//
// Every read is bounded by the budget, not by the tree: which descendants fit is decided by counting at most the
// budget's worth of their steps, halving the run of descendants until it fits, so a flow of a million steps costs a
// chunk no more than one of a thousand.
func (p *pass) deleteChunk(ctx context.Context, root int, pos *chunkPos) (deleted int, done bool, err error) {
	ctx, cancel := p.attemptCtx(ctx)
	defer cancel()
	id := strconv.Itoa(root)
	var at chunkPos
	err = p.shard.DB.Transact(ctx, func(tx *sequel.Tx) error {
		deleted, done, at = 0, false, *pos
		locked, err := lockRoots(ctx, tx, live.flows, true, "flow_id="+id+" AND "+deletionDue(0).due)
		if err != nil {
			return errors.Trace(err)
		}
		if len(locked) == 0 {
			done = true
			return nil
		}
		budget := max(p.cfg.BatchRows, 1)
		for budget > 0 {
			below := ""
			if at.below > 0 {
				below = " AND flow_id<" + strconv.Itoa(at.below)
			}
			children, err := ints(ctx, tx, "SELECT flow_id FROM dwarf_flows WHERE root_flow_id="+id+" AND flow_id<>"+id+
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
				n, last, err := deleteStepsFrom(ctx, tx, children[0], at.stepAfter, budget)
				if err != nil {
					return errors.Trace(err)
				}
				if n == budget {
					at.stepAfter = last
					return errors.Trace(p.r.midTree(root))
				}
				if err := p.r.midTree(root); err != nil {
					return errors.Trace(err)
				}
				res, err := tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id="+strconv.Itoa(children[0]))
				if err != nil {
					return errors.Trace(err)
				}
				flows, _ := res.RowsAffected()
				deleted += int(flows)
				budget -= n + 1
				at.below, at.stepFlow, at.stepAfter = children[0], 0, 0
				continue
			}
			// The longest leading run whose steps and flow rows fit what is left of the budget.
			run := len(children)
			for run > 0 {
				room := budget - run
				if room >= 0 {
					n, err := stepsUpTo(ctx, tx, children[:run], room+1)
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
				_, last, err := deleteStepsFrom(ctx, tx, children[0], 0, budget)
				if err != nil {
					return errors.Trace(err)
				}
				at.stepFlow, at.stepAfter = children[0], last
				return errors.Trace(p.r.midTree(root))
			}
			list := intCSV(children[:run])
			res, err := tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+list+")")
			if err != nil {
				return errors.Trace(err)
			}
			steps, _ := res.RowsAffected()
			if err := p.r.midTree(root); err != nil {
				return errors.Trace(err)
			}
			res, err = tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id IN ("+list+")")
			if err != nil {
				return errors.Trace(err)
			}
			flows, _ := res.RowsAffected()
			deleted += int(flows)
			budget -= int(steps) + run
			at.below = children[run-1]
		}
		if budget <= 0 {
			return nil
		}
		from := at.stepsFrom(root)
		n, last, err := deleteStepsFrom(ctx, tx, root, from, budget)
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
		if err := p.r.midTree(root); err != nil {
			return errors.Trace(err)
		}
		p.r.seams.Checkpoint(ctx, CheckpointBeforeRootDelete)
		// The root goes last, and only once the tree holds nothing else: two probes, not an EXISTS over the tree,
		// which MariaDB materializes over the whole steps index. The walk just found no descendant below its position,
		// and deleted those between it and top, so a descendant left can only be one newer than top: the probe seeks
		// there rather than through the entries the chunks deleted. On MySQL and MariaDB the probes are locking reads:
		// a plain read there sees the snapshot the chunk's first plain read took, so it would miss a row committed since.
		locking := ""
		if d := tx.DriverName(); d == "mysql" || d == "mariadb" {
			locking = " FOR UPDATE"
		}
		rest, err := ints(ctx, tx, "SELECT flow_id FROM dwarf_flows WHERE root_flow_id="+id+" AND flow_id>"+
			strconv.Itoa(max(at.top, root))+" ORDER BY flow_id LIMIT_OFFSET(?, 0)"+locking, 1)
		if err != nil || len(rest) > 0 {
			return errors.Trace(err)
		}
		rest, err = ints(ctx, tx, "SELECT step_id FROM dwarf_steps WHERE flow_id="+id+" AND step_id>"+strconv.Itoa(from)+
			" ORDER BY step_id LIMIT_OFFSET(?, 0)"+locking, 1)
		if err != nil || len(rest) > 0 {
			return errors.Trace(err)
		}
		res, err := tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id="+id)
		if err != nil {
			return errors.Trace(err)
		}
		rows, _ := res.RowsAffected()
		deleted += int(rows)
		done = true
		return nil
	})
	if err != nil {
		return 0, false, errors.Trace(err)
	}
	*pos = at
	return deleted, done, nil
}

// stepsUpTo counts the steps of flows, reading at most limit of them: enough to tell whether they fit a budget,
// whatever their number.
func stepsUpTo(ctx context.Context, tx *sequel.Tx, flows []int, limit int) (int, error) {
	inner := "SELECT step_id FROM dwarf_steps WHERE flow_id IN (" + intCSV(flows) + ") LIMIT " + strconv.Itoa(limit)
	if tx.DriverName() == "mssql" {
		inner = "SELECT TOP (" + strconv.Itoa(limit) + ") step_id FROM dwarf_steps WHERE flow_id IN (" + intCSV(flows) + ")"
	}
	var n int
	err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM ("+inner+") c").Scan(&n)
	return n, errors.Trace(err)
}

// deleteStepsFrom deletes up to limit steps of flow after the step after, in step_id order, and returns how many it
// read and deleted, and the last one's id. The ids are read first and deleted by a literal list: a range or an ORDER
// BY with a limit on the DELETE is not portable, and the read is a seek into (flow_id, step_id) whatever else the table
// holds.
func deleteStepsFrom(ctx context.Context, tx *sequel.Tx, flow, after, limit int) (n, last int, err error) {
	ids, err := ints(ctx, tx, "SELECT step_id FROM dwarf_steps WHERE flow_id="+strconv.Itoa(flow)+" AND step_id>"+
		strconv.Itoa(after)+" ORDER BY step_id LIMIT_OFFSET(?, 0)", limit)
	if err != nil || len(ids) == 0 {
		return 0, after, errors.Trace(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE step_id IN ("+intCSV(ids)+")"); err != nil {
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
