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
	"database/sql"
	"slices"
	"strconv"
	"strings"

	"github.com/microbus-io/dwarf/internal/archiver"
	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
)

// treeChunk is how many of a tree's descendant flows one read of them returns.
const treeChunk = 256

// descendantsAfterSQL is the staged copy's ordered read of a tree's next descendants.
const descendantsAfterSQL = "SELECT flow_id, " + flowBytesSQL + ", CASE WHEN status IN (" + database.TerminalStatusesSQL + ") THEN 0 ELSE 1 END" +
	" FROM dwarf_flows WHERE root_flow_id=? AND flow_id<>? AND flow_id>? ORDER BY flow_id LIMIT_OFFSET(?, 0)"

// descendantsAfter returns up to treeChunk of root's live descendants after the flow after, in flow_id order, with their
// payload bytes, and whether any of them is not closed.
func descendantsAfter(ctx context.Context, tx *sequel.Tx, root, after int) (children, bytes []int, open bool, err error) {
	rows, err := tx.QueryContext(ctx, descendantsAfterSQL, root, root, after, treeChunk)
	if err != nil {
		return nil, nil, false, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, b, notClosed int
		if err := rows.Scan(&id, &b, &notClosed); err != nil {
			return nil, nil, false, errors.Trace(err)
		}
		children, bytes = append(children, id), append(bytes, b)
		open = open || notClosed == 1
	}
	return children, bytes, open, errors.Trace(rows.Err())
}

// fitRun returns the longest leading run of live flows, halving from all of them, whose steps and flow rows fit budget,
// and their size; run 0 when the first flow alone is over it. flowBytes are the flows' own payload bytes. Each probe
// reads at most the budget's worth of steps.
func fitRun(ctx context.Context, tx *sequel.Tx, sfx string, flows, flowBytes []int, budget limit) (run int, sz size, err error) {
	for run = len(flows); run > 0; run /= 2 {
		own := size{rows: run, flows: run}
		for _, b := range flowBytes[:run] {
			own.bytes += b
		}
		room := budget.minus(own)
		if room.rows < 0 || room.bytes < 0 || room.flows < 0 {
			continue
		}
		st, err := stepsSizeUpTo(ctx, tx, sfx, flows[:run], room.rows+1)
		if err != nil {
			return 0, size{}, errors.Trace(err)
		}
		if !room.over(st) {
			return run, st.plus(own), nil
		}
	}
	return 0, size{}, nil
}

// stepsSizeUpTo sizes the live steps of flows, reading at most limit of them.
func stepsSizeUpTo(ctx context.Context, q sequel.Executor, sfx string, flows []int, limit int) (sz size, err error) {
	driver := q.DriverName()
	sum := "COALESCE(SUM(b), 0)"
	if driver == "mssql" {
		sum = "COALESCE(SUM(CAST(b AS BIGINT)), 0)"
	}
	ph, args := database.IDList(flows)
	inner, args := capped(driver, stepBytesSQL+" AS b FROM dwarf_steps"+sfx+" s WHERE s.flow_id IN ("+ph+")", args, limit)
	err = q.QueryRowContext(ctx, "SELECT COUNT(*), "+sum+" FROM ("+inner+") c", args...).Scan(&sz.rows, &sz.bytes)
	return sz, errors.Trace(err)
}

// stepsWithinSQL is stepsWithin's read, bound to the flow, the step after and the limit, in that order: the steps of
// the flow after the step, in step_id order.
func stepsWithinSQL(sfx string) string {
	return "SELECT s.step_id, " + stepBytesSQL + " FROM dwarf_steps" + sfx + " s WHERE s.flow_id=? AND s.step_id>?" +
		" ORDER BY s.step_id LIMIT_OFFSET(?, 0)"
}

// boundReads keeps a chunk's reads bounded by its budget on PostgreSQL, whatever the planner's statistics: it turns
// bitmap scans off for the chunk's transaction, the first statement of every chunk, deleting or copying. With no
// statistics on a large flow - a flow written since the last ANALYZE - the planner reads its steps by a bitmap scan
// of the flow index and sorts them, every remaining step of the flow on every chunk (measured: 40,000 steps scattered
// at 4% density, about a second a chunk cold); without the bitmap scan it walks the index in order and stops at the
// limit. The ANALYZE trigger at 1% narrows the window and cannot close it.
//
// It also plans every statement of the chunk for its own values: the driver keeps each statement prepared, and
// PostgreSQL may switch a prepared statement to a generic plan after five runs, made without the values - and without
// this transaction's setting, since a cached plan is not replanned when a setting changes. Elsewhere it does nothing.
func boundReads(ctx context.Context, tx *sequel.Tx) error {
	if tx.DriverName() != "pgx" {
		return nil
	}
	_, err := tx.ExecContext(ctx, "SELECT set_config('enable_bitmapscan', 'off', true), set_config('plan_cache_mode', 'force_custom_plan', true)")
	return errors.Trace(err)
}

// stepsWithin returns the leading live steps of flow after the step after, in step_id order, that fit budget; their
// size; and more when the budget, not the flow, ended them. When fresh - nothing else in the chunk yet - it takes at
// least one, so a step over the whole budget goes alone; otherwise it may take none, and the chunk ends there. It reads
// up to budget's rows, a seek into (flow_id, step_id) whatever else the table holds.
func stepsWithin(ctx context.Context, tx *sequel.Tx, sfx string, flow, after int, budget limit, fresh bool) (ids []int, sz size, more bool, err error) {
	rows, err := tx.QueryContext(ctx, stepsWithinSQL(sfx), flow, after, max(budget.rows, 1))
	if err != nil {
		return nil, size{}, false, errors.Trace(err)
	}
	var read, bytes []int
	for rows.Next() {
		var id, b int
		if err := rows.Scan(&id, &b); err != nil {
			rows.Close()
			return nil, size{}, false, errors.Trace(err)
		}
		read, bytes = append(read, id), append(bytes, b)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, size{}, false, errors.Trace(err)
	}
	for k := range read {
		if (k > 0 || !fresh) && sz.bytes+bytes[k] > budget.bytes {
			return read[:k], sz, true, nil
		}
		sz = sz.plus(size{rows: 1, bytes: bytes[k]})
	}
	return read, sz, len(read) >= max(budget.rows, 1), nil
}

// rowFits reports whether a flow row of rowBytes fits a chunk's budget alongside taken, the steps just taken with it:
// it does when both fit what is left, or when the chunk has taken nothing else - fresh, and no step with it - so a row
// over the whole budget goes alone. One that does not fit goes first in the next chunk.
func rowFits(left limit, fresh bool, taken size, rowBytes int) bool {
	return !left.over(taken.plus(size{rows: 1, bytes: rowBytes, flows: 1})) || (fresh && taken.rows == 0)
}

// flowBytes reads the payload bytes of the live flow row of flow.
func flowBytes(ctx context.Context, tx *sequel.Tx, sfx string, flow int) (int, error) {
	var n int
	err := tx.QueryRowContext(ctx, "SELECT "+flowBytesSQL+" FROM dwarf_flows"+sfx+" WHERE flow_id=?", flow).Scan(&n)
	return n, errors.Trace(err)
}

// chunkPos is where the chunks of a chunked deletion stand on a tree: every descendant from below up to top is deleted,
// and of stepFlow, a flow part of whose steps are deleted, every step up to stepAfter. It lives in memory only: lost, the
// next chunk starts again from the newest descendant and the first step. Keeping it lets each chunk's reads seek past
// the entries the earlier chunks deleted, which stay in the indexes until vacuum: read from the start, a flow of a
// million steps would cost each chunk every step deleted before it.
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

// deleteChunk deletes, in one transaction on h under the root's lock, up to about budget's rows and bytes of the tree
// of root in the tables of sfx - the live tables for "", a day's for its suffix (daySuffix) - whose root must be marked:
// stamped for deletion and due in the live tables, marked for deletion in a day's. It deletes the descendant flows
// newest first, each flow's steps and then the flow, and once none is left the root's steps, then the root itself
// after a recheck that the tree holds nothing else; on a day, the directory's rows for each flow with it. It returns
// what it deleted, and done when the tree is gone or is not this chunk's: no longer marked, or held by another
// transaction. pos advances only when the chunk commits.
//
// Every read is bounded by the budget, not by the tree: which descendants fit is decided by sizing at most the budget's
// worth of their steps, halving the run of descendants until it fits, so a flow of a million steps costs a chunk no
// more than one of a thousand. A chunk always takes at least one row, so one row over the byte budget still goes,
// alone.
func deleteChunk(ctx context.Context, h handle, seams *seamster.Seamster, sfx string, root int, budget limit, pos *chunkPos) (deleted removal, done bool, err error) {
	// midTree fails a chunk after it deleted steps and before their flows, so a test proves the chunk rolls back whole.
	midTree := func() error {
		if seams.IsFault(diagnostics.FaultReapMidTree) {
			return errors.New("injected fault: " + diagnostics.FaultReapMidTree)
		}
		return targetedFault(seams, diagnostics.FaultReapMidTree, root)
	}
	archived := sfx != ""
	marked := stampedDueSQL
	if archived {
		marked = markedSQL
	}
	dropFlows := func(tx *sequel.Tx, flows ...int) (int, error) {
		list, args := database.IDList(flows)
		if archived {
			if _, err := tx.ExecContext(ctx, deleteSQL(tx.DriverName(), archiver.DirectoryTable, "1(flow_id)", "flow_id IN ("+list+")"), args...); err != nil {
				return 0, errors.Trace(err)
			}
		}
		res, err := tx.ExecContext(ctx, deleteSQL(tx.DriverName(), "dwarf_flows"+sfx, "1(flow_id)", "flow_id IN ("+list+")"), args...)
		if err != nil {
			return 0, errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		return int(n), nil
	}
	var at chunkPos
	var published int
	chunk := func(tx *sequel.Tx) error {
		deleted, done, at, published = removal{}, false, *pos, 0
		if err := boundReads(ctx, tx); err != nil {
			return err
		}
		locked, err := lockRoots(ctx, tx, sfx, true, "flow_id=? AND "+marked, root)
		if err != nil {
			return errors.Trace(err)
		}
		if len(locked) == 0 {
			done = true
			return nil
		}
		if !archived {
			// Counted under the root lock: a published tree's live copy is deleted, not reaped.
			cond, args := publishedSQL(tx.DriverName())
			if err := tx.QueryRowContext(ctx, "SELECT CASE WHEN "+cond+" THEN 1 ELSE 0 END FROM dwarf_flows WHERE flow_id=?", append(args, root)...).Scan(&published); err != nil {
				return errors.Trace(err)
			}
		}
		left := budget
		for !left.spent() {
			children, childBytes, err := descendantsBelow(ctx, tx, sfx, root, at.below)
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
				sz, last, more, err := deleteStepsFrom(ctx, tx, sfx, children[0], at.stepAfter, left, fresh)
				if err != nil {
					return errors.Trace(err)
				}
				if more || !rowFits(left, fresh, sz, childBytes[0]) {
					at.stepAfter = last
					return errors.Trace(midTree())
				}
				if err := midTree(); err != nil {
					return errors.Trace(err)
				}
				flows, err := dropFlows(tx, children[0])
				if err != nil {
					return errors.Trace(err)
				}
				deleted.flows += flows
				left = left.minus(sz.plus(size{rows: 1, bytes: childBytes[0], flows: 1}))
				at.below, at.stepFlow, at.stepAfter = children[0], 0, 0
				continue
			}
			run, sz, err := fitRun(ctx, tx, sfx, children, childBytes, left)
			if err != nil {
				return errors.Trace(err)
			}
			if run == 0 {
				// The newest descendant alone is over what is left: part of its steps now, the rest - and its flow row -
				// in the next chunk.
				_, last, _, err := deleteStepsFrom(ctx, tx, sfx, children[0], 0, left, left == budget)
				if err != nil {
					return errors.Trace(err)
				}
				at.stepFlow, at.stepAfter = children[0], last
				return errors.Trace(midTree())
			}
			list, args := database.IDList(children[:run])
			if _, err := tx.ExecContext(ctx, deleteSQL(tx.DriverName(), "dwarf_steps"+sfx, "idx_dwarf_steps"+sfx+"_flow_id(flow_id)", "flow_id IN ("+list+")"), args...); err != nil {
				return errors.Trace(err)
			}
			if err := midTree(); err != nil {
				return errors.Trace(err)
			}
			flows, err := dropFlows(tx, children[:run]...)
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
		sz, last, more, err := deleteStepsFrom(ctx, tx, sfx, root, from, left, fresh)
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
		rootBytes, err := flowBytes(ctx, tx, sfx, root)
		if err != nil {
			return errors.Trace(err)
		}
		if !rowFits(left, fresh, sz, rootBytes) {
			at.stepFlow, at.stepAfter = root, from
			return nil
		}
		if err := midTree(); err != nil {
			return errors.Trace(err)
		}
		seams.Checkpoint(ctx, diagnostics.CheckpointReapBeforeRootDelete)
		// The root goes last, and only once the tree holds nothing else: two probes, not an EXISTS over the tree,
		// which MariaDB materializes over the whole steps index. The walk just found no descendant below its position,
		// and deleted those between it and top, so a descendant left can only be one newer than top: the probe seeks
		// there rather than through the entries the chunks deleted. On MySQL and MariaDB the probes are locking reads:
		// a plain read there sees the snapshot the chunk's first plain read took, so it would miss a row committed since.
		locking := ""
		if d := tx.DriverName(); d == "mysql" || d == "mariadb" {
			locking = " FOR UPDATE"
		}
		var rest []int
		if rest, err = firstID(ctx, tx, "SELECT flow_id FROM dwarf_flows"+sfx+" WHERE root_flow_id=? AND flow_id>?"+
			" ORDER BY flow_id LIMIT_OFFSET(1, 0)"+locking, root, max(at.top, root)); err != nil || len(rest) > 0 {
			// A descendant newer than any the chunks read keeps the root, and the descendant read starts over from the
			// newest: kept, the position reads only below what it walked, so it would never see this one, and every
			// chunk after would end here.
			at.top, at.below = 0, 0
			return errors.Trace(err)
		}
		if rest, err = firstID(ctx, tx, "SELECT step_id FROM dwarf_steps"+sfx+" WHERE flow_id=? AND step_id>?"+
			" ORDER BY step_id LIMIT_OFFSET(1, 0)"+locking, root, from); err != nil || len(rest) > 0 {
			return errors.Trace(err)
		}
		rows, err := dropFlows(tx, root)
		if err != nil {
			return errors.Trace(err)
		}
		deleted.flows += rows
		done = true
		return nil
	}
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		if err := chunk(tx); err != nil {
			return err
		}
		seams.Checkpoint(ctx, diagnostics.CheckpointReapDeleted)
		return nil
	})
	if err != nil {
		return removal{}, false, errors.Trace(err)
	}
	if seams.Enabled() && seams.IsFault(diagnostics.Join(diagnostics.FaultJanitorLostResponse, strconv.Itoa(root))) {
		return removal{}, false, errors.New("injected fault: " + diagnostics.FaultJanitorLostResponse)
	}
	if !archived && published == 0 {
		deleted.unarchived = deleted.flows
	}
	*pos = at
	return deleted, done, nil
}

// descendantsBelow returns up to treeChunk of root's live descendants below the flow below - all of them, when below is
// 0 - newest first, with their payload bytes.
func descendantsBelow(ctx context.Context, tx *sequel.Tx, sfx string, root, below int) (children, bytes []int, err error) {
	stmt, args := descendantsBelowSQL(sfx, root, below)
	rows, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, b int
		if err := rows.Scan(&id, &b); err != nil {
			return nil, nil, errors.Trace(err)
		}
		children, bytes = append(children, id), append(bytes, b)
	}
	return children, bytes, errors.Trace(rows.Err())
}

// descendantsBelowSQL is a deletion chunk's ordered read, on the live tables or an archive day.
func descendantsBelowSQL(sfx string, root, below int) (string, []any) {
	cond, args := "", []any{root, root}
	if below > 0 {
		cond, args = " AND flow_id<?", append(args, below)
	}
	return "SELECT flow_id, " + flowBytesSQL + " FROM dwarf_flows" + sfx + " WHERE root_flow_id=?" +
		" AND flow_id<>?" + cond + " ORDER BY flow_id DESC LIMIT_OFFSET(?, 0)", append(args, treeChunk)
}

// deleteStepsFrom deletes live steps of flow after the step after, in step_id order, up to budget - at least one,
// whatever its bytes, when fresh - and returns their size, the last one's id, and more when the budget, not the flow,
// ended them. The ids are read first and deleted by bound lists of at most database.MaxListIDs: a range or an ORDER BY
// with a limit on the DELETE is not portable, and the read is a seek into (flow_id, step_id) whatever else the table
// holds.
func deleteStepsFrom(ctx context.Context, tx *sequel.Tx, sfx string, flow, after int, budget limit, fresh bool) (sz size, last int, more bool, err error) {
	ids, sz, more, err := stepsWithin(ctx, tx, sfx, flow, after, budget, fresh)
	if err != nil || len(ids) == 0 {
		return size{}, after, more, errors.Trace(err)
	}
	for chunk := range slices.Chunk(ids, database.MaxListIDs) {
		ph, args := database.IDList(chunk)
		if _, err := tx.ExecContext(ctx, deleteSQL(tx.DriverName(), "dwarf_steps"+sfx, "1(step_id)", "step_id IN ("+ph+")"), args...); err != nil {
			return size{}, after, false, errors.Trace(err)
		}
	}
	return sz, ids[len(ids)-1], more, nil
}

// markedSQL is the condition, on a day's flows row, that it is a root marked for deletion: by a Delete or a Purge, or
// as a published tree's archive copy a Delete took back. A deletion rechecks it under the root's lock.
const markedSQL = "delete_after_ms>0 AND surgraph_flow_id=0"

// daySuffix is the suffix of day's archive tables: dwarf_flows and dwarf_steps with it are the day's.
func daySuffix(day int) string {
	return strings.TrimPrefix(migrations.FlowsTable(day), "dwarf_flows")
}

// firstID returns the id the statement reads first, none when it reads nothing.
func firstID(ctx context.Context, tx *sequel.Tx, stmt string, args ...any) ([]int, error) {
	var id int
	err := tx.QueryRowContext(ctx, stmt, args...).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return []int{id}, nil
}
