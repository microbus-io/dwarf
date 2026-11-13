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
	"strconv"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
	"github.com/microbus-io/sequel"
)

// treeChunk is how many of a tree's descendant flows one read of them returns.
const treeChunk = 256

// descendantsAfter returns up to treeChunk of root's live descendants after the flow after, in flow_id order, with their
// payload bytes, and whether any of them is not closed.
func descendantsAfter(ctx context.Context, tx *sequel.Tx, root, after int) (children, bytes []int, open bool, err error) {
	rows, err := tx.QueryContext(ctx, "SELECT flow_id, "+flowBytesSQL+", CASE WHEN status IN ("+database.TerminalStatusesSQL+") THEN 0 ELSE 1 END"+
		" FROM dwarf_flows WHERE root_flow_id="+strconv.Itoa(root)+" AND flow_id<>"+strconv.Itoa(root)+
		" AND flow_id>"+strconv.Itoa(after)+" ORDER BY flow_id LIMIT_OFFSET(?, 0)", treeChunk)
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
func fitRun(ctx context.Context, tx *sequel.Tx, flows, flowBytes []int, budget limit) (run int, sz size, err error) {
	for run = len(flows); run > 0; run /= 2 {
		own := size{rows: run, flows: run}
		for _, b := range flowBytes[:run] {
			own.bytes += b
		}
		room := budget.minus(own)
		if room.rows < 0 || room.bytes < 0 || room.flows < 0 {
			continue
		}
		st, err := stepsSizeUpTo(ctx, tx, flows[:run], room.rows+1)
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
func stepsSizeUpTo(ctx context.Context, q sequel.Executor, flows []int, limit int) (sz size, err error) {
	driver := q.DriverName()
	sum := "COALESCE(SUM(b), 0)"
	if driver == "mssql" {
		sum = "COALESCE(SUM(CAST(b AS BIGINT)), 0)"
	}
	inner := capped(driver, stepBytesSQL+" AS b FROM dwarf_steps s WHERE s.flow_id IN ("+database.IntList(flows)+")", limit)
	err = q.QueryRowContext(ctx, "SELECT COUNT(*), "+sum+" FROM ("+inner+") c").Scan(&sz.rows, &sz.bytes)
	return sz, errors.Trace(err)
}

// stepsWithin returns the leading live steps of flow after the step after, in step_id order, that fit budget; their
// size; and more when the budget, not the flow, ended them. When fresh - nothing else in the chunk yet - it takes at
// least one, so a step over the whole budget goes alone; otherwise it may take none, and the chunk ends there. It reads
// up to budget's rows, a seek into (flow_id, step_id) whatever else the table holds.
func stepsWithin(ctx context.Context, tx *sequel.Tx, flow, after int, budget limit, fresh bool) (ids []int, sz size, more bool, err error) {
	rows, err := tx.QueryContext(ctx, "SELECT s.step_id, "+stepBytesSQL+" FROM dwarf_steps s WHERE s.flow_id="+strconv.Itoa(flow)+
		" AND s.step_id>"+strconv.Itoa(after)+" ORDER BY s.step_id LIMIT_OFFSET(?, 0)", max(budget.rows, 1))
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
func flowBytes(ctx context.Context, tx *sequel.Tx, flow int) (int, error) {
	var n int
	err := tx.QueryRowContext(ctx, "SELECT "+flowBytesSQL+" FROM dwarf_flows WHERE flow_id="+strconv.Itoa(flow)).Scan(&n)
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

// deleteChunk deletes, in one transaction on h under the root's lock, up to about budget's rows and bytes of the live
// tree of root, which must be stamped for deletion and due: its descendant flows newest first, each flow's steps and
// then the flow, and once none is left the root's steps, then the root itself after a recheck that the tree holds
// nothing else. It returns what it deleted, and done when the tree is gone or is not this chunk's: no longer stamped
// and due, or held by another transaction. pos advances only when the chunk commits.
//
// Every read is bounded by the budget, not by the tree: which descendants fit is decided by sizing at most the budget's
// worth of their steps, halving the run of descendants until it fits, so a flow of a million steps costs a chunk no
// more than one of a thousand. A chunk always takes at least one row, so one row over the byte budget still goes,
// alone.
func deleteChunk(ctx context.Context, h handle, seams *seamster.Seamster, root int, budget limit, pos *chunkPos) (deleted removal, done bool, err error) {
	id := strconv.Itoa(root)
	// midTree fails a chunk after it deleted steps and before their flows, so a test proves the chunk rolls back whole.
	midTree := func() error {
		if seams.IsFault(diagnostics.FaultReapMidTree) {
			return errors.New("injected fault: " + diagnostics.FaultReapMidTree)
		}
		return targetedFault(seams, diagnostics.FaultReapMidTree, root)
	}
	dropFlows := func(tx *sequel.Tx, list string) (int, error) {
		res, err := tx.ExecContext(ctx, "DELETE FROM dwarf_flows WHERE flow_id IN ("+list+")")
		if err != nil {
			return 0, errors.Trace(err)
		}
		n, _ := res.RowsAffected()
		return int(n), nil
	}
	var at chunkPos
	var published int
	err = h.Transact(ctx, func(tx *sequel.Tx) error {
		deleted, done, at, published = removal{}, false, *pos, 0
		locked, err := lockRoots(ctx, tx, true, "flow_id="+id+" AND "+stampedDueSQL)
		if err != nil {
			return errors.Trace(err)
		}
		if len(locked) == 0 {
			done = true
			return nil
		}
		// Counted under the root lock: a published tree's live copy is deleted, not reaped.
		cond, args := publishedSQL(tx.DriverName())
		if err := tx.QueryRowContext(ctx, "SELECT CASE WHEN "+cond+" THEN 1 ELSE 0 END FROM dwarf_flows WHERE flow_id="+id, args...).Scan(&published); err != nil {
			return errors.Trace(err)
		}
		left := budget
		for !left.spent() {
			children, childBytes, err := descendantsBelow(ctx, tx, root, at.below)
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
				sz, last, more, err := deleteStepsFrom(ctx, tx, children[0], at.stepAfter, left, fresh)
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
				flows, err := dropFlows(tx, strconv.Itoa(children[0]))
				if err != nil {
					return errors.Trace(err)
				}
				deleted.flows += flows
				left = left.minus(sz.plus(size{rows: 1, bytes: childBytes[0], flows: 1}))
				at.below, at.stepFlow, at.stepAfter = children[0], 0, 0
				continue
			}
			run, sz, err := fitRun(ctx, tx, children, childBytes, left)
			if err != nil {
				return errors.Trace(err)
			}
			if run == 0 {
				// The newest descendant alone is over what is left: part of its steps now, the rest - and its flow row -
				// in the next chunk.
				_, last, _, err := deleteStepsFrom(ctx, tx, children[0], 0, left, left == budget)
				if err != nil {
					return errors.Trace(err)
				}
				at.stepFlow, at.stepAfter = children[0], last
				return errors.Trace(midTree())
			}
			list := database.IntList(children[:run])
			if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE flow_id IN ("+list+")"); err != nil {
				return errors.Trace(err)
			}
			if err := midTree(); err != nil {
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
		sz, last, more, err := deleteStepsFrom(ctx, tx, root, from, left, fresh)
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
		rootBytes, err := flowBytes(ctx, tx, root)
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
		if rest, err = firstID(ctx, tx, "SELECT flow_id FROM dwarf_flows WHERE root_flow_id="+id+" AND flow_id>"+
			strconv.Itoa(max(at.top, root))+" ORDER BY flow_id LIMIT_OFFSET(1, 0)"+locking); err != nil || len(rest) > 0 {
			// A descendant newer than any the chunks read keeps the root, and the descendant read starts over from the
			// newest: kept, the position reads only below what it walked, so it would never see this one, and every
			// chunk after would end here.
			at.top, at.below = 0, 0
			return errors.Trace(err)
		}
		if rest, err = firstID(ctx, tx, "SELECT step_id FROM dwarf_steps WHERE flow_id="+id+" AND step_id>"+
			strconv.Itoa(from)+" ORDER BY step_id LIMIT_OFFSET(1, 0)"+locking); err != nil || len(rest) > 0 {
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
	if published == 0 {
		deleted.unarchived = deleted.flows
	}
	*pos = at
	return deleted, done, nil
}

// descendantsBelow returns up to treeChunk of root's live descendants below the flow below - all of them, when below is
// 0 - newest first, with their payload bytes.
func descendantsBelow(ctx context.Context, tx *sequel.Tx, root, below int) (children, bytes []int, err error) {
	cond := ""
	if below > 0 {
		cond = " AND flow_id<" + strconv.Itoa(below)
	}
	rows, err := tx.QueryContext(ctx, "SELECT flow_id, "+flowBytesSQL+" FROM dwarf_flows WHERE root_flow_id="+strconv.Itoa(root)+
		" AND flow_id<>"+strconv.Itoa(root)+cond+" ORDER BY flow_id DESC LIMIT_OFFSET(?, 0)", treeChunk)
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

// deleteStepsFrom deletes live steps of flow after the step after, in step_id order, up to budget - at least one,
// whatever its bytes, when fresh - and returns their size, the last one's id, and more when the budget, not the flow,
// ended them. The ids are read first and deleted by a literal list: a range or an ORDER BY with a limit on the DELETE
// is not portable, and the read is a seek into (flow_id, step_id) whatever else the table holds.
func deleteStepsFrom(ctx context.Context, tx *sequel.Tx, flow, after int, budget limit, fresh bool) (sz size, last int, more bool, err error) {
	ids, sz, more, err := stepsWithin(ctx, tx, flow, after, budget, fresh)
	if err != nil || len(ids) == 0 {
		return size{}, after, more, errors.Trace(err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM dwarf_steps WHERE step_id IN ("+database.IntList(ids)+")"); err != nil {
		return size{}, after, false, errors.Trace(err)
	}
	return sz, ids[len(ids)-1], more, nil
}

// firstID returns the id the statement reads first, none when it reads nothing.
func firstID(ctx context.Context, tx *sequel.Tx, stmt string) ([]int, error) {
	var id int
	err := tx.QueryRowContext(ctx, stmt).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return []int{id}, nil
}
