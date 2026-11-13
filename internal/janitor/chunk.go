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

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/errors"
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
