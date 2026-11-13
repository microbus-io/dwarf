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
	"math"
	"strconv"
	"strings"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// MaxBatchFlows is the most flows one atomic batch archives together: they share one archive time, and a caller
// paging ListClosed cannot page across a tie larger than its Limit, so the lists' maximum Limit must stay above it. A
// single tree of more goes alone.
const MaxBatchFlows = 4096

// size is an amount of a tree: its rows, flows and steps alike, their payload bytes - the sum of the rows' *_bytes
// columns - and how many of the rows are flows.
type size struct{ rows, bytes, flows int }

// plus returns s and o together.
func (s size) plus(o size) size { return size{s.rows + o.rows, s.bytes + o.bytes, s.flows + o.flows} }

// limit caps an amount on every axis. An amount is over it when it passes any.
type limit struct{ rows, bytes, flows int }

// capOf returns the limit of rows rows, at least one, and bytes bytes, none when bytes is 0 or less, with no limit on
// flows.
func capOf(rows, bytes int) limit {
	if bytes <= 0 {
		bytes = math.MaxInt
	}
	return limit{max(rows, 1), bytes, math.MaxInt}
}

// over reports whether s passes l on any axis.
func (l limit) over(s size) bool { return s.rows > l.rows || s.bytes > l.bytes || s.flows > l.flows }

// tighter returns the limit within both l and o.
func (l limit) tighter(o limit) limit {
	return limit{min(l.rows, o.rows), min(l.bytes, o.bytes), min(l.flows, o.flows)}
}

// flowBytesSQL is a flow row's payload bytes, its columns qualified by alias ("f." or "").
func flowBytesSQL(alias string) string {
	return alias + "graph_bytes+" + alias + "baggage_bytes+" + alias + "final_state_bytes"
}

// stepBytesSQL is a step row's payload bytes, its columns qualified by alias ("s." or "").
func stepBytesSQL(alias string) string {
	return alias + "state_bytes+" + alias + "changes_bytes+" + alias + "interrupt_payload_bytes+" + alias +
		"resume_data_bytes+" + alias + "subgraph_result_bytes"
}

// sumSQL is the sum of expr over a group, 0 for none. SQL Server sums an INT as an INT, which a tree's bytes can
// overflow, so it sums a BIGINT there.
func sumSQL(driver, expr string) string {
	if driver == "mssql" {
		return "COALESCE(SUM(CAST(" + expr + " AS BIGINT)), 0)"
	}
	return "COALESCE(SUM(" + expr + "), 0)"
}

// treesSizesUpTo sizes each tree rooted at roots in the live tables - its flow rows and its step rows, and their payload bytes - each
// read up to its own limit of rows, in one statement (treesSizesUpToSQL), so a group of trees costs one round trip and
// no tree is read past its limit. A tree read up to its limit reports at least that many rows.
func treesSizesUpTo(ctx context.Context, q sequel.Executor, roots, limits []int) ([]size, error) {
	rows, err := q.QueryContext(ctx, treesSizesUpToSQL(q.DriverName(), roots, limits))
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()
	sizes := make([]size, len(roots))
	for rows.Next() {
		var k, n, b int
		if err := rows.Scan(&k, &n, &b); err != nil {
			return nil, errors.Trace(err)
		}
		sizes[k/2] = sizes[k/2].plus(size{rows: n, bytes: b})
		if k%2 == 0 {
			sizes[k/2].flows += n
		}
	}
	return sizes, errors.Trace(rows.Err())
}

// treesSizesUpToSQL is treesSizesUpTo's statement: a UNION ALL of two capped reads a tree, its flows by root - keyed
// twice the tree's index - and their steps by flow - keyed one more. On PostgreSQL the steps are read through a LATERAL
// join and on SQL Server a CROSS APPLY, each flow's steps a seek into its own: a plain join there may be planned as a
// hash join over a scan of every step in the table, which the LIMIT stops early only for a tree larger than it, so a
// small tree costs the whole table. MySQL, MariaDB and SQLite join an indexed equality by nested loops. Each read is
// capped on its own, since a LIMIT over a UNION ALL is applied after MySQL materializes it whole.
func treesSizesUpToSQL(driver string, roots, limits []int) string {
	branches := make([]string, 0, 2*len(roots))
	for k, root := range roots {
		n, id := strconv.Itoa(limits[k]), strconv.Itoa(root)
		steps := stepBytesSQL("s.") + " AS b FROM dwarf_flows f JOIN dwarf_steps s ON s.flow_id=f.flow_id"
		switch driver {
		case "pgx":
			steps = "s.b FROM dwarf_flows f CROSS JOIN LATERAL (SELECT " + stepBytesSQL("s.") + " AS b FROM dwarf_steps" +
				" s WHERE s.flow_id=f.flow_id LIMIT " + n + ") s"
		case "mssql":
			steps = "s.b FROM dwarf_flows f CROSS APPLY (SELECT TOP (" + n + ") " + stepBytesSQL("s.") + " AS b FROM dwarf_steps" +
				" s WHERE s.flow_id=f.flow_id) s"
		}
		flows := capped(driver, flowBytesSQL("")+" AS b FROM dwarf_flows WHERE root_flow_id="+id, limits[k])
		stepRows := capped(driver, steps+" WHERE f.root_flow_id="+id, limits[k])
		agg := func(key int) string {
			return "SELECT " + strconv.Itoa(key) + " AS k, COUNT(*) AS n, " + sumSQL(driver, "b") + " AS b FROM ("
		}
		branches = append(branches, agg(2*k)+flows+") cf"+strconv.Itoa(k), agg(2*k+1)+stepRows+") cs"+strconv.Itoa(k))
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

// cutWithin returns the leading roots whose trees fit in batch, and at least the first, with the first's size. It sizes
// the roots in groups - the first of firstGroup, then of sizeGroup - each by one call of sizes, which reads each tree up
// to its limit of rows: the first root up to firstRows, every other up to one past what was left of the batch's rows
// when its group was sent, which is all it takes to tell whether it fits. So no read takes a large tree whole, and a
// batch of small trees takes a few calls, not one a tree.
func cutWithin(roots []int, batch limit, firstRows int, sizes func(group, limits []int) ([]size, error)) (cut []int, first size, err error) {
	var total size
	for i := 0; i < len(roots); {
		n := sizeGroup
		if i == 0 {
			n = firstGroup
		}
		group := roots[i:min(i+n, len(roots))]
		limits := make([]int, len(group))
		for k := range group {
			limits[k] = batch.rows - total.rows + 1
		}
		if i == 0 {
			limits[0] = firstRows
		}
		got, err := sizes(group, limits)
		if err != nil {
			return nil, size{}, errors.Trace(err)
		}
		for k, sz := range got {
			if i+k == 0 {
				first = sz
			}
			total = total.plus(sz)
			if batch.over(total) {
				return roots[:max(i+k, 1)], first, nil
			}
		}
		i += len(group)
	}
	return roots, first, nil
}

// The groups cutWithin sizes in one call: a small first one, since its first tree may be large and the batch then
// takes it alone, then larger ones.
const (
	firstGroup = 16
	sizeGroup  = 64
)
