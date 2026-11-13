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
	"slices"
	"strconv"
	"strings"

	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
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

// minus returns what is left of l once s is spent, which may be below zero; an axis with no limit stays without one.
func (l limit) minus(s size) limit {
	sub := func(a, b int) int {
		if a == math.MaxInt {
			return a
		}
		return a - b
	}
	return limit{sub(l.rows, s.rows), sub(l.bytes, s.bytes), sub(l.flows, s.flows)}
}

// spent reports whether nothing is left of l on some axis.
func (l limit) spent() bool { return l.rows <= 0 || l.bytes <= 0 || l.flows <= 0 }

// tighter returns the limit within both l and o.
func (l limit) tighter(o limit) limit {
	return limit{min(l.rows, o.rows), min(l.bytes, o.bytes), min(l.flows, o.flows)}
}

// flowBytesSQL is a flow row's payload bytes, and stepBytesSQL a step row's, aliased s.
const (
	flowBytesSQL = "graph_bytes+baggage_bytes+final_state_bytes"
	stepBytesSQL = "s.state_bytes+s.changes_bytes+s.interrupt_payload_bytes+s.resume_data_bytes+s.subgraph_result_bytes"
)

// treesSizesUpTo sizes each tree rooted at roots in the live tables - its flow rows and its step rows, and their payload bytes - each
// read up to its own limit of rows, in one statement (treesSizesUpToSQL), so a group of trees costs one round trip and
// no tree is read past its limit. A tree read up to its limit reports at least that many rows.
func treesSizesUpTo(ctx context.Context, q sequel.Executor, sfx string, roots, limits []int) ([]size, error) {
	stmt, args := treesSizesUpToSQL(q.DriverName(), sfx, roots, limits)
	rows, err := q.QueryContext(ctx, stmt, args...)
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
		if k/2 >= len(sizes) {
			continue // a padding branch
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
//
// Every root and limit is bound, and the trees are padded to 1, 4, 16 or 64 (sizeGroup) of them with branches limited
// to no rows, so the statement takes one of four texts, and a lone tree - a sweep's retry of a root - is one branch
// pair, not sixteen.
func treesSizesUpToSQL(driver, sfx string, roots, limits []int) (stmt string, args []any) {
	// SQL Server sums an INT as an INT, which a tree's bytes can overflow, so it sums a BIGINT there.
	sum := "COALESCE(SUM(b), 0)"
	if driver == "mssql" {
		sum = "COALESCE(SUM(CAST(b AS BIGINT)), 0)"
	}
	if len(roots) > sizeGroup {
		panic("treesSizesUpTo: " + strconv.Itoa(len(roots)) + " trees, more than " + strconv.Itoa(sizeGroup))
	}
	arity := 1
	for arity < len(roots) {
		arity *= 4
	}
	branches := make([]string, 0, 2*arity)
	for k := range arity {
		root, n := 0, 0 // a padding branch reads no rows
		if k < len(roots) {
			root, n = roots[k], limits[k]
		}
		// The step read's own limit is bound first on PostgreSQL and SQL Server, where it is inside the join.
		steps, stepsArgs := stepBytesSQL+" AS b FROM dwarf_flows"+sfx+" f JOIN dwarf_steps"+sfx+" s ON s.flow_id=f.flow_id", []any(nil)
		switch driver {
		case "pgx":
			steps, stepsArgs = "s.b FROM dwarf_flows"+sfx+" f CROSS JOIN LATERAL (SELECT "+stepBytesSQL+" AS b FROM dwarf_steps"+sfx+
				" s WHERE s.flow_id=f.flow_id LIMIT ?) s", []any{n}
		case "mssql":
			steps, stepsArgs = "s.b FROM dwarf_flows"+sfx+" f CROSS APPLY (SELECT TOP (?) "+stepBytesSQL+" AS b FROM dwarf_steps"+sfx+
				" s WHERE s.flow_id=f.flow_id) s", []any{n}
		}
		flows, flowsArgs := capped(driver, flowBytesSQL+" AS b FROM dwarf_flows"+sfx+" WHERE root_flow_id=?", []any{root}, n)
		stepRows, stepRowsArgs := capped(driver, steps+" WHERE f.root_flow_id=?", append(stepsArgs, root), n)
		agg := func(key int) string {
			return "SELECT " + strconv.Itoa(key) + " AS k, COUNT(*) AS n, " + sum + " AS b FROM ("
		}
		branches = append(branches, agg(2*k)+flows+") cf"+strconv.Itoa(k), agg(2*k+1)+stepRows+") cs"+strconv.Itoa(k))
		args = append(append(args, flowsArgs...), stepRowsArgs...)
	}
	return strings.Join(branches, " UNION ALL "), args
}

// capped is "SELECT "+selectFrom reading at most limit rows, a bound LIMIT, or a TOP on SQL Server, with its arguments:
// selectFrom's, args, and the limit, in the order the statement binds them.
func capped(driver, selectFrom string, args []any, limit int) (stmt string, cappedArgs []any) {
	if driver == "mssql" {
		return "SELECT TOP (?) " + selectFrom, append([]any{limit}, args...)
	}
	return "SELECT " + selectFrom + " LIMIT ?", append(slices.Clone(args), limit)
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

// cutTrees is cutWithin over the trees of roots in the tables of sfx, sized by treesSizesUpTo; FaultReapSizeErr,
// targeted at a root, fails the sizing of its group. On PostgreSQL the sizing runs in a transaction of its own whose
// reads are bounded (boundReads): left to its planner, a flow's step read there is a bitmap scan of the flow's whole
// index, whatever the limit, with statistics or without, custom plan or generic. It commits before the batch's own
// transaction begins.
func cutTrees(ctx context.Context, h handle, seams *seamster.Seamster, sfx string, roots []int, batch limit, firstRows int) (cut []int, first size, err error) {
	err = sizingReads(ctx, h, func(q sequel.Executor) (err error) {
		cut, first, err = cutWithin(roots, batch, firstRows, func(group, limits []int) ([]size, error) {
			if err := targetedFault(seams, diagnostics.FaultReapSizeErr, group...); err != nil {
				return nil, err
			}
			return treesSizesUpTo(ctx, q, sfx, group, limits)
		})
		return err
	})
	return cut, first, errors.Trace(err)
}

// sizingReads runs fn, reads that size trees, on h: on PostgreSQL in a transaction of their own whose reads are
// bounded (boundReads), elsewhere on h itself.
func sizingReads(ctx context.Context, h handle, fn func(q sequel.Executor) error) error {
	if h.DriverName() != "pgx" {
		return fn(h)
	}
	return h.Transact(ctx, func(tx *sequel.Tx) error {
		if err := boundReads(ctx, tx); err != nil {
			return err
		}
		return fn(tx)
	})
}

// The groups cutWithin sizes in one call: a small first one, since its first tree may be large and the batch then
// takes it alone, then larger ones.
const (
	firstGroup = 16
	sizeGroup  = 64
)
