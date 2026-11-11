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
	"math"
	"strconv"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// size is an amount of a tree: its rows, flows and steps alike, and their payload bytes, the sum of the rows'
// *_bytes columns.
type size struct{ rows, bytes int }

// plus returns s and o together.
func (s size) plus(o size) size { return size{s.rows + o.rows, s.bytes + o.bytes} }

// limit caps an amount on both axes: rows and bytes. An amount is over it when it passes either.
type limit struct{ rows, bytes int }

// capOf returns the limit of rows rows, at least one, and bytes bytes, none when bytes is 0 or less.
func capOf(rows, bytes int) limit {
	if bytes <= 0 {
		bytes = math.MaxInt
	}
	return limit{max(rows, 1), bytes}
}

// over reports whether s passes l on either axis.
func (l limit) over(s size) bool { return s.rows > l.rows || s.bytes > l.bytes }

// minus returns what is left of l once s is spent, which may be below zero on either axis.
func (l limit) minus(s size) limit {
	if l.bytes == math.MaxInt {
		return limit{l.rows - s.rows, l.bytes}
	}
	return limit{l.rows - s.rows, l.bytes - s.bytes}
}

// spent reports whether nothing is left of l on either axis.
func (l limit) spent() bool { return l.rows <= 0 || l.bytes <= 0 }

// tighter returns the limit within both l and o.
func (l limit) tighter(o limit) limit { return limit{min(l.rows, o.rows), min(l.bytes, o.bytes)} }

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

// rowFits reports whether a flow row of rowBytes fits a chunk's budget alongside taken, the steps just taken with it:
// it does when both fit what is left, or when the chunk has taken nothing else - fresh, and no step with it - so a row
// over the whole budget goes alone. One that does not fit goes first in the next chunk.
func rowFits(left limit, fresh bool, taken size, rowBytes int) bool {
	return !left.over(taken.plus(size{1, rowBytes})) || (fresh && taken.rows == 0)
}

// flowBytes reads the payload bytes of flow's row in flows.
func flowBytes(ctx context.Context, tx *sequel.Tx, flows string, flow int) (int, error) {
	var n int
	err := tx.QueryRowContext(ctx, "SELECT "+flowBytesSQL("")+" FROM "+flows+" WHERE flow_id="+strconv.Itoa(flow)).Scan(&n)
	return n, errors.Trace(err)
}
