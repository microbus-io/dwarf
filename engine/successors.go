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

package engine

import (
	"strings"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// successorBatchMax is the largest number of successor rows one INSERT carries. Chunks are powers of two up to
// it, so the statement texts a transition can produce are a fixed handful rather than one per fan-out width.
// At 24 binds per row, 64 rows stay under SQL Server's 2,100-parameter ceiling, the tightest of the dialects.
const successorBatchMax = 64

// successorArrayMax is the largest number of successor rows one PostgreSQL INSERT carries, as arrays
// (successorArraySQL). The statement's text and its binds are the same whatever the count, so it needs no
// power-of-two chunks; it is bounded for the transition's memory and the statement's size, with maxBytes.
const successorArrayMax = 4096

/*
successorRowSQL is one successor row's VALUES, in successorCols order. Its last column, the Cancel mark, is decided
by the row itself: the successor is marked when its source step already existed when the flow was cancelled (its
id is at or below the flow's cancel_watermark) and the source was not itself covered (the inherit argument, 1 or
0). That reaches the one step Cancel's own mark cannot - a source whose completion write landed before Cancel,
with its successors still to be inserted - while the successor of a step inserted after the Cancel, such as a
handler that caught the cancellation and everything it leads to, stays unmarked, which is what lets a flow
recover. Every insert of a step holds the flow row's lock, and Cancel writes the watermark only under it, so the
read cannot race it. Arguments: the twenty-one step columns in order, then the inherit flag, the source step id and
the flow id.

Do not compare timestamps here (the flow's cancel time against the source's started_at). Three dialects store
milliseconds, and a source that starts in the same millisecond as the Cancel reads as not-before it, losing the
Cancel - measured at ~1 in 4 runs of TestCancelInTransitionGap on SQLite. Making it inclusive only moves the tie to
the handler, whose successors would then be cancelled too.
*/
const successorRowSQL = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?, ?," +
	" (SELECT CASE WHEN ?=1 AND ? <= cancel_watermark THEN 1 ELSE 0 END FROM dwarf_flows WHERE flow_id=?))"

// successorCols is a successor row's column list, shared by the single-row and multi-row INSERTs.
const successorCols = "flow_id, step_depth, step_token, task_name, task_url, state, state_bytes, state_refs, state_reduced, status, parked, time_budget_ms, lineage_id, fan_out_ordinal, predecessor_id, not_before, priority, fairness_key, fairness_weight, engine_id, flow_created_hour, cancelling"

// successorInsertSQL inserts one successor row (successorRowSQL).
const successorInsertSQL = "INSERT INTO dwarf_steps (" + successorCols + ") VALUES " + successorRowSQL

/*
successorArraySQL inserts any number of successor rows on PostgreSQL in one statement, with the columns that differ
between rows passed as arrays and expanded by unnest. A multi-row VALUES list costs the server a parse, a plan and a
bind per row, and a Cancel mark subquery per row; here the statement is one plan whatever the count, and the mark,
the same for every row of a transition, is one InitPlan. Its arguments are the shared columns in the order they
appear, then the Cancel mark's three, then the eight arrays (arrayArgs).

The same rows as successorRowSQL, column for column. Every scalar is cast: unnest's output and the select list do not
take a parameter's type from the column it lands in. The payloads travel as BYTEA arrays of the rows' own slices and
are decoded in the statement, so the transition holds no second copy of them beside the driver's buffer.
*/
const successorArraySQL = "INSERT INTO dwarf_steps (" + successorCols + ")" +
	" SELECT CAST(? AS BIGINT), CAST(? AS INT), u.step_token, u.task_name, u.task_url," +
	" CAST(convert_from(u.state, 'UTF8') AS JSONB), u.state_bytes, CAST(convert_from(u.state_refs, 'UTF8') AS JSONB)," +
	" CAST(convert_from(u.state_reduced, 'UTF8') AS JSONB), CAST(? AS VARCHAR), CAST(? AS SMALLINT), CAST(? AS INT)," +
	" CAST(? AS BIGINT), u.fan_out_ordinal, CAST(? AS BIGINT), DATE_ADD_MILLIS(NOW_UTC(), CAST(? AS BIGINT)), CAST(? AS INT)," +
	" CAST(? AS VARCHAR), CAST(? AS INT), CAST(? AS BIGINT), CAST(? AS INT)," +
	" (SELECT CASE WHEN CAST(? AS INT)=1 AND CAST(? AS BIGINT) <= cancel_watermark THEN 1 ELSE 0 END" +
	" FROM dwarf_flows WHERE flow_id=CAST(? AS BIGINT))" +
	" FROM unnest(CAST(? AS TEXT[]), CAST(? AS TEXT[]), CAST(? AS TEXT[]), CAST(? AS BYTEA[]), CAST(? AS INT[])," +
	" CAST(? AS BYTEA[]), CAST(? AS BYTEA[]), CAST(? AS INT[]))" +
	" AS u(step_token, task_name, task_url, state, state_bytes, state_refs, state_reduced, fan_out_ordinal)" +
	" RETURNING step_id, step_token"

// successorRow is what differs between the successors one transition inserts.
type successorRow struct {
	taskName    string
	taskURL     string
	stateJSON   []byte
	refsJSON    []byte
	reducedJSON []byte // the fan-in's state_reduced record; nil stores an empty one
	ordinal     int
}

/*
successorBatch holds the successors of one transition, which share everything but the per-row fields, and queues
their INSERTs onto the transition's statement batch.

Where the driver returns inserted rows (RETURNING on PostgreSQL and SQLite, OUTPUT on SQL Server) several rows go
in multi-row INSERTs, so a fan-out of N costs about N/64 statements plus one per set bit of the remainder, rather
than N - on PostgreSQL N/4,096, by arrays (successorArraySQL) - and their ids are matched back by step_token, because
neither RETURNING nor OUTPUT promises the rows' order.
MySQL has no RETURNING, and its multi-row insert ids are not guaranteed contiguous under the default
innodb_autoinc_lock_mode=2, so it inserts row by row and reads each id as LastInsertId.

Within a chunk, step ids follow VALUES order on every supported dialect in practice, but none promises it. So
nothing that must be deterministic may order siblings by step_id: the fan-in merge and the failed-cohort merge order
them by fan_out_ordinal.
*/
type successorBatch struct {
	flowID         int
	flowHour       int // the flow's creation hour, which every step of it carries
	stepDepth      int
	timeBudgetMs   int
	lineageID      int
	predecessorID  int
	sleepMs        int64
	priority       int
	fairnessKey    string
	fairnessWeight int
	engineID       int64
	inheritCancel  int
	maxBytes       int  // the payload bytes past which the pending rows are flushed (add)
	arrays         bool // insert several rows by arrays (successorArraySQL), on PostgreSQL
	rows           []successorRow
	bytes          int // the payload bytes of rows
}

// maxRows is how many rows are pending before they are sent.
func (s *successorBatch) maxRows() int {
	if s.arrays {
		return successorArrayMax
	}
	return successorBatchMax
}

// size is the bytes of the row's payload columns.
func (r successorRow) size() int {
	return len(r.stateJSON) + len(r.refsJSON) + len(r.reducedJSON)
}

// full reports whether the pending rows must be sent before r joins them: they are maxRows, or r would take their
// payloads past maxBytes. A row alone is never held back, whatever its size.
//
// Do not bound a batch by its row count alone. A fan-out whose source holds a large folded field writes it inline
// into every branch, so 64 branches of a 100 MB state make one 6.4 GB INSERT: PostgreSQL refuses a protocol message
// over 1 GB, after the task has run, and the transition holds every row of it in memory meanwhile.
func (s *successorBatch) full(r successorRow) bool {
	return len(s.rows) == s.maxRows() || len(s.rows) > 0 && s.bytes+r.size() > s.maxBytes
}

// add adds r to the pending rows, first queueing and flushing those already pending if they are full.
func (s *successorBatch) add(b *txBatch, ids *[]int, r successorRow) error {
	if s.full(r) {
		s.queue(b, ids)
		if err := b.flush(); err != nil {
			return err
		}
	}
	s.rows = append(s.rows, r)
	s.bytes += r.size()
	return nil
}

// returnsInsertedRows reports whether the driver can return the rows an INSERT created.
func returnsInsertedRows(driver string) bool {
	switch driver {
	case "pgx", "sqlite", "mssql":
		return true
	}
	return false
}

// queue queues the INSERTs of the pending rows onto b, whose readers append the new step ids to ids in row order,
// and empties the pending rows. A chunk of one row - every row on MySQL - goes in by a single-row INSERT.
func (s *successorBatch) queue(b *txBatch, ids *[]int) {
	defer func() { s.rows, s.bytes = s.rows[:0], 0 }()
	appendID := func(id int64) { *ids = append(*ids, int(id)) }
	for rest := s.rows; len(rest) > 0; {
		if s.arrays && len(rest) > 1 {
			n := min(len(rest), successorArrayMax)
			tokens := make([]string, n)
			for i := range tokens {
				tokens[i] = keys.RandomIdentifier(16)
			}
			queueInsertedByToken(b, ids, tokens, successorArraySQL, s.arrayArgs(rest[:n], tokens)...)
			rest = rest[n:]
			continue
		}
		n := 1
		if returnsInsertedRows(b.tx.DriverName()) {
			n = successorBatchMax
			for n > len(rest) {
				n /= 2
			}
		}
		if n == 1 {
			queueInsertID(b, "step_id", appendID, successorInsertSQL, s.rowArgs(rest[0], keys.RandomIdentifier(16))...)
			rest = rest[1:]
			continue
		}
		tokens := make([]string, n)
		args := make([]any, 0, n*23)
		for i, r := range rest[:n] {
			tokens[i] = keys.RandomIdentifier(16)
			args = append(args, s.rowArgs(r, tokens[i])...)
		}
		values := strings.Repeat(successorRowSQL+",", n-1) + successorRowSQL
		stmt := "INSERT INTO dwarf_steps (" + successorCols + ") VALUES " + values + " RETURNING step_id, step_token"
		if b.tx.DriverName() == "mssql" {
			stmt = "INSERT INTO dwarf_steps (" + successorCols + ") OUTPUT INSERTED.step_id, INSERTED.step_token VALUES " + values
		}
		queueInsertedByToken(b, ids, tokens, stmt, args...)
		rest = rest[n:]
	}
}

// queueInsertedByToken queues an INSERT of several rows that returns each row's (step_id, step_token), and appends
// the ids to ids in the order of tokens - neither RETURNING nor OUTPUT promises the rows' order.
func queueInsertedByToken(b *txBatch, ids *[]int, tokens []string, stmt string, args ...any) {
	b.queue(func(res *sequel.BatchResults) error {
		rows, err := res.Query()
		if err != nil {
			return errors.Trace(err)
		}
		idByToken := make(map[string]int, len(tokens))
		for rows.Next() {
			var id int
			var token string
			if err := rows.Scan(&id, &token); err != nil {
				return errors.Trace(err)
			}
			idByToken[token] = id
		}
		if err := rows.Err(); err != nil {
			return errors.Trace(err)
		}
		for _, token := range tokens {
			id, ok := idByToken[token]
			if !ok {
				return errors.New("successor insert returned %d of %d rows", len(idByToken), len(tokens))
			}
			*ids = append(*ids, id)
		}
		return nil
	}, stmt, args...)
}

// arrayArgs are successorArraySQL's arguments for rows, whose tokens are tokens.
func (s *successorBatch) arrayArgs(rows []successorRow, tokens []string) []any {
	n := len(rows)
	taskNames, taskURLs := make([]string, n), make([]string, n)
	states, refs, reduced := make([][]byte, n), make([][]byte, n), make([][]byte, n)
	stateBytes, ordinals := make([]int32, n), make([]int32, n)
	for i, r := range rows {
		taskNames[i], taskURLs[i] = r.taskName, r.taskURL
		states[i], refs[i], reduced[i] = r.stateJSON, r.refsJSON, orEmptyJSON(r.reducedJSON)
		stateBytes[i], ordinals[i] = int32(len(r.stateJSON)), int32(r.ordinal)
	}
	return []any{
		s.flowID, s.stepDepth, workflow.StatusPending, parkedNone, s.timeBudgetMs, s.lineageID, s.predecessorID, s.sleepMs,
		s.priority, s.fairnessKey, s.fairnessWeight, s.engineID, s.flowHour,
		s.inheritCancel, s.predecessorID, s.flowID,
		tokens, taskNames, taskURLs, states, stateBytes, refs, reduced, ordinals,
	}
}

// rowArgs are successorRowSQL's arguments for r.
func (s *successorBatch) rowArgs(r successorRow, token string) []any {
	return []any{
		s.flowID, s.stepDepth, token, r.taskName, r.taskURL, r.stateJSON, len(r.stateJSON), r.refsJSON, orEmptyJSON(r.reducedJSON), workflow.StatusPending, parkedNone, s.timeBudgetMs, s.lineageID, r.ordinal, s.predecessorID, s.sleepMs, s.priority, s.fairnessKey, s.fairnessWeight, s.engineID, s.flowHour,
		s.inheritCancel, s.predecessorID, s.flowID,
	}
}

// orEmptyJSON is b, or an empty JSON object if b is empty.
func orEmptyJSON(b []byte) []byte {
	if len(b) == 0 {
		return emptyJSON
	}
	return b
}
