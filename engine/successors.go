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
// At 21 binds per row, 64 rows stay under SQL Server's 2,100-parameter ceiling, the tightest of the dialects.
const successorBatchMax = 64

/*
successorRowSQL is one successor row's VALUES, in successorCols order. Its last column, the Cancel mark, is decided
by the row itself: the successor is marked when its source step already existed when the flow was cancelled (its
id is at or below the flow's cancel_watermark) and the source was not itself covered (the inherit argument, 1 or
0). That reaches the one step Cancel's own mark cannot - a source whose completion write landed before Cancel,
with its successors still to be inserted - while the successor of a step inserted after the Cancel, such as a
handler that caught the cancellation and everything it leads to, stays unmarked, which is what lets a flow
recover. Every insert of a step holds the flow row's lock, and Cancel writes the watermark only under it, so the
read cannot race it. Arguments: the eighteen step columns in order, then the inherit flag, the source step id and
the flow id.

Do not compare timestamps here (the flow's cancel time against the source's started_at). Three dialects store
milliseconds, and a source that starts in the same millisecond as the Cancel reads as not-before it, losing the
Cancel - measured at ~1 in 4 runs of TestCancelInTransitionGap on SQLite. Making it inclusive only moves the tie to
the handler, whose successors would then be cancelled too.
*/
const successorRowSQL = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?," +
	" (SELECT CASE WHEN ?=1 AND ? <= cancel_watermark THEN 1 ELSE 0 END FROM dwarf_flows WHERE flow_id=?))"

// successorInsertSQL inserts one successor row (successorRowSQL).
const successorInsertSQL = "INSERT INTO dwarf_steps (" + successorCols + ") VALUES " + successorRowSQL

// successorRow is what differs between the successors one transition inserts.
type successorRow struct {
	taskName  string
	taskURL   string
	stateJSON []byte
	refsJSON  []byte
	ordinal   int
}

/*
successorBatch holds the successors of one transition, which share everything but the per-row fields, and queues
their INSERTs onto the transition's statement batch.

Where the driver returns inserted rows (RETURNING on PostgreSQL and SQLite, OUTPUT on SQL Server) the rows go in
multi-row INSERTs, so a fan-out of N costs about N/64 statements plus one per set bit of the remainder, rather than
N, and their ids are matched back by step_token, because neither RETURNING nor OUTPUT promises VALUES order. MySQL
has no RETURNING, and its multi-row insert ids are not guaranteed contiguous under the default
innodb_autoinc_lock_mode=2, so it inserts row by row and reads each id as LastInsertId.

Within a chunk, step ids follow VALUES order on every supported dialect in practice, but none promises it. So
nothing that must be deterministic may order siblings by step_id: the fan-in merge and the failed-cohort merge order
them by fan_out_ordinal.
*/
type successorBatch struct {
	flowID         int
	stepDepth      int
	timeBudgetMs   int
	lineageID      int
	predecessorID  int
	sleepMs        int64
	priority       int
	fairnessKey    string
	fairnessWeight float64
	engineID       int64
	inheritCancel  int
	rows           []successorRow
}

// returnsInsertedRows reports whether the driver can return the rows an INSERT created.
func returnsInsertedRows(driver string) bool {
	switch driver {
	case "pgx", "sqlite", "mssql":
		return true
	}
	return false
}

// queue queues the INSERTs of the pending rows, whose readers append the new step ids to ids in row order, and
// empties the pending rows.
func (s *successorBatch) queue(queue queueFunc, driver string, ids *[]int) {
	defer func() { s.rows = s.rows[:0] }()
	if !returnsInsertedRows(driver) {
		for _, r := range s.rows {
			queue(func(res *sequel.BatchResults) error {
				result, err := res.Exec()
				if err != nil {
					return errors.Trace(err)
				}
				id, err := result.LastInsertId()
				if err != nil {
					return errors.Trace(err)
				}
				*ids = append(*ids, int(id))
				return nil
			}, successorInsertSQL, s.rowArgs(r, keys.RandomIdentifier(16))...)
		}
		return
	}
	for rest := s.rows; len(rest) > 0; {
		n := successorBatchMax
		for n > len(rest) {
			n /= 2
		}
		tokens := make([]string, n)
		args := make([]any, 0, n*21)
		for i, r := range rest[:n] {
			tokens[i] = keys.RandomIdentifier(16)
			args = append(args, s.rowArgs(r, tokens[i])...)
		}
		values := strings.Repeat(successorRowSQL+",", n-1) + successorRowSQL
		stmt := "INSERT INTO dwarf_steps (" + successorCols + ") VALUES " + values + " RETURNING step_id, step_token"
		if driver == "mssql" {
			stmt = "INSERT INTO dwarf_steps (" + successorCols + ") OUTPUT INSERTED.step_id, INSERTED.step_token VALUES " + values
		}
		queue(func(res *sequel.BatchResults) error {
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
		rest = rest[n:]
	}
}

// rowArgs are successorRowSQL's arguments for r.
func (s *successorBatch) rowArgs(r successorRow, token string) []any {
	return []any{
		s.flowID, s.stepDepth, token, r.taskName, r.taskURL, r.stateJSON, r.refsJSON, workflow.StatusPending, parkedNone, s.timeBudgetMs, s.lineageID, r.ordinal, s.predecessorID, s.sleepMs, s.priority, s.fairnessKey, s.fairnessWeight, s.engineID,
		s.inheritCancel, s.predecessorID, s.flowID,
	}
}
