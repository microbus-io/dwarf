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
// At 19 binds per row, 64 rows stay under SQL Server's 2,100-parameter ceiling, the tightest of the dialects.
const successorBatchMax = 64

// successorBatchRow is one row's placeholders, in successorCols order.
const successorBatchRow = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_ADD_MILLIS(NOW_UTC(), ?), ?, ?, ?, ?, ?)"

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
N, and their ids are matched back by step_token, because neither RETURNING nor OUTPUT promises VALUES order. They
are inserted unmarked: their Cancel mark depends on the cancel_watermark the flow-row lock returns in the same
batch, so it is applied afterwards (markCancelled), and only when a Cancel reached the source. MySQL has no
RETURNING, and its multi-row insert ids are not guaranteed contiguous under the default innodb_autoinc_lock_mode=2,
so it inserts row by row with successorInsertSQL, which decides the mark itself and whose id is LastInsertId.

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

// returnsInsertedRows reports whether the driver can return the rows an INSERT created, and the row a flow-row lock
// grab updated, which is what lets successors go in several to a statement and their Cancel mark be decided from
// the lock's cancel_watermark.
func returnsInsertedRows(driver string) bool {
	switch driver {
	case "pgx", "sqlite", "mssql":
		return true
	}
	return false
}

// queue adds the INSERTs of every row to b, and returns the function that reads their step ids back from b's
// results, in row order, once b is sent.
func (s *successorBatch) queue(b *sequel.Batch, driver string) func(res *sequel.BatchResults, ids *[]int) error {
	if !returnsInsertedRows(driver) {
		for _, r := range s.rows {
			b.Queue(successorInsertSQL, s.selfMarkingArgs(r)...)
		}
		n := len(s.rows)
		return func(res *sequel.BatchResults, ids *[]int) error {
			for range n {
				result, err := res.Exec()
				if err != nil {
					return errors.Trace(err)
				}
				id, err := result.LastInsertId()
				if err != nil {
					return errors.Trace(err)
				}
				*ids = append(*ids, int(id))
			}
			return nil
		}
	}
	var chunks [][]string // the tokens of each queued INSERT, in row order
	for rest := s.rows; len(rest) > 0; {
		n := successorBatchMax
		for n > len(rest) {
			n /= 2
		}
		tokens := make([]string, n)
		args := make([]any, 0, n*19)
		for i, r := range rest[:n] {
			tokens[i] = keys.RandomIdentifier(16)
			args = append(args,
				s.flowID, s.stepDepth, tokens[i], r.taskName, r.taskURL, r.stateJSON, r.refsJSON, workflow.StatusPending, parkedNone, s.timeBudgetMs, s.lineageID, r.ordinal, s.predecessorID, s.sleepMs, s.priority, s.fairnessKey, s.fairnessWeight, s.engineID, 0,
			)
		}
		values := strings.Repeat(successorBatchRow+",", n-1) + successorBatchRow
		if driver == "mssql" {
			b.Queue("INSERT INTO dwarf_steps ("+successorCols+") OUTPUT INSERTED.step_id, INSERTED.step_token VALUES "+values, args...)
		} else {
			b.Queue("INSERT INTO dwarf_steps ("+successorCols+") VALUES "+values+" RETURNING step_id, step_token", args...)
		}
		chunks = append(chunks, tokens)
		rest = rest[n:]
	}
	return func(res *sequel.BatchResults, ids *[]int) error {
		for _, tokens := range chunks {
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
		}
		return nil
	}
}

// selfMarkingArgs are successorInsertSQL's arguments for r.
func (s *successorBatch) selfMarkingArgs(r successorRow) []any {
	return []any{
		s.flowID, s.stepDepth, keys.RandomIdentifier(16), r.taskName, r.taskURL, r.stateJSON, r.refsJSON, workflow.StatusPending, parkedNone, s.timeBudgetMs, s.lineageID, r.ordinal, s.predecessorID, s.sleepMs, s.priority, s.fairnessKey, s.fairnessWeight, s.engineID,
		s.inheritCancel, s.flowID, s.predecessorID,
	}
}

/*
queueLone adds to b the INSERT of the batch's only row on PostgreSQL, and returns the function that reads the row's
step id back once b is sent.

The row goes in by successorInsertSQL, which decides its Cancel mark itself, so the transition needs no later
statement for it. Statements queued after it in the same batch can name the new id in SQL as loneSuccessorID,
which is what lets a single-successor transition be one batch.
*/
func (s *successorBatch) queueLone(b *sequel.Batch) func(res *sequel.BatchResults) (int, error) {
	b.Queue(successorInsertSQL+" RETURNING step_id", s.selfMarkingArgs(s.rows[0])...)
	return func(res *sequel.BatchResults) (int, error) {
		var id int
		err := res.QueryRow().Scan(&id)
		return id, errors.Trace(err)
	}
}

/*
loneSuccessorID is PostgreSQL SQL for the id of the step the session inserted last, valid after queueLone's INSERT.

It reads dwarf_steps' own sequence. Do not use lastval(), nor the other dialects' session last-id (@@IDENTITY on
SQL Server): they report the last id generated in ANY table, so a trigger a deployment adds to dwarf_steps that
inserts into a table with its own identity would silently become the step's successor_id and the flow's step_id.
step_id is BIGSERIAL, so a role that can insert a step already holds the sequence privilege currval needs.
*/
const loneSuccessorID = "currval(pg_get_serial_sequence('dwarf_steps', 'step_id'))"

// markCancelled queues onto b the Cancel mark of successors inserted unmarked, in power-of-two chunks of at most
// 512 ids - under the tightest bind-parameter ceiling, and a fixed handful of statement texts. It is queued only
// when a Cancel reached the source, so a transition normally pays nothing for it.
func markCancelled(b *sequel.Batch, ids []int) {
	for len(ids) > 0 {
		n := 512
		for n > len(ids) {
			n /= 2
		}
		args := make([]any, n)
		for i, id := range ids[:n] {
			args[i] = id
		}
		b.Queue("UPDATE dwarf_steps SET cancelling=1 WHERE step_id IN ("+strings.Repeat("?,", n-1)+"?)", args...)
		ids = ids[n:]
	}
}
