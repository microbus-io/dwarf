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
	"context"
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

// successorBatch inserts the successors of one transition, which share everything but the per-row fields.
//
// Rows are inserted by multi-row INSERTs, so a fan-out of N costs about N/64 statements plus one per set bit of the
// remainder, rather than N. The Cancel mark is decided in Go from the cancel_watermark the transaction's flow-row
// lock grab returned (watermarkRead): the flag is the same for every successor of one source, and the watermark
// cannot move while the lock is held. Only the drivers whose lock grab can return that row batch; any other -
// MySQL, which has no RETURNING and whose multi-row insert ids are not guaranteed contiguous under the default
// innodb_autoinc_lock_mode=2 - inserts row by row with successorInsertSQL, which reads the watermark itself.
//
// Within a chunk, step ids follow VALUES order on every supported dialect in practice, but none promises it. So
// nothing that must be deterministic may order siblings by step_id: the fan-in merge and the failed-cohort merge
// order them by fan_out_ordinal.
type successorBatch struct {
	flowID          int
	stepDepth       int
	timeBudgetMs    int
	lineageID       int
	predecessorID   int
	sleepMs         int64
	priority        int
	fairnessKey     string
	fairnessWeight  float64
	engineID        int64
	inheritCancel   int
	cancelWatermark int
	watermarkRead   bool
	rows            []successorRow
}

// batchesSuccessors reports whether the driver's flow-row lock grab returns the cancel watermark, which is what
// lets successorBatch insert several rows to a statement. It must agree with the lock grab in processStep.
func batchesSuccessors(driver string) bool {
	switch driver {
	case "pgx", "sqlite", "mssql":
		return true
	}
	return false
}

// flush inserts the pending rows and appends their step ids to ids in row order.
func (b *successorBatch) flush(ctx context.Context, tx *sequel.Tx, ids *[]int) error {
	defer func() { b.rows = b.rows[:0] }()
	if len(b.rows) == 0 {
		return nil
	}
	if !batchesSuccessors(tx.DriverName()) {
		for _, r := range b.rows {
			id, err := tx.InsertReturnID(ctx, "step_id", successorInsertSQL,
				b.flowID, b.stepDepth, keys.RandomIdentifier(16), r.taskName, r.taskURL, r.stateJSON, r.refsJSON, workflow.StatusPending, parkedNone, b.timeBudgetMs, b.lineageID, r.ordinal, b.predecessorID, b.sleepMs, b.priority, b.fairnessKey, b.fairnessWeight, b.engineID,
				b.inheritCancel, b.flowID, b.predecessorID,
			)
			if err != nil {
				return errors.Trace(err)
			}
			*ids = append(*ids, int(id))
		}
		return nil
	}
	if !b.watermarkRead {
		// Inserting without the watermark would silently drop the Cancel mark of every row.
		return errors.New("successors inserted before the flow-row lock grab read the cancel watermark")
	}
	cancelling := 0
	if b.inheritCancel == 1 && b.predecessorID <= b.cancelWatermark {
		cancelling = 1
	}
	for rest := b.rows; len(rest) > 0; {
		n := successorBatchMax
		for n > len(rest) {
			n /= 2
		}
		if err := b.insertChunk(ctx, tx, rest[:n], cancelling, ids); err != nil {
			return errors.Trace(err)
		}
		rest = rest[n:]
	}
	return nil
}

// insertChunk inserts rows with one statement. The returned ids are matched to rows by step_token rather than by
// position, because neither RETURNING nor OUTPUT promises to report rows in VALUES order.
func (b *successorBatch) insertChunk(ctx context.Context, tx *sequel.Tx, rows []successorRow, cancelling int, ids *[]int) error {
	tokens := make([]string, len(rows))
	args := make([]any, 0, len(rows)*19)
	for i, r := range rows {
		tokens[i] = keys.RandomIdentifier(16)
		args = append(args,
			b.flowID, b.stepDepth, tokens[i], r.taskName, r.taskURL, r.stateJSON, r.refsJSON, workflow.StatusPending, parkedNone, b.timeBudgetMs, b.lineageID, r.ordinal, b.predecessorID, b.sleepMs, b.priority, b.fairnessKey, b.fairnessWeight, b.engineID, cancelling,
		)
	}
	values := strings.Repeat(successorBatchRow+",", len(rows)-1) + successorBatchRow
	var stmt string
	if tx.DriverName() == "mssql" {
		stmt = "INSERT INTO dwarf_steps (" + successorCols + ") OUTPUT INSERTED.step_id, INSERTED.step_token VALUES " + values
	} else {
		stmt = "INSERT INTO dwarf_steps (" + successorCols + ") VALUES " + values + " RETURNING step_id, step_token"
	}
	result, err := tx.QueryContext(ctx, stmt, args...)
	if err != nil {
		return errors.Trace(err)
	}
	defer result.Close()
	idByToken := make(map[string]int, len(rows))
	for result.Next() {
		var id int
		var token string
		if err := result.Scan(&id, &token); err != nil {
			return errors.Trace(err)
		}
		idByToken[token] = id
	}
	if err := result.Err(); err != nil {
		return errors.Trace(err)
	}
	for _, token := range tokens {
		id, ok := idByToken[token]
		if !ok {
			return errors.New("successor insert returned %d of %d rows", len(idByToken), len(rows))
		}
		*ids = append(*ids, id)
	}
	return nil
}
