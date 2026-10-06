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

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// queueFunc queues a statement onto a batch together with the function that reads its result once sent.
type queueFunc func(read func(res *sequel.BatchResults) error, query string, args ...any)

/*
txBatch sends a transaction's statements as statement batches - one round trip each where the driver pipelines -
pairing every statement with the function that reads its result. queue adds a statement; flush sends the batch,
runs the readers in queue order and closes the results.

A reader's error is returned as is, so an in-transaction sentinel survives it. Results are closed by flush and
never by a defer: on a driver that does not pipeline, Close runs every statement not read yet, while the rollback
that follows an early return abandons them unrun. Nothing else may run on the transaction while a flush is
reading.
*/
type txBatch struct {
	ctx   context.Context
	tx    *sequel.Tx
	batch *sequel.Batch
	reads []func(*sequel.BatchResults) error
}

// newTxBatch returns an empty txBatch on tx.
func newTxBatch(ctx context.Context, tx *sequel.Tx) *txBatch {
	return &txBatch{ctx: ctx, tx: tx, batch: tx.Batch()}
}

// queue adds a statement and the function that reads its result.
func (b *txBatch) queue(read func(res *sequel.BatchResults) error, query string, args ...any) {
	b.batch.Queue(query, args...)
	b.reads = append(b.reads, read)
}

// flush sends the queued statements, if any, and reads their results in order.
func (b *txBatch) flush() error {
	if b.batch.Len() == 0 {
		return nil
	}
	res := b.batch.Send(b.ctx)
	for _, read := range b.reads {
		if err := read(res); err != nil {
			return err
		}
	}
	b.batch, b.reads = b.tx.Batch(), nil
	return errors.Trace(res.Close())
}

// execOnly reads a statement's result for its error alone.
func execOnly(res *sequel.BatchResults) error {
	_, err := res.Exec()
	return errors.Trace(err)
}

// queueInsertID queues an INSERT ... VALUES and reads the id it generates in column into id: by RETURNING on
// PostgreSQL and SQLite, OUTPUT on SQL Server, and LastInsertId on MySQL. query must spell its VALUES keyword as
// " VALUES ", once: that is where SQL Server's OUTPUT clause goes.
func queueInsertID(queue queueFunc, driver, column string, id *int64, query string, args ...any) {
	switch {
	case driver == "mssql":
		query = strings.Replace(query, " VALUES ", " OUTPUT INSERTED."+column+" VALUES ", 1)
	case returnsInsertedRows(driver):
		query += " RETURNING " + column
	default:
		queue(func(res *sequel.BatchResults) error {
			result, err := res.Exec()
			if err != nil {
				return errors.Trace(err)
			}
			*id, err = result.LastInsertId()
			return errors.Trace(err)
		}, query, args...)
		return
	}
	queue(func(res *sequel.BatchResults) error {
		return errors.Trace(res.QueryRow().Scan(id))
	}, query, args...)
}

/*
lastFlowIDSQL and lastStepIDSQL are PostgreSQL SQL for the id of the flow, or of the step, the session inserted
last. They let the statements queued after an INSERT name the new row without waiting for the INSERT's result,
which is what lets a chain of dependent writes be one batch.

They read each table's own sequence. Do not use lastval(), nor another dialect's session last-id (@@IDENTITY on SQL
Server): they report the last id generated in ANY table, so a trigger a deployment adds that inserts into a table
with its own identity would silently become the id written. Both ids are BIGSERIAL, so a role that can insert a row
already holds the sequence privilege currval needs.

currval is volatile, so a WHERE clause must compare with it as a scalar subquery - `flow_id=(SELECT ...)` - which
PostgreSQL evaluates once; compared directly, it cannot be an index key and the statement scans the table.
*/
const (
	lastFlowIDSQL = "currval(pg_get_serial_sequence('dwarf_flows', 'flow_id'))"
	lastStepIDSQL = "currval(pg_get_serial_sequence('dwarf_steps', 'step_id'))"
)
