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

/*
txBatch sends a transaction's statements as statement batches - one round trip each where the driver pipelines -
pairing every statement with the function that reads its result. queue adds a statement; flush sends the batch,
runs the readers in queue order and closes the results.

A reader's error is returned as is, so an in-transaction sentinel survives it. Results are closed by flush, never
by a defer: on a driver that does not pipeline, Close runs every statement not read yet. Nothing else may run on
the transaction while a flush is reading.
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

/*
insertedID names, for a statement queued after an INSERT, the id that INSERT generates. On PostgreSQL it is pgSQL -
lastFlowIDSQL or lastStepIDSQL, the sequence's current value - so both statements share a batch. Elsewhere the
batch is flushed so that id() is read, and it is bound; those drivers do not pipeline, so the flush costs nothing.
*/
func (b *txBatch) insertedID(pgSQL string, id func() any) (string, []any, error) {
	if b.tx.DriverName() == "pgx" {
		return pgSQL, nil, nil
	}
	if err := b.flush(); err != nil {
		return "", nil, err
	}
	return "?", []any{id()}, nil
}

// execOnly reads a statement's result for its error alone.
func execOnly(res *sequel.BatchResults) error {
	_, err := res.Exec()
	return errors.Trace(err)
}

// queueInsertID queues an INSERT ... VALUES and passes the id it generates in column to set: read by RETURNING on
// PostgreSQL and SQLite, OUTPUT on SQL Server, and LastInsertId on MySQL. query must spell its VALUES keyword as
// " VALUES ", once: that is where SQL Server's OUTPUT clause goes.
func queueInsertID(b *txBatch, column string, set func(int64), query string, args ...any) {
	switch driver := b.tx.DriverName(); {
	case driver == "mssql":
		query = strings.Replace(query, " VALUES ", " OUTPUT INSERTED."+column+" VALUES ", 1)
	case returnsInsertedRows(driver):
		query += " RETURNING " + column
	default:
		b.queue(func(res *sequel.BatchResults) error {
			result, err := res.Exec()
			if err != nil {
				return errors.Trace(err)
			}
			id, err := result.LastInsertId()
			if err != nil {
				return errors.Trace(err)
			}
			set(id)
			return nil
		}, query, args...)
		return
	}
	b.queue(func(res *sequel.BatchResults) error {
		var id int64
		if err := res.QueryRow().Scan(&id); err != nil {
			return errors.Trace(err)
		}
		set(id)
		return nil
	}, query, args...)
}

// lastFlowIDSQL and lastStepIDSQL are PostgreSQL SQL for the id of the flow, or of the step, the session inserted
// last: each table's own sequence, which a role able to insert into the BIGSERIAL table may read. Do not use
// lastval(), nor another dialect's session last-id (@@IDENTITY): they report ids from ANY table, so a trigger a
// deployment adds would silently become the id written. In a WHERE clause, compare with them as `(SELECT ...)`:
// bare, the volatile function cannot be an index key.
const (
	lastFlowIDSQL = "currval(pg_get_serial_sequence('dwarf_flows', 'flow_id'))"
	lastStepIDSQL = "currval(pg_get_serial_sequence('dwarf_steps', 'step_id'))"
)
