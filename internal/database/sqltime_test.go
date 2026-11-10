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

package database

import (
	"context"
	"testing"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestSQLTime_ReadsAsTheInstantItNames pins the bound SQLTime writes: compared to the database's NOW_UTC(), the
// current instant reads within a second of it on every dialect, and on PostgreSQL in a session whose time zone is not
// UTC.
func TestSQLTime_ReadsAsTheInstantItNames(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	var set ShardSet
	err := set.Open(ctx, Config{
		Shards:      map[int]ShardConfig{1: {MaxIdleConns: 2, MaxOpenConns: 4}},
		TestID:      TestID(t.Name()),
		TestConnCap: 4,
	})
	assert.Must(assert.NoError(err))
	defer set.Close()
	db, err := set.Shard(1)
	assert.Must(assert.NoError(err))
	diff := func(x sequel.Executor) float64 {
		expr, arg := SQLTime(db.DriverName(), time.Now())
		var ms float64
		assert.NoError(x.QueryRowContext(ctx, "SELECT DATE_DIFF_MILLIS(NOW_UTC(), "+expr+")", arg).Scan(&ms))
		return ms
	}
	ms := diff(db)
	assert.True(ms > -1000 && ms < 1000, "%vms off", ms)
	if db.DriverName() != "pgx" {
		return
	}
	errRollback := errors.New("rollback")
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if _, err := tx.ExecContext(ctx, "SET LOCAL TIME ZONE 'Asia/Tokyo'"); err != nil {
			return err
		}
		ms = diff(tx)
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		assert.NoError(err)
	}
	assert.True(ms > -1000 && ms < 1000, "%vms off on a session in Tokyo", ms)
}

// TestSQLTime_RoundsUpToTheColumnsPrecision pins the text SQLTime binds: rounded up to the millisecond on every
// dialect, an exact millisecond kept, and cast to the dialect's time column type.
func TestSQLTime_RoundsUpToTheColumnsPrecision(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	expr, arg := SQLTime("mysql", time.Date(2026, 11, 4, 1, 2, 3, 123400000, time.UTC))
	assert.Equal("CAST(? AS DATETIME(3))", expr)
	assert.Equal("2026-11-04 01:02:03.124", arg)
	_, arg = SQLTime("sqlite", time.Date(2026, 11, 4, 1, 2, 3, 123000000, time.UTC))
	assert.Equal("2026-11-04 01:02:03.123", arg, "an exact millisecond is kept")
	expr, arg = SQLTime("pgx", time.Date(2026, 11, 4, 1, 2, 3, 123000100, time.UTC))
	assert.Equal("CAST(? AS TIMESTAMP(3))", expr)
	assert.Equal("2026-11-04 01:02:03.124", arg, "milliseconds on PostgreSQL too")
}

// TestSQLTime_AFarFutureBoundIsTheLastInstant pins the clamp: a time past the year 9999 is written as MaxSQLTime.
func TestSQLTime_AFarFutureBoundIsTheLastInstant(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	_, arg := SQLTime("mssql", time.Date(20000, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal("9999-12-31 23:59:59.999", arg)
}
