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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// TestTimestamps_SurviveANonUTCSession pins that a time written with NOW_UTC() reads back as the current instant
// on a PostgreSQL session whose time zone is not UTC. NOW_UTC() is a timestamp WITHOUT time zone holding UTC wall
// time; stored into a TIMESTAMPTZ column it is converted through the session's time zone, so on such a session
// every stored instant is off by the zone's offset (a flow.Retry horizon anchored at creation then never expires).
func TestTimestamps_SurviveANonUTCSession(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(hosts.NewMux())
	assert.NoError(e.SetWorkers(0))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	db, err := e.db.Shard(1)
	if !assert.NoError(err) {
		return
	}
	if db.DriverName() != "pgx" {
		t.Skip("time zone conversion of a stored timestamp is PostgreSQL's")
	}

	// No engine table holds a zoned timestamp.
	var zoned int
	err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.columns"+
		" WHERE table_schema=current_schema() AND table_name LIKE 'dwarf_%' AND data_type='timestamp with time zone'").Scan(&zoned)
	assert.NoError(err)
	assert.Equal(0, zoned, "an engine time column is TIMESTAMPTZ")

	// Every engine time column holds milliseconds, as NOW_UTC() writes them: a finer column would store a finer value
	// rounded, and a bare TIMESTAMP is microseconds.
	rows, err := db.QueryContext(ctx, "SELECT table_name, column_name, datetime_precision FROM information_schema.columns"+
		" WHERE table_schema=current_schema() AND (table_name LIKE 'dwarf_%' OR table_name='sequel_migrations')"+
		" AND data_type LIKE 'timestamp%'")
	if assert.NoError(err) {
		columns := 0
		for rows.Next() {
			var table, column string
			var precision int
			assert.NoError(rows.Scan(&table, &column, &precision))
			assert.Equal(3, precision, "%s.%s", table, column)
			columns++
		}
		assert.NoError(rows.Err())
		rows.Close()
		assert.True(columns >= 20, "found %d time columns", columns)
	}

	// A write made on a non-UTC session reads back as now.
	errRollback := errors.New("rollback")
	var seenAt time.Time
	err = db.Transact(ctx, func(tx *sequel.Tx) error {
		if _, err := tx.ExecContext(ctx, "SET LOCAL TIME ZONE 'America/Los_Angeles'"); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "INSERT INTO dwarf_peers (engine_id, seen_at, ordering_seq, working, draining, rtt_us)"+
			" VALUES (?, NOW_UTC(), 0, 0, 0, 0)", int64(1)<<62); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, "SELECT seen_at FROM dwarf_peers WHERE engine_id=?", int64(1)<<62).Scan(&seenAt); err != nil {
			return err
		}
		return errRollback
	})
	if !errors.Is(err, errRollback) {
		assert.NoError(err)
	}
	drift := time.Since(seenAt)
	assert.True(drift > -time.Minute && drift < time.Minute, "seen_at is %v off the current time", drift)
}
