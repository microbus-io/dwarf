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
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel"

	"github.com/microbus-io/dwarf/migrations"
)

const sequenceName = "github.com/microbus-io/dwarf" // Namespaces this engine's migrations in sequel_migrations; do not change once deployed

// shard returns the database connection for the given 1-based shard index.
func (e *Engine) shard(n int) (*sequel.DB, error) {
	e.dbsLock.RLock()
	defer e.dbsLock.RUnlock()
	if n < 1 || n > len(e.dbs) {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	return e.dbs[n-1], nil
}

// numDBShards returns the current number of open database shards.
func (e *Engine) numDBShards() int {
	e.dbsLock.RLock()
	n := len(e.dbs)
	e.dbsLock.RUnlock()
	return n
}

// onEachShard fans op out over every shard concurrently using an errgroup.
func (e *Engine) onEachShard(ctx context.Context, op func(ctx context.Context, db *sequel.DB, shard int) error) error {
	numShards := e.numDBShards()
	if numShards == 1 {
		db, err := e.shard(1)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(op(ctx, db, 1))
	}
	errs := make([]error, numShards+1)
	var wg sync.WaitGroup
	for i := 1; i <= numShards; i++ {
		si := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			db, err := e.shard(si)
			if err != nil {
				errs[si] = err
				return
			}
			errs[si] = op(ctx, db, si)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// openDatabase opens connections to all database shards and runs migrations.
func (e *Engine) openDatabase(ctx context.Context) error {
	numShards := int(e.numShards.Load())
	for i := 1; i <= numShards; i++ {
		db, err := e.openDatabaseShard(ctx, i)
		if err != nil {
			return errors.Trace(err)
		}
		e.dbs = append(e.dbs, db)
	}
	return nil
}

// expandShards reconciles the open shards up to the current numShards target, opening+migrating any not yet
// live. Append-only and concurrency-safe: expandLock serializes callers, open+migrate runs outside dbsLock,
// and each ready shard is appended under dbsLock so the hot path only sees fully-ready shards. No-op before
// Startup or when the target is at/below the live count.
func (e *Engine) expandShards(ctx context.Context) error {
	if !e.started.Load() {
		return nil
	}
	e.expandLock.Lock()
	defer e.expandLock.Unlock()

	target := int(e.numShards.Load())
	e.dbsLock.RLock()
	current := len(e.dbs)
	e.dbsLock.RUnlock()
	if target <= current {
		return nil
	}

	for i := current + 1; i <= target; i++ {
		db, err := e.openDatabaseShard(ctx, i)
		if err != nil {
			return errors.Trace(err)
		}
		e.dbsLock.Lock()
		e.dbs = append(e.dbs, db)
		e.dbsLock.Unlock()
	}
	// The shard count is the pool-sizing divisor, so growth shrinks every shard's share: resize the
	// pre-existing shards down too (the newly opened ones were already sized for the new count at open).
	e.dbsLock.RLock()
	for _, db := range e.dbs {
		e.applyConnPoolSizes(db)
	}
	e.dbsLock.RUnlock()
	return nil
}

// openDatabaseShard opens a single database shard connection and runs migrations. It resolves the base DSN
// (in test mode an unset explicit DSN falls back to SEQUEL_TESTING_DSN, then the SQLite in-memory default),
// substitutes %d with the shard index, and in test mode (testHashedID set) wraps the result via
// sequel.CreateTestingDatabase into an isolated, auto-dropped database keyed on (driver, baseDSN, testID) -
// the base DSN's %d already distinguishes the shards.
func (e *Engine) openDatabaseShard(ctx context.Context, shardIndex int) (db *sequel.DB, err error) {
	dsn := e.dsn.Load().(string)
	if e.testHashedID != "" {
		if dsn == "" {
			dsn = os.Getenv("SEQUEL_TESTING_DSN")
		}
		if dsn == "" {
			dsn = "file:dwarf_%d?mode=memory&cache=shared"
		}
	}
	if strings.Contains(dsn, "%d") {
		dsn = fmt.Sprintf(dsn, shardIndex)
	} else if shardIndex > 1 {
		// No %d to distinguish shards, yet this is shard 2+ - every shard would collapse onto one database.
		return nil, errors.New("DSN must contain %%d when NumShards > 1")
	}
	if e.testHashedID != "" {
		var err error
		dsn, err = sequel.CreateTestingDatabase("", dsn, e.testHashedID)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	db, err = e.openAndMigrate(dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// openAndMigrate opens a database connection and runs schema migrations.
func (e *Engine) openAndMigrate(dsn string) (*sequel.DB, error) {
	const driverName = ""
	db, err := sequel.Open(driverName, dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.applyConnPoolSizes(db)
	// Drain idle connections and recycle aged ones - but not on SQLite, whose in-memory test databases are
	// dropped the moment their last connection closes (a closed idle/expired conn would lose the data).
	if db.DriverName() != "sqlite" {
		db.SetConnMaxIdleTime(2 * time.Minute)
		db.SetConnMaxLifetime(1 * time.Hour)
	}
	// Point Sequel at the engine's own observability providers
	e.configureDBTelemetry(db)
	err = db.Migrate(sequenceName, migrations.FS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// calcConnPoolSizes sizes the connection pool of a shard given the sizing parameters.
func calcConnPoolSizes(workers, shards, workersPerConn, cap int) (idle, open int) {
	if shards < 1 {
		shards = 1
	}
	if workersPerConn < 1 {
		workersPerConn = 1
	}
	if cap < 1 {
		cap = 1
	}
	denom := shards * workersPerConn
	idle = (workers + denom - 1) / denom // ceil(workers / (shards*workersPerConn))
	idle = max(idle, 2)                  // at least 2 connections per shard
	open = idle*2 + 2                    // warm core + burst headroom
	if open > cap {
		open = cap
	}
	if idle > open { // a tight ceiling can pull open below the formula idle
		idle = open
	}
	return idle, open
}

// applyConnPoolSizes computes the per-shard connection pool sizes from the live config and applies them to db.
func (e *Engine) applyConnPoolSizes(db *sequel.DB) {
	idle, open := calcConnPoolSizes(int(e.workers.Load()), int(e.numShards.Load()), int(e.workersPerConn.Load()), int(e.maxOpenConns.Load()))
	db.SetMaxIdleConns(idle)
	db.SetMaxOpenConns(open)
}

// configureDBTelemetry directs a shard's sequel DB to the engine's logger, tracer, and meter providers.
func (e *Engine) configureDBTelemetry(db *sequel.DB) {
	db.SetLogger(e.logger)
	tp := e.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	db.SetTracerProvider(tp)
	mp := e.meterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	db.SetMeterProvider(mp)
}

// closeDatabase closes all database shard connections.
func (e *Engine) closeDatabase() {
	for _, db := range e.dbs {
		db.Close()
	}
	e.dbs = nil
}
