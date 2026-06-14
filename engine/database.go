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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

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

// numDBShards returns the current number of database shards.
func (e *Engine) numDBShards() int {
	e.dbsLock.RLock()
	n := len(e.dbs)
	e.dbsLock.RUnlock()
	return n
}

// eachShard fans op out over every shard concurrently using an errgroup.
func (e *Engine) eachShard(ctx context.Context, op func(ctx context.Context, db *sequel.DB, shard int) error) error {
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
	dataSourceName := e.dsn.Load().(string)
	numShards := int(e.numShards.Load())
	if numShards > 1 && !strings.Contains(dataSourceName, "%d") {
		return errors.New("DSN must contain %%d when NumShards > 1")
	}
	for i := 1; i <= numShards; i++ {
		db, err := e.openDatabaseShard(ctx, dataSourceName, i)
		if err != nil {
			return errors.Trace(err)
		}
		e.dbs = append(e.dbs, db)
	}
	return nil
}

// openDatabaseShardForTest opens a single shard using sequel.CreateTestingDatabase
// for per-test isolation.
func (e *Engine) openDatabaseShardForTest(ctx context.Context, dataSourceName string, shardIndex int, testID string) (*sequel.DB, error) {
	const driverName = ""
	dsn := dataSourceName
	if strings.Contains(dataSourceName, "%d") {
		dsn = fmt.Sprintf(dataSourceName, shardIndex)
	}
	// Each shard must resolve to its own isolated testing database. CreateTestingDatabase keys its
	// database on (driver, baseDSN, testID), so without a per-shard distinguisher every shard of a
	// multi-shard engine would collapse onto a single shared in-memory database. A DSN carrying a
	// %d placeholder already produces a distinct base per shard; otherwise fold the shard index into
	// the test ID so the cache key differs per shard.
	shardTestID := fmt.Sprintf("%s#%d", testID, shardIndex)
	dsn, err := sequel.CreateTestingDatabase(driverName, dsn, shardTestID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return e.openAndMigrate(dsn)
}

// openDatabaseShard opens a single database shard connection and runs migrations.
func (e *Engine) openDatabaseShard(ctx context.Context, dataSourceName string, shardIndex int) (*sequel.DB, error) {
	dsn := dataSourceName
	if strings.Contains(dataSourceName, "%d") {
		dsn = fmt.Sprintf(dataSourceName, shardIndex)
	}
	return e.openAndMigrate(dsn)
}

// openAndMigrate opens a database connection and runs schema migrations.
func (e *Engine) openAndMigrate(dsn string) (*sequel.DB, error) {
	const driverName = ""
	db, err := sequel.Open(driverName, dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	poolSize := int(e.maxOpenConns.Load())
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)
	// Point sequel at the engine's own observability providers before migrating, so the SQL layer's
	// sequel_* spans/metrics and its migration logs flow through the same logger/tracer/meter the host
	// injected into dwarf - and migration telemetry is captured too. Resolved injected-or-global,
	// matching how the engine resolves its own providers in initTracer/initMetrics.
	e.configureDBTelemetry(db)
	err = db.Migrate(sequenceName, migrations.FS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// configureDBTelemetry directs a shard's sequel DB to the engine's logger, tracer, and meter providers,
// so the SQL layer emits sequel_* telemetry under the host's configured OTEL pipeline. The tracer/meter
// fall back to the global providers (no-op unless the host configured the SDK) when none was injected,
// exactly as initTracer/initMetrics resolve the engine's own providers.
func (e *Engine) configureDBTelemetry(db *sequel.DB) {
	// Only hand sequel a logger when the host explicitly set one. The engine's own e.logger defaults to a
	// discard logger so its internal logging is silent until configured; sequel similarly should not log
	// (it emits migration events at Info) unless the host opted in. A nil logger disables sequel's logging
	// entirely.
	if e.loggerSet {
		db.SetLogger(e.logger)
	}
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

// openTestDatabase opens per-test SQLite databases for all shards.
func (e *Engine) openTestDatabase(t *testing.T) error {
	t.Helper()
	dataSourceName := e.dsn.Load().(string)
	// Allow the whole fixture suite to run against a real server database without changing any test:
	// when no DSN was set explicitly, fall back to SEQUEL_TESTING_DSN — the same variable sequel itself
	// reads, so one knob redirects dwarf and any other sequel-backed suite at the same server. Unset/empty
	// keeps the SQLite in-memory default. sequel.CreateTestingDatabase creates an isolated, auto-dropped
	// testing_* database per shard off this base DSN (needs CREATE/DROP DATABASE privilege on a server).
	if dataSourceName == "" {
		dataSourceName = os.Getenv("SEQUEL_TESTING_DSN")
	}
	numShards := int(e.numShards.Load())
	// sequel.CreateTestingDatabase derives the throwaway database name as testing_<hour>_<base>_<testID>,
	// which must fit the strictest SQL identifier limit (Postgres 63, MySQL 64 chars). Hash the test name
	// to a fixed 16 hex chars so the derived name is always bounded, regardless of how long or deeply
	// nested the Go (sub)test name is. 16 hex chars (64 bits) is collision-free across a test suite.
	sum := sha256.Sum256([]byte(t.Name()))
	testID := hex.EncodeToString(sum[:])[:16]
	for i := 1; i <= numShards; i++ {
		db, err := e.openDatabaseShardForTest(context.Background(), dataSourceName, i, testID)
		if err != nil {
			return errors.Trace(err)
		}
		e.dbs = append(e.dbs, db)
	}
	return nil
}
