/*
Copyright (c) 2023-2026 Microbus LLC and various contributors

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
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"

	"github.com/microbus-io/dwarf/engine/resources"
)

const sequenceName = "foreman@2026-03-10" // Do not change

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
	dirFS, err := fs.Sub(resources.FS, "sql")
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = db.Migrate(sequenceName, dirFS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
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
	numShards := int(e.numShards.Load())
	testID := t.Name()
	for i := 1; i <= numShards; i++ {
		db, err := e.openDatabaseShardForTest(context.Background(), dataSourceName, i, testID)
		if err != nil {
			return errors.Trace(err)
		}
		e.dbs = append(e.dbs, db)
	}
	return nil
}
