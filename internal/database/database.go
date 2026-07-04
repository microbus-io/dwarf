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

// Package database owns the engine's sharded SQL connections: it opens and migrates every shard, routes by
// 1-based shard index, fans an operation out over all shards, and closes them. It is DB-lifecycle-complete
// (open → migrate → size → fan-out → close) and speaks only in resolved connection-pool sizes - the sizing
// *policy* (how many connections a worker count implies) stays with the caller, which hands ShardSet the two
// computed integers via Config / SetMax*Conns.
package database

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"log/slog"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	"github.com/microbus-io/dwarf/internal/migrations"
)

// sequenceName namespaces this engine's migrations in sequel_migrations; do not change once deployed.
const sequenceName = "github.com/microbus-io/dwarf"

// Config is the full open-time configuration of a ShardSet. Field names mirror the corresponding sequel.DB /
// *sql.DB setters (Logger→SetLogger, TracerProvider→SetTracerProvider, MeterProvider→SetMeterProvider,
// MaxIdleConns→SetMaxIdleConns, MaxOpenConns→SetMaxOpenConns), so the mapping from field to setter is 1:1.
type Config struct {
	// DSN is the sequel data source name; "%d" is substituted with the 1-based shard index. In test mode
	// (TestID set) an empty DSN falls back to SEQUEL_TESTING_DSN, then to the SQLite in-memory default.
	DSN       string
	NumShards int
	// TestID, when non-empty, wraps each shard via sequel.CreateTestingDatabase into an isolated, auto-dropped
	// database keyed on (driver, baseDSN, TestID) - the base DSN's %d already distinguishes the shards.
	TestID string

	Logger         *slog.Logger
	TracerProvider trace.TracerProvider // nil → otel.GetTracerProvider()
	MeterProvider  metric.MeterProvider // nil → otel.GetMeterProvider()

	// MaxIdleConns / MaxOpenConns are the resolved per-shard pool sizes, computed by the caller and applied
	// verbatim. The connection-sizing formula is not this package's concern.
	MaxIdleConns int
	MaxOpenConns int
}

// ShardSet is the engine's set of open, migrated database shards. The zero value is a usable, empty set. The
// shard count is fixed for the set's life (established at Open), so a caller may size per-shard state from
// NumShards() and index it by the shard arg without racing a concurrent change.
type ShardSet struct {
	mu  sync.RWMutex
	dbs []*sequel.DB
}

// Open opens and migrates every shard, applying cfg's pool sizes and telemetry. On any shard failure it closes
// the shards already opened this attempt (so a partial failure leaks no connections and leaves the set empty
// for a clean retry) and returns the error. Not safe to call concurrently with itself; call once at startup.
func (s *ShardSet) Open(ctx context.Context, cfg Config) error {
	numShards := max(cfg.NumShards, 1)
	for i := 1; i <= numShards; i++ {
		db, err := s.openShard(cfg, i)
		if err != nil {
			s.Close()
			return errors.Trace(err)
		}
		s.dbs = append(s.dbs, db)
	}
	return nil
}

// openShard resolves the base DSN (in test mode an unset DSN falls back to SEQUEL_TESTING_DSN, then the SQLite
// in-memory default), substitutes %d with the shard index, and in test mode wraps the result via
// sequel.CreateTestingDatabase before opening and migrating.
func (s *ShardSet) openShard(cfg Config, shardIndex int) (*sequel.DB, error) {
	dsn := cfg.DSN
	if cfg.TestID != "" {
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
	if cfg.TestID != "" {
		var err error
		dsn, err = sequel.CreateTestingDatabase("", dsn, cfg.TestID)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	db, err := openAndMigrate(cfg, dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// openAndMigrate opens a single connection, applies pool sizes and telemetry, and runs schema migrations.
func openAndMigrate(cfg Config, dsn string) (*sequel.DB, error) {
	const driverName = ""
	db, err := sequel.Open(driverName, dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	// Drain idle connections and recycle aged ones - but not on SQLite, whose in-memory test databases are
	// dropped the moment their last connection closes (a closed idle/expired conn would lose the data).
	if db.DriverName() != "sqlite" {
		db.SetConnMaxIdleTime(2 * time.Minute)
		db.SetConnMaxLifetime(1 * time.Hour)
	}
	applyTelemetry(cfg, db)
	err = db.Migrate(sequenceName, migrations.FS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// applyTelemetry points a shard's sequel DB at the configured logger, tracer, and meter providers, defaulting
// a nil tracer/meter provider to the global otel one.
func applyTelemetry(cfg Config, db *sequel.DB) {
	db.SetLogger(cfg.Logger)
	tp := cfg.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	db.SetTracerProvider(tp)
	mp := cfg.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	db.SetMeterProvider(mp)
}

// Shard returns the database connection for the given 1-based shard index.
func (s *ShardSet) Shard(n int) (*sequel.DB, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if n < 1 || n > len(s.dbs) {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	return s.dbs[n-1], nil
}

// NumShards returns the number of open shards.
func (s *ShardSet) NumShards() int {
	s.mu.RLock()
	n := len(s.dbs)
	s.mu.RUnlock()
	return n
}

// OnEach fans op out over every shard concurrently using an errgroup-style wait, returning the first error.
func (s *ShardSet) OnEach(ctx context.Context, op func(ctx context.Context, db *sequel.DB, shard int) error) error {
	numShards := s.NumShards()
	if numShards == 1 {
		db, err := s.Shard(1)
		if err != nil {
			return errors.Trace(err)
		}
		return errors.Trace(op(ctx, db, 1))
	}
	errs := make([]error, numShards+1)
	var wg sync.WaitGroup
	for i := 1; i <= numShards; i++ {
		si := i
		wg.Go(func() {
			db, err := s.Shard(si)
			if err != nil {
				errs[si] = err
				return
			}
			errs[si] = op(ctx, db, si)
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

// SetMaxIdleConns applies the given idle-connection pool size to every open shard.
func (s *ShardSet) SetMaxIdleConns(n int) {
	s.mu.RLock()
	for _, db := range s.dbs {
		db.SetMaxIdleConns(n)
	}
	s.mu.RUnlock()
}

// SetMaxOpenConns applies the given open-connection pool ceiling to every open shard.
func (s *ShardSet) SetMaxOpenConns(n int) {
	s.mu.RLock()
	for _, db := range s.dbs {
		db.SetMaxOpenConns(n)
	}
	s.mu.RUnlock()
}

// Close closes all shard connections and empties the set.
func (s *ShardSet) Close() {
	s.mu.Lock()
	for _, db := range s.dbs {
		db.Close()
	}
	s.dbs = nil
	s.mu.Unlock()
}
