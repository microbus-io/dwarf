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
	"net/http"
	"os"
	"slices"
	"strconv"
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

// ShardConfig is one shard's open-time configuration: its DSN and its resolved pool sizes. The
// connection-sizing formula is the caller's concern; this package applies the two integers verbatim.
type ShardConfig struct {
	// DSN is the sequel data source name, used EXACTLY as given: it is never formatted or substituted, so
	// a percent-encoded credential survives it intact. In test mode (Config.TestID set) it is a template -
	// an empty DSN falls back to SEQUEL_TESTING_DSN and then the SQLite in-memory default, and a "%d" is
	// replaced with the shard index, which is what makes each shard's test database distinct.
	DSN          string
	MaxIdleConns int
	MaxOpenConns int
}

// Config is the full open-time configuration of a ShardSet.
type Config struct {
	// Shards maps each shard index to its configuration. Indices must be >= 1 but need not be
	// contiguous (index 0 is the "no shard / all shards" sentinel). A nil/empty map defaults to a
	// single shard 1 with an empty DSN and a minimal pool.
	Shards map[int]ShardConfig
	// TestID, when non-empty, wraps each shard via sequel.CreateTestingDatabase into an isolated, auto-dropped
	// database keyed on (driver, baseDSN, TestID) - the resolved per-shard DSNs already distinguish the shards.
	TestID string

	Logger         *slog.Logger
	TracerProvider trace.TracerProvider // nil → otel.GetTracerProvider()
	MeterProvider  metric.MeterProvider // nil → otel.GetMeterProvider()
}

// ShardSet is the engine's set of open, migrated database shards. The zero value is a usable, empty set. The
// shard indices are fixed for the set's life (established at Open), so a caller may size per-shard state from
// Indices()/NumShards() and key it by the shard arg without racing a concurrent change. Indices are sparse:
// they must be unique and >= 1 but need not be contiguous.
type ShardSet struct {
	mu      sync.RWMutex
	dbs     map[int]*sequel.DB
	indices []int // sorted ascending
}

// Open opens and migrates every shard, applying cfg's pool sizes and telemetry. On any shard failure it closes
// the shards already opened this attempt (so a partial failure leaks no connections and leaves the set empty
// for a clean retry) and returns the error. Not safe to call concurrently with itself; call once at startup.
func (s *ShardSet) Open(ctx context.Context, cfg Config) error {
	shards := cfg.Shards
	if len(shards) == 0 {
		shards = map[int]ShardConfig{1: {MaxIdleConns: 2, MaxOpenConns: 8}}
	}
	indices := make([]int, 0, len(shards))
	for idx := range shards {
		if idx < 1 {
			return errors.New("shard index must be at least 1")
		}
		indices = append(indices, idx)
	}
	slices.Sort(indices)
	dbs := make(map[int]*sequel.DB, len(indices))
	closeAll := func() {
		for _, db := range dbs {
			db.Close()
		}
	}
	seen := make(map[string]int, len(indices))
	for _, idx := range indices {
		dsn := resolveShardDSN(cfg, idx, shards[idx].DSN)
		// Two shards resolving to the same database would collapse onto one another - distinct flows would
		// share flow_id sequences and every cross-shard invariant breaks. Loud error, never silent.
		if prev, ok := seen[dsn]; ok {
			closeAll()
			return errors.New("shards %d and %d resolve to the same DSN", prev, idx)
		}
		seen[dsn] = idx
		db, err := openShard(cfg, dsn, shards[idx])
		if err != nil {
			closeAll()
			return errors.Trace(err)
		}
		dbs[idx] = db
	}
	s.mu.Lock()
	s.dbs = dbs
	s.indices = indices
	s.mu.Unlock()
	return nil
}

// resolveShardDSN resolves one shard's DSN.
//
// In PRODUCTION the operator's DSN is used exactly as given - no formatting, no substitution. A DSN is a
// credential-bearing string and percent-encoding in it is routine (`p%40ss` for a password `p@ss`), so
// running it through fmt.Sprintf - which interprets EVERY `%` as a verb, not just `%d` - silently
// corrupted it: `%40s` parsed as "width 40, verb s", swallowed the shard index there, and left the real
// `%d` with no argument (`%!d(MISSING)`). The driver then failed with an opaque parse error naming
// neither the shard nor the cause. Shards are declared one by one with their own DSN, so a template was
// never needed here.
//
// In TEST MODE the DSN is a template by design - one base is shared across shards and `%d` is what makes
// each shard's database distinct (the per-shard isolation the fixtures rely on).
// The substitution is a literal ReplaceAll, so it still cannot interpret any other `%` sequence.
func resolveShardDSN(cfg Config, shardIndex int, dsn string) string {
	if cfg.TestID == "" {
		return dsn
	}
	if dsn == "" {
		dsn = os.Getenv("SEQUEL_TESTING_DSN")
		if dsn == "" {
			dsn = "file:dwarf_%d?mode=memory&cache=shared"
		}
	}
	return strings.ReplaceAll(dsn, "%d", strconv.Itoa(shardIndex))
}

// openShard opens and migrates one shard from its resolved DSN, in test mode first wrapping it via
// sequel.CreateTestingDatabase into an isolated, auto-dropped database.
func openShard(cfg Config, dsn string, sc ShardConfig) (*sequel.DB, error) {
	if cfg.TestID != "" {
		var err error
		dsn, err = sequel.CreateTestingDatabase("", dsn, cfg.TestID)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	db, err := openAndMigrate(cfg, dsn, sc)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return db, nil
}

// openAndMigrate opens a single connection, applies pool sizes and telemetry, and runs schema migrations.
func openAndMigrate(cfg Config, dsn string, sc ShardConfig) (*sequel.DB, error) {
	const driverName = ""
	db, err := sequel.Open(driverName, dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db.SetMaxIdleConns(sc.MaxIdleConns)
	db.SetMaxOpenConns(sc.MaxOpenConns)
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

// Shard returns the database connection for the given shard index. An unknown index returns a uniform
// not-found error: a flow key referencing an unregistered shard must be indistinguishable from a bad key.
func (s *ShardSet) Shard(n int) (*sequel.DB, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	db, ok := s.dbs[n]
	if !ok {
		return nil, errors.New("flow not found", http.StatusNotFound)
	}
	return db, nil
}

// Has reports whether the given shard index is registered.
func (s *ShardSet) Has(n int) bool {
	s.mu.RLock()
	_, ok := s.dbs[n]
	s.mu.RUnlock()
	return ok
}

// NumShards returns the number of open shards.
func (s *ShardSet) NumShards() int {
	s.mu.RLock()
	n := len(s.dbs)
	s.mu.RUnlock()
	return n
}

// Indices returns the open shard indices in ascending order.
func (s *ShardSet) Indices() []int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return slices.Clone(s.indices)
}

// OnEach fans op out over every shard concurrently using an errgroup-style wait. Every shard is always
// attempted; if several fail, the error of the LOWEST shard index is the one returned (deterministic, not
// whichever failed first - the ops run concurrently, so "first" would be a race).
//
// The returned error carries a "shard" property naming the failing shard. Without it an operator sees the
// reaper or the refiller fail with a bare driver error and no clue WHICH database is unhealthy - and this
// package's whole contract is that a persistent outage degrades loudly. The property is a NAMED pair
// deliberately: an unnamed int in this errors package is read as an HTTP status code, so passing the index
// bare would silently set the error's status.
func (s *ShardSet) OnEach(ctx context.Context, op func(ctx context.Context, db *sequel.DB, shard int) error) error {
	indices := s.Indices()
	if len(indices) == 1 {
		db, err := s.Shard(indices[0])
		if err != nil {
			return errors.Trace(err, "shard", indices[0])
		}
		return errors.Trace(op(ctx, db, indices[0]), "shard", indices[0])
	}
	errs := make([]error, len(indices))
	var wg sync.WaitGroup
	for i, idx := range indices {
		wg.Go(func() {
			db, err := s.Shard(idx)
			if err != nil {
				errs[i] = err
				return
			}
			errs[i] = op(ctx, db, idx)
		})
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			return errors.Trace(err, "shard", indices[i])
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
	s.indices = nil
	s.mu.Unlock()
}
