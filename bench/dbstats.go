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

package main

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// dbStatsSampler reads the two DATABASE-side drift signals the process cannot see from inside: the
// server's connection count (does the pool stay bounded, and does the per-server budget split hold as
// replicas come and go) and the on-disk size of the two dwarf tables including their indexes and TOAST
// (does create-purge churn bloat storage even when the row count plateaus - rows can hold steady while
// dead tuples accumulate if autovacuum falls behind). Postgres-only: the catalog queries are dialect-
// specific, so a non-Postgres DSN yields a nil sampler and zero readings. Like the RTT sampler it holds
// one dedicated connection (never competing with the engine's pool) and samples shard 1 only - the
// local-harness shape; a multi-shard cloud run samples per shard.
type dbStatsSampler struct {
	db *sequel.DB
	// syncSQL is the WAL flush-count query that worked on this server, found on first use: the counter is
	// pg_stat_wal.wal_sync through PostgreSQL 17 and pg_stat_io's fsyncs for object 'wal' from 18. "-" = neither.
	// Guarded: the window's readings and the per-interval probe sample from different goroutines.
	syncMu  sync.Mutex
	syncSQL string
}

// dbStats is one database-side reading. Connections counts every backend on the database - including
// this sampler's own and the RTT sampler's, a small constant that cancels in any drift/step comparison.
// WALBytes is the server's cumulative WAL volume (pg_stat_wal, PostgreSQL 14+). It is cluster-wide, so a
// window delta is this run's WAL only on an instance nothing else writes to - the bench rig's shape.
type dbStats struct {
	Connections int   `json:"connections"`
	StepsBytes  int64 `json:"stepsBytes"`
	FlowsBytes  int64 `json:"flowsBytes"`
	WALBytes    int64 `json:"walBytes"`
	// The WAL pressure counters beside it (pg_stat_wal, cluster-wide like WALBytes), and commits across every
	// database so commits per WAL flush can be formed. The database is WAL-write-bound at its ceiling
	// (LWLock:WALWrite dominates the waits), so these are the candidates for "past the sustainable rate".
	WALFPI         int64 `json:"walFpi"`
	WALBuffersFull int64 `json:"walBuffersFull"`
	WALSyncs       int64 `json:"walSyncs"`
	Commits        int64 `json:"commits"`
	// ok is whether every query of this reading succeeded. A failed one reads as zero, and a delta taken
	// against a zero would report cumulative WAL, or negative growth, as one window's.
	ok bool
}

// dbDelta is what the database stored and logged over one measurement window: the growth of the two dwarf
// tables (with their indexes and TOAST) and the WAL written. Against the bytes the tasks declared, these are
// the amplification - stored and logged bytes per task byte.
type dbDelta struct {
	StepsBytes     int64 `json:"stepsBytes"`
	FlowsBytes     int64 `json:"flowsBytes"`
	WALBytes       int64 `json:"walBytes"`
	WALFPI         int64 `json:"walFpi"`
	WALBuffersFull int64 `json:"walBuffersFull"`
	WALSyncs       int64 `json:"walSyncs"`
	Commits        int64 `json:"commits"`
}

// windowDelta is the change between two readings, or nil when there is no sampler (a non-Postgres DSN) or
// either reading failed (including WAL on PostgreSQL before 14, which has no pg_stat_wal).
func (s *dbStatsSampler) windowDelta(before, after dbStats) *dbDelta {
	if s == nil || !before.ok || !after.ok {
		return nil
	}
	return &dbDelta{
		StepsBytes:     after.StepsBytes - before.StepsBytes,
		FlowsBytes:     after.FlowsBytes - before.FlowsBytes,
		WALBytes:       after.WALBytes - before.WALBytes,
		WALFPI:         after.WALFPI - before.WALFPI,
		WALBuffersFull: after.WALBuffersFull - before.WALBuffersFull,
		WALSyncs:       syncDelta(before.WALSyncs, after.WALSyncs),
		Commits:        after.Commits - before.Commits,
	}
}

// startDBStatsSampler opens the sampler for a Postgres DSN, or returns nil (no error) for any other
// dialect - callers treat a nil sampler as "no DB-side readings".
func startDBStatsSampler(dsn string) (*dbStatsSampler, error) {
	if !strings.HasPrefix(dsn, "postgres://") {
		return nil, nil
	}
	db, err := sequel.Open("", dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return &dbStatsSampler{db: db}, nil
}

// sample reads the current connection count and table sizes. Errors yield a zero reading (a transient
// blip must not abort a soak; a fault run deliberately severs this connection too).
func (s *dbStatsSampler) sample(ctx context.Context) dbStats {
	if s == nil {
		return dbStats{}
	}
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var out dbStats
	err1 := s.db.QueryRowContext(sctx,
		"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()",
	).Scan(&out.Connections)
	err2 := s.db.QueryRowContext(sctx,
		"SELECT COALESCE(pg_total_relation_size('dwarf_steps'), 0), COALESCE(pg_total_relation_size('dwarf_flows'), 0)",
	).Scan(&out.StepsBytes, &out.FlowsBytes)
	err3 := s.db.QueryRowContext(sctx,
		"SELECT wal_bytes::bigint, wal_fpi, wal_buffers_full FROM pg_stat_wal",
	).Scan(&out.WALBytes, &out.WALFPI, &out.WALBuffersFull)
	out.WALSyncs = s.walSyncs(sctx)
	err4 := s.db.QueryRowContext(sctx,
		"SELECT COALESCE(SUM(xact_commit), 0)::bigint FROM pg_stat_database",
	).Scan(&out.Commits)
	out.ok = err1 == nil && err2 == nil && err3 == nil && err4 == nil
	return out
}

// syncDelta is the flush count over a window, or -1 when either reading had none.
func syncDelta(before, after int64) int64 {
	if before < 0 || after < 0 {
		return -1
	}
	return after - before
}

// walSyncs reads the server's cumulative WAL flush count, or -1 when this server exposes none. Never fatal to
// the reading: the flush count only feeds commits per flush.
func (s *dbStatsSampler) walSyncs(ctx context.Context) int64 {
	candidates := []string{
		"SELECT wal_sync FROM pg_stat_wal",
		"SELECT COALESCE(SUM(fsyncs), 0)::bigint FROM pg_stat_io WHERE object = 'wal'",
	}
	s.syncMu.Lock()
	defer s.syncMu.Unlock()
	if s.syncSQL == "-" {
		return -1
	}
	if s.syncSQL != "" {
		candidates = []string{s.syncSQL}
	}
	for _, q := range candidates {
		var n int64
		if err := s.db.QueryRowContext(ctx, q).Scan(&n); err == nil {
			s.syncSQL = q
			return n
		}
	}
	if s.syncSQL == "" {
		s.syncSQL = "-"
	}
	return -1
}

func (s *dbStatsSampler) close() {
	if s != nil {
		s.db.Close()
	}
}
