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
}

// dbStats is one database-side reading. Connections counts every backend on the database - including
// this sampler's own and the RTT sampler's, a small constant that cancels in any drift/step comparison.
type dbStats struct {
	Connections int   `json:"connections"`
	StepsBytes  int64 `json:"stepsBytes"`
	FlowsBytes  int64 `json:"flowsBytes"`
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
	_ = s.db.QueryRowContext(sctx,
		"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()",
	).Scan(&out.Connections)
	_ = s.db.QueryRowContext(sctx,
		"SELECT COALESCE(pg_total_relation_size('dwarf_steps'), 0), COALESCE(pg_total_relation_size('dwarf_flows'), 0)",
	).Scan(&out.StepsBytes, &out.FlowsBytes)
	return out
}

func (s *dbStatsSampler) close() {
	if s != nil {
		s.db.Close()
	}
}
