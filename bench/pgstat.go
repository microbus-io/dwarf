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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/microbus-io/sequel"
)

// pgssSampler reads SERVER-side per-statement execution totals from pg_stat_statements. It exists to
// split a client-observed query duration into its server and non-server parts: the engine's own
// instruments (and sequel's) time the whole QueryContext, which lumps connection-pool wait, wire time
// and server execution together, so a slow client-side reading cannot say WHERE the time went.
// total_exec_time is measured inside the server and excludes all of that - the difference between the
// two clocks is the attribution.
//
// Postgres-only, and only when pg_stat_statements is preloaded (Cloud SQL preloads it by default; a
// local server needs shared_preload_libraries=pg_stat_statements). Anything else yields a nil sampler,
// which every method tolerates. Like the other samplers it holds one dedicated connection and never
// competes with the engine's pool.
type pgssSampler struct {
	db *sequel.DB
}

// pgssRow is one statement's totals - a snapshot row, or a window delta of two snapshots. Times are
// milliseconds, as pg_stat_statements reports them. PlanMs stays zero unless the server runs with
// pg_stat_statements.track_planning=on.
type pgssRow struct {
	QueryID string  `json:"queryId"`
	Query   string  `json:"query"`
	Calls   int64   `json:"calls"`
	ExecMs  float64 `json:"execMs"`
	PlanMs  float64 `json:"planMs"`
	Rows    int64   `json:"rows"`
	// MeanMs = ExecMs/Calls, precomputed on the window delta so artifacts are readable raw.
	MeanMs float64 `json:"meanMs"`
}

// startPgssSampler opens the sampler for a Postgres DSN, or returns nil for any other dialect or when
// the extension cannot be created (not preloaded, or no privilege). A nil return is deliberate and
// non-fatal: the run proceeds, it just records no server-side statement timings.
func startPgssSampler(dsn string) *pgssSampler {
	if !strings.HasPrefix(dsn, "postgres://") {
		return nil
	}
	db, err := sequel.Open("", dsn)
	if err != nil {
		return nil
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = db.ExecContext(ctx, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
	if err != nil {
		fmt.Printf("pg_stat_statements unavailable (server-side timings not recorded): %v\n", err)
		db.Close()
		return nil
	}
	return &pgssSampler{db: db}
}

// snapshot reads the cumulative totals for every statement recorded against the CURRENT database (the
// view is instance-wide; the dbid filter drops other databases' noise, e.g. a sibling arm's leftovers on
// a shared instance). Keyed by queryid. Errors yield an empty map - a window whose before or after
// snapshot failed simply records no statements, it does not abort the run.
func (s *pgssSampler) snapshot(ctx context.Context) map[string]pgssRow {
	if s == nil {
		return nil
	}
	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	rows, err := s.db.QueryContext(sctx,
		"SELECT queryid::text, LEFT(query, 300), calls, total_exec_time, total_plan_time, rows"+
			" FROM pg_stat_statements"+
			" WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())")
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := map[string]pgssRow{}
	for rows.Next() {
		var r pgssRow
		if err := rows.Scan(&r.QueryID, &r.Query, &r.Calls, &r.ExecMs, &r.PlanMs, &r.Rows); err != nil {
			return nil
		}
		out[r.QueryID] = r
	}
	return out
}

// pgssDelta subtracts two snapshots and returns the statements that actually ran in the window, largest
// server-side execution total first, capped to the top 25 (the tail is idle-session and catalog noise).
// A statement absent from `before` (first seen mid-run) deltas against zero, which is exact: the
// snapshot totals are cumulative from statement birth.
func pgssDelta(before, after map[string]pgssRow) []pgssRow {
	var out []pgssRow
	for id, a := range after {
		b := before[id]
		d := pgssRow{
			QueryID: id,
			Query:   a.Query,
			Calls:   a.Calls - b.Calls,
			ExecMs:  a.ExecMs - b.ExecMs,
			PlanMs:  a.PlanMs - b.PlanMs,
			Rows:    a.Rows - b.Rows,
		}
		if d.Calls <= 0 {
			continue
		}
		d.MeanMs = d.ExecMs / float64(d.Calls)
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ExecMs > out[j].ExecMs })
	if len(out) > 25 {
		out = out[:25]
	}
	return out
}

func (s *pgssSampler) close() {
	if s != nil {
		s.db.Close()
	}
}
