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
	"math"
	"time"

	"github.com/microbus-io/sequel"
)

// probeRTT measures the round-trip time to a shard with a few `SELECT 1`s, returning the MINIMUM of the samples: the
// minimum approximates pure network RTT, where a mean is polluted by scheduler jitter and warmup. The first sample is
// discarded - it pays connection establishment. Measured once when the shard opens, on connections the pool is
// opening anyway (~10ms), never adapted: a latency change is measured again on the next Open, the same posture as the
// shard set itself. A failed probe returns 0, for the caller to fall back on rather than mis-size on a transient
// error.
func probeRTT(ctx context.Context, db *sequel.DB) time.Duration {
	const samples = 6
	best := time.Duration(math.MaxInt64)
	for i := range samples {
		start := time.Now()
		var one int
		err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
		elapsed := time.Since(start)
		if err != nil {
			return 0
		}
		if i == 0 {
			continue // discard: pays connection establishment
		}
		best = min(best, elapsed)
	}
	if best == time.Duration(math.MaxInt64) {
		return 0
	}
	return best
}

// readMaxConnections returns the server's connection limit for one shard, or 0 when it cannot say - SQLite has none,
// and a failed read is treated the same. SQL Server reports 32767 unless "user connections" is configured, and Azure
// SQL always does. Read once when the shard opens: Postgres and SQL Server change the limit only on a server restart,
// and a managed service's resize restarts or fails the server over.
func readMaxConnections(ctx context.Context, db *sequel.DB) int {
	var q string
	switch db.DriverName() {
	case "pgx":
		q = "SELECT CAST(setting AS INTEGER) FROM pg_settings WHERE name='max_connections'"
	case "mysql", "mariadb":
		q = "SELECT @@max_connections"
	case "mssql":
		q = "SELECT @@MAX_CONNECTIONS"
	default:
		return 0
	}
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0
	}
	return max(0, n)
}
