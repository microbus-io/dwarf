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
	"sync"

	"golang.org/x/sync/semaphore"
)

// Test-mode connection budgeting. Under `NewEngineUnderTest`, dozens of
// per-test engines can open pools
// against ONE shared backend server concurrently (once fixtures run `t.Parallel()`), and that server's
// `max_connections` is finite. Each ShardSet reserves its WHOLE pool budget from a per-driver global
// semaphore at Open and releases it at Close, so the sum of live test pools never exceeds the cap.
//
// The reservation is one weight per engine, acquired ATOMICALLY (never per connection or per shard): a
// per-connection global limiter would deadlock the moment one engine's multi-shard OnEach needs several
// connections at once while the count is exhausted by peers each holding one, and a per-shard sequential
// acquire has the same partial-hold hazard. One atomic acquire of `cap × shards` cannot partially hold.
//
// Production never touches this: it engages only when `Config.TestID != "" && Config.TestConnCap > 0`, and
// SQLite (no server cap) gets an effectively-unbounded budget so the default in-memory suite never blocks.

// driverConnCap is the per-backend connection budget the whole test binary may consume at once, sized
// under each engine's default max_connections with headroom for monitoring and the server's housekeeping.
// Keyed by sequel's driver name (from db.DriverName()).
func driverConnCap(driver string) int64 {
	switch driver {
	case "pgx":
		return 80 // PostgreSQL default max_connections = 100
	case "mysql":
		return 120 // MySQL / MariaDB default 151
	case "mssql":
		return 4000 // SQL Server default ~32767
	default:
		return 1 << 20 // sqlite (no server cap) and unknown drivers: effectively unbounded
	}
}

var (
	testBudgetMu sync.Mutex
	testBudgets  = map[string]*semaphore.Weighted{} // one weighted semaphore per driver, lazily created
)

// acquireTestBudget reserves n connections' worth of the driver's global budget, blocking until it is
// available, and returns an idempotent release. n is clamped to the driver cap so a single engine larger
// than the whole server can never block forever (it just serializes against everything else).
func acquireTestBudget(driver string, n int64) func() {
	capacity := driverConnCap(driver)
	testBudgetMu.Lock()
	sem := testBudgets[driver]
	if sem == nil {
		sem = semaphore.NewWeighted(capacity)
		testBudgets[driver] = sem
	}
	testBudgetMu.Unlock()
	return acquireFrom(sem, capacity, n)
}

// acquireFrom is the driver-agnostic core, split out so the block/clamp/release-once behavior is testable
// against a private semaphore rather than the process-global per-driver map.
func acquireFrom(sem *semaphore.Weighted, capacity, n int64) func() {
	if n > capacity {
		n = capacity
	}
	if n < 1 {
		n = 1
	}
	// context.Background(): the wait is for a slot to free as other tests finish, not a cancellable
	// operation. Acquire only errors on a cancelled context, so with Background it cannot fail.
	_ = sem.Acquire(context.Background(), n)
	var once sync.Once
	return func() { once.Do(func() { sem.Release(n) }) }
}
