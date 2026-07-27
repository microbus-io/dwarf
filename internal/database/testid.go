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
	"crypto/sha256"
	"encoding/hex"
	"math/rand/v2"
	"strconv"
)

// testRunNonce makes one process's testing databases distinct from every other process's, and it is the
// whole reason TestID exists rather than a bare hash of the test name.
//
// The name sequel derives carries the hour and this key, and nothing else that varies: two `go test` runs
// of the SAME package in the same hour therefore land on the same database names. That is not theoretical
// - the second run's CreateTestingDatabase drops and recreates a database the first run is still using,
// and both then fail in ways that read as engine bugs (measured: `failed to create database
// testing_01_dwarftest_1_...: duplicate key value violates unique constraint "pg_database_datname_index"`,
// and a soak fixture failing on rows that had been deleted underneath it). It costs nothing to prevent and
// is expensive to diagnose, because neither failure names the real cause.
//
// PER PROCESS, not per call or per test: several engines in ONE test must resolve to the SAME databases -
// that is how a multi-replica fixture gives its peer engines shared state - and a test that wants separate
// ones already says so by passing a distinct name. It follows that this does NOT distinguish the
// iterations of `go test -count=N`, which run in one process; nothing needs it to. Sequel reference-counts
// the handles on a testing database and evicts its cached DSN when the last one closes, so each iteration
// re-mints the database it dropped, and the iterations are sequential anyway (the parent of a count round
// blocks on its parallel children before the next round starts).
//
// Base 36 to stay short and inside the identifier-charset scrub sequel applies to the database name.
var testRunNonce = strconv.FormatUint(rand.Uint64(), 36)

// TestID hashes a caller-chosen test name into the key [Config.TestID] takes: bounded to 16 hex chars so
// the database name sequel derives from it stays inside the strictest SQL identifier limit (Postgres 63 /
// MySQL 64) whatever the name's length, and salted with this process's run nonce so concurrent runs of the
// same package do not fight over one database.
//
// The same name yields the same id for the life of the process and a different one in the next, which is
// the property every caller wants: shared within a test, isolated between runs.
func TestID(name string) string {
	sum := sha256.Sum256([]byte(name + "|" + testRunNonce))
	return hex.EncodeToString(sum[:])[:16]
}
