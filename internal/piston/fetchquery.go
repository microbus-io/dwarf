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

package piston

import (
	"encoding/json"
	"strings"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// dueNow is a step that is pending, not parked, past its not_before and holding no live lease. Inlined status,
// never bound - a bound status defeats the selection index's filter on the dialects that have one.
const dueNow = "status='" + workflow.StatusPending + "' AND parked=0" +
	" AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC()"

// dueIndexGuard is the conjunct idx_dwarf_steps_due's partial predicate carries besides the status: always true
// (step ids start at 1), and stated only by a query that may use that index. A database uses a partial index
// only for a query whose own predicate implies the index's, so the lateral fetch, which states dueNow without
// it, cannot be planned onto the due index under any statistics.
//
// That is the point. The lateral's early stop needs idx_dwarf_steps_selection, which seeks (parked, priority,
// fairness_key) and returns each key's run already ordered by (created_at, step_id). The due index seeks
// (parked, priority) too, and when its estimates are small - a fresh database, statistics taken at a small
// backlog - the planner takes it, filters each key row by row and sorts: O(backlog) per key. Measured on a
// rig: one such fetch, a custom plan at 503 keys, ran 1,224 s. Steering the plan (a CASE around a column, a
// plan-cache mode) only removes the cases it names; parked and priority lead both indexes, so wrapping them
// blinds the selection index too.
const dueIndexGuard = " AND step_id>0"

// dueSteps is dueNow for the queries that want the due index: the band scan and its MIN(priority) probe, and
// the window-shaped fetches, whose cost is the band's due rows however they are found.
const dueSteps = dueNow + dueIndexGuard

// fetchQuery builds one dialect's per-key fetch: the statement, and the bind arguments in the order their
// placeholders appear in it. Every branch projects (fairness_key, step_id), in that order, so the caller's
// scan loop is dialect-blind.
//
// THE KEYS TRAVEL AS ONE BIND - a text[] on pgx, a JSON array elsewhere - and that is a correctness bound
// rather than a tuning one. One bind per
// key overruns SQL Server's hard ceiling of 2,100 parameters per statement at ~2,095 keys - and the key
// count is bounded by the candidate cache's capacity, which is twice the worker count, so a 192-connection
// pool plans at 3,072. SQLite's own ceiling (SQLITE_MAX_VARIABLE_NUMBER) is 32,766 on a current build but
// 999 on anything before 3.32. The overrun surfaces as a failed fetch, which the pipeline treats as "push
// nothing, clear nothing" - so the shard goes on publishing an honest band claim while supplying zero
// candidates, every cycle, for as long as the workload's key cardinality stays high. Joining the keys back
// in SQL fixes the bind count at four, or five once the partition predicate carries its own.
//
// THE PER-KEY CAP IS A REAL EARLY STOP on pgx and mssql: a LIMIT (TOP on mssql) inside a lateral join,
// over an index that already orders each key's run by (created_at, step_id), so the scan stops after perKey
// rows per key instead of ranking the key's whole run and cutting afterwards.
//
// MYSQL AND SQLITE KEEP THE RANKING SHAPE, because neither can be given a lateral join - see each branch.
// Their bind count is fixed like everyone else's, which is the bug this closes; their cost stays O(due rows
// at the band), which is the same shape ScanBand has on every dialect, so it is a standing ceiling rather
// than a new one.
//
// MINIMUM SERVER VERSIONS for the json argument: SQL Server 2016, below what the rest of the engine already
// requires. The pgx branch binds an array and the mysql branch uses no JSON function, so neither adds a floor.
//
// Non-UTF-8 bytes in a fairness key would be replaced with U+FFFD by the json encoding and would then match
// nothing. Postgres, SQL Server and MySQL all reject such bytes in the text column the key came out of, so
// the case can only arise on SQLite.
func fetchQuery(driverName string, band int, keys []string, perKey int, part string, partArgs []any) (string, []any, error) {
	keysJSON, err := json.Marshal(keys)
	if err != nil {
		return "", nil, errors.Trace(err)
	}
	args := make([]any, 0, len(partArgs)+3)
	switch driverName {
	case "pgx":
		// A native text[] expanded by unnest, so a custom plan knows how many keys there are. A JSON
		// set-returning function is estimated at 100 rows whatever it holds, which multiplies the fetch's
		// estimated cost by 100/keys: at one key a flipped plan priced ~500k and crossed Postgres's JIT
		// thresholds, so it was compiled with full optimization on every execution. A text literal cast to
		// text[] does not help - the cast is not folded at plan time, so the estimate stays at 100.
		args = append(args, keys, band)
		args = append(args, partArgs...)
		args = append(args, perKey)
		return "SELECT k.fairness_key, s.step_id" +
			" FROM unnest(CAST(? AS TEXT[])) AS k(fairness_key)" +
			" CROSS JOIN LATERAL (" +
			"SELECT step_id FROM dwarf_steps" +
			" WHERE " + dueNow +
			" AND priority=? AND fairness_key=k.fairness_key" +
			part +
			" ORDER BY created_at, step_id LIMIT ?" +
			") AS s", args, nil

	case "mysql":
		// THE ONLY BRANCH THAT STILL BINDS ONE PARAMETER PER KEY, and both halves of that are deliberate.
		//
		// No lateral join: the driver name covers MySQL and MariaDB, and MariaDB has none at any version
		// (10.11 rejects CROSS JOIN LATERAL as a syntax error; MDEV-19078 is open). MySQL 8.0.14 has one, so
		// a lateral would pass CI and fail on half of a supported production dialect - the same ambiguity
		// staterefs names when it says a value keyed on this driver name cannot tell the two engines apart.
		//
		// And NO JSON_TABLE either, which is the subtler trap. MySQL 8 gives the extracted column the
		// SERVER's default collation while dwarf_steps.fairness_key carries the DATABASE's, and comparing
		// the two raises "Error 1267: Illegal mix of collations" - measured on MySQL 8.4 with a
		// utf8mb4_0900_ai_ci server against a utf8mb4_general_ci database. MariaDB cannot reproduce it (it
		// has no utf8mb4_0900 family at all), so the fault passes every MariaDB run and fails on the engine
		// CI uses. A bind parameter has no such problem: its collation is COERCIBLE, so the column's wins,
		// which is why the IN-list is the safe shape here.
		//
		// One bind per key is sound on this dialect specifically. The ceiling is 65,535 placeholders, while
		// the key count is bounded by the candidate cache's capacity - twice the worker count, a few
		// thousand on a large pool - so the margin is better than 20x. The overflow this file exists to fix
		// is SQL Server's hard 2,100, which no other dialect comes near.
		args = append(args, band)
		for _, k := range keys {
			args = append(args, k)
		}
		args = append(args, partArgs...)
		args = append(args, perKey)
		return "SELECT fairness_key, step_id FROM (" +
			"SELECT fairness_key, step_id," +
			" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn" +
			" FROM dwarf_steps" +
			" WHERE " + dueSteps +
			" AND priority=? AND fairness_key IN (" + strings.Repeat("?,", len(keys)-1) + "?)" +
			part +
			") t WHERE rn<=? ORDER BY fairness_key, rn", args, nil

	case "mssql":
		// CROSS APPLY is the lateral join, and TOP takes the cap - so perKey binds BEFORE the band here,
		// where the other dialects bind it last. The order of this slice is the order of the placeholders,
		// never the order of the parameters as a reader might group them.
		args = append(args, string(keysJSON), perKey, band)
		args = append(args, partArgs...)
		return "SELECT k.fairness_key, s.step_id" +
			" FROM OPENJSON(?) WITH (fairness_key NVARCHAR(256) '$') AS k" +
			" CROSS APPLY (" +
			"SELECT TOP (?) step_id FROM dwarf_steps" +
			" WHERE " + dueNow +
			" AND priority=? AND fairness_key=k.fairness_key" +
			part +
			" ORDER BY created_at, step_id" +
			") AS s", args, nil

	default: // sqlite
		// No lateral join to be had, so the window ranks each key's whole run and the cut lands after it.
		// rn IS the oldest-first ordinal, so ORDER BY fairness_key, rn is right by construction.
		args = append(args, band, string(keysJSON))
		args = append(args, partArgs...)
		args = append(args, perKey)
		return "SELECT fairness_key, step_id FROM (" +
			"SELECT fairness_key, step_id," +
			" ROW_NUMBER() OVER (PARTITION BY fairness_key ORDER BY created_at, step_id) AS rn" +
			" FROM dwarf_steps" +
			" WHERE " + dueSteps +
			" AND priority=? AND fairness_key IN (SELECT value FROM json_each(?))" +
			part +
			") t WHERE rn<=? ORDER BY fairness_key, rn", args, nil
	}
}
