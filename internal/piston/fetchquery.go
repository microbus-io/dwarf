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
	"strconv"
	"strings"

	"github.com/microbus-io/dwarf/internal/planner"
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

// dueSteps is dueNow for the queries that want the due index: the band scan's counts and tally, and
// the window-shaped fetches, whose cost is the band's due rows however they are found.
const dueSteps = dueNow + dueIndexGuard

// fetchQuery builds one dialect's fetch over a plan's (band, key) pairs: the statement, and the bind arguments in
// the order their placeholders appear in it. Every branch projects (priority, fairness_key, step_id), in that
// order, so the caller's scan loop is dialect-blind. One statement serves every band the plan reached, so a plan
// spanning several bands costs one round trip, not one per band.
//
// THE PAIRS TRAVEL AS ONE OR TWO BINDS - two arrays on pgx, a JSON array of [band, key] pairs on mssql and sqlite -
// and that is a correctness bound rather than a tuning one. One bind per key overruns SQL Server's hard ceiling of
// 2,100 parameters per statement at ~2,095 keys - and the key count is bounded by the candidate cache's capacity,
// which is twice the worker count, so a 192-connection pool plans at 3,072. SQLite's own ceiling
// (SQLITE_MAX_VARIABLE_NUMBER) is 32,766 on a current build but 999 on anything before 3.32. The overrun surfaces
// as a failed fetch, which the pipeline treats as "push nothing, clear nothing" - so the shard goes on publishing
// an honest band claim while supplying zero candidates, every cycle, for as long as the workload's key cardinality
// stays high. Joining the pairs back in SQL fixes the bind count.
//
// THE PER-KEY CAP IS A REAL EARLY STOP on pgx and mssql: a LIMIT (TOP on mssql) inside a lateral join, over an
// index that already orders each (band, key) run by (created_at, step_id), so the scan stops after perKey rows per
// pair instead of ranking the pair's whole run and cutting afterwards.
//
// MYSQL AND SQLITE KEEP THE RANKING SHAPE, because neither can be given a lateral join - see each branch. Their
// cost stays O(due rows at the planned bands' keys), the same shape ScanBand has everywhere, so it is a standing
// ceiling rather than a new one.
//
// MINIMUM SERVER VERSIONS for the json argument: SQL Server 2016, below what the rest of the engine already
// requires; SQLite 3.15 for the row-value IN. The pgx branch binds arrays and the mysql branch uses no JSON
// function, so neither adds a floor.
//
// Non-UTF-8 bytes in a fairness key would be replaced with U+FFFD by the json encoding and would then match
// nothing. Postgres, SQL Server and MySQL all reject such bytes in the text column the key came out of, so the
// case can only arise on SQLite.
func fetchQuery(driverName string, pairs []planner.Slot, perKey int, part string, partArgs []any) (string, []any, error) {
	args := make([]any, 0, len(partArgs)+3)
	switch driverName {
	case "pgx":
		// Native arrays expanded together by unnest, so a custom plan knows how many pairs there are. A JSON
		// set-returning function is estimated at 100 rows whatever it holds, which multiplies the fetch's
		// estimated cost by 100/pairs: at one key a flipped plan priced ~500k and crossed Postgres's JIT
		// thresholds, so it was compiled with full optimization on every execution. A text literal cast to an
		// array does not help - the cast is not folded at plan time, so the estimate stays at 100.
		bands := make([]int32, len(pairs))
		keys := make([]string, len(pairs))
		for i, pr := range pairs {
			bands[i], keys[i] = int32(pr.Band), pr.Key
		}
		args = append(args, bands, keys)
		args = append(args, partArgs...)
		args = append(args, perKey)
		return "SELECT k.priority, k.fairness_key, s.step_id" +
			" FROM unnest(CAST(? AS INT[]), CAST(? AS TEXT[])) AS k(priority, fairness_key)" +
			" CROSS JOIN LATERAL (" +
			"SELECT step_id FROM dwarf_steps" +
			" WHERE " + dueNow +
			" AND priority=k.priority AND fairness_key=k.fairness_key" +
			part +
			" ORDER BY created_at, step_id LIMIT ?" +
			") AS s", args, nil

	case "mysql":
		// THE ONLY BRANCH THAT STILL BINDS PER PAIR (two each), and both halves of that are deliberate.
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
		// which is why the row-value IN-list is the safe shape here.
		//
		// Two binds per pair is sound on this dialect specifically. The ceiling is 65,535 placeholders, while
		// the pair count is bounded by the candidate cache's capacity - twice the worker count, a few thousand
		// on a large pool - so the margin is better than 10x. The overflow this file exists to fix is SQL
		// Server's hard 2,100, which no other dialect comes near.
		for _, pr := range pairs {
			args = append(args, pr.Band, pr.Key)
		}
		args = append(args, partArgs...)
		args = append(args, perKey)
		return "SELECT priority, fairness_key, step_id FROM (" +
			"SELECT priority, fairness_key, step_id," +
			" ROW_NUMBER() OVER (PARTITION BY priority, fairness_key ORDER BY created_at, step_id) AS rn" +
			" FROM dwarf_steps" +
			" WHERE " + dueSteps +
			bandsIn(pairs) +
			" AND (priority, fairness_key) IN (" + strings.Repeat("(?,?),", len(pairs)-1) + "(?,?))" +
			part +
			") t WHERE rn<=? ORDER BY priority, fairness_key, rn", args, nil

	case "mssql":
		// CROSS APPLY is the lateral join, and TOP takes the cap - so perKey binds BEFORE the partition here,
		// where the other dialects bind it last. The order of this slice is the order of the placeholders,
		// never the order of the parameters as a reader might group them.
		pairsJSON, err := pairsJSON(pairs)
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		args = append(args, pairsJSON, perKey)
		args = append(args, partArgs...)
		return "SELECT k.priority, k.fairness_key, s.step_id" +
			" FROM OPENJSON(?) WITH (priority INT '$[0]', fairness_key NVARCHAR(256) '$[1]') AS k" +
			" CROSS APPLY (" +
			"SELECT TOP (?) step_id FROM dwarf_steps" +
			" WHERE " + dueNow +
			" AND priority=k.priority AND fairness_key=k.fairness_key" +
			part +
			" ORDER BY created_at, step_id" +
			") AS s", args, nil

	default: // sqlite
		// No lateral join to be had, so the window ranks each pair's whole run and the cut lands after it.
		// rn IS the oldest-first ordinal, so ORDER BY priority, fairness_key, rn is right by construction.
		pairsJSON, err := pairsJSON(pairs)
		if err != nil {
			return "", nil, errors.Trace(err)
		}
		args = append(args, pairsJSON)
		args = append(args, partArgs...)
		args = append(args, perKey)
		return "SELECT priority, fairness_key, step_id FROM (" +
			"SELECT priority, fairness_key, step_id," +
			" ROW_NUMBER() OVER (PARTITION BY priority, fairness_key ORDER BY created_at, step_id) AS rn" +
			" FROM dwarf_steps" +
			" WHERE " + dueSteps +
			bandsIn(pairs) +
			" AND (priority, fairness_key) IN (SELECT json_extract(value, '$[0]'), json_extract(value, '$[1]') FROM json_each(?))" +
			part +
			") t WHERE rn<=? ORDER BY priority, fairness_key, rn", args, nil
	}
}

// bandsIn is the pairs' distinct bands as an inlined IN-list of integer literals, for the ranking-shape fetches.
// It gives the planner an equality on priority to seek the index by: a row-value IN alone does not, and SQLite
// with ANALYZE statistics then planned the fetch as a rowid range over the whole table off the step_id>0 guard.
func bandsIn(pairs []planner.Slot) string {
	seen := map[int]bool{}
	var list []string
	for _, pr := range pairs {
		if !seen[pr.Band] {
			seen[pr.Band] = true
			list = append(list, strconv.Itoa(pr.Band))
		}
	}
	return " AND priority IN (" + strings.Join(list, ",") + ")"
}

// pairsJSON encodes the pairs as a JSON array of [band, key] arrays, the one bind mssql and sqlite take them as.
func pairsJSON(pairs []planner.Slot) (string, error) {
	out := make([][2]any, len(pairs))
	for i, pr := range pairs {
		out[i] = [2]any{pr.Band, pr.Key}
	}
	b, err := json.Marshal(out)
	if err != nil {
		return "", errors.Trace(err)
	}
	return string(b), nil
}
