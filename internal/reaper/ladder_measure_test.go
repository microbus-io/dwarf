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

package reaper

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/migrations"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
)

// TestMeasure_Ladder times the reaper's transactions against the size of what they take, on PostgreSQL, for choosing
// the transaction budget: for each tree shape, payload and size, the atomic move of the tree in one transaction, and
// the same tree staged - copied chunk by chunk, then its live copy deleted chunk by chunk - at the configured budget.
// Beside each, a probe committing a one-row UPDATE every 5 ms reads how much the reaper's transactions slow other
// commits. Opt-in (DWARF_MEASURE_LADDER=1), a measurement rather than a check; run it against the database wanted
// (SEQUEL_TESTING_DSN), quiet or beside a saturated bench on another database of the same server. Settings, each
// optional:
//
//	DWARF_LADDER_SHAPES   narrow,wide          narrow: one flow of steps; wide: children of 10 steps each
//	DWARF_LADDER_PAYLOADS 200,8192,65536,1048576  bytes of incompressible state a step
//	DWARF_LADDER_ROWS     64,256,1024,4096,16384,65536,262144  rows a tree, flows and steps
//	DWARF_LADDER_MAX_MB   256                  skip a tree whose payload passes this
//	DWARF_LADDER_BATCH    4096,16777216        the staged path's budget: rows,bytes
//	DWARF_LADDER_STAGED   1                    0 times the atomic move only
//	DWARF_LADDER_OUT      <path>               append a CSV line a measurement
func TestMeasure_Ladder(t *testing.T) {
	if os.Getenv("DWARF_MEASURE_LADDER") == "" {
		t.Skip("set DWARF_MEASURE_LADDER=1 to measure")
	}
	ctx := t.Context()
	r := newRig(t, 30)
	if r.db.DriverName() != "pgx" {
		t.Skip("the ladder seeds with generate_series: PostgreSQL only")
	}
	shapes := envList(t, "DWARF_LADDER_SHAPES", "narrow,wide")
	payloads := envInts(t, "DWARF_LADDER_PAYLOADS", "200,8192,65536,1048576")
	sizes := envInts(t, "DWARF_LADDER_ROWS", "64,256,1024,4096,16384,65536,262144")
	maxBytes := envInts(t, "DWARF_LADDER_MAX_MB", "256")[0] << 20
	batch := envInts(t, "DWARF_LADDER_BATCH", "4096,16777216")
	staged := os.Getenv("DWARF_LADDER_STAGED") != "0"
	r.cfg.BatchRows, r.cfg.BatchBytes, r.cfg.StageRows = batch[0], batch[1], 1
	budget := r.p().batchLimit()
	today, err := r.archive.Today(ctx)
	if err != nil {
		t.Fatal(err)
	}

	probe := startProbe(t, r.db)
	time.Sleep(2 * time.Second)
	idle := probe.over(time.Now().Add(-2*time.Second), time.Now())
	t.Logf("probe idle: p50 %v p99 %v max %v (%d commits)", idle.p50, idle.p99, idle.max, idle.n)
	out := os.Getenv("DWARF_LADDER_OUT")

	report := func(shape string, payload int, sz size, op string, txns []time.Duration, from, to time.Time) {
		p := probe.over(from, to)
		slowest := slices.Max(txns)
		var total time.Duration
		for _, d := range txns {
			total += d
		}
		t.Logf("%-6s payload %7d rows %8d MB %8.1f | %-7s %4d txns, slowest %9v, total %9v | probe p50 %v p99 %v max %v",
			shape, payload, sz.rows, float64(sz.bytes)/(1<<20), op, len(txns), slowest.Round(time.Microsecond),
			total.Round(time.Microsecond), p.p50, p.p99, p.max)
		if out == "" {
			return
		}
		f, err := os.OpenFile(out, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		fmt.Fprintf(f, "%s,%s,%d,%d,%d,%s,%d,%d,%d,%d,%d,%d,%d,%d,%d\n", time.Now().UTC().Format(time.RFC3339), shape, payload,
			sz.rows, sz.bytes, op, len(txns), slowest.Microseconds(), total.Microseconds(), p.p50.Microseconds(),
			p.p99.Microseconds(), p.max.Microseconds(), from.UnixMilli(), to.UnixMilli(), budget.rows)
	}

	for _, shape := range shapes {
		for _, payload := range payloads {
			for _, rows := range sizes {
				if rows*payload > maxBytes {
					continue
				}
				// The atomic move: one transaction, however large.
				root, sz := seedLadderTree(t, r, shape, rows, payload)
				start := time.Now()
				if _, err := r.reaper.MoveTrees(ctx, r.db, r.archive, today, []int{root}); err != nil {
					t.Fatal(err)
				}
				end := time.Now()
				report(shape, payload, sz, "atomic", []time.Duration{end.Sub(start)}, start, end)
				clearLadder(t, r, today)
				if !staged {
					continue
				}

				// The same tree staged: the copy in chunks, published, then the live copy deleted in chunks.
				root, sz = seedLadderTree(t, r, shape, rows, payload)
				d := &filingDay{}
				if err := r.p().refreshDay(ctx, d); err != nil {
					t.Fatal(err)
				}
				at, ok, err := r.p().claimAttempt(ctx, closedDue(0, 0), root, d, time.Time{})
				if err != nil || !ok {
					t.Fatalf("claim: %v %v", ok, err)
				}
				pos := stagePos{at: at}
				var copies []time.Duration
				start = time.Now()
				for {
					s := time.Now()
					n, _, done, err := r.p().stageChunk(ctx, root, r.archive, &pos)
					if err != nil {
						t.Fatal(err)
					}
					copies = append(copies, time.Since(s))
					if n > 0 || done {
						break
					}
				}
				end = time.Now()
				report(shape, payload, sz, "copy", copies, start, end)
				var cleanups []time.Duration
				var cpos chunkPos
				cleanStart := time.Now()
				for {
					s := time.Now()
					_, done, err := r.reaper.chunk(ctx, r.db, liveTarget, root, budget, &cpos)
					if err != nil {
						t.Fatal(err)
					}
					cleanups = append(cleanups, time.Since(s))
					if done {
						break
					}
				}
				cleanEnd := time.Now()
				report(shape, payload, sz, "cleanup", cleanups, cleanStart, cleanEnd)
				report(shape, payload, sz, "staged", append(copies, cleanups...), start, cleanEnd)
				clearLadder(t, r, today)
			}
		}
	}
}

// seedLadderTree inserts a closed tree of about rows rows, flows and steps, every step holding payload bytes of
// incompressible state, and returns its root and its size. narrow is one flow of steps; wide is a root of no steps
// over children of 10 steps each. One random payload is repeated: PostgreSQL compresses each value apart, so the
// repetition saves nothing. Steps go in by generate_series, a statement of at most 64 MB of payload at a time.
func seedLadderTree(t *testing.T, r *rig, shape string, rows, payload int) (root int, sz size) {
	t.Helper()
	ctx := t.Context()
	raw := make([]byte, max(payload*3/4, 1))
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	state := `{"pad":"` + base64.StdEncoding.EncodeToString(raw) + `"}`
	const token = "0123456789abcdef"
	id, err := r.db.InsertReturnID(ctx, "flow_id",
		"INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph, graph_hash, status, surgraph_flow_id,"+
			" surgraph_step_id, root_flow_id) VALUES (?, 'seed/ladder', 'Ladder', ?, '', '"+workflow.StatusCompleted+"', 0, 0, 0)",
		token, []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	root = int(id)
	if _, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET root_flow_id=flow_id WHERE flow_id=?", root); err != nil {
		t.Fatal(err)
	}
	per := max(1, (64<<20)/max(len(state), 1))
	insertSteps := func(flows string, n int) {
		for from := 1; from <= n; from += per {
			to := min(n, from+per-1)
			_, err := r.db.ExecContext(ctx, "INSERT INTO dwarf_steps (flow_id, step_depth, step_token, task_name, task_url,"+
				" status, time_budget_ms, flow_created_hour, state, state_bytes)"+
				" SELECT f.flow_id, g, '"+token+"', 'T', 'seed/t', '"+workflow.StatusCompleted+"', 60000, f.created_hour,"+
				" CAST(? AS JSONB), ? FROM dwarf_flows f CROSS JOIN generate_series("+strconv.Itoa(from)+", "+strconv.Itoa(to)+") g"+
				" WHERE "+flows, state, len(state))
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	switch shape {
	case "narrow":
		insertSteps("f.flow_id="+strconv.Itoa(root), rows-1)
	case "wide":
		children := max((rows-1)/11, 1)
		if _, err := r.db.ExecContext(ctx, "INSERT INTO dwarf_flows (flow_token, workflow_url, workflow_name, graph,"+
			" graph_hash, status, surgraph_flow_id, surgraph_step_id, root_flow_id) SELECT '"+token+"', 'seed/ladder',"+
			" 'Ladder', CAST('{}' AS JSONB), '', '"+workflow.StatusCompleted+"', ?, 0, ? FROM generate_series(1, "+strconv.Itoa(children)+")",
			root, root); err != nil {
			t.Fatal(err)
		}
		insertSteps("f.root_flow_id="+strconv.Itoa(root)+" AND f.flow_id<>"+strconv.Itoa(root), 10)
	default:
		t.Fatalf("shape %q: narrow or wide", shape)
	}
	if _, err := r.db.ExecContext(ctx, "ANALYZE dwarf_steps"); err != nil {
		t.Fatal(err)
	}
	got, err := treesSizesUpTo(ctx, r.db, live, []int{root}, []int{1 << 30})
	if err != nil {
		t.Fatal(err)
	}
	return root, got[0]
}

// clearLadder empties the live and the archive day's tables between measurements, so each tree is measured on tables
// holding only itself.
func clearLadder(t *testing.T, r *rig, day int) {
	t.Helper()
	for _, table := range []string{"dwarf_steps", "dwarf_flows", migrations.StepsTable(day), migrations.FlowsTable(day)} {
		if _, err := r.db.ExecContext(t.Context(), "TRUNCATE "+table); err != nil {
			t.Fatal(err)
		}
	}
}

// probe commits a one-row UPDATE every 5 ms on a table of its own, and keeps each commit's start and latency.
type probe struct {
	mu   sync.Mutex
	at   []time.Time
	took []time.Duration
}

// startProbe starts a probe on db until the test ends.
func startProbe(t *testing.T, db *sequel.DB) *probe {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS ladder_probe (id INT PRIMARY KEY, n BIGINT NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO ladder_probe (id, n) VALUES (1, 0) ON CONFLICT (id) DO NOTHING"); err != nil {
		t.Fatal(err)
	}
	p := &probe{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			s := time.Now()
			if _, err := db.ExecContext(ctx, "UPDATE ladder_probe SET n=n+1 WHERE id=1"); err != nil {
				continue
			}
			p.mu.Lock()
			p.at, p.took = append(p.at, s), append(p.took, time.Since(s))
			p.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return p
}

// probeStats are the probe's commits begun within a window.
type probeStats struct {
	n             int
	p50, p99, max time.Duration
}

// over returns the probe's commits begun between from and to.
func (p *probe) over(from, to time.Time) probeStats {
	p.mu.Lock()
	var in []time.Duration
	for i, s := range p.at {
		if !s.Before(from) && !s.After(to) {
			in = append(in, p.took[i])
		}
	}
	p.mu.Unlock()
	if len(in) == 0 {
		return probeStats{}
	}
	slices.Sort(in)
	q := func(f float64) time.Duration {
		return in[min(len(in)-1, int(f*float64(len(in))))].Round(10 * time.Microsecond)
	}
	return probeStats{n: len(in), p50: q(0.5), p99: q(0.99), max: in[len(in)-1].Round(10 * time.Microsecond)}
}

// envList returns the comma-separated values of the environment variable name, or of def when it is unset.
func envList(t *testing.T, name, def string) []string {
	v := os.Getenv(name)
	if v == "" {
		v = def
	}
	return strings.Split(v, ",")
}

// envInts returns envList's values as integers.
func envInts(t *testing.T, name, def string) []int {
	var out []int
	for _, s := range envList(t, name, def) {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil {
			t.Fatalf("%s: %q is not a whole number", name, s)
		}
		out = append(out, n)
	}
	return out
}
