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

package engine

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// The open-flow scan measurements: orphan repair and the stranded-flow alarm (sweepOpenFlows) against a few open flows
// with large completed histories. Measurements, not pins: each runs only when DWARF_TESTING_OPEN_SCAN_DSN names a
// database, which is kept across runs - TestOpenScan_Seed fills it, TestOpenScan_Measure and TestOpenScan_Loop read it.
// To measure dispatch beside the scans, run the bench against the same database; TestOpenScan_Loop adds passes back to
// back without joining the fleet.

const (
	openScanURL      = "openscan/g"
	openScanChildURL = "openscan/child"
	openScanSmallURL = "openscan/small"
)

// openScanEngine returns a production engine on the measurement database, its janitors driven by hand. workers < 0
// leaves the crew derived.
func openScanEngine(t *testing.T, workers int) *Engine {
	dsn := os.Getenv("DWARF_TESTING_OPEN_SCAN_DSN")
	if dsn == "" {
		t.Skip("set DWARF_TESTING_OPEN_SCAN_DSN to measure")
	}
	assert := testarossa.For(t)
	e := NewEngine()
	assert.NoError(e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))))
	e.janitorManual = true
	assert.NoError(e.SetShard(ShardSpec{Index: 1, DSN: dsn}))
	if workers >= 0 {
		assert.NoError(e.SetWorkers(workers))
	}
	return e
}

// TestOpenScan_Seed runs one forEach per width in DWARF_TESTING_OPEN_SCAN_WIDTHS (comma-separated) and leaves each flow
// open: every branch completes and arrives but the last, which is parked on a subgraph child that sleeps for 30 days, so
// the flow is running with one parked step at the end of its history. DWARF_TESTING_OPEN_SCAN_SMALL adds that many open
// flows of four steps, the last asleep. DWARF_TESTING_OPEN_SCAN_PAYLOAD is the bytes each step writes (256).
func TestOpenScan_Seed(t *testing.T) {
	var widths []int
	for s := range strings.SplitSeq(os.Getenv("DWARF_TESTING_OPEN_SCAN_WIDTHS"), ",") {
		if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n > 1 {
			widths = append(widths, n)
		}
	}
	small, _ := strconv.Atoi(os.Getenv("DWARF_TESTING_OPEN_SCAN_SMALL"))
	if len(widths) == 0 && small <= 0 {
		t.Skip("set DWARF_TESTING_OPEN_SCAN_WIDTHS or DWARF_TESTING_OPEN_SCAN_SMALL to seed")
	}
	payload := 256
	if n, err := strconv.Atoi(os.Getenv("DWARF_TESTING_OPEN_SCAN_PAYLOAD")); err == nil && n >= 0 {
		payload = n
	}
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("OpenScan")
	g.SetURL("Seed", "openscan/seed")
	g.SetURL("Branch", "openscan/branch")
	g.SetURL("Join", "openscan/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Seed", "Branch", "items", "item")
	g.AddTransitionChain("Branch", "Join", workflow.End)
	mux.HandleGraph(openScanURL, g)
	mux.HandleTask("openscan/seed", func(ctx context.Context, f *workflow.Flow) error {
		items := make([]int, f.GetInt("width"))
		for i := range items {
			items[i] = i
		}
		return f.Set("items", items)
	})
	result := strings.Repeat("x", payload)
	mux.HandleTask("openscan/branch", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetInt("itemIndex") == f.GetInt("itemCount")-1 {
			_, err := f.Subgraph(openScanChildURL, nil, nil)
			return err // parked: the child sleeps
		}
		return f.Set("result", result)
	})
	mux.HandleTask("openscan/join", func(ctx context.Context, f *workflow.Flow) error { return nil })
	child := workflow.NewGraph("OpenScanChild")
	child.SetURL("Nap", "openscan/nap")
	child.SetURL("Wake", "openscan/wake")
	child.AddTransitionChain("Nap", "Wake", workflow.End)
	mux.HandleGraph(openScanChildURL, child)
	mux.HandleTask("openscan/nap", func(ctx context.Context, f *workflow.Flow) error {
		f.Sleep(30 * 24 * time.Hour)
		return nil
	})
	mux.HandleTask("openscan/wake", func(ctx context.Context, f *workflow.Flow) error { return nil })
	// A small open flow: three steps done, the third sleeping before the fourth.
	smallGraph := workflow.NewGraph("OpenScanSmall")
	for _, name := range []string{"A", "B", "C", "D"} {
		smallGraph.SetURL(name, "openscan/small/"+strings.ToLower(name))
	}
	smallGraph.AddTransitionChain("A", "B", "C", "D", workflow.End)
	mux.HandleGraph(openScanSmallURL, smallGraph)
	for _, name := range []string{"a", "b", "d"} {
		mux.HandleTask("openscan/small/"+name, func(ctx context.Context, f *workflow.Flow) error { return f.Set("result", result) })
	}
	mux.HandleTask("openscan/small/c", func(ctx context.Context, f *workflow.Flow) error {
		f.Sleep(30 * 24 * time.Hour)
		return f.Set("result", result)
	})

	e := openScanEngine(t, -1)
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(mux))
	assert.Must(assert.NoError(e.Startup(ctx)))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	for _, w := range widths {
		start := time.Now()
		key, err := e.Create(ctx, openScanURL, map[string]any{"width": w}, nil)
		assert.Must(assert.NoError(err))
		_, _, flowID, _, err := keys.ParseFlowKey(key)
		assert.Must(assert.NoError(err))
		for {
			time.Sleep(time.Second)
			var arrivals int
			err := db.QueryRowContext(ctx, "SELECT COALESCE(MAX(cohort_arrivals), 0) FROM dwarf_steps WHERE flow_id=? AND cohort_size>0", flowID).Scan(&arrivals)
			assert.Must(assert.NoError(err))
			var parked, napping int
			err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusRunning+"' AND parked<>0", flowID).Scan(&parked)
			assert.Must(assert.NoError(err))
			err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps s JOIN dwarf_flows c ON c.flow_id=s.flow_id"+
				" WHERE c.surgraph_flow_id=? AND s.status='"+workflow.StatusPending+"'", flowID).Scan(&napping)
			assert.Must(assert.NoError(err))
			if arrivals >= w-1 && parked == 1 && napping == 1 {
				break
			}
			if time.Since(start) > 2*time.Hour {
				t.Fatalf("width %d: %d arrivals after %v", w, arrivals, time.Since(start))
			}
		}
		var steps int
		assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps WHERE flow_id=?", flowID).Scan(&steps))
		t.Logf("width=%d flow=%d steps=%d payload=%d seeded in %v", w, flowID, steps, payload, time.Since(start).Round(time.Millisecond))
	}

	if small > 0 {
		start := time.Now()
		for range small {
			_, err := e.Create(ctx, openScanSmallURL, nil, nil)
			assert.Must(assert.NoError(err))
		}
		for {
			time.Sleep(time.Second)
			var asleep int
			err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id"+
				" WHERE f.workflow_url=? AND s.task_name='D' AND s.status='"+workflow.StatusPending+"'", openScanSmallURL).Scan(&asleep)
			assert.Must(assert.NoError(err))
			if asleep >= small {
				break
			}
			if time.Since(start) > time.Hour {
				t.Fatalf("%d of %d small flows asleep after %v", asleep, small, time.Since(start))
			}
		}
		t.Logf("small=%d seeded in %v", small, time.Since(start).Round(time.Millisecond))
	}
}

// openScanSQL returns sweepOpenFlows' chunk statement for the repair (alarm false) or the alarm, over placeholders ph.
// A copy of the statement in wedge.go: keep the two in step.
func openScanSQL(ph string, alarm bool) string {
	const lostTransition = "EXISTS (SELECT 1 FROM dwarf_steps s WHERE s.flow_id=f.flow_id AND s.status='" + workflow.StatusCompleted + "' AND s.advancing=1 AND s.lease_expires<=NOW_UTC())"
	const stranded = "(f.status='" + workflow.StatusRunning + "'" +
		" AND NOT EXISTS (SELECT 1 FROM dwarf_steps s2 WHERE s2.flow_id=f.flow_id AND (s2.status IN ('" + workflow.StatusCreated + "', '" + workflow.StatusPending + "', '" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "')" +
		" OR (s2.status='" + workflow.StatusCompleted + "' AND s2.advancing=1))))"
	hit := lostTransition
	if alarm {
		hit = stranded
	}
	return "SELECT f.flow_id, f.created_hour, f.workflow_url FROM dwarf_flows f" +
		" WHERE f.flow_id IN (" + ph + ") AND f.status IN ('" + workflow.StatusRunning + "', '" + workflow.StatusInterrupted + "') AND " + hit
}

// openScanTime runs a query n times, draining its rows, and returns each run's duration, sorted.
func openScanTime(t *testing.T, db *sequel.DB, n int, query string, args ...any) []time.Duration {
	assert := testarossa.For(t)
	var out []time.Duration
	for range n {
		start := time.Now()
		rows, err := db.QueryContext(context.Background(), query, args...)
		assert.Must(assert.NoError(err))
		for rows.Next() {
		}
		assert.NoError(rows.Err())
		rows.Close()
		out = append(out, time.Since(start))
	}
	slices.Sort(out)
	return out
}

// openScanExplain logs a PostgreSQL EXPLAIN (ANALYZE, BUFFERS) of the query.
func openScanExplain(t *testing.T, db *sequel.DB, label string, query string, args ...any) {
	if db.DriverName() != "pgx" {
		return
	}
	assert := testarossa.For(t)
	rows, err := db.QueryContext(context.Background(), "EXPLAIN (ANALYZE, BUFFERS) "+query, args...)
	assert.Must(assert.NoError(err))
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var line string
		assert.NoError(rows.Scan(&line))
		b.WriteString("\n    " + line)
	}
	t.Logf("%s:%s", label, b.String())
}

// TestOpenScan_Measure times the repair's and the alarm's chunk statement over each seeded flow alone, with its plan,
// then whole passes over the shard's open flows. With DWARF_TESTING_OPEN_SCAN_PLANT=1 it then flags the newest completed
// branch of the widest flow as a completion whose transition was lost, times the pass that rewinds it, and puts the step
// back.
func TestOpenScan_Measure(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()
	e := openScanEngine(t, 0)
	defer e.Shutdown(ctx)
	assert.NoError(e.SetHost(hosts.NewMux()))
	assert.Must(assert.NoError(e.Startup(ctx)))
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()

	var open int
	assert.NoError(db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows WHERE status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')").Scan(&open))
	type wide struct{ flowID, steps int }
	var flows []wide
	rows, err := db.QueryContext(ctx, "SELECT f.flow_id, COUNT(*) FROM dwarf_flows f JOIN dwarf_steps s ON s.flow_id=f.flow_id"+
		" WHERE f.workflow_url=? AND f.status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"') GROUP BY f.flow_id ORDER BY COUNT(*)", openScanURL)
	assert.Must(assert.NoError(err))
	for rows.Next() {
		var w wide
		assert.NoError(rows.Scan(&w.flowID, &w.steps))
		flows = append(flows, w)
	}
	rows.Close()
	t.Logf("open flows on the shard: %d, seeded: %v", open, flows)

	for _, w := range flows {
		ph, ids := inList([]int{w.flowID})
		for _, alarm := range []bool{false, true} {
			name := "repair"
			if alarm {
				name = "alarm"
			}
			q := openScanSQL(ph, alarm)
			d := openScanTime(t, db, 7, q, ids...)
			t.Logf("flow=%d steps=%d %s: min=%v median=%v max=%v", w.flowID, w.steps, name, d[0], d[len(d)/2], d[len(d)-1])
			openScanExplain(t, db, "flow="+strconv.Itoa(w.flowID)+" "+name, q, ids...)
		}
	}

	for _, alarm := range []bool{false, true} {
		var d []time.Duration
		for range 7 {
			start := time.Now()
			assert.NoError(e.sweepOpenFlows(ctx, db, 1, alarm))
			d = append(d, time.Since(start))
		}
		slices.Sort(d)
		t.Logf("pass alarm=%v over %d open flows: min=%v median=%v max=%v", alarm, open, d[0], d[len(d)/2], d[len(d)-1])
	}

	if os.Getenv("DWARF_TESTING_OPEN_SCAN_PLANT") != "1" || len(flows) == 0 {
		return
	}
	widest := flows[len(flows)-1]
	// The newest completed branch, beside the parked one at the end of the flow: a completion whose transition was lost.
	// Nothing dispatches it here (this engine has no workers), and it is put back as it was afterwards.
	var stepID, seq int
	assert.Must(assert.NoError(db.QueryRowContext(ctx, "SELECT step_id, lease_seq FROM dwarf_steps WHERE flow_id=? AND status='"+workflow.StatusCompleted+"'"+
		" AND task_name='Branch' ORDER BY step_id DESC LIMIT_OFFSET(1, 0)", widest.flowID).Scan(&stepID, &seq)))
	_, err = db.ExecContext(ctx, "UPDATE dwarf_steps SET advancing=1, lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -1000) WHERE step_id=?", stepID)
	assert.Must(assert.NoError(err))
	defer func() {
		_, err := db.ExecContext(ctx, "UPDATE dwarf_steps SET status='"+workflow.StatusCompleted+"', advancing=0, lease_seq=? WHERE step_id=?", seq, stepID)
		assert.NoError(err, "the planted step must be put back")
	}()
	ph, ids := inList([]int{widest.flowID})
	openScanExplain(t, db, "planted flow="+strconv.Itoa(widest.flowID)+" repair", openScanSQL(ph, false), ids...)
	start := time.Now()
	assert.NoError(e.recoverOrphanedFlows(ctx, db, 1))
	took := time.Since(start)
	var status string
	var after int
	assert.NoError(db.QueryRowContext(ctx, "SELECT status, lease_seq FROM dwarf_steps WHERE step_id=?", stepID).Scan(&status, &after))
	t.Logf("planted step %d in flow %d (%d steps): the pass took %v, the step is %s at lease_seq %d (was %d)", stepID, widest.flowID, widest.steps, took, status, after, seq)
	assert.Equal(workflow.StatusPending, status)
	assert.Equal(seq+1, after)
}

// TestOpenScan_Loop runs the repair's statements - the open-flow listing, then a chunk statement per 500 flows - back
// to back for DWARF_TESTING_OPEN_SCAN_LOOP, on a plain connection rather than an engine, so it registers no replica
// beside a load run on the same database.
func TestOpenScan_Loop(t *testing.T) {
	dsn := os.Getenv("DWARF_TESTING_OPEN_SCAN_DSN")
	dur, _ := time.ParseDuration(os.Getenv("DWARF_TESTING_OPEN_SCAN_LOOP"))
	if dsn == "" || dur <= 0 {
		t.Skip("set DWARF_TESTING_OPEN_SCAN_DSN and DWARF_TESTING_OPEN_SCAN_LOOP to loop")
	}
	assert := testarossa.For(t)
	ctx := context.Background()
	db, err := sequel.Open("", dsn)
	assert.Must(assert.NoError(err))
	defer db.Close()
	db.SetMaxOpenConns(1)
	var passes []time.Duration
	end := time.Now().Add(dur)
	for time.Now().Before(end) {
		start := time.Now()
		var open []int
		rows, err := db.QueryContext(ctx, "SELECT flow_id FROM dwarf_flows WHERE status IN ('"+workflow.StatusRunning+"', '"+workflow.StatusInterrupted+"')")
		assert.Must(assert.NoError(err))
		for rows.Next() {
			var id int
			assert.NoError(rows.Scan(&id))
			open = append(open, id)
		}
		rows.Close()
		slices.Sort(open)
		for chunk := range slices.Chunk(open, inListChunk) {
			ph, ids := inList(chunk)
			openScanTime(t, db, 1, openScanSQL(ph, false), ids...)
		}
		passes = append(passes, time.Since(start))
	}
	slices.Sort(passes)
	t.Logf("%d passes in %v: min=%v median=%v max=%v", len(passes), dur, passes[0], passes[len(passes)/2], passes[len(passes)-1])
}
