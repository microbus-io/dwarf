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
	"os"
	"strings"
	"testing"
	"time"
)

// TestMeasure_Sizing times a batch's sizing - the capped counts, one round trip a tree - against one GROUP BY over the
// batch, at no added latency and at a simulated round trip, for a batch of small trees and for a large tree first in
// the batch. Opt-in (DWARF_MEASURE_SIZING=1), a measurement rather than a check; run it against the database wanted
// (SEQUEL_TESTING_DSN).
func TestMeasure_Sizing(t *testing.T) {
	if os.Getenv("DWARF_MEASURE_SIZING") == "" {
		t.Skip("set DWARF_MEASURE_SIZING=1 to measure")
	}
	ctx := t.Context()
	r := newRig(t, 30)
	r.cfg.StageRows = 131072
	small := seedTrees(t, r.db, 512, callShape)
	big := seedWideTree(t, r.db, 1, 20, 10000) // 21 flows, 200,001 steps
	groupBy := func(roots []int) time.Duration {
		start := time.Now()
		rows, err := r.db.QueryContext(ctx, "SELECT f.root_flow_id, COUNT(*) FROM dwarf_flows f"+
			" LEFT JOIN dwarf_steps s ON s.flow_id=f.flow_id WHERE f.root_flow_id IN ("+intCSV(roots)+") GROUP BY f.root_flow_id")
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
		}
		rows.Close()
		return time.Since(start)
	}
	capped := func(roots []int) (time.Duration, int) {
		start := time.Now()
		cut, _, _, err := r.p().rootsWithinRows(ctx, roots, true)
		if err != nil {
			t.Fatal(err)
		}
		return time.Since(start), len(cut)
	}
	best := func(f func() time.Duration) time.Duration {
		m := time.Hour
		for range 5 {
			m = min(m, f())
		}
		return m
	}
	for _, rtt := range []time.Duration{0, 500 * time.Microsecond} {
		r.db.SimulateRTT(rtt)
		var cut int
		smallCapped := best(func() time.Duration { d, n := capped(small); cut = n; return d })
		smallGroup := best(func() time.Duration { return groupBy(small) })
		withBig := append([]int{big}, small[:511]...)
		bigCapped := best(func() time.Duration { d, _ := capped(withBig); return d })
		bigGroup := best(func() time.Duration { return groupBy(withBig) })
		t.Logf("driver=%s rtt=%v | 512 small trees: capped %v (%d in the cut), GROUP BY %v | a 200k-row tree first: capped %v, GROUP BY %v",
			r.db.DriverName(), rtt, smallCapped, cut, smallGroup, bigCapped, bigGroup)
	}
	r.db.SimulateRTT(0)
}

// TestMeasure_Staging times archiving one large tree both ways on the rig's shard: the atomic move in one transaction,
// and the staged copy chunk by chunk at the default batch of 4,096 rows - its chunk count, slowest chunk and total -
// for a narrow tree, a wide one, and one with large states. Opt-in (DWARF_MEASURE_STAGING=1); run it against the
// database wanted (SEQUEL_TESTING_DSN).
func TestMeasure_Staging(t *testing.T) {
	if os.Getenv("DWARF_MEASURE_STAGING") == "" {
		t.Skip("set DWARF_MEASURE_STAGING=1 to measure")
	}
	for _, shape := range []struct {
		name                    string
		children, steps, rootSt int
		stateBytes              int
		unrelated               int  // live steps of an open flow beside the tree
		analyze                 bool // fresh statistics before the copy (PostgreSQL)
	}{
		{"narrow 200k steps, ~200 B", 0, 0, 200000, 200, 0, false},
		{"wide 2,000 x 10 steps, ~200 B", 2000, 10, 1, 200, 0, false},
		{"narrow 50k steps, 8 KB", 0, 0, 50000, 8192, 0, false},
		{"wide 2,000 x 10 beside 200k live steps, no ANALYZE", 2000, 10, 1, 200, 200000, false},
		{"wide 2,000 x 10 beside 200k live steps, ANALYZE", 2000, 10, 1, 200, 200000, true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			measureStaging(t, shape.children, shape.steps, shape.rootSt, shape.stateBytes, shape.unrelated, shape.analyze, shape.name)
		})
	}
}

// measureStaging is one shape of TestMeasure_Staging, on a database of its own.
func measureStaging(t *testing.T, children, steps, rootSteps, stateBytes, unrelated int, analyze bool, name string) {
	ctx := t.Context()
	shape := struct {
		name                    string
		children, steps, rootSt int
		stateBytes              int
	}{name, children, steps, rootSteps, stateBytes}
	{
		r := newRig(t, 30)
		if unrelated > 0 {
			open := seedWideTree(t, r.db, unrelated, 0, 0)
			if _, err := r.db.ExecContext(ctx, "UPDATE dwarf_flows SET status='running' WHERE flow_id=?", open); err != nil {
				t.Fatal(err)
			}
		}
		r.cfg.BatchRows = 4096
		r.cfg.StageRows = 1
		today, err := r.archive.Today(ctx)
		if err != nil {
			t.Fatal(err)
		}
		state := `{"pad":"` + strings.Repeat("x", shape.stateBytes) + `"}`
		seed := func() int {
			root := seedWideTree(t, r.db, shape.rootSt, shape.children, shape.steps)
			if _, err := r.db.ExecContext(ctx, "UPDATE dwarf_steps SET state=? WHERE flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?)", []byte(state), root); err != nil {
				t.Fatal(err)
			}
			return root
		}
		atomic := seed()
		start := time.Now()
		if _, err := r.reaper.MoveTrees(ctx, r.db, r.archive, today, []int{atomic}); err != nil {
			t.Fatal(err)
		}
		moveTime := time.Since(start)

		staged := seed()
		d := &filingDay{}
		if err := r.p().refreshDay(ctx, d); err != nil {
			t.Fatal(err)
		}
		at, ok, err := r.p().claimAttempt(ctx, closedDue(0, 0), staged, d, time.Time{})
		if err != nil || !ok {
			t.Fatalf("claim: %v %v", ok, err)
		}
		if analyze && r.db.DriverName() == "pgx" {
			if _, err := r.db.ExecContext(ctx, "ANALYZE dwarf_steps"); err != nil {
				t.Fatal(err)
			}
			if _, err := r.db.ExecContext(ctx, "ANALYZE dwarf_flows"); err != nil {
				t.Fatal(err)
			}
		}
		pos := stagePos{at: at}
		var chunks int
		var slowest, total time.Duration
		for {
			start := time.Now()
			n, _, done, err := r.p().stageChunk(ctx, staged, r.archive, &pos)
			if err != nil {
				t.Fatal(err)
			}
			took := time.Since(start)
			chunks++
			total += took
			slowest = max(slowest, took)
			if n > 0 || done {
				break
			}
		}
		t.Logf("driver=%s %s | atomic move %v | staged %d chunks, slowest %v, total %v",
			r.db.DriverName(), shape.name, moveTime, chunks, slowest, total)
	}
}
