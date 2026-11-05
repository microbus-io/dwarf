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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/sequel"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The volume mode fills a database to a target dwarf_steps row count WITHOUT deleting anything, probing
// the read paths at checkpoints along the way. It answers the question short benchmarks cannot: does the
// index story hold at size? Every probe is deliberately taken UNDER the fill load, because that is the
// shape an operator actually experiences - a big table with work still arriving. The fill uses whatever
// -workload is set (default linear = 10 steps per flow, so 10M steps is ~1M flows).
//
// What each probe pins:
//   - fill steps/s        - the refiller's band scan and the claim CAS at depth. Flat = the selection
//     index still seeks; a decay = the hot path started scanning the accumulated backlog.
//   - History latency     - the root_flow_id tree scan plus the per-flow step scan on a real tree.
//   - List latency        - ListLive's newest 100 flows, the live table per shard however large.
//   - disk MB             - the "append-only terminal sections" fragmentation claim against a real B-tree.
//   - throughput (with -volume-probe) - the fill pauses at each checkpoint, including the empty database at
//     the start, for an open-loop SATURATED window and then one at 2/3 of the flows/s it measured, both at
//     the pinned pool. That is the question an operator asks of a mature database - what does it serve now,
//     and how does latency look at a normal operating point - which the fill rate alone cannot answer: a
//     closed-loop fill measures its own concurrency, not the database's ceiling.
//
// The finish measures retention at size: how fast Purge MARKS roots, and then how fast the reaper
// actually DELETES the rows it marked (the two are separate - Purge stamps delete_after_ms, the reaper
// does the set-based delete on its own ticker).

// volumeCheckpoint is one probe taken during the fill.
type volumeCheckpoint struct {
	TSec            float64 `json:"tSec"`
	StepRows        int     `json:"stepRows"`
	FlowRows        int     `json:"flowRows"`
	FillStepsPerSec float64 `json:"fillStepsPerSec"` // since the previous checkpoint
	HistoryMs       float64 `json:"historyMs"`
	ListFirstMs     float64 `json:"listFirstMs"`
	StepsDiskMB     float64 `json:"stepsDiskMB"`
	FlowsDiskMB     float64 `json:"flowsDiskMB"`
	// RowCountError is why the exact row count failed at this checkpoint; StepRows is then the estimate that
	// triggered it and FlowRows is 0.
	RowCountError string `json:"rowCountError,omitzero"`
	// Saturated and TwoThirds are the -volume-probe windows taken at this checkpoint, nil without it.
	Saturated *stepResult `json:"saturated,omitempty"`
	TwoThirds *stepResult `json:"twoThirds,omitempty"`
}

// volumeReport is the volume run's artifact section.
type volumeReport struct {
	TargetStepRows   int                `json:"targetStepRows"`
	DurationSec      float64            `json:"durationSec"`
	Concurrency      int                `json:"concurrency"`
	FlowsSubmitted   int64              `json:"flowsSubmitted"`
	Errors           int64              `json:"errors"`
	Checkpoints      []volumeCheckpoint `json:"checkpoints"`
	PurgeMarkedRoots int                `json:"purgeMarkedRoots"`
	PurgeMarkMs      float64            `json:"purgeMarkMs"`
	ReapedRows       int                `json:"reapedRows"`
	ReapSec          float64            `json:"reapSec"`
	ReapRowsPerSec   float64            `json:"reapRowsPerSec"`
	// Invalidity names the first probe window that was not a measurement (windowInvalidity), "" when every
	// one was. A volume run carrying one is invalid, like a sweep with an invalid step.
	Invalidity string `json:"invalidity,omitzero"`
}

// runVolume fills to targetSteps rows, probing at each of checkpoints (ascending step-row counts; the target is
// probed whether or not it is listed), then measures retention. With checkpointsMB it fills by stored size
// instead: it probes as dwarf_steps passes each size on disk, ascending MB, and stops at the last. Size is the
// axis for a bytes fill, because the probes' own small rows outnumber the fill's large ones by far: two 0 B
// probes on the empty database wrote ~15M step rows, twice a 512 GB fill's ~7M.
//
// twoThirds adds a window at 2/3 of each saturated probe's rate. It doubles the rows and time each checkpoint
// costs, so a fill whose ladder starts low can leave it out.
//
// Between checkpoints the fill tracks its progress from the executed-steps counter: every step row a sweep
// workload creates executes once, so the rows at the last exact count plus the steps executed since is the
// table's size less the steps still pending. The exact COUNT(*) runs only at a checkpoint. Polled every 5s it
// is a full scan of both tables that holds a snapshot for its whole run, and at a size where it outlasts its
// timeout the fill never sees a checkpoint at all.
//
// probe, when non-nil, runs one open-loop measurement window at a commanded flow rate (0 = flat out) and
// reports why it was not a measurement, if it was not; the fill is paused around it, so the probe measures
// the database at its current size and not a fill and a probe competing for it.
//
// probeStepsPerFlow is the probe workload's steps per flow: the 2/3 window's commanded flow rate is 2/3 of the
// saturated window's EXECUTED steps/s in flows, never of its flowsPerSec, which is the admission rate and runs
// ahead of what was served.
func runVolume(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader, dbs *dbStatsSampler,
	pick func() *workload, k, targetSteps int, checkpoints []int, checkpointsMB []float64,
	probe func(rate float64) (stepResult, string), probeStepsPerFlow int, twoThirds bool) *volumeReport {

	var (
		stop       atomic.Bool
		inFlight   atomic.Int64
		flows      atomic.Int64
		errCount   atomic.Int64
		recentKey  atomic.Value // string: a completed flow key, for the History probe
		submitters sync.WaitGroup
	)
	recentKey.Store("")
	watch := watchProgress(ctx, engines, readers, inFlight.Load)
	defer watch.stop()

	fill := func() {
		stop.Store(false)
		for i := range k {
			eng := engines[i%len(engines)]
			submitters.Go(func() {
				for !stop.Load() {
					w := pick()
					// No FlowOptions: nothing is marked for deletion, so rows accumulate - that is the point.
					inFlight.Add(1)
					key, out, err := eng.Run(ctx, w.graphURL, w.initialState(), nil)
					inFlight.Add(-1)
					if err != nil || out.Status != workflow.StatusCompleted {
						errCount.Add(1)
						continue
					}
					flows.Add(1)
					if flows.Load()%64 == 0 {
						recentKey.Store(key)
					}
				}
			})
		}
	}
	// pause stops the fill and waits for its submitters, and reports false when the watchdog fires first: a
	// submitter stuck in Run on a hung engine never returns.
	pause := func() bool {
		stop.Store(true)
		return watch.await(submitters.Wait)
	}
	report := &volumeReport{TargetStepRows: targetSteps, Concurrency: k}
	invalid := func(window string, why string) {
		if why != "" && report.Invalidity == "" {
			report.Invalidity = window + ": " + why
		}
	}
	// executed is the fleet's executed-step count, or -1 when the collection failed.
	executed := func() int64 {
		c := collectAllCounters(readers)
		if c == nil {
			return -1
		}
		return c["dwarf_steps_executed"]
	}
	// exactRows takes the exact count and rebases the estimate on it. On a failed count it keeps the old base,
	// records the failure and returns the estimate.
	var baseRows, baseExec int64
	estimate := func() int64 {
		if n := executed(); n >= 0 {
			return baseRows + n - baseExec
		}
		return -1
	}
	exactRows := func(at string) (steps, flowRows int, err error) {
		steps, flowRows, err = rowCounts(ctx, engines[0])
		if err != nil {
			invalid("row count "+at, err.Error())
			return int(max(0, estimate())), 0, err
		}
		if n := executed(); n >= 0 {
			baseRows, baseExec = int64(steps), n
		}
		return steps, flowRows, nil
	}
	// probeThroughput takes the saturated window, then the 2/3 one at the rate the first measured. It reports
	// whether a window stalled, and stops there: every drain and window after it would wait on hung engines.
	probeThroughput := func(cp *volumeCheckpoint) (stalled bool) {
		if probe == nil {
			return false
		}
		sat, why := probe(0)
		invalid(fmt.Sprintf("saturated probe at %d step rows", cp.StepRows), why)
		cp.Saturated = &sat
		if stalledRun.Load() { // the window's watchdog fired, or its drain gave up
			return true
		}
		// A saturated window ends with a backlog of created-but-unfinished flows, which would otherwise drain
		// into the next window and be counted as its throughput. The fill resumes on a drained database for
		// the same reason.
		drainRunning(ctx, engines[0])
		if rate := sat.StepsPerSec / float64(max(1, probeStepsPerFlow)) * 2 / 3; twoThirds && rate > 0 {
			twoThirds, why := probe(rate)
			invalid(fmt.Sprintf("2/3 probe at %d step rows", cp.StepRows), why)
			cp.TwoThirds = &twoThirds
			if stalledRun.Load() {
				return true
			}
			drainRunning(ctx, engines[0])
		}
		fmt.Printf("probe at %d step rows: saturated %.0f steps/s (p99 %.0fms)", cp.StepRows, sat.StepsPerSec, sat.P99ms)
		if cp.TwoThirds != nil {
			fmt.Printf(", 2/3 %.0f steps/s (p50 %.0fms, p99 %.0fms)", cp.TwoThirds.StepsPerSec, cp.TwoThirds.P50ms, cp.TwoThirds.P99ms)
		}
		fmt.Println()
		return false
	}

	start := time.Now()
	// abandon ends the run at a stall: a probe's, or the fill's own watchdog. Neither waits for the fill's
	// submitters, which may be stuck on the hung engine.
	abandon := func(cp *volumeCheckpoint) *volumeReport {
		if cp != nil {
			report.Checkpoints = append(report.Checkpoints, *cp)
		}
		if why := watch.reason(); why != "" {
			fmt.Println("STALLED:", why)
			invalid("fill", "stalled: "+why)
		}
		report.DurationSec = time.Since(start).Seconds()
		report.FlowsSubmitted = flows.Load()
		report.Errors = errCount.Load()
		return report
	}
	steps, flowRows, _ := exactRows("at the start")
	lastRows, lastT := steps, start
	if probe != nil {
		// The empty-database baseline every later checkpoint is read against. The fill rate to the first
		// checkpoint starts after it, so it is not charged with the probe's windows, drains or rows.
		cp := volumeCheckpoint{StepRows: steps, FlowRows: flowRows}
		if probeThroughput(&cp) {
			return abandon(&cp)
		}
		report.Checkpoints = append(report.Checkpoints, cp)
		lastRows, _, _ = exactRows("after the baseline probe")
		lastT = time.Now()
	}
	fill()
	fmt.Printf("%-7s %11s %10s %12s %10s %10s %9s %9s\n",
		"t(s)", "stepRows", "flowRows", "fillSteps/s", "histMs", "listMs", "stepsMB", "flowsMB")

	// nextAfter is the first checkpoint past rows, or the target once none is left.
	nextAfter := func(rows int) int {
		for _, c := range checkpoints {
			if c > rows {
				return c
			}
		}
		return targetSteps
	}
	next := nextAfter(lastRows)
	// storedMB is dwarf_steps' size on disk, or -1 when the reading failed. A size query is a catalog lookup,
	// cheap enough for every tick, unlike the row count.
	storedMB := func() float64 {
		st := dbs.sample(ctx)
		if !st.ok {
			return -1
		}
		return float64(st.StepsBytes) / (1 << 20)
	}
	nextMBAfter := func(mb float64) float64 {
		for _, c := range checkpointsMB {
			if c > mb {
				return c
			}
		}
		return math.Inf(1)
	}
	nextMB := math.Inf(1)
	if len(checkpointsMB) > 0 {
		nextMB = nextMBAfter(storedMB())
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-watch.stalled:
			return abandon(nil)
		}
		if len(checkpointsMB) > 0 {
			// By size, with the row target as a cap: either one reached makes a checkpoint.
			if mb := storedMB(); mb < 0 || mb < nextMB {
				if est := estimate(); est < 0 || est < int64(targetSteps) {
					continue // a failed reading is retried on the next tick
				}
			}
		} else if est := estimate(); est < 0 || (est < int64(next) && est < int64(targetSteps)) {
			continue // a failed collection is retried on the next tick
		}
		now := time.Now()
		steps, flowRows, cerr := exactRows(fmt.Sprintf("near %d step rows", next))
		cp := volumeCheckpoint{
			TSec:            now.Sub(start).Seconds(),
			StepRows:        steps,
			FlowRows:        flowRows,
			FillStepsPerSec: float64(steps-lastRows) / now.Sub(lastT).Seconds(),
		}
		if cerr != nil {
			cp.RowCountError = cerr.Error()
		}
		cp.HistoryMs, cp.ListFirstMs = probeReads(ctx, engines[0], recentKey.Load().(string))
		st := dbs.sample(ctx)
		cp.StepsDiskMB = float64(st.StepsBytes) / (1 << 20)
		cp.FlowsDiskMB = float64(st.FlowsBytes) / (1 << 20)
		fmt.Printf("%-7.0f %11d %10d %12.0f %10.1f %10.1f %9.1f %9.1f\n",
			cp.TSec, cp.StepRows, cp.FlowRows, cp.FillStepsPerSec,
			cp.HistoryMs, cp.ListFirstMs, cp.StepsDiskMB, cp.FlowsDiskMB)
		done := steps >= targetSteps
		if len(checkpointsMB) > 0 {
			done = done || cp.StepsDiskMB >= checkpointsMB[len(checkpointsMB)-1]
		}
		if probe != nil {
			if !pause() {
				return abandon(&cp)
			}
			if probeThroughput(&cp) {
				return abandon(&cp)
			}
			if !done {
				fill()
			}
			// The probe windows add rows too (they count toward the fill, as the plan expects), but the fill
			// rate to the NEXT checkpoint must not be charged with the probe's time or rows.
			steps, _, _ = exactRows(fmt.Sprintf("after the probe at %d step rows", cp.StepRows))
			now = time.Now()
		}
		report.Checkpoints = append(report.Checkpoints, cp)

		lastRows, lastT = steps, now
		next = nextAfter(steps)
		if len(checkpointsMB) > 0 {
			// The probe's rows count toward the size, so the next checkpoint is the first past where it left it - and
			// never one at or below the checkpoint just taken, which a failed size reading (-1) would otherwise restart
			// the ladder from.
			nextMB = nextMBAfter(max(storedMB(), cp.StepsDiskMB, nextMB))
		}
		if done {
			break
		}
	}

	if !pause() {
		return abandon(nil)
	}
	watch.stop()
	report.DurationSec = time.Since(start).Seconds()
	report.FlowsSubmitted = flows.Load()
	report.Errors = errCount.Load()

	// Retention at size, in two separately-measured halves: Purge (4096 roots a call) deletes the archived trees it
	// takes at once and stamps delete_after_ms on the live ones, and the reaper's own ticker does the set-based
	// delete of those afterwards. PurgeMarkMs therefore includes the archive's deletes. A failed row count
	// invalidates the reap rate, which is a difference of two of them.
	before, _, err := rowCounts(ctx, engines[0])
	if err != nil {
		invalid("row count before the purge", err.Error())
	}
	pctx, pcancel := context.WithTimeout(ctx, 2*time.Minute)
	markStart := time.Now()
	marked, _, err := engines[0].Purge(pctx, workflow.Query{Status: workflow.StatusCompleted,
		FromTime: time.Unix(0, 0), ToTime: time.Now().Add(time.Hour), Limit: 4096})
	report.PurgeMarkMs = float64(time.Since(markStart)) / float64(time.Millisecond)
	pcancel()
	if err == nil {
		report.PurgeMarkedRoots = marked
	}
	fmt.Printf("purge: marked %d roots in %.0fms; waiting for the reaper...\n", report.PurgeMarkedRoots, report.PurgeMarkMs)

	// Watch the rows actually drain. The reaper ticks ~1min, so give it a few cycles and measure the
	// drop; a plateau means it finished.
	reapStart := time.Now()
	prev := before
	stable := 0
	for time.Since(reapStart) < 5*time.Minute {
		time.Sleep(15 * time.Second)
		cur, _, err := rowCounts(ctx, engines[0])
		if err != nil {
			continue
		}
		if cur < prev {
			stable = 0
		} else if prev-cur == 0 && cur < before {
			stable++
			if stable >= 2 {
				break
			}
		}
		prev = cur
	}
	after, _, err := rowCounts(ctx, engines[0])
	if err != nil {
		invalid("row count after the reap", err.Error())
	}
	report.ReapedRows = before - after
	report.ReapSec = time.Since(reapStart).Seconds()
	if report.ReapSec > 0 {
		report.ReapRowsPerSec = float64(report.ReapedRows) / report.ReapSec
	}
	fmt.Printf("reaper: deleted %d rows in %.0fs (%.0f rows/s)\n",
		report.ReapedRows, report.ReapSec, report.ReapRowsPerSec)
	return report
}

// drainRunning waits until no flow is running, polling ListLive, so a window opens on a database with no
// backlog left over from the one before. It gives up after ten minutes rather than hang a campaign; a
// backlog that does not drain in that time is itself the finding, and the next window's numbers show it.
func drainRunning(ctx context.Context, eng *engine.Engine) {
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		lctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		running, err := eng.ListLive(lctx, workflow.Query{Status: workflow.StatusRunning, Limit: 1})
		cancel()
		if err == nil && len(running) == 0 {
			return
		}
		time.Sleep(time.Second)
	}
	fmt.Println("drain: flows still running after 10m; the next window opens on a backlog")
}

// rowCounts counts the live step and flow rows across every shard, exactly, with a COUNT(*) on each table:
// dialect-agnostic, unlike the catalog queries the disk sampler uses. It is a full scan of both tables per
// shard, which the bench can afford against databases it owns, but not on a poll. It fails if any shard fails
// to count: a zero would read as an empty database.
func rowCounts(ctx context.Context, eng *engine.Engine) (steps, flows int, err error) {
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var mu sync.Mutex
	err = engineseams.DB(eng).OnEach(sctx, func(ctx context.Context, db *sequel.DB, _ int) error {
		var s, f int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_steps").Scan(&s); err != nil {
			return err
		}
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM dwarf_flows").Scan(&f); err != nil {
			return err
		}
		mu.Lock()
		steps, flows = steps+s, flows+f
		mu.Unlock()
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return steps, flows, nil
}

// probeReads times the read paths at the current volume: History on one real flow, and ListLive.
func probeReads(ctx context.Context, eng *engine.Engine, flowKey string) (historyMs, listMs float64) {
	pctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if flowKey != "" {
		t := time.Now()
		if _, err := eng.History(pctx, flowKey); err == nil {
			historyMs = float64(time.Since(t)) / float64(time.Millisecond)
		}
	}

	t := time.Now()
	if _, err := eng.ListLive(pctx, workflow.Query{Limit: 100}); err == nil {
		listMs = float64(time.Since(t)) / float64(time.Millisecond)
	}
	return historyMs, listMs
}
