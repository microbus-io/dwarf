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
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
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
//   - List first/deep     - keyset (cursor) pagination. First-page ~= deep-page is the proof it is a
//     seek per page, not a growing scan; a rising deep-page cost would mean OFFSET-like behavior.
//   - disk MB             - the "append-only terminal sections" fragmentation claim against a real B-tree.
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
	ListDeepMs      float64 `json:"listDeepMs"`
	ListPagesWalked int     `json:"listPagesWalked"`
	StepsDiskMB     float64 `json:"stepsDiskMB"`
	FlowsDiskMB     float64 `json:"flowsDiskMB"`
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
}

// runVolume fills to targetSteps rows, probing every checkpointEvery rows, then measures retention.
func runVolume(ctx context.Context, engines []*engine.Engine, dbs *dbStatsSampler, pick func() *workload,
	k, targetSteps, checkpointEvery int) *volumeReport {

	var (
		stop       atomic.Bool
		flows      atomic.Int64
		errCount   atomic.Int64
		recentKey  atomic.Value // string: a completed flow key, for the History probe
		submitters sync.WaitGroup
	)
	recentKey.Store("")

	for i := range k {
		eng := engines[i%len(engines)]
		submitters.Go(func() {
			for !stop.Load() {
				w := pick()
				// No FlowOptions: nothing is marked for deletion, so rows accumulate - that is the point.
				key, out, err := eng.Run(ctx, w.graphURL, w.initialState(), nil)
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

	report := &volumeReport{TargetStepRows: targetSteps, Concurrency: k}
	start := time.Now()
	fmt.Printf("%-7s %11s %10s %12s %10s %10s %10s %9s %9s\n",
		"t(s)", "stepRows", "flowRows", "fillSteps/s", "histMs", "listMs", "listDeepMs", "stepsMB", "flowsMB")

	lastRows, lastT := 0, start
	next := checkpointEvery
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		<-ticker.C
		steps, flowRows := rowCounts(ctx, engines[0])
		if steps < next && steps < targetSteps {
			continue
		}
		now := time.Now()
		cp := volumeCheckpoint{
			TSec:            now.Sub(start).Seconds(),
			StepRows:        steps,
			FlowRows:        flowRows,
			FillStepsPerSec: float64(steps-lastRows) / now.Sub(lastT).Seconds(),
		}
		cp.HistoryMs, cp.ListFirstMs, cp.ListDeepMs, cp.ListPagesWalked = probeReads(ctx, engines[0], recentKey.Load().(string))
		st := dbs.sample(ctx)
		cp.StepsDiskMB = float64(st.StepsBytes) / (1 << 20)
		cp.FlowsDiskMB = float64(st.FlowsBytes) / (1 << 20)
		report.Checkpoints = append(report.Checkpoints, cp)
		fmt.Printf("%-7.0f %11d %10d %12.0f %10.1f %10.1f %10.1f %9.1f %9.1f\n",
			cp.TSec, cp.StepRows, cp.FlowRows, cp.FillStepsPerSec,
			cp.HistoryMs, cp.ListFirstMs, cp.ListDeepMs, cp.StepsDiskMB, cp.FlowsDiskMB)

		lastRows, lastT = steps, now
		for next <= steps {
			next += checkpointEvery
		}
		if steps >= targetSteps {
			break
		}
	}

	stop.Store(true)
	submitters.Wait()
	report.DurationSec = time.Since(start).Seconds()
	report.FlowsSubmitted = flows.Load()
	report.Errors = errCount.Load()

	// Retention at size, in two separately-measured halves: Purge only STAMPS delete_after_ms (bounded by
	// purgeCap, 4096 roots per call), and the reaper's own ticker does the set-based delete afterwards.
	before, _ := rowCounts(ctx, engines[0])
	pctx, pcancel := context.WithTimeout(ctx, 2*time.Minute)
	markStart := time.Now()
	marked, err := engines[0].Purge(pctx, workflow.Query{Status: workflow.StatusCompleted, Limit: 4096})
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
		cur, _ := rowCounts(ctx, engines[0])
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
	after, _ := rowCounts(ctx, engines[0])
	report.ReapedRows = before - after
	report.ReapSec = time.Since(reapStart).Seconds()
	if report.ReapSec > 0 {
		report.ReapRowsPerSec = float64(report.ReapedRows) / report.ReapSec
	}
	fmt.Printf("reaper: deleted %d rows in %.0fs (%.0f rows/s)\n",
		report.ReapedRows, report.ReapSec, report.ReapRowsPerSec)
	return report
}

// rowCounts sums live step/flow rows across shards via ShardInfo (dialect-agnostic, unlike the catalog
// queries the disk sampler uses).
func rowCounts(ctx context.Context, eng *engine.Engine) (steps, flows int) {
	sctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	infos, err := eng.ShardInfo(sctx)
	if err != nil {
		return 0, 0
	}
	for _, s := range infos {
		steps += s.Steps
		flows += s.Flows
	}
	return steps, flows
}

// probeReads times the read paths at the current volume: History on one real flow, the first List page,
// and a page reached by walking the cursor (keyset pagination should make the two indistinguishable).
func probeReads(ctx context.Context, eng *engine.Engine, flowKey string) (historyMs, listFirstMs, listDeepMs float64, pages int) {
	pctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	if flowKey != "" {
		t := time.Now()
		if _, err := eng.History(pctx, flowKey); err == nil {
			historyMs = float64(time.Since(t)) / float64(time.Millisecond)
		}
	}

	t := time.Now()
	_, cursor, err := eng.List(pctx, workflow.Query{Limit: 100})
	if err != nil {
		return historyMs, 0, 0, 0
	}
	listFirstMs = float64(time.Since(t)) / float64(time.Millisecond)

	// Walk the cursor to a depth that would expose an OFFSET-shaped cost, timing only the last page.
	const walk = 50
	for pages = 1; pages < walk && cursor != ""; pages++ {
		t = time.Now()
		var summaries []workflow.FlowSummary
		summaries, cursor, err = eng.List(pctx, workflow.Query{Limit: 100, Cursor: cursor})
		listDeepMs = float64(time.Since(t)) / float64(time.Millisecond)
		if err != nil || len(summaries) == 0 {
			break
		}
	}
	return historyMs, listFirstMs, listDeepMs, pages
}
