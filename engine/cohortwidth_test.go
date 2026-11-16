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
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCohortWidth_Measure times one wide forEach's arrivals: how long the fan-in waits after the last branch's task
// returned, which is the time the cohort takes to absorb the arrivals still queued, and the most workers the crew
// held meanwhile. A measurement, not a pin: it runs only when DWARF_TESTING_COHORT_WIDTH names the width, and
// DWARF_TESTING_COHORT_HOLD=0 runs it with the fan-in hold off, as the control.
func TestCohortWidth_Measure(t *testing.T) {
	width, _ := strconv.Atoi(os.Getenv("DWARF_TESTING_COHORT_WIDTH"))
	if width <= 0 {
		t.Skip("set DWARF_TESTING_COHORT_WIDTH to measure")
	}
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("CohortWidth")
	g.SetURL("Seed", "width/seed")
	g.SetURL("Branch", "width/branch")
	g.SetURL("Join", "width/join")
	g.SetFanIn("Join")
	g.AddTransitionForEach("Seed", "Branch", "items", "item")
	g.AddTransitionChain("Branch", "Join", workflow.End)
	mux.HandleGraph("width/g", g)
	mux.HandleTask("width/seed", func(ctx context.Context, f *workflow.Flow) error {
		items := make([]int, width)
		for i := range items {
			items[i] = i
		}
		f.Set("items", items)
		return nil
	})
	var lastBranch, firstBranch atomic.Int64
	mux.HandleTask("width/branch", func(ctx context.Context, f *workflow.Flow) error {
		now := time.Now().UnixNano()
		firstBranch.CompareAndSwap(0, now)
		lastBranch.Store(now)
		return nil
	})
	var joinAt atomic.Int64
	mux.HandleTask("width/join", func(ctx context.Context, f *workflow.Flow) error {
		joinAt.CompareAndSwap(0, time.Now().UnixNano())
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))
	holdOff := os.Getenv("DWARF_TESTING_COHORT_HOLD") == "0"
	if holdOff {
		e.fanInHoldAt.Store(0)
	}
	// Sampled, so the peaks are a lower bound: a peak shorter than the interval can be missed.
	var crewPeak, heapPeak atomic.Int64
	sampled := make(chan struct{})
	stopSampling := make(chan struct{})
	go func() {
		defer close(sampled)
		var ms runtime.MemStats
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for n := 0; ; n++ {
			select {
			case <-stopSampling:
				return
			case <-tick.C:
			}
			if holdOff {
				e.fanInHoldAt.Store(0) // a pool recompute re-derives it
			}
			if r := int64(e.crew.Resident()); r > crewPeak.Load() {
				crewPeak.Store(r)
			}
			if n%20 == 0 {
				runtime.ReadMemStats(&ms)
				if h := int64(ms.HeapInuse); h > heapPeak.Load() {
					heapPeak.Store(h)
				}
			}
		}
	}()

	start := time.Now()
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	_, out, err := e.Run(awaitCtx, "width/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	total := time.Since(start)
	close(stopSampling)
	<-sampled
	branches := time.Duration(lastBranch.Load() - firstBranch.Load())
	drain := time.Duration(joinAt.Load() - lastBranch.Load())
	driver := ""
	if sh, err := e.db.Shard(1); err == nil {
		driver = sh.DB().DriverName()
	}
	t.Logf("width=%d driver=%s hold=%v total=%v branches-ran-over=%v join-after-last-branch=%v (%.0f arrivals/s overall) crew-peak=%d (resident %d, max %d, hold at %d) heap-peak=%dMB",
		width, driver, !holdOff, total.Round(time.Millisecond), branches.Round(time.Millisecond), drain.Round(time.Millisecond),
		float64(width)/time.Duration(joinAt.Load()-firstBranch.Load()).Seconds(),
		crewPeak.Load(), e.crew.Min(), e.crew.Max(), e.fanInHoldAt.Load(), heapPeak.Load()>>20)
}
