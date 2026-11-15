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
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestCohortWidth_Measure times one wide forEach's arrivals: how long the fan-in waits after the last branch's task
// returned, which is the time the cohort takes to absorb the arrivals still queued. A measurement, not a pin: it
// runs only when DWARF_TESTING_COHORT_WIDTH names the width.
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

	start := time.Now()
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	_, out, err := e.Run(awaitCtx, "width/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, out.Status)
	total := time.Since(start)
	branches := time.Duration(lastBranch.Load() - firstBranch.Load())
	drain := time.Duration(joinAt.Load() - lastBranch.Load())
	driver := ""
	if sh, err := e.db.Shard(1); err == nil {
		driver = sh.DB().DriverName()
	}
	t.Logf("width=%d driver=%s total=%v branches-ran-over=%v join-after-last-branch=%v (%.0f arrivals/s overall)",
		width, driver, total.Round(time.Millisecond), branches.Round(time.Millisecond), drain.Round(time.Millisecond),
		float64(width)/time.Duration(joinAt.Load()-firstBranch.Load()).Seconds())
}
