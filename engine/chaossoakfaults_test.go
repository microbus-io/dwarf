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
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestChaosSoak_Faults layers random test-only fault injection over the chaos soak: alongside the
// random lifecycle operations (Cancel/Resume/Fork/Delete/DeliverSignal) a fault goroutine continuously arms
// random recovery faults - transition/completion-commit failures, lock contention, stale-lease zombie writes,
// dropped wakes, a lost subgraph revive, a refiller scan error (and, under longsoak, a mid-tree reaper abort)
// - scoped to random live task names. This exercises *recovery interacting with chaos*, the surface where the
// sharpest residue bugs hide (deferred-deletion and lease-fence races).
//
// The faults are all cleared before the drain, so termination is deterministic: after draining every flow to
// terminal, the same asserts as the fault-free soak must hold - every Await returns (no permanent wedge), the
// structural invariants are clean, and the always-on dwarf_steps_unwedged alarm reads zero. The alarm stays a
// meaningful zero because the wedge sweep keeps its default 5m cadence/age-guard, so a fault that wedges a step
// (e.g. subgraphReviveLost, leaseStaleWrite) is terminalized by the drain Cancel, never papered over by the
// sweep within the window. Seed is logged (DWARF_SOAK_SEED overrides); DWARF_LONGSOAK=1 lengthens the window
// and raises fault density.
func TestChaosSoak_Faults(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	longsoak := os.Getenv("DWARF_LONGSOAK") != ""

	seed := int64(20260704)
	if s := os.Getenv("DWARF_SOAK_SEED"); s != "" {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			seed = v
		}
	}
	t.Logf("fault chaos soak seed=%d longsoak=%v (set DWARF_SOAK_SEED to reproduce, DWARF_LONGSOAK=1 for the heavy run)", seed, longsoak)
	rng := newLockedRNG(seed)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	proxy := NewTestProxy()
	registerChaosGraphs(t, proxy)

	eng := NewEngineUnderTest(t)
	assert.NoError(eng.SetWorkers(8))
	eng.SetHost(proxy)
	eng.SetMeterProvider(mp)
	assert.NoError(eng.Startup(t.Context()))

	shapes := []string{
		"chaossoak.verify:428/linear",
		"chaossoak.verify:428/fanout",
		"chaossoak.verify:428/parent",
	}

	flows := 40
	window := 4 * time.Second
	faultInterval := 40 * time.Millisecond
	if longsoak {
		flows = 60
		window = 25 * time.Second
		faultInterval = 12 * time.Millisecond
	}

	var mu sync.Mutex
	live := make([]string, 0, flows)
	stepKeys := make([]string, 0)
	addFlow := func(k string) { mu.Lock(); live = append(live, k); mu.Unlock() }
	randFlow := func() string {
		mu.Lock()
		defer mu.Unlock()
		if len(live) == 0 {
			return ""
		}
		return live[rng.intn(len(live))]
	}
	randStep := func() string {
		mu.Lock()
		defer mu.Unlock()
		if len(stepKeys) == 0 {
			return ""
		}
		return stepKeys[rng.intn(len(stepKeys))]
	}
	recordSteps := func(flowKey string) {
		hist, err := eng.History(ctx, flowKey)
		if err != nil {
			return
		}
		mu.Lock()
		for _, s := range hist {
			if s.StepKey != "" {
				stepKeys = append(stepKeys, s.StepKey)
			}
		}
		mu.Unlock()
	}

	for i := range flows {
		var st map[string]any
		url := shapes[i%len(shapes)]
		if url == "chaossoak.verify:428/parent" {
			st = map[string]any{"mode": rng.intn(2)}
		}
		k, err := eng.Create(ctx, url, st, nil)
		if !assert.NoError(err) {
			return
		}
		addFlow(k)
	}

	// The fault repertoire: task-scoped commit/lease faults + process-wide wake/revive/scan faults. reapMidTree
	// is longsoak-only (the reaper keeps its default 1m tick, so it rarely runs in the short window anyway).
	type faultSpec struct {
		name   string
		scoped bool // append a random task node name as the scope
	}
	faultSpecs := []faultSpec{
		{faultTransitionCommit, true},
		{faultContention, true},
		{faultLeaseStaleWrite, true},
		{faultCompleteFlowCommit, false},
		{faultDropSignalStop, false},
		{faultDropDoorbell, false},
		{faultSubgraphReviveLost, false},
		{faultRefillScanErr, false},
	}
	if longsoak {
		faultSpecs = append(faultSpecs, faultSpec{faultReapMidTree, false})
	}
	// The graph-node names across the three chaos shapes (the scope for task-scoped faults).
	taskNodes := []string{"A", "B", "C", "D", "S", "P", "Q", "R", "J", "P0", "CA", "CB", "CC"}

	var armedMu sync.Mutex
	armed := map[string]string{}
	armRandomFault := func() {
		fs := faultSpecs[rng.intn(len(faultSpecs))]
		armedMu.Lock()
		if fs.scoped {
			eng.seams.Inject(fs.name, taskNodes[rng.intn(len(taskNodes))])
			armed[fs.name] = taskNodes[rng.intn(len(taskNodes))]
		} else {
			eng.seams.Inject(fs.name)
			armed[fs.name] = ""
		}
		armedMu.Unlock()
	}

	stop := make(chan struct{})
	var chaosWG sync.WaitGroup

	// Two lifecycle-chaos goroutines (as in the fault-free soak).
	for range 2 {
		chaosWG.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := randFlow()
				if k != "" {
					// Bound every chaos op: under the heavy fault contention a stuck engine call must fail fast,
					// never block this goroutine (and thus chaosWG.Wait) indefinitely. An op erroring on the
					// deadline is a legitimate chaos outcome, so all returns are discarded as elsewhere.
					opCtx, opCancel := context.WithTimeout(ctx, 15*time.Second)
					switch rng.intn(8) {
					case 0:
						_ = eng.Cancel(opCtx, k, "chaos")
					case 1:
						_ = eng.Resume(opCtx, k, map[string]any{"chaos": true})
					case 2:
						_, _ = eng.Snapshot(opCtx, k)
					case 3:
						recordSteps(k)
					case 4:
						if sk := randStep(); sk != "" {
							if fk, err := eng.Fork(opCtx, sk, nil); err == nil {
								addFlow(fk)
							}
						}
					case 5:
						_ = eng.Delete(opCtx, k)
					case 6:
						chaosEnqueue(opCtx, eng, k)
					case 7:
						_, _ = eng.History(opCtx, k)
					}
					opCancel()
				}
				time.Sleep(time.Duration(rng.intn(3)+1) * time.Millisecond)
			}
		})
	}

	// One fault-arming goroutine, running throughout the chaos window.
	chaosWG.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			armRandomFault()
			time.Sleep(faultInterval)
		}
	})

	time.Sleep(window)
	close(stop)
	chaosWG.Wait()

	// Disarm every fault before draining, so termination is deterministic (no leftover fault fires mid-drain).
	armedMu.Lock()
	for name, scope := range armed {
		if scope != "" {
			eng.seams.Withdraw(name, scope)
		} else {
			eng.seams.Withdraw(name)
		}
	}
	armedMu.Unlock()

	// Drive every known flow to terminal, then assert each Await returns (a timeout is a wedge).
	mu.Lock()
	all := append([]string(nil), live...)
	mu.Unlock()
	for _, k := range all {
		_ = eng.Cancel(ctx, k, "drain")
	}
	// A List sweep catches any non-terminal root not in `live`.
	for _, st := range []string{workflow.StatusRunning, workflow.StatusInterrupted} {
		summaries, _, err := eng.List(ctx, workflow.Query{Status: st, Limit: 200})
		if err == nil {
			for _, s := range summaries {
				_ = eng.Cancel(ctx, s.FlowKey, "drain")
			}
		}
	}
	for _, k := range all {
		awaitCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		_, err := eng.Await(awaitCtx, k)
		cancel()
		if err != nil && errors.StatusCode(err) == 0 {
			t.Errorf("Await(%s) did not return a stopped flow: %v", k, err)
		}
	}

	// The workload has quiesced: structural invariants clean and the wedge alarm silent (armed wedges were
	// terminalized by the drain Cancel, not papered over by the still-5m-cadence sweep).
	assertInvariants(t, eng)
	var rm metricdata.ResourceMetrics
	if assert.NoError(reader.Collect(ctx, &rm)) {
		if unwedged, ok := sumCounter(rm, "dwarf_steps_unwedged", "", ""); ok {
			assert.Equal(int64(0), unwedged, "dwarf_steps_unwedged fired: a fault drove a step into a genuinely-wedged state the drain missed")
		}
	}
}

// lockedRNG is a tiny mutex-guarded RNG so the lifecycle- and fault-chaos goroutines share one seeded stream
// (reproducible under a fixed DWARF_SOAK_SEED).
type lockedRNG struct {
	mu  sync.Mutex
	src *rand.Rand
}

func newLockedRNG(seed int64) *lockedRNG { return &lockedRNG{src: rand.New(rand.NewSource(seed))} }
func (r *lockedRNG) intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.src.Intn(n)
}
