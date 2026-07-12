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
	"math/rand/v2"

	"github.com/microbus-io/dwarf/workflow"
)

// A workload is one benchmark graph plus the knowledge needed to drive and account for it.
type workload struct {
	name         string
	graphURL     string
	initialState func() map[string]any
	stepsPerFlow int // expected steps per flow, to sanity-check the metric-based steps/s
}

const (
	fanOutWidth = 16
	stateSteps  = 5
	linearSteps = 10
)

// registerWorkloads builds every benchmark graph and task into the host and returns the workloads by name.
//
//   - linear: a 10-step chain of no-op tasks - the steps/s floor (pure dispatch + transaction cost).
//   - fanout: forEach over 16 elements converging on a fan-in - cohort accounting + flow-row contention.
//   - state:  a 5-step chain where every task rewrites a payload of -payload bytes - MB/s throughput.
//   - mixed:  chosen per flow by the load generator (70% linear / 20% fanout / 10% state).
func registerWorkloads(h *benchHost, payloadBytes int) map[string]*workload {
	nop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	h.tasks["bench/nop"] = nop

	// linear
	linear := workflow.NewGraph("Linear")
	names := make([]string, 0, linearSteps+1)
	for i := range linearSteps {
		n := fmt.Sprintf("T%d", i)
		linear.SetEndpoint(n, "bench/nop")
		names = append(names, n)
	}
	names = append(names, workflow.END)
	linear.AddTransitionChain(names...)
	h.graphs["bench/linear"] = linear

	// fanout
	fanout := workflow.NewGraph("FanOut")
	fanout.SetEndpoint("Split", "bench/nop")
	fanout.SetEndpoint("Work", "bench/nop")
	fanout.SetEndpoint("Join", "bench/nop")
	fanout.SetFanIn("Join")
	fanout.AddTransitionForEach("Split", "Work", "items", "item")
	fanout.AddTransitionChain("Work", "Join", workflow.END)
	h.graphs["bench/fanout"] = fanout
	fanoutItems := make([]int, fanOutWidth)
	for i := range fanoutItems {
		fanoutItems[i] = i
	}

	// state: every step rewrites the payload so each step row carries the full write. The payload must be
	// incompressible: a repeated-character payload TOAST-compresses to ~nothing on the Postgres side, so
	// MB/s would measure serialization+network while the storage/WAL path idles - random alphanumerics
	// keep the on-disk and on-wire byte counts honest (alnum only, so JSON escaping doesn't inflate it).
	const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	payloadB := make([]byte, payloadBytes)
	for i := range payloadB {
		payloadB[i] = alnum[rand.IntN(len(alnum))]
	}
	payload := string(payloadB)
	h.tasks["bench/state"] = func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("data", payload)
		h.bytesWritten.Add(int64(len(payload)))
		return nil
	}
	state := workflow.NewGraph("State")
	stateNames := make([]string, 0, stateSteps+1)
	for i := range stateSteps {
		n := fmt.Sprintf("S%d", i)
		state.SetEndpoint(n, "bench/state")
		stateNames = append(stateNames, n)
	}
	stateNames = append(stateNames, workflow.END)
	state.AddTransitionChain(stateNames...)
	h.graphs["bench/state"] = state

	return map[string]*workload{
		"linear": {
			name:         "linear",
			graphURL:     "bench/linear",
			initialState: func() map[string]any { return nil },
			stepsPerFlow: linearSteps,
		},
		"fanout": {
			name:         "fanout",
			graphURL:     "bench/fanout",
			initialState: func() map[string]any { return map[string]any{"items": fanoutItems} },
			stepsPerFlow: 1 + fanOutWidth + 1,
		},
		"state": {
			name:         "state",
			graphURL:     "bench/state",
			initialState: func() map[string]any { return nil },
			stepsPerFlow: stateSteps,
		},
	}
}

// chooseWorkload resolves the -workload flag to a per-flow picker. "mixed" draws 70% linear / 20% fanout /
// 10% state per flow; any other name is a fixed pick.
func chooseWorkload(workloads map[string]*workload, name string) (func() *workload, error) {
	if name == "mixed" {
		linear, fanout, state := workloads["linear"], workloads["fanout"], workloads["state"]
		return func() *workload {
			r := rand.IntN(10)
			switch {
			case r < 7:
				return linear
			case r < 9:
				return fanout
			default:
				return state
			}
		}, nil
	}
	w := workloads[name]
	if w == nil {
		return nil, fmt.Errorf("unknown workload %q (linear, fanout, state, mixed)", name)
	}
	return func() *workload { return w }, nil
}
