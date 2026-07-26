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

// A workload is one benchmark graph plus the knowledge needed to drive it. The workload's name is the key it is
// registered under, not a field on it.
type workload struct {
	graphURL     string
	initialState func() map[string]any
}

const (
	stateSteps = 5
	// linearSteps is the DEFAULT length of the linear chain; -linear-steps overrides it.
	//
	// Chain depth is not a cosmetic knob under cross-replica partitioning. A step's residue class is
	// effectively independent per hop (step ids interleave across concurrent flows), so the chance a
	// depth-D flow never touches a given replica's class is ((R-1)/R)^D - 30% at D=3 and R=3, but 1.7% at
	// D=10. A replica that is slow rather than dead therefore blocks a share of FLOWS that rises with
	// depth, while blocking a share of STEPS that does not. Sweeping this is how the two are told apart.
	linearSteps = 10
)

// registerWorkloads builds every benchmark graph and task into the host and returns the workloads by name.
//
//   - linear: a 10-step chain of no-op tasks - the steps/s floor (pure dispatch + transaction cost).
//   - fanout: forEach over 16 elements converging on a fan-in - cohort accounting + flow-row contention.
//   - state:  a 5-step chain where every task rewrites a payload of -payload bytes - MB/s throughput.
//   - mixed:  chosen per flow by the load generator (70% linear / 20% fanout / 10% state).
//
// fanOutWidth is the forEach branch count, and it is the ONLY knob that decouples the pending-step
// backlog from the submitter concurrency: a linear flow holds exactly one pending-or-running step at a
// time, so a closed-loop generator's backlog can never exceed its concurrency, whereas a fan-out puts
// `width` steps pending the instant its spawn completes. That is what lets a closed-loop harness reach
// the deep-backlog regime the refiller's cache bound and scan floor were designed for.
func registerWorkloads(h *benchHost, payloadBytes, fanOutWidth, chainSteps int) map[string]*workload {
	if chainSteps < 1 {
		chainSteps = linearSteps
	}
	nop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	h.tasks["bench/nop"] = nop

	// linear
	linear := workflow.NewGraph("Linear")
	names := make([]string, 0, chainSteps+1)
	for i := range chainSteps {
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

	// llm: ONE step per flow, so a flow's wall time is exactly the -task-delay. The multi-step workloads
	// above multiply the delay by their chain length, which makes a minutes-long task unmeasurable in any
	// sane window. This is the shape that validates the derived worker ceiling: the pool must grow past
	// its (connection-derived) resident set to keep thousands of minutes-long tasks in flight at once.
	llm := workflow.NewGraph("LLM")
	llm.SetEndpoint("Call", "bench/nop") // the delay lives in the host's ExecuteTask (-task-delay)
	llm.AddTransition("Call", workflow.END)
	h.graphs["bench/llm"] = llm

	// interrupt: an entry gate that interrupts once (the soak's load generator resumes it), then
	// proceeds to a no-op. Registered here (not lazily) so the shared registry is never mutated after
	// the engines start. Not a sweep workload - the soak references "bench/interrupt" directly.
	h.tasks["bench/gate"] = func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(nil, nil)
		return err
	}
	interrupt := workflow.NewGraph("Interrupt")
	interrupt.SetEndpoint("Gate", "bench/gate")
	interrupt.SetEndpoint("Work", "bench/nop")
	interrupt.AddTransitionChain("Gate", "Work", workflow.END)
	h.graphs["bench/interrupt"] = interrupt

	return map[string]*workload{
		"llm": {
			graphURL:     "bench/llm",
			initialState: func() map[string]any { return nil },
		},
		"linear": {
			graphURL:     "bench/linear",
			initialState: func() map[string]any { return nil },
		},
		"fanout": {
			graphURL:     "bench/fanout",
			initialState: func() map[string]any { return map[string]any{"items": fanoutItems} },
		},
		"state": {
			graphURL:     "bench/state",
			initialState: func() map[string]any { return nil },
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
		return nil, fmt.Errorf("unknown workload %q (linear, fanout, state, llm, mixed)", name)
	}
	return func() *workload { return w }, nil
}
