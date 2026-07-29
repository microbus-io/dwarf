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
//   - carry:  the same payload written ONCE and then only carried - the state-ref shape (see below).
//   - carryfanout: that payload carried across a fan-out's branches - the N x D case.
//   - mixed:  chosen per flow by the load generator (70% linear / 20% fanout / 10% state).
//
// fanOutWidth is the forEach branch count, and it is the ONLY knob that decouples the pending-step
// backlog from the submitter concurrency: a linear flow holds exactly one pending-or-running step at a
// time, so a closed-loop generator's backlog can never exceed its concurrency, whereas a fan-out puts
// `width` steps pending the instant its spawn completes. That is what lets a closed-loop harness reach
// the deep-backlog regime the refiller's cache bound and scan floor were designed for.
//
// carryReadsDoc makes every carry task READ the carried document rather than merely pass it along. It is
// the arm switch for any question about how carried state is HELD (as against stored): a task that never
// reads a field is the case an engine can decline to materialize, and a task that reads every field is the
// case where it cannot. Both arms store identically, so a storage measurement should not vary with it - if
// one does, the workload is not carrying what it thinks it is.
func registerWorkloads(h *benchHost, payloadBytes, fanOutWidth, chainSteps int, carryReadsDoc bool) map[string]*workload {
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

	// The carry payload is STRUCTURED, and that is not a detail - it decides whether the workload can see
	// the thing it exists to measure. `state`'s payload is one opaque incompressible string, which is right
	// for MB/s (it must not TOAST-compress), and wrong here: a JSON string decodes to a Go string of about
	// the same size, so an opaque blob has a decode expansion of ~1x and a carried-state measurement taken
	// with one reads flat no matter what the engine does. Measured with `go run` against encoding/json:
	// a 64KB string expands ~1x, while 800 small records (50KB of JSON) expand to 341KB in Go - 6.8x, since
	// every element costs an interface, a boxed float64 and a map entry. Real carried documents - pages,
	// rows, chunks - are the second shape. Element count, not byte count, is what drives it.
	records := make([]map[string]any, 0, 1+payloadBytes/80)
	for i := 0; len(records)*80 < payloadBytes; i++ {
		records = append(records, map[string]any{
			"id":    i,
			"name":  payload[(i*37)%max(1, len(payload)-24):][:24],
			"score": float64(i) * 1.5,
			"ok":    i%2 == 0,
		})
	}

	// carry: the payload is written ONCE - as the flow's initial state, so the ENTRY step's state column
	// anchors it - and thereafter only carried. Every task writes a small counter and leaves the document
	// alone. This is the complement of `state` above and measures the opposite thing: `state` rewrites the
	// payload every step, so every copy is a legitimate change and nothing can ever be stored by reference;
	// here D steps carry one document, which is the shape state refs exist for and the only shape in which
	// their win is visible at all. Read dwarf_state_write_bytes{column=state} against the task-declared
	// bytes below: without refs it is ~D x payload, with them ~1 x payload.
	//
	// The Has check is not decoration. A carried field silently vanishing past a fan-in is the exact failure
	// the ref carry-forward and the anchor pin exist to prevent, and it would otherwise present here as an
	// excellent byte number - the benchmark would be measuring data loss and reporting it as a saving. Has
	// reads no value, so the check costs nothing even in the never-read arm.
	// The task declares almost no write volume of its own, deliberately - so `bytesWritten` (and the MB/s
	// it feeds) is NOT this workload's metric and is left untouched. The whole point is that the bytes the
	// ENGINE writes are large while the bytes the TASK writes are ~zero; reading the engine's counter
	// against a task-declared total that included the document would hide exactly that gap.
	//
	// `steps` is written as a DELTA (always 1), never as an accumulated total, because the fan-out variant
	// below folds it with ReducerAdd - the delta convention every reducer-managed field obeys. Writing
	// state+1 here would double-count on every fan-in.
	carryTask := func(ctx context.Context, f *workflow.Flow) error {
		if !f.Has("doc") {
			return fmt.Errorf("carry: the carried document is missing at %s - a dropped carry, not a saving", f.StepKey())
		}
		if carryReadsDoc {
			// Reading it is what forces the decode, which is the whole point of the -carry-read arm: the
			// never-read arm should hold the document's BYTES and this one its expanded Go form.
			var doc []map[string]any
			if err := f.Get("doc", &doc); err != nil {
				return fmt.Errorf("carry: reading the carried document: %w", err)
			}
			if len(doc) != len(records) {
				return fmt.Errorf("carry: carried document has %d records, want %d", len(doc), len(records))
			}
		}
		f.SetInt("steps", 1)
		return nil
	}
	h.tasks["bench/carry"] = carryTask

	carry := workflow.NewGraph("Carry")
	carryNames := make([]string, 0, chainSteps+1)
	for i := range chainSteps {
		n := fmt.Sprintf("C%d", i)
		carry.SetEndpoint(n, "bench/carry")
		carryNames = append(carryNames, n)
	}
	carryNames = append(carryNames, workflow.END)
	carry.AddTransitionChain(carryNames...)
	h.graphs["bench/carry"] = carry

	// carryfanout: the N x D case, which is where carrying gets expensive - a fan-out of N branches, each
	// two steps deep, all carrying the same document. Split is the ENTRY step, so it holds the document and
	// is the anchor its branches point at; the branches are multi-step so the cost is N x D copies rather
	// than N. Widen it with -fanout-width, the same knob the fanout workload uses.
	carryFanOut := workflow.NewGraph("CarryFanOut")
	carryFanOut.SetEndpoint("Split", "bench/carry")
	carryFanOut.SetEndpoint("Work", "bench/carry")
	carryFanOut.SetEndpoint("Enrich", "bench/carry")
	carryFanOut.SetEndpoint("Join", "bench/carry")
	carryFanOut.SetFanIn("Join")
	carryFanOut.SetReducer("steps", workflow.ReducerAdd)
	carryFanOut.AddTransitionForEach("Split", "Work", "items", "item")
	carryFanOut.AddTransitionChain("Work", "Enrich", "Join", workflow.END)
	h.graphs["bench/carryfanout"] = carryFanOut

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
		// The document rides in as INITIAL STATE rather than being written by a first task, which is what
		// makes the entry step its anchor - the case where a payload lives in a step's `state` column with
		// no task having produced it, and so appears in no `changes` anywhere.
		"carry": {
			graphURL:     "bench/carry",
			initialState: func() map[string]any { return map[string]any{"doc": records} },
		},
		"carryfanout": {
			graphURL: "bench/carryfanout",
			initialState: func() map[string]any {
				return map[string]any{"doc": records, "items": fanoutItems}
			},
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
		return nil, fmt.Errorf("unknown workload %q (linear, fanout, state, carry, carryfanout, llm, mixed)", name)
	}
	return func() *workload { return w }, nil
}
