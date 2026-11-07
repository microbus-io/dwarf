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
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync"

	"github.com/microbus-io/dwarf/workflow"
)

// A workload is one benchmark graph plus the knowledge needed to drive it. The workload's name is the key it is
// registered under, not a field on it.
type workload struct {
	graphURL     string
	initialState func() map[string]any
	// stepsPerFlow is the steps one flow of this workload executes - its graph's shape. It converts a
	// throughput in steps/s into flows/s; an open-loop window's own flowsPerSec is the ADMISSION rate, which
	// runs ahead of completion by up to -max-outstanding flows and is no measure of what was served.
	stepsPerFlow int
	// check, when set, verifies a completed flow's outcome; a flow that fails it counts as an error, which
	// invalidates the run. It is how a workload whose tasks cannot see their own last write checks it landed.
	check func(*workflow.FlowOutcome) error
	// class names the -mix class a workload was registered for ("linear:1K"), empty outside a mix.
	class string
	// holdURL is the graph that runs this workload's flow as its subgraph and then holds the tree open
	// (-volume-side live).
	holdURL string
	// build builds the payload the workload's tasks write or carry, nil for none. It runs once, before any
	// window (prepare): built by the first task instead, it blocks every concurrent writer on the build -
	// ~10-20s for the 4GB a -payload 250MB -fanout-width 8 run needs, inside a short warmup's window.
	build func()
}

// prepare builds the workload's payload ahead of the run that uses it.
func (w *workload) prepare() {
	if w != nil && w.build != nil {
		w.build()
	}
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
	// defaultStatePayload is what `state` and `carry` write when -payload is 0. Those two exist to move a
	// payload, so 0 cannot mean "none" for them the way it does for linear, fanout and scatter.
	defaultStatePayload = 64 * 1024
	// defaultLoopMessage is the size of the message each `loop`/`loopreplace` step adds when -payload is 0.
	defaultLoopMessage = 1024
	// loopMessageOverhead is a generous bound on the JSON a loop message adds around its content (its index,
	// role and keys), for the up-front check of the final history against maxFinalState.
	loopMessageOverhead = 64
	// maxFinalState is the engine's per-value limit, which a flow's final state must fit in.
	maxFinalState = 256_000_000
)

// effectivePayload is the payload size a workload writes for a -payload value.
func effectivePayload(workload string, payloadBytes int) int {
	switch workload {
	case "state", "carry", "carryfanout":
		if payloadBytes == 0 {
			return defaultStatePayload
		}
	case "loop", "loopreplace":
		if payloadBytes == 0 {
			return defaultLoopMessage
		}
	}
	return payloadBytes
}

// freshBytes hands out payloads of a fixed size cut from one pre-generated random buffer at a random offset,
// so every write is new data without generating it per task - at several thousand steps/s, generating each
// payload would load the engine host the run is measuring. The buffer is incompressible alphanumerics, as
// `state`'s payload is, so TOAST compression cannot hide the cost; alphanumerics also need no JSON escaping,
// so the bytes written are the bytes declared.
type freshBytes struct {
	buf  string
	size int
}

// newFreshBytes builds the buffer for payloads of size bytes, with room for perValue of them cut side by
// side at a random offset.
func newFreshBytes(size, perValue int) *freshBytes {
	const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, max(2*perValue*size, 64*1024))
	for i := range b {
		b[i] = alnum[rand.IntN(len(alnum))]
	}
	return &freshBytes{buf: string(b), size: size}
}

// next returns a payload of the configured size. Substrings share the buffer, so handing one out allocates
// nothing; the copy happens where the task's write serializes it.
func (f *freshBytes) next() string {
	return f.nextN(1)[0]
}

// nextN returns n payloads that are consecutive, non-overlapping cuts of the buffer, for n values stored in
// ONE column value - scatter's array. Overlapping cuts would repeat bytes inside that value, which is
// exactly what TOAST compression finds and hides. Values in different rows may overlap: each is compressed
// on its own. n must not exceed the perValue the buffer was built for; tasks call this concurrently, so the
// buffer never changes after construction.
func (f *freshBytes) nextN(n int) []string {
	off := rand.IntN(len(f.buf) - n*f.size + 1)
	out := make([]string, n)
	for i := range out {
		out[i] = f.buf[off+i*f.size : off+(i+1)*f.size]
	}
	return out
}

// registerWorkloads builds every benchmark graph and task into the host and returns the workloads by name. ns
// prefixes every graph and task URL under "bench/" (empty for the run's own set, "m1/" and so on for -mix classes).
//
//   - linear: a 10-step chain - the steps/s floor (pure dispatch + transaction cost). With -payload, every
//     task writes fresh bytes of that size to `data`, or with payloadJSON a compressible JSON document of about
//     that size.
//   - fanout: forEach over -fanout-width elements converging on a fan-in - cohort accounting + flow-row
//     contention. With -payload, Split, every branch and Join each write fresh bytes to `data`, the same
//     per-step write as linear, so at equal steps per flow the only difference between the two is the shape.
//   - scatter: Split writes `items`, -fanout-width elements of -payload bytes (with payloadJSON, JSON documents
//     of about that size); each branch, -branch-depth steps deep, reads its element and writes nothing; Join is
//     a no-op. The doc -> pages shape, where each branch carries one element of an array.
//   - state:  a 5-step chain where every task rewrites a payload of -payload bytes - MB/s throughput.
//   - carry:  the same payload written ONCE and then only carried - the state-ref shape (see below).
//   - carryfanout: that payload carried across a fan-out's branches - the N x D case.
//   - loop: an agent loop - a -linear-steps chain where every step adds one message of -payload bytes to
//     `messages`, wired to ReducerAppend: each task writes only its message.
//   - loopreplace: the same loop with no reducer on `messages`: each task writes the whole history plus its
//     message, as a loop written before reducers does.
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
func registerWorkloads(h *benchHost, ns string, payloadBytes int, payloadJSON bool, fanOutWidth, chainSteps, branchDepth int, carryReadsDoc bool) map[string]*workload {
	// pre namespaces every graph and task URL, so -mix can register one set per payload and shape side by side.
	pre := "bench/" + ns
	if chainSteps < 1 {
		chainSteps = linearSteps
	}
	if branchDepth < 1 {
		branchDepth = 1
	}
	nop := func(ctx context.Context, f *workflow.Flow) error { return nil }
	h.tasks[pre+"nop"] = nop

	// The payloads below are built for the workloads the run picks (workload.prepare), not here. Every
	// workload is registered whatever the run picks, and a payload built eagerly is resident for the whole
	// run: at -payload 250MB that is a 500MB fresh buffer (x -fanout-width), the 250MB state payload and ~2GB
	// of unused carry records, which would read as engine memory in exactly the run that measures it.
	fresh := sync.OnceValue(func() *freshBytes { return newFreshBytes(payloadBytes, max(1, fanOutWidth)) })
	docs := sync.OnceValue(func() *jsonPayloads { return newJSONPayloads(payloadBytes) })
	buildFresh := func() {
		if payloadBytes > 0 {
			fresh()
		}
	}
	buildWrite := func() {
		if payloadBytes > 0 && payloadJSON {
			docs()
		} else {
			buildFresh()
		}
	}

	// write: the linear and fanout task. -payload 0 makes it a no-op, the dispatch-cost floor.
	h.tasks[pre+"write"] = func(ctx context.Context, f *workflow.Flow) error {
		switch {
		case payloadBytes > 0 && payloadJSON:
			doc := docs().next()
			if err := f.Set("data", doc); err != nil {
				return err
			}
			h.bytesWritten.Add(int64(len(doc)))
		case payloadBytes > 0:
			f.SetString("data", fresh().next())
			h.bytesWritten.Add(int64(payloadBytes))
		}
		return nil
	}

	// linear
	linear := workflow.NewGraph("Linear")
	names := make([]string, 0, chainSteps+1)
	for i := range chainSteps {
		n := fmt.Sprintf("T%d", i)
		linear.SetURL(n, pre+"write")
		names = append(names, n)
	}
	names = append(names, workflow.End)
	linear.AddTransitionChain(names...)
	h.graphs[pre+"linear"] = linear

	// fanout
	fanout := workflow.NewGraph("FanOut")
	fanout.SetURL("Split", pre+"write")
	fanout.SetURL("Work", pre+"write")
	fanout.SetURL("Join", pre+"write")
	fanout.SetFanIn("Join")
	fanout.AddTransitionForEach("Split", "Work", "items", "item")
	fanout.AddTransitionChain("Work", "Join", workflow.End)
	h.graphs[pre+"fanout"] = fanout
	fanoutItems := make([]int, fanOutWidth)
	for i := range fanoutItems {
		fanoutItems[i] = i
	}

	// scatter: Split writes the array the branches fan out over, fresh per flow; each branch step reads its
	// element and checks it arrived whole. The check is what makes a storage number trustworthy here, as in
	// carry: an element that went missing would otherwise read as an excellent byte saving.
	// A JSON element is checked by its records rather than its bytes, which the database does not keep.
	jsonScatter := payloadJSON && payloadBytes > 0
	h.tasks[pre+"scatter-split"] = func(ctx context.Context, f *workflow.Flow) error {
		if jsonScatter {
			items := docs().nextN(fanOutWidth)
			if err := f.Set("items", items); err != nil {
				return err
			}
			for _, doc := range items {
				h.bytesWritten.Add(int64(len(doc)))
			}
			return nil
		}
		f.Set("items", fresh().nextN(fanOutWidth))
		h.bytesWritten.Add(int64(fanOutWidth * payloadBytes))
		return nil
	}
	h.tasks[pre+"scatter-read"] = func(ctx context.Context, f *workflow.Flow) error {
		if jsonScatter {
			var doc json.RawMessage
			if _, err := f.Get("item", &doc); err != nil {
				return fmt.Errorf("scatter: %s reading its element: %w", f.StepKey(), err)
			}
			if !docs().holds(doc) {
				return fmt.Errorf("scatter: %s sees an element that is none of the documents written (%d bytes)", f.StepKey(), len(doc))
			}
			return nil
		}
		if got := len(f.GetString("item")); got != payloadBytes {
			return fmt.Errorf("scatter: %s sees a %d-byte element, want %d", f.StepKey(), got, payloadBytes)
		}
		return nil
	}
	scatter := workflow.NewGraph("Scatter")
	scatter.SetURL("Split", pre+"scatter-split")
	scatter.SetURL("Join", pre+"nop")
	scatter.SetFanIn("Join")
	branch := make([]string, 0, branchDepth+2)
	for i := range branchDepth {
		n := fmt.Sprintf("B%d", i)
		scatter.SetURL(n, pre+"scatter-read")
		branch = append(branch, n)
	}
	scatter.AddTransitionForEach("Split", branch[0], "items", "item")
	scatter.AddTransitionChain(append(branch, "Join", workflow.End)...)
	h.graphs[pre+"scatter"] = scatter

	// state: every step rewrites the payload so each step row carries the full write. The payload must be
	// incompressible: a repeated-character payload TOAST-compresses to ~nothing on the Postgres side, so
	// MB/s would measure serialization+network while the storage/WAL path idles - random alphanumerics
	// keep the on-disk and on-wire byte counts honest (alnum only, so JSON escaping doesn't inflate it).
	statePayload := payloadBytes
	if statePayload == 0 {
		statePayload = defaultStatePayload
	}
	payload := sync.OnceValue(func() string {
		const alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, statePayload)
		for i := range b {
			b[i] = alnum[rand.IntN(len(alnum))]
		}
		return string(b)
	})
	h.tasks[pre+"state"] = func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("data", payload())
		h.bytesWritten.Add(int64(statePayload))
		return nil
	}
	state := workflow.NewGraph("State")
	stateNames := make([]string, 0, stateSteps+1)
	for i := range stateSteps {
		n := fmt.Sprintf("S%d", i)
		state.SetURL(n, pre+"state")
		stateNames = append(stateNames, n)
	}
	stateNames = append(stateNames, workflow.End)
	state.AddTransitionChain(stateNames...)
	h.graphs[pre+"state"] = state

	// The carry payload is STRUCTURED, and that is not a detail - it decides whether the workload can see
	// the thing it exists to measure. `state`'s payload is one opaque incompressible string, which is right
	// for MB/s (it must not TOAST-compress), and wrong here: a JSON string decodes to a Go string of about
	// the same size, so an opaque blob has a decode expansion of ~1x and a carried-state measurement taken
	// with one reads flat no matter what the engine does. Measured with `go run` against encoding/json:
	// a 64KB string expands ~1x, while 800 small records (50KB of JSON) expand to 341KB in Go - 6.8x, since
	// every element costs an interface, a boxed float64 and a map entry. Real carried documents - pages,
	// rows, chunks - are the second shape. Element count, not byte count, is what drives it.
	records := sync.OnceValue(func() []map[string]any {
		payload := payload()
		records := make([]map[string]any, 0, 1+statePayload/80)
		for i := 0; len(records)*80 < statePayload; i++ {
			records = append(records, map[string]any{
				"id":    i,
				"name":  payload[(i*37)%max(1, len(payload)-24):][:24],
				"score": float64(i) * 1.5,
				"ok":    i%2 == 0,
			})
		}
		return records
	})

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
			if _, err := f.Get("doc", &doc); err != nil {
				return fmt.Errorf("carry: reading the carried document: %w", err)
			}
			if len(doc) != len(records()) {
				return fmt.Errorf("carry: carried document has %d records, want %d", len(doc), len(records()))
			}
		}
		f.SetInt("steps", 1)
		return nil
	}
	h.tasks[pre+"carry"] = carryTask

	carry := workflow.NewGraph("Carry")
	carryNames := make([]string, 0, chainSteps+1)
	for i := range chainSteps {
		n := fmt.Sprintf("C%d", i)
		carry.SetURL(n, pre+"carry")
		carryNames = append(carryNames, n)
	}
	carryNames = append(carryNames, workflow.End)
	carry.AddTransitionChain(carryNames...)
	h.graphs[pre+"carry"] = carry

	// carryfanout: the N x D case, which is where carrying gets expensive - a fan-out of N branches, each
	// two steps deep, all carrying the same document. Split is the ENTRY step, so it holds the document and
	// is the anchor its branches point at; the branches are multi-step so the cost is N x D copies rather
	// than N. Widen it with -fanout-width, the same knob the fanout workload uses.
	carryFanOut := workflow.NewGraph("CarryFanOut")
	carryFanOut.SetURL("Split", pre+"carry")
	carryFanOut.SetURL("Work", pre+"carry")
	carryFanOut.SetURL("Enrich", pre+"carry")
	carryFanOut.SetURL("Join", pre+"carry")
	carryFanOut.SetFanIn("Join")
	carryFanOut.SetReducer("steps", workflow.ReducerAdd)
	carryFanOut.AddTransitionForEach("Split", "Work", "items", "item")
	carryFanOut.AddTransitionChain("Work", "Enrich", "Join", workflow.End)
	h.graphs[pre+"carryfanout"] = carryFanOut

	// loop / loopreplace: an agent loop, one message added per step - the shape where a carried history grows
	// with the flow. The two differ only in how the task writes: `loop` contributes its message to an append
	// field, `loopreplace` rewrites the whole history. Read dwarf_state_write_bytes{column} per flow against
	// -linear-steps: a history stored again at every step grows with the square of the chain length.
	//
	// Step i checks it was handed exactly i messages, each carrying its index, and the outcome check (closed loop,
	// and the open loop's sampled awaits) that the flow ended with -linear-steps of them. A message dropped,
	// duplicated or never folded therefore fails the run rather than reading as a byte saving: an index check
	// alone passes a step whose predecessor's message never arrived, since the shorter history is still in order.
	loopMessage := payloadBytes
	if loopMessage == 0 {
		loopMessage = defaultLoopMessage
	}
	loopFresh := sync.OnceValue(func() *freshBytes { return newFreshBytes(loopMessage, 1) })
	type loopMsg struct {
		N       int    `json:"n"`
		Role    string `json:"role"`
		Content string `json:"content"`
	}
	checkHistory := func(history []loopMsg, want int) error {
		if len(history) != want {
			return fmt.Errorf("%d messages, want %d - a lost or repeated message, not a saving", len(history), want)
		}
		for i, m := range history {
			if m.N != i {
				return fmt.Errorf("message %d of %d carries index %d - a lost or repeated message, not a saving", i, len(history), m.N)
			}
		}
		return nil
	}
	loopTask := func(appendMode bool, step int) func(context.Context, *workflow.Flow) error {
		return func(ctx context.Context, f *workflow.Flow) error {
			var history []loopMsg
			if _, err := f.Get("messages", &history); err != nil {
				return fmt.Errorf("loop: reading the history at %s: %w", f.StepKey(), err)
			}
			if err := checkHistory(history, step); err != nil {
				return fmt.Errorf("loop: %s: %w", f.StepKey(), err)
			}
			msg := loopMsg{N: len(history), Role: "assistant", Content: loopFresh().next()}
			if appendMode {
				h.bytesWritten.Add(int64(loopMessage))
				return f.Set("messages", []loopMsg{msg})
			}
			// loopreplace writes the whole history, so that is what it counts.
			h.bytesWritten.Add(int64((len(history) + 1) * loopMessage))
			return f.Set("messages", append(history, msg))
		}
	}
	loopCheck := func(out *workflow.FlowOutcome) error {
		var history []loopMsg
		if _, err := out.State.Get("messages", &history); err != nil {
			return fmt.Errorf("loop: reading the final history: %w", err)
		}
		if err := checkHistory(history, chainSteps); err != nil {
			return fmt.Errorf("loop: the final history: %w", err)
		}
		return nil
	}
	for _, mode := range []string{"loop", "loopreplace"} {
		g := workflow.NewGraph("Loop")
		loopNames := make([]string, 0, chainSteps+1)
		for i := range chainSteps {
			n := fmt.Sprintf("L%d", i)
			url := pre + fmt.Sprintf("%s/%s", mode, n)
			h.tasks[url] = loopTask(mode == "loop", i)
			g.SetURL(n, url)
			loopNames = append(loopNames, n)
		}
		loopNames = append(loopNames, workflow.End)
		g.AddTransitionChain(loopNames...)
		if mode == "loop" {
			g.SetReducer("messages", workflow.ReducerAppend)
		}
		h.graphs[pre+mode] = g
	}

	// llm: ONE step per flow, so a flow's wall time is exactly the -task-delay. The multi-step workloads
	// above multiply the delay by their chain length, which makes a minutes-long task unmeasurable in any
	// sane window. This is the shape that validates the derived worker ceiling: the pool must grow past
	// its (connection-derived) resident set to keep thousands of minutes-long tasks in flight at once.
	llm := workflow.NewGraph("LLM")
	llm.SetURL("Call", pre+"nop") // the delay lives in the host's ExecuteTask (-task-delay)
	llm.AddTransition("Call", workflow.End)
	h.graphs[pre+"llm"] = llm

	// interrupt: an entry gate that interrupts once (the soak's load generator resumes it), then
	// proceeds to a no-op. Registered here (not lazily) so the shared registry is never mutated after
	// the engines start. Not a sweep workload - the soak references "bench/interrupt" directly.
	h.tasks[pre+"gate"] = func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(nil, nil)
		return err
	}
	interrupt := workflow.NewGraph("Interrupt")
	interrupt.SetURL("Gate", pre+"gate")
	interrupt.SetURL("Work", pre+"nop")
	interrupt.AddTransitionChain("Gate", "Work", workflow.End)
	h.graphs[pre+"interrupt"] = interrupt

	// hold: -volume-side live. It runs a workload's graph as its subgraph, then interrupts and is never resumed, so
	// the whole tree stays open - in the live tables - once the workload's own flow completes: the archive moves a tree
	// only once its root closes. A workload flow that fails fails the root, which then closes and is archived, and the
	// fill counts it as an error. Two tasks, because a step parks once, for a subgraph or an interrupt.
	h.tasks[pre+"holdrun"] = func(ctx context.Context, f *workflow.Flow) error {
		var in map[string]any
		if _, err := f.Get("holdIn", &in); err != nil {
			return err
		}
		_, err := f.Subgraph(f.GetString("holdGraph"), in, nil)
		return err
	}
	h.tasks[pre+"hold"] = func(ctx context.Context, f *workflow.Flow) error {
		_, err := f.Interrupt(nil, nil)
		return err
	}
	hold := workflow.NewGraph("Hold")
	hold.SetURL("Run", pre+"holdrun")
	hold.SetURL("Hold", pre+"hold")
	hold.AddTransitionChain("Run", "Hold", workflow.End)
	h.graphs[pre+"hold"] = hold

	workloads := map[string]*workload{
		"llm": {
			graphURL:     pre + "llm",
			initialState: func() map[string]any { return nil },
			stepsPerFlow: 1,
		},
		"linear": {
			graphURL:     pre + "linear",
			initialState: func() map[string]any { return nil },
			stepsPerFlow: chainSteps,
			build:        buildWrite,
		},
		"fanout": {
			graphURL:     pre + "fanout",
			initialState: func() map[string]any { return map[string]any{"items": fanoutItems} },
			stepsPerFlow: fanOutWidth + 2,
			build:        buildWrite,
		},
		"scatter": {
			graphURL:     pre + "scatter",
			initialState: func() map[string]any { return nil },
			stepsPerFlow: fanOutWidth*branchDepth + 2,
			build:        buildWrite,
		},
		"state": {
			graphURL:     pre + "state",
			initialState: func() map[string]any { return nil },
			stepsPerFlow: stateSteps,
			build:        func() { payload() },
		},
		// The document rides in as INITIAL STATE rather than being written by a first task, which is what
		// makes the entry step its anchor - the case where a payload lives in a step's `state` column with
		// no task having produced it, and so appears in no `changes` anywhere.
		"carry": {
			graphURL:     pre + "carry",
			initialState: func() map[string]any { return map[string]any{"doc": records()} },
			stepsPerFlow: chainSteps,
			build:        func() { records() },
		},
		"loop": {
			graphURL:     pre + "loop",
			initialState: func() map[string]any { return nil },
			stepsPerFlow: chainSteps,
			check:        loopCheck,
			build:        func() { loopFresh() },
		},
		"loopreplace": {
			graphURL:     pre + "loopreplace",
			initialState: func() map[string]any { return nil },
			stepsPerFlow: chainSteps,
			check:        loopCheck,
			build:        func() { loopFresh() },
		},
		"carryfanout": {
			graphURL: pre + "carryfanout",
			initialState: func() map[string]any {
				return map[string]any{"doc": records(), "items": fanoutItems}
			},
			stepsPerFlow: 2*fanOutWidth + 2,
			build:        func() { records() },
		},
	}
	for _, w := range workloads {
		w.holdURL = pre + "hold"
	}
	return workloads
}

// chooseWorkload resolves the -workload flag to a per-flow picker, and prepares every workload it can pick.
// "mixed" draws 70% linear / 20% fanout / 10% state per flow; any other name is a fixed pick.
func chooseWorkload(workloads map[string]*workload, name string) (func() *workload, error) {
	if name == "mixed" {
		linear, fanout, state := workloads["linear"], workloads["fanout"], workloads["state"]
		linear.prepare()
		fanout.prepare()
		state.prepare()
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
		return nil, fmt.Errorf("unknown workload %q (linear, fanout, scatter, state, carry, carryfanout, loop, loopreplace, llm, mixed)", name)
	}
	w.prepare()
	return func() *workload { return w }, nil
}
