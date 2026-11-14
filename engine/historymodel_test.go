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
	"fmt"
	"math/rand/v2"
	"slices"

	"github.com/microbus-io/dwarf/workflow"
)

// The reference model for generated operation histories. It is a second, deliberately small statement of what the
// engine does to one flow - which executions run, what each contributes, where an operation lands and what it
// leaves - written as rules over the shapes it knows, not as a second engine. histories_test.go drives the engine
// through a history one event at a time and compares it with the model after every event.
//
// What a step carries is two lists of tokens, its state and its changes, each execution adding one token to the
// trail field (an append reducer). The rules the model holds the engine to:
//
//   - An execution starts from the step's changes and adds its token: a completion, an interrupt and a covered
//     completion write the result. A retry folds state and changes into the step's state and clears its changes.
//     A failure writes nothing. A fenced write (Terminate landed first) writes nothing.
//   - A successor starts from its source's state followed by its changes, and a closed flow's final state is its
//     tail's state followed by its tail's changes, frozen at the close.
//   - Cancel marks every pending, running and interrupted step and sets the watermark at the newest step. A marked
//     step is preempted at its next claim, so it never runs again. A marked step that is running finishes: its
//     completion is covered (its changes stand, it closes cancelled), its error wins over the cancellation, and a
//     retry or an interrupt it asks for is honored and then preempted. A successor of a step at or under the
//     watermark that was not itself covered is inserted marked.
//   - Terminate closes every open step and the flow; a running step's later writes are fenced.
//   - Resume of an interrupted flow re-pends the leaf with the resume data, which its next execution receives.
//     Resume of a flow that is not interrupted is refused. Cancel and Terminate of a closed flow change nothing.

// hShape names a graph the model knows.
type hShape string

const hLinear hShape = "linear" // A -> B -> C -> END

// hShapeTasks is each shape's tasks, in the order they run.
var hShapeTasks = map[hShape][]string{
	hLinear: {"A", "B", "C"},
}

// hBehavior is what one execution of a task does after it contributes its token.
type hBehavior string

const (
	hOK        hBehavior = "ok"        // returns
	hSleep     hBehavior = "sleep"     // delays its successor by hSleep, and returns
	hRetry     hBehavior = "retry"     // asks for an immediate retry
	hInterrupt hBehavior = "interrupt" // interrupts the flow
	hFail      hBehavior = "fail"      // returns an error
)

// hPoint is where the operation of a run event lands.
type hPoint string

const (
	hMidTask          hPoint = "midTask"          // while the task runs, before it returns
	hBeforeTransition hPoint = "beforeTransition" // after the completion write, before the transition
	hBeforeRewind     hPoint = "beforeRewind"     // after the task asked for a retry, before the rewind
)

// hOp is an operation a history applies.
type hOp string

const (
	hCancel    hOp = "cancel"
	hTerminate hOp = "terminate"
	hResume    hOp = "resume"
)

// hEvent is one event of a history. With a Task it runs that task's next execution with Behavior, and applies Op
// at At, if set. Without, it applies Op to the flow at rest: interrupted, its next step asleep, or closed.
type hEvent struct {
	Task     string    `json:"task,omitempty"`
	Behavior hBehavior `json:"behavior,omitempty"`
	At       hPoint    `json:"at,omitempty"`
	Op       hOp       `json:"op,omitempty"`
}

func (ev hEvent) String() string {
	if ev.Task == "" {
		return string(ev.Op)
	}
	s := ev.Task + ":" + string(ev.Behavior)
	if ev.Op != "" {
		s += "+" + string(ev.Op) + "@" + string(ev.At)
	}
	return s
}

// hHistory is a replayable history: the shape and its events. The seed is the one it was generated from, if any.
type hHistory struct {
	Shape  hShape   `json:"shape"`
	Seed   uint64   `json:"seed,omitempty"`
	Events []hEvent `json:"events"`
}

// mStep is the model of one step row.
type mStep struct {
	task          string
	status        string
	cancelling    bool
	sleeping      bool // pending, its not_before ahead
	interruptDone bool
	resumeData    string // the data the next execution receives, once resumed
	state         []string
	changes       []string
	execs         int
}

// hModel is the model of one flow.
type hModel struct {
	shape     hShape
	tasks     []string
	steps     []*mStep
	status    string
	watermark int // the index of the newest step when Cancel ran; -1 before
	final     []string
	closedBy  string // what closed the flow, for coverage
	resumes   int
	execs     map[string]int
	ops       int // Cancel and Terminate events applied
}

func newHModel(shape hShape) *hModel {
	m := &hModel{shape: shape, tasks: hShapeTasks[shape], status: workflow.StatusRunning, watermark: -1, execs: map[string]int{}}
	m.steps = []*mStep{{task: m.tasks[0], status: workflow.StatusPending}}
	return m
}

// tail is the flow's newest step.
func (m *hModel) tail() *mStep { return m.steps[len(m.steps)-1] }

func (m *hModel) closed() bool {
	switch m.status {
	case workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled, workflow.StatusTerminated:
		return true
	}
	return false
}

// merged is a step's state followed by its changes.
func (s *mStep) merged() []string {
	return append(slices.Clone(s.state), s.changes...)
}

// close closes the flow with status, freezing its final state from the tail.
func (m *hModel) close(status, by string) {
	m.status = status
	m.final = m.tail().merged()
	m.closedBy = by
}

// runnable is the step a run event executes, or nil when none can run.
func (m *hModel) runnable() *mStep {
	if m.status != workflow.StatusRunning {
		return nil
	}
	s := m.tail()
	if s.status != workflow.StatusPending || s.cancelling {
		return nil
	}
	return s
}

// token is what the next execution of s contributes.
func (m *hModel) token(s *mStep) string {
	tok := fmt.Sprintf("%s%d", s.task, s.execs+1)
	if s.resumeData != "" {
		tok += ":" + s.resumeData
	}
	return tok
}

// apply advances the model by one event and reports whether the operation it applies is expected to be refused.
// It returns an error for an event the model's state does not allow, as a hand-edited history can hold.
func (m *hModel) apply(ev hEvent) (refused bool, err error) {
	if ev.Task == "" {
		if !m.atRest() {
			return false, fmt.Errorf("event %s: the flow is not at rest", ev)
		}
		for _, s := range m.steps {
			s.sleeping = false
		}
		return m.op(ev.Op), nil
	}
	s := m.runnable()
	if s == nil || s.task != ev.Task {
		return false, fmt.Errorf("event %s: %s cannot run", ev, ev.Task)
	}
	if ev.Behavior == hInterrupt && s.interruptDone {
		return false, fmt.Errorf("event %s: the step was already interrupted", ev)
	}
	if ev.Op != "" && !pointFits(ev.Behavior, ev.At) {
		return false, fmt.Errorf("event %s: %s does not reach %s", ev, ev.Behavior, ev.At)
	}
	tok := m.token(s)
	s.execs++
	m.execs[s.task]++
	s.sleeping = false
	s.status = workflow.StatusRunning
	if ev.At == hMidTask {
		refused = m.op(ev.Op)
	}
	if s.status == workflow.StatusTerminated {
		return refused, nil // every write the task makes now is fenced
	}
	switch ev.Behavior {
	case hFail:
		s.status = workflow.StatusFailed
		m.close(workflow.StatusFailed, "fail")
	case hRetry:
		if ev.At == hBeforeRewind {
			refused = m.op(ev.Op)
			if s.status == workflow.StatusTerminated {
				return refused, nil
			}
		}
		s.state = append(s.merged(), tok)
		s.changes = nil
		s.status = workflow.StatusPending
		s.interruptDone = false
		s.resumeData = ""
		m.preempt(s)
	case hInterrupt:
		s.changes = append(s.changes, tok)
		s.status = workflow.StatusInterrupted
		s.interruptDone = true
		m.status = workflow.StatusInterrupted
	case hOK, hSleep:
		s.changes = append(s.changes, tok)
		if s.cancelling {
			s.status = workflow.StatusCancelled
			m.close(workflow.StatusCancelled, "covered")
			return refused, nil
		}
		s.status = workflow.StatusCompleted
		if ev.At == hBeforeTransition {
			refused = m.op(ev.Op)
			if m.closed() {
				return refused, nil
			}
		}
		idx := slices.Index(m.tasks, s.task)
		if idx == len(m.tasks)-1 {
			m.close(workflow.StatusCompleted, "completed")
			return refused, nil
		}
		next := &mStep{
			task:       m.tasks[idx+1],
			status:     workflow.StatusPending,
			state:      s.merged(),
			sleeping:   ev.Behavior == hSleep,
			cancelling: m.watermark >= len(m.steps)-1, // the source is at or under the watermark, and was not covered
		}
		m.steps = append(m.steps, next)
		m.preempt(next)
	}
	return refused, nil
}

// pointFits reports whether an execution with behavior b reaches point p.
func pointFits(b hBehavior, p hPoint) bool {
	switch p {
	case hMidTask:
		return true
	case hBeforeTransition:
		return b == hOK || b == hSleep
	case hBeforeRewind:
		return b == hRetry
	}
	return false
}

// preempt closes a marked pending step at its claim, without running it: a trunk step's cancellation cancels the
// flow.
func (m *hModel) preempt(s *mStep) {
	if s.status != workflow.StatusPending || !s.cancelling {
		return
	}
	s.status = workflow.StatusCancelled
	s.sleeping = false
	m.close(workflow.StatusCancelled, "preempted")
}

// atRest reports whether an operation event can land now: the flow is interrupted, closed, or its next step
// asleep.
func (m *hModel) atRest() bool {
	if m.status != workflow.StatusRunning {
		return true
	}
	return m.tail().sleeping
}

// op applies an operation and reports whether the engine refuses it.
func (m *hModel) op(op hOp) (refused bool) {
	switch op {
	case hCancel:
		m.ops++
		if m.closed() {
			return false
		}
		m.watermark = len(m.steps) - 1
		for _, s := range m.steps {
			switch s.status {
			case workflow.StatusPending, workflow.StatusRunning, workflow.StatusInterrupted:
				s.cancelling = true
			}
		}
		m.preempt(m.tail()) // a pending step, asleep or not, is preempted when it is claimed
		return false
	case hTerminate:
		m.ops++
		if m.closed() {
			return false
		}
		for _, s := range m.steps {
			switch s.status {
			case workflow.StatusPending, workflow.StatusRunning, workflow.StatusInterrupted:
				s.status = workflow.StatusTerminated
				s.sleeping = false
			}
		}
		m.close(workflow.StatusTerminated, "terminate")
		return false
	case hResume:
		if m.status != workflow.StatusInterrupted {
			return true
		}
		m.resumes++
		s := m.tail()
		s.status = workflow.StatusPending
		s.resumeData = fmt.Sprintf("r%d", m.resumes)
		m.status = workflow.StatusRunning
		m.preempt(s)
		return false
	}
	return false
}

// --- Generation ---

// hMaxEvents bounds a generated history: past it, every event drawn closes the flow.
const hMaxEvents = 24

type hChoice struct {
	ev     hEvent
	weight int
}

// choices lists the events the model allows now, weighted. A history runs at most two Cancel or Terminate events
// before its flow closes, a step at most four executions, and a closed flow takes at most one more operation. With
// mustClose it offers only an event that closes the flow.
func (m *hModel) choices(postClose int, mustClose bool) []hChoice {
	var out []hChoice
	if mustClose && !m.closed() {
		if s := m.runnable(); s != nil {
			return []hChoice{{hEvent{Task: s.task, Behavior: hFail}, 1}}
		}
		return []hChoice{{hEvent{Op: hTerminate}, 1}}
	}
	if m.closed() {
		if postClose > 0 {
			return nil
		}
		return []hChoice{{hEvent{}, 6}, {hEvent{Op: hCancel}, 1}, {hEvent{Op: hTerminate}, 1}, {hEvent{Op: hResume}, 1}}
	}
	opsLeft := m.ops < 2
	if m.status == workflow.StatusInterrupted {
		out = append(out, hChoice{hEvent{Op: hResume}, 8})
		if opsLeft {
			out = append(out, hChoice{hEvent{Op: hCancel}, 2}, hChoice{hEvent{Op: hTerminate}, 1})
		}
		return out
	}
	if m.atRest() { // the next step is asleep
		if opsLeft {
			out = append(out, hChoice{hEvent{Op: hCancel}, 40}, hChoice{hEvent{Op: hTerminate}, 20})
		}
		out = append(out, hChoice{hEvent{Op: hResume}, 10})
	}
	s := m.runnable()
	if s == nil {
		return out
	}
	behaviors := []hChoice{{hEvent{Behavior: hOK}, 6}, {hEvent{Behavior: hSleep}, 1}, {hEvent{Behavior: hFail}, 1}}
	if s.execs < 3 {
		behaviors = append(behaviors, hChoice{hEvent{Behavior: hRetry}, 2})
		if !s.interruptDone {
			behaviors = append(behaviors, hChoice{hEvent{Behavior: hInterrupt}, 2})
		}
	}
	for _, b := range behaviors {
		ev := hEvent{Task: s.task, Behavior: b.ev.Behavior}
		out = append(out, hChoice{ev, b.weight * 20})
		if !opsLeft {
			continue
		}
		for _, at := range []hPoint{hMidTask, hBeforeTransition, hBeforeRewind} {
			if !pointFits(b.ev.Behavior, at) {
				continue
			}
			out = append(out,
				hChoice{hEvent{Task: s.task, Behavior: b.ev.Behavior, At: at, Op: hCancel}, b.weight},
				hChoice{hEvent{Task: s.task, Behavior: b.ev.Behavior, At: at, Op: hTerminate}, (b.weight + 1) / 2},
			)
		}
	}
	return out
}

// generateHistory draws a history of shape from seed: the model is run forward, each event drawn from the ones it
// allows, until the flow closed and took its last operation.
func generateHistory(shape hShape, seed uint64) hHistory {
	rng := rand.New(rand.NewPCG(seed, 0x6869_7374_6f72_79))
	m := newHModel(shape)
	h := hHistory{Shape: shape, Seed: seed}
	postClose := 0
	for {
		choices := m.choices(postClose, len(h.Events) >= hMaxEvents)
		if len(choices) == 0 {
			break
		}
		total := 0
		for _, c := range choices {
			total += c.weight
		}
		pick := rng.IntN(total)
		var ev hEvent
		for _, c := range choices {
			if pick < c.weight {
				ev = c.ev
				break
			}
			pick -= c.weight
		}
		if ev == (hEvent{}) {
			break // a closed flow, left alone
		}
		if m.closed() {
			postClose++
		}
		if _, err := m.apply(ev); err != nil {
			panic(err) // the model offered an event it then refused
		}
		h.Events = append(h.Events, ev)
	}
	return h
}
