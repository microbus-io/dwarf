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
	"strings"

	"github.com/microbus-io/dwarf/workflow"
)

// The reference model for generated operation histories, and the generator that draws them. What a history is and
// how one is run against the engine is explained at the top of histories_test.go.
//
// The model is a second, deliberately small statement of what the engine does to one tree of flows - which
// executions run, what each is handed and contributes, where an operation lands and what it leaves - written as
// rules over the shapes it knows, not as a second engine. Applying an event to it (hModel.apply) moves it to where
// the engine must stand once the event's work is done; it is a few lists and counters per flow and step (mFlow,
// mStep), never a database.
//
// What a step carries is two lists of tokens, its state and its changes, each execution adding one token to the
// trail field (an append reducer). The rules the model holds the engine to:
//
//   - An execution starts from the step's changes and adds its token. A completion, an interrupt, a subgraph
//     spawn, a covered completion and a retry write the result; a failure writes nothing; a write Terminate fenced
//     writes nothing.
//   - A successor starts from its source's state followed by its changes. A fan-out's branches each start from the
//     source's; the fan-in starts from the source's followed by every branch's changes, in declaration order. A
//     subgraph child starts empty.
//   - A closed flow's final state is frozen at the close: its tail's state followed by its changes, or, while a
//     cohort is open, the fan-out source's followed by every branch's changes - a branch that failed, was
//     cancelled, terminated or is interrupted contributes what its runs stored, as a lost tail does.
//   - A trunk step's loss closes its flow. A branch's loss is an arrival that counts against the cohort, which
//     resolves once every branch arrived: into the fan-in with no loss, else the flow closes cancelled when every
//     loss was a cancellation and failed otherwise. A flow with an interrupted step and no close rests interrupted.
//   - A child that closes completed, failed or cancelled revives its parked caller, whose next execution receives
//     the child's final state or its error. A caller revived after its child is never preempted. A retry of a
//     caller deletes the child it spawned. An interrupt in a child interrupts its caller and every flow above it,
//     and Resume of the root re-parks them.
//   - A step with an error handler that fails, or whose completion is covered, or that is preempted, completes and
//     routes to its handler instead of being lost. A failure's run writes nothing, a covered run's changes stand, and
//     the handler starts from the step's state followed by its changes, with the error that routed it: the step's
//     own when it failed, marked or not, and a cancellation when its completion was covered or it was preempted.
//   - A step the engine has claimed runs whatever happens next; a pending step that is awake is claimed before any
//     operation lands, and a sleeping one is not. Cancel sets each open flow's watermark at its newest step and
//     marks every pending, running and interrupted step at or under it. A tree is cancelled once: a repeat Cancel
//     sets no watermark and only marks again, so it never reaches a flow created since the first. A marked step not
//     yet claimed is preempted at its claim: it never runs and is cancelled. A marked step that runs finishes: its
//     completion is covered (its changes stand, it is cancelled), its error wins over the cancellation, and a
//     retry, interrupt or subgraph it asks for is honored - a child spawned then runs unmarked. A successor of a
//     step at or under its flow's watermark that was not itself covered is inserted marked.
//   - Terminate closes every open step and flow of the tree; a running step's later writes are fenced.
//   - Interrupted steps are addressed in the order they interrupted. Resume re-pends the first with the resume
//     data, which its next execution receives; a flow runs again once none of its steps is interrupted. Resume of a
//     tree that is not interrupted is refused. Cancel and Terminate of a closed tree change nothing.
//   - Lease recovery re-pends a running step whose lease lapsed, and the engine claims it again: a second execution,
//     or a preemption if the step is marked. Every write the first execution makes after that claim is fenced. The
//     orphan repair rewinds a completed step whose transition never committed and replays the transition without
//     running the task; the held worker's own transition is fenced, so the step advances once, as without it.
//   - A lost reply to a commit changes nothing the model holds: the outcome, and each task run as often, as without
//     it. A Resume or Terminate whose reply was lost has landed; its caller is told the connection was lost.
//   - An operation held midway around an execution: Cancel's watermark is set before it and its mark applied after
//     it, so a successor the execution inserts is marked by the watermark alone; Terminate's scan runs before it and
//     its transaction after, and still closes a child the execution spawned in between.
//   - Two executions admitted together are ordered by the model in the history's order. Racing, the generator admits
//     together only combinations that lead to the same place in either order: two interrupts do not (the first to
//     commit is the one Resume addresses first), nor a sleep (the fan-in takes the last arrival's). Two interrupts
//     held before their transactions are released in the history's order, and recorded in it.

// hShape names a graph the model knows.
type hShape string

const (
	hLinear  hShape = "linear"  // A -> B -> C -> END
	hCohort  hShape = "cohort"  // E -> {L, R} -> J -> END, J the fan-in
	hChild   hShape = "child"   // P -> C -> Q -> END, C running sub as a subgraph
	hSub     hShape = "sub"     // X -> Y -> END, the child
	hRecover hShape = "recover" // S -> T -> U -> END, T's error handler H -> U
)

// hGraph is a graph's tasks and edges as the model reads them.
type hGraph struct {
	tasks    []string            // every task, the entry first
	next     map[string]string   // a trunk task's successor; none ends the flow
	branches map[string][]string // a fan-out source's branches, in declaration order; each transitions to fanIn
	fanIn    string
	calls    map[string]hShape // a task that runs a child, and the child's graph
	onError  map[string]string // a task's error handler
}

var hGraphs = map[hShape]hGraph{
	hLinear: {tasks: []string{"A", "B", "C"}, next: map[string]string{"A": "B", "B": "C"}},
	hCohort: {tasks: []string{"E", "L", "R", "J"}, branches: map[string][]string{"E": {"L", "R"}}, fanIn: "J"},
	hChild:  {tasks: []string{"P", "C", "Q"}, next: map[string]string{"P": "C", "C": "Q"}, calls: map[string]hShape{"C": hSub}},
	hSub:    {tasks: []string{"X", "Y"}, next: map[string]string{"X": "Y"}},
	hRecover: {tasks: []string{"S", "T", "H", "U"}, next: map[string]string{"S": "T", "T": "U", "H": "U"},
		onError: map[string]string{"T": "H"}},
}

// hBehavior is what one execution of a task does after it contributes its token.
type hBehavior string

const (
	hOK        hBehavior = "ok"        // returns
	hSleep     hBehavior = "sleep"     // delays its successor by hSleep, and returns
	hRetry     hBehavior = "retry"     // asks for an immediate retry
	hInterrupt hBehavior = "interrupt" // interrupts the flow
	hFail      hBehavior = "fail"      // returns an error
	hSpawn     hBehavior = "spawn"     // runs its child, parking until the child closes
)

// hPoint is where the operation of a run event lands.
type hPoint string

const (
	hMidTask          hPoint = "midTask"          // while the task runs, before it returns
	hBeforeTransition hPoint = "beforeTransition" // after the completion write, before the transition
	hBeforeRewind     hPoint = "beforeRewind"     // after the task asked for a retry, before the rewind
	hBeforeInterrupt  hPoint = "beforeInterrupt"  // after the task asked to interrupt, before the interrupt's transaction
	hAcross           hPoint = "across"           // the operation is held midway, and the whole execution runs inside it
)

// hOp is an operation a history applies, always to the root.
type hOp string

const (
	hCancel    hOp = "cancel"
	hTerminate hOp = "terminate"
	hResume    hOp = "resume"

	// Recovery, landing on a held execution rather than called on the root:
	hLeaseExpiry  hOp = "leaseExpiry"  // at midTask: the execution's lease lapses and lease recovery runs
	hOrphanRepair hOp = "orphanRepair" // at beforeTransition: the lease lapses and the orphan repair runs
)

// hEvent is one event of a history:
//
//   - With a Task and no With, it runs that task's next execution with Behavior, and applies Op at At, if set.
//   - With a Task and a With, it admits both executions at once. With an Op (at beforeTransition), both are held
//     after their completion writes while Op lands, then released Task first; without, they race.
//   - Without a Task, it applies Op to the tree between executions.
type hEvent struct {
	Task         string    `json:"task,omitempty"`
	Behavior     hBehavior `json:"behavior,omitempty"`
	With         string    `json:"with,omitempty"`
	WithBehavior hBehavior `json:"withBehavior,omitempty"`
	At           hPoint    `json:"at,omitempty"`
	Op           hOp       `json:"op,omitempty"`
	Lost         bool      `json:"lost,omitempty"` // the reply to the event's commit is lost: the execution's transaction, or the operation's
}

func (ev hEvent) String() string {
	if ev.Task == "" {
		return string(ev.Op) + map[bool]string{true: " (reply lost)"}[ev.Lost]
	}
	s := ev.Task + ":" + string(ev.Behavior)
	if ev.With != "" {
		s += " & " + ev.With + ":" + string(ev.WithBehavior)
	}
	if ev.Op != "" {
		s += " +" + string(ev.Op) + "@" + string(ev.At)
	}
	if ev.Lost {
		s += " (reply lost)"
	}
	return s
}

// hHistory is a replayable history: the shape and its events. The seed is the one it was generated from, if any.
type hHistory struct {
	Shape  hShape   `json:"shape"`
	Seed   uint64   `json:"seed,omitempty"`
	Events []hEvent `json:"events"`
}

// mFlow is the model of one flow of the tree.
type mFlow struct {
	g         hGraph
	status    string
	watermark int // the index of the newest step when Cancel ran; -1 before
	final     []string
	closedBy  string   // what closed the flow, for coverage
	failed    []string // the runs whose own error was a loss of this flow, any of which its error may name
	caller    *mStep   // the step parked on this flow, for a child
	steps     []*mStep

	spawn                             *mStep   // the fan-out source, once it fanned out
	members                           []*mStep // its branches, in declaration order
	arrivals, failures, cancellations int
}

func (f *mFlow) closed() bool {
	switch f.status {
	case workflow.StatusCompleted, workflow.StatusFailed, workflow.StatusCancelled, workflow.StatusTerminated:
		return true
	}
	return false
}

// mStep is the model of one step row.
type mStep struct {
	idx           int // its place in insertion order, which is step id order
	flow          *mFlow
	task          string
	branch        bool
	status        string
	cancelling    bool
	claimed       bool // it runs, marked or not: claimed before a Cancel marked it, or a caller revived by its child
	sleeping      bool // pending, its not_before ahead
	slept         bool // completed with a sleep, which its successors take
	interruptDone bool
	resumeData    string // the data the next execution receives, once resumed
	child         *mFlow // the child it spawned, while it parks on it or until a retry deletes it
	subgraphDone  bool
	childOutcome  string // what its next execution receives from its child
	routeTo       string // the handler it completed into, routed by an error or a cancellation
	covered       bool   // it was marked when it routed to its handler, which is then inserted unmarked
	routedBy      string // what routed it: failed for its own error, cancelled for a cancellation
	onErr         string // a handler's: what routed its source, which its executions read
	ask           string // the interrupt's payload, once it interrupted
	state         []string
	changes       []string
	execs         int
}

// merged is a step's state followed by its changes.
func (s *mStep) merged() []string {
	return append(slices.Clone(s.state), s.changes...)
}

// hModel is the model of one tree of flows.
type hModel struct {
	shape      hShape
	root       *mFlow
	flows      []*mFlow // every flow of the tree, in creation order
	steps      []*mStep // every step of the tree, in insertion order
	inserted   int      // steps inserted, deleted ones included: the next step's index
	resumes    int
	reaped     int      // children deleted by their caller's retry
	ops        int      // Cancel and Terminate events applied
	interrupts []*mStep // interrupted steps, in the order they are addressed
	recoveries int      // lease expiries and orphan repairs applied
	preempted  []*mStep // the steps the last event preempted, which the engine does in a dispatch of their own
	routes     []string // how each step the last event routed to its handler got there, for coverage
	midDoor    []*mStep // for an event across an operation: the steps at the door once the execution ran, before the operation's second half
	midPreempt []*mStep // and the steps the execution itself preempted, before the operation's second half
}

func newHModel(shape hShape) *hModel {
	m := &hModel{shape: shape}
	m.root = m.newFlow(shape, nil)
	return m
}

// newFlow starts a flow of shape, with its entry step pending.
func (m *hModel) newFlow(shape hShape, caller *mStep) *mFlow {
	f := &mFlow{g: hGraphs[shape], status: workflow.StatusRunning, watermark: -1, caller: caller}
	m.flows = append(m.flows, f)
	m.insert(f, f.g.tasks[0], nil)
	return f
}

func (m *hModel) closed() bool { return m.root.closed() }

// insert adds a pending step of task with state to flow f.
func (m *hModel) insert(f *mFlow, task string, state []string) *mStep {
	s := &mStep{idx: m.inserted, flow: f, task: task, status: workflow.StatusPending, state: state}
	m.inserted++
	m.steps = append(m.steps, s)
	f.steps = append(f.steps, s)
	return s
}

// step is task's step, or nil before it was inserted. Every task of a shape has at most one live step.
func (m *hModel) step(task string) *mStep {
	for _, s := range m.steps {
		if s.task == task {
			return s
		}
	}
	return nil
}

// runnable lists the steps a run event can execute: pending, and not waiting to be preempted.
func (m *hModel) runnable() []*mStep {
	if m.closed() {
		return nil
	}
	var out []*mStep
	for _, s := range m.steps {
		if s.status == workflow.StatusPending && (!s.cancelling || s.claimed) {
			out = append(out, s)
		}
	}
	return out
}

// atDoor lists the steps the engine has claimed and that wait for the test to admit them: pending and awake.
func (m *hModel) atDoor() []*mStep {
	if m.closed() {
		return nil
	}
	var out []*mStep
	for _, s := range m.steps {
		if s.status == workflow.StatusPending && !s.sleeping {
			out = append(out, s)
		}
	}
	return out
}

// wake ends every sleep: by the next event, the sleeping steps' time has come.
func (m *hModel) wake() {
	for _, s := range m.steps {
		s.sleeping = false
	}
}

// settleStatus sets each open flow's status from its steps.
func (m *hModel) settleStatus() {
	for _, f := range m.flows {
		if f.closed() {
			continue
		}
		f.status = workflow.StatusRunning
		if slices.ContainsFunc(f.steps, func(s *mStep) bool { return s.status == workflow.StatusInterrupted }) {
			f.status = workflow.StatusInterrupted
		}
	}
}

// close closes flow f with status, freezing its final state. A child's close revives its caller, unless the whole
// tree is closing.
func (m *hModel) close(f *mFlow, status, by string) {
	f.status = status
	f.closedBy = by
	if f.spawn != nil && !slices.ContainsFunc(f.steps, func(s *mStep) bool { return s.task == f.g.fanIn }) {
		f.final = cohortFold(f)
	} else {
		f.final = f.steps[len(f.steps)-1].merged()
	}
	if f == m.root {
		m.interrupts = nil
		return
	}
	if status == workflow.StatusTerminated {
		return
	}
	c := f.caller
	c.status = workflow.StatusPending
	c.subgraphDone = true
	c.claimed = true // a caller resuming after its child is not preempted
	switch status {
	case workflow.StatusCompleted:
		c.childOutcome = strings.Join(f.final, ".")
	default:
		c.childOutcome = status
	}
	m.settleStatus()
}

// cohortFold is the fan-out source's state and changes followed by every branch's changes, in declaration order.
func cohortFold(f *mFlow) []string {
	out := f.spawn.merged()
	for _, b := range f.members {
		out = append(out, b.changes...)
	}
	return out
}

// token is what the next execution of s contributes: its task and run, the resume data it receives, and what its
// child returned.
func (m *hModel) token(s *mStep) string {
	tok := fmt.Sprintf("%s%d", s.task, s.execs+1)
	if s.resumeData != "" {
		tok += ":" + s.resumeData
	}
	if s.subgraphDone {
		tok += ":" + s.childOutcome
	}
	if s.onErr != "" {
		tok += ":" + s.onErr
	}
	return tok
}

// apply advances the model by one event and reports whether the operation it applies is expected to be refused.
// It returns an error for an event the model's state does not allow, as a hand-edited history can hold.
func (m *hModel) apply(ev hEvent) (refused bool, err error) {
	m.preempted, m.routes, m.midDoor, m.midPreempt = nil, nil, nil, nil
	if ev.Task == "" {
		if m.closed() && ev.Op == "" {
			return false, fmt.Errorf("an empty event")
		}
		if ev.Lost && !m.opCommits(ev.Op) {
			return false, fmt.Errorf("event %s: the operation commits nothing to lose the reply of", ev)
		}
		refused = m.op(ev.Op)
		m.wake()
		return refused, nil
	}
	m.wake()
	if ev.With != "" {
		return m.applyTogether(ev)
	}
	s, err := m.runnableTask(ev.Task, ev.Behavior)
	if err != nil {
		return false, fmt.Errorf("event %s: %w", ev, err)
	}
	if ev.Op != "" && !reaches(s, ev.Behavior, ev.At) {
		return false, fmt.Errorf("event %s: %s does not reach %s", ev, ev.Behavior, ev.At)
	}
	if ev.Lost && (ev.Op != "" || lostCommit(s, ev.Behavior) == "") {
		return false, fmt.Errorf("event %s: no transaction of it has a reply to lose", ev)
	}
	if ev.At == hAcross {
		m.ops++
		if ev.Op == hCancel {
			m.cancelWatermark()
		}
		m.run(s, ev.Behavior)
		m.midDoor, m.midPreempt = m.atDoor(), slices.Clone(m.preempted)
		if ev.Op == hCancel {
			m.cancelMark()
		} else {
			m.ops--
			m.op(ev.Op)
		}
		return false, nil
	}
	tok := m.start(s)
	switch {
	case ev.Op == hLeaseExpiry:
		// The held execution's writes are all fenced. Lease recovery re-pends the step, and the engine claims it
		// again at once: a second execution, unless the step is marked and so preempted at that claim.
		m.recoveries++
		s.status = workflow.StatusPending
		m.preempt(s)
		return false, nil
	case ev.At == hMidTask || ev.At == hBeforeInterrupt:
		refused = m.op(ev.Op)
	}
	rewindOp := hOp("")
	if ev.At == hBeforeRewind {
		rewindOp = ev.Op
	}
	completed, rewindRefused := m.finish(s, ev.Behavior, tok, rewindOp)
	refused = refused || rewindRefused
	if !completed {
		return refused, nil
	}
	if ev.At == hBeforeTransition {
		refused = m.op(ev.Op)
		if m.closed() {
			return refused, nil
		}
	}
	m.transition(s)
	return refused, nil
}

// run is one execution of s with behavior b and no operation landing in it.
func (m *hModel) run(s *mStep, b hBehavior) {
	tok := m.start(s)
	if completed, _ := m.finish(s, b, tok, ""); completed {
		m.transition(s)
	}
}

// lostCommit names the transaction an execution of s with behavior b commits last, whose reply a history can
// lose: advance (a transition, a branch's arrival, a route to a handler), completeFlow, failStep or spawn. A retry
// and an interrupt have no lost-reply fault, and name none.
func lostCommit(s *mStep, b hBehavior) string {
	handler := s.flow.g.onError[s.task] != ""
	switch b {
	case hSpawn:
		return "spawn"
	case hFail:
		if handler {
			return "advance"
		}
		return "failStep"
	case hOK, hSleep:
		switch {
		case s.cancelling && !handler:
			return "failStep"
		case s.cancelling, s.branch, s.flow.g.branches[s.task] != nil:
			return "advance"
		}
		if _, ok := s.flow.g.next[s.task]; ok {
			return "advance"
		}
		return "completeFlow"
	}
	return ""
}

// opCommits reports whether op on the tree as it stands commits anything, whose reply a history can lose: a Resume
// of an interrupted tree and a Terminate of an open one. Cancel has no lost-reply fault.
func (m *hModel) opCommits(op hOp) bool {
	switch op {
	case hResume:
		return m.root.status == workflow.StatusInterrupted
	case hTerminate:
		return !m.closed()
	}
	return false
}

// applyTogether admits two executions at once.
func (m *hModel) applyTogether(ev hEvent) (refused bool, err error) {
	a, err := m.runnableTask(ev.Task, ev.Behavior)
	if err != nil {
		return false, fmt.Errorf("event %s: %w", ev, err)
	}
	b, err := m.runnableTask(ev.With, ev.WithBehavior)
	if err != nil || a == b {
		return false, fmt.Errorf("event %s: %w", ev, err)
	}
	if !togetherFits(ev.Behavior, ev.WithBehavior, ev.Op, ev.At) || ev.At != "" && (!reaches(a, ev.Behavior, ev.At) || !reaches(b, ev.WithBehavior, ev.At)) {
		return false, fmt.Errorf("event %s: not order-independent", ev)
	}
	ta, tb := m.start(a), m.start(b)
	if ev.At == hBeforeInterrupt {
		// Both are held before their interrupt's transaction, then released in the event's order, so the one
		// released first is the one recorded first.
		if ev.Op != "" {
			refused = m.op(ev.Op)
		}
		m.finish(a, ev.Behavior, ta, "")
		m.finish(b, ev.WithBehavior, tb, "")
		return refused, nil
	}
	if ev.Op == "" {
		if done, _ := m.finish(a, ev.Behavior, ta, ""); done {
			m.transition(a)
		}
		if done, _ := m.finish(b, ev.WithBehavior, tb, ""); done {
			m.transition(b)
		}
		return false, nil
	}
	doneA, _ := m.finish(a, ev.Behavior, ta, "")
	doneB, _ := m.finish(b, ev.WithBehavior, tb, "")
	refused = m.op(ev.Op)
	for _, d := range []struct {
		s    *mStep
		done bool
	}{{a, doneA}, {b, doneB}} {
		if d.done && !m.closed() {
			m.transition(d.s)
		}
	}
	return refused, nil
}

// togetherFits reports whether two executions with behaviors a and b, admitted together, lead to one place in
// either order, or are held where the event's order decides: racing, no sleep and not two interrupts; held before
// their transitions, two completions; held before their interrupts' transactions, two interrupts.
func togetherFits(a, b hBehavior, op hOp, at hPoint) bool {
	switch at {
	case hBeforeTransition:
		return a == hOK && b == hOK
	case hBeforeInterrupt:
		return a == hInterrupt && b == hInterrupt
	case "":
		return op == "" && a != hSleep && b != hSleep && !(a == hInterrupt && b == hInterrupt)
	}
	return false
}

// runnableTask is task's step if a run event can execute it with behavior.
func (m *hModel) runnableTask(task string, behavior hBehavior) (*mStep, error) {
	s := m.step(task)
	if s == nil || !slices.Contains(m.runnable(), s) {
		return nil, fmt.Errorf("%s cannot run", task)
	}
	if !slices.ContainsFunc(behaviorsFor(s), func(c hChoice) bool { return c.ev.Behavior == behavior }) {
		return nil, fmt.Errorf("%s cannot %s now", task, behavior)
	}
	return s, nil
}

// start begins an execution of s and returns its token.
func (m *hModel) start(s *mStep) string {
	tok := m.token(s)
	s.execs++
	s.status = workflow.StatusRunning
	s.claimed = false
	return tok
}

// finish applies what the execution returned. It reports whether the step completed and must transition, and
// whether an operation landing before a retry's rewind was refused.
func (m *hModel) finish(s *mStep, b hBehavior, tok string, rewindOp hOp) (completed, refused bool) {
	if s.status == workflow.StatusTerminated {
		return false, false // every write the task makes now is fenced
	}
	handler := s.flow.g.onError[s.task]
	switch b {
	case hFail:
		if handler != "" {
			s.status, s.routeTo, s.covered, s.routedBy = workflow.StatusCompleted, handler, s.cancelling, workflow.StatusFailed
			m.routes = append(m.routes, map[bool]string{false: "an error", true: "an error while marked"}[s.cancelling])
			return true, refused
		}
		s.status = workflow.StatusFailed
		s.flow.failed = append(s.flow.failed, fmt.Sprintf("%s%d", s.task, s.execs))
		m.lose(s, false)
	case hRetry:
		if rewindOp != "" {
			refused = m.op(rewindOp)
			if s.status == workflow.StatusTerminated {
				return false, refused
			}
		}
		s.changes = append(s.changes, tok)
		s.status = workflow.StatusPending
		s.interruptDone = false
		s.resumeData = ""
		if s.child != nil { // the rewind deletes the child the earlier run spawned
			m.removeFlow(s.child)
			s.child = nil
		}
		s.subgraphDone = false
		s.childOutcome = ""
		m.preempt(s)
	case hInterrupt:
		s.changes = append(s.changes, tok)
		s.status = workflow.StatusInterrupted
		s.interruptDone = true
		s.ask = tok
		for f := s.flow; f.caller != nil; f = f.caller.flow {
			f.caller.status = workflow.StatusInterrupted
		}
		m.interrupts = append(m.interrupts, s)
		m.settleStatus()
	case hSpawn:
		s.changes = append(s.changes, tok)
		s.child = m.newFlow(s.flow.g.calls[s.task], s)
	case hOK, hSleep:
		s.changes = append(s.changes, tok)
		if s.cancelling {
			if handler != "" {
				s.status, s.routeTo, s.covered, s.routedBy = workflow.StatusCompleted, handler, true, workflow.StatusCancelled
				m.routes = append(m.routes, "a covered completion")
				return true, refused
			}
			s.status = workflow.StatusCancelled
			m.lose(s, true)
			return false, refused
		}
		s.status = workflow.StatusCompleted
		s.slept = b == hSleep
		return true, refused
	}
	return false, refused
}

// removeFlow deletes flow f and its steps.
func (m *hModel) removeFlow(f *mFlow) {
	m.reaped++
	m.flows = slices.DeleteFunc(m.flows, func(g *mFlow) bool { return g == f })
	m.steps = slices.DeleteFunc(m.steps, func(s *mStep) bool { return s.flow == f })
}

// transition advances a flow past a step that completed and was not covered.
func (m *hModel) transition(s *mStep) {
	f := s.flow
	marked := f.watermark >= s.idx && !s.covered
	if s.routeTo != "" {
		h := m.insert(f, s.routeTo, s.merged())
		h.cancelling, h.onErr = marked, s.routedBy
		m.preempt(h)
		return
	}
	if branches := f.g.branches[s.task]; branches != nil {
		f.spawn = s
		for _, task := range branches {
			b := m.insert(f, task, s.merged())
			b.branch, b.sleeping, b.cancelling = true, s.slept, marked
			f.members = append(f.members, b)
		}
		for _, b := range f.members {
			m.preempt(b)
		}
		return
	}
	if s.branch {
		f.arrivals++
		m.resolve(f, s)
		return
	}
	next, ok := f.g.next[s.task]
	if !ok {
		m.close(f, workflow.StatusCompleted, "completed")
		return
	}
	n := m.insert(f, next, s.merged())
	n.sleeping, n.cancelling = s.slept, marked
	m.preempt(n)
}

// resolve settles a cohort once its last branch arrived: the fan-in with no loss, else the flow closes.
func (m *hModel) resolve(f *mFlow, last *mStep) {
	if f.arrivals < len(f.members) {
		return
	}
	if f.failures > 0 {
		if f.cancellations == f.failures {
			m.close(f, workflow.StatusCancelled, "cohort cancelled")
		} else {
			m.close(f, workflow.StatusFailed, "cohort failed")
		}
		return
	}
	j := m.insert(f, f.g.fanIn, cohortFold(f))
	j.sleeping, j.cancelling = last.slept, f.watermark >= last.idx
	m.preempt(j)
}

// lose records a step's unrecovered loss: a branch's counts against its cohort, a trunk step's closes its flow.
func (m *hModel) lose(s *mStep, cancelled bool) {
	status, by := workflow.StatusFailed, "fail"
	if cancelled {
		status, by = workflow.StatusCancelled, "cancel"
	}
	if !s.branch {
		m.close(s.flow, status, by)
		return
	}
	f := s.flow
	f.arrivals++
	f.failures++
	if cancelled {
		f.cancellations++
	}
	m.resolve(f, s)
	m.settleStatus()
}

// reaches reports whether an execution of s with behavior b reaches point p. A marked step's completion is covered
// and takes no transition, unless it routes to its handler, as a failure does.
func reaches(s *mStep, b hBehavior, p hPoint) bool {
	handler := s.flow.g.onError[s.task] != ""
	switch p {
	case hMidTask:
		return true
	case hBeforeTransition:
		return (b == hOK || b == hSleep) && (!s.cancelling || handler) || b == hFail && handler
	case hBeforeRewind:
		return b == hRetry
	case hBeforeInterrupt:
		return b == hInterrupt
	case hAcross:
		return true
	}
	return false
}

// preempt cancels a marked step at its claim, without running it.
func (m *hModel) preempt(s *mStep) {
	if s.status != workflow.StatusPending || !s.cancelling || s.claimed || s.subgraphDone || s.flow.closed() {
		return // a caller resuming after its child runs once more, marked or not
	}
	s.sleeping = false
	m.preempted = append(m.preempted, s)
	if handler := s.flow.g.onError[s.task]; handler != "" {
		s.status, s.routeTo, s.covered, s.routedBy = workflow.StatusCompleted, handler, true, workflow.StatusCancelled
		m.routes = append(m.routes, "a preemption")
		m.transition(s)
		return
	}
	s.status = workflow.StatusCancelled
	m.lose(s, true)
}

// op applies an operation to the root and reports whether the engine refuses it.
func (m *hModel) op(op hOp) (refused bool) {
	switch op {
	case hCancel:
		m.ops++
		if m.closed() {
			return false
		}
		m.cancelWatermark()
		m.cancelMark()
		return false
	case hOrphanRepair:
		// The repair replays the held step's transition, and the held worker's own is fenced: once, as without it.
		m.recoveries++
		return false
	case hTerminate:
		m.ops++
		if m.closed() {
			return false
		}
		for _, s := range m.steps {
			switch s.status {
			case workflow.StatusPending, workflow.StatusRunning, workflow.StatusInterrupted:
				if !s.flow.closed() {
					s.status = workflow.StatusTerminated
					s.sleeping = false
				}
			}
		}
		for i := len(m.flows) - 1; i >= 0; i-- {
			if f := m.flows[i]; !f.closed() {
				m.close(f, workflow.StatusTerminated, "terminate")
			}
		}
		return false
	case hResume:
		if m.root.status != workflow.StatusInterrupted {
			return true
		}
		m.resumes++
		s := m.interrupts[0]
		m.interrupts = m.interrupts[1:]
		s.status = workflow.StatusPending
		s.resumeData = fmt.Sprintf("r%d", m.resumes)
		for f := s.flow; f.caller != nil; f = f.caller.flow {
			f.caller.status = workflow.StatusRunning // parked again
		}
		m.settleStatus()
		m.preempt(s)
		return false
	}
	return false
}

// url is the workflow URL flow f runs.
func (m *hModel) url(f *mFlow) string {
	if f.caller != nil {
		return "history/" + string(f.caller.flow.g.calls[f.caller.task])
	}
	return "history/" + string(m.shape)
}

// cancelWatermark is Cancel's first half: each open flow's watermark at the newest step. A tree is cancelled once,
// so a repeat sets none.
func (m *hModel) cancelWatermark() {
	if m.root.watermark >= 0 || m.closed() {
		return
	}
	for _, f := range m.flows {
		if !f.closed() {
			f.watermark = m.inserted - 1
		}
	}
}

// cancelMark is Cancel's second half: it marks every pending, running and interrupted step at or under its flow's
// watermark. A pending step that is awake has been claimed and runs on, covered; one asleep is preempted when it
// wakes.
func (m *hModel) cancelMark() {
	for _, f := range m.flows {
		if f.closed() {
			continue
		}
		for _, s := range f.steps {
			if s.idx > f.watermark {
				continue
			}
			switch s.status {
			case workflow.StatusPending, workflow.StatusRunning, workflow.StatusInterrupted:
				s.claimed = s.claimed || s.status == workflow.StatusPending && !s.sleeping
				s.cancelling = true
			}
		}
	}
	for _, s := range slices.Clone(m.steps) {
		m.preempt(s)
	}
}

// snapshotStep is the step an interrupted root's Snapshot reports: the first interrupted step, or, for one in a
// child, the root's step on its chain.
func (m *hModel) snapshotStep() *mStep {
	s := m.interrupts[0]
	for s.flow.caller != nil {
		s = s.flow.caller
	}
	return s
}

// --- Situations ---

// situation names ev as it lands on the model's state now, before it is applied: what the histories' coverage
// counts, and the floor is written against.
func (m *hModel) situation(ev hEvent) string {
	switch {
	case ev.Task == "":
		return fmt.Sprintf("op %s%s on %s", ev.Op, map[bool]string{true: " (reply lost)"}[ev.Lost], m.restingAs())
	case ev.With != "":
		if ev.At == "" {
			pair := []string{string(ev.Behavior), string(ev.WithBehavior)}
			slices.Sort(pair)
			return fmt.Sprintf("together %s & %s", pair[0], pair[1])
		}
		return fmt.Sprintf("together %s & %s, held @%s%s", ev.Behavior, ev.WithBehavior, ev.At, map[bool]string{true: " +" + string(ev.Op)}[ev.Op != ""])
	}
	s := m.step(ev.Task)
	if s == nil {
		return "run " + string(ev.Behavior)
	}
	label := string(ev.Behavior)
	switch {
	case s.resumeData != "":
		label += " resumed"
	case s.subgraphDone:
		label += " after its child"
	}
	switch {
	case ev.Op != "":
		return fmt.Sprintf("run %s +%s@%s on %s", label, ev.Op, ev.At, ev.Task)
	case ev.Lost:
		return fmt.Sprintf("run %s, the reply to its %s lost", label, lostCommit(s, ev.Behavior))
	}
	return "run " + label
}

// settled names what the event just applied left: each route to a handler, and the root's close, the first time it
// closes.
func (m *hModel) settled(wasClosed bool) []string {
	var out []string
	for _, route := range m.routes {
		out = append(out, "routed to a handler by "+route)
	}
	if !wasClosed && m.closed() {
		out = append(out, fmt.Sprintf("closed %s by %s", m.root.status, m.root.closedBy))
	}
	return out
}

// restingAs names the state an operation between executions lands on.
func (m *hModel) restingAs() string {
	s := m.root.status
	if s == workflow.StatusRunning {
		s = fmt.Sprintf("running with %d at the door", len(m.atDoor()))
		if slices.ContainsFunc(m.steps, func(s *mStep) bool { return s.sleeping }) {
			s = "asleep"
		}
	}
	if m.root.watermark >= 0 && !m.closed() {
		s += " cancelling"
	}
	return s
}

// --- Generation ---

// hMaxEvents bounds a generated history: past it, every event drawn closes the flow.
const hMaxEvents = 24

type hChoice struct {
	ev     hEvent
	weight int
}

// behaviorsFor lists the behaviors an execution of s can draw, weighted. A step runs at most four times; a task
// that runs a child spawns it unless it already parked once in this run of the step - on its child, or on an
// interrupt, which excludes the other.
func behaviorsFor(s *mStep) []hChoice {
	fail := 1
	if s.flow.g.onError[s.task] != "" {
		fail = 4
	}
	out := []hChoice{{hEvent{Behavior: hOK}, 6}, {hEvent{Behavior: hSleep}, 1}, {hEvent{Behavior: hFail}, fail}}
	if s.flow.g.calls[s.task] != "" && !s.subgraphDone && !s.interruptDone {
		out = append(out, hChoice{hEvent{Behavior: hSpawn}, 16})
	}
	if s.execs < 3 {
		out = append(out, hChoice{hEvent{Behavior: hRetry}, 2})
		if !s.interruptDone && !s.subgraphDone {
			out = append(out, hChoice{hEvent{Behavior: hInterrupt}, 2})
		}
	}
	return out
}

// choices lists the events the model allows now, weighted. A history runs at most two Cancel or Terminate events
// before its flow closes and at most two recoveries, and a closed flow takes at most one more operation. With
// mustClose it offers only events that close the flow.
func (m *hModel) choices(postClose int, mustClose bool) []hChoice {
	if m.closed() {
		if postClose > 0 {
			return nil
		}
		return []hChoice{{hEvent{}, 6}, {hEvent{Op: hCancel}, 1}, {hEvent{Op: hTerminate}, 1}, {hEvent{Op: hResume}, 1}}
	}
	if mustClose {
		return []hChoice{{hEvent{Op: hTerminate}, 1}}
	}
	var out []hChoice
	opsLeft, recoveriesLeft := m.ops < 2, m.recoveries < 2
	// Operations between executions, the ones that commit sometimes with their reply lost.
	rest := func(op hOp, weight int) {
		out = append(out, hChoice{hEvent{Op: op}, weight})
		if m.opCommits(op) {
			out = append(out, hChoice{hEvent{Op: op, Lost: true}, (weight + 1) / 2})
		}
	}
	sleeping := slices.ContainsFunc(m.steps, func(s *mStep) bool { return s.sleeping })
	switch {
	case m.root.status == workflow.StatusInterrupted:
		rest(hResume, 160)
		if opsLeft {
			rest(hCancel, 40)
			rest(hTerminate, 20)
		}
	case sleeping:
		if opsLeft {
			rest(hCancel, 40)
			rest(hTerminate, 20)
		}
		rest(hResume, 10)
	default:
		if opsLeft {
			rest(hCancel, 6)
			rest(hTerminate, 3)
		}
		rest(hResume, 2)
	}
	runnable := m.runnable()
	for _, s := range runnable {
		for _, b := range behaviorsFor(s) {
			out = append(out, hChoice{hEvent{Task: s.task, Behavior: b.ev.Behavior}, b.weight * 20})
			switch lostCommit(s, b.ev.Behavior) {
			case "":
			case "completeFlow":
				out = append(out, hChoice{hEvent{Task: s.task, Behavior: b.ev.Behavior, Lost: true}, b.weight * 6})
			default:
				out = append(out, hChoice{hEvent{Task: s.task, Behavior: b.ev.Behavior, Lost: true}, b.weight * 2})
			}
			if opsLeft {
				for _, at := range []hPoint{hMidTask, hBeforeTransition, hBeforeRewind, hBeforeInterrupt, hAcross} {
					if !reaches(s, b.ev.Behavior, at) {
						continue
					}
					cancel, terminate := b.weight, (b.weight+1)/2
					if b.ev.Behavior == hSpawn {
						cancel, terminate = 2*b.weight, 2*b.weight // the spawn honored under a mark; the rescan for a child
					}
					out = append(out,
						hChoice{hEvent{Task: s.task, Behavior: b.ev.Behavior, At: at, Op: hCancel}, cancel},
						hChoice{hEvent{Task: s.task, Behavior: b.ev.Behavior, At: at, Op: hTerminate}, terminate},
					)
				}
			}
			if recoveriesLeft && reaches(s, b.ev.Behavior, hBeforeTransition) {
				out = append(out, hChoice{hEvent{Task: s.task, Behavior: b.ev.Behavior, At: hBeforeTransition, Op: hOrphanRepair}, b.weight * 3})
			}
		}
		if recoveriesLeft {
			// The delayed worker's writes are all fenced, so what it does is immaterial.
			out = append(out, hChoice{hEvent{Task: s.task, Behavior: hOK, At: hMidTask, Op: hLeaseExpiry}, 12})
		}
	}
	// Two branches admitted together: racing; held before their transitions while an operation lands; or both
	// interrupting, held before their interrupts' transactions and released in order.
	for i, a := range runnable {
		for _, b := range runnable[i+1:] {
			if !a.branch || !b.branch {
				continue
			}
			for _, ba := range behaviorsFor(a) {
				for _, bb := range behaviorsFor(b) {
					if togetherFits(ba.ev.Behavior, bb.ev.Behavior, "", "") {
						out = append(out, hChoice{hEvent{Task: a.task, Behavior: ba.ev.Behavior, With: b.task, WithBehavior: bb.ev.Behavior}, ba.weight * bb.weight})
					}
				}
			}
			if !a.interruptDone && !b.interruptDone && a.execs < 3 && b.execs < 3 {
				for _, pair := range [][2]*mStep{{a, b}, {b, a}} {
					out = append(out, hChoice{hEvent{Task: pair[0].task, Behavior: hInterrupt, With: pair[1].task, WithBehavior: hInterrupt, At: hBeforeInterrupt}, 6})
					if opsLeft {
						for _, op := range []hOp{hCancel, hTerminate} {
							out = append(out, hChoice{hEvent{Task: pair[0].task, Behavior: hInterrupt, With: pair[1].task, WithBehavior: hInterrupt, At: hBeforeInterrupt, Op: op}, 3})
						}
					}
				}
			}
			if opsLeft && !a.cancelling && !b.cancelling {
				for _, op := range []hOp{hCancel, hTerminate} {
					out = append(out,
						hChoice{hEvent{Task: a.task, Behavior: hOK, With: b.task, WithBehavior: hOK, At: hBeforeTransition, Op: op}, 12},
						hChoice{hEvent{Task: b.task, Behavior: hOK, With: a.task, WithBehavior: hOK, At: hBeforeTransition, Op: op}, 12},
					)
				}
			}
		}
	}
	return out
}

// generateHistory draws a history of shape from seed: the model is run forward, each event drawn from the ones it
// allows, until the flow closed and took its last operation. It returns the situations the history reaches.
func generateHistory(shape hShape, seed uint64) (h hHistory, situations []string) {
	rng := rand.New(rand.NewPCG(seed, 0x6869_7374_6f72_79))
	m := newHModel(shape)
	h = hHistory{Shape: shape, Seed: seed}
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
		wasClosed := m.closed()
		if wasClosed {
			postClose++
		}
		situations = append(situations, m.situation(ev))
		if _, err := m.apply(ev); err != nil {
			panic(err) // the model offered an event it then refused
		}
		situations = append(situations, m.settled(wasClosed)...)
		h.Events = append(h.Events, ev)
	}
	return h, situations
}
