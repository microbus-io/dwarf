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
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"github.com/microbus-io/testarossa"
)

// Generated operation histories: the engine against a reference model, over thousands of small scripted scenarios.
//
// # What a history is
//
// A history is a script for one flow: a short list of events, each saying what happens next. An event is one of:
//
//   - an execution: "task B runs once and does X" - completes, sleeps, retries, interrupts, fails or spawns its
//     child - optionally with something landing at a chosen point inside it: a Cancel or Terminate, the loss of its
//     lease, or the loss of the reply to its commit;
//   - two executions at once: two fan-out branches admitted together, racing, or both held at one point;
//   - an operation between executions: Cancel, Terminate or Resume of the root, sometimes with its reply lost.
//
// For example, in the linear shape (A -> B -> C):
//
//	A:ok                          A runs and completes
//	B:interrupt                   B runs and interrupts the flow
//	cancel                        Cancel lands while the flow waits
//	resume                        Resume re-pends B, which Cancel has marked: it must be cancelled, not run
//
// A history is data (hHistory, JSON), so it can be printed, edited and replayed. The generator draws one from a seed
// by running the model forward, each event chosen at random among those the model allows at that moment, so every
// history is valid and a seed always gives the same one.
//
// # How one is run, and what is checked
//
// The model (historymodel_test.go) is a small statement of what the engine must do: a few lists and counters per
// flow and step, and rules - not a second engine. The test runs one fresh engine through the history, in lockstep:
// each event is applied to the model, which says where the tree must stand afterwards, and then driven on the
// engine, which must stand there too. What the model predicts is concrete: each task's executions write one token
// to a "trail" field (an append reducer), so the trail a step is handed and a flow's final state show exactly which
// executions counted, in which order.
//
// Checked after every event: the root's status, an interrupted root's state and prompt, every flow's status and a
// closed flow's final state, that no closed outcome moved since it closed, and that no execution ran which the
// model did not predict. At the end: the step rows, every recovery detector run once more with nothing to change,
// and the invariant checker (internal/enginetest).
//
// # The patterns that make it deterministic
//
//   - The door. Every task runs through the test's own handler, which reports its arrival and waits to be admitted.
//     So the engine runs executions in the history's order, not the scheduler's, and an execution nobody predicted
//     is caught where it arrives (expected, receive).
//   - Holds. Something lands at a point by holding the execution there: inside the task (the handler waits), or at
//     a targeted checkpoint - after the completion write, before a retry's rewind, before an interrupt's transaction
//     - armed for that step alone (holdAt). Two branches can be held at once and released in the event's order.
//   - Across. An operation is held at its own midpoint while a whole execution runs inside it: Cancel between its
//     watermark's commit and its mark, Terminate between its scan and its transaction (runAcross).
//   - Claimed before an operation lands. A pending step that is awake is claimed within moments, and an operation
//     racing that claim finds it pending or running, which the model cannot predict; so every such step is pulled
//     to the door first (ensureAtDoor), and the operation finds it running.
//   - Sleep and lease by hand. A sleep is an hour, ended by the driver where the model wakes the step
//     (wakeSleepers); a lost lease is a backdated lease_expires and a recovery pass the driver runs itself
//     (recover). Neither races a clock.
//   - Every dispatch awaited. An event's checks wait for each dispatch it set going - the execution it admitted, each
//     preemption (a dispatch of its own), each execution a Terminate let go - by counting ClaimedDispatchDone.
//
// # Coverage, the floor, replay and minimizing
//
// Each event is named as the situation it landed on (hModel.situation: "run ok +cancel@beforeTransition", "op
// resume on interrupted"), and a run logs how often each was reached: a count of seeds says nothing about breadth.
// hFloor lists, per shape, the situations a default run must reach; a default run takes seeds 1 to hBaseSeeds, then
// the lowest later seed for each floor situation they miss, chosen from the model alone before anything runs.
//
// A failing history prints itself as JSON with the command that replays it, on SQLite or any SEQUEL_TESTING_DSN;
// DWARF_TESTING_HISTORY_MINIMIZE then cuts it to the fewest events that still fail.
//
// Environment:
//
//	DWARF_TESTING_HISTORY_SEEDS=n     run seeds 1..n instead of the default run (8 with -short), without the floor
//	DWARF_TESTING_HISTORY_SEED=s      run seed s alone
//	DWARF_TESTING_HISTORY='{...}'     replay one history, as a failure prints it ('@path' reads it from a file; a relative
//	                          path is from the package directory)
//	DWARF_TESTING_HISTORY_MINIMIZE=1  with DWARF_TESTING_HISTORY, reduce the failing history it holds to a minimal one

const (
	hSleepFor        = time.Hour // a sleep no operation can outlast; the driver ends it when the model wakes the step
	hCancelReason    = "history cancel"
	hTerminateReason = "history terminate"
	hFailPrefix      = "history fail "
)

// hArrival is an execution waiting at the task handler's door.
type hArrival struct {
	task   string
	shard  int
	flowID int
	stepID int
	admit  chan hDirective
}

// hDirective is what an admitted execution does.
type hDirective struct {
	behavior hBehavior
	input    []string      // the trail the task must be handed: the model's view of the step's input
	token    string        // the token it contributes, before the resume data it receives
	resumed  bool          // it reads the resume data first
	child    string        // the workflow URL of the child it runs: spawned, or read back once it closed
	reentry  bool          // the child closed: it reads what the child returned
	onErr    bool          // it is an error handler: it reads what routed its source
	abandon  bool          // it returns at once: its flow was terminated while it waited
	inside   chan struct{} // closed by the task once it contributed, when an operation lands mid-task
	hold     chan struct{} // closed by the driver once that operation is done
}

// hRunner drives one engine through one history.
type hRunner struct {
	t        *testing.T
	e        *Engine
	h        hHistory
	m        *hModel
	cov      *hCoverage
	flowKey  string
	shard    int
	flowID   int
	arrivals chan *hArrival
	waiting  map[string]*hArrival // executions at the door that no event admitted yet, by task
	refs     map[*mStep][2]int    // each step's shard and id
	claims   map[*mStep]int       // the dispatches of each step that won their claim
	awaited  map[*mStep]bool      // the preempted steps whose preempting dispatch was awaited
	done     chan struct{}
	released []*mStep // executions let go from the door by a Terminate, whose dispatches the event awaits

	kept       enginetest.Outcomes // the closed outcomes, recorded when a flow last closed
	keptFlows  map[*mFlow]bool     // the flows closed when they were recorded
	keptReaped int                 // the children deleted by a retry when they were recorded

	mu       sync.Mutex
	problems []string // what a task handler saw that the model did not predict
}

func (r *hRunner) report(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.problems = append(r.problems, fmt.Sprintf(format, args...))
}

// mux serves the history's graph and the graphs it calls, each task through the handler that waits at the door.
func (r *hRunner) mux() *hosts.Mux {
	mux := hosts.NewMux()
	r.serve(mux, r.h.Shape)
	return mux
}

// serve registers the graph of shape, and the graphs its tasks call.
func (r *hRunner) serve(mux *hosts.Mux, shape hShape) {
	gr := hGraphs[shape]
	for _, child := range gr.calls {
		r.serve(mux, child)
	}
	g := workflow.NewGraph("History")
	for _, task := range gr.tasks {
		g.SetURL(task, "history/"+task)
		mux.HandleTask("history/"+task, r.handler(task))
	}
	g.SetEntryPoint(gr.tasks[0])
	g.SetReducer("trail", workflow.ReducerAppend)
	branch := map[string]bool{}
	for source, branches := range gr.branches {
		for _, b := range branches {
			g.AddTransition(source, b)
			g.AddTransition(b, gr.fanIn)
			branch[b] = true
		}
	}
	for _, task := range gr.tasks {
		if next, ok := gr.next[task]; ok {
			g.AddTransition(task, next)
		} else if gr.branches[task] == nil && !branch[task] {
			g.AddTransition(task, workflow.End)
		}
	}
	for task, handler := range gr.onError {
		g.AddTransitionOnError(task, handler)
	}
	if gr.fanIn != "" {
		g.SetFanIn(gr.fanIn)
	}
	mux.HandleGraph("history/"+string(shape), g)
}

func (r *hRunner) handler(task string) func(ctx context.Context, f *workflow.Flow) error {
	return func(ctx context.Context, f *workflow.Flow) error {
		shard, _, flowID, stepID, _, err := keys.ParseStepKey(f.StepKey())
		if err != nil {
			return err
		}
		a := &hArrival{task: task, shard: shard, flowID: flowID, stepID: stepID, admit: make(chan hDirective, 1)}
		select {
		case r.arrivals <- a:
		case <-r.done:
			return errors.New("the history is over")
		case <-ctx.Done():
			return ctx.Err()
		}
		var d hDirective
		select {
		case d = <-a.admit:
		case <-r.done:
			return errors.New("the history is over")
		case <-ctx.Done():
			return ctx.Err()
		}
		if d.abandon {
			return errors.New("the flow was terminated")
		}
		if got := nonNil(f.GetStrings("trail")); !slices.Equal(got, nonNil(d.input)) {
			r.report("%s: execution %s was handed the trail %v, the model has %v", task, d.token, got, d.input)
		}
		tok := d.token
		if d.resumed {
			var out map[string]any
			yield, err := f.Interrupt(nil, &out)
			if err != nil || yield {
				r.report("%s: execution %s expected its resume data, got yield=%v err=%v", task, tok, yield, err)
				return errors.New("no resume data")
			}
			tok += ":" + fmt.Sprint(out["answer"])
		}
		if d.onErr {
			var routed errors.TracedError
			if ok, err := f.Get("onErr", &routed); !ok || err != nil {
				r.report("%s: execution %s found no onErr: ok=%v err=%v", task, tok, ok, err)
				return errors.New("no onErr")
			}
			tok += ":" + map[bool]string{true: workflow.StatusCancelled, false: workflow.StatusFailed}[workflow.IsCancelled(&routed)]
		}
		if d.reentry {
			var out struct {
				Trail []string `json:"trail"`
			}
			yield, err := f.Subgraph(d.child, nil, &out)
			switch {
			case yield:
				r.report("%s: execution %s expected its child's outcome, but spawned another", task, tok)
				return errors.New("no child outcome")
			case workflow.IsCancelled(err):
				tok += ":" + workflow.StatusCancelled
			case err != nil:
				tok += ":" + workflow.StatusFailed
			default:
				tok += ":" + strings.Join(out.Trail, ".")
			}
		}
		if err := f.Set("trail", []string{tok}); err != nil {
			return err
		}
		if d.inside != nil {
			close(d.inside)
			select {
			case <-d.hold:
			case <-r.done:
				return errors.New("the history is over")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		switch d.behavior {
		case hSleep:
			f.Sleep(hSleepFor)
		case hRetry:
			if !f.Retry(0, 1, 0, 0) {
				r.report("%s: execution %s could not retry", task, tok)
			}
		case hInterrupt:
			yield, err := f.Interrupt(map[string]any{"ask": tok}, nil)
			if err != nil || !yield {
				r.report("%s: execution %s could not interrupt: yield=%v err=%v", task, tok, yield, err)
			}
		case hSpawn:
			yield, err := f.Subgraph(d.child, nil, nil)
			if err != nil || !yield {
				r.report("%s: execution %s could not spawn its child: yield=%v err=%v", task, tok, yield, err)
			}
		case hFail:
			return errors.New("%s", hFailPrefix+d.token)
		}
		return nil
	}
}

// timeout is how long the driver waits for the engine to reach a point the model predicts, as the enginetest
// helpers take it: unscaled, since they stretch it under -race themselves.
func (r *hRunner) timeout() time.Duration { return 10 * time.Second }

// scaled is timeout as a wait of the driver's own takes it.
func (r *hRunner) scaled() time.Duration { return r.timeout() * enginetest.TimeoutScale() }

// wakeSleepers ends the sleeps of the tree's pending steps - only the marked ones, with marked - where the model
// wakes them: their not_before comes to now, as if the hour had passed, so an operation that must land while a step
// sleeps cannot race the clock. Each is offered to the dispatcher as a step coming due would be: the piston
// schedules its next look by the earliest not_before it saw, an hour off.
func (r *hRunner) wakeSleepers(marked bool) {
	r.t.Helper()
	ctx := r.t.Context()
	sh, err := r.e.db.Shard(r.shard)
	if err != nil {
		r.t.Fatal(err)
	}
	db := sh.DB()
	rows, err := db.QueryContext(ctx,
		"SELECT step_id, priority FROM dwarf_steps WHERE status='"+workflow.StatusPending+"' AND not_before>NOW_UTC()"+
			" AND flow_id IN (SELECT flow_id FROM dwarf_flows WHERE root_flow_id=?) AND (cancelling=1 OR ?=0)", r.flowID, map[bool]int{true: 1}[marked])
	if err != nil {
		r.t.Fatal(err)
	}
	var sleepers [][2]int
	for rows.Next() {
		var stepID, priority int
		if err := rows.Scan(&stepID, &priority); err != nil {
			rows.Close()
			r.t.Fatal(err)
		}
		sleepers = append(sleepers, [2]int{stepID, priority})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		r.t.Fatal(err)
	}
	for _, s := range sleepers {
		if _, err := db.ExecContext(ctx, "UPDATE dwarf_steps SET not_before=NOW_UTC() WHERE step_id=?", s[0]); err != nil {
			r.t.Fatal(err)
		}
		r.e.enqueueStepDue(ctx, r.shard, s[0], s[1])
	}
}

// expected reports whether an arrival of task is one the model accounts for: its step is pending, and not already
// at the door.
func (r *hRunner) expected(task string) bool {
	s := r.m.step(task)
	return s != nil && s.status == workflow.StatusPending && r.waiting[task] == nil
}

// receive takes one arrival off the door, or fails the test at the deadline. An arrival the model does not account
// for fails the test, unless its task is one of also: a step the driver is pulling to the door ahead of an operation
// the model has already applied.
func (r *hRunner) receive(deadline <-chan time.Time, waitingFor string, also ...*mStep) {
	r.t.Helper()
	select {
	case a := <-r.arrivals:
		if !r.expected(a.task) && !slices.ContainsFunc(also, func(s *mStep) bool { return s.task == a.task && r.waiting[a.task] == nil }) {
			r.t.Fatalf("an execution of %s arrived that the model did not predict (waiting for %s)", a.task, waitingFor)
		}
		r.waiting[a.task] = a
	case <-deadline:
		r.t.Fatalf("no execution of %s arrived", waitingFor)
	}
}

// takeArrival returns task's execution at the door, waiting for it.
func (r *hRunner) takeArrival(task string) *hArrival {
	r.t.Helper()
	deadline := time.After(r.scaled())
	for r.waiting[task] == nil {
		r.receive(deadline, task)
	}
	a := r.waiting[task]
	delete(r.waiting, task)
	return a
}

// ensureAtDoor waits for every step in steps to be at the door: claimed, so an operation landing now finds it
// running.
func (r *hRunner) ensureAtDoor(steps []*mStep) {
	r.t.Helper()
	deadline := time.After(r.scaled())
	for _, s := range steps {
		for r.waiting[s.task] == nil {
			r.receive(deadline, s.task, steps...)
		}
	}
}

// callOp calls op on the root and returns what the engine answered.
func (r *hRunner) callOp(ctx context.Context, op hOp) error {
	switch op {
	case hCancel:
		return r.e.Cancel(ctx, r.flowKey, hCancelReason)
	case hTerminate:
		return r.e.Terminate(ctx, r.flowKey, hTerminateReason)
	case hResume:
		return r.e.Resume(ctx, r.flowKey, map[string]any{"answer": fmt.Sprintf("r%d", r.m.resumes)})
	}
	return nil
}

// checkAnswer checks the engine's answer to op: refused exactly when the model says it is, and, for an operation
// whose reply was lost, the lost connection the caller sees although the operation landed.
func (r *hRunner) checkAnswer(op hOp, refused, lost bool, err error) {
	r.t.Helper()
	assert := testarossa.For(r.t)
	switch {
	case lost:
		assert.True(sequel.IsConnectionError(err), "%s with its reply lost answers a lost connection: %v", op, err)
	case refused:
		assert.Error(err, "%s was expected to be refused", op)
	default:
		assert.NoError(err, "%s", op)
	}
}

// applyOp runs op on the root and checks the answer.
func (r *hRunner) applyOp(op hOp, refused bool) {
	r.t.Helper()
	r.checkAnswer(op, refused, false, r.callOp(r.t.Context(), op))
}

// recover runs recovery on the held execution a: its lease lapses, then lease recovery or the orphan repair runs on
// its shard, as the janitor's job would.
func (r *hRunner) recover(op hOp, a *hArrival) {
	r.t.Helper()
	ctx := r.t.Context()
	sh, err := r.e.db.Shard(a.shard)
	if err != nil {
		r.t.Fatal(err)
	}
	if _, err := sh.DB().ExecContext(ctx, "UPDATE dwarf_steps SET lease_expires=DATE_ADD_MILLIS(NOW_UTC(), -60000) WHERE step_id=?", a.stepID); err != nil {
		r.t.Fatal(err)
	}
	switch op {
	case hLeaseExpiry:
		r.e.recoverLeases(ctx)
	case hOrphanRepair:
		if err := r.e.recoverOrphanedFlows(ctx, sh.DB(), a.shard); err != nil {
			r.t.Fatal(err)
		}
	}
}

// lostReplyOf arms the loss of the reply to the commit kind (lostCommit) an execution of task makes; a is the
// execution, child the workflow URL it spawns. The loss is held before the engine's retry, except a spawn's: its
// fault reports an error of its own and passes no CheckpointCommitLost, and a spawn closes no outcome its retry
// could rewrite, so it is only armed (the returned hold is nil) and checked to have fired.
func (r *hRunner) lostReplyOf(kind, task string, a *hArrival, child string) (l *lostReply, fault string) {
	switch kind {
	case "advance":
		l = armLostReply(r.t, r.e, diagnostics.FaultAdvanceCommitLost, task)
	case "completeFlow":
		l = armLostReply(r.t, r.e, diagnostics.FaultCompleteFlowCommitLost, strconv.Itoa(a.flowID))
	case "failStep":
		l = armLostReply(r.t, r.e, diagnostics.FaultFailStepCommitLost, task)
	case "spawn":
		fault = diagnostics.Join(diagnostics.FaultSubgraphSpawnCommitLost, child)
		r.e.diags.Seams().Inject(fault)
		return nil, fault
	default:
		r.t.Fatalf("no lost reply for %s", kind)
	}
	return l, l.fault
}

// afterOp follows an operation that just landed. A Terminate that closed the tree records the closed outcomes
// there, before a held execution goes on: what it writes next must not move them, and a record taken after it
// would hold whatever it wrote. The executions still waiting at the door are then let go; their writes are fenced,
// and the event awaits their dispatches before it checks anything.
func (r *hRunner) afterOp(op hOp) {
	r.t.Helper()
	if op != hTerminate || r.m.root.status != workflow.StatusTerminated {
		return
	}
	r.checkKept()
	r.keep()
	for task, a := range r.waiting {
		s := r.m.step(task)
		r.admit(s, a, hDirective{abandon: true})
		r.released = append(r.released, s)
		delete(r.waiting, task)
	}
}

// admit lets a's execution of s in with d, counting its dispatch.
func (r *hRunner) admit(s *mStep, a *hArrival, d hDirective) {
	r.refs[s] = [2]int{a.shard, a.stepID}
	r.claims[s]++
	a.admit <- d
}

// awaitDispatches waits for every counted dispatch of s to have returned.
func (r *hRunner) awaitDispatches(s *mStep) {
	r.t.Helper()
	ref := r.refs[s]
	name := diagnostics.Join(diagnostics.CheckpointClaimedDispatchDone, strconv.Itoa(ref[0]), strconv.Itoa(ref[1]))
	enginetest.AwaitVisitCount(r.t, r.e.diags.Seams(), r.claims[s], r.timeout(), name)
}

// awaitReleased waits for the dispatches of the executions a Terminate let go from the door.
func (r *hRunner) awaitReleased() {
	r.t.Helper()
	for _, s := range r.released {
		r.awaitDispatches(s)
	}
	r.released = nil
}

// awaitPreempted waits for the dispatches that preempt the steps the last event marked before their claim. The
// model settles a preemption at once and the engine in a dispatch of its own, after the claim - a sleeping step's
// once it wakes, which the model's preemption stands for - so an event that follows must not land before it.
func (r *hRunner) awaitPreempted() {
	r.t.Helper()
	r.wakeSleepers(true)
	for _, s := range r.m.preempted {
		r.awaitPreemption(s)
	}
}

// awaitPreemption waits once for the dispatch that preempts s.
func (r *hRunner) awaitPreemption(s *mStep) {
	r.t.Helper()
	if r.awaited[s] {
		return
	}
	r.awaited[s] = true
	if _, ok := r.refs[s]; !ok {
		r.refs[s] = [2]int{r.shard, r.newestStepID(s.task)}
	}
	r.claims[s]++
	r.awaitDispatches(s)
}

// directive is what the next execution of s does with behavior.
func (r *hRunner) directive(s *mStep, behavior hBehavior) hDirective {
	d := hDirective{behavior: behavior, input: s.merged(), token: fmt.Sprintf("%s%d", s.task, s.execs+1), resumed: s.resumeData != "", reentry: s.subgraphDone, onErr: s.onErr != ""}
	if child := s.flow.g.calls[s.task]; child != "" {
		d.child = "history/" + string(child)
	}
	return d
}

// newestStepID waits for a step of task to exist in the tree, and returns the newest one's id.
func (r *hRunner) newestStepID(task string) int {
	r.t.Helper()
	assert := testarossa.For(r.t)
	var id int
	assert.True(enginetest.WaitUntil(r.t, r.timeout(), func() bool {
		id = enginetest.CountRows(r.t, r.e, r.shard, "SELECT COALESCE(MAX(s.step_id), 0) FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE f.root_flow_id=? AND s.task_name=?", r.flowID, task)
		return id > 0
	}), "no %s step in the tree", task)
	return id
}

// holdAt is the targeted checkpoint an operation at point waits at, for a's step, or "" for none.
func holdAt(point hPoint, a *hArrival) string {
	switch point {
	case hBeforeTransition:
		return diagnostics.Join(diagnostics.CheckpointBeforeTransitionTx, strconv.Itoa(a.shard), strconv.Itoa(a.stepID))
	case hBeforeRewind:
		return diagnostics.Join(diagnostics.CheckpointBeforeRetryRewind, strconv.Itoa(a.shard), strconv.Itoa(a.stepID))
	case hBeforeInterrupt:
		return diagnostics.Join(diagnostics.CheckpointBeforeInterruptTx, strconv.Itoa(a.shard), strconv.Itoa(a.stepID))
	}
	return ""
}

// others is the steps at the door except the ones the event runs.
func (r *hRunner) others(except ...string) []*mStep {
	var out []*mStep
	for _, s := range r.m.atDoor() {
		if !slices.Contains(except, s.task) {
			out = append(out, s)
		}
	}
	return out
}

// step runs one event on the engine and the model.
func (r *hRunner) step(ev hEvent) {
	r.t.Helper()
	wasClosed := r.m.closed()
	r.cov.add("%s", r.m.situation(ev))
	switch {
	case ev.Task == "":
		r.restOp(ev)
	case ev.With != "":
		r.runTogether(ev)
	case ev.At == hAcross:
		r.runAcross(ev)
	default:
		r.runOne(ev)
	}
	for _, name := range r.m.settled(wasClosed) {
		r.cov.add("%s", name)
	}
	r.mu.Lock()
	for _, p := range r.problems {
		r.t.Errorf("event %s: %s", ev, p)
	}
	r.problems = nil
	r.mu.Unlock()
	if r.t.Failed() {
		return
	}
	r.checkFlows()
	r.checkKept()
	r.keep()
}

// restOp applies an operation between executions.
func (r *hRunner) restOp(ev hEvent) {
	r.t.Helper()
	r.ensureAtDoor(r.m.atDoor())
	fault := ""
	if ev.Lost {
		fault = map[hOp]string{hResume: diagnostics.FaultResumeCommitLost, hTerminate: diagnostics.FaultTerminateCommitLost}[ev.Op]
		fault = diagnostics.Join(fault, strconv.Itoa(r.flowID))
		r.e.diags.Seams().Inject(fault)
	}
	refused, err := r.m.apply(ev)
	if err != nil {
		r.t.Fatal(err)
	}
	r.checkAnswer(ev.Op, refused, ev.Lost, r.callOp(r.t.Context(), ev.Op))
	if ev.Lost {
		assertFired(r.t, r.e, fault)
	}
	r.afterOp(ev.Op)
	r.wakeSleepers(false)
	r.awaitPreempted()
	r.awaitReleased()
	r.settle()
}

// runOne runs one execution, with an operation at a point of it if the event names one.
func (r *hRunner) runOne(ev hEvent) {
	r.t.Helper()
	seams := r.e.diags.Seams()
	r.m.wake()
	r.wakeSleepers(false)
	s, err := r.m.runnableTask(ev.Task, ev.Behavior)
	if err != nil {
		r.t.Fatalf("event %s: %v", ev, err)
	}
	d := r.directive(s, ev.Behavior)
	lost := ""
	if ev.Lost {
		lost = lostCommit(s, ev.Behavior)
	}
	others := r.others(ev.Task)
	a := r.takeArrival(ev.Task)
	hold := ""
	if ev.Op != "" {
		hold = holdAt(ev.At, a)
	}
	if ev.Op != "" && ev.At == hMidTask {
		d.inside, d.hold = make(chan struct{}), make(chan struct{})
	}
	if hold != "" {
		seams.Break(hold)
		r.t.Cleanup(func() { seams.Resume(hold) })
	}
	var l *lostReply
	fault := ""
	if lost != "" {
		l, fault = r.lostReplyOf(lost, ev.Task, a, d.child)
	}
	refused, err := r.m.apply(ev)
	if err != nil {
		r.t.Fatal(err)
	}
	r.admit(s, a, d)
	switch {
	case ev.Op == hLeaseExpiry:
		// The execution is held inside its task while its lease lapses and recovery re-pends the step. It is let
		// go only once the step is claimed again, which bumps the generation its every write is fenced on: let go
		// before, its completion would land on the step recovery had re-pended.
		r.awaitInside(ev, d)
		r.ensureAtDoor(others)
		r.recover(ev.Op, a)
		if s.status == workflow.StatusPending {
			r.ensureAtDoor([]*mStep{s})
		} else {
			// A marked step is preempted by that claim instead: its dispatch returns while the held one is
			// still held.
			r.awaited[s] = true
			r.claims[s]++
			name := diagnostics.Join(diagnostics.CheckpointClaimedDispatchDone, strconv.Itoa(a.shard), strconv.Itoa(a.stepID))
			enginetest.AwaitVisitCount(r.t, r.e.diags.Seams(), r.claims[s]-1, r.timeout(), name)
		}
		close(d.hold)
	case ev.Op == hOrphanRepair:
		// The execution is held after its completion write while its lease lapses and the orphan repair rewinds the
		// step for a replay. The replay - a dispatch that runs no task - and the held worker's own transition then
		// race to transition it, and the worker's is fenced.
		r.awaitHold(ev, hold)
		r.ensureAtDoor(others)
		r.recover(ev.Op, a)
		r.claims[s]++ // the replay
		seams.Resume(hold)
	case d.inside != nil:
		r.awaitInside(ev, d)
		r.ensureAtDoor(others)
		r.applyOp(ev.Op, refused)
		r.afterOp(ev.Op)
		close(d.hold)
	case hold != "":
		r.awaitHold(ev, hold)
		r.ensureAtDoor(others)
		r.applyOp(ev.Op, refused)
		r.afterOp(ev.Op)
		seams.Resume(hold)
	case l != nil:
		// The commit landed and its reply is lost: the outcomes it left are checked against the model before the
		// retry that follows can touch them, and again after.
		l.committed()
		r.checkKept()
		r.keep()
		l.release()
	}
	r.awaitDispatches(s)
	if fault != "" {
		assertFired(r.t, r.e, fault)
	}
	r.awaitPreempted()
	r.awaitReleased()
	r.settle()
}

// awaitInside waits for an execution admitted with d to be held inside its task.
func (r *hRunner) awaitInside(ev hEvent, d hDirective) {
	r.t.Helper()
	select {
	case <-d.inside:
	case <-time.After(r.scaled()):
		r.t.Fatalf("event %s: the task never ran", ev)
	}
}

// awaitHold waits for the engine to be held at checkpoint hold.
func (r *hRunner) awaitHold(ev hEvent, hold string) {
	r.t.Helper()
	if !r.e.diags.Seams().WaitTimeout(r.t.Context(), hold, r.scaled()) {
		r.t.Fatalf("event %s: the engine never reached %s", ev, hold)
	}
}

// runAcross runs one execution while an operation is held midway: Cancel between its watermark's commit and its
// mark, Terminate between its scan of the tree and its transaction. The execution starts after the operation's
// first half and ends before its second, which is the overlap neither half alone produces: a successor inserted
// after the watermark, a child spawned after the scan.
func (r *hRunner) runAcross(ev hEvent) {
	r.t.Helper()
	seams := r.e.diags.Seams()
	r.m.wake()
	r.wakeSleepers(false)
	s, err := r.m.runnableTask(ev.Task, ev.Behavior)
	if err != nil {
		r.t.Fatalf("event %s: %v", ev, err)
	}
	d := r.directive(s, ev.Behavior)
	others := r.others(ev.Task)
	a := r.takeArrival(ev.Task)
	r.ensureAtDoor(others)
	hold := map[hOp]string{hCancel: diagnostics.CheckpointCancelBeforeMark, hTerminate: diagnostics.CheckpointTerminateAfterScan}[ev.Op]
	seams.Break(hold)
	r.t.Cleanup(func() { seams.Resume(hold) })
	if _, err := r.m.apply(ev); err != nil {
		r.t.Fatal(err)
	}
	answered := make(chan error, 1)
	go func() { answered <- r.callOp(r.t.Context(), ev.Op) }()
	r.awaitHold(ev, hold)
	r.admit(s, a, d)
	r.awaitDispatches(s)
	// What the execution itself preempted - a successor inserted marked after Cancel's watermark - goes now, a
	// sleeper woken for it; then every step at the door is claimed before the second half lands, as it would be
	// before any operation.
	r.wakeSleepers(true)
	for _, p := range r.m.midPreempt {
		r.awaitPreemption(p)
	}
	r.ensureAtDoor(r.m.midDoor)
	seams.Resume(hold)
	select {
	case err := <-answered:
		r.checkAnswer(ev.Op, false, false, err)
	case <-time.After(r.scaled()):
		r.t.Fatalf("event %s: %s never returned", ev, ev.Op)
	}
	r.afterOp(ev.Op)
	r.awaitPreempted()
	r.awaitReleased()
	r.settle()
}

// runTogether admits two executions at once: racing, or both held before their transitions while an operation
// lands, then released in the event's order.
func (r *hRunner) runTogether(ev hEvent) {
	r.t.Helper()
	seams := r.e.diags.Seams()
	r.m.wake()
	r.wakeSleepers(false)
	sa, err := r.m.runnableTask(ev.Task, ev.Behavior)
	if err != nil {
		r.t.Fatalf("event %s: %v", ev, err)
	}
	sb, err := r.m.runnableTask(ev.With, ev.WithBehavior)
	if err != nil {
		r.t.Fatalf("event %s: %v", ev, err)
	}
	da, db := r.directive(sa, ev.Behavior), r.directive(sb, ev.WithBehavior)
	others := r.others(ev.Task, ev.With)
	a, b := r.takeArrival(ev.Task), r.takeArrival(ev.With)
	holdA, holdB := "", ""
	if ev.At != "" {
		holdA, holdB = holdAt(ev.At, a), holdAt(ev.At, b)
		seams.Break(holdA)
		seams.Break(holdB)
		r.t.Cleanup(func() { seams.Resume(holdA); seams.Resume(holdB) })
	}
	refused, err := r.m.apply(ev)
	if err != nil {
		r.t.Fatal(err)
	}
	r.admit(sa, a, da)
	r.admit(sb, b, db)
	if ev.At != "" {
		for _, hold := range []string{holdA, holdB} {
			r.awaitHold(ev, hold)
		}
		r.ensureAtDoor(others)
		if ev.Op != "" {
			r.applyOp(ev.Op, refused)
			r.afterOp(ev.Op)
		}
		seams.Resume(holdA)
		r.awaitDispatches(sa)
		seams.Resume(holdB)
	}
	r.awaitDispatches(sa)
	r.awaitDispatches(sb)
	r.awaitPreempted()
	r.awaitReleased()
	r.settle()
}

// settle waits for the flow to stand where the model says once an event's work is done, and checks it there: an
// interrupted flow's prompt, a closed flow's outcome.
func (r *hRunner) settle() {
	r.t.Helper()
	assert := testarossa.For(r.t)
	if r.m.root.status == workflow.StatusRunning {
		return // the next execution's arrival is the check
	}
	// Await, watching the door: an execution the model does not account for is the engine running what the model
	// says it would not.
	type awaited struct {
		outcome *workflow.FlowOutcome
		err     error
	}
	ctx, cancel := context.WithTimeout(r.t.Context(), r.scaled())
	defer cancel()
	stopped := make(chan awaited, 1)
	go func() {
		outcome, err := r.e.Await(ctx, r.flowKey)
		stopped <- awaited{outcome, err}
	}()
	var outcome *workflow.FlowOutcome
	for outcome == nil {
		select {
		case a := <-r.arrivals:
			if !r.expected(a.task) {
				r.t.Fatalf("an execution of %s arrived, the model has the flow %s", a.task, r.m.root.status)
			}
			r.waiting[a.task] = a
		case got := <-stopped:
			if got.err != nil {
				r.t.Fatalf("the flow did not stop as %s: %v", r.m.root.status, got.err)
			}
			outcome = got.outcome
		}
	}
	if outcome.Status != r.m.root.status {
		r.t.Fatalf("the flow is %s, the model has it %s", outcome.Status, r.m.root.status)
	}
	if r.m.root.status == workflow.StatusInterrupted {
		assert.Equal(nonNil(r.m.snapshotStep().merged()), nonNil(outcome.State.GetStrings("trail")), "the interrupted step's state")
		assert.Equal(r.m.interrupts[0].ask, outcome.InterruptPayload.GetString("ask"), "the interrupt payload")
		return
	}
	assert.Equal(nonNil(r.m.root.final), nonNil(outcome.State.GetStrings("trail")), "the final state")
	// Each reason belongs to its own status alone: a Cancel that landed on a flow that then completed or failed
	// leaves no reason on the outcome.
	want := map[string]string{r.m.root.status: "*"}
	assert.Equal(map[bool]string{true: hTerminateReason}[want[workflow.StatusTerminated] != ""], outcome.TerminateReason, "the terminate reason")
	assert.Equal(map[bool]string{true: hCancelReason}[want[workflow.StatusCancelled] != ""], outcome.CancelReason, "the cancel reason")
	if r.m.root.status == workflow.StatusFailed {
		assert.True(slices.ContainsFunc(r.m.root.failed, func(run string) bool { return strings.Contains(outcome.Error, hFailPrefix+run) }),
			"the error %q names none of the runs that failed: %v", outcome.Error, r.m.root.failed)
	} else {
		assert.Equal("", outcome.Error, "the error")
	}
}

// keep records the closed outcomes again when the model has a flow closed that the last record did not hold, so
// every later event is compared against them.
func (r *hRunner) keep() {
	r.t.Helper()
	newly := false
	for _, f := range r.m.flows {
		if f.closed() && !r.keptFlows[f] {
			r.keptFlows[f], newly = true, true
		}
	}
	if newly {
		r.kept, r.keptReaped = enginetest.CaptureOutcomes(r.t, r.e), r.m.reaped
	}
}

// checkKept fails the test for a recorded closed outcome that moved. The only closed flows that may leave the live
// tables are children a retry of their caller deleted since the record.
func (r *hRunner) checkKept() {
	r.t.Helper()
	if len(r.keptFlows) == 0 {
		return
	}
	assert := testarossa.For(r.t)
	removed := enginetest.AssertOutcomesKept(r.t, r.e, r.kept)
	assert.Len(removed, r.m.reaped-r.keptReaped, "closed flows that left the live tables: %v", removed)
}

// checkFlows compares the tree's flows with the model's, in the order they were created: each one's graph and
// status, and a closed one's final state, which the model froze at its close.
func (r *hRunner) checkFlows() {
	r.t.Helper()
	assert := testarossa.For(r.t)
	want := []string{}
	for _, f := range r.m.flows {
		line := r.m.url(f) + ":" + f.status
		if f.closed() {
			line += fmt.Sprint(nonNil(f.final))
		}
		want = append(want, line)
	}
	sh, err := r.e.db.Shard(r.shard)
	if err != nil {
		r.t.Fatal(err)
	}
	rows, err := sh.DB().QueryContext(r.t.Context(), "SELECT workflow_url, status, final_state FROM dwarf_flows WHERE root_flow_id=? ORDER BY flow_id", r.flowID)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	got := []string{}
	for rows.Next() {
		var url, status string
		var final []byte
		if err := rows.Scan(&url, &status, &final); err != nil {
			r.t.Fatal(err)
		}
		line := url + ":" + status
		if workflow.IsTerminalStatus(status) {
			var fs struct {
				Trail []string `json:"trail"`
			}
			if err := json.Unmarshal(final, &fs); err != nil {
				r.t.Fatalf("flow final state %s: %v", final, err)
			}
			line += fmt.Sprint(nonNil(fs.Trail))
		}
		got = append(got, line)
	}
	if err := rows.Err(); err != nil {
		r.t.Fatal(err)
	}
	assert.Equal(want, got, "the flows")
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// stepRows reads the tree's steps as task:status, in the order they were inserted.
func (r *hRunner) stepRows() []string {
	r.t.Helper()
	sh, err := r.e.db.Shard(r.shard)
	if err != nil {
		r.t.Fatal(err)
	}
	rows, err := sh.DB().QueryContext(r.t.Context(), "SELECT s.task_name, s.status FROM dwarf_steps s JOIN dwarf_flows f ON f.flow_id=s.flow_id WHERE f.root_flow_id=? ORDER BY s.step_id", r.flowID)
	if err != nil {
		r.t.Fatal(err)
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var task, status string
		if err := rows.Scan(&task, &status); err != nil {
			r.t.Fatal(err)
		}
		out = append(out, task+":"+status)
	}
	if err := rows.Err(); err != nil {
		r.t.Fatal(err)
	}
	return out
}

// noStray fails the test if an execution is at the door or arrives within a moment: the model ran everything it
// predicted.
func (r *hRunner) noStray(when string) {
	r.t.Helper()
	for task := range r.waiting {
		r.t.Errorf("%s: an execution of %s waits at the door", when, task)
	}
	select {
	case a := <-r.arrivals:
		r.t.Errorf("%s: an execution of %s arrived that the model did not predict", when, a.task)
	case <-time.After(200 * time.Millisecond * enginetest.TimeoutScale()):
	}
}

// runHistory drives a fresh engine through h and checks it against the model.
func runHistory(t *testing.T, h hHistory, cov *hCoverage) {
	assert := testarossa.For(t)
	r := &hRunner{t: t, h: h, m: newHModel(h.Shape), cov: cov, arrivals: make(chan *hArrival), waiting: map[string]*hArrival{}, refs: map[*mStep][2]int{}, claims: map[*mStep]int{}, awaited: map[*mStep]bool{}, done: make(chan struct{}), keptFlows: map[*mFlow]bool{}}
	t.Cleanup(func() {
		if t.Failed() {
			b, _ := json.Marshal(h)
			t.Logf("replay with DWARF_TESTING_HISTORY='%s'", b)
			for i, ev := range h.Events {
				t.Logf("  %2d %s", i, ev)
			}
		}
	})
	r.e = NewEngineUnderTest(t.Name())
	t.Cleanup(func() { r.e.Shutdown(context.Background()) })
	t.Cleanup(func() { close(r.done) }) // before the shutdown, so no task waits at the door through it
	r.e.SetHost(r.mux())
	r.e.persistBackoff = []time.Duration{time.Millisecond, time.Millisecond}
	assert.Must(assert.NoError(r.e.Startup(t.Context())))

	var err error
	r.flowKey, err = r.e.Create(t.Context(), "history/"+string(h.Shape), nil, nil)
	assert.Must(assert.NoError(err))
	r.shard, _, r.flowID, _, err = keys.ParseFlowKey(r.flowKey)
	assert.Must(assert.NoError(err))

	for i, ev := range h.Events {
		t.Logf("%2d %s", i, ev)
		r.step(ev)
		if t.Failed() {
			return
		}
	}
	if !r.m.closed() {
		t.Fatalf("the history ends with the flow %s", r.m.root.status)
	}
	r.noStray("after the history")
	wantSteps := []string{}
	for _, s := range r.m.steps {
		wantSteps = append(wantSteps, s.task+":"+s.status)
	}
	assert.Equal(wantSteps, r.stepRows(), "the step rows")
	r.mu.Lock()
	assert.Len(r.problems, 0, "the task handlers saw: %v", r.problems)
	r.mu.Unlock()

	sweepEveryDetector(t, r.e)
	r.noStray("after every recovery detector")
	r.checkFlows()
	r.checkKept()
	enginetest.AssertInvariants(t, r.e)
}

// hCoverage counts the situations the histories of one test reached: each kind of event in the state it landed
// on, and each way a flow closed. A seed count says nothing about breadth; this does.
type hCoverage struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *hCoverage) add(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[fmt.Sprintf(format, args...)]++
}

func (c *hCoverage) log(t *testing.T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	names := make([]string, 0, len(c.counts))
	for name := range c.counts {
		names = append(names, name)
	}
	sort.Strings(names)
	var b strings.Builder
	for _, name := range names {
		fmt.Fprintf(&b, "\n  %4d  %s", c.counts[name], name)
	}
	t.Logf("situations reached: %d%s", len(names), b.String())
}

// minimizeHistory reduces a failing history to one from which no single event can be removed and the history still
// fail, and logs it for replay. Each candidate is h without one event - kept only if the model still accepts every
// event, a Terminate appended if the flow would end open - run as a subtest on a fresh engine: a candidate that
// fails replaces h, and the passes repeat until one removes nothing. It keeps any failure, not only the original's,
// so the messages of the last failing candidate are the ones to read. The test fails, as the history it was given
// does.
func minimizeHistory(t *testing.T, h hHistory) {
	cov := &hCoverage{counts: map[string]int{}}
	fails := func(name string, c hHistory) bool {
		return !t.Run(name, func(t *testing.T) { runHistory(t, c, cov) })
	}
	if !fails("given", h) {
		t.Log("the history passes; nothing to minimize")
		return
	}
	cur := h
	for pass := 1; ; pass++ {
		removed := false
		for i := len(cur.Events) - 1; i >= 0; i-- {
			cand, ok := replayable(hHistory{Shape: cur.Shape, Events: slices.Delete(slices.Clone(cur.Events), i, i+1)})
			if ok && fails(fmt.Sprintf("pass%d/without%d", pass, i), cand) {
				cur, removed = cand, true
			}
		}
		if !removed {
			break
		}
	}
	b, _ := json.Marshal(cur)
	t.Logf("minimal history, %d of the %d events: DWARF_TESTING_HISTORY='%s'", len(cur.Events), len(h.Events), b)
}

// replayable reports whether the model accepts every event of h, returning h with a Terminate appended when its flow
// would end open.
func replayable(h hHistory) (hHistory, bool) {
	m := newHModel(h.Shape)
	for _, ev := range h.Events {
		if _, err := m.apply(ev); err != nil {
			return h, false
		}
	}
	if !m.closed() {
		h.Events = append(h.Events, hEvent{Op: hTerminate})
	}
	return h, true
}

const (
	hBaseSeeds   = 24     // the seeds a default run takes in order, before the floor's own
	hFloorSearch = 20_000 // the last seed searched for a floor situation the base seeds miss
)

// historySeeds is the seeds a run explores, and whether they are the default run's, which the floor holds to. A
// default run takes seeds 1 to hBaseSeeds, then for each floor pattern they do not reach, the lowest later seed whose
// history does - read off the model alone, which generates a history in microseconds. A floor pattern no seed up to
// hFloorSearch reaches fails the test: the generator no longer produces it.
func historySeeds(t *testing.T, shape hShape) (seeds []uint64, floored bool) {
	if s := os.Getenv("DWARF_TESTING_HISTORY_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("DWARF_TESTING_HISTORY_SEED: %v", err)
		}
		return []uint64{seed}, false
	}
	n, explicit := hBaseSeeds, false
	if testing.Short() {
		n, explicit = 8, true
	}
	if s := os.Getenv("DWARF_TESTING_HISTORY_SEEDS"); s != "" {
		var err error
		if n, err = strconv.Atoi(s); err != nil {
			t.Fatalf("DWARF_TESTING_HISTORY_SEEDS: %v", err)
		}
		explicit = true
	}
	var reached []string
	for seed := uint64(1); seed <= uint64(n); seed++ {
		seeds = append(seeds, seed)
		_, sits := generateHistory(shape, seed)
		reached = append(reached, sits...)
	}
	if explicit {
		return seeds, false
	}
	generated := map[uint64][]string{}
	for _, pattern := range hFloor[shape] {
		re := regexp.MustCompile(pattern)
		if slices.ContainsFunc(reached, re.MatchString) {
			continue
		}
		found := false
		for seed := uint64(n + 1); seed <= hFloorSearch && !found; seed++ {
			sits, ok := generated[seed]
			if !ok {
				_, sits = generateHistory(shape, seed)
				generated[seed] = sits
			}
			if slices.ContainsFunc(sits, re.MatchString) {
				seeds, reached, found = append(seeds, seed), append(reached, sits...), true
			}
		}
		if !found {
			t.Errorf("no seed up to %d generates a situation matching %q: the generator no longer produces it", hFloorSearch, pattern)
		}
	}
	return seeds, true
}

// hFloor is, per shape, the situations a default run must reach - each a pattern over the coverage's names. It names
// what the histories exist to exercise: every way an operation or a recovery lands, every way a flow closes, every
// lost reply the shape commits, and the shape's own mechanisms. historySeeds adds a seed for each pattern the base
// seeds miss, so the floor holds as long as the generator can produce every situation on it.
var hFloor = map[hShape][]string{
	hLinear: {
		`closed completed`, `closed failed`, `closed cancelled`, `closed terminated`,
		`run (ok|sleep) \+cancel@midTask`, `run (ok|sleep) \+cancel@beforeTransition`, `run retry \+cancel@beforeRewind`,
		`run \w+ \+cancel@across`, `run \w+ \+terminate@(midTask|beforeTransition)`, `run \w+ \+terminate@across`,
		`run interrupt \+(cancel|terminate)@beforeInterrupt`,
		`run ok \+leaseExpiry@midTask`, `run \w+ \+orphanRepair@beforeTransition`,
		`the reply to its advance lost`, `the reply to its completeFlow lost`, `the reply to its failStep lost`,
		`op resume on interrupted`, `op cancel on interrupted`, `op cancel on asleep`, `op terminate on interrupted`,
		`op resume \(reply lost\)`, `op terminate \(reply lost\)`,
		`run \w+ resumed`,
	},
	hCohort: {
		`closed completed`, `closed failed by cohort failed`, `closed cancelled by cohort cancelled`, `closed terminated`,
		`together ok & ok, held @beforeTransition \+cancel`, `together ok & ok, held @beforeTransition \+terminate`,
		`together interrupt & interrupt, held @beforeInterrupt`, `together \w+ & \w+$`,
		`run \w+ \+cancel@across`, `run \w+ \+terminate@across`,
		`run ok \+leaseExpiry@midTask`, `run \w+ \+orphanRepair@beforeTransition`, `the reply to its advance lost`,
		`op resume on interrupted`, `op cancel on interrupted`,
	},
	hChild: {
		`closed completed`, `closed failed`, `closed cancelled`, `closed terminated`,
		`run spawn$`, `run spawn \+cancel@midTask`, `run spawn \+terminate@across`, `run \w+ after its child`,
		`run \w+ \+cancel@across`, `run ok \+leaseExpiry@midTask`, `run \w+ \+orphanRepair@beforeTransition`,
		`the reply to its spawn lost`, `the reply to its completeFlow lost`,
		`op resume on interrupted`, `op cancel on interrupted`, `op resume \(reply lost\)`,
	},
	hRecover: {
		`closed completed`, `closed failed`, `closed cancelled`, `closed terminated`,
		`routed to a handler by an error$`, `routed to a handler by an error while marked`,
		`routed to a handler by a covered completion`, `routed to a handler by a preemption`,
		`run \w+ \+cancel@across`, `run ok \+leaseExpiry@midTask`, `run fail \+orphanRepair@beforeTransition`,
		`the reply to its advance lost`,
	},
}

// checkFloor fails the test for every pattern of shape's floor that no situation the run reached matches.
func checkFloor(t *testing.T, shape hShape, cov *hCoverage) {
	t.Helper()
	cov.mu.Lock()
	defer cov.mu.Unlock()
	reached := slices.Collect(maps.Keys(cov.counts))
	for _, pattern := range hFloor[shape] {
		re := regexp.MustCompile(pattern)
		if !slices.ContainsFunc(reached, re.MatchString) {
			t.Errorf("the default seeds no longer reach a situation matching %q: raise their count or the weight that draws it", pattern)
		}
	}
}

// runHistories runs the seeded histories of shape, or the one DWARF_TESTING_HISTORY holds.
func runHistories(t *testing.T, shape hShape) {
	cov := &hCoverage{counts: map[string]int{}}
	if raw := os.Getenv("DWARF_TESTING_HISTORY"); raw != "" {
		if path, ok := strings.CutPrefix(raw, "@"); ok {
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			raw = string(b)
		}
		var h hHistory
		if err := json.Unmarshal([]byte(raw), &h); err != nil {
			t.Fatalf("DWARF_TESTING_HISTORY: %v", err)
		}
		if h.Shape != shape {
			t.Skipf("DWARF_TESTING_HISTORY holds a %s history", h.Shape)
		}
		if os.Getenv("DWARF_TESTING_HISTORY_MINIMIZE") != "" {
			minimizeHistory(t, h)
			return
		}
		runHistory(t, h, cov)
		return
	}
	seeds, floored := historySeeds(t, shape)
	t.Cleanup(func() {
		cov.log(t)
		if floored && !t.Failed() {
			checkFloor(t, shape, cov)
		}
	})
	for _, seed := range seeds {
		h, _ := generateHistory(shape, seed)
		t.Run("seed="+strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			runHistory(t, h, cov)
		})
	}
}

func TestHistories_Linear(t *testing.T) {
	t.Parallel()
	runHistories(t, hLinear)
}

func TestHistories_Cohort(t *testing.T) {
	t.Parallel()
	runHistories(t, hCohort)
}

func TestHistories_Child(t *testing.T) {
	t.Parallel()
	runHistories(t, hChild)
}

func TestHistories_Recover(t *testing.T) {
	t.Parallel()
	runHistories(t, hRecover)
}
