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
	"os"
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
	"github.com/microbus-io/testarossa"
)

// Generated operation histories. Each history is a list of events drawn from a seed by the reference model
// (historymodel_test.go): a task's next execution with what it does, an operation landing at a chosen point of
// that execution, or an operation landing on the flow at rest. The test drives one engine through the history in
// lockstep with the model. Every execution enters the test's task handler and waits there to be admitted, so the
// order the engine runs things in is the history's order and not the scheduler's, and an execution the model did
// not predict is caught at the door. An operation lands at its point by a hold: inside the task, or at the
// checkpoint after the completion write or before the retry's rewind.
//
// After every event the flow must stand where the model says, and at the end its outcome, its step rows and its
// executions must be the model's, no closed outcome may have moved - through the operations a history applies
// after the close, and through every recovery detector - and the invariant checker must find nothing.
//
// Environment:
//
//	DWARF_HISTORY_SEEDS=n     run seeds 1..n (default 32; 8 with -short)
//	DWARF_HISTORY_SEED=s      run seed s alone
//	DWARF_HISTORY='{...}'     replay one history, as a failure prints it ('@path' reads it from a file; a relative
//	                          path is from the package directory)

const (
	hSleepFor        = time.Second // a sleep long enough for an operation to land while the next step waits
	hCancelReason    = "history cancel"
	hTerminateReason = "history terminate"
	hFailPrefix      = "history fail "
)

// hArrival is an execution waiting at the task handler's door.
type hArrival struct {
	task    string
	stepKey string
	admit   chan hDirective
}

// hDirective is what an admitted execution does.
type hDirective struct {
	behavior hBehavior
	token    string        // the token it contributes, before the resume data it receives
	resumed  bool          // it reads the resume data first
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
	done     chan struct{}
	kept     enginetest.Outcomes // the closed outcome, as it first closed
	closed   bool
	mu       sync.Mutex
	problems []string // what a task handler saw that the model did not predict
}

func (r *hRunner) report(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.problems = append(r.problems, fmt.Sprintf(format, args...))
}

// mux serves the history's graph, each task through the handler that waits at the door.
func (r *hRunner) mux() *hosts.Mux {
	mux := hosts.NewMux()
	tasks := hShapeTasks[r.h.Shape]
	g := workflow.NewGraph("History")
	for _, task := range tasks {
		g.SetURL(task, "history/"+task)
		mux.HandleTask("history/"+task, r.handler(task))
	}
	g.SetReducer("trail", workflow.ReducerAppend)
	g.AddTransitionChain(append(slices.Clone(tasks), workflow.End)...)
	mux.HandleGraph("history/"+string(r.h.Shape), g)
	return mux
}

func (r *hRunner) handler(task string) func(ctx context.Context, f *workflow.Flow) error {
	return func(ctx context.Context, f *workflow.Flow) error {
		a := &hArrival{task: task, stepKey: f.StepKey(), admit: make(chan hDirective, 1)}
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
		if err := f.Set("trail", []string{tok}); err != nil {
			return err
		}
		if d.inside != nil {
			close(d.inside)
			select {
			case <-d.hold:
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
		case hFail:
			return errors.New("%s", hFailPrefix+d.token)
		}
		return nil
	}
}

// timeout is how long the driver waits for the engine to reach a point the model predicts.
func (r *hRunner) timeout() time.Duration { return 10 * time.Second * enginetest.TimeoutScale() }

// awaitArrival waits for the next execution at the door, which must be task's.
func (r *hRunner) awaitArrival(task string) *hArrival {
	r.t.Helper()
	select {
	case a := <-r.arrivals:
		if a.task != task {
			r.t.Fatalf("expected an execution of %s, %s arrived", task, a.task)
		}
		return a
	case <-time.After(r.timeout() + hSleepFor):
		r.t.Fatalf("no execution of %s arrived", task)
		return nil
	}
}

// applyOp runs op on the flow and checks the engine refuses it exactly when the model says it does.
func (r *hRunner) applyOp(op hOp, refused bool) {
	r.t.Helper()
	ctx := r.t.Context()
	var err error
	switch op {
	case hCancel:
		err = r.e.Cancel(ctx, r.flowKey, hCancelReason)
	case hTerminate:
		err = r.e.Terminate(ctx, r.flowKey, hTerminateReason)
	case hResume:
		err = r.e.Resume(ctx, r.flowKey, map[string]any{"answer": fmt.Sprintf("r%d", r.m.resumes)})
	}
	if refused {
		testarossa.For(r.t).Error(err, "%s was expected to be refused", op)
	} else {
		testarossa.For(r.t).NoError(err, "%s", op)
	}
}

// step runs one event on the engine and the model.
func (r *hRunner) step(ev hEvent) {
	r.t.Helper()
	seams := r.e.diags.Seams()
	if ev.Task == "" {
		r.cov.add("op %s on %s", ev.Op, r.restingAs())
		refused, err := r.m.apply(ev)
		if err != nil {
			r.t.Fatal(err)
		}
		r.applyOp(ev.Op, refused)
		r.settle()
		return
	}
	s := r.m.runnable()
	if s == nil {
		r.t.Fatalf("event %s: no step can run", ev)
	}
	d := hDirective{behavior: ev.Behavior, token: fmt.Sprintf("%s%d", s.task, s.execs+1), resumed: s.resumeData != ""}
	if ev.Op == "" {
		r.cov.add("run %s%s", ev.Behavior, map[bool]string{true: " resumed"}[d.resumed])
	} else {
		r.cov.add("run %s%s +%s@%s on %s", ev.Behavior, map[bool]string{true: " resumed"}[d.resumed], ev.Op, ev.At, ev.Task)
	}
	a := r.awaitArrival(ev.Task)
	shard, _, _, stepID, _, err := keys.ParseStepKey(a.stepKey)
	if err != nil {
		r.t.Fatal(err)
	}
	dispatchDone := diagnostics.Join(diagnostics.CheckpointClaimedDispatchDone, strconv.Itoa(shard), strconv.Itoa(stepID))
	visits := seams.Visits(dispatchDone)
	hold := ""
	switch {
	case ev.Op != "" && ev.At == hMidTask:
		d.inside, d.hold = make(chan struct{}), make(chan struct{})
	case ev.Op != "" && ev.At == hBeforeTransition:
		hold = diagnostics.CheckpointBeforeTransitionTx
	case ev.Op != "" && ev.At == hBeforeRewind:
		hold = diagnostics.CheckpointBeforeRetryRewind
	}
	if hold != "" {
		seams.Break(hold)
		r.t.Cleanup(func() { seams.Resume(hold) })
	}
	refused, err := r.m.apply(ev)
	if err != nil {
		r.t.Fatal(err)
	}
	a.admit <- d
	switch {
	case d.inside != nil:
		select {
		case <-d.inside:
		case <-time.After(r.timeout()):
			r.t.Fatalf("event %s: the task never ran", ev)
		}
		r.applyOp(ev.Op, refused)
		r.keepIfTerminated(ev.Op)
		close(d.hold)
	case hold != "":
		if !seams.WaitTimeout(r.t.Context(), hold, r.timeout()) {
			r.t.Fatalf("event %s: the engine never reached %s", ev, hold)
		}
		r.applyOp(ev.Op, refused)
		r.keepIfTerminated(ev.Op)
		seams.Resume(hold)
	}
	enginetest.AwaitVisitCount(r.t, seams, visits+1, r.timeout(), dispatchDone)
	r.settle()
}

// restingAs names the state an operation event lands on, for coverage.
func (r *hRunner) restingAs() string {
	if r.m.status == workflow.StatusRunning && r.m.tail().sleeping {
		return "asleep"
	}
	s := r.m.status
	if r.m.watermark >= 0 && !r.m.closed() {
		s += " cancelling"
	}
	return s
}

// settle waits for the flow to stand where the model says once an event's work is done, and checks it there: an
// interrupted flow's prompt, a closed flow's outcome.
func (r *hRunner) settle() {
	r.t.Helper()
	assert := testarossa.For(r.t)
	if r.m.status == workflow.StatusRunning {
		return // the next execution's arrival is the check
	}
	// Await, watching the door: an execution arriving while the model has the flow stopped is the engine running
	// what the model says it would not.
	type awaited struct {
		outcome *workflow.FlowOutcome
		err     error
	}
	ctx, cancel := context.WithTimeout(r.t.Context(), r.timeout()+hSleepFor)
	defer cancel()
	stopped := make(chan awaited, 1)
	go func() {
		outcome, err := r.e.Await(ctx, r.flowKey)
		stopped <- awaited{outcome, err}
	}()
	var outcome *workflow.FlowOutcome
	select {
	case a := <-r.arrivals:
		r.t.Fatalf("an execution of %s arrived, the model has the flow %s", a.task, r.m.status)
	case got := <-stopped:
		if got.err != nil {
			r.t.Fatalf("the flow did not stop as %s: %v", r.m.status, got.err)
		}
		outcome = got.outcome
	}
	if outcome.Status != r.m.status {
		r.t.Fatalf("the flow is %s, the model has it %s", outcome.Status, r.m.status)
	}
	if r.m.status == workflow.StatusInterrupted {
		assert.Equal(nonNil(r.m.tail().merged()), nonNil(outcome.State.GetStrings("trail")), "the interrupted step's state")
		return
	}
	assert.Equal(nonNil(r.m.final), nonNil(outcome.State.GetStrings("trail")), "the final state")
	// Each reason belongs to its own status alone: a Cancel that landed on a flow that then completed or failed
	// leaves no reason on the outcome.
	want := map[string]string{r.m.status: "*"}
	assert.Equal(map[bool]string{true: hTerminateReason}[want[workflow.StatusTerminated] != ""], outcome.TerminateReason, "the terminate reason")
	assert.Equal(map[bool]string{true: hCancelReason}[want[workflow.StatusCancelled] != ""], outcome.CancelReason, "the cancel reason")
	if r.m.status == workflow.StatusFailed {
		assert.Contains(outcome.Error, hFailPrefix)
	} else {
		assert.Equal("", outcome.Error, "the error")
	}
	r.keepIfClosed()
}

// keepIfTerminated records the closed outcome where a Terminate closed the flow under a held execution, before
// the execution goes on: what it writes next must not move the outcome, and a record taken after it would hold
// whatever it wrote. Only a Terminate has closed the flow by then; what a Cancel leads to happens once the
// execution is released.
func (r *hRunner) keepIfTerminated(op hOp) {
	if op == hTerminate {
		r.keepIfClosed()
	}
}

// keepIfClosed records the closed outcome the first time the model has the flow closed.
func (r *hRunner) keepIfClosed() {
	r.t.Helper()
	if r.closed || !r.m.closed() {
		return
	}
	r.closed = true
	r.cov.add("closed %s by %s", r.m.status, r.m.closedBy)
	r.kept = enginetest.CaptureOutcomes(r.t, r.e)
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

// stepRows reads the flow's steps as task:status, in the order they were inserted.
func (r *hRunner) stepRows() []string {
	r.t.Helper()
	sh, err := r.e.db.Shard(r.shard)
	if err != nil {
		r.t.Fatal(err)
	}
	rows, err := sh.DB().QueryContext(r.t.Context(), "SELECT task_name, status FROM dwarf_steps WHERE flow_id=? ORDER BY step_id", r.flowID)
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

// noStray fails the test if an execution arrives within a moment: the model ran everything it predicted.
func (r *hRunner) noStray(when string) {
	r.t.Helper()
	select {
	case a := <-r.arrivals:
		r.t.Errorf("%s: an execution of %s arrived that the model did not predict", when, a.task)
	case <-time.After(200 * time.Millisecond * enginetest.TimeoutScale()):
	}
}

// runHistory drives a fresh engine through h and checks it against the model.
func runHistory(t *testing.T, h hHistory, cov *hCoverage) {
	assert := testarossa.For(t)
	r := &hRunner{t: t, h: h, m: newHModel(h.Shape), cov: cov, arrivals: make(chan *hArrival), done: make(chan struct{})}
	t.Cleanup(func() {
		if t.Failed() {
			b, _ := json.Marshal(h)
			t.Logf("replay with DWARF_HISTORY='%s'", b)
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
		t.Fatalf("the history ends with the flow %s", r.m.status)
	}
	r.noStray("after the history")
	want := []string{}
	for _, s := range r.m.steps {
		want = append(want, s.task+":"+s.status)
	}
	assert.Equal(want, r.stepRows(), "the step rows")
	r.mu.Lock()
	assert.Len(r.problems, 0, "the task handlers saw: %v", r.problems)
	r.mu.Unlock()

	assert.Len(enginetest.AssertOutcomesKept(t, r.e, r.kept), 0, "a closed flow left the live tables")
	sweepEveryDetector(t, r.e)
	r.noStray("after every recovery detector")
	assert.Len(enginetest.AssertOutcomesKept(t, r.e, r.kept), 0, "a closed flow left the live tables")
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

// historySeeds is the seeds a run explores, from the environment.
func historySeeds(t *testing.T) []uint64 {
	if s := os.Getenv("DWARF_HISTORY_SEED"); s != "" {
		seed, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			t.Fatalf("DWARF_HISTORY_SEED: %v", err)
		}
		return []uint64{seed}
	}
	n := 32
	if testing.Short() {
		n = 8
	}
	if s := os.Getenv("DWARF_HISTORY_SEEDS"); s != "" {
		var err error
		if n, err = strconv.Atoi(s); err != nil {
			t.Fatalf("DWARF_HISTORY_SEEDS: %v", err)
		}
	}
	seeds := make([]uint64, n)
	for i := range seeds {
		seeds[i] = uint64(i + 1)
	}
	return seeds
}

// runHistories runs the seeded histories of shape, or the one DWARF_HISTORY holds.
func runHistories(t *testing.T, shape hShape) {
	cov := &hCoverage{counts: map[string]int{}}
	if raw := os.Getenv("DWARF_HISTORY"); raw != "" {
		if path, ok := strings.CutPrefix(raw, "@"); ok {
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			raw = string(b)
		}
		var h hHistory
		if err := json.Unmarshal([]byte(raw), &h); err != nil {
			t.Fatalf("DWARF_HISTORY: %v", err)
		}
		if h.Shape != shape {
			t.Skipf("DWARF_HISTORY holds a %s history", h.Shape)
		}
		runHistory(t, h, cov)
		return
	}
	t.Cleanup(func() { cov.log(t) })
	for _, seed := range historySeeds(t) {
		h := generateHistory(shape, seed)
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
