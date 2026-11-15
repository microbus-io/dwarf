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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestPayloadSize_OversizedChangesFailTheStepOnce pins the check on a task's changes: the step fails at once,
// naming the field, and the task is not run again - neither by the write retries nor by lease recovery.
func TestPayloadSize_OversizedChangesFailTheStepOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var runs atomic.Int32
	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadSize")
	g.SetURL("A", "ps/changes/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ps/changes/wf", g)
	mux.HandleTask("ps/changes/a", func(ctx context.Context, f *workflow.Flow) error {
		runs.Add(1)
		f.SetString("small", "x")
		f.SetString("big", strings.Repeat("b", 2000))
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	e.maxPayloadBytes = 1000
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, outcome, err := e.Run(ctx, "ps/changes/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)
	// The changes check names its payload; the final-state check would catch the same field otherwise.
	assert.Contains(outcome.Error, "changes of task 'A'")
	assert.Contains(outcome.Error, "over the 1000-byte limit")
	assert.Contains(outcome.Error, `"big"`)
	assert.Equal(int32(1), runs.Load(), "the task must run exactly once")
}

// TestPayloadSize_CreateRejectsWithoutInserting pins the check at the door: an oversized initial state or
// baggage is a 413 and no flow is created.
func TestPayloadSize_CreateRejectsWithoutInserting(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadSize")
	g.SetURL("A", "ps/create/a")
	g.AddTransition("A", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ps/create/wf", g)
	mux.HandleTask("ps/create/a", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.maxPayloadBytes = 1000
	assert.Must(assert.NoError(e.Startup(t.Context())))

	big := map[string]any{"doc": strings.Repeat("d", 2000)}
	_, err := e.Create(ctx, "ps/create/wf", big, nil)
	assert.Error(err)
	assert.Equal(http.StatusRequestEntityTooLarge, errors.StatusCode(err))
	_, err = e.Create(ctx, "ps/create/wf", nil, &workflow.FlowOptions{Baggage: big})
	assert.Error(err)
	assert.Equal(http.StatusRequestEntityTooLarge, errors.StatusCode(err))

	flows, err := enginetest.ListAll(ctx, e, workflow.Query{})
	assert.NoError(err)
	assert.Len(flows, 0, "a rejected Create must insert nothing")
}

// TestPayloadSize_RefsCarryStateTheFinalStateCannotHold pins what the limit means for a flow. Mid-flow, each
// large field is stored once and carried by a ref, so steps whose combined state is over the limit run fine.
// The final state is flattened, so it holds every field at once: the flow then fails with an empty final state.
func TestPayloadSize_RefsCarryStateTheFinalStateCannotHold(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var lastRan atomic.Bool
	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadSize")
	g.SetURL("A", "ps/final/a")
	g.SetURL("B", "ps/final/b")
	g.SetURL("C", "ps/final/c")
	g.AddTransition("A", "B")
	g.AddTransition("B", "C")
	g.AddTransition("C", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ps/final/wf", g)
	mux.HandleTask("ps/final/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("x", strings.Repeat("x", 2000))
		return nil
	})
	mux.HandleTask("ps/final/b", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("y", strings.Repeat("y", 2000))
		return nil
	})
	mux.HandleTask("ps/final/c", func(ctx context.Context, f *workflow.Flow) error {
		// Both fields reach the last step whole, though together they are over the limit.
		if len(f.GetString("x")) == 2000 && len(f.GetString("y")) == 2000 {
			lastRan.Store(true)
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	e.maxPayloadBytes = 3000
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, outcome, err := e.Run(ctx, "ps/final/wf", nil, nil)
	assert.NoError(err)
	assert.True(lastRan.Load(), "every step must run with the whole state")
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.Contains(outcome.Error, "final state")
	assert.Equal(0, outcome.State.Len(), "a final state too large to store is stored empty")
}

// TestPayloadSize_CancelRerouteIsChecked pins the check on the changes a Cancel-covered step is rerouted with.
// The task's own changes fit, but the onErr carrying the cancel reason takes them over the limit: the step fails
// on the size, once, and the handler never runs.
func TestPayloadSize_CancelRerouteIsChecked(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var workRuns, handlerRuns atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadSize")
	g.SetURL("Work", "ps/reroute/work")
	g.SetURL("Handler", "ps/reroute/handler")
	g.AddTransition("Work", workflow.End)
	g.AddTransitionOnError("Work", "Handler")
	g.AddTransition("Handler", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ps/reroute/wf", g)
	mux.HandleTask("ps/reroute/work", func(ctx context.Context, f *workflow.Flow) error {
		if workRuns.Add(1) == 1 {
			close(started)
		}
		<-release
		f.SetString("big", strings.Repeat("b", 800))
		return nil
	})
	mux.HandleTask("ps/reroute/handler", func(ctx context.Context, f *workflow.Flow) error {
		handlerRuns.Add(1)
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	e.maxPayloadBytes = 1000
	assert.Must(assert.NoError(e.Startup(t.Context())))

	fk, err := e.Create(ctx, "ps/reroute/wf", nil, nil)
	if !assert.NoError(err) {
		return
	}
	<-started
	assert.NoError(e.Cancel(ctx, fk, strings.Repeat("r", 400)))
	close(release)
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	out, err := e.Await(awaitCtx, fk)
	if !assert.NoError(err) || !assert.NotNil(out) {
		return
	}
	assert.Equal(workflow.StatusFailed, out.Status, "a real error wins over the cancellation")
	assert.Contains(out.Error, "changes of task 'Work'")
	assert.Contains(out.Error, "over the 1000-byte limit")
	assert.Equal(int32(1), workRuns.Load(), "the task must run exactly once")
	assert.Equal(int32(0), handlerRuns.Load())
}

// TestPayloadSize_PercentInTaskName pins that a task name is an argument of the error, not part of its pattern:
// a '%' in it must neither be read as a verb nor shift the sizes that follow.
func TestPayloadSize_PercentInTaskName(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadSize")
	g.SetURL("Pct%dTask", "ps/percent/a")
	g.AddTransition("Pct%dTask", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("ps/percent/wf", g)
	mux.HandleTask("ps/percent/a", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("big", strings.Repeat("b", 2000))
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	e.maxPayloadBytes = 1000
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, outcome, err := e.Run(ctx, "ps/percent/wf", nil, nil)
	assert.NoError(err)
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.Contains(outcome.Error, "changes of task 'Pct%dTask'")
	assert.Contains(outcome.Error, "over the 1000-byte limit")
	assert.Contains(outcome.Error, `"big"`)
	assert.NotContains(outcome.Error, "%!")
}

// TestPayloadText_NamesWhatPostgresRefuses pins unstorableText's verdicts: invalid UTF-8, a NUL and an unpaired
// surrogate escape are named; an escaped backslash before u0000 is text, and a surrogate pair is a character.
func TestPayloadText_NamesWhatPostgresRefuses(t *testing.T) {
	assert := testarossa.For(t)

	for payload, want := range map[string]string{
		`{"a":"x"}`:                      "",
		`{"a":"\\u0000"}`:                "",
		`{"a":"\\\\u0000"}`:              "",
		`{"a":"\ud83d\ude00"}`:           "",
		`{"a":"\uD83D\uDE00"}`:           "",
		`{"a":"\u00e9\n\"\\"}`:           "",
		`{"a":"caf` + "\xc3\xa9" + `"}`:  "",
		`{"a":"\u0000"}`:                 "a NUL character",
		`{"a":"\\\u0000"}`:               "a NUL character",
		`{"a":"x` + "\x00" + `"}`:        "a NUL character",
		`{"a\u0000":1}`:                  "a NUL character",
		`{"a":"x` + "\xff" + `"}`:        "invalid UTF-8",
		`{"a":"` + "\xed\xa0\x80" + `"}`: "invalid UTF-8",
		`{"a":"\ud800"}`:                 "an unpaired surrogate escape",
		`{"a":"\ud800x"}`:                "an unpaired surrogate escape",
		`{"a":"\ud800\n"}`:               "an unpaired surrogate escape",
		`{"a":"\ud800\ud800"}`:           "an unpaired surrogate escape",
		`{"a":"\ude00"}`:                 "an unpaired surrogate escape",
		`{"a":"\ud83d\ude00\ude00"}`:     "an unpaired surrogate escape",
		`{"a":"x\`:                       "",
	} {
		assert.Equal(want, unstorableText([]byte(payload)), "%s", payload)
	}
}

// TestPayloadText_CreateAndResumeRefuseUnstorableText pins the check at the door on every dialect: initial state,
// baggage or resume data holding a NUL, invalid UTF-8 or an unpaired surrogate escape is a 400 naming the field, and
// a refused Create inserts nothing. PostgreSQL refuses these values and the other dialects store them.
func TestPayloadText_CreateAndResumeRefuseUnstorableText(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadText")
	g.SetURL("Ask", "pt/door/ask")
	g.AddTransition("Ask", workflow.End)
	mux.HandleGraph("pt/door/wf", g)
	mux.HandleTask("pt/door/ask", func(ctx context.Context, f *workflow.Flow) error {
		var answer map[string]any
		yield, err := f.Interrupt(nil, &answer)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.Must(assert.NoError(e.Startup(t.Context())))

	unstorable := []any{
		"a\x00b",
		json.RawMessage("\"a\xffb\""),
		json.RawMessage(`"\ud800"`),
	}
	for _, v := range unstorable {
		_, err := e.Create(ctx, "pt/door/wf", map[string]any{"ok": 1, "doc": v}, nil)
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "%q: %v", v, err)
		assert.Contains(errors.Convert(err).Error(), `initial state holds`, "%q", v)
		assert.Contains(errors.Convert(err).Error(), `field "doc"`, "%q", v)
		_, err = e.Create(ctx, "pt/door/wf", nil, &workflow.FlowOptions{Baggage: map[string]any{"doc": v}})
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "baggage %q: %v", v, err)
	}
	flows, err := enginetest.ListAll(ctx, e, workflow.Query{})
	assert.NoError(err)
	assert.Len(flows, 0, "a refused Create must insert nothing")

	flowKey, err := e.Create(ctx, "pt/door/wf", nil, nil)
	assert.Must(assert.NoError(err))
	outcome, err := e.Await(ctx, flowKey)
	assert.Must(assert.NoError(err))
	assert.Must(assert.Equal(workflow.StatusInterrupted, outcome.Status, outcome.Error))
	for _, v := range unstorable {
		err := e.Resume(ctx, flowKey, map[string]any{"doc": v})
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "resume %q: %v", v, err)
	}
	assert.NoError(e.Resume(ctx, flowKey, map[string]any{"doc": "fine"}))
	outcome, err = e.Await(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
}

// TestPayloadText_UnstorableChangesFailTheStepOnce pins the check on a task's changes: a value PostgreSQL refuses
// fails the step at once on every dialect, naming the field, and the task is not run again.
func TestPayloadText_UnstorableChangesFailTheStepOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	cases := []struct {
		name    string
		value   any
		problem string
	}{
		{"nul", "a\x00b", "a NUL character"},
		{"utf8", json.RawMessage("\"a\xffb\""), "invalid UTF-8"},
		{"surrogate", json.RawMessage(`"\ud800"`), "an unpaired surrogate escape"},
	}
	runs := make([]atomic.Int32, len(cases))
	mux := hosts.NewMux()
	for i, c := range cases {
		g := workflow.NewGraph("PayloadText")
		g.SetURL("A", "pt/changes/"+c.name+"/a")
		g.AddTransition("A", workflow.End)
		mux.HandleGraph("pt/changes/"+c.name+"/wf", g)
		mux.HandleTask("pt/changes/"+c.name+"/a", func(ctx context.Context, f *workflow.Flow) error {
			runs[i].Add(1)
			f.SetString("small", "x")
			return f.Set("raw", c.value)
		})
	}

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	assert.Must(assert.NoError(e.Startup(t.Context())))

	for i, c := range cases {
		_, outcome, err := e.Run(ctx, "pt/changes/"+c.name+"/wf", nil, nil)
		if !assert.NoError(err, c.name) {
			continue
		}
		assert.Equal(workflow.StatusFailed, outcome.Status, c.name)
		assert.Contains(outcome.Error, "changes of task 'A' holds "+c.problem, c.name)
		assert.Contains(outcome.Error, `field "raw"`, c.name)
		assert.Equal(int32(1), runs[i].Load(), "%s: the task must run exactly once", c.name)
	}
}

// TestPayloadText_ContinueForkAndInterruptRefuseUnstorableText pins the check on the other ways a value enters a
// flow: Continue's additional state and Fork's overrides are a 400, and an interrupt payload fails the step.
func TestPayloadText_ContinueForkAndInterruptRefuseUnstorableText(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadText")
	g.SetURL("Work", "pt/later/work")
	g.AddTransition("Work", workflow.End)
	mux.HandleGraph("pt/later/wf", g)
	mux.HandleTask("pt/later/work", func(ctx context.Context, f *workflow.Flow) error { return nil })
	park := workflow.NewGraph("PayloadText")
	park.SetURL("Ask", "pt/later/ask")
	park.AddTransition("Ask", workflow.End)
	mux.HandleGraph("pt/later/park", park)
	mux.HandleTask("pt/later/ask", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"doc": "a\x00b"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	assert.Must(assert.NoError(e.Startup(t.Context())))

	flowKey, outcome, err := e.Run(ctx, "pt/later/wf", nil, nil)
	assert.Must(assert.NoError(err))
	assert.Must(assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error))
	_, err = e.Continue(ctx, flowKey, map[string]any{"doc": "a\x00b"})
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "Continue: %v", err)
	history, err := e.History(ctx, flowKey)
	assert.Must(assert.NoError(err))
	assert.Must(assert.True(len(history) > 0))
	_, err = e.Fork(ctx, history[0].StepKey, map[string]any{"doc": "a\x00b"})
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err), "Fork: %v", err)

	_, outcome, err = e.Run(ctx, "pt/later/park", nil, nil)
	assert.Must(assert.NoError(err))
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.Contains(outcome.Error, "interrupt payload holds a NUL character")
}

// TestPayloadText_AnUnstorableErrorStillReachesItsHandler pins that an error holding a NUL, in its message and in a
// property, is made storable for its onError handler rather than refused: the handler runs and reads the message
// and the status code, and the property is dropped.
func TestPayloadText_AnUnstorableErrorStillReachesItsHandler(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	var routed atomic.Pointer[map[string]any]
	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadText")
	g.SetURL("A", "pt/onerr/a")
	g.SetURL("H", "pt/onerr/h")
	g.AddTransition("A", workflow.End)
	g.AddTransitionOnError("A", "H")
	g.AddTransition("H", workflow.End)
	mux.HandleGraph("pt/onerr/wf", g)
	mux.HandleTask("pt/onerr/a", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("downstream said: bad\x00byte", http.StatusTeapot, "detail", "x\x00y")
	})
	mux.HandleTask("pt/onerr/h", func(ctx context.Context, f *workflow.Flow) error {
		var onErr map[string]any
		if ok, err := f.Get("onErr", &onErr); !ok || err != nil {
			return errors.New("no onErr")
		}
		routed.Store(&onErr)
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, outcome, err := e.Run(ctx, "pt/onerr/wf", nil, nil)
	assert.Must(assert.NoError(err))
	assert.Equal(workflow.StatusCompleted, outcome.Status, outcome.Error)
	if onErr := routed.Load(); assert.NotNil(onErr, "the handler did not run") {
		assert.Equal("downstream said: bad\uFFFDbyte", (*onErr)["error"])
		assert.Equal(float64(http.StatusTeapot), (*onErr)["statusCode"])
		assert.NotContains(*onErr, "detail")
	}
}

// TestPayloadText_AGotoTargetHoldingANULNamesNoTask pins that a Goto target Graph.Validate would refuse as a task name
// fails the step as a target with no goto transition, on every dialect, rather than as a completion write PostgreSQL
// refuses.
func TestPayloadText_AGotoTargetHoldingANULNamesNoTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	g := workflow.NewGraph("PayloadText")
	g.SetURL("A", "pt/goto/a")
	g.SetURL("B", "pt/goto/b")
	g.AddTransition("A", workflow.End)
	g.AddTransitionGoto("A", "B")
	g.AddTransition("B", workflow.End)
	mux.HandleGraph("pt/goto/wf", g)
	mux.HandleTask("pt/goto/a", func(ctx context.Context, f *workflow.Flow) error {
		f.Goto("B\x00")
		return nil
	})
	mux.HandleTask("pt/goto/b", func(ctx context.Context, f *workflow.Flow) error { return nil })

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	e.persistBackoff = shortPersistBackoff
	assert.Must(assert.NoError(e.Startup(t.Context())))

	_, outcome, err := e.Run(ctx, "pt/goto/wf", nil, nil)
	assert.Must(assert.NoError(err))
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.Contains(outcome.Error, "no goto transition exists")
}
