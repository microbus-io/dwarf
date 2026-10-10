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
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// TestFanIn_ReducerErrorFailsTheFlow pins that a value a reducer cannot fold - a string under `add` - fails the
// flow naming the field, rather than reaching the fan-in with the field's accumulated contributions dropped. It is
// an authoring fault, not a storage one: the merge is not retried, and neither write metric counts it.
func TestFanIn_ReducerErrorFailsTheFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := NewHostMux()
	g := workflow.NewGraph("ReducerError")
	g.SetURL("Split", "reducererror.verify:429/split")
	g.SetURL("Work", "reducererror.verify:429/work")
	g.SetURL("Join", "reducererror.verify:429/join")
	g.SetFanIn("Join")
	g.SetReducer("total", workflow.ReducerAdd)
	g.AddTransitionForEach("Split", "Work", "items", "item")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("reducererror.verify:429/g", g)
	mux.HandleTask("reducererror.verify:429/split", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("reducererror.verify:429/work", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("item") == "bad" {
			f.SetString("total", "not a number")
			return nil
		}
		f.SetInt("total", 1)
		return nil
	})
	var joined atomic.Bool
	mux.HandleTask("reducererror.verify:429/join", func(ctx context.Context, f *workflow.Flow) error {
		joined.Store(true)
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	reader := withManualReader(eng)
	assert.NoError(eng.Startup(t.Context()))

	_, outcome, err := eng.Run(ctx, "reducererror.verify:429/g", map[string]any{"items": []string{"a", "bad", "b"}}, nil)
	if !assert.NoError(err) || !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusFailed, outcome.Status)
	assert.True(strings.Contains(outcome.Error, "total"), outcome.Error)
	assert.False(joined.Load(), "the fan-in ran on a state that lost the field's contributions")

	var rm metricdata.ResourceMetrics
	if assert.NoError(reader.Collect(ctx, &rm)) {
		for _, name := range []string{"dwarf_steps_write_retried", "dwarf_steps_write_failed"} {
			n, _ := sumCounter(rm, name, "", "")
			assert.Equal(int64(0), n, name)
		}
	}
}

// TestTerminate_ReducerErrorDoesNotBlockTerminate pins that a flow whose state a reducer cannot fold can still be
// stopped. A completed branch holds a string under `add` while a sibling waits on an interrupt, so computing the
// final state fails identically on every attempt; Terminate terminalizes the tree with an empty final state rather
// than returning that error forever.
func TestTerminate_ReducerErrorDoesNotBlockTerminate(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := NewHostMux()
	g := workflow.NewGraph("TerminateReducerError")
	g.SetURL("Split", "reducererror.verify:429/split")
	g.SetURL("Work", "reducererror.verify:429/work-or-wait")
	g.SetURL("Join", "reducererror.verify:429/join")
	g.SetFanIn("Join")
	g.SetReducer("total", workflow.ReducerAdd)
	g.AddTransitionForEach("Split", "Work", "items", "item")
	g.AddTransitionChain("Work", "Join", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("reducererror.verify:429/terminate", g)
	mux.HandleTask("reducererror.verify:429/split", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	mux.HandleTask("reducererror.verify:429/work-or-wait", func(ctx context.Context, f *workflow.Flow) error {
		switch f.GetString("item") {
		case "bad":
			f.SetString("total", "not a number")
		case "wait":
			_, err := f.Interrupt(nil, nil)
			return err
		}
		return nil
	})
	mux.HandleTask("reducererror.verify:429/join", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	flowKey, outcome, err := eng.Run(ctx, "reducererror.verify:429/terminate", map[string]any{"items": []string{"bad", "wait"}, "total": 0}, nil)
	if !assert.NoError(err) || !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusInterrupted, outcome.Status)

	assert.NoError(eng.Terminate(ctx, flowKey, "stop"))
	outcome, err = eng.Snapshot(ctx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusTerminated, outcome.Status)
	}
}

// TestContinue_ReducerErrorIs400 pins that a Continue whose additionalState a reducer cannot fold onto the thread's
// state is rejected as the caller's mistake - a 400 naming the field - and starts no turn.
func TestContinue_ReducerErrorIs400(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := NewHostMux()
	g := workflow.NewGraph("ContinueReducerError")
	g.SetURL("Turn", "reducererror.verify:429/turn")
	g.SetReducer("total", workflow.ReducerAdd)
	g.AddTransition("Turn", workflow.End)
	assert.NoError(g.Validate())
	mux.HandleGraph("reducererror.verify:429/continue", g)
	mux.HandleTask("reducererror.verify:429/turn", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	flowKey, outcome, err := eng.Run(ctx, "reducererror.verify:429/continue", map[string]any{"total": 10}, nil)
	if !assert.NoError(err) || !assert.NotNil(outcome) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	nextKey, err := eng.Continue(ctx, flowKey, map[string]any{"total": "not a number"})
	assert.Error(err)
	assert.Equal("", nextKey)
	assert.Equal(http.StatusBadRequest, errors.StatusCode(err))
	assert.True(strings.Contains(err.Error(), "total"), err.Error())
}
