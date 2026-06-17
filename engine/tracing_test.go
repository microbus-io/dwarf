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
	"testing"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestTracing_SpansEmittedOnRun pins the two span sites and their nesting:
//   - the root "workflow" span minted (detached) at Create,
//   - the per-step span (named by task) created in processStep, parented to the reconstructed root,
//   - a subgraph's OWN "workflow" span parented to the caller step's span, so the subgraph subtree nests
//     under the launching task: workflow → runInner → workflow(subgraph) → taskX.
//
// It also pins reentrancy: a step that yields and re-dispatches gets one span per execution attempt, so
// the subgraph caller (runInner) has two step spans.
//
// Graph: taskA → runInner → done → END, with runInner launching the inner subgraph (taskX → END). The
// trailing `done` task matters: the very last step of the root flow is the one whose completion wakes
// Await, and its span ends in a defer that fires just after - so its span may not be flushed when Run
// returns. Keeping `done` last (and not asserting on it) makes every span we DO assert on deterministic.
func TestTracing_SpansEmittedOnRun(t *testing.T) {
	assert := testarossa.For(t)
	ctx := context.Background()

	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))

	proxy := NewTestProxy()

	parent := workflow.NewGraph("Parent")
	parent.SetEndpoint("taskA", "tracingflow.verify:428/task-a")
	parent.SetEndpoint("runInner", "tracingflow.verify:428/run-inner")
	parent.SetEndpoint("done", "tracingflow.verify:428/done")
	parent.AddTransition("taskA", "runInner")
	parent.AddTransition("runInner", "done")
	parent.AddTransition("done", workflow.END)
	proxy.HandleGraph("tracingflow.verify:428/parent", parent)

	inner := workflow.NewGraph("Inner")
	inner.SetEndpoint("taskX", "tracingflow.verify:428/task-x")
	inner.AddTransition("taskX", workflow.END)
	proxy.HandleGraph("tracingflow.verify:428/inner", inner)

	proxy.HandleTask("tracingflow.verify:428/task-a", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("tracingflow.verify:428/task-x", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("tracingflow.verify:428/done", func(ctx context.Context, f *workflow.Flow) error { return nil })
	proxy.HandleTask("tracingflow.verify:428/run-inner", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("tracingflow.verify:428/inner", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})

	eng := NewEngine()
	eng.SetHost(proxy)
	eng.SetTracerProvider(tp)
	eng.RunInTest(t)

	_, outcome, err := eng.Run(ctx, "tracingflow.verify:428/parent", nil, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)

	// Index the ended spans. Two "workflow" spans are minted (parent flow root + subgraph); runInner
	// appears once per dispatch; the rest are unique per task.
	var workflowSpans, runInnerSpans []sdktrace.ReadOnlySpan
	byName := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range sr.Ended() {
		switch s.Name() {
		case "workflow":
			workflowSpans = append(workflowSpans, s)
		case "runInner":
			runInnerSpans = append(runInnerSpans, s)
		default:
			byName[s.Name()] = s
		}
	}

	// Reentrancy: runInner runs twice - once to arm the subgraph (yield) and once after it resolves - so
	// it has two step spans.
	if !assert.Equal(2, len(runInnerSpans), "the subgraph caller step should have one span per dispatch") {
		return
	}

	taskA, okA := byName["taskA"]
	taskX, okX := byName["taskX"]
	if !assert.True(okA && okX, "expected per-step spans taskA, taskX") {
		return
	}

	// Two workflow spans: the detached root (parent flow) and the subgraph's (parented to runInner).
	if !assert.Equal(2, len(workflowSpans), "expected a workflow span for the parent flow and the subgraph") {
		return
	}
	var root, subRoot sdktrace.ReadOnlySpan
	for _, s := range workflowSpans {
		if s.Parent().IsValid() {
			subRoot = s
		} else {
			root = s
		}
	}
	if !assert.True(root != nil && subRoot != nil, "expected one detached root and one parented subgraph workflow span") {
		return
	}

	// Everything shares one trace.
	for name, s := range map[string]sdktrace.ReadOnlySpan{"taskA": taskA, "taskX": taskX, "subRoot": subRoot} {
		assert.Equal(root.SpanContext().TraceID(), s.SpanContext().TraceID(), "%s shares the root trace", name)
	}

	// Parent flow's step spans nest directly under the root workflow span - taskA and both runInner spans.
	assert.Equal(root.SpanContext().SpanID(), taskA.Parent().SpanID(), "taskA parented to root")
	for _, ri := range runInnerSpans {
		assert.Equal(root.SpanContext().TraceID(), ri.SpanContext().TraceID(), "runInner shares the root trace")
		assert.Equal(root.SpanContext().SpanID(), ri.Parent().SpanID(), "runInner parented to root")
	}

	// The subgraph's workflow span nests under the runInner dispatch that armed it, and the subgraph's
	// step nests under that subgraph workflow span: workflow → runInner → workflow → taskX.
	armerFound := false
	for _, ri := range runInnerSpans {
		if ri.SpanContext().SpanID() == subRoot.Parent().SpanID() {
			armerFound = true
		}
	}
	assert.True(armerFound, "subgraph workflow span should be parented to the runInner dispatch that armed it")
	assert.Equal(subRoot.SpanContext().SpanID(), taskX.Parent().SpanID(), "subgraph step parented to the subgraph workflow span")
}
