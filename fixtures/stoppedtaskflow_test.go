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

package fixtures

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// listedTask returns the StoppedTaskName List reports for flowKey.
func listedTask(t *testing.T, eng *engine.Engine, flowKey string) string {
	assert := testarossa.For(t)
	flows, err := eng.List(context.Background(), workflow.Query{Search: flowKey})
	if !assert.NoError(err) {
		return ""
	}
	for _, f := range flows {
		if f.FlowKey == flowKey {
			return f.StoppedTaskName
		}
	}
	assert.True(false, "flow not listed")
	return ""
}

// TestStoppedTask_FanOutInterrupts pins the task a flow records while interrupted across a fan-out: the first
// interrupt is recorded and a later one does not replace it, a Resume that leaves the flow interrupted records
// the remaining interrupted step (the one Snapshot reports), and completion records the step that completed it.
func TestStoppedTask_FanOutInterrupts(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("StoppedFan")
	g.SetURL("Src", "stoppedtask.verify:428/src")
	g.SetURL("A", "stoppedtask.verify:428/a")
	g.SetURL("B", "stoppedtask.verify:428/b")
	g.SetURL("J", "stoppedtask.verify:428/j")
	g.SetFanIn("J")
	g.AddTransition("Src", "A")
	g.AddTransition("Src", "B")
	g.AddTransition("A", "J")
	g.AddTransition("B", "J")
	g.AddTransition("J", workflow.End)
	mux.HandleGraph("stoppedtask.verify:428/fan", g)

	gateB := make(chan struct{})
	interrupt := func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	}
	mux.HandleTask("stoppedtask.verify:428/src", func(ctx context.Context, f *workflow.Flow) error { return nil })
	mux.HandleTask("stoppedtask.verify:428/a", interrupt)
	mux.HandleTask("stoppedtask.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		// B interrupts only after A has, so A's interrupt is unambiguously the first.
		select {
		case <-gateB:
		case <-ctx.Done():
			return ctx.Err()
		}
		return interrupt(ctx, f)
	})
	mux.HandleTask("stoppedtask.verify:428/j", func(ctx context.Context, f *workflow.Flow) error { return nil })

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	assert.NoError(eng.SetWorkers(4))
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	flowKey, err := eng.Create(ctx, "stoppedtask.verify:428/fan", nil, nil)
	assert.NoError(err)

	awaitPropagatedInterrupts(t, eng, flowKey, 1)
	assert.Equal("A", listedTask(t, eng, flowKey))
	close(gateB)
	awaitPropagatedInterrupts(t, eng, flowKey, 2)
	assert.Equal("A", listedTask(t, eng, flowKey), "a later interrupt must not replace the first")

	// Resuming A leaves the flow interrupted at B.
	assert.NoError(eng.Resume(ctx, flowKey, nil))
	assert.Equal("B", listedTask(t, eng, flowKey))

	assert.NoError(eng.Resume(ctx, flowKey, nil))
	final, err := eng.Await(ctx, flowKey)
	assert.NoError(err)
	assert.Equal(workflow.StatusCompleted, final.Status)
	assert.Equal("J", listedTask(t, eng, flowKey))
}

// TestStoppedTask_Linear pins the task recorded on a single path: none while the flow runs, the failing step on
// a failure (and the List filter finds the flow by it), and the interrupted step, which Terminate leaves in place.
func TestStoppedTask_Linear(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	g := workflow.NewGraph("StoppedLinear")
	g.SetURL("First", "stoppedtask.verify:428/first")
	g.SetURL("Second", "stoppedtask.verify:428/second")
	g.AddTransitionChain("First", "Second", workflow.End)
	mux.HandleGraph("stoppedtask.verify:428/linear", g)

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	mux.HandleTask("stoppedtask.verify:428/first", func(ctx context.Context, f *workflow.Flow) error {
		switch f.GetString("mode") {
		case "hold":
			started <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		case "ask":
			yield, err := f.Interrupt(nil, nil)
			if yield || err != nil {
				return err
			}
		}
		return nil
	})
	mux.HandleTask("stoppedtask.verify:428/second", func(ctx context.Context, f *workflow.Flow) error {
		if f.GetString("mode") == "fail" {
			return errors.New("boom")
		}
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) }) // declared after Shutdown, so it unwinds first
	assert.NoError(eng.SetWorkers(4))
	eng.SetHost(mux)
	assert.NoError(eng.Startup(t.Context()))

	t.Run("running_records_none", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, err := eng.Create(ctx, "stoppedtask.verify:428/linear", map[string]any{"mode": "hold"}, nil)
		assert.NoError(err)
		<-started
		assert.Equal("", listedTask(t, eng, flowKey))
		releaseOnce.Do(func() { close(release) })
		final, err := eng.Await(ctx, flowKey)
		assert.NoError(err)
		assert.Equal(workflow.StatusCompleted, final.Status)
		assert.Equal("Second", listedTask(t, eng, flowKey))
	})

	t.Run("failed_records_the_failing_step", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, out, err := eng.Run(ctx, "stoppedtask.verify:428/linear", map[string]any{"mode": "fail"}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusFailed, out.Status)
		assert.Equal("Second", listedTask(t, eng, flowKey))

		flows, err := eng.List(ctx, workflow.Query{WorkflowURL: "stoppedtask.verify:428/linear", Status: workflow.StatusFailed, StoppedTaskName: "Second"})
		assert.NoError(err)
		if assert.Len(flows, 1) {
			assert.Equal(flowKey, flows[0].FlowKey)
		}
	})

	t.Run("terminate_keeps_the_interrupted_step", func(t *testing.T) {
		assert := testarossa.For(t)
		flowKey, out, err := eng.Run(ctx, "stoppedtask.verify:428/linear", map[string]any{"mode": "ask"}, nil)
		assert.NoError(err)
		assert.Equal(workflow.StatusInterrupted, out.Status)
		assert.Equal("First", listedTask(t, eng, flowKey))
		assert.NoError(eng.Terminate(ctx, flowKey, "test"))
		assert.Equal("First", listedTask(t, eng, flowKey))
	})
}
