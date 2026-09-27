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
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// stepByName returns the payload-bearing record of the first step of the given graph node in a flow.
// FlowStep.TaskName is the node name, and History omits payloads, so the key is looked up there and the
// step read with Step.
func stepByName(ctx context.Context, e *engine.Engine, flowKey, nodeName string) (*workflow.FlowStep, error) {
	steps, err := e.History(ctx, flowKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, s := range steps {
		if s.TaskName == nodeName {
			return e.Step(ctx, s.StepKey)
		}
	}
	return nil, errors.New("step not found: %s", nodeName)
}

// awaitSignal waits for a task to report it holds the worker. The bound is a "did it hang" ceiling only.
func awaitSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		t.Fatalf("%s never happened", what)
	}
}

// recordCancellation is the onError handler every covered node below routes to. It records whether the error
// it received is a cancellation. The covered task's own changes need no echo: they are in the handler's input
// state, which is what the flow's final state is built from, so a test reads them off the outcome.
func recordCancellation(ctx context.Context, f *workflow.Flow) error {
	var te errors.TracedError
	_ = f.Get("onErr", &te)
	f.SetBool("recovered", workflow.IsCancelled(&te))
	return nil
}

// TestGracefulCancelFlow pins graceful cancellation end to end. A step Cancel reaches before it starts is
// preempted without running; a step already running finishes, and what it asked for next is not honored.
// Either way the cancellation arrives on the node's onError transition, or, with none, fails the flow. A
// subgraph caller runs once more when its child returns, so it sees the child's result before its own
// cancellation is applied; an interrupted step resumed after Cancel is preempted, so its resume data is not
// acted on. A flow whose every unrecovered loss was a cancellation resolves `cancelled`; one real error mixed
// in resolves it `failed`. A child's cancellation reaches its caller as an error IsCancelled recognizes.
func TestGracefulCancelFlow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// holder is a one-node flow whose task holds the engine's only worker until released, so a second flow's
	// entry step is guaranteed to still be pending - not yet claimed - when Cancel lands on it.
	proxy.HandleGraph("gracefulcancelflow.verify:428/holder", func() *workflow.Graph {
		g := workflow.NewGraph("Holder")
		g.SetEndpoint("Hold", "gracefulcancelflow.verify:428/hold")
		g.AddTransition("Hold", workflow.END)
		return g
	}())
	type holder struct {
		running chan struct{}
		release chan struct{}
		once    sync.Once
	}
	holders := sync.Map{} // test name -> *holder
	proxy.HandleTask("gracefulcancelflow.verify:428/hold", func(ctx context.Context, f *workflow.Flow) error {
		h, _ := holders.Load(f.GetString("holder"))
		close(h.(*holder).running)
		<-h.(*holder).release
		return nil
	})
	holdWorker := func(t *testing.T, eng *engine.Engine) func() {
		h := &holder{running: make(chan struct{}), release: make(chan struct{})}
		holders.Store(t.Name(), h)
		_, err := eng.Create(ctx, "gracefulcancelflow.verify:428/holder", map[string]any{"holder": t.Name()}, nil)
		if err != nil {
			t.Fatal(err)
		}
		awaitSignal(t, h.running, "the holder taking the worker")
		return func() { h.once.Do(func() { close(h.release) }) }
	}

	// preempted: B is pending when Cancel lands, so it never runs. With no onError the step settles
	// cancelled and the flow fails, carrying the reason.
	t.Run("preempted_no_onerror", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("PreemptNoOnError")
		graph.SetEndpoint("B", "gracefulcancelflow.verify:428/pno-b")
		graph.AddTransition("B", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/preempt-no-onerror", graph)
		var bRan atomic.Bool
		proxy.HandleTask("gracefulcancelflow.verify:428/pno-b", func(ctx context.Context, f *workflow.Flow) error {
			bRan.Store(true)
			return nil
		})

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		eng.SetHost(proxy)
		eng.SetWorkers(1)
		assert.NoError(eng.Startup(t.Context()))
		release := holdWorker(t, eng)
		defer release()

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/preempt-no-onerror", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		release()

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal("test reason", outcome.CancelReason)
		assert.False(bRan.Load(), "a preempted task must never run")
		if step, err := stepByName(ctx, eng, flowKey, "B"); assert.NoError(err) {
			assert.Equal(workflow.StatusCancelled, step.Status)
		}
	})

	// preempted, with onError: the handler receives a cancellation and recovers the flow.
	t.Run("preempted_with_onerror", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("PreemptWithOnError")
		graph.SetEndpoint("B", "gracefulcancelflow.verify:428/pw-b")
		graph.SetEndpoint("Handler", "gracefulcancelflow.verify:428/pw-handler")
		graph.AddTransition("B", workflow.END)
		graph.AddTransitionOnError("B", "Handler")
		graph.AddTransition("Handler", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/preempt-with-onerror", graph)
		var bRan atomic.Bool
		proxy.HandleTask("gracefulcancelflow.verify:428/pw-b", func(ctx context.Context, f *workflow.Flow) error {
			bRan.Store(true)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/pw-handler", recordCancellation)

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		eng.SetHost(proxy)
		eng.SetWorkers(1)
		assert.NoError(eng.Startup(t.Context()))
		release := holdWorker(t, eng)
		defer release()

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/preempt-with-onerror", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		release()

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.False(bRan.Load(), "a preempted task must never run")
		assert.True(outcome.State.GetBool("recovered"))
	})

	// ran, covered: C is running when Cancel lands and returns nil. Its changes stand (it ran), its transition
	// to D is not honored, and with no onError it settles cancelled and fails the flow.
	t.Run("ran_covered_no_onerror", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("RanCoveredNoOnError")
		graph.SetEndpoint("C", "gracefulcancelflow.verify:428/rcn-c")
		graph.SetEndpoint("D", "gracefulcancelflow.verify:428/rcn-d")
		graph.AddTransitionChain("C", "D", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/ran-covered-no-onerror", graph)
		running := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		var dRan atomic.Bool
		proxy.HandleTask("gracefulcancelflow.verify:428/rcn-c", func(ctx context.Context, f *workflow.Flow) error {
			close(running)
			<-release
			f.SetBool("cWorked", true)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/rcn-d", func(ctx context.Context, f *workflow.Flow) error {
			dRan.Store(true)
			return nil
		})

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		defer releaseOnce.Do(func() { close(release) })
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/ran-covered-no-onerror", nil, nil)
		if !assert.NoError(err) {
			return
		}
		awaitSignal(t, running, "C starting")
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		releaseOnce.Do(func() { close(release) })

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal("test reason", outcome.CancelReason)
		assert.False(dRan.Load(), "C's transition must not be honored")
		if step, err := stepByName(ctx, eng, flowKey, "C"); assert.NoError(err) {
			assert.Equal(workflow.StatusCancelled, step.Status)
			assert.True(step.Changes.GetBool("cWorked"), "a task that ran keeps its changes")
		}
	})

	// ran, covered, with onError: the handler receives the cancellation together with C's real changes, and
	// C's persisted changes carry onErr exactly as an error-routed step's do.
	t.Run("ran_covered_with_onerror", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("RanCoveredWithOnError")
		graph.SetEndpoint("C", "gracefulcancelflow.verify:428/rcw-c")
		graph.SetEndpoint("D", "gracefulcancelflow.verify:428/rcw-d")
		graph.SetEndpoint("Handler", "gracefulcancelflow.verify:428/rcw-handler")
		graph.SetEndpoint("Tail", "gracefulcancelflow.verify:428/rcw-tail")
		graph.AddTransitionChain("C", "D", workflow.END)
		graph.AddTransitionOnError("C", "Handler")
		graph.AddTransitionChain("Handler", "Tail", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/ran-covered-with-onerror", graph)
		running := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		var dRan atomic.Bool
		proxy.HandleTask("gracefulcancelflow.verify:428/rcw-c", func(ctx context.Context, f *workflow.Flow) error {
			close(running)
			<-release
			f.SetBool("cWorked", true)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/rcw-d", func(ctx context.Context, f *workflow.Flow) error {
			dRan.Store(true)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/rcw-handler", recordCancellation)
		// The handler's successor started after the Cancel, so it must not inherit the cancellation: a flow
		// that caught it carries on.
		var tailRan atomic.Bool
		proxy.HandleTask("gracefulcancelflow.verify:428/rcw-tail", func(ctx context.Context, f *workflow.Flow) error {
			tailRan.Store(true)
			return nil
		})

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		defer releaseOnce.Do(func() { close(release) })
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/ran-covered-with-onerror", nil, nil)
		if !assert.NoError(err) {
			return
		}
		awaitSignal(t, running, "C starting")
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		releaseOnce.Do(func() { close(release) })

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.False(dRan.Load(), "C's transition must be redirected to its handler")
		assert.True(outcome.State.GetBool("recovered"))
		assert.True(outcome.State.GetBool("cWorked"), "the handler's input must carry C's changes")
		assert.True(tailRan.Load(), "a flow that caught the cancellation carries on past its handler")
		if step, err := stepByName(ctx, eng, flowKey, "C"); assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, step.Status)
			assert.True(step.Changes.GetBool("cWorked"))
			assert.True(step.Changes.Has("onErr"), "the persisted changes carry onErr, as an error-routed step's do")
		}
	})

	// retry: G is running when Cancel lands and arms a retry. The retry is scheduled as usual, and the
	// rewound step - still marked - is preempted at its next claim, so the task does not run again.
	t.Run("retry_then_preempted", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("RetryThenPreempted")
		graph.SetEndpoint("G", "gracefulcancelflow.verify:428/rtp-g")
		graph.SetEndpoint("Handler", "gracefulcancelflow.verify:428/rtp-handler")
		graph.AddTransition("G", workflow.END)
		graph.AddTransitionOnError("G", "Handler")
		graph.AddTransition("Handler", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/retry-then-preempted", graph)
		running := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		var attempts atomic.Int32
		proxy.HandleTask("gracefulcancelflow.verify:428/rtp-g", func(ctx context.Context, f *workflow.Flow) error {
			if n := attempts.Add(1); n > 1 {
				return errors.New("re-ran after cancel") // a regression fails fast rather than retrying forever
			}
			close(running)
			<-release
			f.SetBool("gWorked", true)
			f.Retry(10*time.Millisecond, 1.0, 0, 0)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/rtp-handler", recordCancellation)

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		defer releaseOnce.Do(func() { close(release) })
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/retry-then-preempted", nil, nil)
		if !assert.NoError(err) {
			return
		}
		awaitSignal(t, running, "G starting")
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		releaseOnce.Do(func() { close(release) })

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(int32(1), attempts.Load(), "the rewound step must be preempted, not re-run")
		assert.True(outcome.State.GetBool("recovered"))
		assert.True(outcome.State.GetBool("gWorked"), "the retry carries the first attempt's changes to the handler")
	})

	// subgraph, Cancel during the caller's first run: E arms a subgraph after Cancel landed. The child is
	// spawned and runs, and when it returns E runs once more - seeing the child's result - before its own
	// cancellation redirects it to its handler, which receives what E made of that result.
	t.Run("subgraph_caller_sees_child_result", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("SubgraphCallerSeesChildResult")
		graph.SetEndpoint("E", "gracefulcancelflow.verify:428/scr-e")
		graph.SetEndpoint("F", "gracefulcancelflow.verify:428/scr-f")
		graph.SetEndpoint("Handler", "gracefulcancelflow.verify:428/scr-handler")
		graph.AddTransitionChain("E", "F", workflow.END)
		graph.AddTransitionOnError("E", "Handler")
		graph.AddTransition("Handler", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/subgraph-caller-sees-child-result", graph)
		child := workflow.NewGraph("SubgraphChild")
		child.SetEndpoint("K", "gracefulcancelflow.verify:428/scr-k")
		child.AddTransition("K", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/scr-child", child)

		running := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		var eRuns atomic.Int32
		var fRan atomic.Bool
		proxy.HandleTask("gracefulcancelflow.verify:428/scr-e", func(ctx context.Context, f *workflow.Flow) error {
			if eRuns.Add(1) == 1 {
				close(running)
				<-release
			}
			var out workflow.State
			yield, err := f.Subgraph("gracefulcancelflow.verify:428/scr-child", nil, &out)
			if yield || err != nil {
				return err
			}
			f.SetInt("fromChild", out.GetInt("childOut"))
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/scr-k", func(ctx context.Context, f *workflow.Flow) error {
			f.SetInt("childOut", 7)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/scr-f", func(ctx context.Context, f *workflow.Flow) error {
			fRan.Store(true)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/scr-handler", recordCancellation)

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		defer releaseOnce.Do(func() { close(release) })
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/subgraph-caller-sees-child-result", nil, nil)
		if !assert.NoError(err) {
			return
		}
		awaitSignal(t, running, "E starting")
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		releaseOnce.Do(func() { close(release) })

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(int32(2), eRuns.Load(), "the caller runs once more when its child returns")
		assert.False(fRan.Load(), "E's transition must be redirected to its handler")
		assert.True(outcome.State.GetBool("recovered"))
		assert.Equal(7, outcome.State.GetInt("fromChild"), "the handler receives what E made of the child's result")
	})

	// subgraph, Cancel while the child is running: the parked caller and the child's running step are both
	// covered. The child's step, with no handler, fails the child; the caller runs once more, receives that
	// failure from flow.Subgraph, and is then redirected to its own handler.
	t.Run("subgraph_cancelled_while_child_runs", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("SubgraphCancelledWhileChildRuns")
		graph.SetEndpoint("E", "gracefulcancelflow.verify:428/scw-e")
		graph.SetEndpoint("F", "gracefulcancelflow.verify:428/scw-f")
		graph.SetEndpoint("Handler", "gracefulcancelflow.verify:428/scw-handler")
		graph.AddTransitionChain("E", "F", workflow.END)
		graph.AddTransitionOnError("E", "Handler")
		graph.AddTransition("Handler", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/subgraph-cancelled-while-child-runs", graph)
		child := workflow.NewGraph("SubgraphChildHeld")
		child.SetEndpoint("K", "gracefulcancelflow.verify:428/scw-k")
		child.AddTransition("K", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/scw-child", child)

		running := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		var eRuns atomic.Int32
		var fRan atomic.Bool
		proxy.HandleTask("gracefulcancelflow.verify:428/scw-e", func(ctx context.Context, f *workflow.Flow) error {
			eRuns.Add(1)
			yield, err := f.Subgraph("gracefulcancelflow.verify:428/scw-child", nil, nil)
			if yield {
				return nil
			}
			if err != nil {
				f.SetString("childErr", err.Error())
				f.SetBool("childCancelled", workflow.IsCancelled(err))
			}
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/scw-k", func(ctx context.Context, f *workflow.Flow) error {
			close(running)
			<-release
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/scw-f", func(ctx context.Context, f *workflow.Flow) error {
			fRan.Store(true)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/scw-handler", recordCancellation)

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		defer releaseOnce.Do(func() { close(release) })
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/subgraph-cancelled-while-child-runs", nil, nil)
		if !assert.NoError(err) {
			return
		}
		awaitSignal(t, running, "the child's step starting")
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		releaseOnce.Do(func() { close(release) })

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(int32(2), eRuns.Load(), "the caller runs once more when its child returns")
		assert.False(fRan.Load(), "E's transition must be redirected to its handler")
		assert.True(outcome.State.GetBool("recovered"), "the caller itself is covered, not only its child")
		assert.Contains(outcome.State.GetString("childErr"), "flow cancelled", "the caller saw its child's cancellation")
		assert.True(outcome.State.GetBool("childCancelled"), "the child's cancellation crosses the subgraph boundary with its marker")
	})

	// interrupt: I is interrupted when Cancel lands. A later Resume does not run the task with the resume
	// data - the step is preempted, and its handler receives the cancellation instead.
	t.Run("interrupt_resumed_is_preempted", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("InterruptResumedIsPreempted")
		graph.SetEndpoint("I", "gracefulcancelflow.verify:428/irp-i")
		graph.SetEndpoint("Handler", "gracefulcancelflow.verify:428/irp-handler")
		graph.AddTransition("I", workflow.END)
		graph.AddTransitionOnError("I", "Handler")
		graph.AddTransition("Handler", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/interrupt-resumed-is-preempted", graph)
		var iRuns atomic.Int32
		var sawResume atomic.Bool
		proxy.HandleTask("gracefulcancelflow.verify:428/irp-i", func(ctx context.Context, f *workflow.Flow) error {
			iRuns.Add(1)
			var resume map[string]any
			yield, err := f.Interrupt(map[string]any{"ask": "approve?"}, &resume)
			if yield || err != nil {
				return err
			}
			sawResume.Store(true)
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/irp-handler", recordCancellation)

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/interrupt-resumed-is-preempted", nil, nil)
		if !assert.NoError(err) {
			return
		}
		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, outcome.Status) {
			return
		}
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"answer": "yes"}))

		outcome, err = eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(int32(1), iRuns.Load(), "the resumed step must be preempted, not re-run")
		assert.False(sawResume.Load(), "resume data arriving after Cancel must not be acted on")
		assert.True(outcome.State.GetBool("recovered"))
	})

	// fanOut builds Src -forEach(items as item)-> W -> Join(fan-in) -> END, with W's behaviour supplied.
	fanOut := func(name string, w engine.TaskHandler) string {
		url := "gracefulcancelflow.verify:428/" + name
		g := workflow.NewGraph(name)
		g.SetEndpoint("Src", url+"-src")
		g.SetEndpoint("W", url+"-w")
		g.SetEndpoint("Join", url+"-join")
		g.AddTransitionForEach("Src", "W", "items", "item")
		g.AddTransition("W", "Join")
		g.SetFanIn("Join")
		g.AddTransition("Join", workflow.END)
		proxy.HandleGraph(url, g)
		proxy.HandleTask(url+"-src", func(ctx context.Context, f *workflow.Flow) error {
			f.Set("items", []string{"a", "b"})
			return nil
		})
		proxy.HandleTask(url+"-w", w)
		proxy.HandleTask(url+"-join", func(ctx context.Context, f *workflow.Flow) error { return nil })
		return url
	}
	// holdBranches makes W report which branch it is on its FIRST run, then hold until released; a later run
	// (a Fork's re-run) neither reports nor holds.
	holdBranches := func(running chan<- string, release <-chan struct{}) func(f *workflow.Flow) {
		return func(f *workflow.Flow) {
			select {
			case running <- f.GetString("item"):
				<-release
			default:
			}
		}
	}
	awaitBranches := func(t *testing.T, running <-chan string, n int) {
		for range n {
			select {
			case <-running:
			case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
				t.Fatal("fan-out branches never started")
			}
		}
	}

	// fan-out, every loss a cancellation: both branches are running when Cancel lands, and both finish
	// covered with no handler. The cohort resolves with only cancellations, so the flow is cancelled.
	t.Run("fanout_all_cancelled", func(t *testing.T) {
		assert := testarossa.For(t)
		running := make(chan string, 2)
		release := make(chan struct{})
		var releaseOnce sync.Once
		hold := holdBranches(running, release)
		url := fanOut("FanOutAllCancelled", func(ctx context.Context, f *workflow.Flow) error {
			hold(f)
			return nil
		})

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		defer releaseOnce.Do(func() { close(release) })
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, url, nil, nil)
		if !assert.NoError(err) {
			return
		}
		awaitBranches(t, running, 2)
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		releaseOnce.Do(func() { close(release) })

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal("test reason", outcome.CancelReason)
	})

	// fan-out, a real error mixed in: branch a fails for real while branch b is cancelled. A real error wins,
	// so the flow is failed and reports a's error. Forking at a with the fault fixed re-runs a alone; the
	// clone keeps b's cancellation, so the cohort now resolves with only cancellations - the fork is
	// cancelled, which it can be only if the clone re-derived the cancellation count along with the failures.
	t.Run("fanout_mixed_fails_then_fork_resolves_cancelled", func(t *testing.T) {
		assert := testarossa.For(t)
		running := make(chan string, 2)
		release := make(chan struct{})
		var releaseOnce sync.Once
		hold := holdBranches(running, release)
		var aFixedRuns atomic.Int32
		url := fanOut("FanOutMixed", func(ctx context.Context, f *workflow.Flow) error {
			hold(f)
			if f.GetString("item") == "a" && f.GetBool("fixed") {
				aFixedRuns.Add(1)
			}
			if f.GetString("item") == "a" && !f.GetBool("fixed") {
				return errors.New("a boom")
			}
			return nil
		})

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		defer releaseOnce.Do(func() { close(release) })
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, url, nil, nil)
		if !assert.NoError(err) {
			return
		}
		awaitBranches(t, running, 2)
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		releaseOnce.Do(func() { close(release) })

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusFailed, outcome.Status, "a real error mixed into the cohort wins")
		assert.Contains(outcome.Error, "a boom")

		steps, err := eng.History(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		var aKey string
		for _, s := range steps {
			if s.TaskName != "W" {
				continue
			}
			if step, err := eng.Step(ctx, s.StepKey); err == nil && step.State.GetString("item") == "a" {
				aKey = s.StepKey
			}
		}
		if !assert.NotEqual("", aKey) {
			return
		}
		forkKey, err := eng.Fork(ctx, aKey, map[string]any{"fixed": true})
		if !assert.NoError(err) {
			return
		}
		forked, err := eng.Await(ctx, forkKey)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCancelled, forked.Status, "the kept cancelled branch is now the cohort's only loss")
			// Not preempted by a mark carried over from the origin, where a was running when Cancel landed: the
			// fork re-runs it, so the kept cancelled branch is the only loss by the recount, not by accident.
			assert.Equal(int32(1), aFixedRuns.Load(), "the fork re-runs branch a rather than preempting it")
			assert.Equal("test reason", forked.CancelReason, "the fork reports the reason for the cancellation it inherited")
		}
	})

	// subgraph, the child's cancellation propagates: E has no handler and simply returns the error its
	// child's cancellation delivered. The error still carries the marker after crossing the boundary, so E's
	// own loss is a cancellation and the flow resolves cancelled rather than failed.
	t.Run("subgraph_child_cancellation_propagates", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("SubgraphChildCancellationPropagates")
		graph.SetEndpoint("E", "gracefulcancelflow.verify:428/sccp-e")
		graph.AddTransition("E", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/subgraph-child-cancellation-propagates", graph)
		child := workflow.NewGraph("SubgraphChildCancellationPropagatesChild")
		child.SetEndpoint("K", "gracefulcancelflow.verify:428/sccp-k")
		child.AddTransition("K", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/sccp-child", child)

		running := make(chan struct{})
		release := make(chan struct{})
		var releaseOnce sync.Once
		proxy.HandleTask("gracefulcancelflow.verify:428/sccp-e", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph("gracefulcancelflow.verify:428/sccp-child", nil, nil)
			if yield {
				return nil
			}
			return err
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/sccp-k", func(ctx context.Context, f *workflow.Flow) error {
			close(running)
			<-release
			return nil
		})

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		defer releaseOnce.Do(func() { close(release) })
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, err := eng.Create(ctx, "gracefulcancelflow.verify:428/subgraph-child-cancellation-propagates", nil, nil)
		if !assert.NoError(err) {
			return
		}
		awaitSignal(t, running, "the child's step starting")
		assert.NoError(eng.Cancel(ctx, flowKey, "test reason"))
		releaseOnce.Do(func() { close(release) })

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal("test reason", outcome.CancelReason)
	})

	// subgraph error fidelity: a child's real failure reaches its caller with its status code intact, not
	// flattened to a bare message.
	t.Run("subgraph_error_keeps_status_code", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("SubgraphErrorKeepsStatusCode")
		graph.SetEndpoint("E", "gracefulcancelflow.verify:428/sesc-e")
		graph.AddTransition("E", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/subgraph-error-keeps-status-code", graph)
		child := workflow.NewGraph("SubgraphErrorKeepsStatusCodeChild")
		child.SetEndpoint("K", "gracefulcancelflow.verify:428/sesc-k")
		child.AddTransition("K", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/sesc-child", child)
		proxy.HandleTask("gracefulcancelflow.verify:428/sesc-e", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph("gracefulcancelflow.verify:428/sesc-child", nil, nil)
			if yield {
				return nil
			}
			f.SetInt("code", errors.StatusCode(err))
			f.SetString("msg", err.Error())
			f.SetBool("cancelled", workflow.IsCancelled(err))
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/sesc-k", func(ctx context.Context, f *workflow.Flow) error {
			return errors.New("nope: %s", "50% off", http.StatusNotFound)
		})

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		_, outcome, err := eng.Run(ctx, "gracefulcancelflow.verify:428/subgraph-error-keeps-status-code", nil, nil)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal(http.StatusNotFound, outcome.State.GetInt("code"))
		assert.Equal("nope: 50% off", outcome.State.GetString("msg"), "a %% in the child's message survives the hop")
		assert.False(outcome.State.GetBool("cancelled"))
	})

	// guards: an unknown key is a 404, a subgraph child's key a 400 (the tree is addressed by its root), and
	// a terminal flow is a benign no-op rather than a conflict.
	t.Run("guards", func(t *testing.T) {
		assert := testarossa.For(t)
		graph := workflow.NewGraph("Guards")
		graph.SetEndpoint("P", "gracefulcancelflow.verify:428/g-p")
		graph.AddTransition("P", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/guards", graph)
		child := workflow.NewGraph("GuardsChild")
		child.SetEndpoint("Q", "gracefulcancelflow.verify:428/g-q")
		child.AddTransition("Q", workflow.END)
		proxy.HandleGraph("gracefulcancelflow.verify:428/guards-child", child)
		var childKey atomic.Value
		proxy.HandleTask("gracefulcancelflow.verify:428/g-p", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph("gracefulcancelflow.verify:428/guards-child", nil, nil)
			if yield || err != nil {
				return err
			}
			return nil
		})
		proxy.HandleTask("gracefulcancelflow.verify:428/g-q", func(ctx context.Context, f *workflow.Flow) error {
			childKey.Store(f.FlowKey())
			return nil
		})

		eng := engine.NewEngineUnderTest(t.Name())
		defer eng.Shutdown(ctx)
		eng.SetHost(proxy)
		assert.NoError(eng.Startup(t.Context()))

		flowKey, outcome, err := eng.Run(ctx, "gracefulcancelflow.verify:428/guards", nil, nil)
		if !assert.NoError(err) || !assert.Equal(workflow.StatusCompleted, outcome.Status) {
			return
		}

		unknown := flowKey[:strings.LastIndex(flowKey, "-")+1] + strings.Repeat("0", len(flowKey)-strings.LastIndex(flowKey, "-")-1)
		err = eng.Cancel(ctx, unknown, "x")
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))

		err = eng.Cancel(ctx, childKey.Load().(string), "x")
		assert.Equal(http.StatusBadRequest, errors.StatusCode(err))

		assert.NoError(eng.Cancel(ctx, flowKey, "x"), "cancelling a terminal flow is a no-op")
		outcome, err = eng.Snapshot(ctx, flowKey)
		if assert.NoError(err) {
			assert.Equal(workflow.StatusCompleted, outcome.Status, "a terminal outcome is never rewritten")
		}
	})
}
