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
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// TestForkInterruptedCohortFlow pins why forking a failed branch of a cohort that also holds an interrupted
// branch is not exploitable: the origin can never be terminal. A cohort can only fail-escalate once it fully
// arrives, but an interrupted branch never arrives, so the flow rests `interrupted` (not `failed`), and Fork
// rejects a non-terminal root. This is the structural guarantee behind the fork.go cloneSubtree guard - the
// interrupted step never reaches a fork clone in the first place.
func TestForkInterruptedCohortFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	graph := workflow.NewGraph("ForkInterruptedCohort")
	graph.SetEndpoint("Src", "forkinterruptedcohortflow.verify:428/src")
	graph.SetEndpoint("A", "forkinterruptedcohortflow.verify:428/a") // fails, no onError
	graph.SetEndpoint("B", "forkinterruptedcohortflow.verify:428/b") // interrupts
	graph.SetEndpoint("J", "forkinterruptedcohortflow.verify:428/j")
	graph.SetFanIn("J")
	graph.AddTransition("Src", "A")
	graph.AddTransition("Src", "B")
	graph.AddTransition("A", "J")
	graph.AddTransitionChain("B", "J", workflow.END)
	proxy.HandleGraph("forkinterruptedcohortflow.verify:428/fork-interrupted-cohort", graph)

	proxy.HandleTask("forkinterruptedcohortflow.verify:428/src", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})
	proxy.HandleTask("forkinterruptedcohortflow.verify:428/a", func(ctx context.Context, f *workflow.Flow) error {
		return errors.New("branch A failed")
	})
	proxy.HandleTask("forkinterruptedcohortflow.verify:428/b", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"branch": "B"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	proxy.HandleTask("forkinterruptedcohortflow.verify:428/j", func(ctx context.Context, f *workflow.Flow) error {
		return nil
	})

	t.Run("failed_plus_interrupted_cohort_rests_interrupted_and_fork_is_rejected", func(t *testing.T) {
		assert := testarossa.For(t)

		flowKey, err := eng.Create(ctx, "forkinterruptedcohortflow.verify:428/fork-interrupted-cohort", nil, nil)
		if !assert.NoError(err) {
			return
		}

		outcome, err := eng.Await(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		// The failed branch does NOT terminalize the flow: the cohort can't fully arrive while B is interrupted.
		assert.Equal(workflow.StatusInterrupted, outcome.Status)

		// Branch A's failure races the Await that returned on B's interrupt, so wait for A to actually reach
		// `failed` (and B `interrupted`) before asserting the flow held.
		var aKey string
		deadline := time.Now().Add(3 * time.Second)
		for {
			hist, herr := eng.History(ctx, flowKey)
			if !assert.NoError(herr) {
				return
			}
			aStatus, bStatus := "", ""
			aKey = ""
			for _, s := range hist {
				switch s.TaskName {
				case "A":
					aStatus, aKey = s.Status, s.StepKey
				case "B":
					bStatus = s.Status
				}
			}
			if aStatus == workflow.StatusFailed && bStatus == workflow.StatusInterrupted {
				break
			}
			if time.Now().After(deadline) {
				assert.Equal(workflow.StatusFailed, aStatus)
				assert.Equal(workflow.StatusInterrupted, bStatus)
				return
			}
			time.Sleep(20 * time.Millisecond)
		}

		// Crux: with branch A now failed, the flow STILL rests interrupted - it did not fail-escalate.
		snap, err := eng.Snapshot(ctx, flowKey)
		if !assert.NoError(err) {
			return
		}
		assert.Equal(workflow.StatusInterrupted, snap.Status)

		// Forking the failed branch (this scenario) is rejected: its root flow is non-terminal, so the
		// interrupted sibling can never be cloned into a running fork.
		if !assert.NotEqual("", aKey) {
			return
		}
		forkKey, err := eng.Fork(ctx, aKey, nil)
		assert.Error(err)
		assert.Equal("", forkKey)
		assert.Equal(http.StatusConflict, errors.StatusCode(err))
	})
}
