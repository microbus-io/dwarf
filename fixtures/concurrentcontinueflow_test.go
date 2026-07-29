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

/*
Concurrent Continue on one thread. Continue bases a new turn on the thread's
latest non-fork flow and rejects with 409 if that latest flow is not completed. continueFlow runs
that check and the new-turn insert in ONE transaction under a write-first lock on the thread anchor
row, so concurrent Continues on one thread are serialized deterministically: exactly one succeeds and
the rest get 409 - the linear thread can never silently branch into sibling turns.

This test fires N concurrent Continues against the same completed turn and asserts exactly one succeeds
and N-1 fail with 409. The winning turn's task blocks until after every Continue has returned, so the
new turn stays `running` throughout the race - a loser can never observe it as `completed` and continue
IT instead (which would yield a second success). It then releases the winner, confirms it completes
with the carried-over state, and that the thread holds exactly 2 flows.
*/
package fixtures

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

func TestConcurrentContinueflow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	proxy := engine.NewTestProxy()

	// block gates the Increment task: armed only after turn 1 completes, so the single winning
	// continuation turn parks `running` until the test releases it - keeping it non-completable during the
	// race so no losing Continue can pick it up as the thread's latest completed turn.
	var armed atomic.Bool
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseTask := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseTask()

	graph := workflow.NewGraph("Counting")
	graph.SetEndpoint("Increment", "concurrentcontinue.verify:428/increment")
	graph.AddTransition("Increment", workflow.END)
	proxy.HandleGraph("concurrentcontinue.verify:428/counting", graph)
	proxy.HandleTask("concurrentcontinue.verify:428/increment", func(ctx context.Context, f *workflow.Flow) error {
		if armed.Load() {
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
		f.SetInt("counter", f.GetInt("counter")+1)
		return nil
	})

	eng := engine.NewEngineUnderTest(t)
	eng.SetHost(proxy)
	assert.NoError(eng.Startup(t.Context()))

	// Turn 1: run to completion (task not armed). counter 0 -> 1.
	turn1, outcome, err := eng.Run(ctx, "concurrentcontinue.verify:428/counting", map[string]any{"counter": 0}, nil)
	if !assert.NoError(err) {
		return
	}
	assert.Equal(workflow.StatusCompleted, outcome.Status)
	assert.Equal(1.0, outcome.State.Value("counter"))

	// From now on, any continuation turn's task parks until released.
	armed.Store(true)

	// Fire N concurrent Continue calls against the same thread, released together via a start channel.
	const n = 6
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]string, n)
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i], errs[i] = eng.Continue(ctx, turn1, map[string]any{})
		}(i)
	}
	close(start)
	wg.Wait()

	// Exactly one Continue succeeds; the other n-1 are rejected with 409 (they saw the winner's still-
	// running turn as the thread's latest, which is not completed).
	var winner string
	successes := 0
	conflicts := 0
	for i := range n {
		if errs[i] == nil {
			successes++
			winner = results[i]
			assert.NotEqual("", winner)
		} else {
			assert.Equal(http.StatusConflict, errors.StatusCode(errs[i]),
				"a losing concurrent Continue must fail with 409, got: %v", errs[i])
			conflicts++
		}
	}
	assert.Equal(1, successes, "exactly one concurrent Continue should succeed")
	assert.Equal(n-1, conflicts, "the other n-1 Continues should be 409")

	// Release the winner; it derives from turn 1's final_state (counter=1) and completes at counter=2.
	releaseTask()
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	out, err := eng.Await(awaitCtx, winner)
	cancel()
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal(2.0, out.State.Value("counter"), "the winning turn should have seen turn 1's counter=1")
	}

	// The thread holds exactly turn 1 plus the single winning continuation.
	summaries, _, err := eng.List(ctx, workflow.Query{ThreadKey: turn1})
	if assert.NoError(err) {
		assert.Equal(2, len(summaries))
		keys := map[string]bool{}
		for _, s := range summaries {
			keys[s.FlowKey] = true
		}
		assert.True(keys[turn1])
		assert.True(keys[winner])
	}
}
