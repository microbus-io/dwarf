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
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestAwait_WakesWhenNoSignalIsDelivered pins that Await returns even when the in-memory signalStop wake
// is never delivered - a worker crash between the terminal commit and the wake, or the ordinary
// cross-replica case, where the stop happens in another process entirely and nothing is sent at all. The
// test blocks a flow so Await parks on the latch against a running flow,
// then forges the terminal commit directly in the DB (bypassing signalStop entirely), and asserts Await
// still wakes rather than hanging until ctx (here, forever).
//
// This is the shape of EVERY cross-replica stop, not only a lost signal: a flow finished by a peer
// commits in the shared database with nothing to announce it locally. So what this really pins is the
// latch detector - the sweep is the only thing that can see a stop this replica did not make.
func TestAwait_WakesWhenNoSignalIsDelivered(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	defer rel()

	g := workflow.NewGraph("AwaitPoll")
	g.SetEndpoint("Block", "awaitpoll.verify:0/block")
	g.AddTransition("Block", workflow.END)
	proxy.HandleGraph("awaitpoll.verify:0/g", g)
	proxy.HandleTask("awaitpoll.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "awaitpoll.verify:0/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shard, flowID, _, err := keys.ParseFlowKey(flowKey)
	if !assert.NoError(err) {
		return
	}
	db, err := e.db.Shard(shard)
	if !assert.NoError(err) {
		return
	}

	type result struct {
		out *workflow.FlowOutcome
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, aerr := e.await(ctx, flowKey)
		done <- result{out, aerr}
	}()

	// Wait until Await has parked on the latch (so its first snapshot has already run against the running
	// flow). Only after this can a terminal commit exercise a wake path rather than being caught by that
	// first snapshot.
	waiterReady := func() bool {
		return e.latches.Waiting(flowKey) > 0
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && !waiterReady() {
		time.Sleep(5 * time.Millisecond)
	}
	if !assert.True(waiterReady(), "Await never registered a waiter") {
		return
	}
	time.Sleep(50 * time.Millisecond) // let the first snapshot (running) complete and the select block

	// Forge the terminal commit WITHOUT signalStop - the exact state the crash window leaves.
	_, err = db.ExecContext(ctx,
		"UPDATE dwarf_flows SET status=?, final_state=?, updated_at=NOW_UTC() WHERE flow_id=?",
		workflow.StatusCompleted, []byte(`{"ok":true}`), flowID)
	assert.NoError(err)

	select {
	case r := <-done:
		assert.NoError(r.err)
		if r.out != nil {
			assert.Equal(workflow.StatusCompleted, r.out.Status)
			ok, _ := stateVal(r.out.State, "ok").(bool)
			assert.True(ok, "final_state should surface through the poll-fallback wake")
		}
	case <-time.After(3 * time.Second):
		assert.True(false, "Await hung past a stop nothing announced; the latch detector did not notice it")
		return
	}
	rel()
}

// TestPoll_RunningThenStopped pins the difference between Poll and Await on a deadline: while the flow is still
// running, Poll returns a non-terminal outcome with no error (so a caller can re-poll), whereas Await turns the
// same deadline into a timeout error. Once the flow stops, Poll returns the terminal outcome.
func TestPoll_RunningThenStopped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	defer rel()

	g := workflow.NewGraph("PollTest")
	g.SetEndpoint("Block", "polltest.verify:0/block")
	g.AddTransition("Block", workflow.END)
	proxy.HandleGraph("polltest.verify:0/g", g)
	proxy.HandleTask("polltest.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "polltest.verify:0/g", nil, nil)
	if !assert.NoError(err) {
		return
	}

	// While the flow is blocked, Poll with a short deadline returns a running outcome and no error.
	pollCtx, cancel := context.WithTimeout(ctx, 150*time.Millisecond)
	out, perr := e.Poll(pollCtx, flowKey)
	cancel()
	assert.NoError(perr)
	if assert.True(out != nil, "Poll returned a nil outcome") {
		assert.False(out.Stopped(), "a still-running flow must not be Stopped()")
	}

	// Await with the same short deadline instead surfaces a timeout error.
	awaitCtx, cancel2 := context.WithTimeout(ctx, 150*time.Millisecond)
	_, aerr := e.Await(awaitCtx, flowKey)
	cancel2()
	assert.Error(aerr)

	// Release the flow; Poll then returns the terminal outcome.
	rel()
	out, perr = e.Poll(ctx, flowKey)
	assert.NoError(perr)
	if assert.True(out != nil, "Poll returned a nil outcome after completion") {
		assert.True(out.Stopped())
		assert.Equal(workflow.StatusCompleted, out.Status)
	}
}

// TestAwait_IgnoresANonSettledWake pins the invariant the loop-free await rests on: a release means the flow
// has SETTLED, so the caller reads the row once and returns what it finds without re-checking. Wake it for a
// status the flow is merely passing through and that read lands on a still-running flow, which Await would
// then hand back as the outcome - a completed-looking answer for work that has not happened.
//
// signalStop is what guards this, by dropping a non-stopped status instead of delivering it. The assertion is
// therefore that a `running` wake changes NOTHING: the caller stays parked, and only the real stop returns it.
func TestAwait_IgnoresANonSettledWake(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	proxy := NewTestProxy()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	defer rel()

	g := workflow.NewGraph("NonSettled")
	g.SetEndpoint("Block", "nonsettled.verify:0/block")
	g.AddTransition("Block", workflow.END)
	proxy.HandleGraph("nonsettled.verify:0/g", g)
	proxy.HandleTask("nonsettled.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(proxy)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "nonsettled.verify:0/g", nil, nil)
	if !assert.NoError(err) {
		return
	}

	type result struct {
		out *workflow.FlowOutcome
		err error
	}
	done := make(chan result, 1)
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	go func() {
		out, aerr := e.await(awaitCtx, flowKey)
		done <- result{out, aerr}
	}()

	// Park first - before that, there is no waiter for a wake to displace and the test would prove nothing.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && e.latches.Waiting(flowKey) == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	if !assert.True(e.latches.Waiting(flowKey) > 0, "Await never parked") {
		return
	}

	// The wake under test. The flow really is running, so this is exactly the status a mid-flight transition
	// site would pass - and it must not reach the board.
	e.signalStop(ctx, flowKey, workflow.StatusRunning)

	select {
	case r := <-done:
		assert.True(false, "a running wake returned Await early with %v (outcome %+v)", r.err, r.out)
		return
	case <-time.After(500 * time.Millisecond):
		// Still parked, which is the point. The window comfortably spans several detector sweeps, so this
		// also pins that the sweep does not release a running flow either.
	}

	// The real stop must still return it, or the guard would be indistinguishable from a broken wake path.
	rel()
	select {
	case r := <-done:
		assert.NoError(r.err)
		if assert.NotNil(r.out) {
			assert.Equal(workflow.StatusCompleted, r.out.Status)
		}
	case <-time.After(10 * time.Second):
		assert.True(false, "Await never returned after the flow actually stopped")
	}
}

// stateVal reads a field as an untyped value. It lives here rather than on State because Get already is
// this, with a type the caller chooses; a one-line untyped read is a test convenience, not API.
func stateVal(s workflow.State, name string) any {
	var v any
	_, _ = s.Get(name, &v)
	return v
}
