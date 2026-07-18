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

// TestAwait_PollFallbackWhenSignalLost pins that Await returns even when the in-memory signalStop wake is
// never delivered - a worker crash between the terminal commit and the signal, a dropped peer broadcast, or
// a no-op SignalPeers on a multi-replica host. The test blocks a flow so Await parks in its select on a
// running flow, then forges the terminal commit directly in the DB (bypassing signalStop entirely), and
// asserts Await still wakes via its periodic re-snapshot rather than hanging until ctx (here, forever).
func TestAwait_PollFallbackWhenSignalLost(t *testing.T) {
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

	e := NewEngine()
	e.SetHost(proxy)
	// Read once when Await builds its ticker, so set it before Startup.
	e.awaitPollInterval = 20 * time.Millisecond
	e.RunInTest(t)

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

	// Wait until Await has registered its waiter (so its first snapshot has run against the running flow and
	// it is now parked in the select). Only after this can a terminal commit exercise the poll fallback
	// rather than being caught by the first snapshot.
	waiterReady := func() bool {
		e.waitersLock.Lock()
		defer e.waitersLock.Unlock()
		return len(e.waiters[flowKey]) > 0
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
			ok, _ := r.out.State["ok"].(bool)
			assert.True(ok, "final_state should surface through the poll-fallback wake")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Await hung past the lost signal; the poll fallback did not fire")
	}
	rel()
}

// TestPoll_RunningThenStopped pins the difference between Poll and Await on a deadline: while the flow is still
// running, Poll returns a non-terminal outcome with no error (so a caller can re-poll), whereas Await turns the
// same deadline into a timeout error. Once the flow stops, Poll returns the terminal outcome.
func TestPoll_RunningThenStopped(t *testing.T) {
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

	e := NewEngine()
	e.SetHost(proxy)
	e.awaitPollInterval = 20 * time.Millisecond
	e.RunInTest(t)

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
