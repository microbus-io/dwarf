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
	"sync"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
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

	mux := hosts.NewMux()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	defer rel()

	g := workflow.NewGraph("AwaitPoll")
	g.SetURL("Block", "awaitpoll.verify:0/block")
	g.AddTransition("Block", workflow.End)
	mux.HandleGraph("awaitpoll.verify:0/g", g)
	mux.HandleTask("awaitpoll.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "awaitpoll.verify:0/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	shard, _, flowID, _, err := keys.ParseFlowKey(flowKey)
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
	parked := e.seams.Waiter(engineseams.Join(engineseams.CheckpointAwaitParked, flowKey)) // armed before the goroutine that trips it
	go func() {
		out, aerr := e.await(ctx, flowKey)
		done <- result{out, aerr}
	}()

	// Wait until Await has parked on the latch, which it does only after its first snapshot read the running
	// flow. Only after this can a terminal commit exercise a wake path rather than being caught by that
	// first snapshot.
	select {
	case <-parked:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await never parked")
		return
	}

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
	case <-time.After(3 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await hung past a stop nothing announced; the latch detector did not notice it")
		return
	}
	rel()
}

// TestAwait_DefaultBudgetIsADeadlineError pins the timeout a caller gets when its ctx names no deadline
// and the wait runs out on await's default budget instead. ctx.Err() is nil then, so Await must still
// report a 408 that wraps context.DeadlineExceeded, and Run must not return a nil error beside its nil
// outcome.
func TestAwait_DefaultBudgetIsADeadlineError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	release := make(chan struct{})

	g := workflow.NewGraph("AwaitPoll")
	g.SetURL("Block", "awaitpoll.verify:0/block")
	g.AddTransition("Block", workflow.End)
	mux.HandleGraph("awaitpoll.verify:0/g", g)
	mux.HandleTask("awaitpoll.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer close(release) // before Shutdown, which waits for the blocked task
	e.SetHost(mux)
	e.awaitDefaultBudget = 200 * time.Millisecond
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "awaitpoll.verify:0/g", nil, nil)
	if !assert.NoError(err) {
		return
	}
	out, err := e.Await(ctx, flowKey)
	assert.Nil(out)
	assert.True(errors.Is(err, context.DeadlineExceeded), "Await: %v", err)
	assert.Equal(http.StatusRequestTimeout, errors.StatusCode(err))

	runKey, out, err := e.Run(ctx, "awaitpoll.verify:0/g", nil, nil)
	assert.NotEqual("", runKey)
	assert.Nil(out)
	assert.True(errors.Is(err, context.DeadlineExceeded), "Run: %v", err)
	assert.Equal(http.StatusRequestTimeout, errors.StatusCode(err))
}

// TestPoll_RunningThenStopped pins the difference between Poll and Await on a deadline: while the flow is still
// running, Poll returns a non-terminal outcome with no error (so a caller can re-poll), whereas Await turns the
// same deadline into a timeout error. Once the flow stops, Poll returns the terminal outcome.
func TestPoll_RunningThenStopped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }
	defer rel()

	g := workflow.NewGraph("PollTest")
	g.SetURL("Block", "polltest.verify:0/block")
	g.AddTransition("Block", workflow.End)
	mux.HandleGraph("polltest.verify:0/g", g)
	mux.HandleTask("polltest.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	e.SetHost(mux)
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

// TestAwait_IgnoresANonSettledWake pins that a release means the flow reached a STOP: signalStop drops a
// non-stopped status instead of delivering it, so a `running` wake changes NOTHING - the caller stays parked,
// and only the real stop returns it.
func TestAwait_IgnoresANonSettledWake(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }

	g := workflow.NewGraph("NonSettled")
	g.SetURL("Block", "nonsettled.verify:0/block")
	g.AddTransition("Block", workflow.End)
	mux.HandleGraph("nonsettled.verify:0/g", g)
	mux.HandleTask("nonsettled.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer rel() // below the Shutdown defer, so it unwinds first and the drain never waits on the held task
	e.SetHost(mux)
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
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	parked := e.seams.Waiter(engineseams.Join(engineseams.CheckpointAwaitParked, flowKey)) // armed before the goroutine that trips it
	go func() {
		out, aerr := e.await(awaitCtx, flowKey)
		done <- result{out, aerr}
	}()

	// Park first - before that, there is no waiter for a wake to displace and the test would prove nothing.
	select {
	case <-parked:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await never parked")
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
	case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await never returned after the flow actually stopped")
	}
}

// TestAwait_StaleStopReparks pins that a release whose stop the flow has already left re-parks the caller
// rather than answering it. An interrupted flow can be Resumed between its signalStop and the woken caller's
// read, so the read finds it running while the caller's ctx is still live; reporting that as a timeout hands a
// 408 to a caller that asked for none. Delivering an `interrupted` release to a parked Await while the flow
// runs is that shape exactly: Await must stay parked until the flow really stops.
func TestAwait_StaleStopReparks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }

	g := workflow.NewGraph("StaleStop")
	g.SetURL("Block", "stalestop.verify:0/block")
	g.AddTransition("Block", workflow.End)
	mux.HandleGraph("stalestop.verify:0/g", g)
	mux.HandleTask("stalestop.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer rel() // below the Shutdown defer, so it unwinds first and the drain never waits on the held task
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "stalestop.verify:0/g", nil, nil)
	if !assert.NoError(err) {
		return
	}

	type result struct {
		out *workflow.FlowOutcome
		err error
	}
	done := make(chan result, 1)
	awaitCtx, cancel := context.WithTimeout(ctx, 30*time.Second*enginetest.TimeoutScale())
	defer cancel()
	parkedName := engineseams.Join(engineseams.CheckpointAwaitParked, flowKey)
	parked := e.seams.Waiter(parkedName) // armed before the goroutine that trips it
	go func() {
		out, aerr := e.awaitStopped(awaitCtx, flowKey)
		done <- result{out, aerr}
	}()
	select {
	case <-parked:
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await never parked")
		return
	}

	// A real stop status, as a flow that interrupted and was then resumed would have signalled. Parking again
	// is the proof Await read the flow running and did not return on the stale stop.
	reparked := e.seams.Waiter(parkedName)
	e.signalStop(ctx, flowKey, workflow.StatusInterrupted)
	select {
	case <-reparked:
	case r := <-done:
		assert.True(false, "a stale stop returned Await early with %v (outcome %+v)", r.err, r.out)
		return
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await never parked again after the stale stop")
		return
	}

	rel()
	select {
	case r := <-done:
		assert.NoError(r.err)
		if assert.NotNil(r.out) {
			assert.Equal(workflow.StatusCompleted, r.out.Status)
		}
	case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
		assert.True(false, "Await never returned after the flow actually stopped")
	}
}

// TestAwait_ReadAfterReleasePastTheDeadline pins that the read after a release maps an error caused by the
// caller's own deadline the way the first read does. That read takes a turn, so on a saturated shard a release
// landing just before the deadline can straddle it; returning the raw ctx error then breaks Poll's re-pollable
// answer and Await's 408. The caller is held between the release and the read while its ctx ends, which makes
// the straddle exact.
func TestAwait_ReadAfterReleasePastTheDeadline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	assert := testarossa.For(t)

	mux := hosts.NewMux()
	release := make(chan struct{})
	var once sync.Once
	rel := func() { once.Do(func() { close(release) }) }

	g := workflow.NewGraph("ReleaseDeadline")
	g.SetURL("Block", "releasedeadline.verify:0/block")
	g.AddTransition("Block", workflow.End)
	mux.HandleGraph("releasedeadline.verify:0/g", g)
	mux.HandleTask("releasedeadline.verify:0/block", func(ctx context.Context, f *workflow.Flow) error {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil
	})

	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(ctx)
	defer rel() // below the Shutdown defer, so it unwinds first and the drain never waits on the held task
	e.SetHost(mux)
	assert.NoError(e.Startup(t.Context()))

	flowKey, err := e.Create(ctx, "releasedeadline.verify:0/g", nil, nil)
	if !assert.NoError(err) {
		return
	}

	for _, poll := range []bool{true, false} {
		type result struct {
			out *workflow.FlowOutcome
			err error
		}
		done := make(chan result, 1)
		awaitCtx, cancel := context.WithCancel(ctx)
		released := engineseams.Join(engineseams.CheckpointAwaitReleased, flowKey)
		e.seams.Break(released)
		parked := e.seams.Waiter(engineseams.Join(engineseams.CheckpointAwaitParked, flowKey))
		go func() {
			var out *workflow.FlowOutcome
			var aerr error
			if poll {
				out, aerr = e.await(awaitCtx, flowKey)
			} else {
				out, aerr = e.awaitStopped(awaitCtx, flowKey)
			}
			done <- result{out, aerr}
		}()
		select {
		case <-parked:
		case <-time.After(30 * time.Second * enginetest.TimeoutScale()):
			assert.True(false, "Await never parked")
			cancel()
			return
		}

		// A real stop status while the flow runs: the caller is released, then held before its read.
		e.signalStop(ctx, flowKey, workflow.StatusInterrupted)
		assert.True(e.seams.WaitTimeout(ctx, released, 10*time.Second*enginetest.TimeoutScale()), "Await was never released")
		cancel() // the caller's deadline ends inside the window; the held checkpoint returns on it
		e.seams.Resume(released)

		select {
		case r := <-done:
			if poll {
				assert.NoError(r.err)
				if assert.NotNil(r.out) {
					assert.Equal(workflow.StatusRunning, r.out.Status)
				}
			} else {
				assert.Nil(r.out)
				assert.Equal(http.StatusRequestTimeout, errors.StatusCode(r.err))
			}
		case <-time.After(10 * time.Second * enginetest.TimeoutScale()):
			assert.True(false, "Await never returned after its ctx ended")
			return
		}
	}
}

// stateVal reads a field as an untyped value. It lives here rather than on State because Get already is
// this, with a type the caller chooses; a one-line untyped read is a test convenience, not API.
func stateVal(s workflow.State, name string) any {
	var v any
	_, _ = s.Get(name, &v)
	return v
}
