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
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/engineseams"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// expiryMux serves the flows the expiration tests leave open: one interrupted, one running (asleep for an hour),
// and a root parked on an interrupted subgraph child.
func expiryMux() *hosts.Mux {
	mux := hosts.NewMux()
	ask := workflow.NewGraph("Ask")
	ask.SetURL("Ask", "exp/ask")
	ask.AddTransition("Ask", workflow.End)
	mux.HandleGraph("exp/ask", ask)
	mux.HandleTask("exp/ask", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Interrupt(map[string]any{"q": "ok?"}, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	nap := workflow.NewGraph("Nap")
	nap.SetURL("Nap", "exp/nap")
	nap.SetURL("Wake", "exp/wake")
	nap.AddTransition("Nap", "Wake")
	nap.AddTransition("Wake", workflow.End)
	mux.HandleGraph("exp/nap", nap)
	mux.HandleTask("exp/nap", func(ctx context.Context, f *workflow.Flow) error {
		f.Sleep(time.Hour)
		return nil
	})
	mux.HandleTask("exp/wake", func(ctx context.Context, f *workflow.Flow) error { return nil })
	parent := workflow.NewGraph("AskParent")
	parent.SetURL("Call", "exp/call")
	parent.AddTransition("Call", workflow.End)
	mux.HandleGraph("exp/parent", parent)
	mux.HandleTask("exp/call", func(ctx context.Context, f *workflow.Flow) error {
		yield, err := f.Subgraph("exp/ask", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	return mux
}

// expiryEngine starts an engine whose flows expire after an hour, whose own reaper stays out of the way, and whose
// counters and logs a test reads.
func expiryEngine(t *testing.T, expiration time.Duration) (*Engine, *sdkmetric.ManualReader, *logCounter) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	logs := &logCounter{seen: map[string]int{}}
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(expiryMux())
	e.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)))
	if err := e.SetLogger(slog.New(logs)); err != nil {
		t.Fatal(err)
	}
	if err := e.SetFlowExpiration(expiration); err != nil {
		t.Fatal(err)
	}
	e.reapInterval = time.Hour
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	return e, reader, logs
}

// startOpen creates a flow of workflowURL and waits for it to rest in status: interrupted, or running asleep -
// its first step done and the next one waiting out the sleep.
func startOpen(t *testing.T, e *Engine, workflowURL, status string) string {
	t.Helper()
	fk, err := e.Create(t.Context(), workflowURL, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if status != workflow.StatusRunning {
		enginetest.AwaitFlowStatus(t, e, fk, status, time.Minute)
		return fk
	}
	deadline := time.Now().Add(time.Minute * enginetest.TimeoutScale())
	for {
		hist, err := e.History(t.Context(), fk)
		if err != nil {
			t.Fatal(err)
		}
		if len(hist) >= 2 {
			return fk
		}
		if time.Now().After(deadline) {
			t.Fatalf("flow %s never went to sleep", fk)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// age moves the creation of a flow's whole tree ago into the past.
func age(t *testing.T, e *Engine, fk string, ago time.Duration) {
	t.Helper()
	_, _, rootID, _, err := keys.ParseFlowKey(fk)
	if err != nil {
		t.Fatal(err)
	}
	db, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET created_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE root_flow_id=?",
		-ago.Milliseconds(), rootID)
	if err != nil {
		t.Fatal(err)
	}
}

// statusOf returns a flow's status, and its terminate reason.
func statusOf(t *testing.T, e *Engine, fk string) (status, reason string) {
	t.Helper()
	outcome, err := e.Snapshot(t.Context(), fk)
	if err != nil {
		t.Fatal(err)
	}
	return outcome.Status, outcome.TerminateReason
}

// TestExpiration_TerminatesTheOldOpenRoots pins the pass: a running root and an interrupted one older than the
// expiration are terminated with the expiration's reason, a root parked on a subgraph with its child, and a root
// younger than it is left alone; the counter counts the roots.
func TestExpiration_TerminatesTheOldOpenRoots(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader, _ := expiryEngine(t, time.Hour)

	asking := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	napping := startOpen(t, e, "exp/nap", workflow.StatusRunning)
	parent := startOpen(t, e, "exp/parent", workflow.StatusInterrupted)
	young := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	for _, fk := range []string{asking, napping, parent} {
		age(t, e, fk, 2*time.Hour)
	}
	hist, err := e.History(ctx, parent)
	assert.NoError(err)
	child := ""
	for _, s := range hist {
		if s.SubgraphKey != "" {
			child = s.SubgraphKey
		}
	}
	assert.NotEqual("", child, "the parent called its child")

	// The walk keeps the open roots only: not the child, not the younger flow.
	db, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	roots, _, reachedYoung, err := e.expiredPage(ctx, db, 0)
	assert.NoError(err)
	assert.True(reachedYoung, "it reached the younger flow")
	var ids []int
	for _, r := range roots {
		ids = append(ids, r.flowID)
	}
	var want []int
	for _, fk := range []string{asking, napping, parent} {
		_, _, id, _, _ := keys.ParseFlowKey(fk)
		want = append(want, id)
	}
	assert.Equal(want, ids)

	e.expireFlows(ctx)
	for _, fk := range []string{asking, napping, parent, child} {
		status, reason := statusOf(t, e, fk)
		assert.Equal(workflow.StatusTerminated, status, "%s", fk)
		assert.Equal(expirationReason(time.Hour), reason, "%s", fk)
	}
	status, _ := statusOf(t, e, young)
	assert.Equal(workflow.StatusInterrupted, status, "a younger flow is left alone")
	assert.Equal(int64(3), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}), "roots, not their children")

	// Nothing more to do: a second pass ends nothing and counts nothing.
	e.expireFlows(ctx)
	assert.Equal(int64(3), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}))
}

// TestExpiration_TheWalkStopsAtTheFirstYoungFlow pins the walk's stop: flows are visited in creation order, so a flow
// past the first that has not expired is not reached, even one whose creation reads as old.
func TestExpiration_TheWalkStopsAtTheFirstYoungFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, _, _ := expiryEngine(t, time.Hour)
	old := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	young := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	beyond := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, old, 2*time.Hour)
	age(t, e, beyond, 2*time.Hour)

	e.expireFlows(ctx)
	status, _ := statusOf(t, e, old)
	assert.Equal(workflow.StatusTerminated, status)
	status, _ = statusOf(t, e, young)
	assert.Equal(workflow.StatusInterrupted, status)
	status, _ = statusOf(t, e, beyond)
	assert.Equal(workflow.StatusInterrupted, status, "past the first young flow, the walk reads nothing")
}

// TestExpiration_PagesPastWhatItDoesNotEnd pins the paging: a page that holds only flows the pass does not end - an
// closed flow, a subgraph child - leads to the next page rather than ending the walk.
func TestExpiration_PagesPastWhatItDoesNotEnd(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, _, _ := expiryEngine(t, time.Hour)
	e.expirePage = 1
	parent := startOpen(t, e, "exp/parent", workflow.StatusInterrupted) // the root, then its child
	ended := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	assert.NoError(e.Terminate(ctx, ended, "ended before the pass"))
	last := startOpen(t, e, "exp/nap", workflow.StatusRunning)
	for _, fk := range []string{parent, ended, last} {
		age(t, e, fk, 2*time.Hour)
	}

	e.expireFlows(ctx)
	status, _ := statusOf(t, e, parent)
	assert.Equal(workflow.StatusTerminated, status)
	status, reason := statusOf(t, e, ended)
	assert.Equal("ended before the pass", reason, "a closed flow keeps its outcome")
	assert.Equal(workflow.StatusTerminated, status)
	status, _ = statusOf(t, e, last)
	assert.Equal(workflow.StatusTerminated, status, "reached past a page of a child and a page of a closed flow")
}

// TestExpiration_ThePassKeepsToItsBudget pins the time budget: a pass past it starts no further termination, after
// the first, and says so once; the next pass goes on.
func TestExpiration_ThePassKeepsToItsBudget(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader, logs := expiryEngine(t, time.Hour)
	e.expireBudget = time.Nanosecond
	first := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	second := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, first, 2*time.Hour)
	age(t, e, second, 2*time.Hour)

	e.expireFlows(ctx)
	status, _ := statusOf(t, e, first)
	assert.Equal(workflow.StatusTerminated, status, "a pass always ends one")
	status, _ = statusOf(t, e, second)
	assert.Equal(workflow.StatusInterrupted, status, "past the budget, the rest waits")
	assert.Equal(1, logs.count("Expiring flows: the pass reached its time budget"))

	e.expireFlows(ctx)
	status, _ = statusOf(t, e, second)
	assert.Equal(workflow.StatusTerminated, status)
	assert.Equal(int64(2), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}))
}

// TestExpiration_OffByDefault pins that an engine never told otherwise expires nothing, however old, and that a
// reader of a shard expires nothing there.
func TestExpiration_OffByDefault(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, _, _ := expiryEngine(t, 0)
	old := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, old, 1000*time.Hour)
	e.expireFlows(ctx)
	status, _ := statusOf(t, e, old)
	assert.Equal(workflow.StatusInterrupted, status)

	e.flowExpiration = time.Hour
	e.dispatchFlag(1).Store(false)
	e.expireFlows(ctx)
	e.dispatchFlag(1).Store(true)
	status, _ = statusOf(t, e, old)
	assert.Equal(workflow.StatusInterrupted, status, "a reader leaves the shard to its dispatchers")
}

// TestExpiration_SetterRules pins the setter: negative is a 400, 0 and any positive duration are accepted, and it
// cannot change once the engine runs.
func TestExpiration_SetterRules(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	defer e.Shutdown(t.Context())
	e.SetHost(hosts.NewMux())
	assert.Equal(http.StatusBadRequest, errors.StatusCode(e.SetFlowExpiration(-time.Second)))
	assert.NoError(e.SetFlowExpiration(0))
	assert.NoError(e.SetFlowExpiration(time.Millisecond))
	assert.Must(assert.NoError(e.Startup(t.Context())))
	err := e.SetFlowExpiration(time.Hour)
	assert.Equal(http.StatusConflict, errors.StatusCode(err))
	assert.True(err != nil && strings.Contains(err.Error(), "flow expiration"))
}

// TestExpiration_TerminateSubtreeReportsWhatItClosed pins what the pass counts by: terminateSubtree reports ending a
// flow that was open, and not one that had closed already.
func TestExpiration_TerminateSubtreeReportsWhatItClosed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, _, _ := expiryEngine(t, 0)
	fk := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	shard, hour, id, token, err := keys.ParseFlowKey(fk)
	assert.NoError(err)
	ended, err := e.terminateSubtree(ctx, shard, id, hour, token, "first", "")
	assert.NoError(err)
	assert.True(ended)
	ended, err = e.terminateSubtree(ctx, shard, id, hour, token, "again", "")
	assert.NoError(err)
	assert.False(ended)
}

// TestExpiration_AFailingRootIsPassedOver pins that a root whose termination fails does not stop the pass - every pass
// walks from the oldest, so it would stop expiration for good - and that the budget counts it as an attempt, so a run
// of failing roots cannot outlast it.
func TestExpiration_AFailingRootIsPassedOver(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader, _ := expiryEngine(t, time.Hour)
	first := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	second := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, first, 2*time.Hour)
	age(t, e, second, 2*time.Hour)

	// Past the budget at once: the failed attempt is the pass's one.
	e.expireBudget = time.Nanosecond
	e.seams.InjectN(engineseams.FaultExpireTerminate, 1)
	e.expireFlows(ctx)
	status, _ := statusOf(t, e, second)
	assert.Equal(workflow.StatusInterrupted, status, "a failed attempt counts against the budget")

	e.expireBudget = time.Minute
	e.seams.InjectN(engineseams.FaultExpireTerminate, 1)
	e.expireFlows(ctx)
	status, _ = statusOf(t, e, first)
	assert.Equal(workflow.StatusInterrupted, status, "its termination failed")
	status, _ = statusOf(t, e, second)
	assert.Equal(workflow.StatusTerminated, status, "the next root is reached all the same")
	assert.Equal(int64(1), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}))
	assert.Equal(int64(2), sumAt(collect(t, reader), "dwarf_flows_expire_failed", map[string]string{"shard": "1"}),
		"one failed attempt per pass")

	e.expireFlows(ctx)
	status, _ = statusOf(t, e, first)
	assert.Equal(workflow.StatusTerminated, status, "the next pass retries it")
	assert.Equal(int64(2), sumAt(collect(t, reader), "dwarf_flows_expire_failed", map[string]string{"shard": "1"}))
}

// TestExpiration_TheReaperRunsThePassAtOnce pins the wiring: the reaper's goroutine runs the pass on its first tick,
// not an interval in, so a replica that lives shorter than the interval still expires flows. The flow is made old
// before that first tick, which comes a few seconds after Startup.
func TestExpiration_TheReaperRunsThePassAtOnce(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(expiryMux())
	assert.NoError(e.SetFlowExpiration(time.Hour))
	tick := 3 * time.Second * enginetest.TimeoutScale()
	e.reapInterval = tick
	e.expireInterval = time.Hour
	started := time.Now()
	assert.Must(assert.NoError(e.Startup(t.Context())))
	old := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, old, 2*time.Hour)
	if time.Since(started) >= tick {
		t.Skip("the setup outlasted the reaper's first tick")
	}
	deadline := started.Add(tick + 10*time.Second*enginetest.TimeoutScale())
	for {
		if status, _ := statusOf(t, e, old); status == workflow.StatusTerminated {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the reaper's first tick did not expire the flow")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
