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
	"github.com/microbus-io/dwarf/internal/diagnostics"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/janitor"
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

// expiryEngine starts an engine whose flows expire after expiration, whose counters and logs a test reads. Its
// janitors are Manual: the expiration job runs only on a drain.
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
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	db := sh.DB()
	_, err = db.ExecContext(t.Context(), "UPDATE dwarf_flows SET created_at=DATE_ADD_MILLIS(NOW_UTC(), ?) WHERE root_flow_id=?",
		-ago.Milliseconds(), rootID)
	if err != nil {
		t.Fatal(err)
	}
}

// expireNow runs one expiration pass over shard 1 from the oldest flow, as the janitor's job does once a pass has
// swept the shard.
func expireNow(ctx context.Context, e *Engine) {
	e.expirePass(ctx, 1, 0)
}

// markDue backdates every deletion mark on shard 1 by a second, so the next drain finds them due: a mark comes due a
// millisecond after it is made, and a drain right behind it can finish first.
func markDue(t *testing.T, e *Engine) {
	t.Helper()
	sh, err := e.db.Shard(1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = sh.DB().ExecContext(t.Context(), "UPDATE dwarf_flows SET updated_at=DATE_ADD_MILLIS(updated_at, ?) WHERE delete_after_ms>0", -1000)
	if err != nil {
		t.Fatal(err)
	}
}

// gone reports whether fk reads as not found: deleted, live and archive alike.
func gone(t *testing.T, e *Engine, fk string) bool {
	t.Helper()
	_, err := e.Snapshot(t.Context(), fk)
	if err != nil && errors.StatusCode(err) != http.StatusNotFound {
		t.Fatal(err)
	}
	return err != nil
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
// younger than it is left alone; the counter counts the roots. Each root it ends is marked for deletion, so the
// janitor's deletion jobs delete the trees, children included, and archive none of them.
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
	sh, err := e.db.Shard(1)
	assert.Must(assert.NoError(err))
	db := sh.DB()
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

	expireNow(ctx, e)
	for _, fk := range []string{asking, napping, parent, child} {
		status, reason := statusOf(t, e, fk)
		assert.Equal(workflow.StatusTerminated, status, "%s", fk)
		assert.Equal(expirationReason(time.Hour), reason, "%s", fk)
	}
	status, _ := statusOf(t, e, young)
	assert.Equal(workflow.StatusInterrupted, status, "a younger flow is left alone")
	assert.Equal(int64(3), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}), "roots, not their children")

	assert.Equal(3, enginetest.CountRows(t, e, 1, "SELECT COUNT(*) FROM dwarf_flows WHERE delete_after_ms=1"),
		"each root marked for deletion with its termination, and only the roots")

	// Nothing more to do: a second pass ends nothing and counts nothing.
	next, progress := e.expirePass(ctx, 1, 0)
	assert.Zero(next, "swept")
	assert.False(progress)
	assert.Equal(int64(3), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}))

	markDue(t, e)
	e.reapDueFlows(ctx)
	for _, fk := range []string{asking, napping, parent, child} {
		assert.True(gone(t, e, fk), "%s deleted, not archived", fk)
	}
	status, _ = statusOf(t, e, young)
	assert.Equal(workflow.StatusInterrupted, status)
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

	expireNow(ctx, e)
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

	expireNow(ctx, e)
	status, _ := statusOf(t, e, parent)
	assert.Equal(workflow.StatusTerminated, status)
	status, reason := statusOf(t, e, ended)
	assert.Equal("ended before the pass", reason, "a closed flow keeps its outcome")
	assert.Equal(workflow.StatusTerminated, status)
	status, _ = statusOf(t, e, last)
	assert.Equal(workflow.StatusTerminated, status, "reached past a page of a child and a page of a closed flow")
}

// TestExpiration_ThePassKeepsToItsBudget pins the time budget: a pass past it starts no further termination, after
// the first, says so once and reports progress, so its job runs the next at once; the next pass goes on.
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

	next, progress := e.expirePass(ctx, 1, 0)
	assert.True(progress, "stopped at its budget")
	status, _ := statusOf(t, e, first)
	assert.Equal(workflow.StatusTerminated, status, "a pass always ends one")
	status, _ = statusOf(t, e, second)
	assert.Equal(workflow.StatusInterrupted, status, "past the budget, the rest waits")
	assert.Equal(1, logs.count("Expiring flows: the pass reached its time budget"))

	e.expirePass(ctx, 1, next)
	status, _ = statusOf(t, e, second)
	assert.Equal(workflow.StatusTerminated, status)
	assert.Equal(int64(2), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}))
}

// TestExpiration_OffByDefault pins that an engine never told otherwise has no expiration job and expires nothing,
// however old, that a reader of a shard expires nothing there, that a dispatcher with the expiration on does, and
// that turning it off removes the job.
func TestExpiration_OffByDefault(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, _, _ := expiryEngine(t, 0)
	old := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, old, 1000*time.Hour)
	_, ok := e.janitorOn(1).JobAges(time.Now())[janitor.JobExpiration]
	assert.False(ok, "no expiration job")
	e.reapDueFlows(ctx)
	status, _ := statusOf(t, e, old)
	assert.Equal(workflow.StatusInterrupted, status)

	e.flowExpiration = time.Hour
	setRole(e, 1, false)
	e.reapDueFlows(ctx)
	setRole(e, 1, true)
	status, _ = statusOf(t, e, old)
	assert.Equal(workflow.StatusInterrupted, status, "a reader leaves the shard to its dispatchers")
	e.reapDueFlows(ctx) // the dispatcher's job expires and marks it; its deletion may wait for the mark to come due
	markDue(t, e)
	e.reapDueFlows(ctx)
	assert.True(gone(t, e, old), "the dispatcher's job expires it, and the deletion jobs delete it")
	_, ok = e.janitorOn(1).JobAges(time.Now())[janitor.JobExpiration]
	assert.True(ok, "the expiration job")

	e.flowExpiration = 0
	e.reapDueFlows(ctx)
	_, ok = e.janitorOn(1).JobAges(time.Now())[janitor.JobExpiration]
	assert.False(ok, "turned off, the job is removed")
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
	ended, err := e.terminateSubtree(ctx, shard, id, hour, token, "first", "", false)
	assert.NoError(err)
	assert.True(ended)
	ended, err = e.terminateSubtree(ctx, shard, id, hour, token, "again", "", false)
	assert.NoError(err)
	assert.False(ended)
}

// TestExpiration_AFailingRootIsPassedOver pins that a root whose termination fails does not stop the pass, that the
// next sweep retries it, and that the budget counts a failed attempt, so a run of failing roots cannot outlast it.
func TestExpiration_AFailingRootIsPassedOver(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, reader, _ := expiryEngine(t, time.Hour)
	first := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	second := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	third := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	for _, fk := range []string{first, second, third} {
		age(t, e, fk, 2*time.Hour)
	}

	// Past the budget at once: the failed attempt is the pass's one.
	e.expireBudget = time.Nanosecond
	e.diags.Seams().InjectN(diagnostics.FaultExpireTerminate, 1)
	next, _ := e.expirePass(ctx, 1, 0)
	status, _ := statusOf(t, e, second)
	assert.Equal(workflow.StatusInterrupted, status, "a failed attempt counts against the budget")

	// The next pass goes on after the failed root and sweeps the rest; the failed root waits for the next sweep.
	e.expireBudget = time.Minute
	e.diags.Seams().InjectN(diagnostics.FaultExpireTerminate, 1)
	next, _ = e.expirePass(ctx, 1, next)
	assert.Zero(next, "swept")
	status, _ = statusOf(t, e, first)
	assert.Equal(workflow.StatusInterrupted, status, "its termination failed, and a pass goes on from where the last stopped")
	status, _ = statusOf(t, e, second)
	assert.Equal(workflow.StatusInterrupted, status, "its termination failed too")
	status, _ = statusOf(t, e, third)
	assert.Equal(workflow.StatusTerminated, status, "the next root is reached all the same")
	assert.Equal(int64(1), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}))
	assert.Equal(int64(2), sumAt(collect(t, reader), "dwarf_flows_expire_failed", map[string]string{"shard": "1"}),
		"one failed attempt per pass")
	assert.Equal(2, e.diags.Seams().Visits(diagnostics.CheckpointJanitorFailed), "each reported as its job's failure")

	expireNow(ctx, e)
	for _, fk := range []string{first, second} {
		status, _ = statusOf(t, e, fk)
		assert.Equal(workflow.StatusTerminated, status, "the next sweep retries it")
	}
	assert.Equal(int64(3), sumAt(collect(t, reader), "dwarf_flows_expired", map[string]string{"shard": "1"}))
	assert.Equal(int64(2), sumAt(collect(t, reader), "dwarf_flows_expire_failed", map[string]string{"shard": "1"}))
}

// TestExpiration_AClosedPrefixDoesNotHoldBackAnExpiredRoot pins the budget on a walk that terminates nothing and the
// position passed from one pass to the next: closed flows waiting for archiving, more than one pass reads within its
// budget, stop each pass at the budget, and the passes after it go on from where it stopped until they reach the
// expired root behind them. Walking from the oldest each time, no pass would ever reach it.
func TestExpiration_AClosedPrefixDoesNotHoldBackAnExpiredRoot(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := t.Context()
	e, _, logs := expiryEngine(t, time.Hour)
	e.expirePage = 1
	e.expireBudget = time.Nanosecond // one page per pass
	const closed = 4
	for range closed {
		fk := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
		assert.NoError(e.Terminate(ctx, fk, "ended before the pass"))
		age(t, e, fk, 2*time.Hour)
	}
	open := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, open, 2*time.Hour)

	next, progress := e.expirePass(ctx, 1, 0)
	assert.True(progress, "stopped at its budget")
	status, _ := statusOf(t, e, open)
	assert.Equal(workflow.StatusInterrupted, status, "a pass that terminates nothing keeps to its budget")
	assert.Equal(1, logs.count("Expiring flows: the pass reached its time budget"))

	for range closed - 1 {
		next, _ = e.expirePass(ctx, 1, next)
	}
	status, _ = statusOf(t, e, open)
	assert.Equal(workflow.StatusInterrupted, status, "one page of closed flows per pass")
	next, _ = e.expirePass(ctx, 1, next)
	status, reason := statusOf(t, e, open)
	assert.Equal(workflow.StatusTerminated, status, "reached by going on from where the last pass stopped")
	assert.Equal(expirationReason(time.Hour), reason)

	// The next pass finds nothing after it, so the one after starts from the oldest again.
	assert.NotEqual(0, next)
	next, _ = e.expirePass(ctx, 1, next)
	assert.Zero(next, "swept")
}

// TestExpiration_TheJobPassesAsItsJanitorResumes pins the wiring: the janitor's expiration job runs a pass as its
// janitor resumes, not an idle wait in - half an hour at this expiration - so a replica that lives shorter than the
// wait, or takes the shard up again, still expires flows.
func TestExpiration_TheJobPassesAsItsJanitorResumes(t *testing.T) {
	t.Parallel()
	e := NewEngineUnderTest(t.Name())
	t.Cleanup(func() { e.Shutdown(context.Background()) })
	e.SetHost(expiryMux())
	if err := e.SetFlowExpiration(time.Hour); err != nil {
		t.Fatal(err)
	}
	e.janitorManual = false
	if err := e.Startup(t.Context()); err != nil {
		t.Fatal(err)
	}
	old := startOpen(t, e, "exp/ask", workflow.StatusInterrupted)
	age(t, e, old, 2*time.Hour)
	setRole(e, 1, false)
	setRole(e, 1, true)
	deadline := time.Now().Add(10 * time.Second * enginetest.TimeoutScale())
	for {
		outcome, err := e.Snapshot(t.Context(), old)
		if errors.StatusCode(err) == http.StatusNotFound || (err == nil && outcome.Status == workflow.StatusTerminated) {
			return // terminated, and perhaps deleted already
		}
		if time.Now().After(deadline) {
			t.Fatal("the resumed job did not expire the flow")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
