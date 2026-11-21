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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/workflow"
	dwarferrors "github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// lockedBuffer is a log sink many goroutines write to.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// keySet collects every flow and step key a test meets.
type keySet struct {
	mu   sync.Mutex
	keys map[string]bool
}

func (s *keySet) add(key ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = map[string]bool{}
	}
	for _, k := range key {
		if k != "" {
			s.keys[k] = true
		}
	}
}

func (s *keySet) addHistory(steps []workflow.FlowStep) {
	for _, st := range steps {
		s.add(st.StepKey, st.PredecessorKey, st.SuccessorKey, st.PrevKey, st.NextKey, st.SubgraphKey)
		s.addHistory(st.SubHistory)
	}
}

// tokens returns each collected key's token, the segment that makes the key a capability.
func (s *keySet) tokens(t *testing.T) map[string]string {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	tokens := map[string]string{}
	for k := range s.keys {
		if _, _, _, _, token, err := keys.ParseStepKey(k); err == nil {
			tokens[token] = k
			continue
		}
		_, _, _, token, err := keys.ParseFlowKey(k)
		if err != nil {
			t.Fatalf("%q is neither a flow key nor a step key", k)
		}
		tokens[token] = k
	}
	return tokens
}

// TestTelemetry_CarriesNoKeyToken runs the engine's main paths with every telemetry sink capturing all it is
// given - a debug logger, a recording tracer and a manual metric reader - and asserts that no flow or step key's
// token reaches any of them. A key is a bearer capability; telemetry names a flow by its correlation id.
func TestTelemetry_CarriesNoKeyToken(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()
	timeout := 60 * time.Second * enginetest.TimeoutScale()

	var seen keySet
	record := func(f *workflow.Flow) { seen.add(f.FlowKey(), f.StepKey()) }
	holdStarted := make(chan struct{}, 2)
	holdRelease := make(chan struct{})
	releaseHold := sync.OnceFunc(func() { close(holdRelease) })
	var zombieCalls atomic.Int64
	zombieStarted := make(chan struct{}, 1)
	zombieRelease := make(chan struct{})

	mux := hosts.NewMux()

	// A fan-out whose branches retry once, a subgraph child, and an interrupt.
	main := workflow.NewGraph("TK-Main")
	main.SetURL("Start", "tk/start")
	main.SetURL("Branch", "tk/branch")
	main.SetURL("Join", "tk/join")
	main.SetURL("Call", "tk/call")
	main.SetURL("Ask", "tk/ask")
	main.AddTransitionForEach("Start", "Branch", "items", "item")
	main.AddTransition("Branch", "Join")
	main.SetFanIn("Join")
	main.AddTransition("Join", "Call")
	main.AddTransition("Call", "Ask")
	main.AddTransition("Ask", workflow.End)
	assert.NoError(main.Validate())
	mux.HandleGraph("tk/main", main)
	child := workflow.NewGraph("TK-Child")
	child.SetURL("Leaf", "tk/leaf")
	child.AddTransition("Leaf", workflow.End)
	assert.NoError(child.Validate())
	mux.HandleGraph("tk/child", child)
	mux.HandleTask("tk/start", func(ctx context.Context, f *workflow.Flow) error {
		record(f)
		f.Set("items", []int{1, 2})
		return nil
	})
	mux.HandleTask("tk/branch", func(ctx context.Context, f *workflow.Flow) error {
		record(f)
		if f.Attempt() == 0 {
			if yield, err := f.Retry(time.Millisecond, 1, time.Millisecond, time.Minute); yield || err != nil {
				return err
			}
		}
		return nil
	})
	mux.HandleTask("tk/join", func(ctx context.Context, f *workflow.Flow) error { record(f); return nil })
	mux.HandleTask("tk/call", func(ctx context.Context, f *workflow.Flow) error {
		record(f)
		yield, err := f.Subgraph("tk/child", nil, nil)
		if yield || err != nil {
			return err
		}
		return nil
	})
	mux.HandleTask("tk/leaf", func(ctx context.Context, f *workflow.Flow) error { record(f); return nil })
	mux.HandleTask("tk/ask", func(ctx context.Context, f *workflow.Flow) error {
		record(f)
		var answer map[string]any
		yield, err := f.Interrupt(map[string]any{"question": "proceed?"}, &answer)
		if yield || err != nil {
			return err
		}
		return nil
	})

	// A failing task that a Fork's override fixes, and an error routed to a handler.
	boom := workflow.NewGraph("TK-Boom")
	boom.SetURL("Boom", "tk/boom")
	boom.AddTransition("Boom", workflow.End)
	assert.NoError(boom.Validate())
	mux.HandleGraph("tk/boom", boom)
	mux.HandleTask("tk/boom", func(ctx context.Context, f *workflow.Flow) error {
		record(f)
		if !f.GetBool("fixed") {
			return errors.New("boom")
		}
		return nil
	})
	handled := workflow.NewGraph("TK-Handled")
	handled.SetURL("Risky", "tk/risky")
	handled.SetURL("Handler", "tk/handler")
	handled.AddTransition("Risky", workflow.End)
	handled.AddTransitionOnError("Risky", "Handler")
	handled.AddTransition("Handler", workflow.End)
	assert.NoError(handled.Validate())
	mux.HandleGraph("tk/handled", handled)
	mux.HandleTask("tk/risky", func(ctx context.Context, f *workflow.Flow) error { record(f); return errors.New("risky") })
	mux.HandleTask("tk/handler", func(ctx context.Context, f *workflow.Flow) error { record(f); return nil })

	// A task held while a Cancel or a Terminate lands, and one whose lease lapses while it runs.
	hold := workflow.NewGraph("TK-Hold")
	hold.SetURL("Hold", "tk/hold")
	hold.AddTransition("Hold", workflow.End)
	assert.NoError(hold.Validate())
	mux.HandleGraph("tk/hold", hold)
	mux.HandleTask("tk/hold", func(ctx context.Context, f *workflow.Flow) error {
		record(f)
		holdStarted <- struct{}{}
		<-holdRelease
		return nil
	})
	zombie := workflow.NewGraph("TK-Zombie")
	zombie.SetURL("Slow", "tk/slow")
	zombie.AddTransition("Slow", workflow.End)
	assert.NoError(zombie.Validate())
	mux.HandleGraph("tk/zombie", zombie)
	mux.HandleTask("tk/slow", func(ctx context.Context, f *workflow.Flow) error {
		record(f)
		if zombieCalls.Add(1) == 1 {
			zombieStarted <- struct{}{}
			<-zombieRelease
		}
		return nil
	})

	var logs lockedBuffer
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	eng := NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.setClaimsTrackerEnabled(false)
	defer releaseHold() // below the Shutdown defer, so it runs first
	assert.NoError(eng.SetHost(mux))
	assert.NoError(eng.SetWorkers(6))
	assert.NoError(eng.SetLogger(slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))))
	assert.NoError(eng.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))))
	assert.NoError(eng.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))))
	eng.archiveGrace = 0
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	opCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	await := func(flowKey, status string) {
		t.Helper()
		outcome, err := eng.Await(opCtx, flowKey)
		if assert.NoError(err) {
			assert.Equal(status, outcome.Status, "%s", flowKey)
		}
	}

	// The main flow, to its interrupt and on through a Resume.
	mainKey, err := eng.Create(opCtx, "tk/main", nil, nil)
	assert.Must(assert.NoError(err))
	seen.add(mainKey)
	await(mainKey, workflow.StatusInterrupted)
	_, err = eng.Snapshot(opCtx, mainKey)
	assert.NoError(err)
	assert.NoError(eng.Resume(opCtx, mainKey, map[string]any{"yes": true}))
	await(mainKey, workflow.StatusCompleted)

	// A failure, a Fork that fixes it, a Continue of the fork, and an error route.
	boomKey, outcome, err := eng.Run(opCtx, "tk/boom", nil, nil)
	assert.Must(assert.NoError(err))
	seen.add(boomKey)
	assert.Equal(workflow.StatusFailed, outcome.Status)
	hist, err := eng.History(opCtx, boomKey)
	assert.Must(assert.NoError(err))
	assert.Must(assert.Len(hist, 1))
	forkKey, err := eng.Fork(opCtx, hist[0].StepKey, map[string]any{"fixed": true})
	assert.Must(assert.NoError(err))
	seen.add(forkKey)
	await(forkKey, workflow.StatusCompleted)
	nextKey, err := eng.Continue(opCtx, forkKey, map[string]any{"again": true})
	assert.Must(assert.NoError(err))
	seen.add(nextKey)
	await(nextKey, workflow.StatusCompleted)
	handledKey, _, err := eng.Run(opCtx, "tk/handled", nil, nil)
	assert.Must(assert.NoError(err))
	seen.add(handledKey)

	// A Cancel and a Terminate landing on held tasks.
	cancelKey, err := eng.Create(opCtx, "tk/hold", nil, nil)
	assert.Must(assert.NoError(err))
	terminateKey, err := eng.Create(opCtx, "tk/hold", nil, nil)
	assert.Must(assert.NoError(err))
	seen.add(cancelKey, terminateKey)
	for range 2 {
		select {
		case <-holdStarted:
		case <-opCtx.Done():
			t.Fatal("the held tasks never started")
		}
	}
	assert.NoError(eng.Cancel(opCtx, cancelKey, "stop"))
	assert.NoError(eng.Terminate(opCtx, terminateKey, "stop"))
	releaseHold()
	await(cancelKey, workflow.StatusCancelled)
	await(terminateKey, workflow.StatusTerminated)

	// A lease lapsing under a running task, recovered and run by a peer.
	zombieKey, zombieOutcome, unblock := zombieDispatch(t, eng, "tk/zombie", "Slow", &zombieCalls, zombieStarted, zombieRelease)
	defer unblock() // below the Shutdown defer, so it runs first
	seen.add(zombieKey)
	if assert.NotNil(zombieOutcome) {
		assert.Equal(workflow.StatusCompleted, zombieOutcome.Status)
	}
	unblock()
	awaitDispatchesReturned(t, eng, zombieKey, "Slow", 2)

	// Every read, live and then archived, and a read of a deleted flow.
	allKeys := []string{mainKey, boomKey, forkKey, nextKey, handledKey, cancelKey, terminateKey, zombieKey}
	readAll := func() {
		t.Helper()
		for _, k := range allKeys {
			hist, err := eng.History(opCtx, k)
			if !assert.NoError(err, "%s", k) {
				continue
			}
			seen.addHistory(hist)
			_, err = eng.Snapshot(opCtx, k)
			assert.NoError(err)
			_, err = eng.Summary(opCtx, k)
			assert.NoError(err)
			for _, st := range hist {
				_, err = eng.Step(opCtx, st.StepKey)
				assert.NoError(err)
			}
		}
	}
	window := workflow.Query{FromTime: time.Now().Add(-time.Hour), ToTime: time.Now().Add(time.Hour), Limit: 100, IncludeSubgraphs: true}
	readAll()
	_, err = eng.ListOpen(opCtx, window)
	assert.NoError(err)
	_, err = eng.ListRecentlyClosed(opCtx, window)
	assert.NoError(err)
	eng.drainJanitors(opCtx, 0)
	readAll()
	_, err = eng.ListClosed(opCtx, window)
	assert.NoError(err)
	assert.NoError(eng.Delete(opCtx, handledKey))
	_, err = eng.Snapshot(opCtx, handledKey)
	assert.Equal(http.StatusNotFound, dwarferrors.StatusCode(err))
	_, _, err = eng.Purge(opCtx, workflow.Query{WorkflowURL: "tk/boom", FromTime: window.FromTime, ToTime: window.ToTime, Limit: 100})
	assert.NoError(err)

	var metrics metricdata.ResourceMetrics
	assert.NoError(reader.Collect(ctx, &metrics))
	assert.NoError(eng.Shutdown(ctx)) // every span ended and every log line written

	sinks := map[string]string{"logs": logs.String()}
	metricsJSON, err := json.Marshal(metrics)
	assert.NoError(err)
	sinks["metrics"] = string(metricsJSON)
	var spanText strings.Builder
	for _, s := range spans.Ended() {
		fmt.Fprintf(&spanText, "%s %v %s %s\n", s.Name(), s.Attributes(), s.Status().Description, s.SpanContext().TraceID())
		for _, ev := range s.Events() {
			fmt.Fprintf(&spanText, "  %s %v\n", ev.Name, ev.Attributes)
		}
		for _, l := range s.Links() {
			fmt.Fprintf(&spanText, "  link %v\n", l.Attributes)
		}
	}
	sinks["spans"] = spanText.String()

	// Each sink captured the flows, so an absent token is evidence, not an empty sink.
	shard, hour, flowID, _, err := keys.ParseFlowKey(mainKey)
	assert.Must(assert.NoError(err))
	corr := keys.CorrelationID(shard, hour, flowID)
	assert.Contains(sinks["logs"], corr)
	assert.Contains(sinks["spans"], corr)
	assert.Contains(sinks["metrics"], `"tk/main"`) // the workflow label

	tokens := seen.tokens(t)
	assert.True(len(tokens) >= 20, "collected %d keys", len(tokens))
	for token, key := range tokens {
		for name, text := range sinks {
			assert.False(strings.Contains(text, token), "the %s carry the token of %s", name, key)
		}
	}
}
