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

package hosts_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

var _ engine.Host = (*hosts.Remote)(nil)

// serve is the tests' task server, over the graphs and tasks registered on mux: a GET of a workflow URL answers
// with the graph's JSON, and a POST of a task URL runs the task on the decoded carrier and answers with its
// result, or with its error as JSON, so the error's properties travel. A request's URL is http, its host and its
// path.
func serve(mux *hosts.Mux) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u := "http://" + r.Host + r.URL.Path
		ctx := r.Context()
		if b := r.Header.Get(hosts.HeaderBaggage); b != "" {
			var baggage workflow.State
			if err := json.Unmarshal([]byte(b), &baggage); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			ctx = workflow.ContextWithBaggage(ctx, baggage)
		}
		switch r.Method {
		case http.MethodGet:
			graph, err := mux.LoadGraph(ctx, u)
			if err != nil {
				http.Error(w, err.Error(), errors.StatusCode(err))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(graph)
		case http.MethodPost:
			var flow workflow.Flow
			if err := json.NewDecoder(r.Body).Decode(workflow.RequestCarrier(&flow)); err != nil {
				http.Error(w, err.Error(), errors.StatusCode(err))
				return
			}
			if err := mux.ExecuteTask(ctx, u, &flow); err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(errors.StatusCode(err))
				_ = json.NewEncoder(w).Encode(errors.Convert(err))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(workflow.ResultCarrier(&flow))
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// gatedClient is a client that refuses every dial to addr while up is false, as a server that is down would,
// calling refused at each refusal. Every request dials, so a server that goes down refuses the next one.
func gatedClient(addr string, up *atomic.Bool, refused func()) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, network, a string) (net.Conn, error) {
			if a == addr && !up.Load() {
				if refused != nil {
					refused()
				}
				return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
			}
			return (&net.Dialer{}).DialContext(ctx, network, a)
		},
	}}
}

func TestRemote_RunsAFlowOverHTTP(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	var mu sync.Mutex
	var keys, attempts []string
	var budgets []int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/greet" {
			budget, _ := strconv.Atoi(r.Header.Get(hosts.HeaderBudgetMillis))
			mu.Lock()
			keys = append(keys, r.Header.Get(hosts.HeaderIdempotencyKey))
			attempts = append(attempts, r.Header.Get(hosts.HeaderAttempt))
			budgets = append(budgets, budget)
			mu.Unlock()
		}
		serve(mux)(w, r)
	}))
	defer srv.Close()

	graph := workflow.NewGraph("Remote")
	graph.SetURL("Greet", srv.URL+"/greet")
	graph.SetURL("Shout", srv.URL+"/shout")
	graph.AddTransitionChain("Greet", "Shout", workflow.End)
	mux.HandleGraph(srv.URL+"/graph", graph)
	mux.HandleTask(srv.URL+"/greet", func(ctx context.Context, f *workflow.Flow) error {
		if f.Attempt() == 0 {
			if yield, err := f.Retry(time.Millisecond, 1, time.Millisecond, time.Minute); yield || err != nil {
				return err // a control signal the task asks for crosses the wire
			}
		}
		f.SetString("greeting", "hello "+workflow.BaggageFrom(ctx).GetString("tenant"))
		return nil
	})
	mux.HandleTask(srv.URL+"/shout", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("greeting", strings.ToUpper(f.GetString("greeting")))
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(&hosts.Remote{})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	_, outcome, err := eng.Run(ctx, srv.URL+"/graph", nil, &workflow.FlowOptions{Baggage: map[string]any{"tenant": "acme"}})
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.Equal("HELLO ACME", outcome.State.GetString("greeting"))
	}
	mu.Lock()
	defer mu.Unlock()
	if assert.Len(keys, 2) {
		assert.NotEqual("", keys[0])
		assert.Equal(keys[0], keys[1], "a step's key is the same on every run")
		assert.Equal([]string{"0", "1"}, attempts)
		assert.True(budgets[0] > 0, "the time budget left is sent")
	}
}

func TestRemote_AStatusIsTheTasksAnswer(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	var mu sync.Mutex
	var attempts []string
	posts := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mu.Lock()
			posts[r.URL.Path]++
			if r.URL.Path == "/patient" {
				attempts = append(attempts, r.Header.Get(hosts.HeaderAttempt))
			}
			mu.Unlock()
			if r.URL.Path == "/limited" {
				http.Error(w, "slow down", http.StatusTooManyRequests)
				return
			}
		}
		serve(mux)(w, r)
	}))
	defer srv.Close()

	for _, task := range []string{"limited", "patient"} {
		graph := workflow.NewGraph("Busy")
		graph.SetURL("Work", srv.URL+"/"+task)
		graph.AddTransition("Work", workflow.End)
		mux.HandleGraph(srv.URL+"/graph-"+task, graph)
	}
	// A task server that wants the step to wait says so with a Defer of its own.
	mux.HandleTask(srv.URL+"/patient", func(ctx context.Context, f *workflow.Flow) error {
		if f.Deferrals() < 2 {
			if yield, err := f.Defer(10*time.Millisecond, 1, 0, time.Minute); yield || err != nil {
				return err
			}
		}
		return f.Set("done", true)
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(&hosts.Remote{})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	// A 429 is the task's error: no wait, one request.
	_, outcome, err := eng.Run(ctx, srv.URL+"/graph-limited", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "slow down")
	}
	_, outcome, err = eng.Run(ctx, srv.URL+"/graph-patient", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.True(outcome.State.GetBool("done"))
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(1, posts["/limited"], "the 429 was not sent again")
	assert.Equal([]string{"0", "0", "0"}, attempts, "two deferrals, then the task ran, all one attempt")
}

func TestRemote_AWaitKeepsAResumedStepsAnswer(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	graphs := httptest.NewServer(serve(mux))
	defer graphs.Close()
	tasks := httptest.NewServer(serve(mux))
	defer tasks.Close()
	taskAddr := strings.TrimPrefix(tasks.URL, "http://")

	graph := workflow.NewGraph("Approve")
	graph.SetURL("Ask", tasks.URL+"/ask")
	graph.AddTransition("Ask", workflow.End)
	mux.HandleGraph(graphs.URL+"/graph", graph)
	var asked atomic.Int32
	mux.HandleTask(tasks.URL+"/ask", func(ctx context.Context, f *workflow.Flow) error {
		var answer struct {
			Approved bool `json:"approved"`
		}
		yield, err := f.Interrupt(map[string]any{"ask": "approve?"}, &answer)
		if yield || err != nil {
			asked.Add(1)
			return err
		}
		f.SetBool("approved", answer.Approved)
		return nil
	})

	var up atomic.Bool
	up.Store(true)
	refused := make(chan struct{})
	var once sync.Once
	client := gatedClient(taskAddr, &up, func() { once.Do(func() { close(refused) }) })

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(&hosts.Remote{Client: client})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, err := eng.Create(ctx, graphs.URL+"/graph", nil, nil)
	assert.Must(assert.NoError(err))
	outcome, err := eng.Await(ctx, flowKey)
	assert.Must(assert.NoError(err))
	assert.Must(assert.Equal(workflow.StatusInterrupted, outcome.Status))

	// The task server is down when the resumed step is sent: the step waits, and keeps its answer.
	up.Store(false)
	assert.Must(assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"approved": true})))
	select {
	case <-refused:
	case <-time.After(time.Minute):
		t.Fatal("the resumed step was never sent")
	}
	up.Store(true)
	awaitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	outcome, err = eng.Await(awaitCtx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status, "the wait did not ask again")
		assert.True(outcome.State.GetBool("approved"))
	}
	assert.Equal(int32(1), asked.Load())
}

func TestRemote_ACancelledCallerStopsDeferring(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	srv := httptest.NewServer(serve(mux))
	defer srv.Close()
	child := workflow.NewGraph("Quick")
	child.SetURL("Quick", srv.URL+"/quick")
	child.AddTransition("Quick", workflow.End)
	mux.HandleGraph(srv.URL+"/child", child)
	mux.HandleTask(srv.URL+"/quick", func(ctx context.Context, f *workflow.Flow) error { return nil })
	graph := workflow.NewGraph("Caller")
	graph.SetURL("Call", srv.URL+"/call")
	graph.AddTransition("Call", workflow.End)
	mux.HandleGraph(srv.URL+"/graph", graph)
	var runs atomic.Int32
	mux.HandleTask(srv.URL+"/call", func(ctx context.Context, f *workflow.Flow) error {
		if yield, err := f.Subgraph(srv.URL+"/child", nil, nil); yield || err != nil {
			return err
		}
		// The child returned: wait without a bound, which only the cancellation can end.
		runs.Add(1)
		if yield, err := f.Defer(10*time.Millisecond, 1, 0, 0); yield || err != nil {
			return err
		}
		return nil
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(&hosts.Remote{})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, err := eng.Create(ctx, srv.URL+"/graph", nil, nil)
	assert.Must(assert.NoError(err))
	assert.Must(assert.True(enginetest.WaitUntil(t, time.Minute, func() bool { return runs.Load() >= 2 }), "the caller deferred"))
	assert.Must(assert.NoError(eng.Cancel(ctx, flowKey, "stop")))
	awaitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	outcome, err := eng.Await(awaitCtx, flowKey)
	if assert.NoError(err) {
		// The task's Defer returned the cancellation, which crossed the wire as JSON with its marker.
		assert.Equal(workflow.StatusCancelled, outcome.Status)
		assert.Equal("stop", outcome.CancelReason)
	}
}

func TestRemote_WaitsForATaskServerThatIsDown(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	graphs := httptest.NewServer(serve(mux))
	defer graphs.Close()
	tasks := httptest.NewServer(serve(mux))
	defer tasks.Close()
	taskAddr := strings.TrimPrefix(tasks.URL, "http://")

	graph := workflow.NewGraph("Down")
	graph.SetURL("Work", tasks.URL+"/work")
	graph.AddTransition("Work", workflow.End)
	mux.HandleGraph(graphs.URL+"/graph", graph)
	mux.HandleTask(tasks.URL+"/work", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("done", true)
	})

	// The task server is down until its first dial has been refused.
	var up atomic.Bool
	refused := make(chan struct{})
	var once sync.Once
	client := gatedClient(taskAddr, &up, func() { once.Do(func() { close(refused) }) })

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(&hosts.Remote{Client: client})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, err := eng.Create(ctx, graphs.URL+"/graph", nil, nil)
	assert.Must(assert.NoError(err))
	select {
	case <-refused:
	case <-time.After(time.Minute):
		t.Fatal("the task server was never dialed")
	}
	up.Store(true)

	awaitCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	outcome, err := eng.Await(awaitCtx, flowKey)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.True(outcome.State.GetBool("done"))
	}
}

func TestRemote_RedeliversAfterALostReply(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	var mu sync.Mutex
	var keys, attempts []string
	charges := 0
	replies := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			serve(mux)(w, r)
			return
		}
		// A server that remembers the reply to a step it ran, by its key, and loses the first one. Its replies carry
		// no control signal, so replaying one is safe.
		key := r.Header.Get(hosts.HeaderIdempotencyKey)
		mu.Lock()
		keys = append(keys, key)
		attempts = append(attempts, r.Header.Get(hosts.HeaderAttempt))
		reply, ran := replies[key]
		mu.Unlock()
		if ran {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(reply)
			return
		}
		var flow workflow.Flow
		if err := json.NewDecoder(r.Body).Decode(workflow.RequestCarrier(&flow)); err != nil {
			http.Error(w, err.Error(), errors.StatusCode(err))
			return
		}
		_ = flow.Set("charged", true)
		reply, err := json.Marshal(workflow.ResultCarrier(&flow))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		mu.Lock()
		charges++
		replies[key] = reply
		mu.Unlock()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			_ = conn.Close() // the reply is lost
		}
	}))
	defer srv.Close()

	graph := workflow.NewGraph("Charge")
	graph.SetURL("Charge", srv.URL+"/charge")
	graph.AddTransition("Charge", workflow.End)
	mux.HandleGraph(srv.URL+"/graph", graph)

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	// A connection of its own for every request: on a reused connection the standard library resends a request
	// that carries an idempotency key itself, which would hide Remote's own redelivery.
	eng.SetHost(&hosts.Remote{Client: &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	_, outcome, err := eng.Run(ctx, srv.URL+"/graph", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.True(outcome.State.GetBool("charged"))
	}
	mu.Lock()
	defer mu.Unlock()
	if assert.Len(keys, 2) {
		assert.Equal(keys[0], keys[1], "the redelivery carries the same key")
		assert.Equal([]string{"0", "0"}, attempts, "Remote dispatched the step again, as the same attempt")
	}
	assert.Equal(1, charges, "the server ran the step once")
}

func TestRemote_GivesUpPastItsHorizon(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	graphs := httptest.NewServer(serve(mux))
	defer graphs.Close()
	tasks := httptest.NewServer(serve(mux))
	defer tasks.Close()

	graph := workflow.NewGraph("Maintenance")
	graph.SetURL("Work", tasks.URL+"/work")
	graph.AddTransition("Work", workflow.End)
	mux.HandleGraph(graphs.URL+"/graph", graph)
	var refusals atomic.Int32
	client := gatedClient(strings.TrimPrefix(tasks.URL, "http://"), &atomic.Bool{}, func() { refusals.Add(1) })

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	// A 1s backoff, then 2s: the second would take the summed backoff to 3s, past the 2s horizon.
	eng.SetHost(&hosts.Remote{Client: client, HTTPRetryHorizon: 2 * time.Second})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	_, outcome, err := eng.Run(ctx, graphs.URL+"/graph", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "cannot be reached")
	}
	assert.Equal(int32(2), refusals.Load())
}

func TestRemote_FailsAStepPastItsTimeBudget(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			// Answers only once the host gives up. The server sees the client go away only once the body is read.
			_, _ = io.Copy(io.Discard, r.Body)
			<-r.Context().Done()
			return
		}
		serve(mux)(w, r)
	}))
	defer srv.Close()

	graph := workflow.NewGraph("Slow")
	graph.SetURL("Work", srv.URL+"/work")
	graph.AddTransition("Work", workflow.End)
	mux.HandleGraph(srv.URL+"/graph", graph)

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(&hosts.Remote{})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	_, outcome, err := eng.Run(ctx, srv.URL+"/graph", nil, &workflow.FlowOptions{TimeBudget: time.Second})
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "did not reply within its time budget")
	}
}

func TestRemote_ReusesConnectionsUnderConcurrency(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	var opened atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var flow workflow.Flow
		if err := json.NewDecoder(r.Body).Decode(workflow.RequestCarrier(&flow)); err != nil {
			http.Error(w, err.Error(), errors.StatusCode(err))
			return
		}
		time.Sleep(20 * time.Millisecond) // the work, so a wave's requests overlap
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(workflow.ResultCarrier(&flow))
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			opened.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()

	remote := &hosts.Remote{}
	wave := func() {
		var wg sync.WaitGroup
		for range 32 {
			wg.Go(func() {
				assert.NoError(remote.ExecuteTask(ctx, srv.URL+"/work", workflow.NewFlow()))
			})
		}
		wg.Wait()
	}
	wave()
	first := opened.Load()
	assert.True(first > 8, "the first wave's requests ran at once, on connections of their own")
	wave()
	// A pool of 2 idle connections per host would open about 30 again; a few may still be returning to the pool.
	assert.True(opened.Load()-first < 8, "the second wave reuses the first wave's connections")
}

func TestRemote_MapsReplies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var landed atomic.Bool
	var baggageHeader atomic.Value
	var budgets sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/graph":
			_ = json.NewEncoder(w).Encode(workflow.NewGraph("Mapped"))
		case "/reject":
			http.Error(w, "bad input", http.StatusUnprocessableEntity)
		case "/fail":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"boom","statusCode":500}`))
		case "/cancelled":
			// The cancellation a task's Defer returned, encoded as the errors package encodes it.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"flow cancelled: stop","statusCode":500,"dwarf_type":"cancelled"}`))
		case "/odd":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte(`{"error":"validation failed","statusCode":"422","field":"name"}`))
		case "/big-cancelled":
			// A cancellation whose stack runs past the message's bound.
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error":"flow cancelled: stop","dwarf_type":"cancelled","stack":"` + strings.Repeat("frame ", 4000) + `"}`))
		case "/slow":
			_, _ = io.Copy(io.Discard, r.Body)
			time.Sleep(300 * time.Millisecond) // simulated work, longer than the client's own timeout
			_, _ = w.Write([]byte(`{"v":1}`))
		case "/hop1", "/hop2", "/hop3":
			// A chain of slow hops: each one alone is within the client's Timeout, the chain is not.
			_, _ = io.Copy(io.Discard, r.Body)
			time.Sleep(150 * time.Millisecond) // simulated work
			next := map[string]string{"/hop1": "/hop2", "/hop2": "/hop3", "/hop3": "/landing"}[r.URL.Path]
			http.Redirect(w, r, next, http.StatusTemporaryRedirect)
		case "/budget-first":
			_, _ = io.Copy(io.Discard, r.Body)
			budgets.Store("first", r.Header.Get(hosts.HeaderBudgetMillis))
			time.Sleep(200 * time.Millisecond) // simulated work
			http.Redirect(w, r, "/budget-second", http.StatusTemporaryRedirect)
		case "/budget-second":
			_, _ = io.Copy(io.Discard, r.Body)
			budgets.Store("second", r.Header.Get(hosts.HeaderBudgetMillis))
			_, _ = w.Write([]byte(`{"v":1}`))
		case "/choices":
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Location", "/landing")
			http.Error(w, "pick one", http.StatusMultipleChoices)
		case "/to-ftp", "/graph-to-ftp":
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Location", "ftp://files.verify:428/task")
			w.WriteHeader(http.StatusTemporaryRedirect)
		case "/bad-location":
			_, _ = io.Copy(io.Discard, r.Body)
			w.Header().Set("Location", "http://[::1")
			w.WriteHeader(http.StatusTemporaryRedirect)
		case "/unversioned":
			_, _ = w.Write([]byte(`{"changes":{"x":1}}`))
		case "/empty":
			w.WriteHeader(http.StatusNoContent)
		case "/busy":
			w.Header().Set("Retry-After", "7")
			http.Error(w, "busy", http.StatusServiceUnavailable)
		case "/gateway":
			http.Error(w, "no upstream", http.StatusBadGateway)
		case "/moved":
			http.Redirect(w, r, "/landing", http.StatusFound)
		case "/moved-with-error":
			w.Header().Set("Location", "/landing")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusFound)
			_, _ = w.Write([]byte(`{"error":"moved for maintenance","ticket":"T-7"}`))
		case "/to-blackhole":
			_, _ = io.Copy(io.Discard, r.Body)
			http.Redirect(w, r, "http://blackhole.verify:428/task", http.StatusTemporaryRedirect)
		case "/landing":
			landed.Store(true)
			_, _ = w.Write([]byte(`{"v":1}`))
		case "/relocated":
			http.Redirect(w, r, "/relocated-here", http.StatusPermanentRedirect)
		case "/relocated-here":
			body, _ := io.ReadAll(r.Body)
			if r.Method != http.MethodPost || len(body) == 0 {
				http.Error(w, "the POST and its body did not arrive", http.StatusBadRequest)
				return
			}
			_, _ = w.Write([]byte(`{"v":1,"changes":{"followed":true}}`))
		case "/graph-moved":
			http.Redirect(w, r, "/graph", http.StatusMovedPermanently)
		case "/loop":
			http.Redirect(w, r, "/loop", http.StatusTemporaryRedirect)
		case "/echo":
			baggageHeader.Store(r.Header.Get(hosts.HeaderBaggage))
			_, _ = w.Write([]byte(`{"v":1}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	remote := &hosts.Remote{}
	down := &hosts.Remote{Client: gatedClient("down.verify:428", &atomic.Bool{}, nil)}

	t.Run("graphs", func(t *testing.T) {
		assert := testarossa.For(t)
		graph, err := remote.LoadGraph(ctx, srv.URL+"/graph")
		if assert.NoError(err) && assert.NotNil(graph) {
			assert.Equal("Mapped", graph.Name())
		}
		graph, err = remote.LoadGraph(ctx, srv.URL+"/missing")
		assert.NoError(err)
		assert.Nil(graph, "a 404 is no such graph")
		graph, err = remote.LoadGraph(ctx, "mux:428/graph")
		assert.NoError(err)
		assert.Nil(graph, "a URL that is not http is no graph of Remote's")
		_, err = down.LoadGraph(ctx, "http://down.verify:428/graph")
		assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err))
		_, err = remote.LoadGraph(ctx, srv.URL+"/gateway")
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err), "a status is the server's answer, as it is")
	})

	t.Run("task_errors", func(t *testing.T) {
		assert := testarossa.For(t)
		err := remote.ExecuteTask(ctx, srv.URL+"/reject", workflow.NewFlow())
		assert.Equal(http.StatusUnprocessableEntity, errors.StatusCode(err))
		assert.Contains(err.Error(), "bad input")
		err = remote.ExecuteTask(ctx, srv.URL+"/fail", workflow.NewFlow())
		assert.Equal(http.StatusInternalServerError, errors.StatusCode(err))
		assert.Equal("boom", err.Error())
		err = remote.ExecuteTask(ctx, srv.URL+"/cancelled", workflow.NewFlow())
		assert.True(workflow.IsCancelled(err), "the cancellation's marker crosses the wire")
		assert.Equal("flow cancelled: stop", err.Error())
		assert.Equal(http.StatusInternalServerError, errors.StatusCode(err))
		// A status asks for no wait, a 503 and a proxy's 502 included: a server that wants one replies with a Defer.
		busy := workflow.NewRawFlow()
		err = remote.ExecuteTask(ctx, srv.URL+"/busy", &busy.Flow)
		assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err))
		assert.Equal("busy", err.Error())
		_, _, _, deferred := busy.DeferRequested()
		assert.False(deferred, "a 503 is the task's error, not a wait")
		err = remote.ExecuteTask(ctx, srv.URL+"/gateway", workflow.NewFlow())
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err))
		err = remote.ExecuteTask(ctx, srv.URL+"/odd", workflow.NewFlow())
		assert.Equal("validation failed", err.Error(), "a field of another type leaves the message")
		assert.Equal(http.StatusUnprocessableEntity, errors.StatusCode(err))
		assert.Equal("name", errors.Convert(err).Properties["field"], "and the other properties")
		err = remote.ExecuteTask(ctx, srv.URL+"/big-cancelled", workflow.NewFlow())
		assert.True(workflow.IsCancelled(err), "a body past the message's bound still decodes")
		assert.Equal("flow cancelled: stop", err.Error())

		// A client's own timeout, after the request went out, is a task that did not answer in time.
		impatient := &hosts.Remote{Client: &http.Client{Timeout: 100 * time.Millisecond}}
		slow := workflow.NewRawFlow()
		err = impatient.ExecuteTask(ctx, srv.URL+"/slow", &slow.Flow)
		assert.Equal(http.StatusRequestTimeout, errors.StatusCode(err))
		_, _, _, deferred = slow.DeferRequested()
		assert.False(deferred, "the request was sent: no wait, no resend")
		err = remote.ExecuteTask(ctx, srv.URL+"/unversioned", workflow.NewFlow())
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err), "a reply without its carrier version")
		err = remote.ExecuteTask(ctx, srv.URL+"/empty", workflow.NewFlow())
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err), "a 2xx must carry a result")
		err = remote.ExecuteTask(ctx, "mux:428/task", workflow.NewFlow())
		assert.Equal(http.StatusNotFound, errors.StatusCode(err))
	})

	t.Run("redirects", func(t *testing.T) {
		assert := testarossa.For(t)
		err := remote.ExecuteTask(ctx, srv.URL+"/moved", workflow.NewFlow())
		assert.Equal(http.StatusFound, errors.StatusCode(err), "a 302 would resend the POST as a GET")
		assert.False(landed.Load(), "the POST did not become a GET of the new location")
		// The client's Timeout bounds the whole chain, not each hop.
		impatient := &hosts.Remote{Client: &http.Client{Timeout: 250 * time.Millisecond}}
		err = impatient.ExecuteTask(ctx, srv.URL+"/hop1", workflow.NewFlow())
		assert.Equal(http.StatusRequestTimeout, errors.StatusCode(err), "three hops of 150ms outlast a 250ms Timeout")

		// Each hop is told the time left in the budget as it is then.
		budgetCtx, budgetCancel := context.WithTimeout(ctx, 5*time.Second)
		defer budgetCancel()
		assert.NoError(remote.ExecuteTask(budgetCtx, srv.URL+"/budget-first", workflow.NewFlow()))
		first, _ := budgets.Load("first")
		second, _ := budgets.Load("second")
		firstMs, _ := strconv.Atoi(first.(string))
		secondMs, _ := strconv.Atoi(second.(string))
		assert.True(secondMs <= firstMs-150, "the second hop is told less: %d then %d", firstMs, secondMs)

		// A transport that reports no progress still has its redirects counted, not taken for a request it cannot send.
		looping := &hosts.Remote{Client: &http.Client{Transport: redirectLoop{}}}
		err = looping.ExecuteTask(ctx, "http://loop.verify:428/task", workflow.NewFlow())
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err))
		assert.Contains(err.Error(), "redirects more than")

		// A 3xx that is no redirect to follow is the task's error with its status, without the redirect's diagnostic.
		err = remote.ExecuteTask(ctx, srv.URL+"/choices", workflow.NewFlow())
		assert.Equal(http.StatusMultipleChoices, errors.StatusCode(err))
		assert.Equal("pick one", err.Error())

		// A late callback of the first hop lands during the second hop's dial, which the budget cuts short: that
		// server never got the request, so the step waits.
		late := &hosts.Remote{Client: &http.Client{Transport: lateFirstHop{}}}
		lateCtx, lateCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer lateCancel()
		lateFlow := workflow.NewRawFlow()
		assert.NoError(late.ExecuteTask(lateCtx, "http://first.verify:428/task", &lateFlow.Flow), "not a 408")
		_, _, _, lateDeferred := lateFlow.DeferRequested()
		assert.True(lateDeferred, "each hop has a progress of its own")

		// A redirect the client cannot follow is the server's answer, not a lost reply to wait for.
		for _, path := range []string{"/to-ftp", "/bad-location"} {
			unfollowed := workflow.NewRawFlow()
			err := remote.ExecuteTask(ctx, srv.URL+path, &unfollowed.Flow)
			assert.Equal(http.StatusBadGateway, errors.StatusCode(err), path)
			assert.Contains(err.Error(), "cannot be followed", path)
			_, _, _, deferred := unfollowed.DeferRequested()
			assert.False(deferred, path)
		}
		_, err = remote.LoadGraph(ctx, srv.URL+"/graph-to-ftp")
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err), "nor is it a graph server that cannot be reached")
		err = remote.ExecuteTask(ctx, srv.URL+"/moved-with-error", workflow.NewFlow())
		assert.Equal(http.StatusFound, errors.StatusCode(err))
		assert.Contains(err.Error(), "redirects with 302")
		assert.Contains(err.Error(), "moved for maintenance", "the reply's own message is kept")
		assert.Equal("T-7", errors.Convert(err).Properties["ticket"], "and its properties")

		// A 307 to a server whose dial outlasts the budget never reached that server: the step waits.
		toBlackhole := &hosts.Remote{Client: &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if addr == "blackhole.verify:428" {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			},
		}}}
		budgetCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		redirected := workflow.NewRawFlow()
		assert.NoError(toBlackhole.ExecuteTask(budgetCtx, srv.URL+"/to-blackhole", &redirected.Flow), "not a 408")
		_, _, _, deferred := redirected.DeferRequested()
		assert.True(deferred, "the redirected request never reached its server")

		followed := workflow.NewRawFlow()
		assert.NoError(remote.ExecuteTask(ctx, srv.URL+"/relocated", &followed.Flow), "a 308 keeps the POST and its body")
		assert.True(followed.RawChanges().Mentions("followed"), "the result came back from the new location")

		graph, err := remote.LoadGraph(ctx, srv.URL+"/graph-moved")
		if assert.NoError(err, "a GET follows any redirect") && assert.NotNil(graph) {
			assert.Equal("Mapped", graph.Name())
		}

		raw := workflow.NewRawFlow()
		err = remote.ExecuteTask(ctx, srv.URL+"/loop", &raw.Flow)
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err), "a redirect loop stops")
		_, _, _, ok := raw.DeferRequested()
		assert.False(ok, "a redirect loop is not waited for")
	})

	t.Run("waits", func(t *testing.T) {
		assert := testarossa.For(t)

		// A connection the network gave up on after the request went out reports a timeout too, but it is a reply
		// lost, which waits, not the client's Timeout.
		dropped := &hosts.Remote{Client: &http.Client{Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				return timedOutReads{conn}, nil
			},
		}}}
		lost := workflow.NewRawFlow()
		assert.NoError(dropped.ExecuteTask(ctx, srv.URL+"/echo", &lost.Flow), "not a 408")
		_, _, _, lostDeferred := lost.DeferRequested()
		assert.True(lostDeferred, "a reply the network lost waits")

		flow := workflow.NewRawFlow()
		assert.NoError(down.ExecuteTask(ctx, "http://down.verify:428/task", &flow.Flow))
		delay, multiplier, maxDelay, ok := flow.DeferRequested()
		if assert.True(ok, "a refused connection defers the step") {
			assert.Equal(time.Second, delay)
			assert.Equal(2.0, multiplier)
			assert.Equal(30*time.Second, maxDelay)
		}

		// A dial still hanging when the budget runs out never reached the server: the step waits, not fails.
		hanging := &hosts.Remote{Client: &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}}}
		budgetCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		flow = workflow.NewRawFlow()
		assert.NoError(hanging.ExecuteTask(budgetCtx, "http://blackhole.verify:428/task", &flow.Flow))
		_, _, _, ok = flow.DeferRequested()
		assert.True(ok, "a dial cut short by the budget defers the step")

		// A step of a cancelled flow does not wait: the cancellation comes back as the task's error.
		flow = workflow.NewRawFlow()
		flow.SetCancellation(errors.New("flow cancelled: stop", "dwarf_type", "cancelled"))
		cancelled := down.ExecuteTask(ctx, "http://down.verify:428/task", &flow.Flow)
		assert.True(workflow.IsCancelled(cancelled), "an absent server on a cancelled flow returns the cancellation")
		_, _, _, ok = flow.DeferRequested()
		assert.False(ok)

		noWait := &hosts.Remote{Client: down.Client, HTTPRetryHorizon: -1}
		flow = workflow.NewRawFlow()
		err := noWait.ExecuteTask(ctx, "http://down.verify:428/task", &flow.Flow)
		assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err), "no horizon fails at once")
		assert.Contains(err.Error(), "cannot be reached")
		_, _, _, ok = flow.DeferRequested()
		assert.False(ok)

		flow = workflow.NewRawFlow()
		flow.SetCancellation(errors.New("flow cancelled: stop", "dwarf_type", "cancelled"))
		err = noWait.ExecuteTask(ctx, "http://down.verify:428/task", &flow.Flow)
		assert.True(workflow.IsCancelled(err), "with no horizon too, a cancelled flow gets its cancellation")
		_, _, _, ok = flow.DeferRequested()
		assert.False(ok)
	})

	t.Run("refused_certificate", func(t *testing.T) {
		assert := testarossa.For(t)
		tlsSrv := httptest.NewTLSServer(http.NotFoundHandler())
		defer tlsSrv.Close()
		flow := workflow.NewRawFlow()
		err := remote.ExecuteTask(ctx, tlsSrv.URL+"/task", &flow.Flow)
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err))
		_, _, _, ok := flow.DeferRequested()
		assert.False(ok, "a certificate the client refuses is not waited for")
	})

	t.Run("baggage_header_is_ascii", func(t *testing.T) {
		assert := testarossa.For(t)
		baggage, err := workflow.NewState("note", "café\x7f😀")
		assert.Must(assert.NoError(err))
		err = remote.ExecuteTask(workflow.ContextWithBaggage(ctx, baggage), srv.URL+"/echo", workflow.NewFlow())
		assert.Must(assert.NoError(err))
		header, _ := baggageHeader.Load().(string)
		for _, c := range header {
			assert.True(c >= 0x20 && c < 0x7F, "%q is printable ASCII", c)
		}
		var got workflow.State
		if assert.NoError(json.Unmarshal([]byte(header), &got)) {
			assert.Equal("café\x7f😀", got.GetString("note"))
		}
	})
}

// lateFirstHop is a transport whose first hop redirects with a 307 and reports its request written only later, as a
// trace callback can arrive after its round trip returned, and whose second hop never connects.
type lateFirstHop struct{}

func (lateFirstHop) RoundTrip(req *http.Request) (*http.Response, error) {
	trace := httptrace.ContextClientTrace(req.Context())
	if trace != nil && trace.GetConn != nil {
		trace.GetConn(req.URL.Host)
	}
	if req.URL.Host == "first.verify:428" {
		if req.Body != nil {
			_, _ = io.Copy(io.Discard, req.Body)
		}
		go func() {
			time.Sleep(50 * time.Millisecond)
			if trace != nil && trace.WroteRequest != nil {
				trace.WroteRequest(httptrace.WroteRequestInfo{})
			}
		}()
		return &http.Response{
			StatusCode: http.StatusTemporaryRedirect,
			Status:     "307 Temporary Redirect",
			Header:     http.Header{"Location": {"http://second.verify:428/task"}},
			Body:       io.NopCloser(strings.NewReader("")), // and no Request, which only net/http's transport sets
		}, nil
	}
	<-req.Context().Done()
	return nil, req.Context().Err()
}

// redirectLoop is a transport that reports no progress through httptrace and redirects every request to itself.
type redirectLoop struct{}

func (redirectLoop) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil {
		_, _ = io.Copy(io.Discard, req.Body)
	}
	return &http.Response{
		StatusCode: http.StatusTemporaryRedirect,
		Status:     "307 Temporary Redirect",
		Header:     http.Header{"Location": {req.URL.String()}},
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

// timedOutReads is a connection whose reads fail as a connection the kernel gave up on does.
type timedOutReads struct{ net.Conn }

func (timedOutReads) Read([]byte) (int, error) {
	return 0, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ETIMEDOUT}
}
