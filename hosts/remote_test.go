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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

var _ engine.Host = (*hosts.Remote)(nil)

// serve is the tests' task server, over the graphs and tasks registered on mux: a GET of a workflow URL answers
// with the graph's JSON, and a POST of a task URL runs the task on the decoded carrier and answers with its
// result. A request's URL is http, its host and its path.
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
				http.Error(w, err.Error(), errors.StatusCode(err))
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
// calling refused at each refusal.
func gatedClient(addr string, up *atomic.Bool, refused func()) *http.Client {
	return &http.Client{Transport: &http.Transport{
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
		if f.Attempt() == 0 && f.Retry(time.Millisecond, 1, time.Millisecond, time.Minute) {
			return nil // a control signal the task asks for crosses the wire
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

func TestRemote_HonorsRetryAfter(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	var mu sync.Mutex
	var attempts []string
	var at []time.Time
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mu.Lock()
			attempts = append(attempts, r.Header.Get(hosts.HeaderAttempt))
			at = append(at, time.Now())
			n := len(attempts)
			mu.Unlock()
			if n <= 2 {
				// Longer than the 1s backoff Remote takes when no Retry-After is named.
				w.Header().Set("Retry-After", "2")
				http.Error(w, "slow down", http.StatusTooManyRequests)
				return
			}
		}
		serve(mux)(w, r)
	}))
	defer srv.Close()

	graph := workflow.NewGraph("Busy")
	graph.SetURL("Work", srv.URL+"/work")
	graph.AddTransition("Work", workflow.End)
	mux.HandleGraph(srv.URL+"/graph", graph)
	mux.HandleTask(srv.URL+"/work", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("done", true)
	})

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(&hosts.Remote{})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	_, outcome, err := eng.Run(ctx, srv.URL+"/graph", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusCompleted, outcome.Status)
		assert.True(outcome.State.GetBool("done"))
	}
	mu.Lock()
	defer mu.Unlock()
	if assert.Equal([]string{"0", "1", "2"}, attempts, "two 429s, then the task ran") {
		for i := 1; i < len(at); i++ {
			assert.True(at[i].Sub(at[i-1]) >= 1900*time.Millisecond, "dispatch %d waited the Retry-After", i)
		}
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
		assert.Equal([]string{"0", "1"}, attempts, "Remote dispatched the step again")
	}
	assert.Equal(1, charges, "the server ran the step once")
}

func TestRemote_GivesUpPastItsHorizon(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	mux := hosts.NewMux()
	var mu sync.Mutex
	posts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			mu.Lock()
			posts++
			mu.Unlock()
			http.Error(w, "down for maintenance", http.StatusServiceUnavailable)
			return
		}
		serve(mux)(w, r)
	}))
	defer srv.Close()

	graph := workflow.NewGraph("Maintenance")
	graph.SetURL("Work", srv.URL+"/work")
	graph.AddTransition("Work", workflow.End)
	mux.HandleGraph(srv.URL+"/graph", graph)

	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	// A 1s backoff, then 2s: the second would take the summed backoff to 3s, past the 2s horizon.
	eng.SetHost(&hosts.Remote{RetryHorizon: 2 * time.Second})
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	_, outcome, err := eng.Run(ctx, srv.URL+"/graph", nil, nil)
	if assert.NoError(err) {
		assert.Equal(workflow.StatusFailed, outcome.Status)
		assert.Contains(outcome.Error, "down for maintenance", "the server's message is kept")
	}
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(2, posts)
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
		assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err), "a proxy's 502 is a server to retry")
	})

	t.Run("task_errors", func(t *testing.T) {
		assert := testarossa.For(t)
		err := remote.ExecuteTask(ctx, srv.URL+"/reject", workflow.NewFlow())
		assert.Equal(http.StatusUnprocessableEntity, errors.StatusCode(err))
		assert.Contains(err.Error(), "bad input")
		err = remote.ExecuteTask(ctx, srv.URL+"/fail", workflow.NewFlow())
		assert.Equal(http.StatusInternalServerError, errors.StatusCode(err))
		assert.Equal("boom", err.Error())
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
		_, _, _, ok := raw.RetryRequested()
		assert.False(ok, "a redirect loop is not waited for")
	})

	t.Run("waits", func(t *testing.T) {
		assert := testarossa.For(t)
		flow := workflow.NewRawFlow()
		assert.NoError(remote.ExecuteTask(ctx, srv.URL+"/busy", &flow.Flow))
		delay, multiplier, maxDelay, ok := flow.RetryRequested()
		if assert.True(ok, "a 503 arms a retry") {
			assert.Equal(7*time.Second, delay)
			assert.Equal(1.0, multiplier)
			assert.Equal(7*time.Second, maxDelay)
		}

		flow = workflow.NewRawFlow()
		assert.NoError(remote.ExecuteTask(ctx, srv.URL+"/gateway", &flow.Flow))
		_, _, _, ok = flow.RetryRequested()
		assert.True(ok, "a proxy's 502 arms a retry")

		flow = workflow.NewRawFlow()
		assert.NoError(down.ExecuteTask(ctx, "http://down.verify:428/task", &flow.Flow))
		_, _, _, ok = flow.RetryRequested()
		assert.True(ok, "a refused connection arms a retry")

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
		_, _, _, ok = flow.RetryRequested()
		assert.True(ok, "a dial cut short by the budget arms a retry")

		flow = workflow.NewRawFlow()
		err := (&hosts.Remote{RetryHorizon: -1}).ExecuteTask(ctx, srv.URL+"/busy", &flow.Flow)
		assert.Equal(http.StatusServiceUnavailable, errors.StatusCode(err), "no horizon fails at once")
		assert.Contains(err.Error(), "busy", "with the server's message")
		_, _, _, ok = flow.RetryRequested()
		assert.False(ok)
	})

	t.Run("refused_certificate", func(t *testing.T) {
		assert := testarossa.For(t)
		tlsSrv := httptest.NewTLSServer(http.NotFoundHandler())
		defer tlsSrv.Close()
		flow := workflow.NewRawFlow()
		err := remote.ExecuteTask(ctx, tlsSrv.URL+"/task", &flow.Flow)
		assert.Equal(http.StatusBadGateway, errors.StatusCode(err))
		_, _, _, ok := flow.RetryRequested()
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
