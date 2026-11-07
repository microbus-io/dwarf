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
A subgraph that cannot be spawned - its graph fails to load, does not exist, is invalid, or its loader panics -
comes back to the calling task from flow.Subgraph as an error carrying its status code, the way a failed child
does. No child flow is created and out receives nothing. The task decides: return the error (taking the graph's onError transition, or
failing the step without one) or flow.Retry, whose next run spawns again.
*/
package fixtures

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

const spawnFailPrefix = "subgraphspawnfailflow.verify:428"

// spawnFailHost serves graphs from its Mux, except for the child URLs that stand for each way a spawn can fail.
type spawnFailHost struct {
	*hosts.Mux
	// unavailable is how many more loads of the flaky child answer 503 before it is served.
	unavailable atomic.Int32
	// flakyLoads counts loads of the flaky child.
	flakyLoads atomic.Int32
}

func (h *spawnFailHost) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	switch workflowURL {
	case spawnFailPrefix + "/unreachable":
		return nil, errors.New("no worker serves %s", workflowURL, http.StatusServiceUnavailable)
	case spawnFailPrefix + "/missing":
		return nil, nil
	case spawnFailPrefix + "/panics":
		panic("graph loader exploded")
	case spawnFailPrefix + "/flaky":
		h.flakyLoads.Add(1)
		if h.unavailable.Add(-1) >= 0 {
			return nil, errors.New("no worker serves %s", workflowURL, http.StatusServiceUnavailable)
		}
	}
	return h.Mux.LoadGraph(ctx, workflowURL)
}

func TestSubgraphSpawnFailFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	ctx := context.Background()

	host := &spawnFailHost{Mux: hosts.NewMux()}
	mux := host.Mux
	eng := engine.NewEngineUnderTest(t.Name())
	defer eng.Shutdown(ctx)
	eng.SetHost(host)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	// A child graph that fails Validate: no transition to END.
	invalid := workflow.NewGraph("Invalid")
	invalid.SetURL("Lonely", spawnFailPrefix+"/lonely")
	mux.HandleGraph(spawnFailPrefix+"/invalid", invalid)

	// A child that works, served by the flaky URL once its 503s run out.
	child := workflow.NewGraph("Child")
	child.SetURL("Work", spawnFailPrefix+"/work")
	child.AddTransition("Work", workflow.End)
	mux.HandleGraph(spawnFailPrefix+"/flaky", child)
	mux.HandleTask(spawnFailPrefix+"/work", func(ctx context.Context, f *workflow.Flow) error {
		f.SetString("childSaid", "hello")
		return nil
	})

	// The caller records what flow.Subgraph returned. Its first run writes firstRun before yielding, so the re-run
	// reading it back proves the first run's changes were kept with the resolved park.
	mux.HandleTask(spawnFailPrefix+"/record", func(ctx context.Context, f *workflow.Flow) error {
		var out map[string]any
		yield, err := f.Subgraph(f.GetString("childURL"), nil, &out)
		if yield {
			f.SetBool("firstRun", true)
			return nil
		}
		f.SetBool("sawFirstRun", f.GetBool("firstRun"))
		f.SetInt("outFields", len(out))
		if err != nil {
			f.SetInt("code", errors.StatusCode(err))
			f.SetString("message", err.Error())
		}
		return nil
	})
	recorder := workflow.NewGraph("Recorder")
	recorder.SetURL("Call", spawnFailPrefix+"/record")
	recorder.AddTransition("Call", workflow.End)
	mux.HandleGraph(spawnFailPrefix+"/recorder", recorder)

	t.Run("each_failure_reaches_the_task_with_its_code", func(t *testing.T) {
		cases := []struct {
			child   string
			code    int
			message string
		}{
			{"unreachable", http.StatusServiceUnavailable, "no worker serves"},
			{"missing", http.StatusNotFound, "subgraph graph not found"},
			{"invalid", http.StatusBadRequest, "invalid subgraph graph"},
			{"panics", http.StatusInternalServerError, "graph loader exploded"},
		}
		for _, tc := range cases {
			t.Run(tc.child, func(t *testing.T) {
				assert := testarossa.For(t)
				flowKey, err := eng.Create(ctx, spawnFailPrefix+"/recorder", map[string]any{"childURL": spawnFailPrefix + "/" + tc.child}, nil)
				if !assert.NoError(err) {
					return
				}
				out, err := enginetest.BoundedAwait(t, eng, flowKey)
				if !assert.NoError(err) || !assert.NotNil(out) {
					return
				}
				assert.Equal(workflow.StatusCompleted, out.Status)
				assert.Equal(float64(tc.code), stateVal(out.State, "code"))
				message, _ := stateVal(out.State, "message").(string)
				assert.True(strings.Contains(message, tc.message), "got %q", message)
				assert.Equal(true, stateVal(out.State, "sawFirstRun"))
				assert.Equal(float64(0), stateVal(out.State, "outFields"))

				// No child flow was created for the caller step.
				history, err := eng.History(ctx, flowKey)
				if assert.NoError(err) && assert.Len(history, 1) {
					assert.False(history[0].Subgraph)
					assert.Equal("", history[0].SubgraphKey)
				}
			})
		}
	})

	t.Run("returned_error_takes_on_error", func(t *testing.T) {
		assert := testarossa.For(t)
		mux.HandleTask(spawnFailPrefix+"/call-return", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph(spawnFailPrefix+"/unreachable", nil, nil)
			if yield {
				return nil
			}
			return err
		})
		mux.HandleTask(spawnFailPrefix+"/recover", func(ctx context.Context, f *workflow.Flow) error {
			var onErr errors.TracedError
			_, _ = f.Get("onErr", &onErr)
			f.SetString("recovered", onErr.Error())
			f.SetInt("recoveredCode", onErr.StatusCode)
			return nil
		})
		mux.HandleTask(spawnFailPrefix+"/after", func(ctx context.Context, f *workflow.Flow) error {
			f.SetBool("afterRan", true)
			return nil
		})
		g := workflow.NewGraph("Handled")
		g.SetURL("Call", spawnFailPrefix+"/call-return")
		g.SetURL("After", spawnFailPrefix+"/after")
		g.SetURL("Recover", spawnFailPrefix+"/recover")
		g.AddTransitionChain("Call", "After", workflow.End)
		g.AddTransitionOnError("Call", "Recover")
		g.AddTransition("Recover", workflow.End)
		mux.HandleGraph(spawnFailPrefix+"/handled", g)

		out := enginetest.BoundedRun(t, eng, spawnFailPrefix+"/handled")
		if !assert.NotNil(out) {
			return
		}
		assert.Equal(workflow.StatusCompleted, out.Status)
		recovered, _ := stateVal(out.State, "recovered").(string)
		assert.True(strings.Contains(recovered, "no worker serves"), "got %q", recovered)
		assert.Equal(float64(http.StatusServiceUnavailable), stateVal(out.State, "recoveredCode"))
		assert.Nil(stateVal(out.State, "afterRan"))
	})

	t.Run("unhandled_error_fails_the_flow", func(t *testing.T) {
		assert := testarossa.For(t)
		mux.HandleTask(spawnFailPrefix+"/call-unhandled", func(ctx context.Context, f *workflow.Flow) error {
			yield, err := f.Subgraph(spawnFailPrefix+"/missing", nil, nil)
			if yield {
				return nil
			}
			return err
		})
		g := workflow.NewGraph("Unhandled")
		g.SetURL("Call", spawnFailPrefix+"/call-unhandled")
		g.AddTransition("Call", workflow.End)
		mux.HandleGraph(spawnFailPrefix+"/unhandled", g)

		out := enginetest.BoundedRun(t, eng, spawnFailPrefix+"/unhandled")
		if !assert.NotNil(out) {
			return
		}
		assert.Equal(workflow.StatusFailed, out.Status)
		assert.True(strings.Contains(out.Error, "subgraph graph not found: "+spawnFailPrefix+"/missing"), "got %q", out.Error)
	})

	t.Run("retry_on_503_spawns_again", func(t *testing.T) {
		assert := testarossa.For(t)
		host.unavailable.Store(2)
		var attempts atomic.Int32
		mux.HandleTask(spawnFailPrefix+"/call-retry", func(ctx context.Context, f *workflow.Flow) error {
			var out struct {
				ChildSaid string `json:"childSaid"`
			}
			yield, err := f.Subgraph(spawnFailPrefix+"/flaky", nil, &out)
			if yield {
				return nil
			}
			if err != nil {
				attempts.Add(1)
				if errors.StatusCode(err) == http.StatusServiceUnavailable && f.Retry(10*time.Millisecond, 1, 10*time.Millisecond, time.Minute) {
					return nil
				}
				return err
			}
			f.SetString("fromChild", out.ChildSaid)
			f.SetInt("attempt", f.Attempt())
			return nil
		})
		g := workflow.NewGraph("Retrying")
		g.SetURL("Call", spawnFailPrefix+"/call-retry")
		g.AddTransition("Call", workflow.End)
		mux.HandleGraph(spawnFailPrefix+"/retrying", g)

		out := enginetest.BoundedRun(t, eng, spawnFailPrefix+"/retrying")
		if !assert.NotNil(out) {
			return
		}
		assert.Equal(workflow.StatusCompleted, out.Status)
		assert.Equal("hello", stateVal(out.State, "fromChild"))
		assert.Equal(float64(2), stateVal(out.State, "attempt"))
		assert.Equal(int32(2), attempts.Load())
		assert.Equal(int32(3), host.flakyLoads.Load())
	})
}
