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
A task reached across a process boundary runs on a carrier that crossed the wire as JSON, and the engine reads
back only the task's changes and control signals. So the same workflow - reducers, routing on a written field, a
retry, a sleep, a goto, a subgraph, a fan-out and fan-in, an interrupt, its resume and two deferrals - reaches the
same outcome whether its tasks run in process, behind the full carrier both ways, behind the request and result
carriers, or behind a task server whose reply forges the state, the reducers, the attempt, the deferrals and the
keys.
*/
package fixtures

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/hosts"
	"github.com/microbus-io/dwarf/internal/enginetest"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

const carrierWirePrefix = "carrierwireflow.verify:428"

// wireHost runs its Mux's tasks as a remote task server would: the request crosses as JSON into a fresh Flow, the
// task runs on that, and the reply crosses back onto the engine's carrier.
type wireHost struct {
	*hosts.Mux
	mode string // "full", "carriers" or "forged"
}

func (h *wireHost) ExecuteTask(ctx context.Context, taskURL string, f *workflow.Flow) error {
	// Host side: send the request.
	var req []byte
	var err error
	if h.mode == "carriers" {
		req, err = json.Marshal(workflow.RequestCarrier(f))
	} else {
		req, err = json.Marshal(f)
	}
	if err != nil {
		return errors.Trace(err)
	}

	// Task server side: decode, run, encode the reply.
	var remote workflow.Flow
	if h.mode == "carriers" {
		err = json.Unmarshal(req, workflow.RequestCarrier(&remote))
	} else {
		err = json.Unmarshal(req, &remote)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if err := h.Mux.ExecuteTask(ctx, taskURL, &remote); err != nil {
		return err // a task's error travels out of band
	}
	var reply []byte
	if h.mode == "carriers" {
		reply, err = json.Marshal(workflow.ResultCarrier(&remote))
	} else {
		reply, err = json.Marshal(&remote)
	}
	if err != nil {
		return errors.Trace(err)
	}
	if h.mode == "forged" {
		var wire map[string]json.RawMessage
		if err := json.Unmarshal(reply, &wire); err != nil {
			return errors.Trace(err)
		}
		wire["state"] = json.RawMessage(`{"route":"c","count":1000,"forged":true}`)
		wire["reducers"] = json.RawMessage(`{"count":"replace","log":"replace"}`)
		wire["attempt"] = json.RawMessage(`99`)
		wire["deferrals"] = json.RawMessage(`99`)
		wire["flowKey"] = json.RawMessage(`"forged"`)
		wire["stepKey"] = json.RawMessage(`"forged"`)
		wire["inBranch"] = json.RawMessage(`true`)
		if reply, err = json.Marshal(wire); err != nil {
			return errors.Trace(err)
		}
	}

	// Host side: read the reply onto the engine's carrier.
	if h.mode == "carriers" {
		return json.Unmarshal(reply, workflow.ResultCarrier(f))
	}
	return json.Unmarshal(reply, f)
}

// registerCarrierWire registers the workflow under test on mux.
func registerCarrierWire(mux *hosts.Mux) {
	p := carrierWirePrefix
	g := workflow.NewGraph("CarrierWire")
	for _, task := range []string{"Start", "B", "C", "G", "Each", "Join", "Ask"} {
		g.SetURL(task, p+"/"+task)
	}
	g.SetReducer("log", workflow.ReducerAppend)
	g.SetReducer("count", workflow.ReducerAdd)
	g.AddTransitionSwitch("Start", "B", `route == "b"`)
	g.AddTransitionSwitch("Start", "C", `route != "b"`)
	g.AddTransition("C", workflow.End)
	g.AddTransitionGoto("B", "G")
	g.AddTransition("B", "C")
	g.AddTransitionForEach("G", "Each", "items", "item")
	g.AddTransition("Each", "Join")
	g.SetFanIn("Join")
	g.AddTransition("Join", "Ask")
	g.AddTransition("Ask", workflow.End)
	mux.HandleGraph(p+"/g", g)

	child := workflow.NewGraph("CarrierWireChild")
	child.SetURL("Double", p+"/Double")
	child.AddTransition("Double", workflow.End)
	mux.HandleGraph(p+"/child", child)

	mux.HandleTask(p+"/Start", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("log", []any{"start"})
		f.Set("count", 1)
		f.SetString("route", "b")
		return f.Set("items", []any{1, 2, 3})
	})
	mux.HandleTask(p+"/B", func(ctx context.Context, f *workflow.Flow) error {
		if f.Attempt() < 2 {
			f.Set("log", []any{"retry"})
			f.Retry(time.Millisecond, 1, time.Millisecond, 0)
			return nil
		}
		f.Set("log", []any{"b"})
		f.Sleep(5 * time.Millisecond)
		f.Goto("G")
		return nil
	})
	mux.HandleTask(p+"/C", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("log", []any{"c"})
	})
	mux.HandleTask(p+"/G", func(ctx context.Context, f *workflow.Flow) error {
		var out struct {
			Doubled int `json:"doubled"`
		}
		yield, err := f.Subgraph(p+"/child", map[string]any{"n": 21}, &out)
		if yield || err != nil {
			return err
		}
		return f.Set("fromChild", out.Doubled)
	})
	mux.HandleTask(p+"/Double", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("doubled", 2*f.GetInt("n"))
	})
	mux.HandleTask(p+"/Each", func(ctx context.Context, f *workflow.Flow) error {
		f.Set("count", f.GetInt("item"))
		return f.Set("log", []any{f.GetInt("item")})
	})
	mux.HandleTask(p+"/Join", func(ctx context.Context, f *workflow.Flow) error {
		return f.Set("log", []any{"join"})
	})
	mux.HandleTask(p+"/Ask", func(ctx context.Context, f *workflow.Flow) error {
		var out struct {
			Answer string `json:"answer"`
		}
		yield, err := f.Interrupt(map[string]any{"question": "ship it?"}, &out)
		if yield || err != nil {
			return err
		}
		// Twice later, keeping the answer; the count is the engine's, whatever a reply says.
		if f.Deferrals() < 2 {
			f.Set("log", []any{"defer"})
			f.Defer(time.Millisecond, 1, time.Millisecond, 0)
			return nil
		}
		f.SetInt("deferrals", f.Deferrals())
		f.SetString("answer", out.Answer)
		return nil
	})
}

// runCarrierWire runs the workflow on host to completion, resuming its interrupt, and returns its final state.
func runCarrierWire(t *testing.T, name string, host engine.Host) (status string, state map[string]any) {
	t.Helper()
	assert := testarossa.For(t)
	ctx := context.Background()
	eng := engine.NewEngineUnderTest(t.Name() + "#" + name)
	defer eng.Shutdown(ctx)
	eng.SetHost(host)
	assert.Must(assert.NoError(eng.Startup(t.Context())))

	flowKey, err := eng.Create(ctx, carrierWirePrefix+"/g", nil, nil)
	if !assert.NoError(err) {
		return "", nil
	}
	out, err := enginetest.BoundedAwait(t, eng, flowKey)
	if !assert.NoError(err) || !assert.Equal(workflow.StatusInterrupted, out.Status, name) {
		return "", nil
	}
	assert.Equal("ship it?", stateVal(out.InterruptPayload, "question"), name)
	if !assert.NoError(eng.Resume(ctx, flowKey, map[string]any{"answer": "yes"})) {
		return "", nil
	}
	out, err = enginetest.BoundedAwait(t, eng, flowKey)
	if !assert.NoError(err) {
		return "", nil
	}
	assert.NoError(out.State.Parse(&state))
	return out.Status, state
}

func TestCarrierWireFlow(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	direct := hosts.NewMux()
	registerCarrierWire(direct)
	wantStatus, want := runCarrierWire(t, "direct", direct)
	if !assert.Equal(workflow.StatusCompleted, wantStatus) {
		return
	}
	// The reference outcome is the one every wire mode must reproduce; check it is the workflow's, not a degenerate one.
	assert.Equal([]any{"start", "retry", "retry", "b", 1.0, 2.0, 3.0, "join", "defer", "defer"}, want["log"])
	assert.Equal(2.0, want["deferrals"])
	assert.Equal(7.0, want["count"])
	assert.Equal(42.0, want["fromChild"])
	assert.Equal("yes", want["answer"])

	for _, mode := range []string{"full", "carriers", "forged"} {
		t.Run(mode, func(t *testing.T) {
			assert := testarossa.For(t)
			mux := hosts.NewMux()
			registerCarrierWire(mux)
			status, got := runCarrierWire(t, mode, &wireHost{Mux: mux, mode: mode})
			assert.Equal(wantStatus, status)
			assert.Equal(want, got)
		})
	}
}
