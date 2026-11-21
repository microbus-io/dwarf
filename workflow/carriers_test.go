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

package workflow

import (
	"bytes"
	"encoding/json"
	"flag"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/errors"
	"github.com/microbus-io/testarossa"
)

// The golden files under testdata/carrier are the carrier's conformance fixtures: what each direction of the wire
// format looks like for each control signal, the bodies a decoder must refuse, and how each reducer folds. They are
// written from this package's own behavior and read back by these tests, so a change to the wire or to a reducer
// shows up as a diff to the files; an implementation of the carrier in another language reads the same files.
// Regenerate them with: go test ./workflow -run 'TestCarrierGolden|TestReducerGolden' -update
var updateGolden = flag.Bool("update", false, "rewrite the golden files under testdata/carrier")

const goldenDir = "testdata/carrier"

// checkGolden compares got, indented, with the named golden file, or rewrites the file under -update.
func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	assert := testarossa.For(t)
	var pretty bytes.Buffer
	if !assert.NoError(json.Indent(&pretty, got, "", "  ")) {
		return
	}
	pretty.WriteByte('\n')
	path := filepath.Join(goldenDir, name)
	if *updateGolden {
		assert.NoError(os.MkdirAll(goldenDir, 0o755))
		assert.NoError(os.WriteFile(path, pretty.Bytes(), 0o644))
		return
	}
	want, err := os.ReadFile(path)
	if !assert.NoError(err, "missing golden file %s: run with -update", path) {
		return
	}
	assert.Equal(string(want), pretty.String(), "%s differs from what this build produces", path)
}

// dispatchedFlow builds the carrier the engine hands a task: the state with an earlier run's changes folded in,
// those changes, the reducers, the keys, the timestamps and the attempt.
func dispatchedFlow(t *testing.T) *RawFlow {
	t.Helper()
	f := NewRawFlow()
	state, err := NewState(map[string]any{"name": "Ada", "log": []any{"a"}, "total": 1})
	if err != nil {
		t.Fatal(err)
	}
	reducers := map[string]Reducer{"log": ReducerAppend, "total": ReducerAdd}
	prior := NewChanges()
	if err := prior.Set("log", []any{"b"}, reducers); err != nil {
		t.Fatal(err)
	}
	if err := state.Apply(prior, reducers); err != nil {
		t.Fatal(err)
	}
	f.SetRawState(state)
	f.SetReducers(reducers)
	f.SetRawChanges(prior)
	f.SetAttempt(1)
	f.SetFlowKey("1-480000-7-AbCdEfGh")
	f.SetStepKey("1-480000-7-42-IjKlMnOp")
	at := time.Date(2026, 11, 7, 12, 0, 0, 0, time.UTC)
	f.SetCreatedAt(at)
	f.SetUpdatedAt(at.Add(time.Second))
	f.SetStepCreatedAt(at.Add(500 * time.Millisecond))
	return f
}

// TestCarrierGolden pins each direction of the wire format, for a task that writes and for each control signal.
func TestCarrierGolden(t *testing.T) {
	cases := []struct {
		name string
		task func(f *RawFlow) error
	}{
		{"writes", func(f *RawFlow) error {
			if err := f.Set("greeting", "hello"); err != nil {
				return err
			}
			if err := f.Set("log", []any{"c"}); err != nil {
				return err
			}
			return f.Set("total", 2)
		}},
		{"overwrite_and_delete", func(f *RawFlow) error {
			f.Del("name")
			return f.Overwrite("log", []any{})
		}},
		{"goto", func(f *RawFlow) error { f.Goto("Review"); return nil }},
		{"retry", func(f *RawFlow) error {
			f.Retry(250*time.Millisecond, 2.0, 5*time.Second, 0)
			return nil
		}},
		{"defer", func(f *RawFlow) error {
			f.SetDeferrals(2) // the engine's count of the step's earlier deferrals
			f.Defer(250*time.Millisecond, 2.0, 5*time.Second, 0)
			return nil
		}},
		{"cancelled", func(f *RawFlow) error {
			// The engine runs the step after its flow was cancelled: a Defer returns the cancellation and arms nothing.
			f.SetCancellation(errors.New("flow cancelled: maintenance", "dwarf_type", "cancelled"))
			_, err := f.Defer(250*time.Millisecond, 2.0, 5*time.Second, 0)
			return f.Set("stopping", IsCancelled(err))
		}},
		{"sleep", func(f *RawFlow) error {
			f.Sleep(1500*time.Millisecond + 700*time.Microsecond) // whole milliseconds on the wire: 1500
			return nil
		}},
		{"interrupt", func(f *RawFlow) error {
			_, err := f.Interrupt(map[string]any{"question": "approve?"}, nil)
			return err
		}},
		{"subgraph", func(f *RawFlow) error {
			_, err := f.Subgraph("child.workflow", map[string]any{"id": 7}, nil)
			return err
		}},
		{"resumed", func(f *RawFlow) error {
			f.SetInterruptResolution(mustState(t, map[string]any{"answer": "yes"}))
			var out map[string]any
			if _, err := f.Interrupt(nil, &out); err != nil {
				return err
			}
			return f.Set("answer", out["answer"])
		}},
		{"subgraph_returned", func(f *RawFlow) error {
			f.SetSubgraphResolution(mustState(t, map[string]any{"score": 9}), errors.New("child failed", http.StatusServiceUnavailable, "quota", "daily"))
			_, err := f.Subgraph("child.workflow", nil, nil)
			return f.Set("childError", errors.StatusCode(err))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert := testarossa.For(t)
			f := dispatchedFlow(t)
			if !assert.NoError(tc.task(f)) {
				return
			}
			full, err := json.Marshal(&f.Flow)
			if assert.NoError(err) {
				checkGolden(t, tc.name+".full.json", full)
			}
			req, err := json.Marshal(RequestCarrier(&f.Flow))
			if assert.NoError(err) {
				checkGolden(t, tc.name+".request.json", req)
			}
			res, err := json.Marshal(ResultCarrier(&f.Flow))
			if assert.NoError(err) {
				checkGolden(t, tc.name+".result.json", res)
			}
		})
	}
}

// TestCarrierGolden_Decode decodes every golden body with the decoder for its form and re-encodes it with the
// matching encoder: the bytes must come back unchanged, so a field a decoder drops fails here.
func TestCarrierGolden_Decode(t *testing.T) {
	assert := testarossa.For(t)
	forms := map[string]struct {
		decode func(f *Flow, data []byte) error
		encode func(f *Flow) ([]byte, error)
	}{
		".full.json": {
			func(f *Flow, data []byte) error { return json.Unmarshal(data, f) },
			func(f *Flow) ([]byte, error) { return json.Marshal(f) },
		},
		".request.json": {
			func(f *Flow, data []byte) error { return json.Unmarshal(data, RequestCarrier(f)) },
			func(f *Flow) ([]byte, error) { return json.Marshal(RequestCarrier(f)) },
		},
		".result.json": {
			func(f *Flow, data []byte) error { return json.Unmarshal(data, ResultCarrier(f)) },
			func(f *Flow) ([]byte, error) { return json.Marshal(ResultCarrier(f)) },
		},
	}
	paths, err := filepath.Glob(filepath.Join(goldenDir, "*.*.json"))
	if !assert.NoError(err) || !assert.NotZero(len(paths)) {
		return
	}
	for _, path := range paths {
		form, ok := forms[filepath.Ext(strings.TrimSuffix(path, ".json"))+".json"]
		if !ok {
			continue
		}
		want, err := os.ReadFile(path)
		if !assert.NoError(err) {
			continue
		}
		var f Flow
		if !assert.NoError(form.decode(&f, want), path) {
			continue
		}
		got, err := form.encode(&f)
		if !assert.NoError(err, path) {
			continue
		}
		var pretty bytes.Buffer
		assert.NoError(json.Indent(&pretty, got, "", "  "))
		pretty.WriteByte('\n')
		assert.Equal(string(want), pretty.String(), "%s does not survive a decode and re-encode", path)
	}
}

func mustState(t *testing.T, v any) State {
	t.Helper()
	s, err := NewState(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// refusedBody is one entry of refused.json: a body every decoder must refuse, and the status code it refuses with.
type refusedBody struct {
	Name   string          `json:"name"`
	Body   json.RawMessage `json:"body"`
	Status int             `json:"status"`
}

// TestCarrierGolden_Refused pins the bodies a decoder refuses: no version, a version that is not a positive
// integer, and one newer than this build reads. Each decoder - the Flow's own and both carriers - refuses each.
func TestCarrierGolden_Refused(t *testing.T) {
	assert := testarossa.For(t)
	table := []refusedBody{
		{"no_version", json.RawMessage(`{"state":{"a":1}}`), http.StatusBadRequest},
		{"version_zero", json.RawMessage(`{"v":0}`), http.StatusBadRequest},
		{"version_negative", json.RawMessage(`{"v":-1}`), http.StatusBadRequest},
		{"version_string", json.RawMessage(`{"v":"1"}`), http.StatusBadRequest},
		{"version_fraction", json.RawMessage(`{"v":1.5}`), http.StatusBadRequest},
		{"version_newer", json.RawMessage(`{"v":2}`), http.StatusNotImplemented},
	}
	data, err := json.Marshal(table)
	if !assert.NoError(err) {
		return
	}
	checkGolden(t, "refused.json", data)

	var golden []refusedBody
	raw, err := os.ReadFile(filepath.Join(goldenDir, "refused.json"))
	if !assert.NoError(err) || !assert.NoError(json.Unmarshal(raw, &golden)) {
		return
	}
	for _, r := range golden {
		decoders := map[string]func([]byte) error{
			"flow":    func(b []byte) error { var f Flow; return json.Unmarshal(b, &f) },
			"request": func(b []byte) error { return json.Unmarshal(b, RequestCarrier(NewFlow())) },
			"result":  func(b []byte) error { return json.Unmarshal(b, ResultCarrier(NewFlow())) },
		}
		for name, decode := range decoders {
			err := decode(r.Body)
			if assert.Error(err, "%s decoder accepted %s", name, r.Name) {
				assert.Equal(r.Status, errors.StatusCode(err), "%s decoder on %s: %v", name, r.Name, err)
			}
		}
	}
	// The newer-version refusal names both versions, so whoever reads it knows which side to upgrade.
	err = json.Unmarshal([]byte(`{"v":2}`), ResultCarrier(NewFlow()))
	if assert.Error(err) {
		assert.True(strings.Contains(err.Error(), "v2") && strings.Contains(err.Error(), "v1"), "got %q", err.Error())
	}
}

// reducerCase is one entry of reducers.json: a base value, contributions folded onto it in order, and the result.
type reducerCase struct {
	Reducer       Reducer           `json:"reducer"`
	Base          json.RawMessage   `json:"base"`
	Contributions []json.RawMessage `json:"contributions"`
	Result        json.RawMessage   `json:"result"`
}

// TestReducerGolden pins how each reducer folds contributions onto a base, as a carrier implementation in another
// language must fold a task's own writes to read them back.
func TestReducerGolden(t *testing.T) {
	assert := testarossa.For(t)
	inputs := []struct {
		reducer       Reducer
		base          string
		contributions []string
	}{
		{ReducerReplace, `"old"`, []string{`"mid"`, `"new"`}},
		{ReducerAppend, `["a"]`, []string{`["b"]`, `["c","d"]`}},
		{ReducerAdd, `1`, []string{`2`, `0.5`, `-4`}},
		{ReducerAdd, `9007199254740993`, []string{`1`}}, // integers stay exact past 2^53
		{ReducerMin, `5`, []string{`7`, `3`}},
		{ReducerMax, `5`, []string{`7`, `3`}},
		{ReducerUnion, `[1,"a"]`, []string{`["a",2]`, `[1.0,3]`}}, // numbers compare by value: 1.0 is 1
		{ReducerMerge, `{"a":1,"b":1}`, []string{`{"b":2}`, `{"c":3}`}},
		{ReducerAnd, `true`, []string{`true`, `false`}},
		{ReducerOr, `false`, []string{`false`, `true`}},
		{ReducerConcat, `"ab"`, []string{`"cd"`, `"é\n"`}},
	}
	var table []reducerCase
	for _, in := range inputs {
		reducers := map[string]Reducer{"x": in.reducer}
		state := mustState(t, map[string]any{})
		state.SetJSON("x", json.RawMessage(in.base))
		rc := reducerCase{Reducer: in.reducer, Base: json.RawMessage(in.base)}
		for _, c := range in.contributions {
			ch := NewChanges()
			if !assert.NoError(ch.Set("x", json.RawMessage(c), reducers), "%s %s", in.reducer, c) {
				return
			}
			if !assert.NoError(state.Apply(ch, reducers), "%s %s", in.reducer, c) {
				return
			}
			rc.Contributions = append(rc.Contributions, json.RawMessage(c))
		}
		rc.Result = state.GetJSON("x")
		table = append(table, rc)
	}
	data, err := json.Marshal(table)
	if assert.NoError(err) {
		checkGolden(t, "reducers.json", data)
	}
}

// TestCarriers_EachDirectionAppliesOnlyItsFields pins what each decoder takes from a full body: the request
// carrier takes the request's fields and clears any control signal, the result carrier takes the changes and
// signals and leaves the rest of the host's Flow alone, and the Flow's own decoder takes everything.
func TestCarriers_EachDirectionAppliesOnlyItsFields(t *testing.T) {
	assert := testarossa.For(t)

	sent := dispatchedFlow(t)
	assert.NoError(sent.Set("greeting", "hello"))
	sent.Goto("Review")
	sent.Defer(time.Second, 1, 0, 0)
	sent.SetDeferrals(3)
	body, err := json.Marshal(&sent.Flow)
	if !assert.NoError(err) {
		return
	}

	// A task server reading a body that carries a signal: the request carrier drops it.
	var task Flow
	task.Goto("Stale")
	if assert.NoError(json.Unmarshal(body, RequestCarrier(&task))) {
		assert.Equal("Ada", task.GetString("name"))
		assert.Equal("hello", task.GetString("greeting"), "the earlier run's changes are in the view")
		assert.Equal("", task.gotoNext, "a request carries no control signals")
		assert.False(task.deferred, "a request carries no control signals")
		assert.Equal(3, task.Deferrals())
		assert.Equal(1, task.carrierVersion)
	}

	// The host reading a reply whose state is not the host's: only the changes and signals land.
	host := dispatchedFlow(t)
	reply := []byte(`{"v":1,"state":{"name":"Mallory"},"reducers":{"name":"add"},"attempt":99,"deferrals":99,` +
		`"flowKey":"forged","changes":{"greeting":"hi"},"goto":"Review","sleepMillis":1500,"defer":true}`)
	if assert.NoError(json.Unmarshal(reply, ResultCarrier(&host.Flow))) {
		assert.Equal("Ada", host.GetString("name"), "the state is the host's own")
		assert.Equal(ReducerAppend, host.reducers["log"], "the reducers are the host's own")
		assert.Equal(1, host.attempt)
		assert.Equal(0, host.deferrals)
		assert.Equal("1-480000-7-AbCdEfGh", host.flowKey)
		_, _, _, deferred := host.DeferRequested()
		assert.True(deferred)
		assert.Equal("Review", host.GotoRequested())
		assert.Equal(1500*time.Millisecond, host.SleepRequested())
		assert.True(host.RawChanges().Mentions("greeting"))
		assert.Equal(0, host.carrierVersion, "the host's Flow keeps encoding in the newest version")
	}

	// The Flow's own decoder takes every field of the same reply.
	var all Flow
	if assert.NoError(json.Unmarshal(reply, &all)) {
		assert.Equal("Mallory", all.GetString("name"))
		assert.Equal(99, all.attempt)
		assert.Equal(99, all.deferrals)
		assert.True(all.deferred)
		assert.Equal("Review", all.gotoNext)
	}
}

// TestCarriers_EncodeInTheDecodedVersion pins that a Flow encodes in the version it was decoded with, through
// every encoder, and in CarrierVersion when it was never decoded.
func TestCarriers_EncodeInTheDecodedVersion(t *testing.T) {
	assert := testarossa.For(t)

	versionOf := func(data []byte) int {
		var w struct {
			V int `json:"v"`
		}
		assert.NoError(json.Unmarshal(data, &w))
		return w.V
	}
	encoders := map[string]func(f *Flow) ([]byte, error){
		"flow":    func(f *Flow) ([]byte, error) { return json.Marshal(f) },
		"request": func(f *Flow) ([]byte, error) { return json.Marshal(RequestCarrier(f)) },
		"result":  func(f *Flow) ([]byte, error) { return json.Marshal(ResultCarrier(f)) },
	}
	for name, decode := range map[string]func(f *Flow) error{
		"flow":    func(f *Flow) error { return json.Unmarshal([]byte(`{"v":1}`), f) },
		"request": func(f *Flow) error { return json.Unmarshal([]byte(`{"v":1}`), RequestCarrier(f)) },
	} {
		var f Flow
		if assert.NoError(decode(&f), name) {
			assert.Equal(1, f.carrierVersion, "%s decoder did not record the version it read", name)
		}
	}
	for name, encode := range encoders {
		fresh := NewFlow()
		data, err := encode(fresh)
		if assert.NoError(err, name) {
			assert.Equal(CarrierVersion, versionOf(data), "%s: a Flow never decoded encodes in the newest version", name)
		}
		// A Flow decoded at an older version repeats it. Only v1 exists, so the decoded version is set directly.
		decoded := NewFlow()
		decoded.carrierVersion = 1
		data, err = encode(decoded)
		if assert.NoError(err, name) {
			assert.Equal(1, versionOf(data), name)
		}
	}
}

// TestCarriers_DurationsAreWholeMilliseconds pins the durations on the wire: truncated to whole milliseconds on
// the way out, as the engine schedules, and saturating at the Duration's range on the way in.
func TestCarriers_DurationsAreWholeMilliseconds(t *testing.T) {
	assert := testarossa.For(t)

	f := NewFlow()
	f.Sleep(2*time.Second + 999*time.Microsecond)                      // truncated, as the engine truncates it
	f.Retry(time.Millisecond+time.Microsecond, 1.5, 90*time.Second, 0) // rounded up
	data, err := json.Marshal(ResultCarrier(f))
	if !assert.NoError(err) {
		return
	}
	var w map[string]any
	assert.NoError(json.Unmarshal(data, &w))
	assert.Equal(float64(2000), w["sleepMillis"])
	assert.Equal(float64(2), w["backoffInitialDelayMillis"])
	assert.Equal(float64(90000), w["backoffMaxDelayMillis"])
	assert.Equal(1.5, w["backoffDelayMultiplier"])

	// A backoff under 1ms arrives as 1ms, not 0, so its multiplier still grows it.
	sub := NewFlow()
	sub.Retry(500*time.Microsecond, 2, time.Minute, 0)
	data, err = json.Marshal(ResultCarrier(sub))
	if assert.NoError(err) {
		var got RawFlow
		if assert.NoError(json.Unmarshal(data, ResultCarrier(&got.Flow))) {
			assert.Equal(time.Millisecond, got.backoffInitialDelay)
			assert.Equal(32*time.Millisecond, got.RetryDelay(5))
		}
	}

	host := NewRawFlow()
	reply := []byte(`{"v":1,"retry":true,"sleepMillis":-9223372036854775807,"backoffInitialDelayMillis":1,` +
		`"backoffMaxDelayMillis":9223372036854775807}`)
	if assert.NoError(json.Unmarshal(reply, ResultCarrier(&host.Flow))) {
		assert.Equal(time.Duration(math.MinInt64), host.sleepDuration)
		_, _, maxDelay, ok := host.RetryRequested()
		assert.True(ok)
		assert.Equal(time.Duration(math.MaxInt64), maxDelay)
	}
}
