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
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/microbus-io/boolexp"
)

// fuzzReducers is the set a fuzz selector cycles through (includes "" = default replace and one
// unknown name to exercise the error path).
var fuzzReducers = []Reducer{
	ReducerReplace, ReducerAppend, ReducerAdd, ReducerMin, ReducerMax,
	ReducerUnion, ReducerMerge, ReducerAnd, ReducerOr, ReducerConcat,
	"", "bogus",
}

// FuzzStateMergeReduce asserts the state-merge core never panics and never leaks a null tombstone into
// materialized state, for arbitrary (state, changes, reducer) combinations. State.MergeReduce (accumulate)
// followed by DelNils (materialize) is what every step transition, fan-in, final-state computation, and
// Continue runs through.
func FuzzStateMergeReduce(f *testing.F) {
	f.Add([]byte(`{"a":1}`), []byte(`{"a":null}`), uint8(0))
	f.Add([]byte(`{"log":["x"]}`), []byte(`{"log":["y"]}`), uint8(1))
	f.Add([]byte(`{"n":1}`), []byte(`{"n":"NaN"}`), uint8(2))
	f.Add([]byte(`{}`), []byte(`{"k":{"x":1}}`), uint8(6))
	f.Add([]byte(`null`), []byte(`null`), uint8(0))
	f.Fuzz(func(t *testing.T, stateJSON, changesJSON []byte, pick uint8) {
		var base, incoming State
		if base.UnmarshalJSON(stateJSON) != nil {
			t.Skip()
		}
		if incoming.UnmarshalJSON(changesJSON) != nil {
			t.Skip()
		}
		// Wire every changed key to a reducer chosen by the selector.
		reducers := map[string]Reducer{}
		i := int(pick)
		for k := range incoming.All() {
			reducers[k] = fuzzReducers[i%len(fuzzReducers)]
			i++
		}
		if err := base.MergeReduce(incoming, reducers); err != nil {
			return // rejected inputs are fine; panics are not
		}
		base.DelNils()
		for k, v := range base.All() {
			if isCleared(v) {
				t.Fatalf("null tombstone leaked into merged state at key %q", k)
			}
		}
		// Materialized state must always be JSON-serializable (it is persisted verbatim).
		if _, err := json.Marshal(base); err != nil {
			t.Fatalf("merged state not marshalable: %v", err)
		}
	})
}

// FuzzReducerReduce asserts each reducer is total over valid-JSON operands: no panic, and on
// success the result is itself valid JSON (it is re-persisted into the changes/state columns). Reducers
// operate on DECODED values, exactly as State holds them, so the operands are unmarshaled before folding.
func FuzzReducerReduce(f *testing.F) {
	f.Add(uint8(1), []byte(`[1,2]`), []byte(`[3]`))
	f.Add(uint8(2), []byte(`1`), []byte(`2.5`))
	f.Add(uint8(6), []byte(`{"a":1}`), []byte(`{"b":2}`))
	f.Add(uint8(9), []byte(`"a"`), []byte(`"b"`))
	f.Add(uint8(7), []byte(`true`), []byte(`null`))
	f.Fuzz(func(t *testing.T, pick uint8, a, b []byte) {
		if !json.Valid(a) || !json.Valid(b) {
			t.Skip() // engine reducers only ever see values that round-tripped through JSON
		}
		var av, bv any
		_ = json.Unmarshal(a, &av)
		_ = json.Unmarshal(b, &bv)
		r := fuzzReducers[int(pick)%len(fuzzReducers)]
		out, err := r.Reduce(av, bv)
		if err != nil {
			return
		}
		raw, merr := json.Marshal(out)
		if merr != nil || !json.Valid(raw) {
			t.Fatalf("reducer %q produced non-JSON result %v from %s + %s", r, out, a, b)
		}
	})
}

// mermaidMetachars are the characters escapeMermaid must never let through raw: each can
// terminate a label or inject Mermaid/HTML syntax into a host-rendered diagram.
const mermaidMetachars = "\"'<>`[]{}()|"

// FuzzEscapeMermaid asserts the single shared escaper neutralizes every metacharacter for
// arbitrary author-controlled input, and that every '#' in the output starts one of the escaper's
// own character references (so pre-shaped input like "#quot;" cannot survive as a live reference
// of the attacker's choosing — it gets its '#' rewritten).
func FuzzEscapeMermaid(f *testing.F) {
	f.Add(`hello "world"`)
	f.Add(`a#quot;b`)
	f.Add("`<script>`")
	f.Add(`x]--> evil["y`)
	f.Add(`(((((`)
	f.Fuzz(func(t *testing.T, s string) {
		out := escapeMermaid(s)
		if strings.ContainsAny(out, mermaidMetachars) {
			t.Fatalf("metacharacter leaked: %q -> %q", s, out)
		}
		emitted := []string{"#35;", "#quot;", "#39;", "#lt;", "#gt;", "#96;", "#91;", "#93;", "#123;", "#125;", "#40;", "#41;", "#124;"}
		rest := out
		for {
			i := strings.IndexByte(rest, '#')
			if i < 0 {
				break
			}
			tail := rest[i:]
			matched := ""
			for _, ref := range emitted {
				if strings.HasPrefix(tail, ref) {
					matched = ref
					break
				}
			}
			if matched == "" {
				t.Fatalf("output %q contains '#' not starting an emitted reference (input %q)", out, s)
			}
			rest = tail[len(matched):]
		}
	})
}

// FuzzWhenExpression asserts the `when` expression engine (boolexp) is total: Validate and Eval
// never panic AND never hang on arbitrary expressions and state. Task authors write these
// expressions and the engine evaluates them on the hot transition path against live flow state, so
// a hang wedges a worker goroutine permanently (lease recovery cannot reclaim a goroutine that
// never returns). A watchdog converts a hang into a test failure so a regression (or replaying a
// saved crasher) fails in bounded time instead of hanging the whole suite.
//
// KNOWN FAILING INPUT: boolexp v1.1.1 infinite-loops on ")(" (a close paren before an open paren):
// evaluateBoolExp's paren-resolution loop sets parenStart but never finds a matching close, nets
// parenDepth to 0 (so the invalid-parenthesis guard misses it), and re-runs forever.
// This target will fail until boolexp is fixed; that failure is the point.
func FuzzWhenExpression(f *testing.F) {
	f.Add(`amount > 100 && status == "ok"`, []byte(`{"amount":200,"status":"ok"}`))
	f.Add(`x =~ "a+b*"`, []byte(`{"x":"aab"}`))
	f.Add(`!(`, []byte(`{}`))
	f.Add(`true`, []byte(`null`))
	f.Add(`a.b.c == 1`, []byte(`{"a":{"b":{"c":1}}}`))
	f.Fuzz(func(t *testing.T, expr string, stateJSON []byte) {
		var state map[string]any
		if json.Unmarshal(stateJSON, &state) != nil {
			t.Skip()
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = boolexp.Validate(expr)       // must not panic
			_, _ = boolexp.Eval(expr, state) // must not panic, even for expressions Validate rejects
		}()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("boolexp hung on expr %q (the %q infinite-loop class)", expr, ")(")
		}
	})
}

// FuzzGraphJSON asserts the graph wire format is total: any bytes either fail to unmarshal or
// yield a graph that Validate and re-marshal handle without panicking, and whose re-marshaled
// form unmarshals again. The engine unmarshals this JSON on every step dispatch (frozen graph),
// so the decoder must never trust its input.
func FuzzGraphJSON(f *testing.F) {
	g := NewGraph("Fuzz")
	g.SetEndpoint("A", "task/a")
	g.AddTransition("A", END)
	seed, _ := json.Marshal(g)
	f.Add(seed)
	f.Add([]byte(`{"entryPoint":"A","tasks":[{"name":"A"}],"transitions":[{"from":"A","to":"A","onError":true}]}`))
	f.Add([]byte(`{"tasks":null,"transitions":null}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var g Graph
		if err := json.Unmarshal(data, &g); err != nil {
			return
		}
		_ = g.Validate() // any verdict is fine; panics and hangs are not
		out, err := json.Marshal(&g)
		if err != nil {
			t.Fatalf("re-marshal failed for graph from %q: %v", data, err)
		}
		var g2 Graph
		if err := json.Unmarshal(out, &g2); err != nil {
			t.Fatalf("re-unmarshal failed: %v (json %s)", err, out)
		}
	})
}

// FuzzFlowJSON asserts the Flow wire format (used when a task executes across a transport) is
// total under unmarshal/marshal.
func FuzzFlowJSON(f *testing.F) {
	fl := NewFlow()
	fl.SetString("k", "v")
	seed, _ := json.Marshal(fl)
	f.Add(seed)
	f.Add([]byte(`{"state":{"a":null},"changes":{"a":null},"attempt":-1}`))
	f.Add([]byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var fl Flow
		if err := json.Unmarshal(data, &fl); err != nil {
			return
		}
		if _, err := json.Marshal(&fl); err != nil {
			t.Fatalf("re-marshal failed: %v", err)
		}
		// Exercise the accessors most tasks call, on adversarial state.
		_ = fl.GetString("a")
		_ = fl.GetInt("a")
		_ = fl.Has("a")
		_ = fl.Snapshot()
	})
}
