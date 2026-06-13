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
	"testing"

	"github.com/microbus-io/testarossa"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestGraph_BuilderAndMarshal(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("create-order")
	g.AddTransitionWhen("order.service/validate", "payment.service/charge", "valid == true")
	g.AddTransitionWhen("order.service/validate", "order.service/reject", "valid != true")
	g.SetReducer("messages", ReducerAppend)

	assert.Equal("create-order", g.Name())
	assert.Equal("order.service/validate", g.EntryPoint())
	assert.Equal(3, len(g.Nodes()))

	data, err := json.Marshal(g)
	assert.NoError(err)

	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)

	assert.Equal("create-order", restored.Name())
	assert.Equal("order.service/validate", restored.EntryPoint())
	assert.Equal(3, len(restored.Nodes()))
	assert.Equal(2, len(restored.Transitions()))
	assert.Equal("valid == true", restored.Transitions()[0].When)
	assert.Equal(ReducerAppend, restored.reducers["messages"])
}

func TestGraph_EmptyReducers(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("simple")
	g.AddTransition("svc/start", "svc/end")

	data, err := json.Marshal(g)
	assert.NoError(err)

	// Reducers should be omitted when empty
	var raw map[string]json.RawMessage
	err = json.Unmarshal(data, &raw)
	assert.NoError(err)
	_, ok := raw["reducers"]
	assert.False(ok)
}

func TestGraph_DefaultEntryPoint(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTask("svc/first", "svc/first")
	g.AddTask("svc/second", "svc/second")

	assert.Equal("svc/first", g.EntryPoint())
}

func TestGraph_ExplicitEntryPoint(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTask("svc/first", "svc/first")
	g.AddTask("svc/second", "svc/second")
	g.SetEntryPoint("svc/second")

	assert.Equal("svc/second", g.EntryPoint())
}

func TestGraph_AutoRegisterTasks(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransitionWhen("svc/b", "svc/c", "done == true")

	tasks := g.Nodes()
	assert.Equal(3, len(tasks))
	assert.Equal("svc/a", tasks[0].Name)
	assert.Equal("svc/b", tasks[1].Name)
	assert.Equal("svc/c", tasks[2].Name)
}

func TestGraph_DuplicateTaskIgnored(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTask("svc/a", "svc/a")
	g.AddTask("svc/a", "svc/a")
	g.AddTransition("svc/a", "svc/b")

	assert.Equal(2, len(g.Nodes()))
}

func TestGraph_Validate(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Valid graph
	g := NewGraph("test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransition("svc/b", END)
	assert.NoError(g.Validate())

	// Empty name
	g2 := NewGraph("")
	g2.AddTask("svc/a", "svc/a")
	assert.Error(g2.Validate())

	// No tasks
	g3 := NewGraph("test")
	assert.Error(g3.Validate())

	// Entry point not in task list
	g4 := NewGraph("test")
	g4.AddTask("svc/a", "svc/a")
	g4.SetEntryPoint("svc/missing")
	assert.Error(g4.Validate())

	// Unreachable task
	g5 := NewGraph("test")
	g5.AddTransition("svc/a", "svc/b")
	g5.AddTask("svc/c", "svc/c")
	assert.Error(g5.Validate())

	// Reachable via goto
	g6 := NewGraph("test")
	g6.AddTransition("svc/a", "svc/b")
	g6.AddTransition("svc/b", END)
	g6.AddTransitionGoto("svc/a", "svc/c")
	g6.AddTransition("svc/c", END)
	assert.NoError(g6.Validate())

	// No END transition
	g7 := NewGraph("test")
	g7.AddTransition("svc/a", "svc/b")
	g7.AddTransition("svc/b", "svc/a")
	assert.Error(g7.Validate())
}

func TestGraph_END(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransitionGoto("svc/b", END)
	g.AddTransition("svc/b", "svc/c")
	g.AddTransition("svc/c", END)

	// END should not appear in the task list
	tasks := g.Nodes()
	assert.Equal(3, len(tasks))
	for _, task := range tasks {
		assert.NotEqual(END, task.Name)
	}

	// Graph should validate successfully
	assert.NoError(g.Validate())

	// END should appear in JSON transitions
	data, err := json.Marshal(g)
	assert.NoError(err)
	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)
	assert.Equal(4, len(restored.Transitions()))
}

func TestGraph_Mermaid(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("create-order")
	g.AddTransitionWhen("order.service/validate", "payment.service/charge", "valid == true")
	g.AddTransitionWhen("order.service/validate", "order.service/reject", "valid != true")
	g.AddTransition("payment.service/charge", END)
	g.AddTransition("order.service/reject", END)

	mmd := must(NewGraphRenderer(g).Render())

	assert.Contains(mmd, "graph LR")
	assert.Contains(mmd, "_start(( ))")
	assert.Contains(mmd, "_end(( ))")
	assert.Contains(mmd, `"valid == true"`)
	assert.Contains(mmd, `_when{"when"}`)
	assert.Contains(mmd, "order.service/validate")
	assert.Contains(mmd, "payment.service/charge")
}

func TestGraph_GotoTransition(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransition("svc/b", END)
	g.AddTransitionGoto("svc/a", "svc/c")
	g.AddTransition("svc/c", END)

	transitions := g.Transitions()
	assert.Equal(4, len(transitions))
	assert.False(transitions[0].WithGoto) // svc/a -> svc/b
	assert.False(transitions[1].WithGoto) // svc/b -> END
	assert.True(transitions[2].WithGoto)  // svc/a -> svc/c (goto)
	assert.False(transitions[3].WithGoto) // svc/c -> END

	// Goto transitions should have a "goto" label in Mermaid
	mmd := must(NewGraphRenderer(g).Render())
	assert.Contains(mmd, `"goto"`)

	// Should validate successfully
	assert.NoError(g.Validate())

	// Should round-trip through JSON
	data, err := json.Marshal(g)
	assert.NoError(err)
	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)
	assert.True(restored.Transitions()[2].WithGoto)
}

func TestGraph_TransitionNoWhen(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("a", "b")

	data, err := json.Marshal(g)
	assert.NoError(err)

	// When should be omitted in JSON
	var raw struct {
		Transitions []map[string]json.RawMessage `json:"transitions"`
	}
	err = json.Unmarshal(data, &raw)
	assert.NoError(err)
	assert.Equal(1, len(raw.Transitions))
	_, ok := raw.Transitions[0]["when"]
	assert.False(ok)
}

func TestGraph_ValidateWhenExpression(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Valid expression
	g1 := NewGraph("test")
	g1.AddTransitionWhen("svc/a", "svc/b", "valid == true")
	g1.AddTransitionWhen("svc/a", "svc/c", "score > 5 && !guest")
	g1.AddTransition("svc/b", "svc/join")
	g1.AddTransition("svc/c", "svc/join")
	g1.AddTransition("svc/join", END)
	g1.SetFanIn("svc/join")
	assert.NoError(g1.Validate())

	// Invalid expression
	g2 := NewGraph("test")
	g2.AddTransitionWhen("svc/a", "svc/b", "(((")
	g2.AddTransition("svc/b", END)
	err := g2.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "invalid 'when' expression")
}

func TestGraph_AddTransitionOnTimeout(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransition("svc/b", END)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", END)
	g.AddTransitionOnTimeout("svc/a", "svc/timeoutHandler")
	g.AddTransition("svc/timeoutHandler", END)

	transitions := g.Transitions()
	assert.Equal(6, len(transitions))

	// AddTransitionOnError: OnError=true, StatusCode=0
	assert.True(transitions[2].OnError)
	assert.Equal(0, transitions[2].StatusCode)

	// AddTransitionOnTimeout: OnError=true, StatusCode=408
	assert.True(transitions[4].OnError)
	assert.Equal(408, transitions[4].StatusCode)

	assert.NoError(g.Validate())
}

func TestGraph_StatusCodeWithoutOnErrorRejected(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTask("svc/a", "svc/a")
	g.AddTask("svc/b", "svc/b")
	// Manually craft a malformed transition that the public API would never produce.
	g.transitions = append(g.transitions, Transition{From: "svc/a", To: "svc/b", StatusCode: 408})
	g.AddTransition("svc/b", END)

	err := g.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "statusCode without onError")
	}
}

func TestGraph_OnErrorAndOnTimeoutJSONRoundTrip(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("svc/a", END)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", END)
	g.AddTransitionOnTimeout("svc/a", "svc/timeoutHandler")
	g.AddTransition("svc/timeoutHandler", END)

	data, err := json.Marshal(g)
	assert.NoError(err)

	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)
	assert.Equal(5, len(restored.Transitions()))

	// Find the timeout transition by status code.
	var foundTimeout bool
	for _, tr := range restored.Transitions() {
		if tr.StatusCode == 408 {
			assert.True(tr.OnError)
			assert.Equal("svc/timeoutHandler", tr.To)
			foundTimeout = true
		}
	}
	assert.True(foundTimeout)
}

func TestGraph_MermaidForEachShape(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransitionForEach("svc/start", "svc/worker", "items", "item")
	g.AddTransition("svc/worker", END)

	mmd := must(NewGraphRenderer(g).Render())
	// forEach is marked with a "for each" edge label, the same convention as onError/goto.
	assert.Contains(mmd, `t0 -->|"for each"| t1`)
	// No enclosing box and no box style line; the branch is not wrapped.
	assert.NotContains(mmd, `subgraph fo_`)
	assert.NotContains(mmd, `style fo_`)
	// The forEach target is a standard rectangle.
	assert.Contains(mmd, `t1["svc/worker"]:::task`)
}

func TestGraph_MermaidFanInShape(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("a", "b")
	g.AddTransition("a", "c")
	g.AddTransition("b", "join")
	g.AddTransition("c", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	mmd := must(NewGraphRenderer(g).Render())
	// Fan-in nodes are standard rectangles; no special shape.
	assert.Contains(mmd, `t3["join"]:::task`)
	assert.NotContains(mmd, `shape: trap-t`)
	// Static When-style fan-out has no enclosing scope block, so edges into the
	// fan-in node do not get a "fan in" label.
	assert.NotContains(mmd, `"fan in"`)
}

func TestGraph_MermaidForEachFanInLabel(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransitionForEach("svc/start", "svc/worker", "items", "item")
	g.AddTransition("svc/worker", "svc/join")
	g.AddTransition("svc/join", END)
	g.SetFanIn("svc/join")
	assert.NoError(g.Validate())

	mmd := must(NewGraphRenderer(g).Render())
	// Only the forEach transition is labeled; the edge from the branch into the fan-in
	// reduce circle is a plain edge.
	assert.Contains(mmd, `t0 -->|"for each"| t1`)
	assert.Contains(mmd, `t1 --> t2_reduce`)
	// The fan-in node itself stays a standard rectangle.
	assert.Contains(mmd, `t2["svc/join"]:::task`)
}

func TestGraph_MermaidNestedForEachLabels(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransitionForEach("svc/outer", "svc/inner", "tenants", "tenant")
	g.AddTransitionForEach("svc/inner", "svc/leaf", "docs", "doc")
	g.AddTransition("svc/leaf", "svc/innerJoin")
	g.AddTransition("svc/innerJoin", "svc/outerJoin")
	g.AddTransition("svc/outerJoin", END)
	g.SetFanIn("svc/innerJoin")
	g.SetFanIn("svc/outerJoin")
	assert.NoError(g.Validate())

	mmd := must(NewGraphRenderer(g).Render())
	// Each forEach transition gets its own "for each" edge label; no nested boxes.
	assert.Contains(mmd, `t0 -->|"for each"| t1`)
	assert.Contains(mmd, `t1 -->|"for each"| t2`)
	assert.NotContains(mmd, `subgraph fo_`)
}

func TestGraph_MermaidLabelsOnErrorAndOnTimeout(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("svc/a", END)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", END)
	g.AddTransitionOnTimeout("svc/a", "svc/timeoutHandler")
	g.AddTransition("svc/timeoutHandler", END)

	mmd := must(NewGraphRenderer(g).Render())
	assert.Contains(mmd, `"onError"`)
	assert.Contains(mmd, `"onTimeout"`)
}

func TestGraph_SelfLoopOnErrorRejected(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("svc/a", END)
	g.AddTransitionOnError("svc/a", "svc/a")

	err := g.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "to itself")
		assert.Contains(err.Error(), "flow.Retry")
	}
}

func TestGraph_SelfLoopOnTimeoutRejected(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("test")
	g.AddTransition("svc/a", END)
	g.AddTransitionOnTimeout("svc/a", "svc/a")

	err := g.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "to itself")
	}
}

func TestGraph_GotoSelfLoopAllowed(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// A goto-driven self-loop is not restricted by the no-error-self-loop rule.
	// (A normal-edge self-loop wouldn't validate under the lineage stack rules anyway,
	// since the source becomes a fan-out source whose only fan-in is itself.)
	g := NewGraph("test")
	g.AddTransitionGoto("svc/a", "svc/a")
	g.AddTransition("svc/a", END)
	assert.NoError(g.Validate())
}

// Lineage validator tests.

func TestLineage_SequentialNoFanOut(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// SetFanIn opts the graph into the lineage validator. With no fan-out, the validator
	// has nothing to check beyond the structural rules; the FanIn marker on a sequentially
	// reached node is ill-formed (no scope to pop) and must be rejected.
	g := NewGraph("seq")
	g.AddTransition("a", "b")
	g.AddTransition("b", END)
	g.SetFanIn("b")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "no fan-out frame to pop")
}

func TestLineage_SimpleFanOutFanIn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("simple-fanin")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
	assert.Equal("join", g.FanInFor("s"))
}

func TestLineage_NestedFanOutFanIn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("nested")
	g.AddTransition("s", "outer1")
	g.AddTransition("s", "outer2")
	// outer1 has its own inner fan-out
	g.AddTransition("outer1", "inner1")
	g.AddTransition("outer1", "inner2")
	g.AddTransition("inner1", "innerJoin")
	g.AddTransition("inner2", "innerJoin")
	g.AddTransition("innerJoin", "outerJoin")
	// outer2 goes straight to outerJoin
	g.AddTransition("outer2", "outerJoin")
	g.AddTransition("outerJoin", END)
	g.SetFanIn("innerJoin")
	g.SetFanIn("outerJoin")
	assert.NoError(g.Validate())
	assert.Equal("innerJoin", g.FanInFor("outer1"))
	assert.Equal("outerJoin", g.FanInFor("s"))
}

func TestLineage_ForEachThenFanIn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("foreach-fanin")
	g.AddTransitionForEach("s", "a", "items", "item")
	g.AddTransition("a", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
	assert.Equal("join", g.FanInFor("s"))
}

func TestLineage_ConditionalWhenFanIn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("when-fanin")
	g.AddTransitionWhen("s", "a", "x > 0")
	g.AddTransitionWhen("s", "b", "x <= 0")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
	assert.Equal("join", g.FanInFor("s"))
}

func TestLineage_AliasedNodesInDifferentScopes(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// The same task URL is registered under two distinct names so that one copy lives
	// inside a fan-out scope (per element) and a second copy lives at the outer scope.
	g := NewGraph("alias")
	g.AddTask("s", "host/s")
	g.AddTask("inner", "host/work") // inside fan-out
	g.AddTask("outer", "host/work") // outside fan-out
	g.AddTask("join", "host/join")
	g.AddTransitionForEach("s", "inner", "items", "item")
	g.AddTransition("inner", "join")
	g.AddTransition("join", "outer")
	g.AddTransition("outer", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_GotoStaysInScope(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Goto from inside a fan-out branch back to the same branch is fine: the target stays
	// in the same scope.
	g := NewGraph("goto-in-scope")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransitionGoto("a", "a")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_OnErrorHandlerConvergesToFanIn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("onerror-fanin")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransitionOnError("a", "handler")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("handler", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_EndWithUnpoppedFrame(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Branch from fan-out reaches END without passing through the FanIn.
	g := NewGraph("end-unpopped")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", END) // skips the join — invalid
	g.AddTransition("join", END)
	g.SetFanIn("join")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "unpopped fan-out frames")
}

func TestLineage_DivergentStacksAtSameNode(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// "shared" is first visited with stack [s] (via a). Then a goto from join (stack [])
	// targets it again, this time with stack []. The validator rejects.
	g := NewGraph("divergent-stacks")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "shared")
	g.AddTransition("b", "shared")
	g.AddTransition("shared", "join")
	g.AddTransitionGoto("join", "shared") // back-edge from outer scope
	g.AddTransition("join", END)
	g.SetFanIn("join")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "two different lineage stacks")
}

func TestLineage_FanInOutsideAnyScope(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("fanin-no-scope")
	g.AddTransition("a", "b")
	g.AddTransition("b", END)
	g.SetFanIn("b")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "no fan-out frame to pop")
}

func TestLineage_GotoCrossingScopeRejected(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// goto from inside the fan-out scope to a node in the parent scope (downstream of
	// the fan-in) is rejected: the source's stack and target's stack differ.
	g := NewGraph("goto-cross-scope")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", "after")
	g.AddTransition("after", END)
	g.AddTransitionGoto("a", "after") // jumps out of the cohort
	g.SetFanIn("join")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "different lineage stacks")
}

func TestLineage_FanOutSourceMissingFanIn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Two parallel branches both reach END without converging at any FanIn.
	g := NewGraph("missing-fanin")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join") // a converges
	g.AddTransition("b", END)    // b doesn't
	g.AddTransition("join", END)
	g.SetFanIn("join")
	err := g.Validate()
	assert.Error(err)
	// "b reaches END with unpopped frame [s]" is what the END check fires on.
	assert.Contains(err.Error(), "unpopped fan-out frames")
}

func TestLineage_FanOutDirectlyToFanIn(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Some siblings go through intermediate work; one sibling goes directly to the join.
	// Both arrive at "join" with the same scope (push-then-pop on the direct edge cancels).
	g := NewGraph("direct-fanin")
	g.AddTransition("s", "a")
	g.AddTransition("s", "join") // direct
	g.AddTransition("a", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
	assert.Equal("join", g.FanInFor("s"))
}

func TestLineage_SetFanInOnUnknownNodeRejected(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("bad-fanin-name")
	g.AddTransition("a", "b")
	g.AddTransition("b", END)
	g.SetFanIn("c")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "unknown node")
}

func TestLineage_FanInFlagSurvivesJSONRoundTrip(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("roundtrip")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	data, err := json.Marshal(g)
	assert.NoError(err)

	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)

	assert.Expect(restored.IsFanIn("join"), true)
	assert.Expect(restored.IsFanIn("a"), false)
	assert.Equal("join", restored.FanInFor("s"))
}

func TestGraph_SwitchValid(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("switch-routing")
	g.AddTransition("entry", "router")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "i==2")
	g.AddTransitionSwitch("router", "c", "true")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	g.AddTransition("c", END)
	assert.NoError(g.Validate())
	// Switch source must not be treated as a fan-out source.
	assert.Expect(g.IsFanOutSource("router"), false)
}

func TestGraph_SwitchRejectsEmptyWhen(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("switch-no-when")
	g.AddTransitionSwitch("router", "a", "")
	g.AddTransition("a", END)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "requires a 'when' expression")
}

func TestGraph_SwitchRejectsMixWithPlain(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("switch-mixed")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransition("router", "b")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch transition")
}

func TestGraph_SwitchRejectsMixWithWhen(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("switch-vs-when")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionWhen("router", "b", "i==2")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch transition")
}

func TestGraph_SwitchAllowedWithGoto(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// WithGoto is an explicit task-requested override; it preempts Switch
	// evaluation at runtime, so the two kinds can coexist from one source.
	g := NewGraph("switch-with-goto")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "true")
	g.AddTransitionGoto("router", "c")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	g.AddTransition("c", END)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchAllowedWithOnError(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("switch-with-onerror")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "true")
	g.AddTransitionOnError("router", "handler")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	g.AddTransition("handler", END)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchNoFanInRequired(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Three Switch branches from one node, no SetFanIn anywhere: validator must accept.
	g := NewGraph("switch-no-fanin")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "i==2")
	g.AddTransitionSwitch("router", "c", "true")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	g.AddTransition("c", END)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchRejectsForEach(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// A switch transition with ForEach set is constructed directly to bypass the
	// constructor; the validator must reject it.
	g := NewGraph("switch-foreach")
	g.AddTransition("router", "a")
	g.AddTransition("a", END)
	g.transitions = append(g.transitions, Transition{From: "router", To: "a", When: "true", Switch: true, ForEach: "items", As: "item"})
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch")
}

func TestGraph_SwitchMermaidDiamond(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("switch-render")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "true")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	assert.NoError(g.Validate())
	m := must(NewGraphRenderer(g).Render())
	// Diamond is emitted as a rhombus labeled "switch" with a per-source suffix.
	assert.Contains(m, `t0_switch{"switch"}`)
	// Source routes through the diamond, not directly to the arms.
	assert.Contains(m, "t0 --> t0_switch")
	// Arms carry the condition as their label; when="true" becomes "default".
	assert.Contains(m, `t0_switch -->|"i==1"| t1`)
	assert.Contains(m, `t0_switch -->|"default"| t2`)
	// No direct labeled edge from source to arms.
	assert.NotContains(m, `t0 -->|"i==1"|`)
}

func TestGraph_MermaidReduceCircle(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	// Classic two-branch fan-out converging on a SetFanIn node.
	g := NewGraph("reduce-render")
	g.AddTransition("split", "a")
	g.AddTransition("split", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
	m := must(NewGraphRenderer(g).Render())

	// The reduce circle sits ahead of the fan-in node, same color as the
	// switch/when diamonds (term class).
	assert.Contains(m, `t3_reduce(("reduce")):::term`)
	// Single edge from reduce circle to the fan-in node.
	assert.Contains(m, "t3_reduce --> t3")
	// Both cohort siblings now point at the reduce circle, not the node.
	assert.Contains(m, "t1 --> t3_reduce")
	assert.Contains(m, "t2 --> t3_reduce")
	// No direct cohort -> fan-in node edges.
	assert.NotContains(m, "t1 --> t3\n")
	assert.NotContains(m, "t2 --> t3\n")
}

func TestGraph_MermaidAnnotation(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("annotated")
	g.AddTransition("a", "b")
	g.AddTransition("b", END)
	g.Annotate("a", "kicks off the loop")

	mmd := must(NewGraphRenderer(g).Render())
	// Annotated node is wrapped in an invisible TB subgraph.
	assert.Contains(mmd, `subgraph t0_anno [" "]`)
	assert.Contains(mmd, "direction TB")
	assert.Contains(mmd, "style t0_anno fill:none,stroke:none")
	// The note is a teal, chromeless text node beneath the source.
	assert.Contains(mmd, `t0_note["kicks off the loop"]:::note`)
	assert.Contains(mmd, "classDef note fill:none,stroke:none,color:#32a7c1,font-size:0.8em")
	// Edges still target the actual task node, not the wrapper.
	assert.Contains(mmd, "t0 --> t1")
	assert.NotContains(mmd, "t0_anno --> ")

	// Re-annotating replaces; passing "" clears.
	g.Annotate("a", "")
	mmd = must(NewGraphRenderer(g).Render())
	assert.NotContains(mmd, "kicks off the loop")
	assert.NotContains(mmd, "t0_anno")
}

func TestGraph_WhenMermaidDiamond(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)

	g := NewGraph("when-render")
	g.AddTransitionWhen("router", "a", "i==1")
	g.AddTransitionWhen("router", "b", "i==2")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
	m := must(NewGraphRenderer(g).Render())
	// Diamond labeled "when" appears for the When-source.
	assert.Contains(m, `t0_when{"when"}`)
	assert.Contains(m, "t0 --> t0_when")
	assert.Contains(m, `t0_when -->|"i==1"| t1`)
	assert.Contains(m, `t0_when -->|"i==2"| t2`)
}
