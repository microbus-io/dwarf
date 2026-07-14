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
	assert := testarossa.For(t)

	g := NewGraph("CreateOrder")
	g.AddTransitionWhen("order.service/validate", "payment.service/charge", "valid == true")
	g.AddTransitionWhen("order.service/validate", "order.service/reject", "valid != true")
	g.SetReducer("messages", ReducerAppend)

	assert.Equal("CreateOrder", g.Name())
	assert.Equal("order.service/validate", g.EntryPoint())
	assert.Equal(3, len(g.Nodes()))

	data, err := json.Marshal(g)
	assert.NoError(err)

	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)

	assert.Equal("CreateOrder", restored.Name())
	assert.Equal("order.service/validate", restored.EntryPoint())
	assert.Equal(3, len(restored.Nodes()))
	assert.Equal(2, len(restored.Transitions()))
	assert.Equal("valid == true", restored.Transitions()[0].When)
	assert.Equal(ReducerAppend, restored.reducers["messages"])
}

func TestGraph_EmptyReducers(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Simple")
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
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.SetEndpoint("svc/first", "svc/first")
	g.SetEndpoint("svc/second", "svc/second")

	assert.Equal("svc/first", g.EntryPoint())
}

func TestGraph_ExplicitEntryPoint(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.SetEndpoint("svc/first", "svc/first")
	g.SetEndpoint("svc/second", "svc/second")
	g.SetEntryPoint("svc/second")

	assert.Equal("svc/second", g.EntryPoint())
}

func TestGraph_AutoRegisterTasks(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransitionWhen("svc/b", "svc/c", "done == true")

	tasks := g.Nodes()
	assert.Equal(3, len(tasks))
	assert.Equal("svc/a", tasks[0].Name)
	assert.Equal("svc/b", tasks[1].Name)
	assert.Equal("svc/c", tasks[2].Name)
}

func TestGraph_DuplicateTaskIgnored(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.SetEndpoint("svc/a", "svc/a")
	g.SetEndpoint("svc/a", "svc/a")
	g.AddTransition("svc/a", "svc/b")

	assert.Equal(2, len(g.Nodes()))
}

func TestGraph_Validate(t *testing.T) {
	assert := testarossa.For(t)

	// Valid graph
	g := NewGraph("Test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransition("svc/b", END)
	assert.NoError(g.Validate())

	// Empty name
	g2 := NewGraph("")
	g2.SetEndpoint("svc/a", "svc/a")
	assert.Error(g2.Validate())

	// No tasks
	g3 := NewGraph("Test")
	assert.Error(g3.Validate())

	// Entry point not in task list
	g4 := NewGraph("Test")
	g4.SetEndpoint("svc/a", "svc/a")
	g4.SetEntryPoint("svc/missing")
	assert.Error(g4.Validate())

	// Unreachable task
	g5 := NewGraph("Test")
	g5.AddTransition("svc/a", "svc/b")
	g5.SetEndpoint("svc/c", "svc/c")
	assert.Error(g5.Validate())

	// Reachable via goto
	g6 := NewGraph("Test")
	g6.AddTransition("svc/a", "svc/b")
	g6.AddTransition("svc/b", END)
	g6.AddTransitionGoto("svc/a", "svc/c")
	g6.AddTransition("svc/c", END)
	assert.NoError(g6.Validate())

	// No END transition
	g7 := NewGraph("Test")
	g7.AddTransition("svc/a", "svc/b")
	g7.AddTransition("svc/b", "svc/a")
	assert.Error(g7.Validate())
}

// TestGraph_ValidateOnError pins the two onError guards: a node may declare at most one onError transition
// (ErrorTransition takes the first, so a second would silently never fire), and an onError transition cannot
// carry a 'when' (onError is unconditional). The 'when' case is only reachable via a hand-crafted graph -
// AddTransitionOnError never sets When - so it is built by appending a raw Transition.
func TestGraph_ValidateOnError(t *testing.T) {
	assert := testarossa.For(t)

	// A single onError is fine.
	ok := NewGraph("Test")
	ok.AddTransition("svc/a", END)
	ok.AddTransitionOnError("svc/a", "svc/h")
	ok.AddTransition("svc/h", END)
	assert.NoError(ok.Validate())

	// Two onError transitions from the same node are rejected.
	dup := NewGraph("Test")
	dup.AddTransition("svc/a", END)
	dup.AddTransitionOnError("svc/a", "svc/h1")
	dup.AddTransition("svc/h1", END)
	dup.AddTransitionOnError("svc/a", "svc/h2")
	dup.AddTransition("svc/h2", END)
	assert.Error(dup.Validate())

	// An onError transition carrying a 'when' (hand-crafted) is rejected.
	cond := NewGraph("Test")
	cond.AddTransition("svc/a", END)
	cond.SetEndpoint("svc/h", "svc/h")
	cond.AddTransition("svc/h", END)
	cond.transitions = append(cond.transitions, Transition{From: "svc/a", To: "svc/h", OnError: true, When: "x > 0"})
	assert.Error(cond.Validate())
}

// TestGraph_ValidateFanOutConvergence pins that all branches of one fan-out must converge on the SAME
// fan-in node. The cohort shares a spawn step and the cohort-resolution path selects the fan-in from
// whichever sibling completes last, so divergent fan-in targets make the convergence node
// nondeterministic - Validate must reject that, while a shared fan-in validates cleanly.
func TestGraph_ValidateFanOutConvergence(t *testing.T) {
	assert := testarossa.For(t)

	// Divergent: S fans out to A,B; A -> J, B -> K (different fan-ins). Rejected.
	bad := NewGraph("Divergent")
	bad.AddTransition("svc/s", "svc/a")
	bad.AddTransition("svc/s", "svc/b")
	bad.AddTransition("svc/a", "svc/j")
	bad.AddTransition("svc/b", "svc/k")
	bad.AddTransition("svc/j", END)
	bad.AddTransition("svc/k", END)
	bad.SetFanIn("svc/j")
	bad.SetFanIn("svc/k")
	assert.Error(bad.Validate())

	// Convergent: same shape, both siblings -> J. Accepted.
	good := NewGraph("Convergent")
	good.AddTransition("svc/s", "svc/a")
	good.AddTransition("svc/s", "svc/b")
	good.AddTransition("svc/a", "svc/j")
	good.AddTransition("svc/b", "svc/j")
	good.AddTransition("svc/j", END)
	good.SetFanIn("svc/j")
	assert.NoError(good.Validate())
}

// TestGraph_ValidateFanInFromOutsideCohort pins the check that a withGoto/onError/switch edge into a
// fan-in node is subject to the same frame-pop rule as a normal edge. The engine treats ANY transition
// into a fan-in node as a cohort arrival, so such an edge from a node with no fan-out frame arrives at a
// cohort that does not exist: at runtime it bumps cohort_arrivals on step_id=0, the transition tx aborts
// on the follow-up SELECT, the recovery defer rewinds the step, and the task re-runs - side effects and
// all - in an unbounded hot loop. validateLineage tested the goto/onError/switch arm FIRST, which skipped
// the frame-pop check for exactly the edge kinds that can reach a fan-in from outside a cohort.
func TestGraph_ValidateFanInFromOutsideCohort(t *testing.T) {
	assert := testarossa.For(t)

	// A goto from a TRUNK node (K, downstream of the fan-in, lineage 0) back into the fan-in node.
	viaGoto := NewGraph("GotoIntoFanIn")
	viaGoto.AddTransition("svc/s", "svc/b")
	viaGoto.AddTransition("svc/s", "svc/c")
	viaGoto.AddTransition("svc/b", "svc/j")
	viaGoto.AddTransition("svc/c", "svc/j")
	viaGoto.SetFanIn("svc/j")
	viaGoto.AddTransition("svc/j", "svc/k")
	viaGoto.AddTransition("svc/k", END)
	viaGoto.AddTransitionGoto("svc/k", "svc/j") // K is not in a cohort: no frame to pop
	assert.Error(viaGoto.Validate(), "a goto into a fan-in from outside a cohort must be rejected")

	// Same shape via onError.
	viaOnError := NewGraph("OnErrorIntoFanIn")
	viaOnError.AddTransition("svc/s", "svc/b")
	viaOnError.AddTransition("svc/s", "svc/c")
	viaOnError.AddTransition("svc/b", "svc/j")
	viaOnError.AddTransition("svc/c", "svc/j")
	viaOnError.SetFanIn("svc/j")
	viaOnError.AddTransition("svc/j", "svc/k")
	viaOnError.AddTransition("svc/k", END)
	viaOnError.AddTransitionOnError("svc/k", "svc/j")
	assert.Error(viaOnError.Validate(), "an onError into a fan-in from outside a cohort must be rejected")

	// A goto into the fan-in from INSIDE the cohort is legitimate - the branch has a frame to pop, and
	// the arrival lands on a real spawn step.
	fromBranch := NewGraph("GotoFromBranch")
	fromBranch.AddTransition("svc/s", "svc/b")
	fromBranch.AddTransition("svc/s", "svc/c")
	fromBranch.AddTransition("svc/b", "svc/j")
	fromBranch.AddTransition("svc/c", "svc/j")
	fromBranch.SetFanIn("svc/j")
	fromBranch.AddTransition("svc/j", END)
	fromBranch.AddTransitionGoto("svc/b", "svc/j")
	assert.NoError(fromBranch.Validate(), "a goto into the fan-in from a cohort branch is legitimate")
}

func TestGraph_AddTransitionChain(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Chain")
	g.AddTransitionChain("a", "b", "c", END)

	trs := g.Transitions()
	assert.Equal(3, len(trs))
	assert.Equal("a", trs[0].From)
	assert.Equal("b", trs[0].To)
	assert.Equal("b", trs[1].From)
	assert.Equal("c", trs[1].To)
	assert.Equal("c", trs[2].From)
	assert.Equal(END, trs[2].To)
	// Chain wires plain unconditional edges, so it validates like the equivalent AddTransition calls.
	assert.NoError(g.Validate())
	// First node in the chain is the default entry point.
	assert.Equal("a", g.EntryPoint())

	// Fewer than two names is a no-op.
	g2 := NewGraph("One")
	g2.AddTransitionChain("solo")
	assert.Equal(0, len(g2.Transitions()))
}

func TestGraph_AddTransitionFanOut(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("FanOut")
	g.AddTransitionFanOut("a", "b", "c", "d")

	trs := g.Transitions()
	assert.Equal(3, len(trs))
	for i, dest := range []string{"b", "c", "d"} {
		assert.Equal("a", trs[i].From)
		assert.Equal(dest, trs[i].To)
	}
	// First (source) node is the default entry point.
	assert.Equal("a", g.EntryPoint())

	// No destinations is a no-op.
	g2 := NewGraph("None")
	g2.AddTransitionFanOut("solo")
	assert.Equal(0, len(g2.Transitions()))
}

func TestGraph_TransitionOutOfENDRejected(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("BadEnd")
	g.AddTransition("a", END)
	g.AddTransition(END, "b") // END is terminal; an outgoing transition is invalid
	err := g.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "out of END")
	}
}

func TestGraph_END(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
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
	assert := testarossa.For(t)

	g := NewGraph("CreateOrder")
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
	assert := testarossa.For(t)

	g := NewGraph("Test")
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
	assert := testarossa.For(t)

	g := NewGraph("Test")
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
	assert := testarossa.For(t)

	// Valid expression
	g1 := NewGraph("Test")
	g1.AddTransitionWhen("svc/a", "svc/b", "valid == true")
	g1.AddTransitionWhen("svc/a", "svc/c", "score > 5 && !guest")
	g1.AddTransition("svc/b", "svc/join")
	g1.AddTransition("svc/c", "svc/join")
	g1.AddTransition("svc/join", END)
	g1.SetFanIn("svc/join")
	assert.NoError(g1.Validate())

	// Invalid expression
	g2 := NewGraph("Test")
	g2.AddTransitionWhen("svc/a", "svc/b", "(((")
	g2.AddTransition("svc/b", END)
	err := g2.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "invalid 'when' expression")
}

func TestGraph_AddTransitionOnError(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransition("svc/b", END)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", END)

	transitions := g.Transitions()
	assert.Equal(4, len(transitions))

	// AddTransitionOnError: OnError=true
	assert.True(transitions[2].OnError)

	assert.NoError(g.Validate())
}

func TestGraph_OnErrorJSONRoundTrip(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", END)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", END)

	data, err := json.Marshal(g)
	assert.NoError(err)

	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)
	assert.Equal(3, len(restored.Transitions()))

	// Find the onError transition.
	var found bool
	for _, tr := range restored.Transitions() {
		if tr.OnError {
			assert.Equal("svc/errHandler", tr.To)
			found = true
		}
	}
	assert.True(found)
}

func TestGraph_MermaidForEachShape(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
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
	assert := testarossa.For(t)

	g := NewGraph("Test")
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
	assert := testarossa.For(t)

	g := NewGraph("Test")
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
	assert := testarossa.For(t)

	g := NewGraph("Test")
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

func TestGraph_MermaidLabelsOnError(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", END)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", END)

	mmd := must(NewGraphRenderer(g).Render())
	assert.Contains(mmd, `"onError"`)
}

func TestGraph_SelfLoopOnErrorRejected(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", END)
	g.AddTransitionOnError("svc/a", "svc/a")

	err := g.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "to itself")
		assert.Contains(err.Error(), "flow.Retry")
	}
}

func TestGraph_GotoSelfLoopAllowed(t *testing.T) {
	assert := testarossa.For(t)

	// A goto-driven self-loop is not restricted by the no-error-self-loop rule.
	// (A normal-edge self-loop wouldn't validate under the lineage stack rules anyway,
	// since the source becomes a fan-out source whose only fan-in is itself.)
	g := NewGraph("Test")
	g.AddTransitionGoto("svc/a", "svc/a")
	g.AddTransition("svc/a", END)
	assert.NoError(g.Validate())
}

// Lineage validator tests.

func TestLineage_SequentialNoFanOut(t *testing.T) {
	assert := testarossa.For(t)

	// SetFanIn opts the graph into the lineage validator. With no fan-out, the validator
	// has nothing to check beyond the structural rules; the FanIn marker on a sequentially
	// reached node is ill-formed (no scope to pop) and must be rejected.
	g := NewGraph("Seq")
	g.AddTransition("a", "b")
	g.AddTransition("b", END)
	g.SetFanIn("b")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "no fan-out frame to pop")
}

func TestLineage_SimpleFanOutFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SimpleFanin")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_NestedFanOutFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Nested")
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
}

func TestLineage_ForEachThenFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("ForeachFanin")
	g.AddTransitionForEach("s", "a", "items", "item")
	g.AddTransition("a", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_ConditionalWhenFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("WhenFanin")
	g.AddTransitionWhen("s", "a", "x > 0")
	g.AddTransitionWhen("s", "b", "x <= 0")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_AliasedNodesInDifferentScopes(t *testing.T) {
	assert := testarossa.For(t)

	// The same task URL is registered under two distinct names so that one copy lives
	// inside a fan-out scope (per element) and a second copy lives at the outer scope.
	g := NewGraph("Alias")
	g.SetEndpoint("s", "host/s")
	g.SetEndpoint("inner", "host/work") // inside fan-out
	g.SetEndpoint("outer", "host/work") // outside fan-out
	g.SetEndpoint("join", "host/join")
	g.AddTransitionForEach("s", "inner", "items", "item")
	g.AddTransition("inner", "join")
	g.AddTransition("join", "outer")
	g.AddTransition("outer", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_GotoStaysInScope(t *testing.T) {
	assert := testarossa.For(t)

	// Goto from inside a fan-out branch back to the same branch is fine: the target stays
	// in the same scope.
	g := NewGraph("GotoInScope")
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
	assert := testarossa.For(t)

	g := NewGraph("OnerrorFanin")
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
	assert := testarossa.For(t)

	// Branch from fan-out reaches END without passing through the FanIn.
	g := NewGraph("EndUnpopped")
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
	assert := testarossa.For(t)

	// "shared" is first visited with stack [s] (via a). Then a goto from join (stack [])
	// targets it again, this time with stack []. The validator rejects.
	g := NewGraph("DivergentStacks")
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
	assert := testarossa.For(t)

	g := NewGraph("FaninNoScope")
	g.AddTransition("a", "b")
	g.AddTransition("b", END)
	g.SetFanIn("b")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "no fan-out frame to pop")
}

func TestLineage_GotoCrossingScopeRejected(t *testing.T) {
	assert := testarossa.For(t)

	// goto from inside the fan-out scope to a node in the parent scope (downstream of
	// the fan-in) is rejected: the source's stack and target's stack differ.
	g := NewGraph("GotoCrossScope")
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
	assert := testarossa.For(t)

	// Two parallel branches both reach END without converging at any FanIn.
	g := NewGraph("MissingFanin")
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
	assert := testarossa.For(t)

	// Some siblings go through intermediate work; one sibling goes directly to the join.
	// Both arrive at "join" with the same scope (push-then-pop on the direct edge cancels).
	g := NewGraph("DirectFanin")
	g.AddTransition("s", "a")
	g.AddTransition("s", "join") // direct
	g.AddTransition("a", "join")
	g.AddTransition("join", END)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_SetFanInOnUnknownNodeRejected(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("BadFaninName")
	g.AddTransition("a", "b")
	g.AddTransition("b", END)
	g.SetFanIn("c")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "unknown node")
}

func TestLineage_FanInFlagSurvivesJSONRoundTrip(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Roundtrip")
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
}

func TestGraph_SwitchValid(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchRouting")
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
	assert := testarossa.For(t)

	g := NewGraph("SwitchNoWhen")
	g.AddTransitionSwitch("router", "a", "")
	g.AddTransition("a", END)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "requires a 'when' expression")
}

func TestGraph_SwitchRejectsMixWithPlain(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchMixed")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransition("router", "b")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch transition")
}

func TestGraph_SwitchRejectsMixWithWhen(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchVsWhen")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionWhen("router", "b", "i==2")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch transition")
}

func TestGraph_SwitchAllowedWithGoto(t *testing.T) {
	assert := testarossa.For(t)

	// WithGoto is an explicit task-requested override; it preempts Switch
	// evaluation at runtime, so the two kinds can coexist from one source.
	g := NewGraph("SwitchWithGoto")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "true")
	g.AddTransitionGoto("router", "c")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	g.AddTransition("c", END)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchAllowedWithOnError(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchWithOnerror")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "true")
	g.AddTransitionOnError("router", "handler")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	g.AddTransition("handler", END)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchNoFanInRequired(t *testing.T) {
	assert := testarossa.For(t)

	// Three Switch branches from one node, no SetFanIn anywhere: validator must accept.
	g := NewGraph("SwitchNoFanin")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "i==2")
	g.AddTransitionSwitch("router", "c", "true")
	g.AddTransition("a", END)
	g.AddTransition("b", END)
	g.AddTransition("c", END)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchRejectsForEach(t *testing.T) {
	assert := testarossa.For(t)

	// A switch transition with ForEach set is constructed directly to bypass the
	// constructor; the validator must reject it.
	g := NewGraph("SwitchForeach")
	g.AddTransition("router", "a")
	g.AddTransition("a", END)
	g.transitions = append(g.transitions, Transition{From: "router", To: "a", When: "true", Switch: true, ForEach: "items", As: "item"})
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch")
}

func TestGraph_SwitchMermaidDiamond(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchRender")
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
	assert := testarossa.For(t)

	// Classic two-branch fan-out converging on a SetFanIn node.
	g := NewGraph("ReduceRender")
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

func TestGraph_WhenMermaidDiamond(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("WhenRender")
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

func TestEscapeMermaid(t *testing.T) {
	assert := testarossa.For(t)

	// The common case (URL-shaped task/graph names) carries no Mermaid metacharacters and is unchanged.
	assert.Equal("CreateOrder", escapeMermaid("CreateOrder"))
	assert.Equal("svc.name:8080/task-v2", escapeMermaid("svc.name:8080/task-v2"))

	// Every metacharacter is rewritten to a Mermaid character reference.
	assert.Equal(
		"#quot;#91;#93;#123;#125;#40;#41;#lt;#gt;#124;#96;#35;#39;",
		escapeMermaid("\"[]{}()<>|`#'"),
	)
	// '#' is escaped first, so an author-supplied "#quot;"-shaped substring cannot survive as a live
	// reference (single non-overlapping pass, no re-scan of emitted references).
	assert.Equal("#35;quot;", escapeMermaid("#quot;"))
}

func TestGraph_MermaidEscapesInjection(t *testing.T) {
	assert := testarossa.For(t)

	// A malicious workflow author names the graph, a task, and a when-expression with Mermaid/HTML
	// metacharacters, aiming to break out of the ["..."] label and inject node/edge/click syntax or
	// an <img> XSS payload into a host UI that renders the diagram.
	g := NewGraph(`Ord"er<x>`)
	evil := "svc/pwn\"]:::term click n \"http\" `x` <img src=q onerror=alert(1)> [z] {q}"
	g.AddTransitionWhen(evil, "svc/next", `a || b == "c"`)
	g.AddTransition("svc/next", END)

	m := must(NewGraphRenderer(g).Render())

	// No raw metacharacter survives to end a label or inject markup/directives.
	assert.NotContains(m, `<img`)
	assert.NotContains(m, `<x>`)
	assert.NotContains(m, "`x`")
	assert.NotContains(m, `"]:::term`)
	// The frontmatter title (author-controlled graph name) is escaped, not passed through by %q.
	assert.Contains(m, `title: "Ord#quot;er#lt;x#gt;"`)
	// Metacharacters are rewritten to Mermaid character references in node and edge labels.
	assert.Contains(m, `#lt;img`)
	assert.Contains(m, `#quot;`)
	assert.Contains(m, `#96;`)  // backticks
	assert.Contains(m, `#124;`) // the || in the when-expression
}

func TestFlow_MermaidEscapesInjection(t *testing.T) {
	assert := testarossa.For(t)

	// Malicious task name, subgraph (workflow) name, and title carried through a flow's rendered history.
	steps := []FlowStep{
		{
			StepID:   1,
			TaskName: "svc/pwn\"]:::term <img src=q onerror=alert(1)> |z|",
			Status:   StatusCompleted,
		},
		{
			StepID:          2,
			TaskName:        "svc/caller",
			PredecessorID:   1,
			Subgraph:        true,
			SubWorkflowName: `Evil"]<img>`,
			SubHistory:      []FlowStep{{StepID: 3, TaskName: "svc/inner", Status: StatusCompleted}},
			Status:          StatusCompleted,
		},
	}

	m := must(NewFlowRenderer(steps).WithTitle(`T"itle<x>`).Render())

	// No raw metacharacter survives in node labels, the subgraph block title, or the chart title.
	assert.NotContains(m, `<img`)
	assert.NotContains(m, `<x>`)
	assert.NotContains(m, `"]:::term`)
	// Escaped forms are emitted instead.
	assert.Contains(m, `#lt;img`)
	assert.Contains(m, `#quot;`)
	assert.Contains(m, `_title{{"T#quot;itle#lt;x#gt;"}}`)
}
