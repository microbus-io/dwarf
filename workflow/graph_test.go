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

func TestGraph_BuilderAndMarshal(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("CreateOrder")
	g.AddTransitionWhen("order.service/validate", "payment.service/charge", "valid == true")
	g.AddTransitionWhen("order.service/validate", "order.service/reject", "valid != true")
	g.SetReducer("messages", ReducerAppend)

	assert.Equal("CreateOrder", g.Name())
	assert.Equal("order.service/validate", g.EntryPoint())
	assert.Equal(3, len(g.Tasks()))

	data, err := json.Marshal(g)
	assert.NoError(err)

	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)

	assert.Equal("CreateOrder", restored.Name())
	assert.Equal("order.service/validate", restored.EntryPoint())
	assert.Equal(3, len(restored.Tasks()))
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
	var raw map[string]any
	err = json.Unmarshal(data, &raw)
	assert.NoError(err)
	_, ok := raw["reducers"]
	assert.False(ok)
}

func TestGraph_DefaultEntryPoint(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.SetURL("svc/first", "svc/first")
	g.SetURL("svc/second", "svc/second")

	assert.Equal("svc/first", g.EntryPoint())
}

func TestGraph_ExplicitEntryPoint(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.SetURL("svc/first", "svc/first")
	g.SetURL("svc/second", "svc/second")
	g.SetEntryPoint("svc/second")

	assert.Equal("svc/second", g.EntryPoint())
}

func TestGraph_AutoRegisterTasks(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransitionWhen("svc/b", "svc/c", "done == true")

	tasks := g.Tasks()
	assert.Equal(3, len(tasks))
	assert.Equal("svc/a", tasks[0].Name)
	assert.Equal("svc/b", tasks[1].Name)
	assert.Equal("svc/c", tasks[2].Name)
}

func TestGraph_DuplicateTaskIgnored(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.SetURL("svc/a", "svc/a")
	g.SetURL("svc/a", "svc/a")
	g.AddTransition("svc/a", "svc/b")

	assert.Equal(2, len(g.Tasks()))
}

func TestGraph_Validate(t *testing.T) {
	assert := testarossa.For(t)

	// Valid graph
	g := NewGraph("Test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransition("svc/b", End)
	assert.NoError(g.Validate())

	// Empty name
	g2 := NewGraph("")
	g2.SetURL("svc/a", "svc/a")
	assert.Error(g2.Validate())

	// No tasks
	g3 := NewGraph("Test")
	assert.Error(g3.Validate())

	// Entry point not in task list
	g4 := NewGraph("Test")
	g4.SetURL("svc/a", "svc/a")
	g4.SetEntryPoint("svc/missing")
	assert.Error(g4.Validate())

	// Unreachable task
	g5 := NewGraph("Test")
	g5.AddTransition("svc/a", "svc/b")
	g5.SetURL("svc/c", "svc/c")
	assert.Error(g5.Validate())

	// Reachable via goto
	g6 := NewGraph("Test")
	g6.AddTransition("svc/a", "svc/b")
	g6.AddTransition("svc/b", End)
	g6.AddTransitionGoto("svc/a", "svc/c")
	g6.AddTransition("svc/c", End)
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
	ok.AddTransition("svc/a", End)
	ok.AddTransitionOnError("svc/a", "svc/h")
	ok.AddTransition("svc/h", End)
	assert.NoError(ok.Validate())

	// Two onError transitions from the same node are rejected.
	dup := NewGraph("Test")
	dup.AddTransition("svc/a", End)
	dup.AddTransitionOnError("svc/a", "svc/h1")
	dup.AddTransition("svc/h1", End)
	dup.AddTransitionOnError("svc/a", "svc/h2")
	dup.AddTransition("svc/h2", End)
	assert.Error(dup.Validate())

	// An onError transition carrying a 'when' (hand-crafted) is rejected.
	cond := NewGraph("Test")
	cond.AddTransition("svc/a", End)
	cond.SetURL("svc/h", "svc/h")
	cond.AddTransition("svc/h", End)
	cond.transitions = append(cond.transitions, Transition{From: "svc/a", To: "svc/h", Kind: TransitionError, When: "x > 0"})
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
	bad.AddTransition("svc/j", End)
	bad.AddTransition("svc/k", End)
	bad.SetFanIn("svc/j")
	bad.SetFanIn("svc/k")
	assert.Error(bad.Validate())

	// Convergent: same shape, both siblings -> J. Accepted.
	good := NewGraph("Convergent")
	good.AddTransition("svc/s", "svc/a")
	good.AddTransition("svc/s", "svc/b")
	good.AddTransition("svc/a", "svc/j")
	good.AddTransition("svc/b", "svc/j")
	good.AddTransition("svc/j", End)
	good.SetFanIn("svc/j")
	assert.NoError(good.Validate())
}

// TestGraph_ValidateFanInFromOutsideCohort pins the check that a goto/error/switch edge into a
// fan-in node is subject to the same frame-pop rule as a normal edge. The engine treats ANY transition
// into a fan-in node as a cohort arrival, so such an edge from a node with no fan-out frame arrives at a
// cohort that does not exist: at runtime it bumps cohort_arrivals on step_id=0, the transition tx aborts
// on the follow-up SELECT, the recovery defer rewinds the step, and the task re-runs - side effects and
// all - in an unbounded hot loop. Testing the goto/onError/switch arm first skips the frame-pop check for
// exactly the edge kinds that can reach a fan-in from outside a cohort.
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
	viaGoto.AddTransition("svc/k", End)
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
	viaOnError.AddTransition("svc/k", End)
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
	fromBranch.AddTransition("svc/j", End)
	fromBranch.AddTransitionGoto("svc/b", "svc/j")
	assert.NoError(fromBranch.Validate(), "a goto into the fan-in from a cohort branch is legitimate")
}

func TestGraph_AddTransitionChain(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Chain")
	g.AddTransitionChain("a", "b", "c", End)

	trs := g.Transitions()
	assert.Equal(3, len(trs))
	assert.Equal("a", trs[0].From)
	assert.Equal("b", trs[0].To)
	assert.Equal("b", trs[1].From)
	assert.Equal("c", trs[1].To)
	assert.Equal("c", trs[2].From)
	assert.Equal(End, trs[2].To)
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
	g.AddTransition("a", End)
	g.AddTransition(End, "b") // END is terminal; an outgoing transition is invalid
	err := g.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "out of END")
	}
}

func TestGraph_END(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransitionGoto("svc/b", End)
	g.AddTransition("svc/b", "svc/c")
	g.AddTransition("svc/c", End)

	// END should not appear in the task list
	tasks := g.Tasks()
	assert.Equal(3, len(tasks))
	for _, task := range tasks {
		assert.NotEqual(End, task.Name)
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
	g.AddTransition("payment.service/charge", End)
	g.AddTransition("order.service/reject", End)

	mmd := NewGraphRenderer(g).Render()

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
	g.AddTransition("svc/b", End)
	g.AddTransitionGoto("svc/a", "svc/c")
	g.AddTransition("svc/c", End)

	transitions := g.Transitions()
	assert.Equal(4, len(transitions))
	assert.NotEqual(TransitionGoto, transitions[0].Kind) // svc/a -> svc/b
	assert.NotEqual(TransitionGoto, transitions[1].Kind) // svc/b -> END
	assert.Equal(TransitionGoto, transitions[2].Kind)    // svc/a -> svc/c (goto)
	assert.NotEqual(TransitionGoto, transitions[3].Kind) // svc/c -> END

	// Goto transitions should have a "goto" label in Mermaid
	mmd := NewGraphRenderer(g).Render()
	assert.Contains(mmd, `"goto"`)

	// Should validate successfully
	assert.NoError(g.Validate())

	// Should round-trip through JSON
	data, err := json.Marshal(g)
	assert.NoError(err)
	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)
	assert.Equal(TransitionGoto, restored.Transitions()[2].Kind)
}

func TestGraph_TransitionNoWhen(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("a", "b")

	data, err := json.Marshal(g)
	assert.NoError(err)

	// When should be omitted in JSON
	var raw struct {
		Transitions []map[string]any `json:"transitions"`
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
	g1.AddTransition("svc/join", End)
	g1.SetFanIn("svc/join")
	assert.NoError(g1.Validate())

	// Invalid expression
	g2 := NewGraph("Test")
	g2.AddTransitionWhen("svc/a", "svc/b", "(((")
	g2.AddTransition("svc/b", End)
	err := g2.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "invalid 'when' expression")
}

func TestGraph_ValidateFieldNames(t *testing.T) {
	assert := testarossa.For(t)

	forEach := func(field, as string) *Graph {
		g := NewGraph("Test")
		g.AddTransitionForEach("svc/a", "svc/b", field, as)
		g.AddTransition("svc/b", "svc/join")
		g.AddTransition("svc/join", End)
		g.SetFanIn("svc/join")
		return g
	}

	assert.NoError(forEach("orders", "order").Validate())
	assert.NoError(forEach("orders", "").Validate()) // 'as' defaults to "item"

	err := forEach("order.items", "item").Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "invalid 'forEach' field name 'order.items'")
	}
	assert.NoError(forEach("line-items", "line-item").Validate())
	err = forEach("orders", "2nd").Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "invalid 'as' field name '2nd'")
	}

	g := forEach("orders", "order")
	g.SetReducer("totalCount", ReducerAdd)
	assert.NoError(g.Validate())
	g.SetReducer("total count", ReducerAdd)
	err = g.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "invalid field name 'total count'")
	}
}

func TestGraph_AddTransitionOnError(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", "svc/b")
	g.AddTransition("svc/b", End)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", End)

	transitions := g.Transitions()
	assert.Equal(4, len(transitions))

	assert.Equal(TransitionError, transitions[2].Kind)

	assert.NoError(g.Validate())
}

func TestGraph_OnErrorJSONRoundTrip(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", End)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", End)

	data, err := json.Marshal(g)
	assert.NoError(err)

	var restored Graph
	err = json.Unmarshal(data, &restored)
	assert.NoError(err)
	assert.Equal(3, len(restored.Transitions()))

	// Find the onError transition.
	var found bool
	for _, tr := range restored.Transitions() {
		if tr.Kind == TransitionError {
			assert.Equal("svc/errHandler", tr.To)
			found = true
		}
	}
	assert.True(found)
}

// TestTransition_DecodesBooleanKinds pins that a graph stored with the boolean withGoto, onError and switch
// fields decodes with the matching Kind. Each flow row freezes its graph's JSON, and an unknown field
// decodes silently, so without this a goto, error or switch transition would come back as an ordinary one
// and the flow would route down it.
func TestTransition_DecodesBooleanKinds(t *testing.T) {
	assert := testarossa.For(t)

	data := []byte(`{"name":"Legacy","entryPoint":"A","tasks":[{"name":"A","url":"a"},{"name":"B","url":"b"},{"name":"C","url":"c"},{"name":"D","url":"d"}],"transitions":[` +
		`{"from":"A","to":"B","when":"x > 1","switch":true},` +
		`{"from":"A","to":"C","when":"true","switch":true},` +
		`{"from":"A","to":"D","withGoto":true},` +
		`{"from":"B","to":"D","onError":true},` +
		`{"from":"B","to":"END"},{"from":"C","to":"END"},{"from":"D","to":"END"}]}`)
	var g Graph
	assert.NoError(json.Unmarshal(data, &g))
	kinds := []string{}
	for _, tr := range g.Transitions() {
		kinds = append(kinds, tr.Kind)
	}
	assert.Equal([]string{TransitionSwitch, TransitionSwitch, TransitionGoto, TransitionError, "", "", ""}, kinds)
	assert.NoError(g.Validate())

	// Re-encoded, the transition carries kind only.
	out, err := json.Marshal(&g)
	assert.NoError(err)
	assert.Contains(string(out), `"kind":"goto"`)
	assert.NotContains(string(out), `withGoto`)
}

// TestTransition_ValidateKind pins the rules a Kind brings: an unknown kind, a forEach on anything but an
// ordinary transition, and a when on a goto or error transition are all rejected.
func TestTransition_ValidateKind(t *testing.T) {
	assert := testarossa.For(t)

	for _, tr := range []Transition{
		{From: "A", To: "B", Kind: "sometimes"},
		{From: "A", To: "B", Kind: TransitionGoto, ForEach: "items"},
		{From: "A", To: "B", Kind: TransitionGoto, When: "x > 1"},
		{From: "A", To: "B", Kind: TransitionError, When: "x > 1"},
	} {
		g := NewGraph("Kinds")
		g.AddTransition("A", End)
		g.AddTransition("B", End)
		g.transitions = append(g.transitions, tr)
		assert.Error(g.Validate(), "%+v", tr)
	}
}

func TestGraph_MermaidForEachShape(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransitionForEach("svc/start", "svc/worker", "items", "item")
	g.AddTransition("svc/worker", End)

	mmd := NewGraphRenderer(g).Render()
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
	g.AddTransition("join", End)
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	mmd := NewGraphRenderer(g).Render()
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
	g.AddTransition("svc/join", End)
	g.SetFanIn("svc/join")
	assert.NoError(g.Validate())

	mmd := NewGraphRenderer(g).Render()
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
	g.AddTransition("svc/outerJoin", End)
	g.SetFanIn("svc/innerJoin")
	g.SetFanIn("svc/outerJoin")
	assert.NoError(g.Validate())

	mmd := NewGraphRenderer(g).Render()
	// Each forEach transition gets its own "for each" edge label; no nested boxes.
	assert.Contains(mmd, `t0 -->|"for each"| t1`)
	assert.Contains(mmd, `t1 -->|"for each"| t2`)
	assert.NotContains(mmd, `subgraph fo_`)
}

func TestGraph_MermaidLabelsOnError(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", End)
	g.AddTransitionOnError("svc/a", "svc/errHandler")
	g.AddTransition("svc/errHandler", End)

	mmd := NewGraphRenderer(g).Render()
	assert.Contains(mmd, `"onError"`)
}

func TestGraph_SelfLoopOnErrorRejected(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Test")
	g.AddTransition("svc/a", End)
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
	g.AddTransition("svc/a", End)
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
	g.AddTransition("b", End)
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
	g.AddTransition("join", End)
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
	g.AddTransition("outerJoin", End)
	g.SetFanIn("innerJoin")
	g.SetFanIn("outerJoin")
	assert.NoError(g.Validate())
}

func TestLineage_ForEachThenFanIn(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("ForeachFanin")
	g.AddTransitionForEach("s", "a", "items", "item")
	g.AddTransition("a", "join")
	g.AddTransition("join", End)
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
	g.AddTransition("join", End)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_AliasedNodesInDifferentScopes(t *testing.T) {
	assert := testarossa.For(t)

	// The same task URL is registered under two distinct names so that one copy lives
	// inside a fan-out scope (per element) and a second copy lives at the outer scope.
	g := NewGraph("Alias")
	g.SetURL("s", "host/s")
	g.SetURL("inner", "host/work") // inside fan-out
	g.SetURL("outer", "host/work") // outside fan-out
	g.SetURL("join", "host/join")
	g.AddTransitionForEach("s", "inner", "items", "item")
	g.AddTransition("inner", "join")
	g.AddTransition("join", "outer")
	g.AddTransition("outer", End)
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
	g.AddTransition("join", End)
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
	g.AddTransition("join", End)
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
	g.AddTransition("b", End) // skips the join — invalid
	g.AddTransition("join", End)
	g.SetFanIn("join")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "unpopped fan-out frames")
}

func TestLineage_GotoEndFromFanOutSource(t *testing.T) {
	assert := testarossa.For(t)

	// A node that is both a forEach fan-out source and a goto-to-END exit: "fan out over the array, or end
	// the flow when the task decides there is nothing to do". The goto is a runtime override the task body
	// chooses, not a branch of the fan-out, so it pushes no frame and END is reached with an empty stack.
	g := NewGraph("GotoEndFromFanOut")
	g.AddTransitionForEach("decide", "work", "items", "item")
	g.AddTransition("work", "join")
	g.AddTransitionGoto("decide", End)
	g.AddTransition("join", "decide")
	g.SetFanIn("join")
	assert.NoError(g.Validate())

	// An onError edge is an override for the same reason and behaves the same way. (A switch edge cannot
	// arise here at all: a node mixing switch with a forEach success-path edge is rejected earlier.)
	g2 := NewGraph("OnErrorEndFromFanOut")
	g2.AddTransitionForEach("decide", "work", "items", "item")
	g2.AddTransition("work", "join")
	g2.AddTransitionOnError("decide", End)
	g2.AddTransition("join", End)
	g2.SetFanIn("join")
	assert.NoError(g2.Validate())

	// Straight to the source's own fan-in is the other permitted target: the source is the spawn, so the
	// fan-in converges on its own state - the "if the array is empty, goto the fan-in" shape.
	g3 := NewGraph("GotoFanInFromFanOut")
	g3.AddTransitionForEach("decide", "work", "items", "item")
	g3.AddTransition("work", "join")
	g3.AddTransitionGoto("decide", "join")
	g3.AddTransition("join", End)
	g3.SetFanIn("join")
	assert.NoError(g3.Validate())
}

// An override edge out of a fan-out source spawns no cohort, so it may not lead back into the fan-in that
// cohort would have converged on. Rejected here rather than at dispatch, where the arrival lands with no
// cohort to arrive at: the trunk case fails the step, and a nested source mis-attributes the arrival to its
// outer cohort. Routing into one of the branches is the same mistake reached from the other side - the
// branch is then reachable both with and without the fan-out frame.
func TestLineage_OverrideRejoiningFanInRejected(t *testing.T) {
	assert := testarossa.For(t)

	// goto -> an intermediate node that then transitions into the fan-in.
	g := NewGraph("GotoRejoinsFanIn")
	g.AddTransitionForEach("decide", "work", "items", "item")
	g.AddTransition("work", "join")
	g.AddTransitionGoto("decide", "skip")
	g.AddTransition("skip", "join")
	g.AddTransition("join", End)
	g.SetFanIn("join")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "no fan-out frame to pop")

	// onError -> a handler that then transitions into the fan-in.
	g2 := NewGraph("OnErrorRejoinsFanIn")
	g2.AddTransitionForEach("decide", "work", "items", "item")
	g2.AddTransition("work", "join")
	g2.AddTransitionOnError("decide", "recover")
	g2.AddTransition("recover", "join")
	g2.AddTransition("join", End)
	g2.SetFanIn("join")
	err = g2.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "no fan-out frame to pop")

	// goto -> one of the fan-out's own branches, which the branch would then reach with two lineage stacks.
	g3 := NewGraph("GotoIntoBranch")
	g3.AddTransitionForEach("decide", "work", "items", "item")
	g3.AddTransition("work", "join")
	g3.AddTransitionGoto("decide", "work")
	g3.AddTransition("join", End)
	g3.SetFanIn("join")
	err = g3.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "two different lineage stacks")
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
	g.AddTransition("join", End)
	g.SetFanIn("join")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "two different lineage stacks")
}

func TestLineage_FanInOutsideAnyScope(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("FaninNoScope")
	g.AddTransition("a", "b")
	g.AddTransition("b", End)
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
	g.AddTransition("after", End)
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
	g.AddTransition("b", End)    // b doesn't
	g.AddTransition("join", End)
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
	g.AddTransition("join", End)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
}

func TestLineage_SetFanInOnUnknownNodeRejected(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("BadFaninName")
	g.AddTransition("a", "b")
	g.AddTransition("b", End)
	g.SetFanIn("c")
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "unknown task")
}

func TestLineage_FanInFlagSurvivesJSONRoundTrip(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Roundtrip")
	g.AddTransition("s", "a")
	g.AddTransition("s", "b")
	g.AddTransition("a", "join")
	g.AddTransition("b", "join")
	g.AddTransition("join", End)
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
	g.AddTransition("a", End)
	g.AddTransition("b", End)
	g.AddTransition("c", End)
	assert.NoError(g.Validate())
	// Switch source must not be treated as a fan-out source.
	assert.Expect(g.IsFanOutSource("router"), false)
}

func TestGraph_SwitchRejectsEmptyWhen(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchNoWhen")
	g.AddTransitionSwitch("router", "a", "")
	g.AddTransition("a", End)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "requires a 'when' expression")
}

func TestGraph_SwitchRejectsMixWithPlain(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchMixed")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransition("router", "b")
	g.AddTransition("a", End)
	g.AddTransition("b", End)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch transition")
}

func TestGraph_SwitchRejectsMixWithWhen(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchVsWhen")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionWhen("router", "b", "i==2")
	g.AddTransition("a", End)
	g.AddTransition("b", End)
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch transition")
}

func TestGraph_SwitchAllowedWithGoto(t *testing.T) {
	assert := testarossa.For(t)

	// A goto is an explicit task-requested override; it preempts a switch
	// evaluation at runtime, so the two kinds can coexist from one source.
	g := NewGraph("SwitchWithGoto")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "true")
	g.AddTransitionGoto("router", "c")
	g.AddTransition("a", End)
	g.AddTransition("b", End)
	g.AddTransition("c", End)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchAllowedWithOnError(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchWithOnerror")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "true")
	g.AddTransitionOnError("router", "handler")
	g.AddTransition("a", End)
	g.AddTransition("b", End)
	g.AddTransition("handler", End)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchNoFanInRequired(t *testing.T) {
	assert := testarossa.For(t)

	// Three Switch branches from one node, no SetFanIn anywhere: validator must accept.
	g := NewGraph("SwitchNoFanin")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "i==2")
	g.AddTransitionSwitch("router", "c", "true")
	g.AddTransition("a", End)
	g.AddTransition("b", End)
	g.AddTransition("c", End)
	assert.NoError(g.Validate())
}

func TestGraph_SwitchRejectsForEach(t *testing.T) {
	assert := testarossa.For(t)

	// A switch transition with ForEach set is constructed directly to bypass the
	// constructor; the validator must reject it.
	g := NewGraph("SwitchForeach")
	g.AddTransition("router", "a")
	g.AddTransition("a", End)
	g.transitions = append(g.transitions, Transition{From: "router", To: "a", Kind: TransitionSwitch, When: "true", ForEach: "items", As: "item"})
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "switch")
}

func TestGraph_SwitchMermaidDiamond(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("SwitchRender")
	g.AddTransitionSwitch("router", "a", "i==1")
	g.AddTransitionSwitch("router", "b", "true")
	g.AddTransition("a", End)
	g.AddTransition("b", End)
	assert.NoError(g.Validate())
	m := NewGraphRenderer(g).Render()
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
	g.AddTransition("join", End)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
	m := NewGraphRenderer(g).Render()

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
	g.AddTransition("join", End)
	g.SetFanIn("join")
	assert.NoError(g.Validate())
	m := NewGraphRenderer(g).Render()
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

	// A malicious workflow author names a task and a when-expression with Mermaid/HTML metacharacters,
	// aiming to break out of the ["..."] label and inject node/edge/click syntax or an <img> XSS payload
	// into a host UI that renders the diagram.
	g := NewGraph("Order")
	evil := "svc/pwn\"]:::term click n \"http\" `x` <img src=q onerror=alert(1)> [z] {q}"
	g.AddTransitionWhen(evil, "svc/next", `a || b == "c"`)
	g.AddTransition("svc/next", End)

	m := NewGraphRenderer(g).Render()

	// No raw metacharacter survives to end a label or inject markup/directives.
	assert.NotContains(m, `<img`)
	assert.NotContains(m, "`x`")
	assert.NotContains(m, `"]:::term`)
	// Metacharacters are rewritten to Mermaid character references in node and edge labels.
	assert.Contains(m, `#lt;img`)
	assert.Contains(m, `#quot;`)
	assert.Contains(m, `#96;`)  // backticks
	assert.Contains(m, `#124;`) // the || in the when-expression
}

func TestFlow_MermaidEscapesInjection(t *testing.T) {
	assert := testarossa.For(t)

	// Malicious task name and subgraph (workflow) name carried through a flow's rendered history.
	steps := []FlowStep{
		{
			StepKey:  "1-1-a",
			TaskName: "svc/pwn\"]:::term <img src=q onerror=alert(1)> |z|",
			Status:   StatusCompleted,
		},
		{
			StepKey:        "1-2-b",
			TaskName:       "svc/caller",
			PredecessorKey: "1-1-a",
			Subgraph:       true,
			SubgraphName:   `Evil"]<img>`,
			SubHistory:     []FlowStep{{StepKey: "1-3-c", TaskName: "svc/inner", Status: StatusCompleted}},
			Status:         StatusCompleted,
		},
	}

	m := NewFlowRenderer(steps).Render()

	// No raw metacharacter survives in node labels or the subgraph block title.
	assert.NotContains(m, `<img`)
	assert.NotContains(m, `"]:::term`)
	// Escaped forms are emitted instead.
	assert.Contains(m, `#lt;img`)
	assert.Contains(m, `#quot;`)
}

// stripProto is shared by the graph's error messages and the flow renderer's node labels. Both sinks are human-facing, so a URL
// with nothing after the scheme is returned WHOLE rather than as the empty string it strips down to - a
// blank diagram label, or an error naming no task at all, is worse than a slightly ugly one.
func TestStripProto(t *testing.T) {
	assert := testarossa.For(t)

	assert.Equal("host:443/task", stripProto("https://host:443/task"))
	assert.Equal("host:443/task", stripProto("host:443/task")) // no scheme: unchanged
	assert.Equal("", stripProto(""))
	assert.Equal("x://", stripProto("x://")) // nothing after the scheme: kept, not blanked
	assert.Equal("b://c", stripProto("a://b://c"))
}

// TestGraph_ValidateUnreachableStripsProto pins that the reachability error names its tasks as every other
// Validate message does, without the scheme.
func TestGraph_ValidateUnreachableStripsProto(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Unreachable")
	g.SetURL("https://svc/a", "svc/a")
	g.SetURL("https://svc/b", "svc/b")
	g.AddTransition("https://svc/a", End)
	g.AddTransition("https://svc/b", End) // nothing reaches it
	err := g.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "task 'svc/b' is not reachable from entry point 'svc/a'")
	}
}

// TestGraph_ValidateLineageStripsProto pins that the lineage messages print the fan-out frames as task names
// without their protocol, as every other Validate message does.
func TestGraph_ValidateLineageStripsProto(t *testing.T) {
	assert := testarossa.For(t)

	unpopped := NewGraph("EndUnpopped")
	unpopped.AddTransition("https://svc/s", "https://svc/a")
	unpopped.AddTransition("https://svc/s", "https://svc/b")
	unpopped.AddTransition("https://svc/a", "https://svc/join")
	unpopped.AddTransition("https://svc/b", End)
	unpopped.AddTransition("https://svc/join", End)
	unpopped.SetFanIn("https://svc/join")
	err := unpopped.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "unpopped fan-out frames [svc/s]")
	}

	divergent := NewGraph("DivergentStacks")
	divergent.AddTransition("https://svc/s", "https://svc/a")
	divergent.AddTransition("https://svc/s", "https://svc/b")
	divergent.AddTransition("https://svc/a", "https://svc/shared")
	divergent.AddTransition("https://svc/b", "https://svc/shared")
	divergent.AddTransition("https://svc/shared", "https://svc/join")
	divergent.AddTransitionGoto("https://svc/join", "https://svc/shared")
	divergent.AddTransition("https://svc/join", End)
	divergent.SetFanIn("https://svc/join")
	err = divergent.Validate()
	if assert.Error(err) {
		assert.Contains(err.Error(), "two different lineage stacks ([svc/s] and [])")
	}
}

// validateLineage memoizes IsFanOutSource rather than re-implementing the fan-out predicate, because the
// engine ROUTES on IsFanOutSource (internal/faninmap, processStep): a validator that disagreed with the
// router about what fans out would bless a graph the engine then mis-executes. This pins the agreement
// across every shape the predicate distinguishes.
func TestGraph_ValidatorAgreesWithIsFanOutSource(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("Shapes")
	g.AddTransition("linear", "two")               // 1 outgoing: not a fan-out
	g.AddTransitionFanOut("two", "l", "r")         // 2 outgoing: fan-out
	g.AddTransitionForEach("l", "each", "xs", "x") // forEach: fan-out even at 1 outgoing
	g.AddTransitionSwitch("r", "s1", "a==1")       // switch: exclusive, NOT a fan-out
	g.AddTransitionSwitch("r", "s2", "true")
	g.AddTransitionOnError("linear", "handler") // onError: never counts
	g.AddTransitionGoto("linear", "handler")    // goto: never counts

	assert.False(g.IsFanOutSource("linear"), "one success edge plus onError/goto is not a fan-out")
	assert.True(g.IsFanOutSource("two"))
	assert.True(g.IsFanOutSource("l"), "a forEach edge fans out on its own")
	assert.False(g.IsFanOutSource("r"), "switch branches are exclusive")
}

// TestRender_NilGraphAndEmptyHistory pins the two degenerate inputs, the only plausible candidates for an error
// return. Rendering builds into a strings.Builder, whose writes cannot fail, so Render
// returns no error at all.
func TestRender_NilGraphAndEmptyHistory(t *testing.T) {
	assert := testarossa.For(t)

	// A nil graph renders empty rather than panicking.
	assert.Equal("", NewGraphRenderer(nil).Render())
	assert.Equal("", NewGraphRenderer(nil).WithLeftRight().Render())

	// An empty history renders a well-formed (if bare) flowchart.
	assert.Contains(NewFlowRenderer(nil).Render(), "flowchart")

	// A graph that would FAIL Validate still renders: a diagram is a diagnostic, and the moment an author most
	// wants to see a graph is the moment it is malformed.
	g := NewGraph("Broken")
	g.SetURL("A", "svc/a")
	g.AddTransition("A", "Undeclared") // no END edge, target never declared -> Validate rejects it
	assert.Error(g.Validate())
	mmd := NewGraphRenderer(g).Render()
	assert.Contains(mmd, "A")
	assert.Contains(mmd, "Undeclared")
}

func TestGraph_ValidateRejectsUnknownReducer(t *testing.T) {
	assert := testarossa.For(t)

	g := NewGraph("UnknownReducer")
	g.AddTransition("a", End)
	g.SetReducer("total", Reducer("sum"))
	err := g.Validate()
	assert.Error(err)
	assert.Contains(err.Error(), "sum")

	g.SetReducer("total", ReducerAdd)
	assert.NoError(g.Validate())
}

// TestGraphRenderer_ConditionalForEachLabel pins that a forEach edge carrying a when expression shows the
// condition, rather than drawing as an unconditional fan-out.
func TestGraphRenderer_ConditionalForEachLabel(t *testing.T) {
	assert := testarossa.For(t)

	var g Graph
	assert.NoError(json.Unmarshal([]byte(`{"name":"G","entryPoint":"S","tasks":[{"name":"S"},{"name":"W"},{"name":"J","fanIn":true}],`+
		`"transitions":[{"from":"S","to":"W","forEach":"items","as":"item","when":"go == true"},{"from":"W","to":"J"},{"from":"J","to":"END"}]}`), &g))
	out := NewGraphRenderer(&g).Render()
	assert.Contains(out, "for each when go == true")
}

// TestStripHostPort_NeverBlank pins that a label stripped of its own host keeps something to show when the
// URL has no path.
func TestStripHostPort_NeverBlank(t *testing.T) {
	assert := testarossa.For(t)

	assert.Equal("task", stripHostPort("https://host/task", "host"))
	assert.Equal("host", stripHostPort("https://host", "host"))
	assert.Equal("other/task", stripHostPort("https://other/task", "host"))
	assert.Equal("Plain", stripHostPort("Plain", "host"))
}

// TestTransition_ForEachAliasDefaultsOnDecode pins that a forEach transition decoded without an `as` takes "item", as
// AddTransitionForEach gives it. The engine strips a cohort's branch-private names at its fan-in under the stored
// alias, so a JSON graph that left it out kept item, itemIndex and itemCount past the fan-in.
func TestTransition_ForEachAliasDefaultsOnDecode(t *testing.T) {
	assert := testarossa.For(t)

	var tr Transition
	assert.NoError(json.Unmarshal([]byte(`{"from":"S","to":"W","forEach":"items"}`), &tr))
	assert.Equal("item", tr.As)
	assert.NoError(json.Unmarshal([]byte(`{"from":"S","to":"W","forEach":"items","as":"page"}`), &tr))
	assert.Equal("page", tr.As)
	tr = Transition{}
	assert.NoError(json.Unmarshal([]byte(`{"from":"S","to":"W"}`), &tr))
	assert.Equal("", tr.As, "a transition with no forEach takes no alias")
}
