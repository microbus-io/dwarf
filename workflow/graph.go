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
	"maps"
	"strings"

	"github.com/microbus-io/boolexp"
	"github.com/microbus-io/errors"
)

// END is a pseudo-node indicating that the workflow should terminate.
// Use it as the target of a transition to mark a terminal path.
const END = "END"

// Node describes a task or subgraph node registered in a workflow graph.
// Name is the node's identifier within the graph and the value stored on
// step rows (dwarf_steps.task_name). URL is the dispatch target the
// engine calls when the node is reached.
type Node struct {
	Name string
	URL  string
}

// Transition defines a possible transition between two nodes in a workflow graph.
// From and To are node names, not URLs.
type Transition struct {
	From     string `json:"from"`
	To       string `json:"to"`
	When     string `json:"when,omitzero"`
	WithGoto bool   `json:"withGoto,omitzero"`
	ForEach  string `json:"forEach,omitzero"` // dynamic fan-out over a state field
	As       string `json:"as,omitzero"`      // alias for the current element during forEach fan-out
	OnError  bool   `json:"onError,omitzero"` // taken when the source task returns an error
	Switch   bool   `json:"switch,omitzero"`  // first-match-wins among siblings; never fans out
}

// Graph is the definition of a workflow. It describes the tasks, transitions between them,
// and reducers for merging state during fan-in.
type Graph struct {
	name        string
	entryPoint  string
	nodes       []Node
	transitions []Transition
	reducers    map[string]Reducer
	fanInNodes  map[string]bool
}

// NewGraph creates a new workflow graph with the given display name. The name is a human-friendly
// label (surfaced in rendering and Validate error messages); it is NOT the resolve key. The value
// passed to Create/Run and to the host's LoadGraph is a separate opaque URL that the engine stores on
// the flow (workflow_url) - it is never kept on the graph itself.
func NewGraph(name string) *Graph {
	return &Graph{
		name: name,
	}
}

// Name returns the graph's display name.
func (g *Graph) Name() string {
	return g.name
}

// EntryPoint returns the node name of the entry point of the graph.
func (g *Graph) EntryPoint() string {
	return g.entryPoint
}

// Nodes returns the list of nodes in the graph.
func (g *Graph) Nodes() []Node {
	result := make([]Node, len(g.nodes))
	copy(result, g.nodes)
	return result
}

// Transitions returns the list of transitions in the graph. The returned slice
// shares the graph's underlying storage; callers must not mutate it. The graph
// is treated as immutable after Validate, so read-only iteration is safe.
func (g *Graph) Transitions() []Transition {
	return g.transitions
}

// SetEndpoint binds a node (identified by its graph name) to the given dispatch URL, creating the node
// if it does not exist and updating its URL if it does. The name is the node's identity in the graph
// (used by transitions, fan-in, goto); the URL is the opaque downstream endpoint the engine
// hands to the host's ExecuteTask and groups the saturation/concurrency metric by. The first node bound
// becomes the default entry point unless SetEntryPoint is called explicitly. The pseudo-node END is not
// registered.
//
// The same URL may be bound under multiple names. This is how a workflow author reuses the same task
// code at distinct positions in the graph with different downstream transitions per position.
func (g *Graph) SetEndpoint(name, url string) {
	if name == END {
		return
	}
	for i := range g.nodes {
		if g.nodes[i].Name == name {
			g.nodes[i].URL = url
			return
		}
	}
	g.nodes = append(g.nodes, Node{Name: name, URL: url})
	if g.entryPoint == "" {
		g.entryPoint = name
	}
}

// URLOf returns the dispatch URL for a node identified by name. Returns the empty string
// if the name is not registered. END maps to itself.
func (g *Graph) URLOf(name string) string {
	if name == END {
		return END
	}
	for _, n := range g.nodes {
		if n.Name == name {
			return n.URL
		}
	}
	return ""
}

// SetEntryPoint sets the entry point of the graph explicitly, overriding the default
// (first task added). The argument is a node name.
func (g *Graph) SetEntryPoint(name string) {
	g.entryPoint = name
}

// AddTransition adds an unconditional transition between two nodes. Both endpoints are
// auto-registered as tasks if not already present (see autoRegister).
func (g *Graph) AddTransition(from, to string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to})
}

// AddTransitionWhen adds a conditional transition between two nodes: the edge is taken when the 'when'
// expression evaluates true against the flow's state.
//
// Every When transition from a node is evaluated INDEPENDENTLY, so two of them are a conditional
// PARALLEL FAN-OUT, not an if/else - if both conditions hold, both branches run. Even when they are
// mutually exclusive by construction, the source is still a fan-out source, so it requires a downstream
// convergence node marked with SetFanIn - Validate rejects the graph otherwise, complaining that a branch
// reached END with an unpopped fan-out frame.
//
// For an if/else - exactly one branch runs, no fan-in needed - use AddTransitionSwitch, which is
// first-match-wins:
//
//	// WRONG: a fan-out that happens to have exclusive conditions, and fails Validate without a fan-in.
//	g.AddTransitionWhen("Check", "Approve", "score > 0.8")
//	g.AddTransitionWhen("Check", "Reject", "score <= 0.8")
//
//	// RIGHT: exactly one of these fires.
//	g.AddTransitionSwitch("Check", "Approve", "score > 0.8")
//	g.AddTransitionSwitch("Check", "Reject", "true")   // catch-all
//
// Use When when a genuinely parallel fan-out should be conditional (each branch opts in on its own
// condition, all surviving branches converging on one SetFanIn node).
func (g *Graph) AddTransitionWhen(from, to string, when string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, When: when})
}

// AddTransitionSwitch adds a first-match-wins transition between two nodes. Multiple
// Switch transitions from the same source are evaluated in registration order and only
// the first whose 'when' expression evaluates true fires; the rest are skipped. If no
// Switch matches the flow ends at the source node, so the last Switch from a node is
// typically a catch-all with when="true". Only one branch ever runs, so a downstream
// SetFanIn is not required.
//
// A node that uses Switch transitions must declare every successful-path outgoing
// transition as Switch (the validator rejects mixing Switch with When/plain/ForEach
// from the same source). OnError and Goto transitions are orthogonal and remain allowed.
func (g *Graph) AddTransitionSwitch(from, to string, when string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, When: when, Switch: true})
}

// AddTransitionGoto adds a transition that is only taken when the source task calls
// flow.Goto with a target that resolves to this transition's destination.
func (g *Graph) AddTransitionGoto(from, to string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, WithGoto: true})
}

// AddTransitionForEach adds a dynamic fan-out transition: 'forEach' names a state field holding an ARRAY,
// and the engine spawns one parallel instance of the 'to' task per element.
//
// Each branch's state is the flow's state at the fan-out, plus three injected fields:
//
//	<as>        the element itself ('as' defaults to "item" when empty)
//	<as>Index   the element's 0-based position in the array
//	<as>Count   the number of elements, i.e. the cohort size
//
// The branches must converge on a single node marked with SetFanIn (Validate rejects a fan-out that does
// not), where their outputs are merged by the fields' reducers - see SetReducer, and note that a branch
// writing to a reducer-managed field writes its DELTA, not the accumulated value. The three injected
// fields do not survive the fan-out: they are branch-private, so the flow's state past the fan-in - and
// the final state of a flow whose fan-out failed - carries none of them. Forward an element value under a
// different key if a downstream task needs it.
//
// An EMPTY array spawns no branches, but the flow does NOT stop there: it routes straight to the fan-in
// node, which runs with the source task's own state and output and no branch contributions. So a fan-in
// task must tolerate a cohort of zero (a reducer-managed field simply keeps its incoming value, and any
// per-element output it expected is absent), and the branch task must tolerate never running at all.
//
// Every branch carries the source array (it is ordinary flow state), so an N-element fan-out over a chain
// of depth D stores N*D copies of it. For a large array, a branch can drop it with f.Set(<forEach>, nil),
// which removes it from the flow's state past the fan-in.
func (g *Graph) AddTransitionForEach(from, to string, forEach string, as string) {
	g.autoRegister(from)
	g.autoRegister(to)
	if as == "" {
		as = "item"
	}
	g.transitions = append(g.transitions, Transition{From: from, To: to, ForEach: forEach, As: as})
}

// AddTransitionOnError adds a transition that is taken when the source task returns an error, instead of
// failing the flow. It fires on ANY error - the engine never inspects the status code or the text - and it
// preempts every other transition from that node, so it cannot combine with when/forEach/goto.
//
// The error is delivered to the handler in the state field "onErr" (a structured error: message, status
// code, trace id, properties; the stack frames are stripped).
//
// AN ERROR VOIDS THE TASK'S CHANGES. Whatever the failing task wrote with Set before it returned the error
// is discarded - the handler does not see it, and it never reaches the flow's final state. (The same is true
// when no handler is declared and the flow fails.) This mirrors Go's own convention that an error voids the
// other results, and it is forced by at-least-once execution: a task whose worker loses its lease mid-body
// re-runs and RECOMPUTES its changes, so what a failing attempt wrote before it died is not a fact anything
// can be built on. To hand the handler something deliberately, put it in the error (it rides through in
// onErr), or give an external side effect its own task so its success is recorded durably before anything
// downstream can fail.
func (g *Graph) AddTransitionOnError(from, to string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, OnError: true})
}

// AddTransitionChain wires an unconditional transition between each consecutive pair of names:
// AddTransitionChain("A", "B", "C") is AddTransition("A", "B") followed by AddTransition("B", "C").
// It is a convenience for linear segments; fewer than two names is a no-op. END belongs last (a node
// after END would produce an invalid transition out of END). Mix with the other AddTransition* methods
// for branching, conditions, and loops.
func (g *Graph) AddTransitionChain(names ...string) {
	for i := 0; i+1 < len(names); i++ {
		g.AddTransition(names[i], names[i+1])
	}
}

// AddTransitionFanOut wires an unconditional transition from one source to each of several destinations:
// AddTransitionFanOut("A", "B", "C") is AddTransition("A", "B") followed by AddTransition("A", "C"), so B
// and C both fire and run in parallel. It is a convenience for static fan-out; no destinations is a no-op.
// It creates only the outgoing edges - if the branches later rejoin at a node, that node still needs
// SetFanIn (and usually a reducer) wired separately. Distinct from AddTransitionForEach, which fans out
// dynamically over a runtime collection rather than across statically-named nodes.
func (g *Graph) AddTransitionFanOut(from string, to ...string) {
	for _, dest := range to {
		g.AddTransition(from, dest)
	}
}

// autoRegister registers a new node if one does not exist for the name.
func (g *Graph) autoRegister(name string) {
	if name == END {
		return
	}
	for _, n := range g.nodes {
		if n.Name == name {
			return
		}
	}
	g.SetEndpoint(name, name)
}

// ErrorTransition returns the error transition from the given node name, if one exists.
func (g *Graph) ErrorTransition(name string) (Transition, bool) {
	for _, tr := range g.transitions {
		if tr.From == name && tr.OnError {
			return tr, true
		}
	}
	return Transition{}, false
}

// SetFanIn marks a node as a fan-in nexus. Opts the graph into the lineage validator.
func (g *Graph) SetFanIn(name string) {
	if g.fanInNodes == nil {
		g.fanInNodes = make(map[string]bool)
	}
	g.fanInNodes[name] = true
}

// IsFanIn reports whether the named node is a fan-in nexus.
func (g *Graph) IsFanIn(name string) bool {
	return g.fanInNodes[name]
}

// IsFanOutSource reports whether the named node has 2+ non-goto/non-error outgoing
// transitions, or any forEach outgoing transition. Switch transitions are exclusive
// (only one branch ever fires) and therefore do not count toward fan-out.
func (g *Graph) IsFanOutSource(name string) bool {
	var normalCount int
	for _, tr := range g.transitions {
		if tr.From != name || tr.WithGoto || tr.OnError || tr.Switch {
			continue
		}
		if tr.ForEach != "" {
			return true
		}
		normalCount++
		if normalCount >= 2 {
			return true
		}
	}
	return false
}

// SetReducer sets the merge strategy for a state field during fan-in.
func (g *Graph) SetReducer(field string, reducer Reducer) {
	if g.reducers == nil {
		g.reducers = make(map[string]Reducer)
	}
	g.reducers[field] = reducer
}

// Reducers returns the reducer map for state fields. The returned map is a copy - like Nodes and
// Transitions, a getter must not hand out a live handle to the graph's internals, or a caller mutating the
// result would silently re-wire the fan-in merge of a graph already frozen onto running flows.
func (g *Graph) Reducers() map[string]Reducer {
	return maps.Clone(g.reducers)
}

// Validate checks the graph for structural errors.
func (g *Graph) Validate() error {
	if g.name == "" {
		return errors.New("graph name is required")
	}
	if len(g.nodes) == 0 {
		return errors.New("graph '%s' has no tasks", g.name)
	}
	nodeSet := make(map[string]bool, len(g.nodes)+1)
	nodeSet[END] = true
	for _, t := range g.nodes {
		if nodeSet[t.Name] {
			return errors.New("duplicate node '%s' in graph '%s'", t.Name, g.name)
		}
		nodeSet[t.Name] = true
		if t.URL == "" {
			return errors.New("node '%s' in graph '%s' has no URL", t.Name, g.name)
		}
	}
	if !nodeSet[g.entryPoint] {
		return errors.New("entry point '%s' is not a registered node in graph '%s'", g.entryPoint, g.name)
	}
	for fanInName := range g.fanInNodes {
		if !nodeSet[fanInName] {
			return errors.New("SetFanIn references unknown node '%s' in graph '%s'", fanInName, g.name)
		}
		if fanInName == END {
			return errors.New("SetFanIn cannot mark END in graph '%s'", g.name)
		}
	}
	onErrorFrom := make(map[string]bool, len(g.nodes))
	for _, tr := range g.transitions {
		if tr.From == END {
			return errors.New("transition out of END to '%s' in graph '%s'; END is terminal and has no outgoing transitions", tr.To, g.name)
		}
		if !nodeSet[tr.From] {
			return errors.New("transition from unknown node '%s' to '%s' in graph '%s'", tr.From, tr.To, g.name)
		}
		if !nodeSet[tr.To] {
			return errors.New("transition from '%s' to unknown node '%s' in graph '%s'", tr.From, tr.To, g.name)
		}
		if tr.ForEach != "" && tr.WithGoto {
			return errors.New("transition from '%s' to '%s' in graph '%s' cannot combine forEach and withGoto", tr.From, tr.To, g.name)
		}
		if tr.As != "" && tr.ForEach == "" {
			return errors.New("transition from '%s' to '%s' in graph '%s' has 'as' without 'forEach'", tr.From, tr.To, g.name)
		}
		if tr.OnError && (tr.ForEach != "" || tr.WithGoto) {
			return errors.New("transition from '%s' to '%s' in graph '%s' cannot combine onError with forEach or withGoto", tr.From, tr.To, g.name)
		}
		if tr.OnError && tr.When != "" {
			return errors.New("onError transition from '%s' to '%s' in graph '%s' cannot have a 'when' expression; onError is unconditional (it fires whenever the source task errors)", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.OnError {
			if onErrorFrom[tr.From] {
				return errors.New("node '%s' in graph '%s' has more than one onError transition; only one is allowed (ErrorTransition takes the first)", stripProto(tr.From), g.name)
			}
			onErrorFrom[tr.From] = true
		}
		if tr.Switch && (tr.ForEach != "" || tr.WithGoto || tr.OnError) {
			return errors.New("transition from '%s' to '%s' in graph '%s' cannot combine switch with forEach, withGoto, or onError", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.Switch && tr.When == "" {
			return errors.New("switch transition from '%s' to '%s' in graph '%s' requires a 'when' expression (use \"true\" for the default branch)", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.OnError && tr.From == tr.To {
			return errors.New("transition from '%s' to itself in graph '%s' would loop unboundedly; use flow.Retry in the task body for bounded retries with backoff", stripProto(tr.From), g.name)
		}
		if tr.When != "" {
			err := boolexp.Validate(tr.When)
			if err != nil {
				return errors.New("transition from '%s' to '%s' in graph '%s' has invalid 'when' expression: %v", stripProto(tr.From), stripProto(tr.To), g.name, err)
			}
		}
	}

	hasSwitchFrom := make(map[string]bool, len(g.nodes))
	for _, tr := range g.transitions {
		if tr.Switch {
			hasSwitchFrom[tr.From] = true
		}
	}
	for _, tr := range g.transitions {
		if !hasSwitchFrom[tr.From] || tr.Switch || tr.OnError || tr.WithGoto {
			continue
		}
		return errors.New("node '%s' in graph '%s' mixes a switch transition with a non-switch success-path transition to '%s'; convert all outgoing success-path transitions to switch (use when=\"true\" for the default), or use withGoto for explicit overrides", stripProto(tr.From), g.name, stripProto(tr.To))
	}

	reachable := make(map[string]bool)
	queue := []string{g.entryPoint}
	reachable[g.entryPoint] = true
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, tr := range g.transitions {
			if tr.From == current && tr.To != END && !reachable[tr.To] {
				reachable[tr.To] = true
				queue = append(queue, tr.To)
			}
		}
	}
	for _, t := range g.nodes {
		if !reachable[t.Name] {
			return errors.New("node '%s' is not reachable from entry point '%s' in graph '%s'", t.Name, g.entryPoint, g.name)
		}
	}

	hasEnd := false
	for _, tr := range g.transitions {
		if tr.To == END {
			hasEnd = true
			break
		}
	}
	if !hasEnd {
		return errors.New("graph '%s' has no transition to END", g.name)
	}

	return g.validateLineage()
}

// validateLineage checks fan-out/fan-in convergence: every fan-out's branches must converge on the same
// fan-in, every branch must pop its frame before END, and no node may be reached with two different lineage
// stacks. It builds a local fan-out->fan-in map to run these checks and stores nothing on the graph; the
// engine derives the same map at dispatch (internal/faninmap) for routing.
func (g *Graph) validateLineage() error {
	fanOutToFanIn := make(map[string]string)

	// Memoized IsFanOutSource, not a second copy of it: the BFS below revisits a node once per incoming
	// edge, and this predicate must stay in lockstep with the engine's routing (internal/faninmap and
	// processStep both branch on IsFanOutSource) - a validator that disagreed with the router about what
	// fans out would bless a graph the engine then mis-executes.
	//
	// It is a SET, not a node->bool table: the "every fan-out source needs a fan-in" check below iterates
	// its KEYS, so storing an explicit false would enrol every node in the graph as a fan-out source.
	isFanOutSource := make(map[string]bool, len(g.nodes))
	for _, t := range g.nodes {
		if g.IsFanOutSource(t.Name) {
			isFanOutSource[t.Name] = true
		}
	}

	stacks := make(map[string][]string, len(g.nodes))
	queue := []string{g.entryPoint}
	stacks[g.entryPoint] = nil

	stackEqual := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	stackCopy := func(s []string) []string {
		if len(s) == 0 {
			return nil
		}
		c := make([]string, len(s))
		copy(c, s)
		return c
	}

	for len(queue) > 0 {
		from := queue[0]
		queue = queue[1:]
		fromStack := stacks[from]
		fromIsFanOut := isFanOutSource[from]

		for _, tr := range g.transitions {
			if tr.From != from {
				continue
			}
			var nextStack []string
			switch {
			// The fan-in arm MUST precede the goto/onError/switch arm. The engine treats any transition
			// whose target is a fan-in node as a cohort arrival - it does not care which kind of edge
			// carried it - so a withGoto/onError/switch edge into a fan-in from a node with no fan-out
			// frame is exactly the "arrive at a cohort that does not exist" shape, and at runtime it bumps
			// cohort_arrivals on step_id=0 and hot-loops the flow. Testing goto/onError/switch first (as
			// this switch once did) skipped the frame-pop check for precisely the edges that can reach a
			// fan-in from outside a cohort.
			case g.fanInNodes[tr.To]:
				var fanOutSource string
				if fromIsFanOut {
					nextStack = fromStack
					fanOutSource = from
				} else {
					if len(fromStack) == 0 {
						return errors.New(
							"transition from '%s' to fan-in node '%s' in graph '%s' has no fan-out frame to pop",
							stripProto(from), stripProto(tr.To), g.name,
						)
					}
					nextStack = stackCopy(fromStack[:len(fromStack)-1])
					fanOutSource = fromStack[len(fromStack)-1]
				}
				// All branches of one fan-out must converge on the SAME fan-in. The cohort shares a spawn
				// step, and the cohort-resolution path picks the fan-in from whichever sibling completes
				// last - so divergent fan-in targets make the convergence node nondeterministic. A prior
				// mapping to a different fan-in is that divergence.
				if prior, ok := fanOutToFanIn[fanOutSource]; ok && prior != tr.To {
					return errors.New(
						"fan-out source '%s' in graph '%s' has branches converging on different fan-in nodes ('%s' and '%s'); all siblings of a fan-out must converge on the same fan-in",
						stripProto(fanOutSource), g.name, stripProto(prior), stripProto(tr.To),
					)
				}
				fanOutToFanIn[fanOutSource] = tr.To
			case fromIsFanOut:
				nextStack = append(stackCopy(fromStack), from)
			default:
				nextStack = fromStack
			}

			if tr.To == END {
				if len(nextStack) != 0 {
					return errors.New(
						"transition from '%s' to END in graph '%s' has unpopped fan-out frames %v; every branch must pass through a fan-in node before reaching END",
						stripProto(from), g.name, nextStack,
					)
				}
				continue
			}

			if prior, seen := stacks[tr.To]; seen {
				if !stackEqual(prior, nextStack) {
					return errors.New(
						"node '%s' in graph '%s' is reachable with two different lineage stacks (%v and %v); register a separate alias node via SetEndpoint to disambiguate",
						stripProto(tr.To), g.name, prior, nextStack,
					)
				}
				continue
			}
			stacks[tr.To] = nextStack
			queue = append(queue, tr.To)
		}
	}

	for source := range isFanOutSource {
		if _, ok := fanOutToFanIn[source]; !ok {
			return errors.New(
				"fan-out source '%s' in graph '%s' has no fan-in node downstream; mark the convergence node with SetFanIn",
				stripProto(source), g.name,
			)
		}
	}

	return nil
}

// stripProto removes the scheme prefix from a URL-like string, for cleaner error messages and diagram
// labels. A string with nothing after the scheme ("x://") is returned whole rather than as the empty
// string it strips down to - both sinks are human-facing, and a blank label or a message naming no task
// is worse than a slightly ugly one.
func stripProto(s string) string {
	var x string
	if _, x, _ = strings.Cut(s, "://"); x == "" {
		x = s
	}
	return x
}

// MarshalJSON serializes the graph to JSON.
func (g *Graph) MarshalJSON() ([]byte, error) {
	type jsonTask struct {
		Name  string `json:"name"`
		URL   string `json:"url,omitzero"`
		FanIn bool   `json:"fanIn,omitzero"`
	}
	jsonTasks := make([]jsonTask, len(g.nodes))
	for i, t := range g.nodes {
		jsonTasks[i] = jsonTask{Name: t.Name, URL: t.URL, FanIn: g.fanInNodes[t.Name]}
	}
	type jsonGraph struct {
		Name        string             `json:"name,omitzero"`
		EntryPoint  string             `json:"entryPoint"`
		Tasks       []jsonTask         `json:"tasks"`
		Transitions []Transition       `json:"transitions"`
		Reducers    map[string]Reducer `json:"reducers,omitzero"`
	}
	jg := jsonGraph{
		Name:        g.name,
		EntryPoint:  g.entryPoint,
		Tasks:       jsonTasks,
		Transitions: g.transitions,
		Reducers:    g.reducers,
	}
	if jg.Tasks == nil {
		jg.Tasks = []jsonTask{}
	}
	if jg.Transitions == nil {
		jg.Transitions = []Transition{}
	}
	return json.Marshal(jg)
}

// UnmarshalJSON deserializes the graph from JSON.
func (g *Graph) UnmarshalJSON(data []byte) error {
	type jsonTask struct {
		Name  string `json:"name"`
		URL   string `json:"url,omitzero"`
		FanIn bool   `json:"fanIn,omitzero"`
	}
	type jsonGraph struct {
		Name        string             `json:"name,omitzero"`
		EntryPoint  string             `json:"entryPoint"`
		Tasks       []jsonTask         `json:"tasks"`
		Transitions []Transition       `json:"transitions"`
		Reducers    map[string]Reducer `json:"reducers,omitzero"`
	}
	var jg jsonGraph
	err := json.Unmarshal(data, &jg)
	if err != nil {
		return err
	}
	g.name = jg.Name
	g.entryPoint = jg.EntryPoint
	g.nodes = make([]Node, len(jg.Tasks))
	g.fanInNodes = nil
	for i, jt := range jg.Tasks {
		g.nodes[i].Name = jt.Name
		g.nodes[i].URL = jt.URL
		if g.nodes[i].URL == "" {
			g.nodes[i].URL = jt.Name
		}
		if jt.FanIn {
			if g.fanInNodes == nil {
				g.fanInNodes = make(map[string]bool)
			}
			g.fanInNodes[jt.Name] = true
		}
	}
	g.transitions = jg.Transitions
	g.reducers = jg.Reducers
	return nil
}
