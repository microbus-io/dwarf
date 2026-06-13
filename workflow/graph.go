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
	"net/http"
	"strings"

	"github.com/microbus-io/boolexp"
	"github.com/microbus-io/errors"
)

// END is a pseudo-node indicating that the workflow should terminate.
// Use it as the target of a transition to mark a terminal path.
const END = "END"

// Node describes a task or subgraph node registered in a workflow graph.
// Name is the node's identifier within the graph and the value stored on
// step rows (microbus_steps.task_name). URL is the dispatch target the
// engine calls when the node is reached.
type Node struct {
	Name string
	URL  string
}

// Transition defines a possible transition between two nodes in a workflow graph.
// From and To are node names, not URLs.
type Transition struct {
	From       string `json:"from"`
	To         string `json:"to"`
	When       string `json:"when,omitzero"`
	WithGoto   bool   `json:"withGoto,omitzero"`
	ForEach    string `json:"forEach,omitzero"` // dynamic fan-out over a state field
	As         string `json:"as,omitzero"`      // alias for the current element during forEach fan-out
	OnError    bool   `json:"onError,omitzero"` // taken when the source task returns an error
	Switch     bool   `json:"switch,omitzero"`  // first-match-wins among siblings; never fans out
	StatusCode int    `json:"statusCode,omitzero"`
}

// Graph is the definition of a workflow. It describes the tasks, transitions between them,
// and reducers for merging state during fan-in.
type Graph struct {
	name          string
	entryPoint    string
	nodes         []Node
	transitions   []Transition
	reducers      map[string]Reducer
	fanInNodes    map[string]bool
	fanOutToFanIn map[string]string // populated by Validate
	annotations   map[string]string // node name -> annotation text rendered as a note under the node
}

// NewGraph creates a new workflow graph with the given name.
func NewGraph(name string) *Graph {
	return &Graph{
		name: name,
	}
}

// Name returns the name of the graph.
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

// AddTask registers a task node in the graph under the given name, with the given URL as
// the dispatch target. The first node added becomes the default entry point unless
// SetEntryPoint is called explicitly. The pseudo-node END is not registered. Re-registering
// the same name is a no-op.
//
// The same URL may be registered under multiple names. This is how a workflow author
// reuses the same task code at distinct positions in the graph with different downstream
// transitions per position.
func (g *Graph) AddTask(name, url string) {
	if name == END {
		return
	}
	for i := range g.nodes {
		if g.nodes[i].Name == name {
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

// NamesForURL returns all node names whose dispatch URL matches the given URL.
// Empty result means no node uses that URL. Multiple results mean the URL is reused
// at distinct graph positions.
func (g *Graph) NamesForURL(url string) []string {
	var names []string
	for _, n := range g.nodes {
		if n.URL == url {
			names = append(names, n.Name)
		}
	}
	return names
}

// SetEntryPoint sets the entry point of the graph explicitly, overriding the default
// (first task added). The argument is a node name.
func (g *Graph) SetEntryPoint(name string) {
	g.entryPoint = name
}

// AddTransition adds an unconditional transition between two nodes. Both endpoints are
// auto-registered as tasks if not already present (see autoRegister).
func (g *Graph) AddTransition(from, to string) {
	from = g.autoRegister(from)
	to = g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to})
}

// AddTransitionWhen adds a conditional transition between two nodes.
func (g *Graph) AddTransitionWhen(from, to string, when string) {
	from = g.autoRegister(from)
	to = g.autoRegister(to)
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
// transition as Switch (the validator rejects mixing Switch with When/plain/ForEach/Goto
// from the same source). OnError transitions are orthogonal and remain allowed.
func (g *Graph) AddTransitionSwitch(from, to string, when string) {
	from = g.autoRegister(from)
	to = g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, When: when, Switch: true})
}

// AddTransitionGoto adds a transition that is only taken when the source task calls
// flow.Goto with a target that resolves to this transition's destination.
func (g *Graph) AddTransitionGoto(from, to string) {
	from = g.autoRegister(from)
	to = g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, WithGoto: true})
}

// AddTransitionForEach adds a dynamic fan-out transition.
func (g *Graph) AddTransitionForEach(from, to string, forEach string, as string) {
	from = g.autoRegister(from)
	to = g.autoRegister(to)
	if as == "" {
		as = "item"
	}
	g.transitions = append(g.transitions, Transition{From: from, To: to, ForEach: forEach, As: as})
}

// AddTransitionOnError adds a transition that is taken when the source task returns an error.
func (g *Graph) AddTransitionOnError(from, to string) {
	from = g.autoRegister(from)
	to = g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, OnError: true})
}

// AddTransitionOnTimeout adds an error transition that is taken only when the source task's
// error carries HTTP status 408.
func (g *Graph) AddTransitionOnTimeout(from, to string) {
	from = g.autoRegister(from)
	to = g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, OnError: true, StatusCode: http.StatusRequestTimeout})
}

// autoRegister resolves a transition endpoint string to a node name, registering a new
// node if needed.
func (g *Graph) autoRegister(s string) string {
	if s == END {
		return END
	}
	for _, n := range g.nodes {
		if n.Name == s {
			return n.Name
		}
	}
	matches := g.NamesForURL(s)
	if len(matches) == 1 {
		return matches[0]
	}
	g.AddTask(s, s)
	return s
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

// HasFanIn reports whether the graph declares any fan-in nexus.
func (g *Graph) HasFanIn() bool {
	return len(g.fanInNodes) > 0
}

// FanInFor returns the fan-in node that pops the frame pushed by a fan-out at the named
// source, or "" if the source is not a fan-out. Populated by Validate.
func (g *Graph) FanInFor(fanOutSource string) string {
	return g.fanOutToFanIn[fanOutSource]
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

// Reducers returns the reducer map for state fields.
func (g *Graph) Reducers() map[string]Reducer {
	return g.reducers
}

// Annotate attaches a short note to a node. The note renders as a teal,
// borderless text label directly beneath the node in the Mermaid diagram.
func (g *Graph) Annotate(name string, note string) {
	if g.annotations == nil {
		g.annotations = make(map[string]string)
	}
	if note == "" {
		delete(g.annotations, name)
		return
	}
	g.annotations[name] = note
}

// Annotation returns the note attached to a node via Annotate, or "" if none.
func (g *Graph) Annotation(name string) string {
	return g.annotations[name]
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
	for _, tr := range g.transitions {
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
		if tr.Switch && (tr.ForEach != "" || tr.WithGoto || tr.OnError) {
			return errors.New("transition from '%s' to '%s' in graph '%s' cannot combine switch with forEach, withGoto, or onError", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.Switch && tr.When == "" {
			return errors.New("switch transition from '%s' to '%s' in graph '%s' requires a 'when' expression (use \"true\" for the default branch)", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.StatusCode != 0 && !tr.OnError {
			return errors.New("transition from '%s' to '%s' in graph '%s' sets statusCode without onError", tr.From, tr.To, g.name)
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

// validateLineage runs when SetFanIn is declared.
// Side effect: populates g.fanOutToFanIn.
func (g *Graph) validateLineage() error {
	g.fanOutToFanIn = make(map[string]string)

	isFanOutSource := make(map[string]bool, len(g.nodes))
	for _, t := range g.nodes {
		var normalCount int
		var hasForEach bool
		for _, tr := range g.transitions {
			if tr.From != t.Name || tr.WithGoto || tr.OnError || tr.Switch {
				continue
			}
			normalCount++
			if tr.ForEach != "" {
				hasForEach = true
			}
		}
		if normalCount >= 2 || hasForEach {
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
			case tr.WithGoto, tr.OnError, tr.Switch:
				nextStack = fromStack
			case g.fanInNodes[tr.To]:
				if fromIsFanOut {
					nextStack = fromStack
					g.fanOutToFanIn[from] = tr.To
				} else {
					if len(fromStack) == 0 {
						return errors.New(
							"transition from '%s' to fan-in node '%s' in graph '%s' has no fan-out frame to pop",
							stripProto(from), stripProto(tr.To), g.name,
						)
					}
					nextStack = stackCopy(fromStack[:len(fromStack)-1])
					g.fanOutToFanIn[fromStack[len(fromStack)-1]] = tr.To
				}
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
						"node '%s' in graph '%s' is reachable with two different lineage stacks (%v and %v); register a separate alias node via AddTask to disambiguate",
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
		if _, ok := g.fanOutToFanIn[source]; !ok {
			return errors.New(
				"fan-out source '%s' in graph '%s' has no fan-in node downstream; mark the convergence node with SetFanIn",
				stripProto(source), g.name,
			)
		}
	}

	return nil
}

// stripProto removes the scheme prefix from a URL-like string for cleaner error messages.
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
		Name          string             `json:"name"`
		EntryPoint    string             `json:"entryPoint"`
		Tasks         []jsonTask         `json:"tasks"`
		Transitions   []Transition       `json:"transitions"`
		Reducers      map[string]Reducer `json:"reducers,omitzero"`
		FanOutToFanIn map[string]string  `json:"fanOutToFanIn,omitzero"`
	}
	jg := jsonGraph{
		Name:          g.name,
		EntryPoint:    g.entryPoint,
		Tasks:         jsonTasks,
		Transitions:   g.transitions,
		Reducers:      g.reducers,
		FanOutToFanIn: g.fanOutToFanIn,
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
		Name          string             `json:"name"`
		EntryPoint    string             `json:"entryPoint"`
		Tasks         []jsonTask         `json:"tasks"`
		Transitions   []Transition       `json:"transitions"`
		Reducers      map[string]Reducer `json:"reducers,omitzero"`
		FanOutToFanIn map[string]string  `json:"fanOutToFanIn,omitzero"`
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
	g.fanOutToFanIn = jg.FanOutToFanIn
	return nil
}
