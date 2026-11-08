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
	"slices"
	"strings"

	"github.com/microbus-io/boolexp"
	"github.com/microbus-io/errors"
)

// End is the target a transition names to end the flow; it is not a task. Its value, "END", is the name it carries
// in a graph's JSON and in rendered diagrams.
const End = "END"

// Task describes one task in a workflow graph.
// Name is the task's identifier within the graph, recorded on every step
// that executes it (see FlowStep.TaskName). URL is the dispatch target the
// engine calls when the task is reached.
type Task struct {
	Name string
	URL  string
}

// The kinds of transition, held in Transition.Kind. The empty kind is an ordinary transition: taken when
// the source task succeeds and its When (if any) holds, with every matching ordinary transition taken, so
// several matches fan out.
const (
	// TransitionSwitch is taken when the source task succeeds, by the first switch transition from the
	// task whose When holds. It never fans out.
	TransitionSwitch = "switch"
	// TransitionGoto is taken only when the source task calls flow.Goto with its destination.
	TransitionGoto = "goto"
	// TransitionError is taken when the source task returns an error.
	TransitionError = "error"
)

// Transition defines a possible transition between two tasks in a workflow graph.
// From and To are task names, not URLs.
type Transition struct {
	From string `json:"from"`
	To   string `json:"to"`
	// Kind is empty for an ordinary transition, or TransitionSwitch, TransitionGoto or TransitionError.
	Kind string `json:"kind,omitzero"`
	// When is a boolean expression over the flow's state. It conditions an ordinary or switch transition,
	// and is required on a switch.
	When string `json:"when,omitzero"`
	// ForEach names a state array to fan out over, one branch per element, on an ordinary transition.
	ForEach string `json:"forEach,omitzero"`
	// As names the element in each forEach branch's state. Empty on a forEach transition means "item".
	As string `json:"as,omitzero"`
}

// UnmarshalJSON decodes a transition, also accepting the boolean withGoto, onError and switch fields in place
// of Kind.
func (t *Transition) UnmarshalJSON(data []byte) error {
	type plain Transition
	var v struct {
		plain
		WithGoto bool `json:"withGoto"`
		OnError  bool `json:"onError"`
		Switch   bool `json:"switch"`
	}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	*t = Transition(v.plain)
	// The alias defaults here, as AddTransitionForEach defaults it, so a decoded graph and a built one mean the
	// same state: the engine strips the alias's branch-private names at the fan-in only under the name stored.
	if t.ForEach != "" && t.As == "" {
		t.As = "item"
	}
	if t.Kind == "" {
		switch {
		case v.OnError:
			t.Kind = TransitionError
		case v.WithGoto:
			t.Kind = TransitionGoto
		case v.Switch:
			t.Kind = TransitionSwitch
		}
	}
	return nil
}

// Graph is the definition of a workflow. It describes the tasks, transitions between them,
// and reducers for how writes to a field combine.
type Graph struct {
	name        string
	entryPoint  string
	tasks       []Task
	transitions []Transition
	reducers    map[string]Reducer
	fanInTasks  map[string]bool
}

// NewGraph creates a new workflow graph with the given display name. The name is a human-friendly
// label (surfaced in rendering and Validate error messages); it is NOT the resolve key. The value
// passed to Create/Run and to the host's LoadGraph is a separate opaque workflow URL that the engine
// keeps with the flow - it is never kept on the graph itself.
func NewGraph(name string) *Graph {
	return &Graph{
		name: name,
	}
}

// Name returns the graph's display name.
func (g *Graph) Name() string {
	return g.name
}

// EntryPoint returns the task name of the entry point of the graph.
func (g *Graph) EntryPoint() string {
	return g.entryPoint
}

// Tasks returns the graph's tasks. The slice is the graph's own storage, not a copy: read it, but do not
// modify it or its elements. Change a graph through SetURL and the AddTransition methods instead.
func (g *Graph) Tasks() []Task {
	return g.tasks
}

// Transitions returns the graph's transitions. The slice is the graph's own storage, not a copy: read it,
// but do not modify it or its elements. Change a graph through the AddTransition methods instead.
func (g *Graph) Transitions() []Transition {
	return g.transitions
}

// SetURL binds a task (identified by its graph name) to the given dispatch URL, creating the task
// if it does not exist and updating its URL if it does. It is optional: a task first named inside an
// AddTransition* call is auto-registered with its URL equal to its name (see AddTransition). The name is the task's identity in the graph
// (used by transitions, fan-in, goto); the URL is the opaque downstream endpoint the engine
// hands to the host's ExecuteTask. The first task bound
// becomes the default entry point unless SetEntryPoint is called explicitly. End is not
// registered.
//
// The same URL may be bound under multiple names. This is how a workflow author reuses the same task
// code at distinct positions in the graph with different downstream transitions per position.
func (g *Graph) SetURL(name, url string) {
	if name == End {
		return
	}
	for i := range g.tasks {
		if g.tasks[i].Name == name {
			g.tasks[i].URL = url
			return
		}
	}
	g.tasks = append(g.tasks, Task{Name: name, URL: url})
	if g.entryPoint == "" {
		g.entryPoint = name
	}
}

// URLOf returns the dispatch URL for a task identified by name. Returns the empty string
// if the name is not registered. End maps to itself.
func (g *Graph) URLOf(name string) string {
	if name == End {
		return End
	}
	for _, n := range g.tasks {
		if n.Name == name {
			return n.URL
		}
	}
	return ""
}

// SetEntryPoint sets the entry point of the graph explicitly, overriding the default
// (first task added). The argument is a task name.
func (g *Graph) SetEntryPoint(name string) {
	g.entryPoint = name
}

// AddTransition adds an unconditional transition between two tasks.
//
// # Naming a task that was never bound with SetURL
//
// Every AddTransition* method auto-registers a task it has not seen before, binding it to a dispatch URL equal to
// ITS OWN NAME. That is deliberate, and it is what makes the concise form work:
//
//	g.AddTransition("billing.charge", "billing.receipt")   // two tasks whose names ARE their dispatch URLs
//
// Use SetURL when the task's graph identity should differ from where it dispatches - to give a task a
// readable position in the graph, or to reuse one task's code at several positions:
//
//	g.SetURL("Charge", "billing.charge")
//	g.SetURL("Retry", "billing.charge")   // same task, a second position with its own transitions
//
// The consequence worth knowing: because an unbound task is a VALID task (dispatching as its own name), a
// forgotten SetURL is not a build-time error and Validate cannot catch it. The graph is well-formed; it just
// names a task the host does not serve, and the host rejects it on that task's first dispatch. If you use
// SetURL for a graph's tasks, use it for all of them.
func (g *Graph) AddTransition(from, to string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to})
}

// AddTransitionWhen adds a conditional transition between two tasks: the edge is taken when the 'when'
// expression evaluates true against the flow's state.
//
// Every When transition from a task is evaluated INDEPENDENTLY, so two of them are a conditional
// PARALLEL FAN-OUT, not an if/else - if both conditions hold, both branches run. Even when they are
// mutually exclusive by construction, the source is still a fan-out source, so it requires a downstream
// convergence task marked with SetFanIn - Validate rejects the graph otherwise, complaining that a branch
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
// condition, all surviving branches converging on one SetFanIn task).
func (g *Graph) AddTransitionWhen(from, to string, when string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, When: when})
}

// AddTransitionSwitch adds a first-match-wins transition between two tasks. Multiple
// Switch transitions from the same source are evaluated in registration order and only
// the first whose 'when' expression evaluates true fires; the rest are skipped. If no
// Switch matches the flow ends at the source task, so the last Switch from a task is
// typically a catch-all with when="true". Only one branch ever runs, so a downstream
// SetFanIn is not required.
//
// A task that uses Switch transitions must declare every successful-path outgoing
// transition as Switch (the validator rejects mixing Switch with When/plain/ForEach
// from the same source). OnError and Goto transitions are orthogonal and remain allowed.
func (g *Graph) AddTransitionSwitch(from, to string, when string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, Kind: TransitionSwitch, When: when})
}

// AddTransitionGoto adds a transition that is only taken when the source task calls
// flow.Goto with a target that resolves to this transition's destination.
func (g *Graph) AddTransitionGoto(from, to string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, Kind: TransitionGoto})
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
// The branches must converge on a single task marked with SetFanIn (Validate rejects a fan-out that does
// not), where their outputs are merged by the fields' reducers - see SetReducer, and note that a branch
// writing to a reducer-managed field writes its DELTA, not the accumulated value. The three injected
// fields do not survive the fan-out: they are branch-private, so the flow's state past the fan-in - and
// the final state of a flow whose fan-out failed - carries none of them. Forward an element value under a
// different key if a downstream task needs it.
//
// An EMPTY array spawns no branches, but the flow does NOT stop there: it routes straight to the fan-in
// task, which runs with the source task's own state and output and no branch contributions. So a fan-in
// task must tolerate a cohort of zero (a reducer-managed field simply keeps its incoming value, and any
// per-element output it expected is absent), and the branch task must tolerate never running at all.
//
// Both 'forEach' and 'as' must be valid state field names (IsValidFieldName), or Validate rejects the graph.
//
// Every branch sees the source array (it is ordinary flow state). A large array, and a large element of
// it, is stored once and shared by the branches rather than copied into each. A branch that wants the
// array gone from the flow's state past the fan-in drops it with f.Set(<forEach>, nil).
func (g *Graph) AddTransitionForEach(from, to string, forEach string, as string) {
	g.autoRegister(from)
	g.autoRegister(to)
	if as == "" {
		as = "item"
	}
	g.transitions = append(g.transitions, Transition{From: from, To: to, ForEach: forEach, As: as})
}

// AddTransitionOnError adds a transition that is taken when the source task returns an error, instead of
// failing the flow. It fires on ANY error - the engine never inspects the status code or the text. When it
// fires, none of the task's other outgoing transitions are evaluated; when the task succeeds, they are and
// this one is not. A task may declare at most one onError transition, alongside any others.
//
// The error is delivered to the handler in the state field "onErr" (a structured error: message, status
// code, trace id, properties; the stack frames are stripped).
//
// AN ERROR VOIDS THE TASK'S CHANGES. Whatever the failing task wrote with Set in the run that returned the
// error is discarded - the handler does not see it, and it never reaches the flow's final state. (The same is
// true when no handler is declared and the flow fails.) What the step's earlier runs wrote - before a Retry,
// or before an Interrupt or Subgraph call the task was dispatched again from - is kept, and the handler sees
// it. This mirrors Go's own convention that an error voids the
// other results, and it is forced by at-least-once execution: a task whose worker loses its lease mid-body
// re-runs and RECOMPUTES its changes, so what a failing attempt wrote before it died is not a fact anything
// can be built on. To hand the handler something deliberately, put it in the error (it rides through in
// onErr), or give an external side effect its own task so its success is recorded durably before anything
// downstream can fail.
func (g *Graph) AddTransitionOnError(from, to string) {
	g.autoRegister(from)
	g.autoRegister(to)
	g.transitions = append(g.transitions, Transition{From: from, To: to, Kind: TransitionError})
}

// AddTransitionChain wires an unconditional transition between each consecutive pair of names:
// AddTransitionChain("A", "B", "C") is AddTransition("A", "B") followed by AddTransition("B", "C").
// It is a convenience for linear segments; fewer than two names is a no-op. End belongs last (a task
// after End would produce an invalid transition out of End). Mix with the other AddTransition* methods
// for branching, conditions, and loops.
func (g *Graph) AddTransitionChain(names ...string) {
	for i := 0; i+1 < len(names); i++ {
		g.AddTransition(names[i], names[i+1])
	}
}

// AddTransitionFanOut wires an unconditional transition from one source to each of several destinations:
// AddTransitionFanOut("A", "B", "C") is AddTransition("A", "B") followed by AddTransition("A", "C"), so B
// and C both fire and run in parallel. It is a convenience for static fan-out; no destinations is a no-op.
// It creates only the outgoing edges - if the branches later rejoin at a task, that task still needs
// SetFanIn (and usually a reducer) wired separately. Distinct from AddTransitionForEach, which fans out
// dynamically over a runtime collection rather than across statically-named tasks.
func (g *Graph) AddTransitionFanOut(from string, to ...string) {
	for _, dest := range to {
		g.AddTransition(from, dest)
	}
}

// autoRegister registers a new task if one does not exist for the name.
func (g *Graph) autoRegister(name string) {
	if name == End {
		return
	}
	for _, n := range g.tasks {
		if n.Name == name {
			return
		}
	}
	g.SetURL(name, name)
}

// ErrorTransition returns the error transition from the given task name, if one exists.
func (g *Graph) ErrorTransition(name string) (transition Transition, ok bool) {
	for _, tr := range g.transitions {
		if tr.From == name && tr.Kind == TransitionError {
			return tr, true
		}
	}
	return Transition{}, false
}

// SetFanIn marks a task as a fan-in: the task where the branches of a fan-out converge, and where their
// changes are merged by the fields' reducers. Validate checks that every fan-out converges on one.
func (g *Graph) SetFanIn(name string) {
	if g.fanInTasks == nil {
		g.fanInTasks = make(map[string]bool)
	}
	g.fanInTasks[name] = true
}

// IsFanIn reports whether the named task is a fan-in nexus.
func (g *Graph) IsFanIn(name string) bool {
	return g.fanInTasks[name]
}

// IsFanOutSource reports whether the named task has 2+ non-goto/non-error outgoing
// transitions, or any forEach outgoing transition. Switch transitions are exclusive
// (only one branch ever fires) and therefore do not count toward fan-out.
func (g *Graph) IsFanOutSource(name string) bool {
	var normalCount int
	for _, tr := range g.transitions {
		if tr.From != name || tr.Kind != "" {
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

// SetReducer sets how writes to a state field combine. On a field with a reducer every write is a contribution,
// combined with the field's value wherever one step hands its state to the next: a successor, a fan-in (in branch
// order), the final state, and Continue. A task writes its contribution with Flow.Set; Flow.Overwrite and Flow.Del
// replace instead. The field must be a valid state field name (IsValidFieldName) and the reducer one of the
// Reducer constants, or Validate rejects the graph.
func (g *Graph) SetReducer(field string, reducer Reducer) {
	if g.reducers == nil {
		g.reducers = make(map[string]Reducer)
	}
	g.reducers[field] = reducer
}

// Reducers returns the graph's reducers, keyed by state field. The map is the graph's own storage, not a
// copy: read it, but do not modify it. Change a field's reducer through SetReducer instead.
func (g *Graph) Reducers() map[string]Reducer {
	return g.reducers
}

// Validate checks the graph for structural errors.
func (g *Graph) Validate() error {
	if g.name == "" {
		return errors.New("graph name is required")
	}
	if len(g.tasks) == 0 {
		return errors.New("graph '%s' has no tasks", g.name)
	}
	taskSet := make(map[string]bool, len(g.tasks)+1)
	taskSet[End] = true
	for _, t := range g.tasks {
		if taskSet[t.Name] {
			return errors.New("duplicate task '%s' in graph '%s'", stripProto(t.Name), g.name)
		}
		taskSet[t.Name] = true
		if t.URL == "" {
			return errors.New("task '%s' in graph '%s' has no URL", stripProto(t.Name), g.name)
		}
	}
	if !taskSet[g.entryPoint] {
		return errors.New("entry point '%s' is not a registered task in graph '%s'", stripProto(g.entryPoint), g.name)
	}
	for fanInName := range g.fanInTasks {
		if !taskSet[fanInName] {
			return errors.New("SetFanIn references unknown task '%s' in graph '%s'", stripProto(fanInName), g.name)
		}
		if fanInName == End {
			return errors.New("SetFanIn cannot mark END in graph '%s'", g.name)
		}
	}
	onErrorFrom := make(map[string]bool, len(g.tasks))
	for _, tr := range g.transitions {
		if tr.From == End {
			return errors.New("transition out of END to '%s' in graph '%s'; END is terminal and has no outgoing transitions", stripProto(tr.To), g.name)
		}
		if !taskSet[tr.From] {
			return errors.New("transition from unknown task '%s' to '%s' in graph '%s'", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if !taskSet[tr.To] {
			return errors.New("transition from '%s' to unknown task '%s' in graph '%s'", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		switch tr.Kind {
		case "", TransitionSwitch, TransitionGoto, TransitionError:
		default:
			return errors.New("transition from '%s' to '%s' in graph '%s' has unknown kind '%s'", stripProto(tr.From), stripProto(tr.To), g.name, tr.Kind)
		}
		if tr.ForEach != "" && tr.Kind != "" {
			return errors.New("%s transition from '%s' to '%s' in graph '%s' cannot have 'forEach'; only an ordinary transition fans out", tr.Kind, stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.As != "" && tr.ForEach == "" {
			return errors.New("transition from '%s' to '%s' in graph '%s' has 'as' without 'forEach'", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.When != "" && (tr.Kind == TransitionGoto || tr.Kind == TransitionError) {
			return errors.New("%s transition from '%s' to '%s' in graph '%s' cannot have a 'when' expression; it is taken whenever its trigger fires", tr.Kind, stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.Kind == TransitionError {
			if onErrorFrom[tr.From] {
				return errors.New("task '%s' in graph '%s' has more than one error transition; only one is allowed (ErrorTransition takes the first)", stripProto(tr.From), g.name)
			}
			onErrorFrom[tr.From] = true
		}
		if tr.Kind == TransitionSwitch && tr.When == "" {
			return errors.New("switch transition from '%s' to '%s' in graph '%s' requires a 'when' expression (use \"true\" for the default branch)", stripProto(tr.From), stripProto(tr.To), g.name)
		}
		if tr.Kind == TransitionError && tr.From == tr.To {
			return errors.New("transition from '%s' to itself in graph '%s' would loop unboundedly; use flow.Retry in the task body for bounded retries with backoff", stripProto(tr.From), g.name)
		}
		if tr.When != "" {
			err := boolexp.Validate(tr.When)
			if err != nil {
				return errors.New("transition from '%s' to '%s' in graph '%s' has invalid 'when' expression: %v", stripProto(tr.From), stripProto(tr.To), g.name, err)
			}
		}
		if tr.ForEach != "" && !IsValidFieldName(tr.ForEach) {
			return errors.New("transition from '%s' to '%s' in graph '%s' has invalid 'forEach' field name '%s'; %s", stripProto(tr.From), stripProto(tr.To), g.name, tr.ForEach, fieldNameRule)
		}
		if tr.As != "" && !IsValidFieldName(tr.As) {
			return errors.New("transition from '%s' to '%s' in graph '%s' has invalid 'as' field name '%s'; %s", stripProto(tr.From), stripProto(tr.To), g.name, tr.As, fieldNameRule)
		}
	}
	for _, field := range slices.Sorted(maps.Keys(g.reducers)) {
		if !IsValidFieldName(field) {
			return errors.New("reducer in graph '%s' is set on invalid field name '%s'; %s", g.name, field, fieldNameRule)
		}
		if r := g.reducers[field]; !r.isKnown() {
			return errors.New("reducer '%s' on field '%s' in graph '%s' is not a known reducer", string(r), field, g.name)
		}
	}

	hasSwitchFrom := make(map[string]bool, len(g.tasks))
	for _, tr := range g.transitions {
		if tr.Kind == TransitionSwitch {
			hasSwitchFrom[tr.From] = true
		}
	}
	for _, tr := range g.transitions {
		if !hasSwitchFrom[tr.From] || tr.Kind != "" {
			continue
		}
		return errors.New("task '%s' in graph '%s' mixes a switch transition with a non-switch success-path transition to '%s'; convert all outgoing success-path transitions to switch (use when=\"true\" for the default), or use a goto transition for explicit overrides", stripProto(tr.From), g.name, stripProto(tr.To))
	}

	reachable := make(map[string]bool)
	queue := []string{g.entryPoint}
	reachable[g.entryPoint] = true
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, tr := range g.transitions {
			if tr.From == current && tr.To != End && !reachable[tr.To] {
				reachable[tr.To] = true
				queue = append(queue, tr.To)
			}
		}
	}
	for _, t := range g.tasks {
		if !reachable[t.Name] {
			return errors.New("task '%s' is not reachable from entry point '%s' in graph '%s'", stripProto(t.Name), stripProto(g.entryPoint), g.name)
		}
	}

	hasEnd := false
	for _, tr := range g.transitions {
		if tr.To == End {
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
// fan-in, every branch must pop its frame before END, and no task may be reached with two different lineage
// stacks. It builds a local fan-out->fan-in map to run these checks and stores nothing on the graph; the
// engine derives the same map at dispatch (internal/faninmap) for routing.
func (g *Graph) validateLineage() error {
	fanOutToFanIn := make(map[string]string)

	// Memoized IsFanOutSource, not a second copy of it: the BFS below revisits a task once per incoming
	// edge, and this predicate must stay in lockstep with the engine's routing (internal/faninmap and
	// processStep both branch on IsFanOutSource) - a validator that disagreed with the router about what
	// fans out would bless a graph the engine then mis-executes.
	//
	// It is a SET, not a task->bool table: the "every fan-out source needs a fan-in" check below iterates
	// its KEYS, so storing an explicit false would enrol every task in the graph as a fan-out source.
	isFanOutSource := make(map[string]bool, len(g.tasks))
	for _, t := range g.tasks {
		if g.IsFanOutSource(t.Name) {
			isFanOutSource[t.Name] = true
		}
	}

	stacks := make(map[string][]string, len(g.tasks))
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
			// The fan-in arm MUST precede the goto/error/switch arm. The engine treats any transition
			// whose target is a fan-in task as a cohort arrival - it does not care which kind of edge
			// carried it - so a goto/error/switch edge into a fan-in from a task with no fan-out
			// frame is exactly the "arrive at a cohort that does not exist" shape, and at runtime it bumps
			// cohort_arrivals on step_id=0 and hot-loops the flow. Testing goto/error/switch first would
			// skip the frame-pop check for precisely the edges that can reach a fan-in from outside a
			// cohort. internal/faninmap's traversal must order its arms the same way.
			case g.fanInTasks[tr.To]:
				var fanOutSource string
				if fromIsFanOut {
					nextStack = fromStack
					fanOutSource = from
				} else {
					if len(fromStack) == 0 {
						return errors.New(
							"transition from '%s' to fan-in task '%s' in graph '%s' has no fan-out frame to pop",
							stripProto(from), stripProto(tr.To), g.name,
						)
					}
					nextStack = stackCopy(fromStack[:len(fromStack)-1])
					fanOutSource = fromStack[len(fromStack)-1]
				}
				// All branches of one fan-out must converge on the SAME fan-in. The cohort shares a spawn
				// step, and the cohort-resolution path picks the fan-in from whichever sibling completes
				// last - so divergent fan-in targets make the convergence task nondeterministic. A prior
				// mapping to a different fan-in is that divergence.
				if prior, ok := fanOutToFanIn[fanOutSource]; ok && prior != tr.To {
					return errors.New(
						"fan-out source '%s' in graph '%s' has branches converging on different fan-in tasks ('%s' and '%s'); all siblings of a fan-out must converge on the same fan-in",
						stripProto(fanOutSource), g.name, stripProto(prior), stripProto(tr.To),
					)
				}
				fanOutToFanIn[fanOutSource] = tr.To
			// A goto/error/switch edge is a runtime override the task body chooses, not a structural
			// branch of the fan-out, so it pushes no frame: the cohort it would push is one the engine
			// never spawns. Without this arm a fan-out source can route nowhere but its own branches -
			// its `goto END` inherits the source's own frame and trips the END check below - which
			// rejects "fan out over the array, or end the flow when the task decides there is nothing
			// to do". The arm must stay BELOW the fan-in arm; see that comment for why.
			case tr.Kind != "":
				nextStack = fromStack
			case fromIsFanOut:
				nextStack = append(stackCopy(fromStack), from)
			default:
				nextStack = fromStack
			}

			if tr.To == End {
				if len(nextStack) != 0 {
					return errors.New(
						"transition from '%s' to END in graph '%s' has unpopped fan-out frames %v; every branch must pass through a fan-in task before reaching END",
						stripProto(from), g.name, stripProtoAll(nextStack),
					)
				}
				continue
			}

			if prior, seen := stacks[tr.To]; seen {
				if !stackEqual(prior, nextStack) {
					return errors.New(
						"task '%s' in graph '%s' is reachable with two different lineage stacks (%v and %v); register a separate alias task via SetURL to disambiguate",
						stripProto(tr.To), g.name, stripProtoAll(prior), stripProtoAll(nextStack),
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
				"fan-out source '%s' in graph '%s' has no fan-in task downstream; mark the convergence task with SetFanIn",
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

// stripProtoAll is stripProto of every name in names, as a new slice.
func stripProtoAll(names []string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		out[i] = stripProto(n)
	}
	return out
}

// graphJSON is the graph's wire form, shared by MarshalJSON and UnmarshalJSON.
type graphJSON struct {
	Name        string             `json:"name,omitzero"`
	EntryPoint  string             `json:"entryPoint"`
	Tasks       []taskJSON         `json:"tasks"`
	Transitions []Transition       `json:"transitions"`
	Reducers    map[string]Reducer `json:"reducers,omitzero"`
}

// taskJSON is one task's wire form.
type taskJSON struct {
	Name  string `json:"name"`
	URL   string `json:"url,omitzero"`
	FanIn bool   `json:"fanIn,omitzero"`
}

// MarshalJSON serializes the graph to JSON.
func (g *Graph) MarshalJSON() ([]byte, error) {
	tasks := make([]taskJSON, len(g.tasks))
	for i, t := range g.tasks {
		tasks[i] = taskJSON{Name: t.Name, URL: t.URL, FanIn: g.fanInTasks[t.Name]}
	}
	jg := graphJSON{
		Name:        g.name,
		EntryPoint:  g.entryPoint,
		Tasks:       tasks,
		Transitions: g.transitions,
		Reducers:    g.reducers,
	}
	if jg.Tasks == nil {
		jg.Tasks = []taskJSON{}
	}
	if jg.Transitions == nil {
		jg.Transitions = []Transition{}
	}
	return json.Marshal(jg)
}

// UnmarshalJSON deserializes the graph from JSON.
func (g *Graph) UnmarshalJSON(data []byte) error {
	var jg graphJSON
	err := json.Unmarshal(data, &jg)
	if err != nil {
		return err
	}
	g.name = jg.Name
	g.entryPoint = jg.EntryPoint
	g.tasks = make([]Task, len(jg.Tasks))
	g.fanInTasks = nil
	for i, jt := range jg.Tasks {
		g.tasks[i].Name = jt.Name
		g.tasks[i].URL = jt.URL
		if g.tasks[i].URL == "" {
			g.tasks[i].URL = jt.Name
		}
		if jt.FanIn {
			if g.fanInTasks == nil {
				g.fanInTasks = make(map[string]bool)
			}
			g.fanInTasks[jt.Name] = true
		}
	}
	g.transitions = jg.Transitions
	g.reducers = jg.Reducers
	return nil
}
