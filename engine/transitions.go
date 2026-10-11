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

package engine

import (
	"strings"

	"github.com/microbus-io/boolexp"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// nextStep describes a step to be created during transition evaluation.
type nextStep struct {
	taskName    string
	item        any
	itemKey     string
	forEachKey  string
	cohortIndex int
	cohortCount int
}

// evaluateTransitions determines the next task(s) to execute based on the graph transitions. Returns multiple
// candidates for fan-out. The task's Goto is read from flow; `when`, `switch` and `forEach` read state, which is
// what the successors will be handed - the engine's own fold of the step's input and changes, never the task's
// returned view.
func evaluateTransitions(graph *workflow.Graph, currentTask string, flow *workflow.RawFlow, state workflow.State) ([]nextStep, error) {
	if gotoTarget := flow.GotoRequested(); gotoTarget != "" {
		for _, tr := range graph.Transitions() {
			if tr.From != currentTask || tr.Kind != workflow.TransitionGoto {
				continue
			}
			if tr.To == gotoTarget {
				return []nextStep{{taskName: tr.To}}, nil
			}
		}
		return nil, errors.New("task '%s' requested goto to '%s' but no goto transition exists from this task", stripProto(currentTask), stripProto(gotoTarget))
	}

	// Switch/When transitions share one boolexp.Normalize of the flow state instead of one per edge,
	// computed lazily so a graph with no conditional edges never pays for it.
	var normalized map[string]any
	var normalizeErr error
	haveNormalized := false
	normalize := func() (map[string]any, error) {
		if !haveNormalized {
			normalized, normalizeErr = boolexp.Normalize(redactMeta(state.Clone()))
			haveNormalized = true
		}
		return normalized, errors.Trace(normalizeErr)
	}

	// A task with switch edges takes the first that matches, in declaration order, or none.
	hasSwitch := false
	for _, sw := range graph.Transitions() {
		if sw.From != currentTask || sw.Kind != workflow.TransitionSwitch {
			continue
		}
		hasSwitch = true
		symbols, err := normalize()
		if err != nil {
			return nil, errors.Trace(err)
		}
		match, err := boolexp.Eval(sw.When, symbols)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if match {
			return []nextStep{{taskName: sw.To}}, nil
		}
	}
	if hasSwitch {
		return nil, nil
	}

	var candidates []nextStep
	for _, tr := range graph.Transitions() {
		if tr.From != currentTask || tr.Kind != "" {
			continue // a switch from this node returned above; goto and error transitions are not evaluated here
		}
		taken := false
		if tr.When == "" {
			taken = true
		} else {
			symbols, err := normalize()
			if err != nil {
				return nil, errors.Trace(err)
			}
			match, err := boolexp.Eval(tr.When, symbols)
			if err != nil {
				return nil, errors.Trace(err)
			}
			taken = match
		}
		if !taken {
			continue
		}

		if tr.ForEach != "" {
			// Reading the source array materializes it - a forEach has to see the elements to spawn one
			// branch each, so this is the one field on the step that is genuinely needed as a Go value.
			// An absent or cleared source yields no elements (like an empty array), which routes straight to
			// the fan-in; any non-array is an error.
			var val any
			present, gerr := state.Get(tr.ForEach, &val)
			if gerr != nil {
				return nil, errors.Trace(gerr)
			}
			if !present {
				continue
			}
			var items []any
			switch v := val.(type) {
			case nil:
			case []any:
				items = v
			default:
				return nil, errors.New("forEach field '%s' is not an array", tr.ForEach)
			}
			itemKey := tr.As
			if itemKey == "" {
				itemKey = "item"
			}
			for idx, item := range items {
				candidates = append(candidates, nextStep{
					taskName:    tr.To,
					item:        item,
					itemKey:     itemKey,
					forEachKey:  tr.ForEach,
					cohortIndex: idx,
					cohortCount: len(items),
				})
			}
		} else {
			candidates = append(candidates, nextStep{taskName: tr.To})
		}
	}

	return candidates, nil
}

// fanInPredecessorTasks returns the distinct task names that transition into fanInTask, by an edge of any
// kind. A goto or onError edge into a fan-in is a cohort arrival like any other, so a branch whose only route
// to the fan-in is one of those must still be linked to the fan-in step as a cohort exit.
func fanInPredecessorTasks(graph *workflow.Graph, fanInTask string) []string {
	seen := map[string]bool{}
	var names []string
	for _, tr := range graph.Transitions() {
		if tr.To == fanInTask && !seen[tr.From] {
			seen[tr.From] = true
			names = append(names, tr.From)
		}
	}
	return names
}

// stripProto removes the scheme prefix from a URL-like string for cleaner error messages.
func stripProto(s string) string {
	var x string
	if _, x, _ = strings.Cut(s, "://"); x == "" {
		x = s
	}
	return x
}
