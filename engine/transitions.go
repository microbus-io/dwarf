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
	"encoding/json"
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

// evaluateTransitions determines the next task(s) to execute based on the graph transitions
// and the current flow state. Returns multiple candidates for fan-out.
func evaluateTransitions(graph *workflow.Graph, currentTask string, flow *workflow.RawFlow) ([]nextStep, error) {
	if gotoTarget := flow.GotoRequested(); gotoTarget != "" {
		for _, tr := range graph.Transitions() {
			if tr.From != currentTask || !tr.WithGoto {
				continue
			}
			if tr.To == gotoTarget || graph.URLOf(tr.To) == gotoTarget {
				return []nextStep{{taskName: tr.To}}, nil
			}
		}
		return nil, errors.New("task '%s' requested goto to '%s' but no WithGoto transition exists from this task", stripProto(currentTask), stripProto(gotoTarget))
	}

	stateMap := make(map[string]any, len(flow.RawState()))
	for k, v := range flow.RawState() {
		if raw, ok := v.(json.RawMessage); ok {
			var val any
			json.Unmarshal(raw, &val)
			stateMap[k] = val
		} else {
			stateMap[k] = v
		}
	}

	for _, tr := range graph.Transitions() {
		if tr.From != currentTask || !tr.Switch {
			continue
		}
		for _, sw := range graph.Transitions() {
			if sw.From != currentTask || !sw.Switch {
				continue
			}
			match, err := boolexp.Eval(sw.When, stateMap)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if match {
				return []nextStep{{taskName: sw.To}}, nil
			}
		}
		return nil, nil
	}

	var candidates []nextStep
	for _, tr := range graph.Transitions() {
		if tr.From != currentTask {
			continue
		}
		if tr.WithGoto {
			continue
		}
		if tr.OnError {
			continue
		}
		taken := false
		if tr.When == "" {
			taken = true
		} else {
			match, err := boolexp.Eval(tr.When, stateMap)
			if err != nil {
				return nil, errors.Trace(err)
			}
			taken = match
		}
		if !taken {
			continue
		}

		if tr.ForEach != "" {
			val, ok := flow.RawState()[tr.ForEach]
			if !ok {
				continue
			}
			raw, err := json.Marshal(val)
			if err != nil {
				return nil, errors.Trace(err)
			}
			var items []json.RawMessage
			if err := json.Unmarshal(raw, &items); err != nil {
				return nil, errors.New("forEach field '%s' is not an array", tr.ForEach, err)
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

// evaluateErrorTransitions determines the error handler task to route to when a task fails.
func evaluateErrorTransitions(graph *workflow.Graph, currentTask string, flow *workflow.RawFlow) ([]nextStep, error) {
	stateMap := make(map[string]any, len(flow.RawState()))
	for k, v := range flow.RawState() {
		if raw, ok := v.(json.RawMessage); ok {
			var val any
			json.Unmarshal(raw, &val)
			stateMap[k] = val
		} else {
			stateMap[k] = v
		}
	}

	for _, tr := range graph.Transitions() {
		if tr.From != currentTask || !tr.OnError {
			continue
		}
		taken := true
		if tr.When != "" {
			match, err := boolexp.Eval(tr.When, stateMap)
			if err != nil {
				return nil, errors.Trace(err)
			}
			taken = match
		}
		if taken {
			return []nextStep{{taskName: tr.To}}, nil
		}
	}
	return nil, nil
}

// fanInPredecessorTasks returns the distinct task node names that transition into
// fanInTask via a normal (non-goto, non-error) transition.
func fanInPredecessorTasks(graph *workflow.Graph, fanInTask string) []string {
	seen := map[string]bool{}
	var names []string
	for _, tr := range graph.Transitions() {
		if tr.To == fanInTask && !tr.WithGoto && !tr.OnError && !seen[tr.From] {
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
