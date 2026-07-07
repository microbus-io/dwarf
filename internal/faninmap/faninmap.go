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

// Package faninmap derives the fan-out -> fan-in convergence map of a workflow graph. It is an engine-side
// optimization view computed from a graph's public definition, not part of the graph itself: workflow.Graph
// carries only the author's definition (nodes, transitions, fan-in flags), and the engine builds this lookup
// from it when it needs to route an empty fan-out cohort to its convergence node.
//
// The map is a pure, deterministic function of the graph structure, so it is never persisted - the engine
// computes it once per flow at dispatch (caching it beside the parsed graph) rather than freezing it into the
// stored graph JSON. The traversal assumes an already-validated graph (workflow.Graph.Validate rejects the
// malformed shapes this analysis would otherwise have to guard against), so it only builds the mapping.
package faninmap

import "github.com/microbus-io/dwarf/workflow"

// Map is the fan-out -> fan-in convergence lookup for one graph.
type Map struct {
	m map[string]string
}

// New computes the fan-in map of a validated graph by walking its lineage: each branch of a fan-out pushes a
// frame identifying its source, and reaching a fan-in node pops the nearest frame and records that source ->
// fan-in mapping. Mirrors the lineage bookkeeping workflow.Graph.Validate performs to check convergence, but
// keeps only the resulting map.
func New(g *workflow.Graph) *Map {
	fanOutToFanIn := make(map[string]string)
	transitions := g.Transitions()
	entry := g.EntryPoint()

	// stacks records, per reached node, the fan-out frames still open on the path to it.
	stacks := map[string][]string{entry: nil}
	queue := []string{entry}
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
		fromIsFanOut := g.IsFanOutSource(from)

		for _, tr := range transitions {
			if tr.From != from {
				continue
			}
			var nextStack []string
			switch {
			case tr.WithGoto, tr.OnError, tr.Switch:
				// Control-flow edges neither push nor pop a fan-out frame.
				nextStack = fromStack
			case g.IsFanIn(tr.To):
				var source string
				if fromIsFanOut {
					// A fan-out feeding a fan-in directly converges on itself.
					nextStack = fromStack
					source = from
				} else if len(fromStack) > 0 {
					nextStack = stackCopy(fromStack[:len(fromStack)-1])
					source = fromStack[len(fromStack)-1]
				}
				if source != "" {
					fanOutToFanIn[source] = tr.To
				}
			case fromIsFanOut:
				nextStack = append(stackCopy(fromStack), from)
			default:
				nextStack = fromStack
			}

			if tr.To == workflow.END {
				continue
			}
			// On a validated graph every path to a node carries the same lineage, so the first visit wins.
			if _, seen := stacks[tr.To]; seen {
				continue
			}
			stacks[tr.To] = nextStack
			queue = append(queue, tr.To)
		}
	}
	return &Map{m: fanOutToFanIn}
}

// For returns the fan-in node that pops the frame pushed by the fan-out at source, or "" if source is not a
// fan-out with a downstream fan-in.
func (fim *Map) For(source string) string {
	return fim.m[source]
}
