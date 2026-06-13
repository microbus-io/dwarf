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
	"fmt"
	"net/url"
	"strings"
)

const (
	defaultPrimaryFill   = "#32a7c1"
	defaultPrimaryText   = "#f4f2ef"
	defaultSecondaryFill = "#e5f4f3"
	defaultSecondaryText = "#434343"
)

// GraphRenderer renders a workflow Graph to a Mermaid flowchart. Configure via
// the With* builder methods, then call Render.
type GraphRenderer struct {
	g               *Graph
	primaryFill     string
	primaryText     string
	secondaryFill   string
	secondaryText   string
	annotationColor string
	direction       string
	titleLabel      bool
	linkParam       string
}

// NewGraphRenderer creates a renderer for the given graph with default styling.
func NewGraphRenderer(g *Graph) *GraphRenderer {
	return &GraphRenderer{
		g:             g,
		primaryFill:   defaultPrimaryFill,
		primaryText:   defaultPrimaryText,
		secondaryFill: defaultSecondaryFill,
		secondaryText: defaultSecondaryText,
		direction:     "LR",
		titleLabel:    true,
	}
}

// WithPrimaryColors overrides the primary brand pair.
func (r *GraphRenderer) WithPrimaryColors(fill, text string) *GraphRenderer {
	r.primaryFill = fill
	r.primaryText = text
	return r
}

// WithSecondaryColors overrides the secondary surface pair.
func (r *GraphRenderer) WithSecondaryColors(fill, text string) *GraphRenderer {
	r.secondaryFill = fill
	r.secondaryText = text
	return r
}

// WithAnnotationColor overrides the color of annotation text.
func (r *GraphRenderer) WithAnnotationColor(color string) *GraphRenderer {
	r.annotationColor = color
	return r
}

// WithTopDown renders the diagram top-to-bottom.
func (r *GraphRenderer) WithTopDown() *GraphRenderer {
	r.direction = "TD"
	return r
}

// WithLeftRight renders the diagram left-to-right.
func (r *GraphRenderer) WithLeftRight() *GraphRenderer {
	r.direction = "LR"
	return r
}

// WithTitleLabel toggles the title tile that precedes the start marker.
func (r *GraphRenderer) WithTitleLabel(show bool) *GraphRenderer {
	r.titleLabel = show
	return r
}

// WithLinks enables click directives on every task node.
func (r *GraphRenderer) WithLinks(paramName string) *GraphRenderer {
	r.linkParam = paramName
	return r
}

// Render returns a fully-styled Mermaid flowchart representation of the graph.
func (r *GraphRenderer) Render() (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "graph %s\n", r.direction)
	fmt.Fprintf(&b, "    classDef task fill:%s,color:%s,stroke:%s\n", r.primaryFill, r.primaryText, r.primaryFill)
	fmt.Fprintf(&b, "    classDef term fill:%s,color:%s,stroke:%s\n", r.secondaryFill, r.secondaryText, r.primaryFill)
	annoColor := r.annotationColor
	if annoColor == "" {
		annoColor = r.primaryFill
	}
	fmt.Fprintf(&b, "    classDef note fill:none,stroke:none,color:%s,font-size:0.8em\n", annoColor)
	fmt.Fprintf(&b, "    linkStyle default stroke:%s\n", r.primaryFill)
	b.WriteString("\n")

	if r.titleLabel {
		graphLabel := stripHostPort(r.g.name, "")
		fmt.Fprintf(&b, "    _title{{%q}}:::term -.-> _start\n", graphLabel)
	}

	heads, endEdges := r.renderBody(&b, "    ", "")

	if len(heads) > 0 {
		for _, h := range heads {
			fmt.Fprintf(&b, "    _start(( )):::term --> %s\n", h)
		}
	} else {
		b.WriteString("    _start(( )):::term\n")
	}
	for _, ee := range endEdges {
		if ee.label != "" {
			fmt.Fprintf(&b, "    %s -->|%q| _end(( )):::term\n", ee.from, ee.label)
		} else {
			fmt.Fprintf(&b, "    %s --> _end(( )):::term\n", ee.from)
		}
	}

	return b.String(), nil
}

type endEdge struct {
	from  string
	label string
}

func (r *GraphRenderer) renderBody(b *strings.Builder, indent string, prefix string) (heads []string, endEdges []endEdge) {
	g := r.g
	ownHost := hostOf(g.name)
	ids := make(map[string]string, len(g.nodes))
	labels := make(map[string]string, len(g.nodes))
	for i, t := range g.nodes {
		ids[t.Name] = fmt.Sprintf("%st%d", prefix, i)
		labels[t.Name] = stripHostPort(t.Name, ownHost)
	}

	entries := make(map[string][]string, len(g.nodes))
	exits := make(map[string][]string, len(g.nodes))
	for _, t := range g.nodes {
		if g.fanInNodes[t.Name] {
			entries[t.Name] = []string{ids[t.Name] + "_reduce"}
		} else {
			entries[t.Name] = []string{ids[t.Name]}
		}
		exits[t.Name] = []string{ids[t.Name]}
	}

	type diamondArm struct {
		to    string
		label string
	}
	switchArms := map[string][]diamondArm{}
	whenArms := map[string][]diamondArm{}
	for _, tr := range g.transitions {
		if tr.WithGoto || tr.OnError {
			continue
		}
		if tr.Switch {
			label := tr.When
			if label == "true" {
				label = "default"
			}
			switchArms[tr.From] = append(switchArms[tr.From], diamondArm{to: tr.To, label: label})
			continue
		}
		if tr.When != "" && tr.ForEach == "" {
			whenArms[tr.From] = append(whenArms[tr.From], diamondArm{to: tr.To, label: tr.When})
		}
	}

	emitNodeBody := func(name string, indent string) {
		fmt.Fprintf(b, "%s%s[%q]:::task\n", indent, ids[name], labels[name])
		if r.linkParam != "" {
			fmt.Fprintf(b, "%sclick %s \"?%s=%s\"\n", indent, ids[name], r.linkParam, url.QueryEscape(name))
		}
	}
	emitOneNode := func(name string, indent string) {
		if note := g.annotations[name]; note != "" {
			annoID := ids[name] + "_anno"
			noteID := ids[name] + "_note"
			fmt.Fprintf(b, "%ssubgraph %s [\" \"]\n", indent, annoID)
			fmt.Fprintf(b, "%s    direction TB\n", indent)
			emitNodeBody(name, indent+"    ")
			fmt.Fprintf(b, "%s    %s[%q]:::note\n", indent, noteID, note)
			fmt.Fprintf(b, "%send\n", indent)
			fmt.Fprintf(b, "%sstyle %s fill:none,stroke:none\n", indent, annoID)
		} else {
			emitNodeBody(name, indent)
		}
		if g.fanInNodes[name] {
			fmt.Fprintf(b, "%s%s_reduce((%q)):::term\n", indent, ids[name], "reduce")
			fmt.Fprintf(b, "%s%s_reduce --> %s\n", indent, ids[name], ids[name])
		}
		if len(switchArms[name]) > 0 {
			fmt.Fprintf(b, "%s%s_switch{%q}:::term\n", indent, ids[name], "switch")
		}
		if len(whenArms[name]) > 0 {
			fmt.Fprintf(b, "%s%s_when{%q}:::term\n", indent, ids[name], "when")
		}
	}

	for _, t := range g.nodes {
		emitOneNode(t.Name, indent)
	}

	type edge struct {
		from string
		to   string
	}
	edgeOrder := []edge{}
	edgeLabels := map[edge][]string{}
	for _, tr := range g.transitions {
		if tr.Switch && !tr.WithGoto && !tr.OnError {
			continue
		}
		if tr.When != "" && tr.ForEach == "" && !tr.WithGoto && !tr.OnError && !tr.Switch {
			continue
		}
		e := edge{tr.From, tr.To}
		if _, ok := edgeLabels[e]; !ok {
			edgeOrder = append(edgeOrder, e)
			edgeLabels[e] = nil
		}
		var label string
		switch {
		case tr.ForEach != "":
			label = "for each"
		case tr.Switch:
			label = "case " + tr.When
		case tr.When != "":
			label = "when " + tr.When
		}
		if tr.WithGoto {
			if label != "" {
				label = "goto; " + label
			} else {
				label = "goto"
			}
		}
		if tr.OnError {
			if label != "" {
				label = "onError; " + label
			} else {
				label = "onError"
			}
		}
		if label != "" {
			edgeLabels[e] = append(edgeLabels[e], label)
		}
	}

	for _, e := range edgeOrder {
		label := strings.Join(edgeLabels[e], " | ")
		srcExits := exits[e.from]
		if e.to == END {
			for _, src := range srcExits {
				endEdges = append(endEdges, endEdge{from: src, label: label})
			}
			continue
		}
		dstEntries := entries[e.to]
		for _, src := range srcExits {
			for _, dst := range dstEntries {
				if label != "" {
					fmt.Fprintf(b, "%s%s -->|%q| %s\n", indent, src, label, dst)
				} else {
					fmt.Fprintf(b, "%s%s --> %s\n", indent, src, dst)
				}
			}
		}
	}

	emitDiamond := func(from string, suffix string, arms []diamondArm) {
		if len(arms) == 0 {
			return
		}
		diamondID := ids[from] + suffix
		for _, src := range exits[from] {
			fmt.Fprintf(b, "%s%s --> %s\n", indent, src, diamondID)
		}
		for _, arm := range arms {
			if arm.to == END {
				endEdges = append(endEdges, endEdge{from: diamondID, label: arm.label})
				continue
			}
			for _, dst := range entries[arm.to] {
				fmt.Fprintf(b, "%s%s -->|%q| %s\n", indent, diamondID, arm.label, dst)
			}
		}
	}
	for _, t := range g.nodes {
		emitDiamond(t.Name, "_switch", switchArms[t.Name])
		emitDiamond(t.Name, "_when", whenArms[t.Name])
	}

	if _, ok := ids[g.entryPoint]; ok {
		heads = entries[g.entryPoint]
	}
	return heads, endEdges
}

func hostOf(s string) string {
	_, after, ok := strings.Cut(s, "://")
	if !ok {
		return ""
	}
	if i := strings.IndexByte(after, '/'); i >= 0 {
		return after[:i]
	}
	return after
}

func stripHostPort(s, ownHost string) string {
	_, after, ok := strings.Cut(s, "://")
	if !ok {
		return s
	}
	host, path := after, ""
	if i := strings.IndexByte(after, '/'); i >= 0 {
		host = after[:i]
		path = after[i+1:]
	}
	if ownHost == "" || host == ownHost {
		return path
	}
	return after
}
