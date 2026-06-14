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
	"strings"
	"time"
)

const (
	defaultFlowPrimaryFill   = "#32a7c1"
	defaultFlowPrimaryText   = "#f4f2ef"
	defaultFlowSecondaryFill = "#e5f4f3"
	defaultFlowSecondaryText = "#434343"
	defaultFlowErrorFill     = "#f15922"
	defaultFlowErrorText     = "#f4f2ef"
	defaultFlowAttentionFill = "#ed2e92"
	defaultFlowAttentionText = "#f4f2ef"
)

// FlowRenderer renders the execution history of a flow as a Mermaid flowchart.
type FlowRenderer struct {
	steps         []FlowStep
	primaryFill   string
	primaryText   string
	secondaryFill string
	secondaryText string
	errorFill     string
	errorText     string
	attentionFill string
	attentionText string
	direction     string
	title         string
	linkParam     string
}

// NewFlowRenderer creates a renderer for a flow's execution history.
func NewFlowRenderer(steps []FlowStep) *FlowRenderer {
	return &FlowRenderer{
		steps:         steps,
		primaryFill:   defaultFlowPrimaryFill,
		primaryText:   defaultFlowPrimaryText,
		secondaryFill: defaultFlowSecondaryFill,
		secondaryText: defaultFlowSecondaryText,
		errorFill:     defaultFlowErrorFill,
		errorText:     defaultFlowErrorText,
		attentionFill: defaultFlowAttentionFill,
		attentionText: defaultFlowAttentionText,
		direction:     "TD",
	}
}

func hasInflightStep(steps []FlowStep) bool {
	for _, s := range steps {
		switch s.Status {
		case StatusPending, StatusRunning, StatusInterrupted, "":
			return true
		}
		if s.Subgraph && hasInflightStep(s.SubHistory) {
			return true
		}
	}
	return false
}

func (r *FlowRenderer) WithPrimaryColors(fill, text string) *FlowRenderer {
	r.primaryFill = fill
	r.primaryText = text
	return r
}

func (r *FlowRenderer) WithSecondaryColors(fill, text string) *FlowRenderer {
	r.secondaryFill = fill
	r.secondaryText = text
	return r
}

func (r *FlowRenderer) WithErrorColors(fill, text string) *FlowRenderer {
	r.errorFill = fill
	r.errorText = text
	return r
}

func (r *FlowRenderer) WithAttentionColors(fill, text string) *FlowRenderer {
	r.attentionFill = fill
	r.attentionText = text
	return r
}

func (r *FlowRenderer) WithTopDown() *FlowRenderer {
	r.direction = "TD"
	return r
}

func (r *FlowRenderer) WithLeftRight() *FlowRenderer {
	r.direction = "LR"
	return r
}

func (r *FlowRenderer) WithTitle(text string) *FlowRenderer {
	r.title = text
	return r
}

func (r *FlowRenderer) WithLinks(paramName string) *FlowRenderer {
	r.linkParam = paramName
	return r
}

// Render returns the Mermaid flowchart representation.
func (r *FlowRenderer) Render() (string, error) {
	var b strings.Builder
	fmt.Fprintf(&b, "flowchart %s\n", r.direction)

	chromeStroke := r.secondaryText
	if chromeStroke == "" {
		chromeStroke = r.secondaryFill
	}
	flowClassDefLine(&b, StatusCompleted, r.primaryFill, r.primaryText, r.primaryFill, "")
	flowClassDefLine(&b, StatusRunning, r.primaryFill, r.primaryText, r.primaryFill, "stroke-dasharray:4 2")
	flowClassDefLine(&b, StatusPending, r.secondaryFill, r.secondaryText, chromeStroke, "")
	flowClassDefLine(&b, StatusFailed, r.errorFill, r.errorText, r.errorFill, "")
	flowClassDefLine(&b, StatusCancelled, r.errorFill, r.errorText, r.errorFill, "")
	flowClassDefLine(&b, StatusInterrupted, r.attentionFill, r.attentionText, r.attentionFill, "")
	flowClassDefLine(&b, "term", r.secondaryFill, r.secondaryText, chromeStroke, "")

	fmt.Fprintf(&b, "    linkStyle default stroke:%s\n", r.primaryFill)
	b.WriteString("\n")

	if r.title != "" {
		fmt.Fprintf(&b, "    _title{{%q}}:::term -.-> _start\n", r.title)
	}
	showEnd := !hasInflightStep(r.steps)

	b.WriteString("    _start((\" \")):::term\n")
	if showEnd {
		b.WriteString("    _end((\" \")):::term\n")
	}

	heads, tails := r.renderSteps(&b, "", r.steps)
	for _, h := range heads {
		fmt.Fprintf(&b, "    _start --> %s\n", h)
	}
	if showEnd {
		for _, t := range tails {
			fmt.Fprintf(&b, "    %s --> _end\n", t)
		}
	}

	return b.String(), nil
}

func (r *FlowRenderer) renderSteps(buf *strings.Builder, prefix string, steps []FlowStep) (heads []string, tails []string) {
	if len(steps) == 0 {
		return nil, nil
	}

	type renderNode struct {
		entries []string
		exits   []string
	}
	byID := make(map[int]*renderNode, len(steps))
	stepByID := make(map[int]FlowStep, len(steps))
	order := make([]int, 0, len(steps))

	type subBlock struct {
		blockID, label, body string
		innerHeads           []string
		callEdgeLabel        string
	}
	subBlocks := map[int]subBlock{}

	for i := range steps {
		s := steps[i]
		if s.StepID == 0 {
			continue
		}
		nodeID := fmt.Sprintf("%ss%d", prefix, s.StepID)
		byID[s.StepID] = &renderNode{entries: []string{nodeID}, exits: []string{nodeID}}
		if s.Subgraph && len(s.SubHistory) > 0 {
			subPrefix := fmt.Sprintf("%ss%d_sub", prefix, s.StepID)
			var body strings.Builder
			subHeads, subTails := r.renderSteps(&body, subPrefix, s.SubHistory)
			blockID := fmt.Sprintf("%ss%d_sg", prefix, s.StepID)
			label := flowStripProto(s.SubWorkflowURL)
			if label == "" {
				label = "subgraph"
			}
			var callEdgeLabel string
			for _, sub := range s.SubHistory {
				if sub.PredecessorID == 0 && sub.HasStarted() && !sub.StartedAt.IsZero() && !sub.CreatedAt.IsZero() {
					if d := sub.StartedAt.Sub(sub.CreatedAt); d > 0 {
						callEdgeLabel = flowFormatDuration(d)
					}
					break
				}
			}
			subBlocks[s.StepID] = subBlock{blockID: blockID, label: label, body: body.String(), innerHeads: subHeads, callEdgeLabel: callEdgeLabel}
			if len(subTails) > 0 {
				byID[s.StepID].exits = subTails
			}
		}
		stepByID[s.StepID] = s
		order = append(order, s.StepID)
	}

	emitClick := func(nodeID, stepKey string) {
		if r.linkParam == "" || stepKey == "" {
			return
		}
		fmt.Fprintf(buf, "    click %s \"?%s=%s\"\n", nodeID, r.linkParam, stepKey)
	}

	emitStep := func(stepID int) {
		s := stepByID[stepID]
		nodeID := fmt.Sprintf("%ss%d", prefix, stepID)
		label := flowStripProto(s.TaskName)
		blk, isSubgraphCaller := subBlocks[stepID]
		if isSubgraphCaller {
			if isTerminalStepStatus(s.Status) && s.HasStarted() && !s.UpdatedAt.IsZero() && !s.StartedAt.IsZero() {
				if subDur, ok := subgraphWallTime(s.SubHistory); ok {
					net := s.UpdatedAt.Sub(s.StartedAt) - subDur
					if net < 0 {
						net = 0
					}
					label += "\n" + flowFormatDuration(net)
				}
			}
		} else if s.HasStarted() && isTerminalStepStatus(s.Status) && !s.UpdatedAt.IsZero() && !s.StartedAt.IsZero() {
			label += "\n" + flowFormatDuration(s.UpdatedAt.Sub(s.StartedAt))
		}
		statusClass := s.Status
		if statusClass == "" {
			statusClass = StatusPending
		}
		fmt.Fprintf(buf, "    %s[\"%s\"]:::%s\n", nodeID, escapeMermaidLabel(label), statusClass)
		emitClick(nodeID, s.StepKey)
		if isSubgraphCaller {
			fmt.Fprintf(buf, "    subgraph %s [%q]\n", blk.blockID, blk.label)
			buf.WriteString("        direction TB\n")
			buf.WriteString(blk.body)
			buf.WriteString("    end\n")
			fmt.Fprintf(buf, "    style %s %s\n", blk.blockID, r.clusterStyle())
		}
	}

	childrenOf := map[int][]int{}
	for _, id := range order {
		s := stepByID[id]
		if s.PredecessorID == 0 {
			continue
		}
		childrenOf[s.PredecessorID] = append(childrenOf[s.PredecessorID], id)
	}
	cohortOf := map[int]int{}
	for parentID, kids := range childrenOf {
		if len(kids) >= 2 {
			for _, k := range kids {
				cohortOf[k] = parentID
			}
		}
	}

	emittedSteps := map[int]bool{}
	for _, id := range order {
		if emittedSteps[id] {
			continue
		}
		pid, ok := cohortOf[id]
		if !ok {
			emitStep(id)
			emittedSteps[id] = true
			continue
		}
		blockID := fmt.Sprintf("%sfo_s%d", prefix, pid)
		fmt.Fprintf(buf, "    subgraph %s [\" \"]\n", blockID)
		buf.WriteString("        direction TB\n")
		for _, child := range childrenOf[pid] {
			emitStep(child)
			emittedSteps[child] = true
		}
		buf.WriteString("    end\n")
		fmt.Fprintf(buf, "    style %s fill:none,stroke:none\n", blockID)
	}

	emitted := map[string]bool{}
	hasIncoming := map[int]bool{}
	hasOutgoing := map[int]bool{}
	emitEdge := func(src, dst, label string) {
		if src == dst {
			return
		}
		key := src + "\x00" + dst
		if emitted[key] {
			return
		}
		emitted[key] = true
		if label == "" {
			fmt.Fprintf(buf, "    %s --> %s\n", src, dst)
			return
		}
		fmt.Fprintf(buf, "    %s -- %q --> %s\n", src, label, dst)
	}
	edgeLabel := func(from, to FlowStep) string {
		if from.UpdatedAt.IsZero() || !to.HasStarted() || to.StartedAt.IsZero() {
			return ""
		}
		d := to.StartedAt.Sub(from.UpdatedAt)
		if d < 0 {
			d = 0
		}
		return flowFormatDuration(d)
	}
	addEdge := func(fromID, toID int) {
		from := byID[fromID]
		to := byID[toID]
		if from == nil || to == nil || fromID == toID {
			return
		}
		label := edgeLabel(stepByID[fromID], stepByID[toID])
		for _, src := range from.exits {
			for _, dst := range to.entries {
				emitEdge(src, dst, label)
			}
		}
		hasOutgoing[fromID] = true
		hasIncoming[toID] = true
	}
	for _, id := range order {
		s := stepByID[id]
		if s.PredecessorID != 0 {
			addEdge(s.PredecessorID, id)
		}
		if s.SuccessorID != 0 {
			addEdge(id, s.SuccessorID)
		}
	}

	for _, id := range order {
		blk, ok := subBlocks[id]
		if !ok {
			continue
		}
		callerNodeID := fmt.Sprintf("%ss%d", prefix, id)
		for _, h := range blk.innerHeads {
			emitEdge(callerNodeID, h, blk.callEdgeLabel)
		}
	}

	for _, id := range order {
		if !hasIncoming[id] {
			heads = append(heads, byID[id].entries...)
		}
		if !hasOutgoing[id] {
			tails = append(tails, byID[id].exits...)
		}
	}
	return heads, tails
}

func (r *FlowRenderer) clusterStyle() string {
	parts := make([]string, 0, 4)
	if r.primaryFill != "" {
		parts = append(parts, "fill:"+r.primaryFill)
	}
	parts = append(parts, "fill-opacity:0.03")
	parts = append(parts, "stroke:none")
	if r.primaryText != "" && r.primaryFill != "" {
		parts = append(parts, "color:"+r.primaryFill)
	}
	return strings.Join(parts, ",")
}

func flowClassDefLine(b *strings.Builder, name, fill, text, stroke, extra string) {
	parts := make([]string, 0, 4)
	if fill != "" {
		parts = append(parts, "fill:"+fill)
	}
	if text != "" {
		parts = append(parts, "color:"+text)
	}
	if stroke != "" {
		parts = append(parts, "stroke:"+stroke)
	}
	if extra != "" {
		parts = append(parts, extra)
	}
	fmt.Fprintf(b, "    classDef %s %s\n", name, strings.Join(parts, ","))
}

func escapeMermaidLabel(s string) string {
	return strings.NewReplacer(
		"\"", "#quot;",
		"[", "#91;",
		"]", "#93;",
	).Replace(s)
}

func isTerminalStepStatus(status string) bool {
	switch status {
	case StatusCompleted, StatusFailed, StatusCancelled:
		return true
	}
	return false
}

func subgraphWallTime(history []FlowStep) (time.Duration, bool) {
	if len(history) == 0 {
		return 0, false
	}
	var minCreated, maxUpdated time.Time
	var walk func(steps []FlowStep) bool
	walk = func(steps []FlowStep) bool {
		for _, s := range steps {
			if !isTerminalStepStatus(s.Status) {
				return false
			}
			if minCreated.IsZero() || s.CreatedAt.Before(minCreated) {
				minCreated = s.CreatedAt
			}
			if s.UpdatedAt.After(maxUpdated) {
				maxUpdated = s.UpdatedAt
			}
			if s.Subgraph && len(s.SubHistory) > 0 {
				if !walk(s.SubHistory) {
					return false
				}
			}
		}
		return true
	}
	if !walk(history) {
		return 0, false
	}
	return maxUpdated.Sub(minCreated), true
}

func flowStripProto(u string) string {
	left, right, cut := strings.Cut(u, "://")
	if !cut {
		return left
	}
	return right
}

func flowFormatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.3gs", d.Seconds())
	case d < time.Hour:
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		if s == 0 {
			return fmt.Sprintf("%dm", m)
		}
		return fmt.Sprintf("%dm%ds", m, s)
	case d < 24*time.Hour:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		if m == 0 {
			return fmt.Sprintf("%dh", h)
		}
		return fmt.Sprintf("%dh%dm", h, m)
	default:
		days := int(d.Hours() / 24)
		h := int(d.Hours()) % 24
		if h == 0 {
			return fmt.Sprintf("%dd", days)
		}
		return fmt.Sprintf("%dd%dh", days, h)
	}
}
