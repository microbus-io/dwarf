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

package main

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
)

// realMaxPriority is the engine's default priority range, 1..5, which is what the harness runs at.
const realMaxPriority = 5

// realDefaultPriority is the engine's default priority, the midpoint of that range.
const realDefaultPriority = (1 + realMaxPriority) / 2

// priorityClass is one priority level of -priority-mix and the share of flows created at it.
type priorityClass struct {
	priority int
	weight   float64
}

// priorityMix is -priority-mix parsed, best (lowest) priority first.
type priorityMix []priorityClass

// parsePriorityMix parses "priority:weight,..." - for example "1:0.03,2:0.12,3:0.55,4:0.2,5:0.1" - with each
// priority in the engine's range, none repeated, and the weights summing to 1. An empty string is every flow at
// the engine's default priority, set explicitly so the flow's own label and the engine's agree.
func parsePriorityMix(s string) (priorityMix, error) {
	if strings.TrimSpace(s) == "" {
		return priorityMix{{priority: realDefaultPriority, weight: 1}}, nil
	}
	var mix priorityMix
	total := 0.0
	for part := range strings.SplitSeq(s, ",") {
		p, w, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("-priority-mix entry %q: want priority:weight", part)
		}
		priority, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || priority < 1 || priority > realMaxPriority {
			return nil, fmt.Errorf("-priority-mix entry %q: priority must be 1..%d", part, realMaxPriority)
		}
		if slices.ContainsFunc(mix, func(c priorityClass) bool { return c.priority == priority }) {
			return nil, fmt.Errorf("-priority-mix entry %q: priority %d given twice", part, priority)
		}
		weight, err := strconv.ParseFloat(strings.TrimSpace(w), 64)
		if err != nil || !(weight > 0) || math.IsInf(weight, 0) {
			return nil, fmt.Errorf("-priority-mix entry %q: weight must be a positive number", part)
		}
		mix = append(mix, priorityClass{priority: priority, weight: weight})
		total += weight
	}
	if math.Abs(total-1) > 1e-6 {
		return nil, fmt.Errorf("-priority-mix weights sum to %g, want 1", total)
	}
	slices.SortFunc(mix, func(a, b priorityClass) int { return a.priority - b.priority })
	return mix, nil
}

// sample returns the priority a uniform draw u in [0, 1) lands on.
func (m priorityMix) sample(u float64) int {
	for _, c := range m {
		if u < c.weight {
			return c.priority
		}
		u -= c.weight
	}
	return m[len(m)-1].priority // rounding left u at or just past the last weight
}

// bandLabel is the series key for one priority level.
func bandLabel(priority int) string {
	return "band:" + strconv.Itoa(priority)
}
