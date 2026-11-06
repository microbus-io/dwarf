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
	"math/rand/v2"
	"strconv"
	"strings"
)

// mixClass is one -mix entry: a share of the flows, run as one workload at one payload and shape.
type mixClass struct {
	share    float64
	workload string
	payload  int
	width    int
	depth    int
	// name is the entry as written, less its share ("fanout:4K:8"), so a report names classes the way the run
	// was asked for them.
	name string
}

// parseMix reads -mix: comma-separated share:workload:payload[:width[:depth]], e.g.
// "0.85:linear:1K,0.15:fanout:4K:8". Payload takes a K or M suffix (1,024 and 1,048,576 bytes). Width and
// depth default to fanoutWidth and 1. Shares must sum to 1.
func parseMix(s string, fanoutWidth int) ([]mixClass, error) {
	var classes []mixClass
	sum := 0.0
	for entry := range strings.SplitSeq(s, ",") {
		parts := strings.Split(strings.TrimSpace(entry), ":")
		if len(parts) < 3 || len(parts) > 5 {
			return nil, fmt.Errorf("-mix: %q is not share:workload:payload[:width[:depth]]", entry)
		}
		share, err := strconv.ParseFloat(parts[0], 64)
		if err != nil || share <= 0 {
			return nil, fmt.Errorf("-mix: %q: the share must be a positive number", entry)
		}
		switch parts[1] {
		case "linear", "fanout", "scatter":
		default:
			return nil, fmt.Errorf("-mix: %q: the workload must be linear, fanout or scatter", entry)
		}
		payload, err := parseBytes(parts[2])
		if err != nil {
			return nil, fmt.Errorf("-mix: %q: %w", entry, err)
		}
		c := mixClass{share: share, workload: parts[1], payload: payload, width: fanoutWidth, depth: 1,
			name: strings.Join(parts[1:], ":")}
		if len(parts) > 3 {
			if c.width, err = strconv.Atoi(parts[3]); err != nil || c.width < 1 {
				return nil, fmt.Errorf("-mix: %q: the width must be a positive integer", entry)
			}
		}
		if len(parts) > 4 {
			if c.depth, err = strconv.Atoi(parts[4]); err != nil || c.depth < 1 {
				return nil, fmt.Errorf("-mix: %q: the depth must be a positive integer", entry)
			}
		}
		classes = append(classes, c)
		sum += share
	}
	if math.Abs(sum-1) > 1e-6 {
		return nil, fmt.Errorf("-mix: the shares sum to %g, not 1", sum)
	}
	return classes, nil
}

// parseBytes reads a byte count with an optional K or M suffix.
func parseBytes(s string) (int, error) {
	mult := 1
	switch {
	case strings.HasSuffix(s, "K"):
		mult, s = 1<<10, strings.TrimSuffix(s, "K")
	case strings.HasSuffix(s, "M"):
		mult, s = 1<<20, strings.TrimSuffix(s, "M")
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%q is not a byte count (e.g. 0, 4K, 1M)", s)
	}
	return n * mult, nil
}

// buildMix registers one workload set per distinct payload and shape in the mix, each under its own namespace so
// their graphs and tasks do not collide, and returns a picker that draws a class per flow by share. Every class's
// payload is built up front. meanSteps is the mix's expected steps per flow, which converts a steps/s target into
// flows/s.
func buildMix(h *benchHost, classes []mixClass, chainSteps int) (pick func() *workload, picked []*workload, meanSteps float64) {
	type shape struct{ payload, width, depth int }
	sets := map[shape]map[string]*workload{}
	picked = make([]*workload, len(classes))
	cum := make([]float64, len(classes))
	total := 0.0
	for i, c := range classes {
		key := shape{c.payload, c.width, c.depth}
		set := sets[key]
		if set == nil {
			set = registerWorkloads(h, fmt.Sprintf("m%d/", len(sets)+1), c.payload, false, c.width, chainSteps, c.depth, false)
			sets[key] = set
		}
		w := *set[c.workload]
		w.class = c.name
		w.prepare()
		picked[i] = &w
		total += c.share
		cum[i] = total
		meanSteps += c.share * float64(w.stepsPerFlow)
	}
	return func() *workload {
		u := rand.Float64() * total
		for i, edge := range cum {
			if u < edge {
				return picked[i]
			}
		}
		return picked[len(picked)-1]
	}, picked, meanSteps
}
