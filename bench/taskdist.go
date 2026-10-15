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
	"strconv"
	"strings"
	"time"
)

// delayDist is a discrete distribution of task durations, sampled once per task invocation.
//
// It exists to put several durations on ONE crew at once - 5 ms lookups beside 30 s model calls - which a
// single -task-delay cannot: an arm per duration measures each class alone and never whether the fast class
// starves behind the slow one. Sampling per INVOCATION rather than per flow or per task name is the point:
// a class tied to a step's position in its chain would make every flow's slow step land on the same depth,
// and the run would measure that alignment instead of the mix.
type delayDist []delayClass

type delayClass struct {
	weight float64 // fraction of invocations, the classes summing to 1
	delay  time.Duration
}

// parseDelayDist reads "0.80:5ms,0.15:200ms,0.05:8s". The weights must sum to 1, so a typo that shifts the
// mix (0.5 for 0.05) fails the run rather than silently normalizing into a different workload.
func parseDelayDist(s string) (delayDist, error) {
	if strings.TrimSpace(s) == "" {
		return nil, nil
	}
	var dist delayDist
	total := 0.0
	for part := range strings.SplitSeq(s, ",") {
		w, d, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("-task-delay-dist entry %q: want weight:duration", part)
		}
		weight, err := strconv.ParseFloat(w, 64)
		if err != nil || !(weight > 0) || math.IsInf(weight, 0) {
			return nil, fmt.Errorf("-task-delay-dist entry %q: weight must be a positive number", part)
		}
		delay, err := time.ParseDuration(d)
		if err != nil || delay < 0 {
			return nil, fmt.Errorf("-task-delay-dist entry %q: invalid duration", part)
		}
		dist = append(dist, delayClass{weight: weight, delay: delay})
		total += weight
	}
	if math.Abs(total-1) > 1e-6 {
		return nil, fmt.Errorf("-task-delay-dist weights sum to %g, want 1", total)
	}
	return dist, nil
}

// sample maps u, uniform in [0, 1), to a class's delay. Taking u rather than drawing it keeps this pure, so
// the class boundaries are testable without statistics.
func (d delayDist) sample(u float64) time.Duration {
	acc := 0.0
	for _, c := range d {
		acc += c.weight
		if u < acc {
			return c.delay
		}
	}
	// u past the last boundary: the weights sum to 1 only within rounding.
	return d[len(d)-1].delay
}

// mean is the expected delay, for the artifact and the stats line.
func (d delayDist) mean() time.Duration {
	m := 0.0
	for _, c := range d {
		m += c.weight * float64(c.delay)
	}
	return time.Duration(m)
}

// longest is the slowest class, which the stall watchdog's timeout must exceed.
func (d delayDist) longest() time.Duration {
	var longest time.Duration
	for _, c := range d {
		longest = max(longest, c.delay)
	}
	return longest
}
