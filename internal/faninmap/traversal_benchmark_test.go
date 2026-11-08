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

package faninmap

import (
	"fmt"
	"testing"

	"github.com/microbus-io/dwarf/workflow"
)

// BenchmarkGraphTraversal excludes graph construction and checks validity before timing either traversal.
// Chain and Wide vary vertex/edge count; Nested also varies lineage depth.
func BenchmarkGraphTraversal(b *testing.B) {
	for _, shape := range []string{"Chain", "Wide", "Nested"} {
		sizes := []int{1000, 2000, 4000, 8000}
		if shape == "Nested" {
			sizes = []int{32, 64, 128, 256}
		}
		for _, n := range sizes {
			b.Run(fmt.Sprintf("%s/%d", shape, n), func(b *testing.B) {
				g := workflow.NewGraph("Traversal")
				switch shape {
				case "Chain":
					for i := 0; i < n; i++ {
						to := workflow.End
						if i+1 < n {
							to = fmt.Sprintf("T%d", i+1)
						}
						g.AddTransition(fmt.Sprintf("T%d", i), to)
					}
				case "Wide":
					for i := 0; i < n; i++ {
						name := fmt.Sprintf("B%d", i)
						g.AddTransition("Start", name)
						g.AddTransition(name, "Join")
					}
					g.SetFanIn("Join")
					g.AddTransition("Join", workflow.End)
				case "Nested":
					for i := 0; i < n; i++ {
						source, branch, join := fmt.Sprintf("S%d", i), fmt.Sprintf("B%d", i), fmt.Sprintf("J%d", i)
						next := "Work"
						if i+1 < n {
							next = fmt.Sprintf("S%d", i+1)
						}
						g.AddTransitionFanOut(source, branch, next)
						g.AddTransition(branch, join)
						g.SetFanIn(join)
						if i == 0 {
							g.AddTransition(join, workflow.End)
						} else {
							g.AddTransition(join, fmt.Sprintf("J%d", i-1))
						}
					}
					g.AddTransition("Work", fmt.Sprintf("J%d", n-1))
				}
				if err := g.Validate(); err != nil {
					b.Fatal(err)
				}
				b.Run("Validate", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						if err := g.Validate(); err != nil {
							b.Fatal(err)
						}
					}
				})
				b.Run("FanInMap", func(b *testing.B) {
					b.ReportAllocs()
					for b.Loop() {
						_ = New(g)
					}
				})
			})
		}
	}
}
