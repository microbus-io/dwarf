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
	"bufio"
	"bytes"
	"cmp"
	"fmt"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
)

// stackGroup is a number of the crew's workers found at one place.
type stackGroup struct {
	count int
	where string
}

// printStackGroups prints where the crew's workers are, the n largest groups, below a stats line. A worker's place
// is the innermost frame of its stack outside the runtime, then the innermost engine frames that led there. The
// profile is the runtime's own goroutine profile, which stops the world while it is taken: at tens of thousands of
// goroutines that is milliseconds, so take it no more often than the stats line.
func printStackGroups(n int) {
	var buf bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&buf, 1); err != nil {
		return
	}
	groups := crewStacks(buf.Bytes())
	total := 0
	for _, g := range groups {
		total += g.count
	}
	var out strings.Builder
	fmt.Fprintf(&out, "        crew stacks: %d workers\n", total)
	for i, g := range groups {
		if i == n {
			break
		}
		fmt.Fprintf(&out, "        %7d  %s\n", g.count, g.where)
	}
	fmt.Print(out.String())
}

// crewStacks reads a goroutine profile (debug=1) and groups the crew's workers by where they are.
func crewStacks(profile []byte) []stackGroup {
	byWhere := map[string]int{}
	sc := bufio.NewScanner(bytes.NewReader(profile))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	count := 0
	var frames []string
	flush := func() {
		defer func() { count, frames = 0, frames[:0] }()
		if count == 0 || !slices.ContainsFunc(frames, func(f string) bool { return strings.Contains(f, "workers.(*Crew).work") }) {
			return
		}
		leaf := ""
		var path []string
		for _, f := range frames {
			if leaf == "" && !strings.HasPrefix(f, "runtime.") && !strings.HasPrefix(f, "internal/") {
				leaf = f
			}
			if rest, ok := strings.CutPrefix(f, "github.com/microbus-io/dwarf/"); ok && !strings.HasPrefix(rest, "internal/workers") && len(path) < 4 {
				path = append(path, rest)
			}
		}
		byWhere[strings.TrimPrefix(leaf, "github.com/microbus-io/")+"  <-  "+strings.Join(path, " < ")] += count
	}
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, "#"):
			fields := strings.Fields(line)
			if len(fields) >= 3 {
				name, _, _ := strings.Cut(fields[2], "+0x")
				frames = append(frames, name)
			}
		default:
			if c, _, ok := strings.Cut(line, " @ "); ok {
				count, _ = strconv.Atoi(c)
			}
		}
	}
	flush()
	groups := make([]stackGroup, 0, len(byWhere))
	for w, c := range byWhere {
		groups = append(groups, stackGroup{count: c, where: w})
	}
	slices.SortFunc(groups, func(a, b stackGroup) int { return cmp.Compare(b.count, a.count) })
	return groups
}
