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

// Package memlimit reports how much memory this process may use, for sizing caches as a share of it.
//
//	limit, source := memlimit.Detect()
//	if limit == 0 {
//		// nothing could be detected; fall back to a fixed size
//	}
//
// The sources, in order: the Go runtime's soft memory limit (GOMEMLIMIT or debug.SetMemoryLimit), when set;
// then, on Linux, the tightest of the memory limits on this process's cgroup and its ancestors, and physical
// memory. Elsewhere only the first.
package memlimit

import (
	"math"
	"runtime/debug"
)

// Source names where a detected limit came from.
const (
	SourceGoMemLimit = "GOMEMLIMIT"
	SourceCgroup     = "cgroup"
	SourcePhysical   = "physical"
	SourceNone       = "none"
)

// Detect returns the memory this process may use, in bytes, and the source it came from. It returns 0 and
// SourceNone when nothing could be detected.
func Detect() (int64, string) {
	if limit := debug.SetMemoryLimit(-1); limit > 0 && limit < math.MaxInt64 {
		return limit, SourceGoMemLimit
	}
	return detectSystem()
}
