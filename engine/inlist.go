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

import "strings"

// inListChunk is the most ids one bound IN (...) list carries. SQL Server rejects a statement with more than
// 2,100 parameters, the tightest ceiling of the four dialects, so an id list whose length the workload sets - a
// cohort's members, a subgraph tree's flows - goes out in chunks of this many, with room left for the
// statement's other binds and for a statement that binds the list twice.
const inListChunk = 512

// inList returns the placeholders and arguments of an IN (...) list over one chunk of at most inListChunk ids.
// The list is padded up to a power of two by repeating the last id, which leaves the set unchanged, so the
// statement text takes one of a few arities rather than one per length.
func inList[T any](ids []T) (placeholders string, args []any) {
	arity := 1
	for arity < len(ids) {
		arity *= 2
	}
	args = make([]any, arity)
	for i := range args {
		args[i] = ids[min(i, len(ids)-1)]
	}
	return strings.Repeat("?,", arity-1) + "?", args
}
