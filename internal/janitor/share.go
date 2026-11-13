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

package janitor

import "strconv"

// ShareName is the name of the k-th fast job of a kind on a shard, job its kind's name (JobFastArchiving,
// JobFastDeletion), and of the sweep behind it (JobSweepArchiving, JobSweepDeletion): job_k.
func ShareName(job string, k int) string {
	return job + "_" + strconv.Itoa(k)
}

// share is which of a kind's fast jobs on a shard a job is, the k-th of of, from 1, and so which roots it takes - and
// its sweep behind it. A share of one takes every root.
type share struct {
	k, of int
}

// sql restricts a scan to the share's roots, whatever their residue class, under a residue class of count
// dispatchers: those whose flow_id / count is k-1 modulo of, or whose flow_id is, when the class restricts nothing. A
// root's share does not depend on its class, so of another class's roots overdue by the steal grace a job steals only
// its share's.
//
// It is one modulus, since / is not integer division on MySQL - flow_id % (count*of) from count*(k-1) to count*k-1 -
// rotated to a single comparison: (flow_id + count*of - count*(k-1)) % (count*of) < count. Do not write it as a range
// (BETWEEN, or two comparisons): PostgreSQL's estimate of a range on an expression takes the scans off their index
// walks (TestShare_TheScansWalkTheirIndexes).
func (s share) sql(count, ordinal int, ok bool) (clause string, args []any) {
	if s.of < 2 {
		return "", nil
	}
	if !restricts(count, ordinal, ok) {
		count = 1
	}
	return " AND (flow_id + ?) % ? < ?", []any{count*s.of - count*(s.k-1), count * s.of, count}
}

// restricts reports whether a residue class of count dispatchers, this replica's the ordinal-th, restricts a scan. It
// fails open, as the piston's partition does: an unknown or invalid pair, or a solo dispatcher, restricts nothing.
func restricts(count, ordinal int, ok bool) bool {
	return ok && count >= 2 && ordinal >= 0 && ordinal < count
}
