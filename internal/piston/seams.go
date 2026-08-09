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

package piston

import "strings"

// FaultScanErr makes ScanBand fail without touching the database - see SetSeams. The name is exported so
// the owning application's fault catalogue can alias it rather than re-spell the string.
const FaultScanErr = "refillScanErr"

// CheckpointCycleDone fires once per SUPPLY cycle that PUSHED - see SetSeams. Exported for the same
// catalogue reason as FaultScanErr.
//
// It is fired only when the cycle reached its push, which is exactly when this shard's cache partition has
// been reconciled against the plan: the fetch-error path returns before pushing and deliberately leaves the
// partition alone, while an empty plan pushes nothing and CLEARS it. So
// a visit means "this shard's partition now reflects the plan", which is the thing a test can neither
// observe from outside nor wait out on a clock - each piston turns on its own cadence, and a shard whose
// goroutine is starved or blocked on a slow round trip can hold an unreconciled partition arbitrarily long
// while its peers turn normally.
//
// Fired BOTH unscoped and scoped by shard (a scoped fire does not wake an unscoped waiter, so a waiter for
// "any shard cycled" and one for "shard 3 cycled" need separate fires). Counting scoped visits is the way
// to wait for a SPECIFIC shard, since with several shards the unscoped count says nothing about which.
const CheckpointCycleDone = "refillCycleDone"

// FaultFetchErr makes FetchSteps fail without touching the database - see SetSeams. The name is exported so
// the owning application's fault catalogue can alias it rather than re-spell the string.
//
// It earns its place on the same boundary rule as FaultScanErr, and it covers the failure that is otherwise
// UNOBSERVABLE from outside: a piston whose every fetch fails goes on tallying honestly and claiming its
// band while its partition takes zero candidates, so it looks healthy from every angle a test can reach. It
// is also the shape a real bind-count overflow produces, which has no error counter of its own.
const FaultFetchErr = "refillFetchErr"

// CheckpointTallyDone fires once per TALLY cycle whose scan succeeded and published a tally - see SetSeams.
// Exported for the same catalogue reason as FaultScanErr.
//
// A PUSH DOES NOT IMPLY A SCAN, which is what this exists for. A supply cycle plans from whatever tally the
// planner already holds and touches the database only to fetch, so any number of CheckpointCycleDone visits
// can resolve a tally that predates the work a caller is waiting on. A caller needing "a scan has SEEN what
// I just committed" waits on this FIRST and only then on CheckpointCycleDone. Gated on the scan succeeding,
// because a failed one clears the shard rather than reporting it.
//
// Fired BOTH unscoped and scoped by shard, for the same reason CheckpointCycleDone is.
const CheckpointTallyDone = "refillTallyDone"

// CheckpointStole fires once per fetch that took at least one step from OUTSIDE this replica's residue
// class - see SetSeams. Exported for the same catalogue reason as FaultScanErr.
//
// It earns its place on the same boundary rule as CheckpointCycleDone: it reports an effect on state the
// package borrows, at the moment the effect happens, and no clock substitutes for it. A test proving that a
// slow peer's work is picked up cannot wait out a duration - the steal fires on the first cycle where the
// grace has elapsed and this replica's own class ran short, which is a function of the pipeline's cadence,
// the peer's degradation and the backlog, none of which the test controls. Without it the only assertion available is "the flows
// eventually finished", which passes just as well against a build where stealing does nothing and the
// dispatch-window eviction did the work several seconds later - i.e. it cannot tell the mechanism under
// test from the mechanism it replaces.
//
// Fired BOTH unscoped and scoped by shard, for the same reason CheckpointCycleDone is.
const CheckpointStole = "refillStole"

// seamsJoin builds a targeted seam name: a base name, then the entity it targets, joined with ":". A consult
// site and the test that arms it both call it, so neither can spell the join the other does not. A targeted
// name and the bare one are DIFFERENT seams, so a site wanting both fires both.
func seamsJoin(parts ...string) string {
	return strings.Join(parts, ":")
}
