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

import (
	"context"
	"maps"
	"slices"
	"time"
)

const (
	// retryInterval is the cadence of driveLeaseRecovery, the one helper that still loops - it DRIVES the engine
	// (it calls recoverLeases) rather than waiting for it, so it is not a spin-wait on an observation.
	retryInterval = 20 * time.Millisecond

	// leaseRecoveryWait bounds a lease-recovery re-dispatch - see driveLeaseRecovery.
	leaseRecoveryWait = 2 * time.Second
)

// recoverLeases runs lease recovery's pass on every shard this replica dispatches, each under a turn of its own, as
// the lease recovery job runs it: what a test drives where it cannot wait for the job, which runs only in a drain or,
// with janitorManual off, an interval apart.
func (e *Engine) recoverLeases(ctx context.Context) {
	actors := e.allActors()
	for _, shard := range slices.Sorted(maps.Keys(actors)) {
		if !e.dispatchesOn(shard) {
			continue
		}
		sh, err := e.db.Shard(shard)
		if err != nil {
			continue
		}
		turnCtx, done := e.dbTurn(ctx, shard)
		_ = e.recoverExpiredLeases(turnCtx, sh.DB(), shard) // logged by the pass; a test asserts on the rows
		done()
	}
}
