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

import (
	"context"
	"time"

	"github.com/microbus-io/dwarf/internal/diagnostics"
)

// The names the engine adds its recovery jobs under.
const (
	JobLeaseRecovery = "lease_recovery"
	JobOrphanRepair  = "orphan_repair"
	JobWedgeSweep    = "wedge_sweep"
)

// RecoveryPass is one pass of a repair over a shard: it looks for what the repair recovers and recovers what it finds.
// It returns an error when it could not look at, or recover, everything it should have, and nil when it did.
type RecoveryPass func(ctx context.Context) error

// Recovery is the job that runs a repair's pass on its shard every interval, through the pass it is given: a round is
// one pass, and the next begins an interval after it ended. A pass that ran to its end without an error is progress.
type Recovery struct {
	every time.Duration
	pass  RecoveryPass
	j     *Janitor

	// ended is when the last pass ended, zero before the first. Only the round and reset touch it.
	ended time.Time
}

// NewRecovery returns a recovery job that runs pass every interval, which must be positive.
func NewRecovery(every time.Duration, pass RecoveryPass) *Recovery {
	return &Recovery{every: every, pass: pass}
}

// attach keeps the janitor, whose seams a failed pass is reported through.
func (r *Recovery) attach(j *Janitor, _ string) { r.j = j }

// detach has nothing to release.
func (r *Recovery) detach() {}

// idleWait is the interval.
func (r *Recovery) idleWait() time.Duration {
	return r.every
}

// reset forgets the last pass: a replica that dispatches the shard again runs one at once.
func (r *Recovery) reset() {
	r.ended = time.Time{}
}

// round runs one pass as one batch, unless the last ended less than an interval ago. The round after a pass that made
// progress therefore finds the interval not yet passed, and the idle wait follows it.
func (r *Recovery) round(ctx context.Context, batch batchFunc) (progress bool) {
	if r.pass == nil || (!r.ended.IsZero() && time.Since(r.ended) < r.every) {
		return false
	}
	err := batch(ctx, func(ctx context.Context, _ handle) error {
		return r.pass(ctx)
	})
	r.ended = time.Now()
	if err != nil && ctx.Err() == nil {
		r.j.diags.Seams().Checkpoint(ctx, diagnostics.CheckpointJanitorFailed)
	}
	return err == nil
}
