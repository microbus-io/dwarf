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
)

// JobExpiration is the name the engine adds its expiration job under.
const JobExpiration = "expiration"

// ExpirePass is one expiration pass over a shard: it walks the flows after the flow_id after, ends the expired open
// roots it finds - marking each for deletion as it ends it - and returns the flow_id to go on after, 0 once it has
// swept the shard, and whether it made progress: ended a root, or stopped at its own budget with more to read.
type ExpirePass func(ctx context.Context, after int) (next int, progress bool)

// Expiration is the job that ends the flows still open past the flow expiration, through the pass it is given: a
// round is one pass, from the position the last one stopped at. The deletion of what it ends is fast and sweep
// deletion's. A round every hour, or every half of the expiration when that is shorter, at least a minute apart.
type Expiration struct {
	expiration time.Duration
	pass       ExpirePass

	// after is the position: the flow_id the next pass reads after, 0 for the oldest. swept is when a pass last came
	// back to the oldest, and a sweep from the oldest waits an idle wait past it. Only the round and reset touch them.
	after int
	swept time.Time
}

// NewExpiration returns an expiration job for flows open longer than expiration, which runs pass.
func NewExpiration(expiration time.Duration, pass ExpirePass) *Expiration {
	return &Expiration{expiration: expiration, pass: pass}
}

// attach has nothing to create.
func (x *Expiration) attach(*Janitor, string) {}

// detach has nothing to release.
func (x *Expiration) detach() {}

// idleWait is an hour, or half the expiration when that is shorter, and at least a minute.
func (x *Expiration) idleWait() time.Duration {
	return min(time.Hour, max(x.expiration/2, time.Minute))
}

// reset forgets the position and the last sweep: a replica that dispatches the shard again sweeps from the oldest at
// once.
func (x *Expiration) reset() {
	x.after = 0
	x.swept = time.Time{}
}

// round runs one pass as one batch, the pass ending each tree in a transaction of its own on the shard's DB. It reports
// the pass's progress, but a sweep from the oldest starts only an idle wait past the last one's end: a sweep that ended
// a root would otherwise start the next at once, and a steady trickle of expiring flows keep the job sweeping back to
// back.
func (x *Expiration) round(ctx context.Context, batch batchFunc) (progress bool) {
	if x.pass == nil {
		return false
	}
	if x.after == 0 && !x.swept.IsZero() && time.Since(x.swept) < x.idleWait() {
		return false
	}
	_ = batch(ctx, func(ctx context.Context, _ handle) error { // one that did not run made no progress
		var next int
		next, progress = x.pass(ctx, x.after)
		x.after = next
		if next == 0 {
			x.swept = time.Now()
		}
		return nil
	})
	return progress
}
