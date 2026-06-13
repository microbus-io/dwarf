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
	"math"
	"time"

	"github.com/microbus-io/throttle"
)

// taskValve is the per-task controller state. wCong (ops/sec) and tCong are gossiped via
// SyncValve; the throttle is per-replica. wCong == 0 means no 429 has anchored yet - the
// throttle counts dispatches but does not gate them.
type taskValve struct {
	wCong    int
	tCong    time.Time
	throttle *throttle.Throttle
}

// recoverRate is the TCP CUBIC recovery curve:
// w(t) = cubicC*(t-K)^3 + wMax, K = cbrt(wMax*cubicBeta/cubicC), wMax = wCong/(1-cubicBeta).
// Clamped to [1, MaxInt]. wCong and tCong are passed by value so callers can snapshot them under
// valvesLock and compute the rate without holding the lock (the fields are gossip-mutated concurrently).
func recoverRate(wCong int, tCong, now time.Time) int {
	const cubicBeta = 0.01
	const cubicC = 0.05

	elapsed := now.Sub(tCong).Seconds()
	if elapsed < 0 {
		elapsed = 0
	}
	wMax := float64(wCong) / (1 - cubicBeta)
	k := math.Cbrt(wMax * cubicBeta / cubicC)
	delta := elapsed - k
	w := cubicC*delta*delta*delta + wMax
	if w >= float64(math.MaxInt) {
		return math.MaxInt
	}
	return max(int(w), 1)
}
