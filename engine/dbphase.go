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
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The two database phases a worker passes through, named here only so the instrumentation below can label
// them. Nothing meters this split - the turnstile bounds database CALLS, not phases - so these are an
// observation and nothing else.
const (
	phaseEnter = "enter"
	phaseExit  = "exit"
)

// enterDBPhase records that a worker has entered a metered database phase and returns the closer for it. The
// closer is idempotent, because the entry phase ends at one specific line (immediately before the host call)
// while every early return before it - a lost claim, a terminal flow, a failed load - must close it too.
//
// WHY THIS EXISTS. Neither of the two questions it answers could be asked before:
//
//   - HOW MANY workers are in a database phase at once. dwarf_workers_resident is the whole crew, and a worker
//     parked in ExecuteTask still holds its candidate, so the crew's idle count cannot be differenced to get
//     it either. This is the quantity that sets connection-pool pressure, and it was invisible.
//   - HOW LONG a phase actually takes. Measured on a 16-vCPU shard, entry residence ran to hundreds of
//     milliseconds against ~1ms of its own database work - the rest being connection wait and Go-side work -
//     which is not a ratio anyone would guess, and it is what says whether the turn count is anywhere near
//     the concurrency the workload really needs.
//
// The counts are per replica rather than per shard: a worker's phase is not shard-scoped in any way the
// caller can cheaply attribute, and pool pressure is what the numbers are read against.
func (e *Engine) enterDBPhase(role string) func() {
	idx := 0
	if role == phaseExit {
		idx = 1
	}
	e.dbPhaseWorkers[idx].Add(1)
	start := time.Now()
	closed := false
	return func() {
		if closed {
			return
		}
		closed = true
		e.dbPhaseWorkers[idx].Add(-1)
		if e.metrics != nil && e.metrics.dbPhaseSeconds != nil {
			e.metrics.dbPhaseSeconds.Record(e.lifetimeCtx, time.Since(start).Seconds(),
				metric.WithAttributes(attribute.String("role", role)))
		}
	}
}
