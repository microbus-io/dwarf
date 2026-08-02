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
	"context"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// benchHost is the proxy host the bench engine runs against: an in-memory graph registry and a local task
// dispatch table, with no transport or framework dependency. Deliberately minimal so the measurements
// isolate the engine+database cost. In a multi-replica run every replica's host shares the same graph/task
// registry and byte counter, and its own view of the fleet (peers) for signal relay.
type benchHost struct {
	graphs map[string]*workflow.Graph
	tasks  map[string]func(ctx context.Context, f *workflow.Flow) error

	// profile is remote-executor latency plus task run time (the "exec" term of the sizing model): every
	// task sleeps for whatever it returns before running its body. It is a PROFILE rather than a constant
	// so a run can raise and drop the exec term partway through - see taskProfile for why a fixed delay
	// cannot measure a crew that only shrinks once load falls.
	profile taskProfile
	// taskJitter adds a uniform random [0, jitter) on top, which DE-SYNCHRONIZES fan-out siblings: without
	// it a cohort's branches are dispatched together, run for identical durations, and therefore all
	// complete at the same instant, piling onto the shared cohort-arrival row at once. Spreading arrivals
	// is how a contention hypothesis about that row is tested causally.
	taskJitter time.Duration

	// bytesWritten counts the state payload bytes tasks wrote, for MB-throughput accounting. A shared
	// pointer so every replica's host accumulates into one fleet total.
	bytesWritten *atomic.Int64
}

func (h *benchHost) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	return h.graphs[workflowURL], nil // nil is a clean 404 at Create
}

func (h *benchHost) ExecuteTask(ctx context.Context, taskURL string, f *workflow.Flow) error {
	task := h.tasks[taskURL]
	if task == nil {
		return errors.New("unknown task %q", taskURL)
	}
	delay := h.profile.delayAt(time.Now())
	if h.taskJitter > 0 {
		delay += time.Duration(rand.Int64N(int64(h.taskJitter)))
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}
	}
	return task(ctx, f)
}
