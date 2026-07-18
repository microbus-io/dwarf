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

	"github.com/microbus-io/dwarf/engine"
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

	// taskDelay simulates remote-executor latency plus task run time (the "exec" term of the sizing
	// model): every task sleeps this long before running its body. taskJitter adds a uniform random
	// [0, jitter) on top, which DE-SYNCHRONIZES fan-out siblings: without it a cohort's branches are
	// dispatched together, run for identical durations, and therefore all complete at the same instant,
	// piling onto the shared cohort-arrival row at once. Spreading arrivals is how a contention
	// hypothesis about that row is tested causally.
	taskDelay  time.Duration
	taskJitter time.Duration

	// bytesWritten counts the state payload bytes tasks wrote, for MB-throughput accounting. A shared
	// pointer so every replica's host accumulates into one fleet total.
	bytesWritten *atomic.Int64

	// peers is the whole fleet (self included; the engine discards its own echo by Origin). A single
	// replica leaves it nil, so SignalPeers is a no-op - the original behavior.
	peers []*engine.Engine

	// Signal-fault injection, simulating an imperfect peer transport. The engine must tolerate all of
	// it: flow doorbells/wakes recover via the poll backstop, and peer discovery must hold R stable
	// under occasional loss (a ping is re-sent every pingInterval; eviction takes 3 misses).
	//
	// signalJitter delays every delivery by a uniform random [0, jitter) - network latency. signalDrop
	// drops each per-peer delivery independently with this probability - random loss. dropOps drops
	// EVERY delivery of the named ops (e.g. "enqueue" for the D3 poll-fallback A/B) while leaving the
	// others - including peer discovery - intact. muted silences ALL outbound signals from this replica,
	// simulating a crashed peer to the rest of the fleet (its pings stop; peers evict it after
	// ~3x pingInterval) while the process itself keeps running.
	signalJitter time.Duration
	signalDrop   float64
	dropOps      map[string]bool
	muted        atomic.Bool
}

func (h *benchHost) LoadGraph(ctx context.Context, workflowURL string) (*workflow.Graph, error) {
	return h.graphs[workflowURL], nil // nil is a clean 404 at Create
}

func (h *benchHost) ExecuteTask(ctx context.Context, taskURL string, f *workflow.Flow) error {
	task := h.tasks[taskURL]
	if task == nil {
		return errors.New("unknown task %q", taskURL)
	}
	delay := h.taskDelay
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

func (h *benchHost) SignalPeers(ctx context.Context, op string, payload []byte) {
	// Relay to every replica in the fleet, through the fault-injection gauntlet above. The transport is
	// in-process (direct DeliverSignal calls); jitter/drop make it imperfect on purpose. Delivering to
	// self is harmless: DeliverSignal discards its own echo by Origin. A nil peers slice (single
	// replica) makes this a no-op.
	if h.muted.Load() || h.dropOps[op] {
		return
	}
	// The engine calls SignalPeers inline on the completion hot path, so the jitter delay must be
	// asynchronous - a real network host sends without blocking the caller. The payload is a freshly
	// marshaled slice, safe to reference after return; delivery uses a background ctx because the
	// caller's may be done by then (a network delivers anyway; a stopped engine's DeliverSignal is inert).
	deliver := func() {
		for _, p := range h.peers {
			if h.signalDrop > 0 && rand.Float64() < h.signalDrop {
				continue
			}
			_ = p.DeliverSignal(context.Background(), op, payload)
		}
	}
	if h.signalJitter > 0 {
		go func() {
			time.Sleep(time.Duration(rand.Int64N(int64(h.signalJitter))))
			deliver()
		}()
		return
	}
	deliver()
}
