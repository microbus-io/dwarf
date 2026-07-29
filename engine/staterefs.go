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
	"encoding/json"
	"strings"

	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// State refs de-duplicate a large state field across the steps that merely CARRY it - the mechanism and its
// size policy live in internal/staterefs. This file is the engine's side of it: the three things the Linker
// deliberately does not own, because each is engine state rather than ref policy.
//
//   - WHICH Linker. One per shard (built in initRuntime), because a step id is only unique within a shard
//     and the resolve cache is keyed on it.
//   - HOW an anchor is read. anchorLoader turns a shard handle - which may be a transaction, since anchors
//     never cross a flow and a flow never crosses a shard - into the Loader the Linker calls once per
//     resolve.
//   - The BYTE METRIC. anchorLoader counts the RAW payload columns it scanned - the honest measure for a
//     metric whose job is to track the database's byte-throughput ceiling - and the wrappers attribute the
//     total to a workflow.
//
// The wrappers below are thin on purpose: they exist so the call sites in execution.go / completion.go do
// not each repeat the linker pick, the loader build and the metric emit.

// linkerFor returns the Linker serving a shard. Every shard opened at Startup has one, so a nil return
// means the shard set changed under us - impossible while running, since the set is construction-time only.
func (e *Engine) linkerFor(shardNum int) *staterefs.Linker {
	return e.linkers[shardNum]
}

// anchorLoader adapts a shard handle to the Linker's Loader: one query for every anchor at once, which is
// what the policy's "an anchor's other fields are free" tier is priced on.
//
// db may be a transaction - anchors never cross a flow, and a flow never crosses a shard, so resolution is
// always a same-connection read and the fan-in and final-state paths can resolve inside the transactions
// they already run in.
//
// state_refs comes along so the one-hop invariant can be ASSERTED rather than trusted - a violated one would
// otherwise degrade silently into a chain walk.
//
// readBytes accumulates the raw length of every payload column scanned. Counted here rather than from the
// decoded values the Linker hands back: those drop each column's keys, braces and separators, and count zero
// for a column that failed to decode - both wrong for a metric tracking a byte-throughput ceiling.
func anchorLoader(db sequel.Executor, readBytes *int) staterefs.Loader {
	return func(ctx context.Context, anchorIDs []int) (map[int]staterefs.Anchor, error) {
		if len(anchorIDs) == 0 {
			return nil, nil
		}
		args := make([]any, len(anchorIDs))
		for i, id := range anchorIDs {
			args[i] = id
		}
		ph := strings.Repeat("?,", len(anchorIDs)-1) + "?"
		rows, err := db.QueryContext(ctx,
			"SELECT step_id, state, changes, state_refs FROM dwarf_steps WHERE step_id IN ("+ph+")",
			args...,
		)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer rows.Close()
		fetched := map[int]staterefs.Anchor{}
		for rows.Next() {
			var anchorID int
			var stateJSON, changesJSON, refsJSON []byte
			if err := rows.Scan(&anchorID, &stateJSON, &changesJSON, &refsJSON); err != nil {
				return nil, errors.Trace(err)
			}
			*readBytes += len(stateJSON) + len(changesJSON)
			anchor := staterefs.Anchor{Refs: staterefs.Parse(refsJSON)}
			json.Unmarshal(stateJSON, &anchor.State)
			json.Unmarshal(changesJSON, &anchor.Changes)
			fetched[anchorID] = anchor
		}
		if err := rows.Err(); err != nil {
			return nil, errors.Trace(err)
		}
		return fetched, nil
	}
}

// resolveStateRefs materializes ref'd fields into state, in place. want, when non-nil, selects which refs to
// resolve; the rest are left for the caller to carry forward.
//
// It returns the bytes of field value it spliced into state, which is what a caller about to HOLD that state
// meters its residency with - distinct from the read bytes reported to dwarf_state_read_bytes, which are the
// raw columns scanned (whole anchor rows, and zero on a cache hit). Only the dispatch path needs it; every
// other caller resolves inside a transaction and discards it.
func (e *Engine) resolveStateRefs(ctx context.Context, db sequel.Executor, shardNum int, state workflow.State, refs staterefs.Refs, want map[string]bool, workflowURL string) (int, error) {
	var readBytes int
	materialized, err := e.linkerFor(shardNum).Resolve(ctx, state, refs, want, anchorLoader(db, &readBytes))
	if readBytes > 0 {
		e.metricStateReadBytes(ctx, workflowURL, "state_ref", readBytes)
	}
	return materialized, errors.Trace(err)
}

// resolveReducedRefs materializes only the ref'd fields a fan-in's reducers actually need, into state in
// place. A merely-carried ref is deliberately left un-materialized and re-emitted by the caller's mint.
func (e *Engine) resolveReducedRefs(ctx context.Context, tx sequel.Executor, shardNum int, state workflow.State, refs staterefs.Refs, reducers map[string]workflow.Reducer, workflowURL string) error {
	var readBytes int
	err := e.linkerFor(shardNum).ResolveReduced(ctx, state, refs, reducers, anchorLoader(tx, &readBytes))
	if readBytes > 0 {
		e.metricStateReadBytes(ctx, workflowURL, "state_ref", readBytes)
	}
	return errors.Trace(err)
}

// resolvedStepState materializes a step's state column into JSON with every ref'd field spliced back in -
// the flatten used where a state snapshot must stand on its own, detached from the anchors that backed it
// (Fork's leaf step, whose anchor ids are about to be remapped into a different flow).
func (e *Engine) resolvedStepState(ctx context.Context, db sequel.Executor, shardNum int, stateJSON, refsJSON []byte, workflowURL string) ([]byte, error) {
	var readBytes int
	resolved, err := e.linkerFor(shardNum).Flatten(ctx, stateJSON, staterefs.Parse(refsJSON), anchorLoader(db, &readBytes))
	if readBytes > 0 {
		e.metricStateReadBytes(ctx, workflowURL, "state_ref", readBytes)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resolved, nil
}
