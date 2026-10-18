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
	"bytes"
	"context"
	"encoding/json"
	"strings"

	"github.com/microbus-io/dwarf/internal/memlimit"
	"github.com/microbus-io/dwarf/internal/staterefs"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
)

// State refs de-duplicate a large state field across the steps that merely CARRY it - the mechanism and its
// size policy live in internal/staterefs. This file is the engine's side of it: the three things the Linker
// deliberately does not own, because each is engine state rather than ref policy.
//
//   - WHICH Linker. One per shard (built in initRuntime), because a step id is only unique within a shard;
//     all of them share the replica's one stateCache, which keys each field by shard.
//   - HOW an anchor is read. anchorLoader turns a shard handle - which may be a transaction, since anchors
//     never cross a flow and a flow never crosses a shard - into the Loader the Linker calls once per
//     resolve.
//   - The BYTE METRIC. anchorLoader counts the RAW payload columns it scanned - the honest measure for a
//     metric whose job is to track the database's byte-throughput ceiling - and the wrappers attribute the
//     total to a workflow.
//
// The wrappers below are thin on purpose: they exist so the call sites in execution.go / completion.go do
// not each repeat the linker pick, the loader build and the metric emit.

// stateCacheFallback is the resolve-cache budget when the process's memory cannot be detected (no
// GOMEMLIMIT, not Linux).
const stateCacheFallback = 1 << 30

// stateCacheBudget is the engine's resolve-cache budget, shared by every shard's Linker, and where it came from:
// the SetStateCacheSize override, else a tenth of the detected memory limit, else stateCacheFallback.
// A tenth, because the cache only needs the fields of flows in flight, and below that a miss costs a re-read,
// cheap next to the write bytes that bind a large-payload workload. Meanwhile the collector's headroom can
// double a full cache, and the rest of the heap - the crew's carriers, row buffers, the host's own memory -
// needs the remainder.
func (e *Engine) stateCacheBudget() (int64, string) {
	if e.stateCacheBytes > 0 {
		return e.stateCacheBytes, "SetStateCacheSize"
	}
	if limit, source := memlimit.Detect(); limit > 0 {
		return limit / 10, source
	}
	return stateCacheFallback, "fallback"
}

// linkerFor returns the Linker serving a shard. Every shard opened at Startup has one, so a nil return
// means the shard set changed under us - impossible while running, since the set is construction-time only.
func (e *Engine) linkerFor(shardNum int) *staterefs.Linker {
	return e.linkers[shardNum]
}

// decodeAnchorColumn decodes an anchor's state or changes column into its fields. An empty column holds none, as
// for a State.
func decodeAnchorColumn(data []byte, fields *map[string]json.RawMessage) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	return json.Unmarshal(data, fields)
}

// anchorChunk is the most anchors one query names. A list ref names an anchor per contributing step, and SQL
// Server takes at most 2,100 parameters in one statement.
const anchorChunk = 512

// anchorLoader adapts a shard handle to the Linker's Loader: one query for every anchor at once, which is
// what the policy's "an anchor's other fields are free" tier is priced on - split into chunks only past
// anchorChunk, and into two queries when some anchors need their state and others only their changes. A list
// reads its contributions from changes alone, and those steps' state holds their own lists, so reading it too
// would make a cold read of a long list cost the square of its length.
//
// db may be a transaction - anchors never cross a flow, and a flow never crosses a shard, so resolution is
// always a same-connection read and the fan-in and final-state paths can resolve inside the transactions
// they already run in.
//
// state_refs comes along with state so the one-hop invariant can be ASSERTED rather than trusted - a violated
// one would otherwise degrade silently into a chain walk.
//
// readBytes accumulates the raw length of every payload column scanned. Counted here rather than from the
// decoded values the Linker hands back: those drop each column's keys, braces and separators, and count zero
// for a column that failed to decode - both wrong for a metric tracking a byte-throughput ceiling.
func anchorLoader(db sequel.Executor, readBytes *int) staterefs.Loader {
	return func(ctx context.Context, reads []staterefs.Read) (map[int]staterefs.Anchor, error) {
		var withState, changesOnly []int
		for _, r := range reads {
			if r.State {
				withState = append(withState, r.ID)
			} else {
				changesOnly = append(changesOnly, r.ID)
			}
		}
		fetched := map[int]staterefs.Anchor{}
		for _, group := range []struct {
			ids       []int
			withState bool
		}{{withState, true}, {changesOnly, false}} {
			for start := 0; start < len(group.ids); start += anchorChunk {
				chunk := group.ids[start:min(start+anchorChunk, len(group.ids))]
				if err := readAnchors(ctx, db, chunk, group.withState, readBytes, fetched); err != nil {
					return nil, errors.Trace(err)
				}
			}
		}
		return fetched, nil
	}
}

// readAnchors reads one chunk of anchor rows into fetched: their changes, and with withState their state and
// refs too.
func readAnchors(ctx context.Context, db sequel.Executor, ids []int, withState bool, readBytes *int, fetched map[int]staterefs.Anchor) error {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	columns := "step_id, changes"
	if withState {
		columns = "step_id, changes, state, state_refs"
	}
	rows, err := db.QueryContext(ctx,
		"SELECT "+columns+" FROM dwarf_steps WHERE step_id IN ("+strings.Repeat("?,", len(ids)-1)+"?)",
		args...,
	)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()
	for rows.Next() {
		var anchorID int
		var stateJSON, changesJSON, refsJSON []byte
		dest := []any{&anchorID, &changesJSON}
		if withState {
			dest = append(dest, &stateJSON, &refsJSON)
		}
		if err := rows.Scan(dest...); err != nil {
			return errors.Trace(err)
		}
		*readBytes += len(stateJSON) + len(changesJSON)
		refs, err := staterefs.Parse(refsJSON)
		if err != nil {
			return errors.Trace(err)
		}
		anchor := staterefs.Anchor{Refs: refs}
		// An unreadable column must say so: left undecoded, every field in it reads as "not found".
		// It is a pure function of stored data, so it is permanent: a persisted write must not retry it, and a
		// path that terminalizes a flow whatever its state holds must not be stopped by it.
		if err := decodeAnchorColumn(stateJSON, &anchor.State); err != nil {
			return errors.Trace(&permanentError{errors.New("anchor step %d: unreadable state: %v", anchorID, err)})
		}
		if err := decodeAnchorColumn(changesJSON, &anchor.Changes); err != nil {
			return errors.Trace(&permanentError{errors.New("anchor step %d: unreadable changes: %v", anchorID, err)})
		}
		fetched[anchorID] = anchor
	}
	return errors.Trace(rows.Err())
}

// resolveStateRefs materializes ref'd fields into state, in place. want, when non-nil, selects which refs to
// resolve; the rest are left for the caller to carry forward.
//
// It returns the bytes of field value it spliced into state, which is what a caller about to HOLD that state
// meters its residency with - distinct from the read bytes reported to dwarf_state_read_bytes, which are the
// raw columns scanned (whole anchor rows, and zero on a cache hit). Only the dispatch path needs it; every
// other caller resolves inside a transaction and discards it.
//
// An introspection read (Snapshot, Step) passes an empty workflowURL and is not counted: the metric tracks the
// execution path's byte throughput, and introspection reads are documented as outside it.
func (e *Engine) resolveStateRefs(ctx context.Context, db sequel.Executor, shardNum int, state workflow.State, refs staterefs.Refs, want map[string]bool, workflowURL string) (int, error) {
	var readBytes int
	materialized, err := e.linkerFor(shardNum).Resolve(ctx, state, refs, want, anchorLoader(db, &readBytes))
	if readBytes > 0 && workflowURL != "" {
		e.metricStateReadBytes(ctx, workflowURL, "state_ref", readBytes)
	}
	return materialized, errors.Trace(err)
}

// resolveDispatchRefs is resolveStateRefs for a step about to be dispatched, which holds no transaction: a
// field a concurrent dispatch on this shard is already reading is taken from that read rather than read
// again (staterefs.ResolveShared). A fan-out's branches dispatch together and miss on the same anchor at once.
//
// The caller holds no turn; the loader takes one around its query and scan, the only part that holds a
// connection. A cache hit or a wait on another dispatch's read never reaches the loader, so neither queues at
// the turnstile or parks a turn. Do not take the turn around the whole resolve: a cohort's branches dispatch
// together, and with more of them than the shard has turns, every turn sits idle behind one anchor read.
func (e *Engine) resolveDispatchRefs(ctx context.Context, db sequel.Executor, shardNum int, state workflow.State, refs staterefs.Refs, workflowURL string) (int, error) {
	var readBytes int
	load := anchorLoader(db, &readBytes)
	materialized, err := e.linkerFor(shardNum).ResolveShared(ctx, state, refs, func(ctx context.Context, reads []staterefs.Read) (map[int]staterefs.Anchor, error) {
		ctx, doneTurn := e.dbTurn(ctx, shardNum)
		defer doneTurn()
		return load(ctx, reads)
	})
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
	refs, err := staterefs.Parse(refsJSON)
	if err != nil {
		return nil, errors.Trace(err)
	}
	var readBytes int
	resolved, err := e.linkerFor(shardNum).Flatten(ctx, stateJSON, refs, anchorLoader(db, &readBytes))
	if readBytes > 0 {
		e.metricStateReadBytes(ctx, workflowURL, "state_ref", readBytes)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return resolved, nil
}
