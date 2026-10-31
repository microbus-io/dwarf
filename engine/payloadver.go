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
	"log/slog"
	"net/http"
	"time"

	"github.com/microbus-io/dwarf/internal/keys"
	"github.com/microbus-io/dwarf/internal/turnstile"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
)

// payloadVer is the newest payload format this build reads and the one it creates flows in. A flow's format
// (dwarf_flows.payload_ver) is fixed when the flow is created and governs every payload column of every step row
// of the flow (state, changes, state_refs, state_reduced): whoever writes into the flow writes in its format.
const payloadVer = 1

// The deferral schedule for a step of a flow in a format newer than this build reads: a backoff doubling from
// payloadDeferralFirst to payloadDeferralCap, failing the step once it has been deferred maxPayloadDeferrals times.
// 63s to reach the cap, then one a minute: at least an hour, since claim latency only adds to it.
const (
	payloadDeferralFirst = time.Second
	payloadDeferralCap   = time.Minute
	maxPayloadDeferrals  = 65
)

// payloadDeferralBackoff is how long a step waits after its deferrals-th deferral (0-based).
func payloadDeferralBackoff(deferrals int) time.Duration {
	// Doubled step by step rather than shifted by deferrals, which overflows a Duration long before the bound.
	backoff := payloadDeferralFirst
	for range deferrals {
		if backoff >= payloadDeferralCap {
			break
		}
		backoff *= 2
	}
	return min(backoff, payloadDeferralCap)
}

// errPayloadNewer is the error for a flow in a payload format this build cannot read.
func errPayloadNewer(ver int) error {
	return errors.New("flow payload is in format %d, and this replica reads formats up to %d; upgrade every replica", ver, payloadVer, http.StatusNotImplemented)
}

// checkPayloadVer refuses a flow whose payload format is newer than this build reads, where its step rows are read
// other than by the dispatch of one of its steps (which defers instead, deferNewerPayload): Snapshot, Step, Fork and
// the final-state merge. It is permanent: the flow reads the same on every attempt here, and only a replica of a
// newer build can read it. The status is restated because a permanentError is not a TracedError, so the wrap would
// not carry it over.
func checkPayloadVer(ver int) error {
	if ver > payloadVer {
		return errors.Trace(&permanentError{errPayloadNewer(ver)}, http.StatusNotImplemented)
	}
	return nil
}

// deferNewerPayload hands a claimed step of a flow in a payload format newer than this build reads back to pending,
// unrun, backed off, for a replica of a newer build to claim. Past the bound it fails the step, naming the cause.
//
// Nothing routes the row to a newer replica: it keeps its step_id, so it stays in this replica's residue class and
// comes back here until the fleet changes under it. The bound is therefore sized to a rolling upgrade, not to a
// number of re-claims. The deferrals are counted apart from attempt, which a task reads through flow.Attempt, and
// over the step's whole life: a step that runs between two rollouts carries its earlier deferrals into the second.
func (e *Engine) deferNewerPayload(ctx context.Context, d *dispatch) error {
	db, shardNum, stepID, leaseSeq, flowID, taskName := d.db, d.shard, d.stepID, d.row.leaseSeq, d.row.flowID, d.row.taskName
	ver, deferrals := d.row.flowPayloadVer, d.row.payloadDeferrals
	if deferrals >= maxPayloadDeferrals {
		return e.failEntry(ctx, d, errPayloadNewer(ver))
	}
	backoff := payloadDeferralBackoff(deferrals)
	// Warned once per step, at its first deferral: a rollout over a large backlog defers every step up to the bound,
	// and a line per deferral would be that many lines per step.
	level := slog.LevelDebug
	if deferrals == 0 {
		level = slog.LevelWarn
	}
	e.logger.Log(ctx, level, "Deferring a step of a flow in a newer payload format",
		"flow", keys.CorrelationID(shardNum, flowID), "task", taskName,
		"payloadVer", ver, "supportedVer", payloadVer, "deferrals", deferrals+1, "backoff", backoff)
	pass := turnstile.WaitTurn(ctx)
	// Fenced and guarded like the retry rewind: a zero-row match means a peer holds the lease or a Terminate
	// terminalized the step, and either way the row is no longer this dispatch's to write.
	_, err := db.ExecContext(ctx,
		"UPDATE dwarf_steps SET status=?, payload_deferrals=payload_deferrals+1, not_before=DATE_ADD_MILLIS(NOW_UTC(), ?), lease_expires=NOW_UTC(), updated_at=NOW_UTC() WHERE step_id=? AND status='"+workflow.StatusRunning+"' AND lease_seq=?",
		workflow.StatusPending, backoff.Milliseconds(), stepID, leaseSeq,
	)
	pass.Return()
	if err != nil {
		return errors.Trace(err)
	}
	e.claims.RelinquishClaim(shardNum, stepID)
	return nil
}
