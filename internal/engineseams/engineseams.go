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

// Package engineseams is the engine's test and benchmark surface for code in this module outside package
// engine: the names of the engine's fault-injection, checkpoint and recorded-variable seams, Join for their
// targeted forms, DB and Seams, which reach a running engine's shard set and seam registry, ListLive and
// ArchiveNow, which list the live tables in every status and archive what just closed, SetRefillInterval, the
// benchmark override of the refill cycle period, and ShardBudget, the pool the engine derives for a shard.
//
// The engine fires these seams from its own production code and consults one seamster.Seamster per engine,
// enabled only under testing.Testing(); the mechanism (arming, scoping, Wait/Break/Resume) is the seamster
// package's. A test arms a name from here on the registry Seams returns:
//
//	engineseams.Seams(eng).Inject(engineseams.Join(engineseams.FaultExecuteTask, "Charge"))
//
// The accessors take the engine as an any because this package cannot import package engine, which imports
// it; they panic when given anything but an *engine.Engine, and are nil until package engine is linked in.
package engineseams

import (
	"context"
	"strings"
	"time"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/dwarf/internal/peers"
	"github.com/microbus-io/dwarf/internal/piston"
	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/seamster"
)

// DB returns the engine's shard set, for a test to inspect flow and step rows directly.
var DB func(eng any) *database.ShardSet

// Seams returns the engine's seam registry, for a test to arm faults and checkpoints and wait on them.
var Seams func(eng any) *seamster.Seamster

// ListLive lists the flows in the engine's live tables in every status, open and closed - what ListOpen lists plus the
// closed flows not archived yet - with ListOpen's rules otherwise, for a test that must see a flow it just closed.
var ListLive func(eng any, ctx context.Context, query workflow.Query) ([]workflow.FlowSummary, error)

// ArchiveNow runs one reaper pass that archives every closed root, however recently it closed, and removes the roots
// scheduled for deletion whose time has come (not a DeleteOnCompletion root inside its grace), for a test that lists
// or purges what it just closed.
var ArchiveNow func(eng any, ctx context.Context)

// SetRefillInterval pins every shard's refill cycle period to d on a running or configured engine, for a
// benchmark sweep to hold the period at fixed values, or a fixture whose backlog makes the derived period too
// short under -race; d <= 0 restores the derived period.
var SetRefillInterval func(eng any, d time.Duration) error

// ShardBudget is the connection budget the engine derives for one PostgreSQL shard with no MaxConnections
// declared, at rttMs of round-trip time against a server whose max_connections is serverMax (0 = unknown) -
// the pool a lone replica opens. A benchmark computes it ahead of a run to pin exactly the pool that ships,
// since the engine's own Startup probe varies run to run.
var ShardBudget func(rttMs float64, serverMax int) int

// Join builds the targeted form of a seam name: the base name, then the entity it targets, joined with ":".
// A consult site and the test that arms it both call it, so neither can spell the join the other does not -
// a site consulting Join(FaultExecuteTask, taskName) is armed by Inject(Join(FaultExecuteTask, "Charge")). A
// targeted name and the bare one are DIFFERENT seams, so a site wanting both fires both.
//
// It assembles a string on every call, which a production hot path pays whether or not the seams are live;
// such sites guard it behind Enabled() on the registry.
func Join(parts ...string) string {
	return strings.Join(parts, ":")
}

// --- Fault injection ---
const (
	// Scoped by task name (the task's name in the graph, not the URL ExecuteTask receives):
	FaultExecuteTask      = "executeTask"      // ExecuteTask returns a synthetic error (as if the task failed)
	FaultPanicExecuteTask = "panicExecuteTask" // ExecuteTask panics (exercises host-call panic isolation)

	// Scoped by workflow URL:
	FaultLoadGraph = "loadGraph" // LoadGraph returns a synthetic error

	// Scoped by task name; force the persistence/transition steps of a dispatch to fail:
	FaultTransitionCommit     = "transitionCommit"     // the post-completion transition transaction errors
	FaultCompleteFlowCommit   = "completeFlowCommit"   // the flow-completion transaction errors
	FaultContention           = "contention"           // a dispatch transaction returns a lock-contention error
	FaultLeaseStaleWrite      = "leaseStaleWrite"      // the completion write carries a stale lease_seq (zombie)
	FaultPersistErr           = "persistErr"           // the step-completion write returns a non-contention database error (consumed per attempt, so InjectN sets how many attempts fail)
	FaultPersistLandedErr     = "persistLandedErr"     // the step-completion write lands, then returns a non-contention database error, as when a commit's acknowledgement is lost (consumed per attempt)
	FaultExtendLeaseErr       = "extendLeaseErr"       // persist's lease renewal returns a database error, as during an outage (consumed per attempt)
	FaultCompletionContention = "completionContention" // the step-completion write returns a lock-contention error (consumed per attempt, so InjectN sets how many attempts fail)
	FaultSubgraphSpawnErr     = "subgraphSpawnErr"     // the subgraph spawn errors before its transaction, with the caller step not yet parked
	FaultRetryRewindErr       = "retryRewindErr"       // the flow.Retry rewind returns a non-contention database error (consumed per attempt)
	FaultRerouteWriteErr      = "rerouteWriteErr"      // the Cancel-covered reroute's follow-up write returns a non-contention database error (consumed per attempt)
	FaultRerouteStaleWrite    = "rerouteStaleWrite"    // the Cancel-covered reroute's follow-up write carries a stale lease_seq (zombie)

	// Scoped by workflow URL of the subgraph child:
	FaultSubgraphSpawnCommitLost = "subgraphSpawnCommitLost" // the subgraph spawn commits but reports an error, as if its acknowledgement were lost
	FaultSubgraphReviveLost      = "subgraphReviveLost"      // the subgraph-completion path skips reviving the parked caller

	// Process-wide, consumed per attempt (InjectN sets how many attempts fail):
	FaultCompleteSurgraphErr = "completeSurgraphErr" // the subgraph-completion write returns a non-contention database error
	FaultCancelReasonReadErr = "cancelReasonReadErr" // a Cancel-preempted step's re-read of the cancel reason returns a non-contention database error
	FaultCancelMarkErr       = "cancelMarkErr"       // a chunk of Cancel's step mark errors before it runs, after the watermark committed

	// Process-wide (no scope):
	FaultInterruptStaleWrite = "interruptStaleWrite" // the interrupt path's in-transaction lease-generation read is forced to mismatch (zombie)
	FaultInterruptChainWrite = "interruptChainWrite" // the interrupt path's combined chain update fails and applies nothing (deadlock victim)
	FaultDropSignalStop      = "dropSignalStop"      // the flow-stop signal delivers nothing (lost terminal wake)
	FaultDropDoorbell        = "dropDoorbell"        // the local work doorbell is dropped (the step waits for a refiller scan)
	FaultSkipRefSeed         = "skipRefSeed"         // a transition does not seed the ref cache, so its successors resolve their refs from the database
	FaultRecoveryResetErr    = "recoveryResetErr"    // the post-execution recovery reset write errors
	FaultReapMidTree         = "reapMidTree"         // the reaper errors after deleting steps, before flows
	FaultReapSelectErr       = "reapSelectErr"       // the reaper's due-root SELECT errors
	FaultReapContention      = "reapContention"      // the reaper's archive move fails on lock contention (a deadlock victim)
	FaultReapArchiveErr      = "reapArchiveErr"      // the reaper's archive move fails with an error that is not lock contention
	FaultArchiveMaintainErr  = "archiveMaintainErr"  // an archive maintenance pass fails after its work
	FaultExpireTerminate     = "expireTerminate"     // the expiration pass's termination of a root fails, rolled back
	FaultRefillScanErr       = piston.FaultScanErr   // the piston's priority-band scan errors (its name, so there is one catalogue)
	FaultPeerReadErr         = peers.FaultReadErr    // a shard's peer-registry reading fails: this replica goes BLIND on that shard (their names, so there is one catalogue)
	FaultPeerBeatErr         = peers.FaultBeatErr    // a shard's peer-registry beat writes nothing: this replica stops proving its liveness THERE, while running on
	FaultSlowPoolPush        = "slowPoolPush"        // recomputePools reaches CheckpointPoolPushHeld between reading the roles and pushing the derived sizes
	FaultDeliverFailureErr   = "deliverFailureErr"   // deliverFlowFailureToParent drops the parked-caller re-dispatch (lost delivery); unscoped, or scoped by the parked caller's task name for per-level control
	FaultTerminateCommit     = "terminateCommit"     // the Terminate transaction errors
	FaultResumeCommit        = "resumeCommit"        // the Resume transaction errors
	FaultForkCommit          = "forkCommit"          // the Fork clone transaction errors
	FaultInitRuntime         = "initRuntime"         // Startup's runtime initialization fails, after the shards are open and the fleet joined
	FaultFailFlowFinalState  = "failFlowFinalState"  // failing a flow cannot compute its final state, so it terminalizes with an empty one
	FaultFailStepErr         = "failStepErr"         // scoped by task name: failStep errors before it writes anything, as if the database were unreachable
)

// --- Execution checkpoints ---
const (
	CheckpointResumeBeforeFlowWrite     = "resumeBeforeFlowWrite"     // Resume, just before its transaction's flow-status gate write
	CheckpointBeforeDeleteStamp         = "beforeDeleteStamp"         // Delete, after it read the root, just before the delete stamp
	CheckpointArchivePassDone           = "archivePassDone"           // the archive loop, after each pass over the shards
	CheckpointBeforeTransitionTx        = "beforeTransitionTx"        // step execution, after the step is marked completed, before whichever transaction transitions it
	CheckpointCancelBeforeMark          = "cancelBeforeMark"          // Cancel, after its watermark transaction commits, before it marks the steps
	CheckpointForkBeforeClone           = "forkBeforeClone"           // Fork, before its first clone transaction
	CheckpointForkRootLocked            = "forkRootLocked"            // Fork, in a clone transaction, after it locked the source root and before it reads the source
	CheckpointBeforeSubgraphSpawn       = "beforeSubgraphSpawn"       // step execution, before the transaction that parks a subgraph caller and creates its child
	CheckpointTerminateAfterScan        = "terminateAfterScan"        // Terminate, after its pre-transaction scan of the tree, before its transaction
	CheckpointTerminateAfterStepsLocked = "terminateAfterStepsLocked" // Terminate, in its transaction, after its first step UPDATE and before it locks the flow rows
	CheckpointTerminateAfterRescan      = "terminateAfterRescan"      // Terminate, in its transaction, after its scan for children spawned since the first scan
	CheckpointSubgraphSpawnParked       = "subgraphSpawnParked"       // step execution, inside the subgraph spawn's transaction, after the caller is parked and before the child is inserted
	CheckpointSubgraphSpawnAbandoned    = "subgraphSpawnAbandoned"    // step execution, a subgraph spawn rolled back because its caller step is no longer running
	CheckpointBeforeRetryRewind         = "beforeRetryRewind"         // step execution, before the flow.Retry rewind transaction
	CheckpointBeforeCompleteFlowWrite   = "beforeCompleteFlowWrite"   // flow completion, just before its transaction's status-gate write
	CheckpointBeforeReviveWrite         = "beforeReviveWrite"         // the subgraph-completion path, just before its transaction's caller-revive write
	CheckpointBeforeRecoveryReset       = "beforeRecoveryReset"       // the post-execution recovery path, just before its fenced step-reset transaction
	CheckpointClaimedDispatchDone       = "claimedDispatchDone"       // step execution returned after winning its claim, recovery reset included; fired bare and targeted by (shard, step id)
	CheckpointLeaseRecoverySelected     = "leaseRecoverySelected"     // lease recovery, after it selected expired steps and before its by-id reset; reached only when it found some
	CheckpointPoolPushHeld              = "poolPushHeld"              // recomputePools, inside poolsLock between reading the roles and pushing the sizes; reached only by the recompute FaultSlowPoolPush picks
	CheckpointWedgeSweepSelected        = "wedgeSweepSelected"        // the wedged-park sweep, after it selected parked callers with no open child and before it reads each one's latest child; reached only when it found some
	CheckpointFingerprintFlowRead       = "fingerprintFlowRead"       // Fingerprint, on each side it reads, after the flow row and before it counts the steps
	CheckpointHistoryFlowRead           = "historyFlowRead"           // History, on each side it reads, after the flow row and before the tree
	CheckpointStepRead                  = "stepRead"                  // Step, on each side it reads, after the step row and before its refs and navigation

	// A COUNTING checkpoint (read with Visits, never a rendezvous), scoped by flow id so concurrent flows
	// count independently. No test arms Wait/Break on it, so Checkpoint just increments and returns; both the
	// count site and the read site consult e.seams directly.
	//
	// It exists for a single pin, and that pin guards a performance property no correctness test can see: a
	// NON-FINAL cohort arrival must issue ZERO flow-row statements. Grabbing the flow row for every arrival
	// serializes an entire cohort on one row (measured at fan-out width 64: 20 of 43 active backends queued on
	// that one statement), so the grab is deferred to the arrival that actually resolves the cohort. Nothing
	// about the flow's OUTCOME changes if someone reintroduces a per-arrival flow-row write - the fan-in still
	// fires and every existing fixture still passes - it just costs ~20% throughput silently. Hence a counter
	// rather than a state assertion.
	//
	// Counts are per flow and cumulative across the flow's whole life (Visits never resets), so a test reads
	// the delta it cares about. A Transact contention retry re-runs the closure and legitimately re-counts, so
	// a test asserting an exact count must be single-worker and contention-free.
	CheckpointFlowRowWrite = "flowRowWrite" // a dwarf_flows lock grab issued by the transition transaction

	// Lifecycle rendezvous (fired at an event, used with Wait - not a freeze site): lets a test wait for
	// exact engine progress instead of polling status / sleeping. signalStop fires it BOTH unscoped and scoped
	// by (flowKey, status): the unscoped name catches whichever flow stops first, which is all a single-flow
	// test needs, while the scoped one lets a test wait for ONE flow to reach ONE status while other flows (a
	// subgraph child, a fan-out sibling, a peer replica's work) stop concurrently. Both are needed because a
	// scoped fire does not wake an unscoped waiter.
	//
	// The scoped form is meaningful only because signalStop runs POST-COMMIT: when it fires, the status is
	// durable, so a test reading the row immediately after sees it - the exact guarantee a status poll spun for.
	CheckpointFlowStopped = "flowStopped" // signalStop(), a flow just reached a stop (completed/failed/terminated/cancelled/interrupted)

	// Lifecycle rendezvous, fired BOTH unscoped and scoped by flow key when an Await has registered on the
	// latch board and is about to block. It is the mirror of CheckpointFlowStopped: that one says a flow
	// settled, this one says a caller was already waiting when it did.
	//
	// Registration order is not a correctness question - the board is POLLED, so a key that settles before
	// its caller arrives is reported by the next sweep, and `await` reads once before parking anyway. It is
	// a question about what a TEST exercised. Every test about a blocked caller being woken has to get its
	// Await onto the board before the thing it is waiting for happens, and the only alternative to this is
	// a sleep long enough to make that likely - which on a slow machine silently becomes its opposite: the
	// Await registers after the stop, its own first read answers it, every assertion still passes, and the
	// wake path under test never ran. A rendezvous makes the premise exact instead of probable.
	//
	// Fired through latch.Board's SetOnPark hook rather than from `await`, because registering and blocking
	// are deliberately one call there (the gap is the race Close is careful not to leave). Wired only when
	// the seams are enabled, so a production board carries no hook at all.
	CheckpointAwaitParked = "awaitParked" // Board.Latch(), an Await is on the board and about to block

	// Fired scoped by flow key when a parked Await has been released and is about to read the flow. A Break
	// here holds the caller between the release and the read, so a test can expire the caller's ctx inside
	// that window.
	CheckpointAwaitReleased = "awaitReleased" // await(), released from the board, before the read that builds the outcome

	// Lifecycle rendezvous, fired at the END of a recovery pass - every detector in it has read the database
	// by then. recoveryLoop sweeps ON ENTRY (Startup), so that first pass runs CONCURRENTLY with the test body,
	// and a test that forges a wedge/orphan shape and then drives one detector itself is racing it: the sweep
	// sees the forged shape too and counts/logs it a second time. The pass is not instant - it is four scans on
	// one goroutine sharing the engine's connection pool, so against a loaded server it lands seconds after
	// Startup (measured on SQL Server: the sweep's own recoverOrphanedFlows landed 27ms before a forge that
	// followed a Create+Await). Wait for this before forging; the wait is free once the sweep is behind (Visits).
	CheckpointRecoverySweepDone = "recoverySweepDone" // runRecoverySweep(), one full recovery pass is over

	FaultRefillFetchErr = piston.FaultFetchErr // make a shard's FetchSteps fail without touching the database

	// CheckpointRefillCycleDone is fired per shard (scoped by the shard number, and once unscoped) when that
	// shard's piston completes a cycle that PUSHED - i.e. its cache partition now reflects the plan. Its name
	// is the piston's, so there is one catalogue, exactly like FaultRefillScanErr.
	//
	// What it exists for: `Offer` admits a step into an EMPTY partition unconditionally, while `Cache.Pop` ranks
	// partitions by the bands they hold and never consults the current plan - so a doorbell-admitted hint is
	// indistinguishable from a plan-selected one until the owning shard's next cycle reconciles it. Every shard reconciles on its own
	// cadence, so a test that needs the fleet's partitions to agree with the plan before it asserts dispatch
	// ORDER must wait for a cycle per shard; no amount of elapsed time substitutes, because one starved
	// piston is exactly the case that breaks it.
	CheckpointRefillCycleDone = piston.CheckpointCycleDone // a shard's piston reconciled its cache partition
	CheckpointRefillTallyDone = piston.CheckpointTallyDone // a shard's piston published a tally from a real scan
	CheckpointRefillStole     = piston.CheckpointStole     // a shard's piston selected steps from OUTSIDE its residue class

	// CheckpointPeerPass is fired scoped by shard (Join(CheckpointPeerPass, shard)) as that shard's Sonar starts
	// a registry pass - its beat and its read - from the turn the engine hands it, so only once the runtime is
	// up. Passes run one at a time, so a visit also says the previous pass is over: two visits counted from a
	// moment mean a whole read ran after it. That is how a test waits for a reading that was attempted, where
	// the Sonar's own BlindFor grows with the clock whether or not anything read.
	CheckpointPeerPass = "peerPass"
)

// --- Recorded variables ---

// VariablePoolIdle is the idle-connection size the engine DERIVED for a shard, targeted by shard index
// (Join(VariablePoolIdle, strconv.Itoa(idx))). It is recorded wherever a derived size is pushed, so a
// test can read the half of the derived pool size that has no other witness: database/sql reports the
// configured max OPEN size through DBStats.MaxOpenConnections, but nothing reports the configured max idle -
// DBStats.Idle is the number of connections currently idle, which is traffic, not configuration. The value
// recorded is the engine's derivation, before the test-mode connection cap clamps what the pool actually
// takes.
const VariablePoolIdle = "poolIdle"

// VariableLockOrder is the flow-id list a tree operation's chunked lock passes bind, targeted by the operation
// (Join(VariableLockOrder, "cancel") or Join(VariableLockOrder, "terminate")), recorded as a copy on each run of
// the passes. The order the statements took their rows in is in no row, so this is its only witness.
const VariableLockOrder = "lockOrder"
