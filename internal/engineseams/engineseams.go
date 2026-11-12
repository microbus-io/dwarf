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
// targeted forms, DB and Seams, which reach a running engine's shard set and seam registry, ListLive,
// ArchiveNow and ArchiveDays, which list the live tables in every status, archive what just closed and read the
// archive's days, FlowsClosed, the count of flows an engine closed, SetRefillInterval, the benchmark override of the refill cycle period, and ShardBudget, the pool
// the engine derives for a shard.
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
	"github.com/microbus-io/dwarf/internal/reaper"
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

// ArchiveDays reads the archive days whose tables exist on a shard, ascending, for a benchmark that counts or sizes
// the archive.
var ArchiveDays func(eng any, ctx context.Context, shard int) ([]int, error)

// FlowsClosed counts the flows this engine has closed - completed, failed, terminated or cancelled - with an atomic
// read that needs no meter, for a benchmark to sum over the engines it runs in process. A flow is closed by whichever
// replica dispatches its last step, so one engine's count means nothing alone.
var FlowsClosed func(eng any) int64

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

// Every name below is a seam a test addresses through Seams, and its comment says how. "By X" marks a targeted
// name, Join(name, X), which arming or waiting on the bare name does not reach; "bare and by X" marks a site that
// consults or fires both, as separate seams; a comment that says neither is a bare name. "Task" is the task's name
// in the graph, not the URL ExecuteTask receives.
//
// A name owned by a package below the engine (internal/reaper, internal/piston, internal/peers) is aliased from
// that package, so there is one catalogue and a test arms the very string the package consults.

// --- Fault injection ---
//
// Each arming is consumed by one consult. "Per attempt" marks a site inside a retry loop, where every attempt
// consults afresh, so InjectN sets how many attempts fail.
const (
	// Startup and connection pools:
	FaultInitRuntime  = "initRuntime"  // Startup's runtime initialization fails, after the shards are open and the fleet joined
	FaultSlowPoolPush = "slowPoolPush" // recomputePools reaches CheckpointPoolPushHeld between reading the roles and pushing the derived sizes

	// Host calls:
	FaultLoadGraph        = "loadGraph"        // by workflow URL: LoadGraph returns a synthetic error
	FaultExecuteTask      = "executeTask"      // by task: ExecuteTask returns a synthetic error, as if the task failed
	FaultPanicExecuteTask = "panicExecuteTask" // by task: ExecuteTask panics, exercising host-call panic isolation

	// Dispatch:
	FaultDropDoorbell        = "dropDoorbell"        // the local work doorbell is dropped, so the step waits for a refiller scan
	FaultClaimReadContention = "claimReadContention" // per attempt: the MySQL/MariaDB post-claim read returns lock contention before scanning a row
	FaultCancelReasonReadErr = "cancelReasonReadErr" // per attempt: a Cancel-preempted step's re-read of the cancel reason returns a non-contention database error
	FaultExtendLeaseErr      = "extendLeaseErr"      // per attempt: persist's lease renewal returns a database error, as during an outage
	FaultRecoveryResetErr    = "recoveryResetErr"    // the post-execution recovery reset write errors

	// The step-completion write:
	FaultPersistErr           = "persistErr"           // by task, per attempt: the write returns a non-contention database error
	FaultPersistConnErr       = "persistConnErr"       // by task, per attempt: the write fails on a lost connection, an error sequel.IsConnectionError recognizes
	FaultPersistLandedErr     = "persistLandedErr"     // by task, per attempt: the write lands, then returns a non-contention database error, as when a commit's acknowledgement is lost
	FaultCompletionContention = "completionContention" // by task, per attempt: the write returns a lock-contention error
	FaultLeaseStaleWrite      = "leaseStaleWrite"      // by task: the write carries a stale lease_seq (zombie)
	FaultRerouteWriteErr      = "rerouteWriteErr"      // by task, per attempt: the Cancel-covered reroute's follow-up write returns a non-contention database error
	FaultRerouteStaleWrite    = "rerouteStaleWrite"    // by task: the Cancel-covered reroute's follow-up write carries a stale lease_seq (zombie)

	// Transitions:
	FaultContention       = "contention"       // by task: the transition transaction returns a lock-contention error
	FaultTransitionCommit = "transitionCommit" // by task: the post-completion transition transaction errors
	FaultSkipRefSeed      = "skipRefSeed"      // a transition does not seed the ref cache, so its successors resolve their refs from the database
	FaultRetryRewindErr   = "retryRewindErr"   // by task, per attempt: the flow.Retry rewind returns a non-contention database error

	// Interrupts:
	FaultInterruptStaleWrite = "interruptStaleWrite" // the interrupt path's in-transaction lease-generation read is forced to mismatch (zombie)
	FaultInterruptChainWrite = "interruptChainWrite" // the interrupt path's combined chain update fails and applies nothing (deadlock victim)

	// Failing and completing a flow:
	FaultFailStepErr        = "failStepErr"        // by task: failStep errors before it writes anything, as if the database were unreachable
	FaultFailFlowFinalState = "failFlowFinalState" // failing a flow cannot compute its final state, so it terminalizes with an empty one
	FaultCompleteFlowCommit = "completeFlowCommit" // the flow-completion transaction errors
	FaultDropSignalStop     = "dropSignalStop"     // the flow-stop signal delivers nothing (lost terminal wake)

	// Subgraphs:
	FaultSubgraphSpawnErr        = "subgraphSpawnErr"        // by the child's workflow URL: the spawn errors before its transaction, with the caller step not yet parked
	FaultSubgraphSpawnCommitLost = "subgraphSpawnCommitLost" // by the child's workflow URL: the spawn commits but reports an error, as if its acknowledgement were lost
	FaultSpawnResolveContention  = "spawnResolveContention"  // by task, per attempt: the write delivering a failed spawn to its caller returns a lock-contention error
	FaultCompleteSurgraphErr     = "completeSurgraphErr"     // per attempt: the subgraph-completion write returns a non-contention database error
	FaultSubgraphReviveLost      = "subgraphReviveLost"      // the subgraph-completion path skips reviving the parked caller
	FaultDeliverFailureErr       = "deliverFailureErr"       // bare and by the parked caller's task, for per-level control: deliverFlowFailureToParent drops the caller's re-dispatch (lost delivery)

	// Cancel, Terminate, Resume and Fork:
	FaultCancelMarkErr   = "cancelMarkErr"   // per attempt: a chunk of Cancel's step mark errors before it runs, after the watermark committed
	FaultTerminateCommit = "terminateCommit" // the Terminate transaction errors
	FaultExpireTerminate = "expireTerminate" // the expiration pass's termination of a root fails, rolled back
	FaultResumeCommit    = "resumeCommit"    // the Resume transaction errors
	FaultForkCommit      = "forkCommit"      // the Fork clone transaction errors

	// The refiller:
	FaultRefillScanErr  = piston.FaultScanErr  // a shard's piston's priority-band scan errors
	FaultRefillFetchErr = piston.FaultFetchErr // a shard's piston's FetchSteps fails without touching the database

	// The peer registry:
	FaultPeerReadErr = peers.FaultReadErr // bare and by shard: a shard's registry reading fails, so this replica goes BLIND on that shard
	FaultPeerBeatErr = peers.FaultBeatErr // bare and by shard: a shard's registry beat writes nothing, so this replica stops proving its liveness THERE, while running on

	// Reaping and archiving:
	FaultReapSelectErr      = reaper.FaultSelectErr     // the reaper's due-root SELECT errors
	FaultReapSizeErr        = reaper.FaultSizeErr       // by root flow_id: sizing a selected root fails
	FaultReapMidTree        = reaper.FaultMidTree       // bare and by root flow_id: a move or delete fails after its steps, before its flows
	FaultReapContention     = reaper.FaultContention    // the archive move fails on lock contention (a deadlock victim)
	FaultReapArchiveErr     = reaper.FaultArchiveErr    // an archive move or staging chunk fails with an error that is not lock contention
	FaultReapStageChunk     = reaper.FaultStageChunk    // by root flow_id: a staging chunk fails under its root's lock, before it writes
	FaultReapStageLost      = reaper.FaultStageLost     // by root flow_id: a staging chunk commits, then reports an error, as a lost response
	FaultArchiveDelete      = reaper.FaultArchiveDelete // by root flow_id: an archived tree's deletion fails before it commits
	FaultArchiveMaintainErr = "archiveMaintainErr"      // an archive maintenance pass fails after its work
)

// --- Execution checkpoints ---
const (
	// Flow stops and Await:

	// CheckpointFlowStopped is fired bare and by (flow key, status) when signalStop reports a stop - completed,
	// failed, terminated, cancelled or interrupted. It is a rendezvous, waited on rather than frozen, that lets a
	// test wait for exact engine progress instead of polling status or sleeping. The bare name catches whichever
	// flow stops first, which is all a single-flow test needs; the targeted one waits for ONE flow to reach ONE
	// status while others - a subgraph child, a fan-out sibling, a peer replica's work - stop concurrently.
	//
	// The targeted form is meaningful only because signalStop runs POST-COMMIT: when it fires, the status is
	// durable, so a test reading the row immediately after sees it - the exact guarantee a status poll spun for.
	CheckpointFlowStopped = "flowStopped"

	// CheckpointAwaitParked is fired bare and by flow key when an Await has registered on the latch board and is
	// about to block. It is the mirror of CheckpointFlowStopped: that one says a flow settled, this one says a
	// caller was already waiting when it did.
	//
	// Registration order is not a correctness question - the board is POLLED, so a key that settles before its
	// caller arrives is reported by the next sweep, and await reads once before parking anyway. It is a question
	// about what a TEST exercised. A test of a blocked caller being woken must get its Await onto the board before
	// the thing it waits for happens, and the only alternative to this rendezvous is a sleep long enough to make
	// that likely - which on a slow machine silently becomes its opposite: the Await registers after the stop, its
	// own first read answers it, every assertion still passes, and the wake path under test never ran.
	//
	// It is fired through latch.Board's SetOnPark hook rather than from await, because registering and blocking
	// are deliberately one call there (the gap is the race Close is careful not to leave). The hook is wired only
	// when the seams are enabled, so a production board carries none.
	CheckpointAwaitParked = "awaitParked"

	// CheckpointAwaitReleased is fired by flow key when a parked Await has been released from the board and is
	// about to read the flow. A Break here holds the caller between the release and the read, so a test can
	// expire the caller's ctx inside that window.
	CheckpointAwaitReleased = "awaitReleased"

	// Step execution:
	CheckpointClaimBeforeRead         = "claimBeforeRead"         // by (shard, step id, the generation it claimed): the MySQL/MariaDB claim, after its CAS committed and before it reads the row back
	CheckpointBeforeCancelRoute       = "beforeCancelRoute"       // after the completion write of a step Cancel marked while it ran, before it is routed to its handler or failed as cancelled
	CheckpointBeforeTransitionTx      = "beforeTransitionTx"      // after the step is marked completed, before whichever transaction transitions it
	CheckpointBeforeRetryRewind       = "beforeRetryRewind"       // before the flow.Retry rewind transaction
	CheckpointBeforeCompleteFlowWrite = "beforeCompleteFlowWrite" // flow completion, just before its transaction's status-gate write
	CheckpointBeforeRecoveryReset     = "beforeRecoveryReset"     // the post-execution recovery path, just before its fenced step-reset transaction
	CheckpointClaimedDispatchDone     = "claimedDispatchDone"     // bare and by (shard, step id): step execution returned after winning its claim, recovery reset included

	// CheckpointFlowRowWrite counts, by flow id, the dwarf_flows lock grabs the transition transaction issues.
	// It is read with Visits and never armed with Wait or Break, so a visit just increments and returns.
	//
	// It pins a performance property no correctness test can see: a NON-FINAL cohort arrival issues ZERO
	// flow-row statements. Grabbing the flow row on every arrival serializes a whole cohort on one row (measured
	// at fan-out width 64: 20 of 43 active backends queued on that one statement), so the grab is deferred to the
	// arrival that resolves the cohort. A reintroduced per-arrival grab changes no OUTCOME - the fan-in still
	// fires and every fixture still passes - it silently costs ~20% throughput. Hence a counter rather than a
	// state assertion.
	//
	// Counts are per flow and cumulative over its whole life (Visits never resets), so a test reads the delta it
	// cares about. A Transact contention retry re-runs the closure and legitimately re-counts, so a test
	// asserting an exact count must be single-worker and contention-free.
	CheckpointFlowRowWrite = "flowRowWrite"

	// Subgraphs:
	CheckpointBeforeSubgraphSpawn    = "beforeSubgraphSpawn"    // before the transaction that parks a subgraph caller and creates its child
	CheckpointSubgraphSpawnParked    = "subgraphSpawnParked"    // inside the spawn's transaction, after the caller is parked and before the child is inserted
	CheckpointSubgraphSpawnAbandoned = "subgraphSpawnAbandoned" // a spawn rolled back because its caller step is no longer running
	CheckpointBeforeSpawnResolve     = "beforeSpawnResolve"     // before a failed spawn is delivered to its caller
	CheckpointBeforeReviveWrite      = "beforeReviveWrite"      // the subgraph-completion path, just before its transaction's caller-revive write

	// Resume, Cancel, Terminate, Fork, Continue and Delete:
	CheckpointResumeLeafSelected        = "resumeLeafSelected"        // Resume, after it selected the leaf interrupt and before it reads the leaf's park record
	CheckpointResumeBeforeFlowWrite     = "resumeBeforeFlowWrite"     // Resume, just before its transaction's flow-status gate write
	CheckpointCancelBeforeMark          = "cancelBeforeMark"          // Cancel, after its watermark transaction commits, before it marks the steps
	CheckpointTerminateAfterScan        = "terminateAfterScan"        // Terminate, after its pre-transaction scan of the tree, before its transaction
	CheckpointTerminateAfterStepsLocked = "terminateAfterStepsLocked" // Terminate, in its transaction, after its first step UPDATE and before it locks the flow rows
	CheckpointTerminateAfterRescan      = "terminateAfterRescan"      // Terminate, in its transaction, after its scan for children spawned since the first scan
	CheckpointForkBeforeClone           = "forkBeforeClone"           // Fork, before its first clone transaction
	CheckpointForkRootLocked            = "forkRootLocked"            // Fork, in a clone transaction, after it locked the source root and before it reads the source
	CheckpointForkPublished             = "forkPublished"             // Fork classified a published live source; its archive pass follows
	CheckpointContinuePublished         = "continuePublished"         // Continue read a published live source; its archive pass follows
	CheckpointBeforeDeleteStamp         = "beforeDeleteStamp"         // Delete, after it read the root, before it acts on what it read

	// Reads, each fired on every side it reads:
	CheckpointFingerprintFlowRead = "fingerprintFlowRead" // Fingerprint, after the flow row and before it counts the steps
	CheckpointHistoryFlowRead     = "historyFlowRead"     // History, after the flow row and before the tree
	CheckpointStepRead            = "stepRead"            // Step, after the step row and before its refs and navigation

	// Background recovery:
	CheckpointLeaseRecoverySelected = "leaseRecoverySelected" // lease recovery, after it selected expired steps and before its by-id reset; reached only when it found some
	CheckpointWedgeSweepSelected    = "wedgeSweepSelected"    // the wedged-park sweep, after it selected parked callers with no open child and before it reads each one's latest child; reached only when it found some

	// CheckpointRecoverySweepDone is fired at the end of a recovery pass, once every detector in it has read the
	// database. recoveryLoop sweeps ON ENTRY (Startup), so that first pass runs CONCURRENTLY with the test body, and
	// a test that forges a wedge or orphan shape and then drives one detector itself is racing it: the sweep sees
	// the forged shape too and counts and logs it a second time. The pass is not instant - four scans on one
	// goroutine sharing the engine's connection pool - so against a loaded server it lands seconds after Startup
	// (measured on SQL Server: the sweep's own recoverOrphanedFlows landed 27ms before a forge that followed a
	// Create+Await). Wait for this before forging; the wait is free once the sweep is behind (Visits).
	CheckpointRecoverySweepDone = "recoverySweepDone"

	// Connection pools:
	CheckpointPoolPushHeld = "poolPushHeld" // recomputePools, inside poolsLock between reading the roles and pushing the sizes; reached only by the recompute FaultSlowPoolPush picks

	// The refiller, each fired bare and by shard:

	// CheckpointRefillCycleDone is fired when a shard's piston completes a cycle that PUSHED, so that its cache
	// partition now reflects the plan.
	//
	// Offer admits a step into an EMPTY partition unconditionally, while Cache.Pop ranks partitions by the bands
	// they hold and never consults the current plan, so a doorbell-admitted hint is indistinguishable from a
	// plan-selected one until the owning shard's next cycle reconciles it. Every shard reconciles on its own
	// cadence, so a test that needs the fleet's partitions to agree with the plan before it asserts dispatch ORDER
	// must wait for a cycle per shard; no amount of elapsed time substitutes, because one starved piston is
	// exactly the case that breaks it.
	CheckpointRefillCycleDone = piston.CheckpointCycleDone

	CheckpointRefillTallyDone = piston.CheckpointTallyDone // a shard's piston published a tally from a real scan
	CheckpointRefillStole     = piston.CheckpointStole     // a shard's piston selected steps from OUTSIDE its residue class

	// The peer registry:

	// CheckpointPeerPass is fired by shard as that shard's Sonar starts a registry pass - its beat and its read -
	// from the turn the engine hands it, so only once the runtime is up. Passes run one at a time, so a visit also
	// says the previous pass is over: two visits counted from a moment mean a whole read ran after it. That is how
	// a test waits for a reading that was attempted, where the Sonar's own BlindFor grows with the clock whether or
	// not anything read.
	CheckpointPeerPass = "peerPass"

	// Reaping and archiving:
	CheckpointReapBatchLocked      = reaper.CheckpointBatchLocked      // a reaper batch has locked its due roots, inside its transaction
	CheckpointReapBeforeRootDelete = reaper.CheckpointBeforeRootDelete // a chunked deletion is about to recheck and delete its root
	CheckpointReapChunkDone        = reaper.CheckpointChunkDone        // a chunk of a chunked deletion has committed and been counted
	CheckpointReapStageChunkDone   = reaper.CheckpointStageChunkDone   // a staging chunk, or the publication, has committed
	CheckpointArchivePassDone      = "archivePassDone"                 // the archive loop, after each pass over the shards
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
