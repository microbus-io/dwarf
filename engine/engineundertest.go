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
	"crypto/sha256"
	"encoding/hex"
	"github.com/microbus-io/dwarf/internal/piston"
	"log/slog"
	"os"
	"testing"

	"github.com/microbus-io/dwarf/internal/database"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/seamster"
)

// This file is the engine's test-support surface. NewEngineUnderTest and SetTestName wire an engine for
// isolated testing; DB and Seams expose the two internals a black-box test (one in another package, e.g.
// fixtures) legitimately needs - the shard set for row inspection and the instrumentation seams for
// deterministic race control. Both are guarded on e.t, so they are usable only on an engine built via
// NewEngineUnderTest and panic on a production engine; no other internal is exported. A test that needs to
// drive an unexported method or forge internal columns directly (failStep, the refiller, pool sizing, ...)
// stays a white-box test in package engine, where it reaches those directly - it is not a candidate for a
// cross-package move, and that is why the exported surface here is only these two accessors.

// NewEngineUnderTest constructs an engine wired for testing: per-test isolated, auto-dropped databases
// (keyed by t.Name()) plus automatic teardown - the subsequent Startup registers a t.Cleanup that
// Shutdowns the engine at test end, so a test needs no explicit defer e.Shutdown. It lets a test
// configure the engine (SetHost, SetShard, ...) and drive Startup itself:
//
//	e := NewEngineUnderTest(t)
//	e.SetHost(h)
//	e.Startup(ctx)          // registers the cleanup; a later defer e.Shutdown(ctx) is optional
//
// It accepts any testing.TB, so it also serves *testing.B and *testing.F. When several engines must share
// one isolated database (a multi-replica test), give each the same t (they all key by t.Name()); when a
// test needs several engines in SEPARATE databases, or a benchmark reused across passes needs a distinct
// key each time, override the key with SetTestName.
//
// Logging default: a *testing.T logs to stderr at Error, so a CI failure surfaces the engine-level alarms
// (wedge sweeps / poll / refill faults) without the Info-level play-by-play noise (stderr, not t.Log,
// because a `go test` timeout panic drops buffered t.Log output but not stderr). A benchmark or fuzz target
// (*testing.B / *testing.F) defaults to SILENT, since per-iteration logging would dominate the measurement /
// flood the fuzz output. DWARF_TEST_LOG_LEVEL overrides the level (e.g. "info" or "debug" for the
// flow-status play-by-play; "silent" or "off" to force the discard logger); any explicit level un-silences a
// benchmark/fuzz. SetLogger before Startup takes over entirely.
func NewEngineUnderTest(t testing.TB) *Engine {
	t.Helper()
	e := NewEngine()
	e.t = t
	// Silent by default for the high-volume harnesses; a plain test gets Error-to-stderr. The env var wins
	// either way, and "silent"/"off" forces discard.
	silent := false
	switch t.(type) {
	case *testing.B, *testing.F:
		silent = true
	}
	level := slog.LevelError
	if s := os.Getenv("DWARF_TEST_LOG_LEVEL"); s != "" {
		switch s {
		case "silent", "off":
			silent = true
		default:
			silent = false
			_ = level.UnmarshalText([]byte(s))
		}
	}
	if silent {
		_ = e.SetLogger(slog.New(slog.DiscardHandler))
	} else {
		_ = e.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	}
	// SetTestName owns the key; e.t is set above so it cannot reject, and a fresh engine is never started.
	if err := e.SetTestName(t.Name()); err != nil {
		t.Fatal(err)
	}
	return e
}

// SetTestName sets the test-database key for an engine built with NewEngineUnderTest, overriding the
// t.Name() default the constructor installs. Construction-time only (rejected once started), and rejected
// on an engine not under test (e.t == nil). Use it to give several engines in one test SEPARATE isolated
// databases (a distinct key each, so they are independent deployments rather than one shared fleet), or to
// give a benchmark reused across passes a fresh key per pass.
func (e *Engine) SetTestName(name string) error {
	if e.t == nil {
		return errors.New("SetTestName requires an engine created with NewEngineUnderTest")
	}
	if e.started.Load() {
		return errSetAfterStartup("test name")
	}
	// Hash the name to a short, bounded id so the testing-database name sequel derives stays within the
	// strictest SQL identifier limit (Postgres 63 / MySQL 64), whatever the name's length. A non-empty
	// testHashedID becomes Config.TestID at Open, which switches the ShardSet's open path onto the
	// isolated-test path; it is set before initRuntime flips started, so it is in place before any shard opens.
	sum := sha256.Sum256([]byte(name))
	e.testHashedID = hex.EncodeToString(sum[:])[:16]
	return nil
}

// DB exposes the engine's shard set so a black-box test in another package can inspect flow/step rows
// directly. Test-only: it panics outside a test binary (!testing.Testing()), so production code never reaches
// the shards through it. The guard is testing.Testing() rather than e.t != nil so it also serves a test that
// builds its engine with the raw NewEngine (e.g. restart_test, which needs a real file DSN and no test-DB
// harness) - such an engine has e.t == nil but is still a legitimate test engine.
func (e *Engine) DB() *database.ShardSet {
	if !testing.Testing() {
		panic("Engine.DB is a test-only accessor; it is not available outside a test binary")
	}
	return &e.db
}

// Seams exposes the test instrumentation seams (fault injection and execution checkpoints) so a black-box
// test in another package can drive the engine's internal race windows deterministically. Test-only: it
// panics outside a test binary (!testing.Testing()); the seams are inert unless armed, and arming is itself
// only possible under testing.Testing(), so this exposes no production behaviour.
func (e *Engine) Seams() *seamster.Seamster {
	if !testing.Testing() {
		panic("Engine.Seams is a test-only accessor; it is not available outside a test binary")
	}
	return e.seams
}

// Fault and checkpoint names for the engine's test-only instrumentation seams. The mechanism (the registry,
// its locking, the production-inert gate, scoping, and the Wait/Break/Resume composition) lives in the
// seamster package and is documented there. The engine embeds one seamster.Seamster as e.seams (enabled under
// testing.Testing() in NewEngine) and consults it directly: e.seams.IsFault / Checkpoint / Inject / Wait /
// Break / Resume. The names live here, next to the constructor and accessors that make up the test-support
// surface, so the valid set is discoverable and a test cannot arm a fault or checkpoint no site consumes.
// They stay in package engine (unlike the shared test HELPERS) because they are fired from production engine
// code, not only from tests, so moving them to a test-only package would invert that dependency.

// --- Fault injection ---
const (
	// Scoped by task name (the graph node name passed to ExecuteTask):
	FaultExecuteTask      = "executeTask"      // ExecuteTask returns a synthetic error (as if the task failed)
	FaultPanicExecuteTask = "panicExecuteTask" // ExecuteTask panics (exercises host-call panic isolation)

	// Scoped by workflow URL:
	FaultLoadGraph = "loadGraph" // LoadGraph returns a synthetic error

	// Scoped by task name; force the persistence/transition steps of a dispatch to fail:
	FaultTransitionCommit   = "transitionCommit"   // the post-completion transition transaction errors
	FaultCompleteFlowCommit = "completeFlowCommit" // the flow-completion transaction errors
	FaultContention         = "contention"         // a dispatch transaction returns a lock-contention error
	FaultLeaseStaleWrite    = "leaseStaleWrite"    // the completion write carries a stale lease_seq (zombie)
	FaultPersistErr         = "persistErr"         // the step-completion write returns a non-contention database error (consumed per attempt, so InjectN sets how many attempts fail)
	FaultSubgraphSpawnErr   = "subgraphSpawnErr"   // createSubgraphFlow errors after the caller step parked

	// Scoped by workflow URL of the subgraph child:
	FaultSubgraphReviveLost = "subgraphReviveLost" // completeSurgraphFlow skips reviving the parked caller

	// Process-wide, consumed per attempt (InjectN sets how many attempts fail):
	FaultCompleteSurgraphErr = "completeSurgraphErr" // completeSurgraphFlow returns a non-contention database error

	// Scoped by signal op:
	FaultSignalPeersPanic = "signalPeersPanic" // the host SignalPeers call panics (host-call panic isolation)

	// Process-wide (no scope):
	FaultInterruptStaleWrite = "interruptStaleWrite" // handleInterrupt's in-tx leaf lease_seq read is forced to mismatch (zombie)
	FaultInterruptChainWrite = "interruptChainWrite" // handleInterrupt's combined chain UPDATE fails and applies nothing (deadlock victim)
	FaultDropSignalStop      = "dropSignalStop"      // signalStop delivers nothing (lost terminal wake)
	FaultDropDoorbell        = "dropDoorbell"        // the local work doorbell is dropped (the step waits for a refiller scan)
	FaultRecoveryResetErr    = "recoveryResetErr"    // the processStep recovery defer's own reset UPDATE errors
	FaultReapMidTree         = "reapMidTree"         // the reaper errors after deleting steps, before flows
	FaultReapSelectErr       = "reapSelectErr"       // the reaper's due-root SELECT errors
	FaultRefillScanErr       = piston.FaultScanErr   // the piston's priority-band scan errors (its name, so there is one catalogue)
	FaultSlowPoolPush        = "slowPoolPush"        // recomputePools stalls between reading R and pushing the derived sizes
	FaultDeliverFailureErr   = "deliverFailureErr"   // deliverFlowFailureToParent drops the parked-caller re-dispatch (lost delivery); unscoped, or scoped by the parked caller's task name for per-level control
	FaultCancelCommit        = "cancelCommit"        // the Cancel transaction errors
	FaultResumeCommit        = "resumeCommit"        // the Resume transaction errors
	FaultForkCommit          = "forkCommit"          // the Fork clone transaction errors
)

// --- Execution checkpoints ---
const (
	CheckpointResumeBeforeFlowWrite   = "resumeBeforeFlowWrite"   // resume(), just before its transaction's flow-status gate write
	CheckpointBeforeTransitionTx      = "beforeTransitionTx"      // processStep, after the step is marked completed, before the transition transaction
	CheckpointAfterCallerPark         = "afterCallerPark"         // processStep, after the subgraph caller step is parked, before createSubgraphFlow
	CheckpointBeforeRetryRewind       = "beforeRetryRewind"       // processStep, before the flow.Retry rewind transaction
	CheckpointBeforeCompleteFlowWrite = "beforeCompleteFlowWrite" // completeFlow(), just before its transaction's status-gate write
	CheckpointBeforeDeleteWrite       = "beforeDeleteWrite"       // deleteFlow(), just before its transaction's delete-stamp/interrupted-CAS write
	CheckpointBeforeReviveWrite       = "beforeReviveWrite"       // completeSurgraphFlow(), just before its transaction's caller-revive write
	CheckpointBeforeRecoveryReset     = "beforeRecoveryReset"     // processStep recovery defer, just before its fenced step-reset transaction

	// A COUNTING checkpoint (read with Visits, never a rendezvous), scoped by flow id so concurrent flows
	// count independently. No test arms Wait/Break on it, so Checkpoint just increments and returns; both the
	// count site (execution.go) and the read site (faninflowlock_test.go) consult e.seams directly.
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
	CheckpointFlowRowWrite = "flowRowWrite" // a dwarf_flows UPDATE issued by the transition transaction

	// Lifecycle rendezvous (fired at an event, used with Wait - not a freeze site): lets a test wait for
	// exact engine progress instead of polling status / sleeping. signalStop fires it BOTH unscoped and scoped
	// by (flowKey, status): the unscoped name catches whichever flow stops first, which is all a single-flow
	// test needs, while the scoped one lets a test wait for ONE flow to reach ONE status while other flows (a
	// subgraph child, a fan-out sibling, a peer replica's work) stop concurrently. Both are needed because a
	// scoped fire does not wake an unscoped waiter.
	//
	// The scoped form is meaningful only because signalStop runs POST-COMMIT: when it fires, the status is
	// durable, so a test reading the row immediately after sees it - the exact guarantee a status poll spun for.
	CheckpointFlowStopped = "flowStopped" // signalStop(), a flow just reached a stop (completed/failed/cancelled/interrupted)

	// Lifecycle rendezvous, fired at the END of a recovery pass - every detector in it has read the database
	// by then. recoveryLoop sweeps ON ENTRY (Startup), so that first pass runs CONCURRENTLY with the test body,
	// and a test that forges a wedge/orphan shape and then drives one detector itself is racing it: the sweep
	// sees the forged shape too and counts/logs it a second time. The pass is not instant - it is four scans on
	// one goroutine sharing the engine's connection pool, so against a loaded server it lands seconds after
	// Startup (measured on SQL Server: the sweep's own detectOrphanedFlows landed 27ms before a forge that
	// followed a Create+Await). Wait for this before forging; the wait is free once the sweep is behind (Visits).
	CheckpointRecoverySweepDone = "recoverySweepDone" // runRecoverySweep(), one full recovery pass is over

	// Fired per shard (scoped by the shard number, and once unscoped) when that shard's piston completes a
	// cycle that PUSHED - i.e. its cache partition now reflects the plan. Its name is the piston's, so there
	// is one catalogue, exactly like FaultRefillScanErr.
	//
	// What it exists for: `Offer` admits a step into an EMPTY partition unconditionally and stamps that
	// partition's band from the arrival, while `Cache.Pop` ranks partitions by that FROZEN band and never
	// consults the current global minimum - so a doorbell-admitted hint is indistinguishable from a
	// plan-selected one until the owning shard's next cycle reconciles it. Every shard reconciles on its own
	// cadence, so a test that needs the fleet's partitions to agree with the plan before it asserts dispatch
	// ORDER must wait for a cycle per shard; no amount of elapsed time substitutes, because one starved
	// piston is exactly the case that breaks it.
	CheckpointRefillCycleDone = piston.CheckpointCycleDone // a shard's piston reconciled its cache partition
)
