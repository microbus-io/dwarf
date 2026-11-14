# Dwarf `internal/enginetest` — the invariant checker and the mutation inventory

> Load when: changing `invariants.go`, adding a durable invariant, or adding or changing an engine transaction.
> Coupled with: `engine/CLAUDE.md` §"Transactions" and §"Background Recovery" (the mechanisms each invariant rests
> on), `fixtures/CLAUDE.md` (the waits that bring a test to quiescence first) and `internal/diagnostics/CLAUDE.md`
> (the seams a lost-reply test arms).

## The checker asserts at rest, once

`AssertInvariants` counts, per shard, the rows violating each check in `invariantChecks`; every count must be zero.
The checks hold only once the workload has **quiesced**: a cohort mid-resolve shows every step terminal on a running
flow, and a completed child's caller is still parked until its delivery commits. So a test calls it after every Await
returned and every repair it induced has been driven, and calls it **once**. Never wrap it in a retry or a settle
window: waiting for an invariant to come true can only hide a violation.

A check excludes a legitimate shape by naming it, never by loosening the predicate:

- **A doomed tree** (its root's `delete_after_ms > 0`): Delete cancels an interrupted flow and leaves its interrupted
  leaf and parked callers for the janitor to remove with the tree.
- **An interrupted caller**: an interrupt flips the surgraph chain's parked callers to `interrupted` with `parked=0`
  and keeps `parked_child_flow_id`, which Resume re-parks. Measured: the park checks without that arm failed
  `TestConcurrentInterrupt_FirstWriterWins` and `TestFault_InterruptStaleWriteRollback`.
- **A closed flow's tail still flagged `advancing`**: the transaction that closes a flow leaves the flag, never read.

`CaptureOutcomes` and `AssertOutcomesKept` check what a snapshot cannot: that a closed outcome does not move. They
record every closed flow's status, final state, error and cancel reason, and every terminal step of a closed flow or
completed with `advancing=0`, then compare a later moment by value (stored JSON is decoded, numbers as `json.Number`).
One kind of terminal step is deliberately not recorded, because a repair legitimately rewinds it: a completed step
still `advancing` on an open flow, which the orphan sweep replays. A flow gone from the live
tables is returned rather than failed, for the test to account for: a Delete, a Purge, or a move to the archive.
`CaptureOutcomesIn` records the same from shard databases a test opened itself, for a moment when no engine runs
there: after a process was killed and before its replacement starts, whose startup would begin recovering at once.

## Invariants

| Invariant | Checked by | Pinned behaviourally by |
|---|---|---|
| A closed flow holds no live step | `closed flow with a non-terminal step`, `terminal step with parked<>0` | |
| A running flow has a step that will advance it | `running flow with zero non-terminal steps` | |
| `created` is transient (Fork's clone only) | `flow at rest in created`, `step at rest in created` | |
| Tree links resolve within one tree | `dangling surgraph link`, `root_flow_id not resolving to a self-root`, `cross-flow DAG edge` | |
| One durable advancement per obligation | `completed step still advancing on an open flow`, `successor inserted twice by one transition`, `cohort converged more than once` | `lostcommit_test.go` |
| Cohort accounting | `cohort counter overshoot`, `cohort converged before it resolved, or with a loss` | `cohortbatch_test.go`, `cohortlock_test.go` |
| A park is settled by the child that owns it | `waiting caller without an open child owning its park`, `open child whose caller does not hold its park`, `step neither waiting nor terminal still naming a park owner` | `subgraphspawn_test.go`, `ownershipfence_test.go` |
| A closed outcome never moves | `AssertOutcomesKept` | `lostcommit_test.go`, `histories_test.go` |
| A closed outcome stays reachable while its tree moves to the archive | `AssertOutcomesKept`'s removed flows, each read again by key | `archivereach_test.go` |
| A process's death strands no obligation: a replacement finishes each with its own recovery, waiting steps keep their deadlines, an interrupted step waits for Resume | the checker and `CaptureOutcomesIn` at the kill, read by the test's own connection before the replacement starts | `processkill_test.go` |
| An operation leaves what the reference model says, wherever it lands: which executions run, what each contributes, the outcome and its reason | the model, after every event of a generated history | `histories_test.go` |
| Generation ownership: a write by a worker that lost its lease changes nothing | not checkable at rest - a fenced write leaves no trace, so a snapshot cannot tell it from one never attempted | `leasefence_test.go`, `terminatefence_test.go`, `interruptfence_test.go`, `ownershipfence_test.go` |

## Generated histories

`engine/histories_test.go` draws short histories from seeds and drives an engine through each in lockstep with a
reference model (`engine/historymodel_test.go`): a task's next execution and what it does (complete, sleep, retry,
interrupt, fail), an operation landing at a point of that execution, or an operation on the flow at rest. The model
predicts where the flow stands after every event; at the end the outcome, the step rows and the executions must be
its, no closed outcome may move, through later operations and every recovery detector, and the checker must find
nothing.

- **Every execution waits at the task handler's door until the driver admits it.** That makes the order the engine
  runs things in the history's order rather than the scheduler's, so a seed replays the same schedule, and an
  execution the model did not predict is caught at the door rather than inferred from timing. An operation lands at
  its point by a hold: inside the task, at `CheckpointBeforeTransitionTx`, at `CheckpointBeforeRetryRewind`; at rest
  the flow is interrupted, closed, or its next step asleep (a sleep long enough for the operation to land first).
- **The model is a set of rules over the shapes it knows, never a second engine.** Its header states the accepted
  contribution and cancellation policies. A disagreement is an engine defect or a policy the model failed to state;
  settle which before changing either. Extend it a rule at a time.
- **Record the closed outcome where a Terminate closed it, under the held execution**, as a lost-reply test records
  its baseline before the retry. Measured: with the retry rewind's `status='running'` guard removed, every seed
  passed while the record was taken after the held dispatch returned, because the rewind had already rewritten the
  terminated step by then.
- A failure prints the history as JSON; `DWARF_HISTORY` replays it on whichever database `SEQUEL_TESTING_DSN`
  selects. The test logs the situations its histories reached - each event kind in the state it landed on, each way a
  flow closed - because a seed count says nothing about breadth.

## The mutation inventory

Each engine transaction: the state it starts from, its first lock, the guard whose zero-row match ends it and what
that means, and what runs after it commits. **A lost reply skips everything after the commit** - the doorbell, the
stop signal, a parent's revive - so each post-commit action needs a backstop, named here.

| Transaction | Starts from | First lock | Guard, and a zero-row match | After commit (backstop) | Lost reply |
|---|---|---|---|---|---|
| Completion write (`persist`, one statement) | step `running`, this `lease_seq` | the step row | `status NOT IN (terminated, cancelled) AND lease_seq=?`: fenced, abandon | the transition | `FaultPersistLandedErr`, `persist_test.go` |
| Transition (`transitionTx`) | step `completed`, `advancing=1` | flow row, guarded non-terminal: flow closed, commit nothing | dispatched row `lease_seq=? AND advancing=1`: fenced, or a retry after a landed commit - roll back | doorbell for the first successor (the piston) | `FaultAdvanceCommitLost`, `TestLostCommit_Transition` |
| Cohort arrival (`bumpCohortArrivals`, in the transition) | as above | the spawn row; the resolving arrival grabs the flow row after the bump | arriving row `lease_seq=? AND advancing=1`: roll back | the fan-in's doorbell (the piston) | `FaultAdvanceCommitLost`, `TestLostCommit_CohortArrival` |
| Batched arrivals (`arrivalBatchTx`) | each member as above | the spawn row | each member's `lease_seq=? AND advancing=1`: that member is taken back off the counter | each member arrives alone, and finds its arrival landed | `FaultArrivalBatchCommitLost`, `TestCohortBatch_LostCommitAcknowledgement` |
| Direct fan-in (`fireFanInDirect`) | source `completed`, `advancing=1` | flow row, guarded non-terminal: commit nothing | source `lease_seq=? AND advancing=1`: roll back | doorbell (the piston) | `FaultFanInDirectCommitLost`, `TestLostCommit_FanInDirect` |
| Flow completion (`completeFlow`) | tail `completed`, `advancing=1` | flow row, unguarded | step `lease_seq=? AND advancing=1` on an open flow: fenced. Flow `status NOT IN (terminal)`: already closed - a retry re-drives the parent's revive | stop signal (the latch board polls), parent revive (the retry re-drives it, then the wedge sweep) | `FaultCompleteFlowCommitLost`, `TestLostCommit_FlowCompletion` |
| `failStep` | step `running`, or `completed` + `advancing=1` (`stepHeld`) | the step row | `<held> AND lease_seq=?`: fenced, abandon | a lost flow's tail; the parent caller's doorbell (the piston) | `FaultFailStepCommitLost`, `TestLostCommit_FailStep` |
| Subgraph spawn (`createWithGraph` with `parkCaller`) | caller `running`, this `lease_seq` | the caller row | caller `status='running' AND parked=0 AND lease_seq=?`: gone, or already parked by a retry after a landed commit - roll back (`errCallerGone`) | child's doorbell (the piston) | `FaultSubgraphSpawnCommitLost`, `TestFaultSite_SubgraphSpawnErr/commit_lost` |
| Revive (`completeSurgraphFlow`) | caller `running`, `parked=1` | the caller row | `parked_child_flow_id=<child>` (`parkOwnerGuard`): already settled, no-op | caller's doorbell (the piston) | `FaultReviveCommitLost`, `TestLostCommit_Revive` |
| Error delivery (`deliverSubgraphError`, the wedge sweep's) | caller `running`, `parked=1`, child closed | the caller row | as the revive | caller's doorbell (the piston) | `FaultDeliverErrorCommitLost`, `TestLostCommit_DeliverError` |
| Create (`createWithGraph`) | nothing | new rows | none | doorbell (the piston); Create reports an uncertain outcome, and a retry is a second flow | `FaultCreateCommitLost`, `TestLostCommit_Create` |
| Resume (`resume`) | leaf `interrupted`, its `lease_seq` | the chain's step rows | leaf `status='interrupted' AND lease_seq=?`, root `status='interrupted'`: 409 | leaf's doorbell (the piston); a retried Resume is a 409 | `FaultResumeCommitLost`, `TestLostCommit_Resume` |
| Terminate (`terminateSubtree`) | any open tree | the tree's step rows, then its flow rows, then steps again | flows `status NOT IN (terminal)` all zero: already closed, no-op; new children: roll back and re-run | stop signals (the latch board polls) | `FaultTerminateCommitLost`, `TestLostCommit_Terminate` |
| Fork (`forkFrom`) | a closed source | the source root | the source gone or marked: 404 / 409 | the leaf's doorbell (the piston); a retried Fork is a second fork | `FaultForkCommitLost`, `TestLostCommit_Fork` |
| Retry rewind (`retry`) | step `running`, this `lease_seq` | the step row | `status='running' AND lease_seq=?`: fenced | none | none yet |
| Interrupt (`handleInterrupt`) | leaf `running`, this `lease_seq` | the chain's step rows | the leaf must move to `interrupted` under its generation: roll back | stop signal | none yet |
| Cancel's watermark (`cancel`) | an open tree | the tree's flow rows | `cancel_watermark=0`: already cancelled, re-apply the mark | the step marks, in chunks | none yet |
| Recovery reset (`recoverStep`) | step `running` (contention) or `completed` (a transition that did not commit) | the step row | `status=<from> AND parked=0 AND lease_seq=?`: no-op | re-poll | none yet |
| Lease recovery (`recoverExpiredLeases`) | step `running`, lease expired | by id | the predicate repeated: no-op | the piston | none yet |
| Orphan repair (`recoverOrphanedFlows`) | step `completed`, `advancing=1`, lease expired, flow open | flow row, guarded `running`/`interrupted` | the step predicate repeated; the rewind bumps `lease_seq` | the piston | none yet |

**A lost-reply test waits for the retry, and records its baseline before it.** What the commit wrote runs on - the
successor dispatches, the flow closes - so an Await returns while the operation that lost its reply is still backing
off, and the test's end then cancels the retry unrun; a baseline captured after the retry cannot see it rewrite an
outcome. So the test holds the operation at `CheckpointCommitLost` (`armLostReply`), records the closed outcomes as
the commit left them, releases it, waits for the dispatch that held it to return (`awaitDispatched`), and compares
after the retry and again after every recovery detector (`assertSettledClean`). Measured: with persist's backoff at an
hour, the transition and flow-completion tests passed when they only awaited the flow, and a retry rewriting a
completed flow's final state went unseen.

The janitor's transactions have their own lost-reply faults (`FaultJanitorLostResponse`, `FaultReapStageLost`) and
tests in `internal/janitor` (`internal/janitor/CLAUDE.md`).

**A new transaction, or a new state transition in an existing one, updates this table in the same commit**: its guard
and what a zero-row match means, what it skips when its reply is lost and what backstops that, a check in
`invariantChecks` or the reason the invariant needs a behavioural test, and a lost-reply fault with its test.
