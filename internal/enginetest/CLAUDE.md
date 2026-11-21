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

**The janitor may be running while they read**, so a tree leaving the live tables must never read as live with its
steps missing:
- **A capture reads the steps first, then the flows.** A tree leaves in one transaction (a move, or a deletion that
  fits one), or over several with its root stamped first and its root row deleted last (chunked deletion,
  `internal/janitor/CLAUDE.md`). So a step missing from the first read has its flow gone or its root stamped by the
  second, at read committed on every dialect. In the other order, a tree moved between the reads reads as live with
  its steps gone. **Do not fold the two reads into one statement** either: that is one snapshot only where reads
  are, and SQL Server's locking reads are not.
- **A tree whose root is stamped may be missing recorded steps**, which chunked deletion takes a chunk at a time,
  each flow's steps before its flow row. Only a step whose row is gone is excused: the steps read records every step
  that exists, terminal or not, so one still there and no longer terminal fails as it would in any tree. A stamp is
  never cleared, so a tree read stamped is on its way out.

Pinned by `TestOutcomesKept_AStampedTreeLosesStepsAChunkAtATime` (fails without the stamp's exemption). Measured in
`TestDispatcherDrop_TheJanitorsKeepEveryFlow`, whose staged trees leave the live tables in chunks: without the
exemption its shutdown arm failed 1 of 10 runs on SQLite and 2 of 10 on PostgreSQL, a recorded step reported gone each
time; with the exemption alone, 10 of 10 passed on each. The read order guards a move landing between the two reads,
which those runs did not catch, and nothing pins it.

`CaptureOutcomesIn` records the same from shard databases a test opened itself, for a moment when no engine runs
there: after a process was killed and before its replacement starts, whose startup would begin recovering at once.
`AssertInvariantsIn` runs the checks the same way; the bench's `TestRig_Invariants` uses it on the database a rig run
left behind.

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
| A closed outcome never moves | `AssertOutcomesKept` | `lostcommit_test.go`, `histories_test.go`, `dispatcherdrop_test.go` (with the janitors running) |
| A closed outcome stays reachable while its tree moves to the archive | `AssertOutcomesKept`'s removed flows, each read again by key | `archivereach_test.go` |
| A process's death strands no obligation: a replacement finishes each with its own recovery, waiting steps keep their deadlines, an interrupted step waits for Resume | the checker and `CaptureOutcomesIn` at the kill, read by the test's own connection before the replacement starts | `processkill_test.go` |
| An operation, a recovery or a lost reply leaves what the reference model says at each point a history lands it - inside a task, before a transition, a rewind or an interrupt, across an operation's midpoint, between executions: which executions run, what each is handed and contributes, the outcome, its reason and prompt | the model, after every event of a generated history | `histories_test.go` |
| Generation ownership: a write by a worker that lost its lease changes nothing | not checkable at rest - a fenced write leaves no trace, so a snapshot cannot tell it from one never attempted | `leasefence_test.go`, `terminatefence_test.go`, `interruptfence_test.go`, `ownershipfence_test.go` |

## The tests the checker serves, and how each works

Each of these drives the engine into a state that is hard to reach on demand, then holds it to the invariants, the
recorded outcomes, or both. The file's opening comment explains its mechanism; this is the map.

| Test | What it proves | How it gets there |
|---|---|---|
| `engine/lostcommit_test.go` | A commit whose reply is lost costs nothing: one advancement, no moved outcome, the operation's result as without the fault | A fault commits, then reports a lost connection; a hold between the commit and the engine's retry (`armLostReply`) records the outcomes before the retry can touch them |
| `engine/archivereach_test.go` | A closed tree answers every read the same while the janitor moves it to the archive, through faults and deletes | Every read of every tree recorded before the moves, then readers looping while the test drives the janitor in waves |
| `engine/processkill_test.go` | A process killed at a durable boundary strands nothing a replacement cannot finish with the engine's own recovery | The test binary re-runs itself as a child process, held at the kill point and killed with SIGKILL; the parent reads the database itself, then starts a replacement |
| `engine/impairedshard_test.go` | A refused, slow or hung shard does not slow the healthy one | A TCP proxy in front of one shard's server, switched between forwarding, refusing, delaying and freezing |
| `engine/histories_test.go` | Every operation, recovery and lost reply leaves what a reference model says, wherever a history lands it | Seeded scripts of events run in lockstep with the model, every execution admitted one at a time (below) |

## Generated histories

`engine/histories_test.go` explains what a history is and how one runs; `engine/historymodel_test.go` holds the
model's rules. Four shapes - a linear flow, a two-branch cohort, a caller with one subgraph child, a step with an
error handler - and events that run an execution (complete, sleep, retry, interrupt, fail, spawn), land Cancel or
Terminate at a point of it or across it, lapse its lease, lose the reply to its commit, admit two branches together,
or call an operation between executions. What follows is what the code alone does not tell you.

- **Every execution waits at the task handler's door until the driver admits it.** That makes the order the engine
  runs things in the history's order rather than the scheduler's, so a seed replays the same schedule, and an
  execution the model did not predict is caught at the door rather than inferred from timing.
- **The handler checks what it is handed, not only what it writes.** Measured: with the carrier handed its input
  without the earlier runs' changes, all 128 default histories passed while only the written tokens were checked.
- **An operation overlaps engine work only where it is held at its own midpoint** - Cancel between its watermark's
  commit and its mark, Terminate between its scan and its transaction - with a whole execution run inside. No hold
  is inside an engine transaction: on SQLite a held transaction serializes every writer, so an overlap inside one is
  a PostgreSQL lock-order question this test does not ask.
- **A sleep is an hour, and the driver ends it where the model wakes the step** - `not_before` brought to now, and
  the step offered to the dispatcher, whose piston would otherwise look again only an hour on. A sleep short enough
  to wait out makes an operation meant to land while the step sleeps race its wake, and lose under load.
- **Before an operation lands, every step the engine has claimed is pulled to the door.** A pending step that is
  awake is claimed within moments; an operation racing that claim would find it pending (a Cancel preempts it) or
  running (a Cancel covers its completion), and the model cannot say which. At the door it is running, always.
- **A preemption is the engine's own dispatch, so the driver waits for it.** The model cancels a marked step the
  moment it is marked; the engine does it at the step's next claim, a sleeping step's once it wakes - so the driver
  wakes a marked sleeper at once. Measured: without the wait, a Terminate in the next event found a preempted branch
  still pending under parallel sweeps, while the same history replayed alone passed. A trunk step's preemption
  closes the flow, and the wait for the close hid it.
- **A delayed worker is let go only after its step is claimed again.** Lease recovery re-pends the step without a
  new generation; the next claim makes one, and fences every write the worker makes after. Measured: let go before
  the claim that preempted a marked branch, the worker's completion landed on the re-pended row.
- **Two executions are admitted together only where the history's order decides.** Racing, two interrupts do not
  lead to one place - the first to commit is the one Resume addresses first - so two interrupting branches are held
  before their transactions and released in order. A history that let them race was unreplayable when the engine
  committed them the other way. The free race is `TestConcurrentInterrupt_FirstWriterWins`'s.
- **The model is a set of rules over the shapes it knows, never a second engine.** Its header states the accepted
  contribution and cancellation policies. A disagreement is an engine defect or a policy the model failed to state;
  settle which before changing either. Extend it a rule at a time.
- **Every closed outcome is recorded when its flow closes and compared after every later event** - a child's as
  well as the root's - and each flow's final state is checked against the model's, frozen at its close. A Terminate
  under a held execution, or a commit whose reply was lost, records them there, before the execution or the retry
  goes on, as a lost-reply test records its baseline before the retry. Measured: with the retry rewind's
  `status='running'` guard removed, every seed passed while the record was taken after the held dispatch returned,
  the rewind having rewritten the terminated step by then; and a child's final state rewritten after its delivery
  passed while only the root's close was recorded.
- **An event's checks wait for every dispatch it set going**: the execution it admitted, each preemption, and each
  execution a Terminate let go from the door, whose fenced writes otherwise land after the checks.
- **The floor is met by choosing seeds, never by tuning weights.** A default run takes seeds 1 to `hBaseSeeds`, then
  the lowest later seed for each `hFloor` situation they miss, found by generating from the model alone. Weights tuned
  until fixed seeds reach the floor reshuffle what every seed draws, so each change drops a different situation.
- A failure prints the history as JSON; `DWARF_TESTING_HISTORY` replays it on whichever database `SEQUEL_TESTING_DSN`
  selects, and `DWARF_TESTING_HISTORY_MINIMIZE` reduces it to the fewest events that still fail - any failure, so read the
  last candidate's messages.

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
| Subgraph spawn (`createWithGraph` with `parkCaller`) | caller `running`, this `lease_seq` | the caller row | caller `status='running' AND parked=0 AND lease_seq=?`: gone, or already parked by a retry after a landed commit - roll back (`errCallerGone`). Then a caller at or under its flow's watermark is marked, its write's rows not counted, and the child is inserted cancelled | child's doorbell (the piston) | `FaultSubgraphSpawnCommitLost`, `TestFaultSite_SubgraphSpawnErr/commit_lost` |
| Revive (`completeSurgraphFlow`) | caller `running`, `parked=1` | the caller row | `parked_child_flow_id=<child>` (`parkOwnerGuard`): already settled, no-op | caller's doorbell (the piston) | `FaultReviveCommitLost`, `TestLostCommit_Revive` |
| Error delivery (`deliverSubgraphError`, the wedge sweep's) | caller `running`, `parked=1`, child closed | the caller row | as the revive | caller's doorbell (the piston) | `FaultDeliverErrorCommitLost`, `TestLostCommit_DeliverError` |
| Create (`createWithGraph`) | nothing | new rows | none | doorbell (the piston); Create reports an uncertain outcome, and a retry is a second flow | `FaultCreateCommitLost`, `TestLostCommit_Create` |
| Resume (`resume`) | leaf `interrupted`, its `lease_seq` | the chain's step rows | leaf `status='interrupted' AND lease_seq=?`, root `status='interrupted'`: 409 | leaf's doorbell (the piston); a retried Resume is a 409 | `FaultResumeCommitLost`, `TestLostCommit_Resume` |
| Terminate (`terminateSubtree`) | any open tree | the tree's step rows, then its flow rows, then steps again | flows `status NOT IN (terminal)` all zero: already closed, no-op; new children: roll back and re-run | stop signals (the latch board polls) | `FaultTerminateCommitLost`, `TestLostCommit_Terminate` |
| Fork (`forkFrom`) | a closed source | the source root | the source gone or marked: 404 / 409 | the leaf's doorbell (the piston); a retried Fork is a second fork | `FaultForkCommitLost`, `TestLostCommit_Fork` |
| Retry rewind (`retry`) | step `running`, this `lease_seq` | the step row | `status='running' AND lease_seq=?`: fenced | none | none yet |
| Defer rewind (`deferStep`, one statement) | step `running`, this `lease_seq` | the step row | `status='running' AND lease_seq=?`: fenced | none | none yet |
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
