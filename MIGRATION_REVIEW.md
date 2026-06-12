# Dwarf Code Review — Migration-Gap Report (foreman → dwarf)

**Date:** 2026-06-11
**Reference (source of truth):** `github.com/microbus-io/fabric/coreservices/foreman` (+ `fabric/workflow` for the workflow types)
**Under review:** `github.com/microbus-io/dwarf`

Dwarf is a port/extraction of the original monolithic `foreman` service into a standalone, dependency-injected engine. This review compares dwarf function-by-function against foreman to find logic the port **dropped or changed**. The intended differences (no bus/transport, no metrics, `metadata` replacing actor-claims, `PeerNotifier`/`FlowStoppedCallback` replacing multicast) are excluded; what remains are regressions.

Method: failing-test triage, `-race`, full foreman↔dwarf diff across lifecycle/operations, execution, completion/subgraph, scheduling/backpressure/breaker, and the workflow package.

> **STATUS (2026-06-12): all findings resolved.** Every HIGH and actionable MEDIUM/LOW regression below has been fixed by porting from foreman verbatim, backed by 6 new fixtures. Full suite + new fixtures green; `-race` clean. Legend: ✅ fixed · ⏭️ deliberately not changed (reason given).

---

## 1. Already applied & verified (3 fixes)

These were fixed first during the review (tests green, `-race` clean).

| Fix | What | Files |
|---|---|---|
| Cross-replica doorbell | All 9 step-origination sites dropped the peer `Enqueue` (kept only the local ring), so a replica with no spare workers stranded steps until a peer's backstop poll. `TestCrossReplicaAwait` deterministically timed out. Routed all 9 through a new `enqueueStep` helper (local ring + self-excluded peer wake); inbound `HandleEnqueue` stays local-only to avoid an echo storm. | operations.go, restart.go, completion.go, execution.go |
| Valve data race | `taskValve.wCong`/`tCong` were read after releasing `valvesLock` (`go test -race` flagged it in `TestDistributedbackpressureflow`). `recoverRate` now takes them by value, snapshotted under the lock. | backpressure.go, engine.go, execution.go |
| `PeerNotifier` contract | Documented that signals are self-excluded (engine applies the local effect, then notifies peers); a bus impl must filter self-delivery. | engine.go |

---

## 2. Migration-gap findings

### HIGH — correctness / recovery wedges

**✅ H1. `flows.step_id` never written at create → `Start` doorbell fires step 0.**
*Fixed: `createWithGraphTx` captures `InsertReturnID` and writes `step_id` in the flow UPDATE (operations.go).*
`createWithGraphTx` discards the inserted step id and the flow UPDATE omits `step_id` (operations.go:132, 145-148); foreman sets it (service.go:768-771). `flows.step_id` stays 0, so `startNotify` reads 0 and the targeted enqueue degrades to a generic refill. (The doorbell fix H-fix above still works via the empty-cache fallback, which is why the test passes — but this is the root cause of the weak targeted dispatch.) Also degrades `Snapshot` of running flows (see M-snapshot).

**✅ H2. `reconstituteBreakers` missing entirely.** Foreman re-arms the in-memory breaker map on startup by scanning `parked=parkedBreaker` rows (breaker.go:316, wired service.go:255). Dwarf has no such function or call. After a restart, breaker-parked backlog is invisible to selection (the index excludes `parked!=0`) and no probe is ever scheduled (the in-memory breaker isn't tripped) → **the parked backlog is stranded permanently.** Recovery wedge.
*Fixed: ported `reconstituteBreakers` (breaker.go) + wired into `initRuntime` before workers start. Fixture: `breakerreconstituteflow`.*

**✅ H3. `parkTrippedSteps` missing.** Foreman, inside the Start/Retry transaction, parks newly-activated pending steps whose task has a tripped breaker (service.go:105, called service.go:844). Dwarf doesn't, so CREATED→PENDING activation births steps at `parked=0` and dispatches them straight into the known-bad endpoint. (Step *inserts* still honor `initialParkedFor`, so only the bulk CREATED→PENDING activation is exposed.)
*Fixed: ported `parkTrippedSteps` (breaker.go) + wired into `startNotify`'s transaction. (dwarf has no public Retry method, so that wiring is N/A.) Fixture: `breakerstartparkflow`.*

**✅ H4. `HandleTripBreaker` drops `breakerBulkPark`.** Foreman's gossip handler trips in-memory AND bulk-parks the receiver's backlog so its DB view converges (service.go:2950-2957). Dwarf only flips the in-memory flag (engine.go:636-642) — it even discards `breakerTrip`'s `(fresh, nextProbeAt)`. A peer that learns of a trip via gossip keeps dispatching the task's pending backlog at full rate → **the distributed breaker is defeated.**
*Fixed: `HandleTripBreaker` now `breakerTrip`s and, when fresh, `breakerBulkPark`s with error logging (engine.go).*

**✅ H5. `Purge` deletes RUNNING flows.** dwarf selects/deletes every match with no `status<>running` guard, no `purgeCap` (10000), no transaction, no `DISTINCT` (history.go:419-462); foreman has all four (service.go:2580-2683). A `Purge(OlderThan=…)` will **delete in-flight running flows and their steps**, corrupting active executions. Most dangerous single finding.
*Fixed: restored the `status<>running` guard (SELECT + DELETE), 10000 cap + per-shard limit, transaction, and `DISTINCT` (history.go). Fixture: `purgerunningflow`.*

**✅ H6. Fan-in cohort SELECT error swallowed → flow hangs forever.** The cohort `SELECT cohort_arrivals, cohort_size, …` error is ignored (execution.go:500-503); foreman traces it and rolls back (execution.go:964-971). On a transient read error, `arrivals/size` stay 0, `fullyResolved` is false, the fan-in step is never inserted, and (since the CAS already bumped `cohort_arrivals`) a retry mis-counts → the cohort is silently abandoned.
*Fixed: the cohort SELECT and the cohort-failure `computeFinalState` now propagate their errors (execution.go).*

**✅ H7. Breakpoint pause never fires `FlowStoppedCallback`.** `handleBreakpoint` only `signalStop`s (execution.go:577-616); foreman fires the root-flow `OnFlowStopped` (execution.go:375-386). Dwarf's own *interrupt* path does fire it — so a `StartNotify` caller is notified for `flow.Interrupt` pauses but **not** breakpoint pauses. Inconsistent omission.
*Fixed: `handleBreakpoint` now fires the root-flow `flowStoppedCallback` (execution.go). Fixture: `breakpointnotifyflow`.*

**✅ H8. `Step` never populates `PrevKey`/`NextKey`.** The ~115-line subgraph-seam navigation block was dropped (history.go:133-175 vs foreman service.go:2078-2192). The field is documented as "populated only by the Step endpoint" (flowstep.go) yet is always empty → prev/next navigation broken.
*Fixed: ported the navigation block + `skipSurgraphForward`/`skipSurgraphBackward` helpers (history.go). Fixture: `stepnavflow`.*

**✅ H9. `pollPendingSteps` dropped the future-lease-expiry wake.** Foreman queries `MIN(lease_expires)` of running steps and sizes the timer to it (database.go:224-238); dwarf omits it (scheduling.go:104-156). A crashed worker's leased step is recovered only on the next `maxPollInterval` sweep instead of right at lease expiry → crash-recovery latency degrades to minutes.
*Fixed: added the `MIN(lease_expires)` wake query folded into `shardNearestDelay` (scheduling.go).*

**✅ H10. `cancel` drops all error checks + the first-cancel-wins guard.** Every step/flow UPDATE and `computeFinalState` error is swallowed, and the `RowsAffected()==0 → 409` conflict guard is gone (operations.go:455-524 vs service.go:1407-1507). A failed `computeFinalState` commits garbage `final_state`; a racing terminal transition is no longer detected, so duplicate `signalStop`/stop callbacks fire. (The `status NOT IN (terminal)` WHERE-guard itself is preserved, so the DB write stays idempotent.)
*Fixed: error checks on both step UPDATEs and `computeFinalState`, plus the `RowsAffected()==0 → 409` guard (operations.go).*

**✅ H11. `breakerBulkPark` swallows its error + lost the lock-contention retry.** It returns nothing and ignores `eachShard`'s error; foreman's `handleBreakerTrip` retries on `IsLockContentionError` (`maxBulkParkAttempts`). Dwarf *did* improve the design by folding the failed-probe demote into the park transaction (so a partial-failure wedge is avoided), which makes this self-healing via re-dispatch→re-trip rather than a hard wedge — but a contention rollback is now **silent** and triggers the bounded re-dispatch avalanche the breaker exists to prevent. At minimum it should log; ideally restore the retry.
*Fixed: `breakerBulkPark` now returns its `eachShard` error and `handleBreakerTrip` retries on lock contention (`maxBulkParkAttempts`), keeping dwarf's atomic-demote improvement.*

### MEDIUM

**✅ M1. `deleteFlow` semantics + race-guard.** Blocks `created` flows (foreman allows them), and the final DELETE dropped both the `flow_token` match and the `status<>running` re-guard (operations.go:551-568 vs service.go:2556-2569) → a flow that flips to `running` between the SELECT and DELETE is deleted out from under execution.
*Fixed: now blocks only `running`; SELECT + DELETE inside one transaction with `flow_token` match and `status<>running` re-guard (operations.go). Fixture: `deletecreatedflow`.*

**⏭️ M2. `completeSurgraphFlow` dropped the `surgraph_step_id==0` fallback.** The depth-based resolution `else` branch is gone (completion.go:231-252 vs foreman completion.go:297-321); when `surgraph_step_id==0` the UPDATE runs `WHERE step_id=0` and no-ops, stranding the parent. Only bites legacy `step_id=0` rows — unreachable in a greenfield dwarf DB, so low real-world impact today.
*Not changed (deliberate): foreman's fallback detects parked steps via a `lease_expires > NOW+1h` threshold, which is **incompatible** with dwarf's parked-column redesign (CLAUDE.md), and dwarf always sets `surgraph_step_id`, so the branch is unreachable. Porting it verbatim would be wrong.*

**✅ M3. `runRefill` floor uses `defaultPriority`, not the selected band.** `cache.refill(batch, defaultPriority)` (scheduling.go:326-331); foreman passes `chosenBand`. Wrong floor makes the doorbell head-insert/no-op preemption decision incorrect (a more-important arrival can be rejected, or a less-important one head-inserted). Bounded by the design's explicitly-accepted "exact ordering is soft" tolerance, but it is a true value regression (the loop scopes `band` with `:=` and discards it).
*Fixed: hoisted `chosenBand` out of the loop and pass it as the floor (scheduling.go).*

**✅ M4. Swallowed errors (cluster).** Several paths drop error checks foreman has, so DB failures surface as silently-wrong results rather than aborts:
- `completeFlow` surgraph-linkage SELECT + `completeSurgraphFlow` return (completion.go:213-220) → surgraph completion silently skipped.
- `failStep` `lineage_id` lookup (completion.go:279-282) → on read error, a cohort member is misclassified (`lineageID==0`) and the whole flow is failed instead of deferring to fan-in.
- `computeFinalState`/`mergeTerminalSteps` json (un)marshal (completion.go:99,102,160,167) → corrupt column yields silently-empty `final_state`.
- terminal-flow-check UPDATE (execution.go:152-157) → step left `running`, and because `nil` is returned the lock-contention recovery `defer` can't act.
- `List`/`fingerprint`/`completeFlowSequential` drop shard/step errors (history.go; completion.go:71-76) → silent partial results / wrong change-detection hash.
*Fixed: all of the above now propagate their errors (completion.go, execution.go, history.go).*

**⏭️ M5. `snapshot` returns empty state for running flows.** Rewritten to switch on flow status; for `running`/`created` it returns `{}` and dropped foreman's fan-out (`step_id==0`) live-state handling and race-retry (operations.go:228-302). Partly a consequence of H1 (no usable `flows.step_id`). Behavioral divergence to confirm against intent.
*Not changed (deferred): the H1 root cause (`flows.step_id`) is now fixed, but `snapshot` was intentionally left switching on flow status. Returning live in-flight merged state for a running flow is a larger, riskier behavioral change worth confirming against product intent before porting foreman's fan-out snapshot.*

**⏭️ M6. Telemetry dropped wholesale.** All breaker-trip / probe-outcome / rate-cut / saturation-drop / steps-executed metrics and spans are absent. Almost certainly a deliberate standalone-engine scope reduction — flag only if metrics are part of the engine's contract. (Concrete line-level tell: `breakerClose` deleted the `cause` read that existed only to label the success metric.)
*Not changed (deliberate): out of scope for the standalone engine; metrics/tracing are a host concern (per CLAUDE.md).*

### LOW

- **✅** `handleBackpressure` drops the `RowsAffected()==0` short-circuit (execution.go:692-695) → misleading "Task backpressured" log + needless poll-shorten on an already-terminal step. *Fixed: error check + `RowsAffected()==0` early return restored (execution.go).*
- **⏭️** `handleEnqueue` can offer a non-existent step (phantom refill) when the PK lookup misses. *Not changed: harmless by design — the claim CAS rejects the phantom job; matches foreman's Enqueue endpoint behavior.*
- **✅/⏭️** `continueFlow`/`step`/`setBreakpoint` drop `json.Unmarshal` error checks (history.go). *`continueFlow` and `step` fixed; `setBreakpoint` left as-is — its fallback to the already-initialized empty map matches foreman's behavior on a corrupt column.*

---

## 3. NOT migration regressions (inherited from the reference)

These were initially flagged but are **identical in `fabric/workflow`** — pre-existing characteristics, not introduced by the port. The entire `dwarf/workflow` package is a faithful port (all 10 reducers, every `Graph.Validate` rule, the renderer math — code-identical modulo package/symbol renames):

- `MergeState` keeps deleted fields as JSON `null`.
- `Graph` JSON round-trip drops `annotations`.
- `FlowRenderer` embeds a raw `\n` inside Mermaid `["…"]` labels.
- `RawFlow.ClearControl` doesn't clear `subgraphWorkflow`/`subgraphInput`.
- `Graph.Transitions()` returns the internal slice.
- `graphrenderer` duplicate-edge-label join (`"for each | for each"`) and `%q` label quoting.

Worth fixing on their own merits, but they belong in `fabric/workflow` upstream, not as dwarf migration fixes.

---

## 4. Why didn't the tests catch these?

The `fixtures/` suite is rich on **single-engine, happy-path feature behavior** but structurally blind to the regression classes above:

1. **The suite was already red.** `TestCrossReplicaAwait` was failing before this review (15s timeout). A regression that *did* have a test wasn't being acted on — CI/local runs were tolerating a red suite. **First recommendation: get to green and keep it green.**
2. **No `-race` in the default run.** The valve data race only appears under `-race` (+ multi-replica timing). It's not in the normal `go test ./...`.
3. **Thin multi-replica coverage.** Only `TestCrossReplicaAwait` and `TestDistributedbackpressureflow` use two engines. The breaker gossip convergence (H4), `parkTrippedSteps` (H3), and restart reconstitution (H2) have no two-engine fixture.
4. **No restart/recovery fixtures.** H2 (`reconstituteBreakers`) and H9 (lease-expiry wake) require *stopping and restarting* an engine, or crashing a worker mid-lease — the suite never does.
5. **No operator-endpoint safety tests.** H5 (`Purge` deletes running) and M1 (`Delete` race) need a test that purges/deletes *while a flow is running* and asserts it's refused. None exists.
6. **No fault injection.** The swallowed-error findings (H6, H10, M4) only manifest on DB errors mid-transaction; nothing injects a failing or flaky executor or DB.
7. **Assertions don't cover dropped output fields.** H8 (`PrevKey`/`NextKey`) and H7 (breakpoint callback) need explicit assertions on those fields/callbacks; existing tests don't check them.

### Recommended fixtures (each maps to a finding)

| Fixture | Catches | Status |
|---|---|---|
| `breakerreconstituteflow` | H2 | ✅ Added — trips a breaker, `Shutdown`, new engine on same file DB, asserts the parked backlog drains. |
| `breakerstartparkflow` | H3 | ✅ Added — trips breaker, `Start`s a fresh flow on that task, asserts its task is not executed while tripped. |
| `purgerunningflow` | H5 | ✅ Added — starts a blocked flow, `Purge`s its workflow, asserts it survives and `deleted==0`. |
| `deletecreatedflow` | M1 | ✅ Added — asserts `Delete` succeeds on a created flow (foreman semantics). |
| `breakpointnotifyflow` | H7 | ✅ Added — `StartNotify` + breakpoint; asserts `FlowStoppedCallback` fires with interrupted status. |
| `stepnavflow` | H8 | ✅ Added — multi-step flow; asserts `Step().PrevKey/NextKey` are populated for the middle step. |
| `distributedbreakerflow` | H4 | ⏭️ Not added — shared-DB "multi-replica" harness can't expose it (eng1's bulk-park already parks the shared rows). Fixed by port. |
| `startdoorbellflow` | H1 | ⏭️ Not added — `flows.step_id` isn't observable via the public API. Fixed by port; covered indirectly by `TestCrossReplicaAwait`. |
| `crashrecoveryflow` | H9 | ⏭️ Not added — needs worker-death injection (timing-heavy). Fixed by port. |
| fault-injection harness | H6, H10, M4 | ⏭️ Not added — needs a DB/executor error-injection wrapper the harness lacks. Fixed by port (error propagation). |

Process: **(a)** make `go test ./...` green and gate merges on it ✅ (now green); **(b)** add `-race` to CI for `engine/` and the multi-replica fixtures; **(c)** adopt a "diff against foreman" checklist for any further ported surface.

---

## 5. Remediation order — completed

All items below were ported from foreman verbatim (same names/godoc/structure) and verified.

1. ✅ **H5 `Purge` running-flow guard** — `status<>running` guard, cap, transaction, `DISTINCT` restored.
2. ✅ **H2 / H3 / H4 breaker trio** — `reconstituteBreakers` (+ startup wire), `parkTrippedSteps` (+ Start wire; no public Retry to wire), and `breakerBulkPark` into `HandleTripBreaker`.
3. ✅ **H1 `flows.step_id` at create** — step-id write restored (re-enables the targeted `Start` doorbell). `Snapshot` live-state (M5) left as a deferred follow-up.
4. ✅ **H6 / H10 swallowed errors that wedge or corrupt** — fan-in cohort SELECT check, `cancel` error checks + 409 guard, `failStep` lineage check.
5. ✅ **H9 lease-expiry wake**, **H7 breakpoint callback**, **H8 Step nav** — all ported.
6. ✅ **M1, M3, M4** — `Delete` guard, refill floor band, remaining swallowed errors. ⏭️ **M2** (incompatible with dwarf's parked-column redesign) and **M5** (deferred) intentionally not changed.
7. ⏭️ **M6 metrics / inherited workflow items** — out of scope; the workflow items belong upstream in `fabric/workflow`.

**Verification:** `go build`/`go vet`/`gofmt` clean; full `go test ./...` green (engine + ~56 fixtures including 6 new); `go test -race` clean on `engine/` and the multi-replica/breaker fixtures.

> Two intentional engineering choices retained over literal foreman fidelity: (1) dwarf's "Purge requires a filter" guard is kept (foreman lacks it) as an extra safety net; (2) dwarf's atomic demote-inside-`breakerBulkPark` design is kept (a documented improvement over foreman's separate-statement demote), with foreman's retry/error-propagation layered back on top.

---

## 6. Coverage

Diffed and found **faithful** (no regression): `create`/`createWithGraph` (retry loop intact), `Run`, `Startup`/`Shutdown` ordering, config builders, `eachShard`/DB open/migrate, `HandleEnqueue`/`handleEnqueue`/`HandleSyncValve`/`HandleNotifyStatusChange`, the `processStep` claim CAS (all four dialects), lease sizing, transition/error-transition evaluation, static & dynamic fan-out (forEach strip, `<as>Index`/`Count`, ordinals, DAG edges), fan-in merge/ordering/no-escalation, retry/backoff, `flow.Sleep`, the valve math (`recoverRate`, `valveRegulate`, SyncValve merge), `scanPriorityBand`, the `runRefill` band-walk/fairness pick, `timerLoop` nextPoll/nextProbe separation, `breakerTrip`/`breakerCommit`/`breakerClose`/`breakerBulkUnpark`/`breakerProbeBackoff`/`initialParkedFor`, `candidatecache`, `computeFinalState`/`mergeTerminalSteps`, the surgraph/subgraph chain walks, and the **entire `workflow` package**.
