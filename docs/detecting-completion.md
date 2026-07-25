# Detecting flow completion

The engine has **no stop-notification callback**. There are three ways to learn a flow's outcome, and they
suit different situations:

1. **`Await`** — block a live caller until the flow stops, then read the outcome.
2. **`Poll`** — `Await` with a bounded wait: on a ctx timeout it returns the flow's *current* outcome instead
   of an error, so a caller whose own deadline is shorter than the flow can answer now and ask again.
3. **Orchestration** — model the follow-up work as tasks inside a workflow that calls the real work as a
   subgraph, so the reaction to success or failure is itself durable.

Pick `Await` when a caller is standing by and can wait for the result. Pick `Poll` when the caller is bounded
by its own deadline — an HTTP handler backing a status endpoint — but the flow is not. Pick orchestration when
the reaction must happen reliably regardless of who is (or isn't) waiting — a push notification, a downstream
call, a compensation.

## Await

```go
outcome, err := eng.Await(ctx, flowKey)
```

`Await` blocks until the flow stops (`completed`/`failed`/`cancelled`/`interrupted`) and returns the outcome.
It wakes promptly whichever replica ran the flow's last step, with no configuration or host support required.

**Good for:** a request/response caller that holds the `flowKey` and can wait; tests; short flows.

**Limits:**
- The caller must **stay alive and connected**. If its context deadline fires before the flow stops, `Await`
  returns without the outcome — the flow keeps running, and the caller must `Await` again (it still holds the
  key). Long-running or human-in-the-loop flows routinely outlive a request context.
- It does **not** survive the caller process restarting — there is no durable "call me back."
- It is a poor fit for **fire-and-forget** submissions, where nobody waits.

## Poll

```go
ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
defer cancel()

outcome, err := eng.Poll(ctx, flowKey)
if err != nil {
    return err // a real failure: the flow is unknown, or the database is down
}
if !outcome.Stopped() {
    // Still running. Answer the caller now; it will ask again.
    return respond(http.StatusAccepted, outcome.Status)
}
return respond(http.StatusOK, outcome)
```

`Poll` waits exactly like `Await`, but **a ctx timeout is not an error**: it returns the flow's current,
non-terminal outcome, whose `Stopped()` reports `false`. So it settles the "flow outlives the caller's
deadline" problem in `Await`'s limits above without a hand-rolled `Snapshot` loop — and it still returns
promptly the moment the flow stops, rather than sleeping out a fixed interval.

**Good for:** an HTTP status endpoint, or any caller whose deadline is shorter than the flow. It gives you
long-polling: wait up to your own budget, answer with whatever is true then, and re-poll.

**Limits:** the same as `Await` otherwise — the caller must hold the `flowKey`, and nothing durable happens
if it never comes back. For work that must happen regardless of who is waiting, use orchestration.

## Orchestration (recommended for reliable follow-up)

Wrap the real work in an **orchestrating graph**: one task launches it as a subgraph, and separate tasks
handle the success and failure paths. Because each follow-up is its own **step**, it is checkpointed and can
be **retried independently** — a delivery that must not be lost belongs here, not in a caller's `Await`.

```go
orch := workflow.NewGraph("ProcessOrder")
orch.SetEndpoint("CallSubgraph", "orch/call")
orch.SetEndpoint("SubgraphSuccess", "orch/success")
orch.SetEndpoint("SubgraphFailed", "orch/failed")

orch.AddTransition("CallSubgraph", "SubgraphSuccess")        // normal path
orch.AddTransitionOnError("CallSubgraph", "SubgraphFailed")  // error path
orch.AddTransition("SubgraphSuccess", workflow.END)
orch.AddTransition("SubgraphFailed", workflow.END)
```

The three roles (the names don't matter — the *shape* does):

**`CallSubgraph`** — calls the subgraph, yields while it runs, and returns any error so the graph routes it:

```go
func callSubgraph(ctx context.Context, f *workflow.Flow) error {
    var out map[string]any
    yield, err := f.Subgraph("the-real.workflow", f.Snapshot(), &out)
    if err != nil {
        return err // routes to SubgraphFailed via AddTransitionOnError
    }
    if yield {
        return nil // child launched; this step parks until it completes
    }
    f.Set("result", out) // stash the outcome for the follow-up task
    return nil           // normal transition -> SubgraphSuccess
}
```

**`SubgraphSuccess`** — runs next on the normal transition. Do the follow-up work here, and because it is its
own step it can **retry itself** until it succeeds. For example, if it makes a web call to notify an upstream
party, it retries that call as many times as needed within a bounded horizon:

```go
func subgraphSuccess(ctx context.Context, f *workflow.Flow) error {
    if err := notifyUpstream(ctx, f.Snapshot()); err != nil {
        // Deliver-or-retry: keep trying with exponential backoff, up to an hour.
        if f.Retry(1*time.Second, 2.0, 1*time.Minute, 1*time.Hour) {
            return nil
        }
        return err // horizon exhausted
    }
    return nil
}
```

**`SubgraphFailed`** — wired with `AddTransitionOnError`, it runs when the subgraph fails. It has the error in
state (`onErr`) and can notify, compensate, or clean up — again as a retryable step of its own.

**Why the split into separate tasks matters:** each follow-up is a durable, independently retryable step. A
crash between the subgraph completing and the notification being delivered is recovered like any other step;
the delivery isn't tied to a caller staying connected. Doing the follow-up work *inside* `CallSubgraph`
instead would couple it to the subgraph call's own attempt and lose that independent retry.

**Good for:** guaranteed delivery, push notifications, downstream calls, compensation/saga steps,
fire-and-forget submissions.

**Limits:** more graph authoring, and you implement the delivery task yourself (the engine won't call an
external party for you).

### A note on interrupts

The orchestrator observes the subgraph's **terminal** outcome — success (normal transition) or failure
(`onError`). It cannot react to the subgraph *interrupting*: an interrupt parks the whole chain, including the
orchestrator's `CallSubgraph` step, so no task runs at that moment. To notify that a flow is **waiting for
input**, put a notify task **before** the interrupting step — the task that is about to `flow.Interrupt`
knows it is about to wait, so it (or a step just ahead of it) reports "pending" first.

---

See also: [Engine operations](operations.md) · [Fan-out & subgraphs](fan-out-and-subgraphs.md) ·
[Writing tasks](tasks.md) (control signals, `flow.Retry`).
