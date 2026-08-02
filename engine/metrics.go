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
	"database/sql"
	"strconv"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/errors"
	"github.com/microbus-io/sequel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// metricScope is the OpenTelemetry instrumentation scope for the engine's instruments. Its Resource
// (service identity) comes from the injected MeterProvider, not from here - the engine never stamps
// service.name/instance attributes (those are resource-level, owned by the host's OTEL pipeline).
const metricScope = "github.com/microbus-io/dwarf"

// Attribute cardinality and PII are the host's to bound via a metric View.

// engineMetrics holds the engine's OpenTelemetry instruments. The counters are incremented inline at
// their event sites; the gauges are observable (async) and pulled by observeGauges at collection time.
type engineMetrics struct {
	flowsStarted        metric.Int64Counter
	flowsTerminated     metric.Int64Counter
	stepsExecuted       metric.Int64Counter
	stepsRecovered      metric.Int64Counter
	stepsUnwedged       metric.Int64Counter
	stepsWriteRetried   metric.Int64Counter
	stepsWriteFailed    metric.Int64Counter
	flowsOrphaned       metric.Int64Counter
	stateWriteBytes     metric.Int64Counter
	stateReadBytes      metric.Int64Counter
	stepsClaimLost      metric.Int64Counter
	stepsClaimPreempted metric.Int64Counter
	stepsOffered        metric.Int64Counter
	turnWait            metric.Float64Histogram
	gateWait            metric.Float64Histogram
	dbPhaseSeconds      metric.Float64Histogram
	peerChanges         metric.Int64Counter

	reg metric.Registration // the observable-gauge callback registration, unregistered at Shutdown
}

// initMetrics creates the engine's instruments from the resolved MeterProvider and registers the
// observable-gauge callback. Called from initRuntime. With no provider injected it falls back to the
// global otel.GetMeterProvider(), which is the no-op provider unless the host configures the SDK - so
// in unit tests (and unconfigured standalone use) the instruments are no-ops and the callback is never
// invoked, incurring no per-collection DB queries.
func (e *Engine) initMetrics() error {
	mp := e.meterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	meter := mp.Meter(metricScope)
	// Held so the pistons resolve their own instruments from the SAME meter, keeping one instrumentation
	// scope per engine however many modules record into it.
	e.meter = meter
	m := &engineMetrics{}

	var errs []error
	ctr := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return c
	}
	// Counter instrument names carry no _total suffix; the Prometheus exporter appends it at scrape time.
	// Unit-denominated counters end with the unit (…_bytes), per the Prometheus naming convention.
	m.flowsStarted = ctr("dwarf_flows_started", "Counts flows that have been started, by the shard they were placed on. The shard attribute measures PLACEMENT: pickShard is capacity-weighted random, so a skew here means the fleet's work is not spread the way the weights intended, and one shard's steps carry more than their share. Against dwarf_flows_terminated{shard} it is that shard's in-flight flow count. Note a closed-loop caller makes starts and terminations equal by construction - only an open-loop arrival rate lets a shortfall here indict the CALLER rather than the engine.")
	m.flowsTerminated = ctr("dwarf_flows_terminated", "Counts flows that have reached a terminal status, by the shard holding them and BY WHICH terminal status - completed, failed or cancelled. All three are counted, which is what makes dwarf_flows_started minus this a stable in-flight count rather than one that drifts upward by every flow that did not finish cleanly. Subgraph children are counted here and as starts, so a tree nets to zero. Split by status for the failure and cancellation rates; dwarf_steps_executed{status} is the step-level view of the same thing.")
	m.stepsExecuted = ctr("dwarf_steps_executed", "Counts steps that have been executed, by the shard holding them. The shard attribute is what turns this into per-shard dispatch throughput - the quantity a per-shard piston, pool and partition all separately govern, and the one a fleet-wide total hides.")
	m.stepsRecovered = ctr("dwarf_steps_recovered", "Counts steps whose worker lease expired and were reset to pending for re-execution - the crash-recovery path. A nonzero rate means workers are dying or overrunning their lease.")
	m.stepsUnwedged = ctr("dwarf_steps_unwedged", "Counts parked steps recovered by the wedge sweep, labelled by park type. A nonzero value signals a latent bug whose effect the sweep papered over.")
	m.stepsWriteRetried = ctr("dwarf_steps_write_retried", "Counts in-place retries of a step's persistence write after a non-contention database error. The task is NOT re-executed; only the write is retried. A rising count tracks database flakiness, not workflow failure.")
	m.stepsWriteFailed = ctr("dwarf_steps_write_failed", "Counts steps terminalized because their outcome could not be persisted while the database was reachable - i.e. the payload, not the database, was the problem. A nonzero value signals a latent bug (an unstorable value, a column/packet limit, a constraint violation), like dwarf_steps_unwedged.")
	m.flowsOrphaned = ctr("dwarf_flows_orphaned", "Counts running flows detected as stranded by the orphan sweep - every step terminal, none touched within the threshold, no successor. A nonzero value signals a latent bug the processStep recovery defer could not cover, like dwarf_steps_unwedged; detection-only, the sweep does not re-drive the flow.")
	bytesCtr := func(name, desc string) metric.Int64Counter {
		c, err := meter.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit("By"))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return c
	}
	// State byte throughput on the dispatch hot path only: rates here track against the database's
	// byte-throughput ceiling (disk/WAL), which binds separately from its steps ceiling. Reads/writes by
	// the introspection APIs (List/History/Snapshot) and flow-row writes (final_state) are not counted.
	m.stateWriteBytes = bytesCtr("dwarf_state_write_bytes", "Counts payload bytes written to step rows on the execution path, labelled by workflow and by column (state, changes, interrupt_payload).")
	m.stateReadBytes = bytesCtr("dwarf_state_read_bytes", "Counts payload bytes read from step rows on the execution path, labelled by workflow and by column (state, changes, resume_data, subgraph_result).")

	// The piston-owned instruments (the three dwarf_refill_* plus dwarf_steps_stolen) are NOT built here.
	// They belong to the pistons, which own the cycle they measure, and are resolved from the meter this
	// engine hands each of them (initRuntime) - one instrumentation scope for the whole engine, whoever
	// records into it. Building them here as well would register each name twice on one meter, with
	// descriptions and bucket boundaries that had already drifted apart. Their `shard` attribute must stay
	// a STRING to match the ones below; see internal/piston for what a type split silently breaks.
	// Buckets span sub-millisecond (uncontended) to seconds (the exit side saturated): the whole question
	// this answers is which of those a deployment is in.
	gateWait, err := meter.Float64Histogram("dwarf_turnstile_gate_wait_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time a worker spent waiting for its FIRST turn - the one taken before it picks a candidate up, per shard. It is the queue in front of dispatch, and the only turn a worker waits for while holding no work, which is also what stops the crew growing into a saturated shard. Observed on every successful acquire, INCLUDING the ones whose pop then lost the race and took nothing, so count is admissions rather than dispatches and sum/count is the mean wait. Read against dwarf_turnstile_wait_seconds: this one rising alone means work is being ADMITTED more slowly, both rising means the shard's connections are the constraint."),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5))
	if err != nil {
		errs = append(errs, errors.Trace(err))
	}
	turnWait, err := meter.Float64Histogram("dwarf_turnstile_wait_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Time a worker spent waiting for the turn that RECORDS a finished step, per shard. A step that has run holds a claim stamped when it was picked up, so it is older than anything admitted while its task ran and is served ahead of it - which means a sustained wait here is not completions queueing behind dispatch but the shard's connections being saturated by other work at least as old. Observed on every completion, so count is completions and sum/count is the mean; near-zero throughout is the healthy state."),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5))
	if err != nil {
		errs = append(errs, errors.Trace(err))
	}
	dbPhaseSeconds, err := meter.Float64Histogram("dwarf_db_phase_seconds",
		metric.WithUnit("s"),
		metric.WithDescription("Wall-clock time a worker spent INSIDE a database phase, by role (enter/exit): admission to the host call for enter, the host call to the end of the step for exit. It is the phase's true residence, and it is measured rather than inferred because nothing else can supply it - the turnstile wait covers only the queue in FRONT of each call, and summing a step's individual query times misses the connection wait and the Go-side work between them. Read it against dwarf_turnstile_gate_wait_seconds to tell a phase that is slow from one that is merely queued, and against the turn count to see whether that count is anywhere near the concurrency this workload actually needs."),
		metric.WithExplicitBucketBoundaries(0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10))
	if err != nil {
		errs = append(errs, errors.Trace(err))
	}
	m.dbPhaseSeconds = dbPhaseSeconds
	m.turnWait = turnWait
	m.gateWait = gateWait
	m.stepsOffered = ctr("dwarf_steps_offered", "Counts steps admitted to this replica's candidate cache by the DOORBELL rather than selected by a refill cycle - a step whose predecessor just completed, taking the slot it vacated. Read against dwarf_refill_candidates_selected it is the share of dispatch that came from step origination instead of from the weighted fairness plan; a high share is normal under load (a cycle supplies only ~1.05-1.5x ahead of consumption, so partitions drain to empty between cycles) and is what keeps a sequential chain from paying a cycle per hop.")
	m.stepsClaimPreempted = ctr("dwarf_steps_claim_preempted", "Counts popped candidates skipped without a claim attempt because a sibling worker in this replica already had one in flight for that step - round trips SAVED, the counterpart to dwarf_steps_claim_lost. The refiller re-selects a step whose claim is uncommitted (the selection predicate reads committed state), so this rises with the scan rate rather than with the replica count. Compare the two: a healthy engine converts what would have been lost claims into skips.")
	m.peerChanges = ctr("dwarf_peer_changes", "Counts times a shard's observed replica count MOVED, by shard. In a settled fleet this must be ZERO for the whole run: every increment past the initial join re-divides that shard's connection pool and re-seats every replica's residue class of step ids, so a nonzero rate means the fleet is churning and the two divisors are chasing it. The usual cause is not a real join or departure but a stale registry row - a replica that died without deleting its row inflates the count until it ages out, and a crash-loop with random ids accumulates them faster than that.")
	m.stepsClaimLost = ctr("dwarf_steps_claim_lost", "Counts claim attempts whose CAS matched no row - the step was already claimed by a peer, or left the claimable state (cancelled, resumed, parked) between selection and claim. The lease-contention signal: measured against dwarf_steps_executed it is the share of dispatch round trips that produced no work, and it rises with the replica count when replicas select overlapping candidates. Not an error - the step is simply someone else's - but a high ratio means the fleet is paying for round trips it cannot use.")

	gauge := func(name, desc, unit string) metric.Int64ObservableGauge {
		g, err := meter.Int64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return g
	}
	// A duration gauge is float64 SECONDS, not integer milliseconds. Seconds is the OTEL convention for a
	// duration, and float64 is what keeps the convention affordable: this quantity is a cycle interval, ~0.067s
	// in a healthy fleet, so an integer-seconds gauge would read 0 both there AND for a shard three cycles
	// behind - the entire range that matters collapsed into one bucket.
	gaugeF := func(name, desc, unit string) metric.Float64ObservableGauge {
		g, err := meter.Float64ObservableGauge(name, metric.WithDescription(desc), metric.WithUnit(unit))
		if err != nil {
			errs = append(errs, errors.Trace(err))
		}
		return g
	}
	// The gauges split into two kinds, and the difference is NOT cosmetic - it decides how a dashboard
	// must aggregate them across replicas:
	//   - PER-REPLICA (read from this replica's memory): queueDepth and most of the rest. Sum across replicas.
	//   - CLUSTER-WIDE (read by querying the SHARED shard databases): stepsPending, oldestAge.
	//     Every replica observes the SAME number, so summing them multiplies by the replica count - a
	//     1,000-step backlog reads as 3,000 on three replicas. Aggregate with max (or avg), never sum.
	// Each description says so, because that is where an operator building a panel actually reads it.
	//
	// These two are also the ONLY instruments that cost a query at collection time (one per shard, in
	// observePendingByBand), so the cluster-wide set is kept to what genuinely cannot be answered any other
	// way. Per-task running concurrency was here and was removed: it cost a second GROUP BY per shard per
	// scrape on every replica to produce R copies of one number, carried the least bounded label in the set
	// (task_url), and per-downstream concurrency belongs to the host, which owns the account and tenant
	// identity the engine cannot see - the same rule that keeps backpressure out of the engine.
	queueDepth := gauge("dwarf_steps_queue_depth", "Per-replica: steps waiting in this replica's local worker cache. Sum across replicas.", "")
	stepsPending := gauge("dwarf_steps_pending", "Cluster-wide: due pending steps in each priority band, read from the shared database. Every replica reports the same value - aggregate with max, not sum.", "")
	oldestAge := gauge("dwarf_steps_oldest_pending_age_seconds", "Cluster-wide: age of the oldest due pending step in each priority band, read from the shared database. Every replica reports the same value - aggregate with max, not sum.", "s")
	// The two peer gauges are PER-REPLICA readings of a per-shard fact, and both are pulled from the Sonars
	// rather than emitted by them - internal/peers deliberately owns no meter, so this is the one scope that
	// has to stay in step. Neither queries anything: both are atomic reads of what the last reading published.
	peerReplicas := gauge("dwarf_peer_replicas", "Per-replica, per-shard: how many replicas this one currently sees holding connections to that shard - the divisor its pool is sized by. Replicas should AGREE on it, so a spread across the fleet is itself the signal: one replica reading 3 while its peers read 4 is sizing its pool for a fleet that does not exist. It is deliberately slow to fall (a reading that did not happen is not evidence anybody left), so a drop lags a real departure by a reading or two.", "")
	tallyAge := gaugeF("dwarf_refill_tally_age_seconds", "Per-replica: how long ago the STALEST shard still in this replica's planner reported. Every shard plans from a merged view of every shard's LAST report, so a piston cycling slowly holds its peers' plans on a picture that old - the global priority band and the per-key slice rule are both computed from those mixed-freshness tallies. Expect roughly one cycle interval in a healthy fleet; sustained multiples name a shard whose piston has fallen behind its peers, which no throughput number can distinguish from a slow database. Single-shard deployments always read ~one interval and can ignore it.", "s")
	// Per ROLE: entering and exiting work draw on separate reservations, and an operator reading one without
	// the other cannot tell "dispatch is gated" from "completions are". Instantaneous, so a reservation that
	// is saturated all window without being sampled empty looks idle - dwarf_turnstile_wait_seconds is the
	// durable half of that pair.
	turnstileAvail := gauge("dwarf_turnstile_available", "Per-replica, per-shard: turns currently free at the shard's turnstile. Turns are a fixed multiple of that shard's connection pool, so this is how much of the admission budget is unspoken for right now. It is deliberately LARGER than the pool: a turn admits a caller to compete for a connection rather than reserving one, and with only as many turns as connections the pool runs below capacity for want of anyone queued for it (measured: a 6x throughput collapse). Sustained zero means the shard's connections are the binding constraint; read the wait histograms for how long callers are actually queueing, since an instantaneous sample cannot tell a turnstile saturated all window from an idle one. A NEGATIVE value is only ever a live resize that shrank the ceiling below what is currently held; it self-corrects as those holders return.", "")
	turnstileWaiting := gauge("dwarf_turnstile_waiting", "Per-replica, per-shard: callers queued at the shard's turnstile right now. Unlike the free-turn count this has no ceiling, so it is the one that shows how deep a queue has become rather than merely that it is non-empty - and because a caller queues once per database call rather than once per step, it counts calls in flight, not workers. Read with dwarf_turnstile_available: available at zero with this climbing is a shard falling behind, while both near zero is an idle one.", "")
	// No high-water companion, deliberately: a peak held since process start is a constant within hours of
	// startup, and a WINDOWED peak - which is the one an operator actually wants - is max_over_time on this
	// gauge, computed by the metrics backend for free. An instrument cannot do that better than the query can.
	dbPhaseWorkers := gauge("dwarf_db_phase_workers", "Per-replica, BY ROLE (enter/exit): worker goroutines inside a metered database phase right now. This is the quantity that sets connection-pool pressure, and nothing else reports it - dwarf_workers_resident is the whole crew, and a worker parked in the host call still holds its candidate, so the crew's idle count cannot be differenced to get it. Read against dwarf_turnstile_available: a phase count far above the turns a shard has means most of those workers are queued rather than working. For the pressure a phase actually reached rather than what the collection tick caught, take max_over_time of this - the two roles peak at different moments, so do not sum them.", "")
	workersResident := gauge("dwarf_workers_resident", "Per-replica: worker goroutines that exist. It grows on demand up to the lease-margin ceiling and falls again as workers measure themselves surplus and retire, never below the connection-derived dispatch count. Read against dwarf_turnstile_available to tell the two long-task regimes apart: a crew far above the turn count with turns free is serving long tasks correctly, while a crew pinned at the ceiling is the one to alarm on. Rising under load and settling back afterwards is the shrink rule working; wide oscillation at steady load is not. Sum across replicas.", "")
	peerBlind := gauge("dwarf_peer_blind_seconds", "Per-replica, per-shard: how long since that shard's registry was last read successfully. Zero on a healthy Sonar. Past two read cadences the replica is BLIND there - it holds its last known fleet (the safe direction for pools) and stops partitioning that shard's candidates (the safe direction for work), so a nonzero value explains both at once and is the first thing to check when a shard's counts look frozen.", "s")
	// The two in-flight state gauges are ONE reading and are near-useless apart: the quotient is the mean
	// state a task carries, and that is what says which of the two multipliers is loading this replica -
	// a few carriers each holding a large document, or a wide fan-out of carriers each holding a little.
	// They point at different work, and no single number distinguishes them.
	inFlightBytes := gauge("dwarf_state_in_flight_bytes", "Per-replica: state payload this replica is holding across host calls right now - the JSON each in-flight task's carrier was built from, summed over the tasks currently inside ExecuteTask. Divide by dwarf_state_in_flight_steps for the mean state a task carries: a large mean says state SIZE is what loads this replica, a small mean against a large count says fan-out WIDTH is. It measures the wire form, not the decoded maps that occupy the heap - read against Go heap to get the decode expansion factor, which is the number that says whether holding state in decoded form is worth what it costs. This is the one instrument for a cost the engine otherwise cannot see at all: workers hold no connection during a host call, so the crew grows freely for long tasks, and held state times crew size is the memory ceiling nothing else reports. Sum across replicas.", "By")
	inFlightSteps := gauge("dwarf_state_in_flight_steps", "Per-replica: tasks currently inside a host call - the denominator for dwarf_state_in_flight_bytes. It is NOT dwarf_workers_resident (workers that EXIST, most of which are idle or in a database phase) and NOT dwarf_db_phase_workers (workers in a database phase, which is the opposite population - a task inside a host call holds no connection); neither can serve as this denominator. Sum across replicas.", "")

	reg, err := meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			return e.observeGauges(ctx, o, observableGauges{
				queueDepth:       queueDepth,
				stepsPending:     stepsPending,
				oldestAge:        oldestAge,
				peerReplicas:     peerReplicas,
				peerBlind:        peerBlind,
				tallyAge:         tallyAge,
				turnstileAvail:   turnstileAvail,
				turnstileWaiting: turnstileWaiting,
				workersRes:       workersResident,
				dbPhase:          dbPhaseWorkers,
				inFlightBytes:    inFlightBytes,
				inFlightSteps:    inFlightSteps,
			})
		},
		queueDepth, stepsPending, oldestAge, peerReplicas, peerBlind, tallyAge,
		turnstileAvail, turnstileWaiting, workersResident, inFlightBytes, inFlightSteps,
		dbPhaseWorkers,
	)
	if err != nil {
		errs = append(errs, errors.Trace(err))
	}
	m.reg = reg

	e.metrics = m
	return errors.Join(errs...)
}

// closeMetrics unregisters the observable-gauge callback so it is not invoked after the databases are
// closed. Safe to call when metrics were never initialized.
func (e *Engine) closeMetrics() {
	if e.metrics != nil && e.metrics.reg != nil {
		_ = e.metrics.reg.Unregister()
	}
}

// observableGauges bundles the gauge instruments handed to the collection callback.
type observableGauges struct {
	queueDepth       metric.Int64ObservableGauge
	stepsPending     metric.Int64ObservableGauge
	oldestAge        metric.Int64ObservableGauge
	peerReplicas     metric.Int64ObservableGauge
	peerBlind        metric.Int64ObservableGauge
	tallyAge         metric.Float64ObservableGauge
	turnstileAvail   metric.Int64ObservableGauge
	turnstileWaiting metric.Int64ObservableGauge
	dbPhase          metric.Int64ObservableGauge
	workersRes       metric.Int64ObservableGauge
	// The two in-flight state gauges are one reading; see their descriptions for why the quotient, not
	// either alone, is what an operator reads.
	inFlightBytes metric.Int64ObservableGauge
	inFlightSteps metric.Int64ObservableGauge
}

// observeGauges is the observable-gauge callback. It reads in-memory engine state and queries the
// shards for the current-state gauges at collection time. Per-replica: cluster-wide aggregates (e.g.
// concurrency_running) are summed at the metrics backend across replicas.
func (e *Engine) observeGauges(ctx context.Context, o metric.Observer, g observableGauges) error {
	// Local in-memory gauges - no DB.
	o.ObserveInt64(g.queueDepth, int64(e.cache.Len()))

	if e.crew != nil {
		o.ObserveInt64(g.workersRes, int64(e.crew.Resident()))
	}

	// Read as a pair, in this order, so a collection landing mid-dispatch cannot report bytes against a
	// count that already includes the carrier those bytes belong to - the quotient is the whole point, and
	// it is only ever read down (a low mean), never up. Neither is a query.
	o.ObserveInt64(g.inFlightBytes, e.inFlightStateBytes.Load())
	o.ObserveInt64(g.inFlightSteps, e.inFlightStateSteps.Load())

	// The turn counts, taken as ONE snapshot rather than a read per shard: they are the quantity a storm
	// moves fastest, and per-shard reads would report different instants as a mixed picture.
	var availBy, waitingBy map[int]int
	if e.turnstiles != nil {
		availBy, waitingBy = e.turnstiles.Snapshot()
	}

	for idx, role := range []string{phaseEnter, phaseExit} {
		o.ObserveInt64(g.dbPhase, e.dbPhaseWorkers[idx].Load(),
			metric.WithAttributes(attribute.String("role", role)))
	}

	// Peer readings, per shard: atomic reads of what each Sonar last published, so this costs no round trip
	// and stays honest while a shard is unreadable (the count holds, and blindFor is what says why).
	for _, idx := range e.db.Indices() {
		shard := metric.WithAttributes(attribute.String("shard", strconv.Itoa(idx)))
		o.ObserveInt64(g.peerReplicas, int64(e.replicasOn(idx)), shard)
		if s := e.sonarFor(idx); s != nil {
			o.ObserveInt64(g.peerBlind, int64(s.BlindFor().Seconds()), shard)
		}
		// Free turns and queued callers are one reading in two halves: the first saturates at zero and so
		// cannot show how bad it has got, the second has no ceiling and so can.
		if n, ok := availBy[idx]; ok {
			o.ObserveInt64(g.turnstileAvail, int64(n), shard)
		}
		if n, ok := waitingBy[idx]; ok {
			o.ObserveInt64(g.turnstileWaiting, int64(n), shard)
		}
	}

	// Tally age: an in-memory read of the planner's own map, unconditional because zero (no shard has
	// reported yet) is a meaningful reading rather than an absent one.
	o.ObserveFloat64(g.tallyAge, e.planner.TallyAge().Seconds())

	// The ONLY query this callback makes: pending count + oldest age per priority band, one statement per
	// shard. Everything above is an in-memory read, which is what keeps a scrape from being load.
	pending, oldest, err := e.observePendingByBand(ctx)
	if err != nil {
		return errors.Trace(err)
	}
	for priority, count := range pending {
		o.ObserveInt64(g.stepsPending, int64(count), metric.WithAttributes(attribute.String("priority", strconv.Itoa(priority))))
	}
	for priority, sec := range oldest {
		o.ObserveInt64(g.oldestAge, int64(sec), metric.WithAttributes(attribute.String("priority", strconv.Itoa(priority))))
	}
	return nil
}

// observePendingByBand returns, across all shards, the count of due pending steps per priority band
// and the age in seconds of the oldest due pending step per band (max across shards).
func (e *Engine) observePendingByBand(ctx context.Context) (countByBand, oldestSecByBand map[int]int, err error) {
	indices, pos := e.shardOrdinals()
	pendingPerShard := make([]map[int]int, len(indices))
	agePerShard := make([]map[int]int, len(indices))
	err = e.db.OnEach(ctx, func(ctx context.Context, db *sequel.DB, shard int) error {
		// THE COMMON BAND, NOT THE REFILLER'S - collection is ordered with ordinary work, not ahead of it.
		// These are O(backlog) aggregates (a COUNT and a MIN over every due step), so at band 0 they
		// preempt dispatch every time the metric reader ticks. Measured with a 50ms reader against a
		// ~26,000-step backlog: 1,360 steps/s at band 0 against 1,559 at band 1. Read the DIRECTION, not
		// the 13% - it is one pair on a rig whose RTT drifted 1.5-5.1ms across the session, and the band-0
		// arm actually had the BETTER RTT of the two, which is what makes the sign trustworthy where the
		// magnitude is not. A production reader ticks far slower, shrinking the tax without changing the
		// shape.
		//
		// This is the band rule's second half, and it is the one that is easy to miss: a band may only be
		// given to a source that cannot flood it, and floods are measured in COST, not in count. A handful
		// of queries per tick is a small count and an unbounded scan.
		//
		// Being ordered here cannot hide a saturated engine either - a late gauge is still a correct gauge,
		// and the reader tries again on its next tick. Contrast the Sonar, which does belong at band 0: its
		// queries are a single-row upsert and a read of the small registry table, bounded by construction,
		// and a Sonar that cannot beat goes BLIND, which changes how the fleet behaves rather than merely
		// when it is observed.
		ctx, doneTurn := e.dbTurn(ctx, shard)
		defer doneTurn()
		// parked=0 is what this gauge MEANS: a parked step is excluded from selection by construction, so
		// counting one would report a backlog no worker will ever pick up. Every other due-pending query in
		// the engine carries the predicate; this one was the lone exception. (No row is miscounted today -
		// `parkedSubgraph` is only ever written alongside status='running' - but that is an accident of the
		// current park kinds, not an invariant, and the next park kind that can coexist with `pending`
		// would silently inflate the gauge.)
		//
		// The reason is DEFINITIONAL, not performance - do not re-justify it as an index fix. Measured on
		// Postgres (300k terminal rows, 2k pending, the real partial index): the planner uses
		// idx_dwarf_steps_selection either way, seeking on the leading `status` column alone, and both plans
		// read every pending row (73 vs 76 buffers). They must: this is a GROUP BY over the whole due band,
		// so every due row is touched by definition and no index prefix can reduce that. Adding `parked`
		// only moves it into the Index Cond.
		rows, err := db.QueryContext(ctx,
			"SELECT priority, COUNT(*), DATE_DIFF_MILLIS(NOW_UTC(), MIN(created_at)) FROM dwarf_steps"+
				" WHERE status='"+workflow.StatusPending+"' AND parked=? AND not_before<=NOW_UTC() AND lease_expires<=NOW_UTC() GROUP BY priority",
			parkedNone,
		)
		if err != nil {
			return errors.Trace(err)
		}
		defer rows.Close()
		counts := map[int]int{}
		ages := map[int]int{}
		for rows.Next() {
			var priority, count int
			var ageMs sql.NullFloat64
			err := rows.Scan(&priority, &count, &ageMs)
			if err != nil {
				return errors.Trace(err)
			}
			counts[priority] = count
			if ageMs.Valid {
				ages[priority] = int(ageMs.Float64 / 1000)
			}
		}
		err = rows.Err()
		if err != nil {
			return errors.Trace(err)
		}
		pendingPerShard[pos[shard]] = counts
		agePerShard[pos[shard]] = ages
		return nil
	})
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	countByBand = map[int]int{}
	oldestSecByBand = map[int]int{}
	for i := range indices {
		for priority, count := range pendingPerShard[i] {
			countByBand[priority] += count
		}
		for priority, sec := range agePerShard[i] {
			if sec > oldestSecByBand[priority] {
				oldestSecByBand[priority] = sec
			}
		}
	}
	return countByBand, oldestSecByBand, nil
}

// --- Inline counter helpers (no-op until initMetrics has run). ---

// metricFlowStarted counts a flow start against the shard it was PLACED on. Every start path must call it
// (Create, Continue and Fork), or the standard in-flight panel - started minus terminated - drifts by one
// per uncounted flow.
func (e *Engine) metricFlowStarted(ctx context.Context, workflowURL string, shardNum int) {
	e.flowsStartedCount.Add(1)
	if e.metrics == nil {
		return
	}
	e.metrics.flowsStarted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", workflowURL), attribute.String("shard", strconv.Itoa(shardNum))))
}

func (e *Engine) metricFlowTerminated(ctx context.Context, workflowURL, status string, shardNum int) {
	e.flowsTerminatedCount.Add(1)
	if e.metrics == nil {
		return
	}
	e.metrics.flowsTerminated.Add(ctx, 1, metric.WithAttributes(
		attribute.String("workflow", workflowURL), attribute.String("status", status),
		attribute.String("shard", strconv.Itoa(shardNum))))
}

// The `column` attribute on the byte counters is the dwarf_steps column the bytes moved through -
// "state" (snapshots), "changes" (task output deltas), "resume_data", "interrupt_payload",
// "subgraph_result" - bounded cardinality, and lets a chart split task data from engine snapshots from
// human-in-the-loop payloads. Sum across the attribute for the total.

func (e *Engine) metricStateWriteBytes(ctx context.Context, workflowURL, column string, n int) {
	if e.metrics == nil || n <= 0 {
		return
	}
	e.metrics.stateWriteBytes.Add(ctx, int64(n), metric.WithAttributes(
		attribute.String("workflow", workflowURL), attribute.String("column", column)))
}

func (e *Engine) metricStateReadBytes(ctx context.Context, workflowURL, column string, n int) {
	if e.metrics == nil || n <= 0 {
		return
	}
	e.metrics.stateReadBytes.Add(ctx, int64(n), metric.WithAttributes(
		attribute.String("workflow", workflowURL), attribute.String("column", column)))
}

// metricStepOffered counts a candidate the doorbell admitted to the local cache.
func (e *Engine) metricStepOffered(ctx context.Context) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsOffered.Add(ctx, 1)
}

func (e *Engine) metricStepExecuted(ctx context.Context, taskName, status string, shardNum int) {
	if e.metrics == nil {
		return
	}
	// Step disposition keys by node name (graph topology - "which node"), unlike the concurrency/saturation
	// metric which keys by task_url (the downstream endpoint). The shard is the third axis and the one that
	// makes per-shard dispatch rate readable: a piston, a pool and a residue class are all per shard, so a
	// fleet-wide total cannot show one shard falling behind the others.
	e.metrics.stepsExecuted.Add(ctx, 1, metric.WithAttributes(
		attribute.String("task_name", taskName), attribute.String("status", status),
		attribute.String("shard", strconv.Itoa(shardNum))))
}

// metricPeerCountChanged records that one shard's observed replica count moved. Deliberately driven from
// the reconcile loop rather than from recomputePools: that one early-returns under a SetMaxOpenConns
// override (pools are pinned, so there is nothing to re-derive), which is exactly the configuration a
// benchmark runs - and a churn counter that reads zero because nobody looked is worse than none at all.
func (e *Engine) metricPeerCountChanged(ctx context.Context, shardNum, from, to int) {
	e.logger.InfoContext(ctx, "Fleet size changed", "shard", shardNum, "from", from, "to", to)
	if e.metrics == nil {
		return
	}
	e.metrics.peerChanges.Add(ctx, 1, metric.WithAttributes(attribute.String("shard", strconv.Itoa(shardNum))))
}

func (e *Engine) metricStepsRecovered(ctx context.Context, n int) {
	if e.metrics == nil || n <= 0 {
		return
	}
	e.metrics.stepsRecovered.Add(ctx, int64(n))
}

// metricStepWriteRetried counts a retry of the WRITE, never of the task. It is the signal that the database is
// flaky; it does not imply anything went wrong with the workflow, and a run in which it rises but
// dwarf_steps_write_failed stays zero is one where every blip was absorbed with no re-execution.
func (e *Engine) metricStepWriteRetried(ctx context.Context, shardNum int) {
	if e.metrics == nil {
		return
	}
	// STRING, not Int - see shardAttr's note in internal/piston. Every `shard` attribute the engine emits
	// must be the same OTLP type or a backend that distinguishes them cannot group across them.
	e.metrics.stepsWriteRetried.Add(ctx, 1, metric.WithAttributes(attribute.String("shard", strconv.Itoa(shardNum))))
}

// metricStepWriteFailed is an alarm, not a statistic: the engine could reach the database and still could not
// store the step's outcome, so the payload is at fault and the failure is permanent.
func (e *Engine) metricStepWriteFailed(ctx context.Context, taskName string) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsWriteFailed.Add(ctx, 1, metric.WithAttributes(attribute.String("task_name", taskName)))
}

// metricStepClaimLost records a claim CAS that matched no row. Unlabelled: a lost claim knows only the
// step id it failed on - the task name lives in the row it did not get to read - and the useful reading
// is fleet-wide anyway (the ratio against dwarf_steps_executed).
func (e *Engine) metricStepClaimLost(ctx context.Context) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsClaimLost.Add(ctx, 1)
}

// metricStepClaimPreempted records a candidate dropped before its claim, the saving that pairs with
// metricStepClaimLost. Unlabelled for the same reason: the useful reading is the fleet-wide rate.
func (e *Engine) metricStepClaimPreempted(ctx context.Context) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsClaimPreempted.Add(ctx, 1)
}

func (e *Engine) metricStepUnwedged(ctx context.Context, parkType string) {
	if e.metrics == nil {
		return
	}
	e.metrics.stepsUnwedged.Add(ctx, 1, metric.WithAttributes(attribute.String("park_type", parkType)))
}

// metricOrphanedFlow is a detection-only alarm, not a statistic: the orphan sweep deliberately never re-drives
// the flow (re-driving would duplicate transition-evaluation logic and a false positive could double-advance
// it), so a nonzero value is a residual bug the recovery defer's own reset could not cover, surfaced for an
// operator alongside the error log. Labelled by workflow, like the other flow counters.
func (e *Engine) metricOrphanedFlow(ctx context.Context, workflowURL string) {
	if e.metrics == nil {
		return
	}
	e.metrics.flowsOrphaned.Add(ctx, 1, metric.WithAttributes(attribute.String("workflow", workflowURL)))
}
