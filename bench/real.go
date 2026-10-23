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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/microbus-io/dwarf/engine"
	"github.com/microbus-io/dwarf/workflow"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// The real mode runs a day on one fleet: a repeating schedule of offered load with ramps, bursts and noise,
// over a mix of flow classes, priority bands and tenants, for hours. Every other mode holds one workload at
// one rate; this one asks whether a database serves a day repeatedly - bursts past capacity drained, latency
// back to its level, nothing accumulating from one day to the next.
//
// The schedule is a pure function of the time since -arrival-epoch, which every process of a fleet is given,
// so replicas in separate processes, and a process restarted mid-run, offer their share in the same phase.
// The run ends at the epoch plus -real, so a restarted process runs only what is left.
//
// The record is a time series, one JSON line per interval written as it goes, so a process killed mid-run
// still leaves everything up to its last interval. A process counts only what it created and what its own
// engines ended, so fleet totals are the sum of the processes' series, and whether every flow ended is read
// from the database, not from here.

// arrivalSegment is one piece of the schedule: a level held, or a linear ramp from one level to another.
type arrivalSegment struct {
	dur      time.Duration
	from, to float64 // steps/s
}

// arrivalBurst replaces the schedule's level with its own for dur, starting at offset into each period. A burst
// steps up and down at once, with no ramp: it is a surprise.
type arrivalBurst struct {
	offset, dur time.Duration
	level       float64
}

// arrivalProfile is the offered load, in steps/s across the whole fleet, as a function of the time since the
// epoch.
type arrivalProfile struct {
	segments []arrivalSegment
	bursts   []arrivalBurst
	period   time.Duration
	noise    float64 // each minute's level is scaled by a factor in [1-noise, 1+noise]
	seed     uint64
}

// parseArrivalProfile reads -arrival-profile ("25m:8000,10m:8000-12000,40m:12000,15m:12000-8000": a duration and
// a level, or a ramp between two) and -arrival-bursts ("10m+2m:16000,58m+5m:16000": an offset into the period, a
// duration and a level).
func parseArrivalProfile(segments, bursts string, noise float64, seed uint64) (*arrivalProfile, error) {
	p := &arrivalProfile{noise: noise, seed: seed}
	for entry := range strings.SplitSeq(segments, ",") {
		durS, levelS, ok := strings.Cut(strings.TrimSpace(entry), ":")
		dur, err := time.ParseDuration(durS)
		if !ok || err != nil || dur <= 0 {
			return nil, fmt.Errorf("-arrival-profile: %q is not duration:level or duration:from-to", entry)
		}
		fromS, toS, ramp := strings.Cut(levelS, "-")
		from, err1 := strconv.ParseFloat(fromS, 64)
		to := from
		var err2 error
		if ramp {
			to, err2 = strconv.ParseFloat(toS, 64)
		}
		if err1 != nil || err2 != nil || from < 0 || to < 0 {
			return nil, fmt.Errorf("-arrival-profile: %q has no valid level", entry)
		}
		p.segments = append(p.segments, arrivalSegment{dur: dur, from: from, to: to})
		p.period += dur
	}
	if bursts != "" {
		for entry := range strings.SplitSeq(bursts, ",") {
			when, levelS, ok := strings.Cut(strings.TrimSpace(entry), ":")
			offS, durS, ok2 := strings.Cut(when, "+")
			off, err1 := time.ParseDuration(offS)
			dur, err2 := time.ParseDuration(durS)
			level, err3 := strconv.ParseFloat(levelS, 64)
			if !ok || !ok2 || err1 != nil || err2 != nil || err3 != nil || off < 0 || dur <= 0 || level < 0 || off+dur > p.period {
				return nil, fmt.Errorf("-arrival-bursts: %q is not offset+duration:level inside the %s period", entry, p.period)
			}
			p.bursts = append(p.bursts, arrivalBurst{offset: off, dur: dur, level: level})
		}
	}
	if noise < 0 || noise >= 1 {
		return nil, fmt.Errorf("-arrival-noise %g must be in [0, 1)", noise)
	}
	return p, nil
}

// phase is where t falls in the period, and which period it is (0-based).
func (p *arrivalProfile) phase(t time.Duration) (time.Duration, int) {
	if t < 0 {
		t = 0
	}
	return t % p.period, int(t / p.period)
}

// rate is the offered steps/s at t since the epoch, noise included, and whether a burst is on.
func (p *arrivalProfile) rate(t time.Duration) (float64, bool) {
	ph, _ := p.phase(t)
	level, burst := p.base(ph)
	if p.noise > 0 {
		// Keyed on the minute of the PERIOD, not of the run, so every period offers the same load and one day
		// can be compared with the next.
		h := fnv.New64a()
		fmt.Fprintf(h, "%d/%d", p.seed, int(ph/time.Minute))
		u := float64(h.Sum64()>>11) / float64(1<<53)
		level *= 1 + p.noise*(2*u-1)
	}
	return level, burst
}

// base is the level at ph into the period before noise.
func (p *arrivalProfile) base(ph time.Duration) (float64, bool) {
	for _, b := range p.bursts {
		if ph >= b.offset && ph < b.offset+b.dur {
			return b.level, true
		}
	}
	at := time.Duration(0)
	for _, s := range p.segments {
		if ph < at+s.dur {
			f := float64(ph-at) / float64(s.dur)
			return s.from + (s.to-s.from)*f, false
		}
		at += s.dur
	}
	last := p.segments[len(p.segments)-1]
	return last.to, false
}

// poisson draws a Poisson count with mean lambda (Knuth's method below 30, a rounded normal above, where the
// two agree to well under a percent).
func poisson(r *rand.Rand, lambda float64) int {
	if lambda <= 0 {
		return 0
	}
	if lambda >= 30 {
		return max(0, int(math.Round(lambda+math.Sqrt(lambda)*r.NormFloat64())))
	}
	limit, k, prod := math.Exp(-lambda), 0, r.Float64()
	for prod > limit {
		k++
		prod *= r.Float64()
	}
	return k
}

// realConfig is everything the real mode takes from the flags.
type realConfig struct {
	profile            *arrivalProfile
	epoch              time.Time
	until              time.Time
	share              float64 // this process's share of the fleet's offered load
	meanSteps          float64 // the mix's steps per flow
	creators           int
	maxOutstanding     int
	fairnessKeys       int
	fairnessSkew       float64       // Zipf exponent over the keys; 0 = uniform
	priorities         priorityMix   // the share of flows created at each priority level
	retention          time.Duration // Purge completed flows older than this; 0 = keep everything
	deleteOnCompletion bool          // create every flow with DeleteOnCompletion
	interval           time.Duration
	seriesPath         string
	// classOf names the class each workflow URL runs, and stepsOf its steps per flow, so the engine's flow counters
	// (labelled by workflow) give each class's rate.
	classOf map[string]string
	stepsOf map[string]int
}

// realPoint is one interval of the series. Rates are this process's; a fleet's are the sum over its processes.
type realPoint struct {
	T          float64 `json:"t"` // seconds since the epoch, at the interval's end
	Cycle      int     `json:"cycle"`
	PhaseMin   float64 `json:"phaseMin"`
	Burst      bool    `json:"burst"`
	Commanded  float64 `json:"commandedFlowsPerSec"` // what the schedule asked of this process
	Offered    float64 `json:"offeredFlowsPerSec"`   // Poisson arrivals drawn
	Admitted   float64 `json:"admittedFlowsPerSec"`  // flows Create accepted
	Dropped    int64   `json:"dropped"`              // arrivals no creator took in time, counted not created
	Throttled  int64   `json:"throttledMs"`          // creator time parked on -max-outstanding (should stay 0)
	Steps      float64 `json:"stepsPerSec"`          // executed by this process's engines
	Ended      float64 `json:"endedFlowsPerSec"`
	Out        int64   `json:"outstanding"`
	Errors     int64   `json:"errors"`
	Purged     int     `json:"purged,omitzero"`
	Recovered  int64   `json:"recovered,omitzero"`
	PeerChange int64   `json:"peerChanges,omitzero"`
	Crew       float64 `json:"crew"`
	ConnsInUse float64 `json:"connsInUse"`
	HeapMB     float64 `json:"heapMB"`
	CPUCores   float64 `json:"cpuCores"` // this process, engine and load generator together
	// Rates by class ("class:linear:1K") and by priority band ("band:2"), flows/s. Admitted counts the flows this process
	// created; ended counts the flows its engines ended, from the engine's counters, and steps is ended x the
	// class's steps per flow (the engine counts executed steps by task, which the classes share).
	AdmittedBy map[string]float64 `json:"admittedBy"`
	EndedBy    map[string]float64 `json:"endedBy"`
	StepsBy    map[string]float64 `json:"stepsBy"`
	// The backlog by band, from the engine's gauges: steps pending and the oldest one's age. Only the shard's
	// backlog leader reports them, refreshed every 30 s, so a fleet's reading is the max over its processes.
	Pending      map[string]float64    `json:"pending,omitempty"`
	OldestSec    map[string]float64    `json:"oldestPendingSec,omitempty"`
	DispatchLag  float64               `json:"dispatchLagSec"`
	RefillPhases map[string]refillStat `json:"refill,omitempty"` // the background queries, by phase
	// Latency of the flows that COMPLETED in the interval, by class, band and tenant: count, p50 and p99 in ms.
	Latency map[string]latencyStat `json:"latency"`
	// Create is the admission latency of every Create in the interval.
	Create latencyStat `json:"create"`
	// LatencyHist holds the same samples, Create's under "create", as counts per log-spaced bucket
	// (latencyBucket): percentiles do not add up across intervals or replicas, and these do.
	LatencyHist map[string]map[string]int `json:"latencyHist"`
}

// refillStat is one refill phase's calls in an interval and their mean duration.
type refillStat struct {
	N      uint64  `json:"n"`
	MeanMs float64 `json:"meanMs"`
}

// latencyBucket is the histogram bucket of a latency in ms: bucket b holds [1.1^b, 1.1^(b+1)) ms, so neighbours
// are 10% apart and a percentile read back from merged buckets is within 10%.
func latencyBucket(ms float64) int {
	return int(math.Floor(math.Log(max(ms, 1e-3)) / math.Log(1.1)))
}

// splitKey parses an instrument key ("name|k=v|k=v") into its name and attributes.
func splitKey(key string) (string, map[string]string) {
	parts := strings.Split(key, "|")
	attrs := make(map[string]string, len(parts)-1)
	for _, kv := range parts[1:] {
		if k, v, ok := strings.Cut(kv, "="); ok {
			attrs[k] = v
		}
	}
	return parts[0], attrs
}

type latencyStat struct {
	N   int     `json:"n"`
	P50 float64 `json:"p50"`
	P99 float64 `json:"p99"`
}

// realReport is the real mode's artifact section: the totals; the series is in its own file.
type realReport struct {
	SeriesPath  string  `json:"seriesPath"`
	Intervals   int     `json:"intervals"`
	Created     int64   `json:"created"`
	Errors      int64   `json:"errors"`
	Dropped     int64   `json:"dropped"`
	ThrottledMs int64   `json:"throttledMs"`
	Purged      int64   `json:"purged"`
	Signalled   bool    `json:"signalled,omitzero"` // stopped by SIGTERM/SIGINT, as a rolling restart does
	Stalled     string  `json:"stalled,omitzero"`
	DurationSec float64 `json:"durationSec"`
	// DrainSec is how long the fleet took, after the schedule ended, to leave no flow running on the shard;
	// DrainGaveUp says nothing moved for -drain first. Neither is set for a process stopped by a signal, which
	// leaves its flows to the replicas that stay.
	DrainSec    float64 `json:"drainSec,omitzero"`
	DrainGaveUp bool    `json:"drainGaveUp,omitzero"`
}

// runReal runs the schedule until cfg.until or until ctx ends (a signal), then stops creating. At the schedule's
// end it keeps its engines running until no flow is left running on the shard, so the fleet finishes what it
// admitted and the database can be checked for every flow ended. A process stopped by a signal does not wait: a
// rolling restart leaves its flows to the replicas that stay.
func runReal(ctx context.Context, engines []*engine.Engine, readers []*sdkmetric.ManualReader, pick func() *workload,
	cfg realConfig) (*realReport, error) {

	series, err := os.OpenFile(cfg.seriesPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	defer series.Close()
	enc := json.NewEncoder(series)

	ended := func() int64 {
		var n int64
		for _, e := range engines {
			n += e.FlowsEnded()
		}
		return n
	}
	baseEnded := ended()
	var (
		created, errCount, dropped, throttled, offered atomic.Int64
		commanded                                      atomic.Uint64 // math.Float64bits of the expected arrivals
		purged                                         atomic.Int64
		latMu                                          sync.Mutex
		lat                                            = map[string][]float64{}
		admitted                                       = map[string]int64{}
		createLat                                      []float64
		wg                                             sync.WaitGroup
	)
	outstanding := func() int64 { return created.Load() - (ended() - baseEnded) }
	addCommanded := func(v float64) {
		for {
			old := commanded.Load()
			if commanded.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
				return
			}
		}
	}
	record := func(keys []string, ms float64) {
		latMu.Lock()
		for _, k := range keys {
			lat[k] = append(lat[k], ms)
		}
		latMu.Unlock()
	}

	runCtx, stopRun := context.WithDeadline(ctx, cfg.until)
	defer stopRun()
	watch := watchProgress(runCtx, engines, readers, outstanding)
	defer watch.stop()
	go func() {
		select {
		case <-watch.stalled:
			stopRun()
		case <-runCtx.Done():
		}
	}()

	flowsPerSec := func(now time.Time) (float64, bool) {
		steps, burst := cfg.profile.rate(now.Sub(cfg.epoch))
		return steps * cfg.share / cfg.meanSteps, burst
	}

	// Arrivals: a Poisson count per 10ms bin at the schedule's current rate, handed to the creators as tokens. A
	// token no creator takes before the buffer fills is dropped and counted, so admission falling short of the
	// schedule shows rather than queueing without bound.
	tokens := make(chan struct{}, 1<<16)
	wg.Go(func() {
		r := rand.New(rand.NewPCG(cfg.profile.seed, uint64(time.Now().UnixNano())))
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		last := time.Now()
		for {
			select {
			case <-runCtx.Done():
				close(tokens)
				return
			case now := <-tick.C:
				rate, _ := flowsPerSec(now)
				expect := rate * now.Sub(last).Seconds()
				last = now
				addCommanded(expect)
				n := poisson(r, expect)
				offered.Add(int64(n))
				for range n {
					select {
					case tokens <- struct{}{}:
					default:
						dropped.Add(1)
					}
				}
			}
		}
	})

	// Await samples: enough per class for a p99 per interval, bounded so sampling cannot storm the database.
	sem := make(chan struct{}, 2048)
	awaitCtx, cancelAwaits := context.WithCancel(context.Background())
	defer cancelAwaits()

	for i := range cfg.creators {
		eng := engines[i%len(engines)]
		wg.Go(func() {
			r := rand.New(rand.NewPCG(cfg.profile.seed+uint64(i)+1, uint64(time.Now().UnixNano())))
			var zipf *rand.Zipf
			if cfg.fairnessKeys > 1 && cfg.fairnessSkew > 1 {
				zipf = rand.NewZipf(r, cfg.fairnessSkew, 1, uint64(cfg.fairnessKeys-1))
			}
			for range tokens {
				for outstanding() >= int64(cfg.maxOutstanding) && runCtx.Err() == nil {
					time.Sleep(time.Millisecond)
					throttled.Add(1)
				}
				w := pick()
				opts := &workflow.FlowOptions{DeleteOnCompletion: cfg.deleteOnCompletion}
				opts.Priority = cfg.priorities.sample(r.Float64())
				band := bandLabel(opts.Priority)
				tenant := "tenant:tail"
				if cfg.fairnessKeys > 1 {
					k := 0
					if zipf != nil {
						k = int(zipf.Uint64())
					} else {
						k = r.IntN(cfg.fairnessKeys)
					}
					opts.FairnessKey = fmt.Sprintf("k%d", k)
					if k == 0 {
						tenant = "tenant:top"
					}
				}
				born := time.Now()
				key, err := eng.Create(runCtx, w.graphURL, w.initialState(), opts)
				if err != nil {
					if runCtx.Err() == nil {
						errCount.Add(1)
					}
					continue
				}
				created.Add(1)
				latMu.Lock()
				createLat = append(createLat, float64(time.Since(born))/float64(time.Millisecond))
				admitted["class:"+w.class]++
				admitted[band]++
				latMu.Unlock()
				// Sample one flow in sampleEvery of its class, at the class's current rate, so a rare class is
				// sampled whole and a common one at ~20 samples a second.
				rate, _ := flowsPerSec(born)
				classRate := rate * classShare(w)
				if classRate > 20 && r.Float64() > 20/classRate {
					continue
				}
				select {
				case sem <- struct{}{}:
				default:
					continue
				}
				wg.Go(func() {
					defer func() { <-sem }()
					oc, aerr := eng.Await(awaitCtx, key)
					if aerr != nil {
						return
					}
					if oc.Status != workflow.StatusCompleted {
						errCount.Add(1)
						return
					}
					if w.check != nil && w.check(oc) != nil {
						errCount.Add(1)
						return
					}
					record([]string{"class:" + w.class, band, tenant, "all"}, float64(time.Since(born))/float64(time.Millisecond))
				})
			}
		})
	}

	// Retention: the periodic Purge a production deployment runs, so the step table reaches a steady size.
	if cfg.retention > 0 {
		wg.Go(func() {
			tick := time.NewTicker(time.Minute)
			defer tick.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-tick.C:
				}
				for runCtx.Err() == nil {
					n, err := engines[0].Purge(runCtx, workflow.Query{Status: workflow.StatusCompleted, OlderThan: cfg.retention, Limit: 4096})
					purged.Add(int64(n))
					if err != nil || n < 4096 {
						break
					}
				}
			}
		})
	}

	report := &realReport{SeriesPath: cfg.seriesPath}
	start := time.Now()
	prevCounters := collectAllCounters(readers)
	prevHists, _ := collectAllHistograms(readers)
	prevHost := sampleHost()
	prevT, prevCreated, prevEnded, prevOffered, prevDropped, prevThrottled, prevErr, prevPurged :=
		start, int64(0), ended(), int64(0), int64(0), int64(0), int64(0), int64(0)
	prevCommanded := 0.0
	tick := time.NewTicker(cfg.interval)
	defer tick.Stop()
	fmt.Printf("%-8s %5s %7s %9s %9s %9s %9s %9s %8s %9s %9s\n",
		"t(s)", "cycle", "phase", "command/s", "admit/s", "steps/s", "out", "p50ms", "p99ms", "topP99ms", "dropped")
	for done := false; !done; {
		select {
		case <-runCtx.Done():
			done = true
		case <-tick.C:
		}
		now := time.Now()
		sec := now.Sub(prevT).Seconds()
		if sec <= 0 {
			continue
		}
		counters := collectAllCounters(readers)
		hists, histsOK := collectAllHistograms(readers)
		gauges := collectAllGauges(readers)
		host := sampleHost()
		latMu.Lock()
		window, creates, adm := lat, createLat, admitted
		lat, createLat, admitted = map[string][]float64{}, nil, map[string]int64{}
		latMu.Unlock()
		ph, cycle := cfg.profile.phase(now.Sub(cfg.epoch))
		_, burst := flowsPerSec(now)
		cmd := math.Float64frombits(commanded.Load())
		cr, en, of, dr, th, er, pu := created.Load(), ended(), offered.Load(), dropped.Load(), throttled.Load(), errCount.Load(), purged.Load()
		p := realPoint{
			T: now.Sub(cfg.epoch).Seconds(), Cycle: cycle, PhaseMin: ph.Minutes(), Burst: burst,
			Commanded: (cmd - prevCommanded) / sec, Offered: float64(of-prevOffered) / sec,
			Admitted: float64(cr-prevCreated) / sec, Dropped: dr - prevDropped, Throttled: th - prevThrottled,
			Ended: float64(en-prevEnded) / sec, Out: outstanding(), Errors: er - prevErr, Purged: int(pu - prevPurged),
			HeapMB: float64(host.heapAlloc) / (1 << 20), CPUCores: usageSince(prevHost, host).CPUCores,
			AdmittedBy: map[string]float64{}, EndedBy: map[string]float64{}, StepsBy: map[string]float64{},
			Latency: map[string]latencyStat{}, LatencyHist: map[string]map[string]int{},
		}
		for k, n := range adm {
			p.AdmittedBy[k] = float64(n) / sec
		}
		band := func(attrs map[string]string) string {
			return "band:" + attrs["priority"]
		}
		if counters != nil && prevCounters != nil {
			d := counterDeltas(prevCounters, counters)
			p.Steps = float64(d["dwarf_steps_executed"]) / sec
			p.Recovered = d["dwarf_steps_recovered"]
			p.PeerChange = d["dwarf_peer_changes"]
			for key, n := range d {
				name, attrs := splitKey(key)
				if name != "dwarf_flows_ended" || len(attrs) == 0 {
					continue
				}
				p.EndedBy[band(attrs)] += float64(n) / sec
				if c, ok := cfg.classOf[attrs["workflow"]]; ok {
					p.EndedBy["class:"+c] += float64(n) / sec
					p.StepsBy["class:"+c] += float64(n*int64(cfg.stepsOf[attrs["workflow"]])) / sec
				}
			}
		}
		for key, v := range gauges {
			name, attrs := splitKey(key)
			switch name {
			case "dwarf_steps_pending":
				if p.Pending == nil {
					p.Pending = map[string]float64{}
				}
				p.Pending[band(attrs)] += v
			case "dwarf_steps_oldest_pending_age_seconds":
				if p.OldestSec == nil {
					p.OldestSec = map[string]float64{}
				}
				p.OldestSec[band(attrs)] = max(p.OldestSec[band(attrs)], v)
			case "dwarf_steps_dispatch_lag_seconds":
				p.DispatchLag = max(p.DispatchLag, v)
			}
		}
		if histsOK && prevHists != nil {
			sums := map[string]float64{}
			for _, h := range histogramDeltas(prevHists, hists) {
				if h.Name != "dwarf_refill_query_duration_seconds" || h.Count == 0 {
					continue
				}
				if p.RefillPhases == nil {
					p.RefillPhases = map[string]refillStat{}
				}
				st := p.RefillPhases[h.Attrs["phase"]]
				st.N += h.Count
				p.RefillPhases[h.Attrs["phase"]] = st
				sums[h.Attrs["phase"]] += h.SumSeconds
			}
			for ph, st := range p.RefillPhases {
				st.MeanMs = sums[ph] / float64(st.N) * 1000
				p.RefillPhases[ph] = st
			}
		}
		p.Crew = gaugeTotal(gauges, "dwarf_workers_resident")
		p.ConnsInUse = gaugeTotal(gauges, "sequel_pool_in_use_connections")
		window["create"] = creates
		for k, v := range window {
			slices.Sort(v)
			st := latencyStat{N: len(v), P50: quantile(v, 0.50), P99: quantile(v, 0.99)}
			if k == "create" {
				p.Create = st
			} else {
				p.Latency[k] = st
			}
			hist := map[string]int{}
			for _, ms := range v {
				hist[strconv.Itoa(latencyBucket(ms))]++
			}
			if len(hist) > 0 {
				p.LatencyHist[k] = hist
			}
		}
		if err := enc.Encode(p); err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: series line not written: %v\n", err)
		}
		report.Intervals++
		fmt.Printf("%-8.0f %5d %7.1f %9.0f %9.0f %9.0f %9d %9.0f %8.0f %9.0f %9d\n",
			p.T, p.Cycle, p.PhaseMin, p.Commanded*cfg.meanSteps, p.Admitted*cfg.meanSteps, p.Steps, p.Out,
			p.Latency["all"].P50, p.Latency["all"].P99, p.Latency[bandLabel(cfg.priorities[0].priority)].P99, p.Dropped)
		prevCounters, prevT, prevCreated, prevEnded, prevOffered, prevDropped, prevThrottled, prevErr, prevPurged, prevCommanded =
			counters, now, cr, en, of, dr, th, er, pu, cmd
		prevHost = host
		if histsOK {
			prevHists = hists
		}
	}
	stopRun()
	cancelAwaits()
	watch.await(wg.Wait)
	report.Created, report.Errors, report.Dropped = created.Load(), errCount.Load(), dropped.Load()
	report.ThrottledMs, report.Purged = throttled.Load(), purged.Load()
	report.Signalled = ctx.Err() != nil
	report.Stalled = watch.reason()
	report.DurationSec = time.Since(start).Seconds()
	if !report.Signalled && report.Stalled == "" {
		report.DrainSec, report.DrainGaveUp = drainFleet(ctx, engines, guard.drain)
		fmt.Printf("drain: %.0fs, gave up: %v\n", report.DrainSec, report.DrainGaveUp)
	}
	return report, nil
}

// drainFleet waits until no flow is left running on the shard, the fleet's own flows included, and gives up once
// no task has started and no flow has ended for quiet. It asks the database rather than counting: a replica in
// another process ends flows this one created, and this one ends theirs.
func drainFleet(ctx context.Context, engines []*engine.Engine, quiet time.Duration) (sec float64, gaveUp bool) {
	start := time.Now()
	ended := func() int64 {
		var n int64
		for _, e := range engines {
			n += e.FlowsEnded()
		}
		return n
	}
	lastEnded, lastTasks, lastMove := ended(), tasksStarted.Load(), time.Now()
	for ctx.Err() == nil {
		page, _, err := engines[0].List(ctx, workflow.Query{Status: workflow.StatusRunning, Limit: 1})
		if err == nil && len(page) == 0 {
			return time.Since(start).Seconds(), false
		}
		if e, t := ended(), tasksStarted.Load(); e != lastEnded || t != lastTasks {
			lastEnded, lastTasks, lastMove = e, t, time.Now()
		}
		if quiet > 0 && time.Since(lastMove) > quiet {
			return time.Since(start).Seconds(), true
		}
		time.Sleep(2 * time.Second)
	}
	return time.Since(start).Seconds(), false
}

// classShares is each -mix class's share of the flows, set by main before the run; read-only after.
var classShares = map[string]float64{}

// classShare is w's share of the flows, 1 outside a mix.
func classShare(w *workload) float64 {
	if s, ok := classShares[w.class]; ok {
		return s
	}
	return 1
}

// quantile reads the q quantile of sorted values by nearest rank, 0 for none.
func quantile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(q*float64(len(sorted)))) - 1
	return sorted[max(0, min(len(sorted)-1, i))]
}
