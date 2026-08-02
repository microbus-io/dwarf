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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/microbus-io/errors"
)

// startProfiling begins a CPU profile and returns the func that ends it and writes a heap profile beside it.
//
// WHAT IT IS FOR, and the caveat that decides how to run it. The headline cost this is meant to attribute -
// microseconds of engine CPU per step rising as the crew grows - is a whole-PROCESS number, and this process
// also contains the load generator. A profile is the only thing that separates them, so it answers a
// question no counter in the artifact can. But it covers the whole run, every -concurrency step included, so
// an attribution run should pass ONE concurrency value: profiling a sweep averages arms that differ in the
// quantity under test and produces a profile of nothing in particular.
//
// The heap profile is written after a GC so it reports live bytes rather than whatever had not been
// collected yet - the crew's stacks are the point, and stacks are not heap, so read it against the runtime's
// own goroutine count rather than expecting it to explain the whole RSS.
func startProfiling(dir, label string) (stop func(), err error) {
	if dir == "" {
		return func() {}, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, errors.Trace(err)
	}
	name := profileName(label)
	cpuPath := filepath.Join(dir, "cpu-"+name+".pprof")
	f, err := os.Create(cpuPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if err := pprof.StartCPUProfile(f); err != nil {
		f.Close()
		return nil, errors.Trace(err)
	}
	// Failures WARN rather than returning quietly. That is this harness's own most expensive lesson: metric
	// collection used to fail silently, which turned a corrupt run into a confident measurement of zero
	// rather than into an error. A missing profile is far less dangerous than that - nothing downstream
	// averages it in - but the failure mode is the same shape, and silence costs whoever goes looking for
	// the file an hour of confusion at the end of a run that is expensive to repeat.
	return func() {
		pprof.StopCPUProfile()
		if cerr := f.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "WARNING: cpu profile %s may be truncated: %v\n", cpuPath, cerr)
		}
		heapPath := filepath.Join(dir, "heap-"+name+".pprof")
		hf, herr := os.Create(heapPath)
		if herr != nil {
			fmt.Fprintf(os.Stderr, "WARNING: heap profile not written: %v\n", herr)
			return
		}
		defer hf.Close()
		runtime.GC() // live heap, not "whatever had not been collected"
		if werr := pprof.WriteHeapProfile(hf); werr != nil {
			fmt.Fprintf(os.Stderr, "WARNING: heap profile %s incomplete: %v\n", heapPath, werr)
		}
	}, nil
}

// profileName turns a free-form run label into something safe to put in a filename, falling back to a
// timestamp so two unlabelled runs in one directory cannot overwrite each other.
func profileName(label string) string {
	clean := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '-'
		}
	}, label)
	clean = strings.Trim(clean, "-")
	if clean == "" {
		return time.Now().UTC().Format("20060102-150405")
	}
	return clean
}

// statsHeader and statsLine are the periodic readout during a measurement window.
//
// CREW is the column this exists for. A burst arm's whole question is whether the crew comes back down once
// tasks stop being long, and that is a SHAPE over time - invisible to the window-edge snapshots and to the
// window mean alike, both of which would report a burst arm as something in between. delayMs beside it is
// what says which half of the cycle a line belongs to, so the shape can be read against its cause without
// counting off the schedule by hand.
//
// dwarf_workers_resident rather than runtime.NumGoroutine: the process also holds the load generator's
// submitters, which is a rounding error against 36,000 workers and most of the count against 800.
func statsHeader() string {
	return fmt.Sprintf("%-7s %8s %7s %8s %8s %9s %9s %9s",
		"t(s)", "delayMs", "crew", "gorout", "heapMB", "turnAvail", "inflMB", "pending")
}

func statsLine(elapsed time.Duration, p taskProfile, g map[string]float64, heapMB float64) string {
	return fmt.Sprintf("%-7.0f %8d %7.0f %8d %8.0f %9.0f %9.1f %9.0f",
		elapsed.Seconds(), p.delayAt(time.Now()).Milliseconds(),
		gaugeTotal(g, "dwarf_workers_resident"), runtime.NumGoroutine(), heapMB,
		gaugeTotal(g, "dwarf_turnstile_available"),
		gaugeTotal(g, "dwarf_state_in_flight_bytes")/(1<<20),
		gaugeTotal(g, "dwarf_steps_pending"))
}

// gaugeTotal sums every attributed series of one instrument.
//
// READING THE BARE NAME GETS ZERO for anything attributed, which is not obvious and is exactly what the
// first cut of the line above did: gauges are recorded per attribute set - dwarf_turnstile_available|shard=1,
// dwarf_steps_pending|priority=100 - with NO bare-name total, unlike counters, which emit one alongside the
// split. Three of the five gauge columns printed a confident 0.0 for a run that was executing hundreds of
// steps a second. Summing is the right total for both shapes present: free turns across shards, and backlog
// across priority bands.
//
// Matching on name+"|" as well as the exact name is what keeps dwarf_steps_pending from also collecting a
// future dwarf_steps_pending_anything.
func gaugeTotal(g map[string]float64, name string) float64 {
	total := 0.0
	for k, v := range g {
		if k == name || strings.HasPrefix(k, name+"|") {
			total += v
		}
	}
	return total
}

// tracing is the periodic-readout configuration, set once at startup and read by the window functions. A
// package-level rather than two more parameters on runStep, whose signature already carries ten and none of
// which this affects.
var tracing struct {
	every   time.Duration
	profile taskProfile
}

// statsTrace is the gauge sampler's callback for the periodic line, or nil when the readout is off. It
// prints the header as a side effect, so each window's lines carry their own.
func statsTrace() func(time.Duration, map[string]float64, float64) {
	if tracing.every <= 0 {
		return nil
	}
	fmt.Println(statsHeader())
	return func(elapsed time.Duration, g map[string]float64, heapMB float64) {
		fmt.Println(statsLine(elapsed, tracing.profile, g, heapMB))
	}
}
