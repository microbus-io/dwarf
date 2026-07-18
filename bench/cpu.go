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
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// The host-resource sampler answers the question the connection/throughput numbers cannot: what does a
// given throughput COST the engine host, in CPU cores and in network bandwidth? That is what converts a
// steps/s figure into an engine-vCPU : database-vCPU sizing ratio, and it is what tells you whether a
// plateau is the database's knee, the engine's CPU, or the NIC.
//
// Two caveats to read every number with:
//   - The bench process is BOTH the engine and the closed-loop load generator, so CPUCores includes the
//     submitters' own cost (Create/Await bookkeeping), not just engine work. It is an upper bound on the
//     engine's share.
//   - Network counters are host-wide (all interfaces except loopback), so they include SSH and any other
//     traffic. On a dedicated bench VM that is negligible next to the database traffic.

// hostSample is one point-in-time reading of the process CPU clock and host network counters.
type hostSample struct {
	at      time.Time
	cpuNano int64 // process user+system CPU consumed since start
	rxBytes int64
	txBytes int64
}

// hostUsage is the delta between two hostSamples, normalized to rates.
type hostUsage struct {
	// CPUCores is the average number of CPU cores fully busy over the interval (2.0 = two cores' worth).
	CPUCores float64 `json:"cpuCores"`
	// CPUPct is CPUCores as a percentage of every core the host has - the saturation signal.
	CPUPct float64 `json:"cpuPct"`
	// StepsPerCore is the headline sizing number when paired with a throughput: how much work one engine
	// core drives. Filled in by the caller, which knows the step rate.
	StepsPerCore float64 `json:"stepsPerCore,omitzero"`
	NetRxMBps    float64 `json:"netRxMBps"`
	NetTxMBps    float64 `json:"netTxMBps"`
	NumCPU       int     `json:"numCPU"`
}

// sampleHost reads the process CPU clock (portable via getrusage) and the host's network counters
// (Linux /proc/net/dev; zero elsewhere, which is fine - the sizing runs happen on Linux VMs).
func sampleHost() hostSample {
	s := hostSample{at: time.Now()}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err == nil {
		s.cpuNano = ru.Utime.Nano() + ru.Stime.Nano()
	}
	s.rxBytes, s.txBytes = readNetCounters()
	return s
}

// usageSince converts two samples into rates.
func usageSince(before, after hostSample) hostUsage {
	secs := after.at.Sub(before.at).Seconds()
	if secs <= 0 {
		return hostUsage{NumCPU: runtime.NumCPU()}
	}
	cores := float64(after.cpuNano-before.cpuNano) / 1e9 / secs
	return hostUsage{
		CPUCores:  cores,
		CPUPct:    cores / float64(runtime.NumCPU()) * 100,
		NetRxMBps: float64(after.rxBytes-before.rxBytes) / (1 << 20) / secs,
		NetTxMBps: float64(after.txBytes-before.txBytes) / (1 << 20) / secs,
		NumCPU:    runtime.NumCPU(),
	}
}

// readNetCounters sums received/transmitted bytes across every non-loopback interface. Returns zeros
// where /proc/net/dev does not exist (macOS), so the sampler degrades to CPU-only rather than failing.
func readNetCounters() (rx, tx int64) {
	data, err := os.ReadFile("/proc/net/dev")
	if err != nil {
		return 0, 0
	}
	for line := range strings.SplitSeq(string(data), "\n") {
		name, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue // header lines
		}
		if strings.TrimSpace(name) == "lo" {
			continue
		}
		// Columns after the colon: rx_bytes ... (1st) and tx_bytes (9th).
		f := strings.Fields(rest)
		if len(f) < 9 {
			continue
		}
		if v, err := strconv.ParseInt(f[0], 10, 64); err == nil {
			rx += v
		}
		if v, err := strconv.ParseInt(f[8], 10, 64); err == nil {
			tx += v
		}
	}
	return rx, tx
}
