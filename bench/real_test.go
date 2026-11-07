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
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/microbus-io/dwarf/workflow"
	"github.com/microbus-io/testarossa"
)

// TestArrivalProfile_Levels pins the schedule: a held level, a linear ramp, a burst that replaces the level
// outright, and the period repeating.
func TestArrivalProfile_Levels(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	p, err := parseArrivalProfile("25m:8000,10m:8000-12000,40m:12000,15m:12000-8000", "10m+2m:16000,58m+5m:16000", 0, 1)
	assert.NoError(err)
	assert.Equal(90*time.Minute, p.period)
	at := func(d time.Duration) float64 { r, _ := p.rate(d); return r }
	assert.Equal(8000.0, at(5*time.Minute))
	assert.Equal(10000.0, at(30*time.Minute), "halfway up the ramp")
	assert.Equal(12000.0, at(50*time.Minute))
	assert.Equal(10000.0, at(82*time.Minute+30*time.Second), "halfway down")
	r, burst := p.rate(11 * time.Minute)
	assert.Equal(16000.0, r)
	assert.True(burst)
	_, burst = p.rate(12 * time.Minute)
	assert.False(burst, "a burst ends at once")
	assert.Equal(at(30*time.Minute), at(120*time.Minute), "the second period repeats the first")
	_, cycle := p.phase(185 * time.Minute)
	assert.Equal(2, cycle)
}

// TestArrivalProfile_Noise pins the noise: inside its bound, the same for every process given the seed, the same
// minute of every period, and different from one minute to the next.
func TestArrivalProfile_Noise(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	p, err := parseArrivalProfile("90m:10000", "", 0.1, 7)
	assert.NoError(err)
	q, _ := parseArrivalProfile("90m:10000", "", 0.1, 7)
	differs := false
	prev := -1.0
	for m := range 90 {
		d := time.Duration(m)*time.Minute + 30*time.Second
		r, _ := p.rate(d)
		assert.True(r >= 9000 && r <= 11000, "minute %d at %g", m, r)
		r2, _ := q.rate(d)
		assert.Equal(r, r2, "the same seed, the same schedule")
		r3, _ := p.rate(d + 90*time.Minute)
		assert.Equal(r, r3, "the same minute of the next period")
		if prev >= 0 && r != prev {
			differs = true
		}
		prev = r
	}
	assert.True(differs)
}

// TestArrivalProfile_Rejects pins the parse errors that would otherwise run a different schedule than intended.
func TestArrivalProfile_Rejects(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	for _, c := range []struct{ profile, bursts string }{
		{"25m", ""},
		{"25m:x", ""},
		{"0m:100", ""},
		{"10m:100", "9m+2m:500"}, // past the period
		{"10m:100", "1m:500"},
	} {
		_, err := parseArrivalProfile(c.profile, c.bursts, 0, 1)
		assert.Error(err, "%q %q", c.profile, c.bursts)
	}
	_, err := parseArrivalProfile("10m:100", "", 1, 1)
	assert.Error(err, "noise of 1 allows a zero rate")
}

// TestPoisson pins the arrival counts' mean on both sides of the method switch.
func TestPoisson(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	r := rand.New(rand.NewPCG(1, 2))
	for _, lambda := range []float64{0.3, 5, 29, 31, 200} {
		sum := 0
		const n = 20000
		for range n {
			sum += poisson(r, lambda)
		}
		mean := float64(sum) / n
		assert.True(math.Abs(mean-lambda) < 0.05*lambda+0.02, "lambda %g: mean %g", lambda, mean)
	}
	assert.Equal(0, poisson(r, 0))
}

// TestParseMix pins the -mix grammar and its defaults.
func TestParseMix(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	classes, err := parseMix("0.5:linear:1K,0.3:fanout:4K:8,0.2:scatter:1M:4:2", 16)
	assert.NoError(err)
	assert.Len(classes, 3)
	assert.Equal(mixClass{share: 0.5, workload: "linear", payload: 1024, width: 16, depth: 1, name: "linear:1K"}, classes[0])
	assert.Equal(8, classes[1].width)
	assert.Equal(1<<20, classes[2].payload)
	assert.Equal(2, classes[2].depth)
	for _, bad := range []string{"0.5:linear:1K", "1:loop:0", "1:linear:1G", "1:fanout:0:0", "x:linear:0"} {
		_, err := parseMix(bad, 16)
		assert.Error(err, bad)
	}
}

// TestBuildMix pins that each class gets its own graphs, its share of the picks, and the mix's mean steps.
func TestBuildMix(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	h := &benchHost{graphs: map[string]*workflow.Graph{}, tasks: map[string]func(context.Context, *workflow.Flow) error{}, bytesWritten: &atomic.Int64{}}
	classes, err := parseMix("0.75:linear:0,0.25:fanout:1K:8", 16)
	assert.NoError(err)
	pick, picked, mean := buildMix(h, classes, 10, false)
	assert.Equal(10.0, mean, "both shapes are ten steps")
	assert.NotEqual(picked[0].graphURL, picked[1].graphURL)
	counts := map[string]int{}
	for range 4000 {
		counts[pick().class]++
	}
	assert.True(counts["linear:0"] > 2700 && counts["linear:0"] < 3300, "%v", counts)
}

// TestVolumeCheckpoints pins both forms of the checkpoint list.
func TestVolumeCheckpoints(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	cp, err := volumeCheckpoints("400,100,200,200", 0, 500)
	assert.NoError(err)
	assert.Equal([]int{100, 200, 400}, cp)
	cp, err = volumeCheckpoints("", 0, 500)
	assert.NoError(err)
	assert.Equal([]int{100, 200, 300, 400}, cp)
	_, err = volumeCheckpoints("100,x", 0, 500)
	assert.Error(err)
}

// TestQuantile pins nearest-rank quantiles at the edges.
func TestQuantile(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.Equal(0.0, quantile(nil, 0.5))
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	assert.Equal(5.0, quantile(v, 0.5))
	assert.Equal(10.0, quantile(v, 0.99))
	assert.Equal(1.0, quantile(v, 0))
}

// TestVolumeCheckpointsMB pins the size ladder's parse: ascending, distinct, positive.
func TestVolumeCheckpointsMB(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	cp, err := volumeCheckpointsMB("32768,16384,16384")
	assert.NoError(err)
	assert.Equal([]float64{16384, 32768}, cp)
	cp, err = volumeCheckpointsMB("")
	assert.NoError(err)
	assert.Nil(cp)
	_, err = volumeCheckpointsMB("16384,0")
	assert.Error(err)
}

// TestLatencyBucket pins the histogram: neighbours 10% apart, and a percentile read back from merged buckets
// within 10% of the exact one, which is what lets intervals and replicas add.
func TestLatencyBucket(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	assert.Equal(0, latencyBucket(1))
	assert.Equal(latencyBucket(100)+1, latencyBucket(110.01))
	assert.Equal(latencyBucket(0), latencyBucket(-5), "a non-positive latency lands in the lowest bucket")

	r := rand.New(rand.NewPCG(3, 4))
	var a, b []float64
	hist := map[int]int{}
	for range 5000 {
		x := math.Exp(r.NormFloat64()*1.5 + 6) // a long-tailed latency around ~400 ms
		if r.IntN(2) == 0 {
			a = append(a, x)
		} else {
			b = append(b, x)
		}
		hist[latencyBucket(x)]++
	}
	all := append(append([]float64{}, a...), b...)
	slices.Sort(all)
	for _, q := range []float64{0.5, 0.99} {
		exact := quantile(all, q)
		// The bucket holding the q-th sample, read back as its lower bound.
		keys := slices.Sorted(maps.Keys(hist))
		need, seen, got := int(math.Ceil(q*float64(len(all)))), 0, 0.0
		for _, k := range keys {
			seen += hist[k]
			if seen >= need {
				got = math.Pow(1.1, float64(k))
				break
			}
		}
		assert.True(got <= exact && exact < got*1.1, "q %g: exact %g, bucket from %g", q, exact, got)
	}
}

// TestSplitKey pins the instrument-key parse the per-class and per-band rates rest on.
func TestSplitKey(t *testing.T) {
	t.Parallel()
	assert := testarossa.For(t)
	name, attrs := splitKey("dwarf_flows_closed|priority=50|shard=1|status=completed|workflow=bench/m1/linear")
	assert.Equal("dwarf_flows_closed", name)
	assert.Equal(map[string]string{"priority": "50", "shard": "1", "status": "completed", "workflow": "bench/m1/linear"}, attrs)
	name, attrs = splitKey("dwarf_steps_executed")
	assert.Equal("dwarf_steps_executed", name)
	assert.Len(attrs, 0)
}
