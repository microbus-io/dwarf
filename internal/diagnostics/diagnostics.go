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

// Package diagnostics holds what an engine's components report through: the logger, the meter and tracer and the
// providers they come from, and the seams a test arms. The engine owns one Diags and hands the same one to every
// component it builds:
//
//	d := diagnostics.New()
//	d.SetLogger(logger)
//	d.SetSeams(seams)
//	...
//	j := janitor.New(shard, d, archive, retentionDays)
//
// Every getter works on a nil *Diags, and on a Diags with nothing set, returning what reports nowhere: a logger that
// discards, inert seams, a no-op meter and tracer. A component built with nil is silent.
//
// Concurrency: set everything before handing the Diags to the components that read it; the getters are then safe
// from any goroutine.
package diagnostics

import (
	"context"
	"log/slog"
	"strconv"

	"github.com/microbus-io/seamster"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// What a getter returns when nothing was set.
var (
	discard    = slog.New(slog.DiscardHandler)
	inertSeams = seamster.New(false)
	noMeters   = metricnoop.NewMeterProvider()
	noMeter    = noMeters.Meter("")
	noTracers  = tracenoop.NewTracerProvider()
	noTracer   = noTracers.Tracer("")
)

// Diags is what a component reports through. Make it with New.
type Diags struct {
	logger         *slog.Logger
	seams          *seamster.Seamster
	meterProvider  metric.MeterProvider
	meter          metric.Meter
	tracerProvider trace.TracerProvider
	tracer         trace.Tracer
}

// New returns a Diags with nothing set, which reports nowhere.
func New() *Diags {
	return &Diags{}
}

// SetLogger sets the logger; nil logs nowhere.
func (d *Diags) SetLogger(l *slog.Logger) {
	d.logger = l
}

// Logger is the logger, one that discards when none was set.
func (d *Diags) Logger() *slog.Logger {
	if d == nil || d.logger == nil {
		return discard
	}
	return d.logger
}

// SetSeams sets the faults and checkpoints the components consult; nil consults none.
func (d *Diags) SetSeams(s *seamster.Seamster) {
	d.seams = s
}

// Seams is the faults and checkpoints the components consult, inert ones when none were set.
func (d *Diags) Seams() *seamster.Seamster {
	if d == nil || d.seams == nil {
		return inertSeams
	}
	return d.seams
}

// CheckpointBy passes checkpoint name twice: bare, then targeted at the entity ids identify, the name Join(name,
// ids...) with each id in decimal. A test waits on or holds one entity's arrival by the targeted name, and any
// entity's by the bare one. The targeted name is built only while the seams are enabled, so a site calls it
// without an Enabled guard.
func (d *Diags) CheckpointBy(ctx context.Context, name string, ids ...int) {
	s := d.Seams()
	if !s.Enabled() {
		return
	}
	s.Checkpoint(ctx, name)
	parts := make([]string, 0, len(ids)+1)
	parts = append(parts, name)
	for _, id := range ids {
		parts = append(parts, strconv.Itoa(id))
	}
	s.Checkpoint(ctx, Join(parts...))
}

// SetMeterProvider sets the meter provider, for a component that instruments itself in a scope of its own (the
// database layer); nil is a no-op provider.
func (d *Diags) SetMeterProvider(mp metric.MeterProvider) {
	d.meterProvider = mp
}

// MeterProvider is the meter provider, a no-op one when none was set.
func (d *Diags) MeterProvider() metric.MeterProvider {
	if d == nil || d.meterProvider == nil {
		return noMeters
	}
	return d.meterProvider
}

// SetMeter sets the meter the components create their instruments on, so they share one instrumentation scope; nil
// is a no-op meter.
func (d *Diags) SetMeter(m metric.Meter) {
	d.meter = m
}

// Meter is the meter the components create their instruments on, a no-op one when none was set.
func (d *Diags) Meter() metric.Meter {
	if d == nil || d.meter == nil {
		return noMeter
	}
	return d.meter
}

// SetTracerProvider sets the tracer provider, for a component that traces in a scope of its own (the database
// layer); nil is a no-op provider.
func (d *Diags) SetTracerProvider(tp trace.TracerProvider) {
	d.tracerProvider = tp
}

// TracerProvider is the tracer provider, a no-op one when none was set.
func (d *Diags) TracerProvider() trace.TracerProvider {
	if d == nil || d.tracerProvider == nil {
		return noTracers
	}
	return d.tracerProvider
}

// SetTracer sets the tracer the components start their spans on; nil is a no-op tracer.
func (d *Diags) SetTracer(t trace.Tracer) {
	d.tracer = t
}

// Tracer is the tracer the components start their spans on, a no-op one when none was set.
func (d *Diags) Tracer() trace.Tracer {
	if d == nil || d.tracer == nil {
		return noTracer
	}
	return d.tracer
}
