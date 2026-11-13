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
	"reflect"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// traceScope is the OpenTelemetry instrumentation scope for the engine's spans (same as the metric
// scope). Service identity comes from the injected TracerProvider's Resource, not from here.
const traceScope = metricScope

// traceProp is the W3C trace-context propagator used to serialize/deserialize the trace_parent column.
// Stateless, so a package-level value is safe to share.
var traceProp = propagation.TraceContext{}

// initProviders resolves the tracer and meter providers for this run into diags - the configured ones, else the
// global ones - and sets the engine's tracer and meter from them. Called from Startup before the shards open, whose
// sequel layer instruments itself with both, and before the shards' actors are built, which create their instruments
// on the meter.
func (e *Engine) initProviders() {
	tp := resolveTracerProvider(e.tracerProvider, otel.GetTracerProvider())
	e.diags.SetTracerProvider(tp)
	e.diags.SetTracer(tp.Tracer(traceScope))
	mp := e.meterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	e.diags.SetMeterProvider(mp)
	// One meter, so every component records under one instrumentation scope per engine.
	e.diags.SetMeter(mp.Meter(metricScope))
}

// otelStandInPkg and otelStandInType name OTel's global stand-in provider: what otel.GetTracerProvider()
// returns until a host installs one. Pinned by TestTracing_OTelStandInIsRecognized.
const (
	otelStandInPkg  = "go.opentelemetry.io/otel/internal/global"
	otelStandInType = "tracerProvider"
)

// resolveTracerProvider picks the provider the engine and its shards' sequel layer trace through, once per
// Startup: the injected one, else the global - and a true no-op in place of OTel's untouched stand-in,
// wherever it came from. The stand-in can start forwarding to a provider installed later, so it builds a span
// per call even while nothing records: ~7% of bytes allocated per step with tracing off, most of it sequel's
// span per query. The cost of the swap is that a tracer installed after Startup is never reached.
//
// A stand-in injected before the host installed its SDK already forwards to it, so global (the current
// otel.GetTracerProvider(), a parameter so tests need not install one) is read for it too: an installed SDK
// wins.
func resolveTracerProvider(injected, global trace.TracerProvider) trace.TracerProvider {
	tp := injected
	if tp == nil || isOTelStandIn(tp) {
		tp = global
	}
	if isOTelStandIn(tp) {
		return noop.NewTracerProvider()
	}
	return tp
}

// isOTelStandIn reports whether tp is OTel's global stand-in. The type is internal to OTel, so it is matched by
// name. Do not match by a pointer captured at package init instead: a tracer installed by another package's init
// before this one's would be captured as "the default" and silently replaced by the no-op. A renamed type only
// fails to match, which keeps the stand-in - today's cost, no lost spans.
func isOTelStandIn(tp trace.TracerProvider) bool {
	t := reflect.TypeOf(tp)
	return t != nil && t.Kind() == reflect.Pointer &&
		t.Elem().PkgPath() == otelStandInPkg && t.Elem().Name() == otelStandInType
}

// mintWorkflowSpan creates a "workflow" span for a flow, ends it immediately, and serializes its W3C
// trace context to a traceparent string for storage in the flow's trace_parent column. Each per-step
// span later reconstructs this context as its parent, so the whole flow nests under this one span.
//
// When parentTraceParent is empty the span is a detached root: any ambient span on ctx is stripped, so a
// top-level flow (and each Continue turn) roots its own fresh trace rather than nesting under the
// request that created it. When parentTraceParent is set - a subgraph - the span is parented to that
// context (the caller step's span), so the subgraph's entire subtree appears nested under the task that
// launched it: workflow → caller-step → workflow(subgraph) → subgraph-steps. Returns "" under the no-op
// tracer.
func (e *Engine) mintWorkflowSpan(ctx context.Context, workflowURL, parentTraceParent string) string {
	if parentTraceParent == "" {
		ctx = trace.ContextWithSpan(ctx, nil)
	} else {
		ctx = injectTraceParent(ctx, parentTraceParent)
	}
	flowCtx, span := e.diags.Tracer().Start(ctx, "workflow",
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(attribute.String("workflow.name", workflowURL)),
	)
	span.End()
	return extractTraceParent(flowCtx)
}

// extractTraceParent serializes the trace context from ctx into a W3C traceparent string.
func extractTraceParent(ctx context.Context) string {
	carrier := propagation.HeaderCarrier{}
	traceProp.Inject(ctx, carrier)
	return carrier.Get("Traceparent")
}

// traceIDFromParent extracts the 32-hex trace id from a stored W3C traceparent
// ("version-traceid-spanid-flags"), for correlating a flow with its trace backend. Returns "" when the
// string is empty (no tracer configured) or malformed - never errors, since it feeds a best-effort
// observability field, not a control path.
func traceIDFromParent(traceParent string) string {
	parts := strings.Split(strings.TrimSpace(traceParent), "-")
	if len(parts) < 2 || len(parts[1]) != 32 {
		return ""
	}
	return parts[1]
}

// injectTraceParent deserializes a W3C traceparent string into ctx so spans created from the returned
// context nest under the stored trace. A blank traceparent leaves ctx unchanged.
func injectTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	carrier := propagation.HeaderCarrier{}
	carrier.Set("Traceparent", traceParent)
	return traceProp.Extract(ctx, carrier)
}

// recordSpanError marks a span as errored with the given error.
func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
