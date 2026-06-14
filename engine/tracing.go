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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// traceScope is the OpenTelemetry instrumentation scope for the engine's spans (same as the metric
// scope). Service identity comes from the injected TracerProvider's Resource, not from here.
const traceScope = metricScope

// traceProp is the W3C trace-context propagator used to serialize/deserialize the trace_parent column.
// Stateless, so a package-level value is safe to share.
var traceProp = propagation.TraceContext{}

// initTracer resolves the engine's tracer from the injected TracerProvider, falling back to the global
// otel.GetTracerProvider() (the no-op provider unless the host configures the OTEL SDK). Called from
// initRuntime. With no provider injected the tracer is a no-op: Start returns a non-recording span,
// mintRootTraceParent yields "" (so trace_parent stays empty), and there is zero per-step overhead.
func (e *Engine) initTracer() {
	tp := e.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	e.tracer = tp.Tracer(traceScope)
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
	flowCtx, span := e.tracer.Start(ctx, "workflow",
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

// recordSpanError marks a span as errored with the given error, matching the foreman's taskSpan.SetError.
func recordSpanError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
