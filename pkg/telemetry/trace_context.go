package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

var propagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

// InjectTraceContext injects W3C TraceContext headers into a string map carrier.
func InjectTraceContext(ctx context.Context, carrier map[string]string) {
	if carrier == nil {
		return
	}
	propagator.Inject(ctx, propagation.MapCarrier(carrier))
}

// ExtractTraceContext extracts W3C TraceContext from a string map into a Context.
func ExtractTraceContext(ctx context.Context, carrier map[string]string) context.Context {
	if carrier == nil {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier(carrier))
}

// GetTraceAndSpanIDs returns the hex formatted traceID and spanID from context.
func GetTraceAndSpanIDs(ctx context.Context) (traceID, spanID string) {
	sc := trace.SpanContextFromContext(ctx)
	if sc.IsValid() {
		return sc.TraceID().String(), sc.SpanID().String()
	}
	return "", ""
}
