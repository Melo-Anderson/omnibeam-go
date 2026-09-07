package telemetry_test

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContext_InjectAndExtract(t *testing.T) {
	carrier := make(map[string]string)
	carrier["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

	ctx := telemetry.ExtractTraceContext(context.Background(), carrier)
	sc := trace.SpanContextFromContext(ctx)

	if !sc.IsValid() {
		t.Fatalf("expected valid span context, got invalid")
	}

	if sc.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace id mismatch: got %v", sc.TraceID().String())
	}
	if sc.SpanID().String() != "00f067aa0ba902b7" {
		t.Fatalf("span id mismatch: got %v", sc.SpanID().String())
	}

	outCarrier := make(map[string]string)
	telemetry.InjectTraceContext(ctx, outCarrier)
	if outCarrier["traceparent"] != carrier["traceparent"] {
		t.Fatalf("inject mismatch: got %v, want %v", outCarrier["traceparent"], carrier["traceparent"])
	}
}

func TestGetTraceAndSpanIDs_ValidAndInvalid(t *testing.T) {
	// 1. Invalid / Empty context
	traceID, spanID := telemetry.GetTraceAndSpanIDs(context.Background())
	if traceID != "" || spanID != "" {
		t.Fatalf("expected empty strings for background context, got %q, %q", traceID, spanID)
	}

	// 2. Valid context
	carrier := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	ctx := telemetry.ExtractTraceContext(context.Background(), carrier)
	traceID, spanID = telemetry.GetTraceAndSpanIDs(ctx)
	if traceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("expected traceID %q, got %q", "4bf92f3577b34da6a3ce929d0e0e4736", traceID)
	}
	if spanID != "00f067aa0ba902b7" {
		t.Fatalf("expected spanID %q, got %q", "00f067aa0ba902b7", spanID)
	}
}

func TestTraceContext_NilCarrier(t *testing.T) {
	// Should not panic on nil carrier
	telemetry.InjectTraceContext(context.Background(), nil)
	ctx := telemetry.ExtractTraceContext(context.Background(), nil)
	if ctx == nil {
		t.Fatal("expected non-nil context")
	}
}
