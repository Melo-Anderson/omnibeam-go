package telemetry_test

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/telemetry"
)

func TestInitTracer_Smoke(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:19999") // unused port — guaranteed no collector
	t.Setenv("OTEL_SERVICE_NAME", "custom-service-name")

	shutdown, err := telemetry.InitTracer(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("InitTracer returned unexpected error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("InitTracer returned nil shutdown func")
	}
	_ = shutdown(context.Background())
}

func TestInitTracer_UnsetEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	shutdown, err := telemetry.InitTracer(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("unexpected error when no endpoint is set: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil no-op shutdown function")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("no-op shutdown should not error: %v", err)
	}
}

func TestInitTracer_Disabled(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")

	shutdown, err := telemetry.InitTracer(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("unexpected error when OTel is disabled: %v", err)
	}
	if shutdown == nil {
		t.Fatal("expected non-nil no-op shutdown function")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("no-op shutdown should not error: %v", err)
	}
}
