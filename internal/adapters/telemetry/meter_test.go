package telemetry_test

import (
	"context"
	"os"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/telemetry"
)

func TestInitMeter_Disabled(t *testing.T) {
	os.Setenv("OTEL_SDK_DISABLED", "true")
	defer os.Unsetenv("OTEL_SDK_DISABLED")

	shutdown, err := telemetry.InitMeter(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("expected no error when disabled, got: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("expected no-op shutdown to succeed, got: %v", err)
	}
}

func TestInitMeter_NoEndpoint(t *testing.T) {
	os.Unsetenv("OTEL_EXPORTER_OTLP_ENDPOINT")

	shutdown, err := telemetry.InitMeter(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("expected no error with empty endpoint, got: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("expected no-op shutdown to succeed, got: %v", err)
	}
}

func TestInitMeter_MetricsExporterNone_And_ConfiguredEndpoint(t *testing.T) {
	t.Run("OTEL_METRICS_EXPORTER=none", func(t *testing.T) {
		t.Setenv("OTEL_METRICS_EXPORTER", "none")
		shutdown, err := telemetry.InitMeter(context.Background(), "test-service")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		_ = shutdown(context.Background())
	})

	t.Run("Configured endpoint initializes successfully", func(t *testing.T) {
		t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")
		t.Setenv("OTEL_SERVICE_NAME", "custom-service")
		t.Setenv("WORKER_ID", "w-123")
		t.Setenv("OTEL_METRICS_EXPORTER", "otlp")

		shutdown, err := telemetry.InitMeter(context.Background(), "default-service")
		if err != nil {
			t.Fatalf("InitMeter: %v", err)
		}
		cancelCtx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = shutdown(cancelCtx)
	})
}
