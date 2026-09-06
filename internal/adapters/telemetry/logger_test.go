package telemetry_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	adaptertelemetry "github.com/omnibeam/dataflow-compute-go/internal/adapters/telemetry"
	pkgtelemetry "github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
)

func TestStructuredLogger_InfoContext_IncludesTraceIDs(t *testing.T) {
	var buf bytes.Buffer
	logger := adaptertelemetry.NewStructuredLogger(&buf)

	carrier := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	ctx := pkgtelemetry.ExtractTraceContext(context.Background(), carrier)

	logger.InfoContext(ctx, "test info message")

	var entry map[string]any
	if err := json.NewDecoder(&buf).Decode(&entry); err != nil {
		t.Fatalf("expected valid JSON log line, got: %v", err)
	}
	if entry["msg"] != "test info message" {
		t.Fatalf("expected msg %q, got %v", "test info message", entry["msg"])
	}
	if entry["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("expected trace_id, got %v", entry["trace_id"])
	}
	if entry["span_id"] != "00f067aa0ba902b7" {
		t.Fatalf("expected span_id, got %v", entry["span_id"])
	}
}

func TestStructuredLogger_ErrorContext_IncludesTraceIDs(t *testing.T) {
	var buf bytes.Buffer
	logger := adaptertelemetry.NewStructuredLogger(&buf)

	carrier := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	ctx := pkgtelemetry.ExtractTraceContext(context.Background(), carrier)

	logger.ErrorContext(ctx, "test error message")

	var entry map[string]any
	if err := json.NewDecoder(&buf).Decode(&entry); err != nil {
		t.Fatalf("expected valid JSON log line, got: %v", err)
	}
	if entry["msg"] != "test error message" {
		t.Fatalf("expected msg %q, got %v", "test error message", entry["msg"])
	}
	if entry["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("expected trace_id, got %v", entry["trace_id"])
	}
	if entry["span_id"] != "00f067aa0ba902b7" {
		t.Fatalf("expected span_id, got %v", entry["span_id"])
	}
}

func TestStructuredLogger_DefaultStdout(t *testing.T) {
	logger := adaptertelemetry.NewStructuredLogger(nil)
	if logger == nil {
		t.Fatal("expected non-nil logger")
	}
	// Calling with background context should not panic
	logger.InfoContext(context.Background(), "stdout fallback message")
}

func TestStructuredLogger_SanitizesSensitiveFields(t *testing.T) {
	var buf bytes.Buffer
	logger := adaptertelemetry.NewStructuredLogger(&buf)

	logger.InfoContext(context.Background(), "user login", "password", "super_secret_123", "user_id", int64(42))

	var entry map[string]any
	if err := json.NewDecoder(&buf).Decode(&entry); err != nil {
		t.Fatalf("expected valid JSON log line, got: %v", err)
	}
	if entry["password"] != "[REDACTED]" {
		t.Fatalf("expected password to be [REDACTED], got %v", entry["password"])
	}
	if entry["user_id"] != float64(42) {
		t.Fatalf("expected user_id to be 42, got %v", entry["user_id"])
	}
}
