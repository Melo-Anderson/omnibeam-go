package telemetry_test

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
)

func FuzzExtractTraceContext(f *testing.F) {
	// Seed corpus with valid and edge case traceparent strings
	f.Add("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	f.Add("00-00000000000000000000000000000000-0000000000000000-00")
	f.Add("invalid-junk-header-data")
	f.Add("")

	f.Fuzz(func(t *testing.T, traceparent string) {
		carrier := map[string]string{
			"traceparent": traceparent,
		}
		// Must never panic regardless of input
		ctx := telemetry.ExtractTraceContext(context.Background(), carrier)
		traceID, spanID := telemetry.GetTraceAndSpanIDs(ctx)
		_ = traceID
		_ = spanID
	})
}
