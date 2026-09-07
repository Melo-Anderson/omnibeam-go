package functional_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/pkg/resilience"
	"github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
)

// Scenario 1: HTTP 429 Storm recovers with exponential backoff & full jitter
func TestChaos_HTTP429Storm_RecoversWithRetry(t *testing.T) {
	var requestCount atomic.Int64

	// Server returns 429 for the first 3 requests, then 200 OK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := requestCount.Add(1)
		if n <= 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"too_many_requests"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	cb, err := resilience.NewCircuitBreaker(resilience.Settings{
		Name:        "chaos-cb",
		MaxFailures: 5,
		Timeout:     1 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error creating cb: %v", err)
	}

	cfg := resilience.BackoffConfig{
		Initial:    5 * time.Millisecond,
		Max:        50 * time.Millisecond,
		Multiplier: 2.0,
		MaxRetries: 5,
	}

	err = resilience.Retry(context.Background(), cfg, func() error {
		return cb.Execute(func() error {
			resp, doErr := http.Get(srv.URL) //nolint:noctx
			if doErr != nil {
				return doErr
			}
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusTooManyRequests {
				return errors.New("429: rate limited")
			}
			return nil
		})
	})

	if err != nil {
		t.Fatalf("expected recovery after retries, got: %v", err)
	}
	if requestCount.Load() < 4 {
		t.Fatalf("expected at least 4 requests (3 failures + 1 success), got %d", requestCount.Load())
	}
}

// Scenario 2: Circuit Breaker Trips on severe outage, fast fails, then heals
func TestChaos_CircuitBreakerTripsAndRecovers(t *testing.T) {
	cb, err := resilience.NewCircuitBreaker(resilience.Settings{
		Name:        "outage-cb",
		MaxFailures: 2,
		Timeout:     40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed creating cb: %v", err)
	}

	errOutage := errors.New("remote service down")

	// Fail twice
	_ = cb.Execute(func() error { return errOutage })
	_ = cb.Execute(func() error { return errOutage })

	if cb.State() != resilience.StateOpen {
		t.Fatalf("expected StateOpen, got %v", cb.State())
	}

	// Must fast-fail
	err = cb.Execute(func() error { return nil })
	if !errors.Is(err, resilience.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}

	// Wait cooldown
	time.Sleep(50 * time.Millisecond)

	if cb.State() != resilience.StateHalfOpen {
		t.Fatalf("expected StateHalfOpen, got %v", cb.State())
	}

	// Service recovered
	err = cb.Execute(func() error { return nil })
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if cb.State() != resilience.StateClosed {
		t.Fatalf("expected StateClosed after recovery, got %v", cb.State())
	}
}

// Scenario 3: Preemption / Graceful Drain executes flush handlers atomically
func TestChaos_GracefulDrainManager(t *testing.T) {
	dm, err := resilience.NewDrainManager(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("failed creating drain manager: %v", err)
	}

	var flushed1, flushed2 atomic.Bool
	dm.Register("parquet-sink-flusher", func(ctx context.Context) error {
		flushed1.Store(true)
		return nil
	})
	dm.Register("dlq-flusher", func(ctx context.Context) error {
		flushed2.Store(true)
		return nil
	})

	err = dm.Drain(context.Background())
	if err != nil {
		t.Fatalf("unexpected drain error: %v", err)
	}

	if !flushed1.Load() || !flushed2.Load() {
		t.Fatalf("expected all handlers to drain: flushed1=%v, flushed2=%v", flushed1.Load(), flushed2.Load())
	}
}

// Scenario 4: W3C TraceContext end-to-end propagation through record and carrier
func TestChaos_W3CTraceContextPropagation(t *testing.T) {
	carrier := map[string]string{
		"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}
	ctx := telemetry.ExtractTraceContext(context.Background(), carrier)
	traceID, spanID := telemetry.GetTraceAndSpanIDs(ctx)

	rec := domain.NewGenericRecord("test-schema", 0)
	rec.AuditFields["_trace_id"] = traceID
	rec.AuditFields["_span_id"] = spanID

	if rec.AuditFields["_trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("expected traceID %q, got %q", "4bf92f3577b34da6a3ce929d0e0e4736", rec.AuditFields["_trace_id"])
	}
	if rec.AuditFields["_span_id"] != "00f067aa0ba902b7" {
		t.Fatalf("expected spanID %q, got %q", "00f067aa0ba902b7", rec.AuditFields["_span_id"])
	}

	outCarrier := make(map[string]string)
	telemetry.InjectTraceContext(ctx, outCarrier)
	if outCarrier["traceparent"] != carrier["traceparent"] {
		t.Fatalf("expected traceparent %q, got %q", carrier["traceparent"], outCarrier["traceparent"])
	}
}
