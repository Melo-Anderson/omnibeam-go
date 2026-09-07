package resilience_test

import (
	"errors"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/pkg/resilience"
)

func TestCircuitBreaker_StateTransitions(t *testing.T) {
	st := resilience.Settings{
		Name:          "test-cb",
		MaxFailures:   2,
		Timeout:       50 * time.Millisecond,
		HalfOpenLimit: 1,
	}
	cb, err := resilience.NewCircuitBreaker(st)
	if err != nil {
		t.Fatalf("unexpected error creating circuit breaker: %v", err)
	}

	if cb.State() != resilience.StateClosed {
		t.Fatalf("expected StateClosed, got %v", cb.State())
	}
	if cb.State().String() != "Closed" {
		t.Fatalf("expected string Closed, got %s", cb.State().String())
	}

	errDummy := errors.New("temporary failure")

	// 1st failure
	_ = cb.Execute(func() error { return errDummy })
	if cb.State() != resilience.StateClosed {
		t.Fatalf("expected StateClosed after 1 failure, got %v", cb.State())
	}

	// 2nd failure -> Open
	_ = cb.Execute(func() error { return errDummy })
	if cb.State() != resilience.StateOpen {
		t.Fatalf("expected StateOpen after 2 failures, got %v", cb.State())
	}
	if cb.State().String() != "Open" {
		t.Fatalf("expected string Open, got %s", cb.State().String())
	}

	// Call while Open should fail-fast
	err = cb.Execute(func() error { return nil })
	if !errors.Is(err, resilience.ErrCircuitOpen) {
		t.Fatalf("expected ErrCircuitOpen, got %v", err)
	}

	// Wait for timeout -> Half-Open
	time.Sleep(60 * time.Millisecond)
	if cb.State() != resilience.StateHalfOpen {
		t.Fatalf("expected StateHalfOpen after timeout, got %v", cb.State())
	}
	if cb.State().String() != "HalfOpen" {
		t.Fatalf("expected string HalfOpen, got %s", cb.State().String())
	}

	// Success in Half-Open -> Closed
	err = cb.Execute(func() error { return nil })
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if cb.State() != resilience.StateClosed {
		t.Fatalf("expected StateClosed after recovery, got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailure(t *testing.T) {
	cb, err := resilience.NewCircuitBreaker(resilience.Settings{
		MaxFailures: 1,
		Timeout:     20 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	errDummy := errors.New("fail")
	_ = cb.Execute(func() error { return errDummy })
	if cb.State() != resilience.StateOpen {
		t.Fatalf("expected Open")
	}

	time.Sleep(30 * time.Millisecond)
	if cb.State() != resilience.StateHalfOpen {
		t.Fatalf("expected HalfOpen")
	}

	// Failure in HalfOpen should immediately reopen circuit
	_ = cb.Execute(func() error { return errDummy })
	if cb.State() != resilience.StateOpen {
		t.Fatalf("expected Open after failure in HalfOpen")
	}
}

func TestNewCircuitBreaker_Defaults(t *testing.T) {
	cb, err := resilience.NewCircuitBreaker(resilience.Settings{})
	if err != nil {
		t.Fatalf("unexpected error with zero values: %v", err)
	}
	if cb.State() != resilience.StateClosed {
		t.Fatalf("expected Closed")
	}
	if resilience.State(99).String() != "Unknown(99)" {
		t.Fatalf("expected Unknown(99)")
	}
}

func TestNewCircuitBreaker_RejectsNegativeTimeout(t *testing.T) {
	_, err := resilience.NewCircuitBreaker(resilience.Settings{
		Name:    "invalid-cb",
		Timeout: -1 * time.Second,
	})
	if err == nil {
		t.Fatal("expected error for negative timeout, got nil")
	}
}

func TestCircuitBreaker_HalfOpenRequiresMultipleSuccesses(t *testing.T) {
	cb, err := resilience.NewCircuitBreaker(resilience.Settings{
		Name:          "test-cb",
		MaxFailures:   1,
		Timeout:       20 * time.Millisecond,
		HalfOpenLimit: 3,
	})
	if err != nil {
		t.Fatalf("NewCircuitBreaker: %v", err)
	}

	// Trip to Open
	_ = cb.Execute(func() error { return errors.New("fail") })
	if cb.State() != resilience.StateOpen {
		t.Fatalf("expected Open, got %v", cb.State())
	}

	// Wait for timeout to transition to HalfOpen
	time.Sleep(30 * time.Millisecond)

	// 1st success -> still HalfOpen
	_ = cb.Execute(func() error { return nil })
	if cb.State() != resilience.StateHalfOpen {
		t.Fatalf("expected HalfOpen after 1 success, got %v", cb.State())
	}

	// 2nd success -> still HalfOpen
	_ = cb.Execute(func() error { return nil })
	if cb.State() != resilience.StateHalfOpen {
		t.Fatalf("expected HalfOpen after 2 successes, got %v", cb.State())
	}

	// 3rd success -> Closed
	_ = cb.Execute(func() error { return nil })
	if cb.State() != resilience.StateClosed {
		t.Fatalf("expected Closed after 3 successes, got %v", cb.State())
	}
}
