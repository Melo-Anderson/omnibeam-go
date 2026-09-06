package resilience_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/pkg/resilience"
)

func TestFullJitter_Bounds(t *testing.T) {
	cfg := resilience.BackoffConfig{
		Initial:    10 * time.Millisecond,
		Max:        100 * time.Millisecond,
		Multiplier: 2.0,
	}

	for attempt := 0; attempt < 5; attempt++ {
		d := resilience.FullJitter(attempt, cfg)
		if d < 0 || d > cfg.Max {
			t.Fatalf("jitter out of bounds: %v (max %v)", d, cfg.Max)
		}
	}
}

func TestBackoffConfig_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     resilience.BackoffConfig
		wantErr bool
	}{
		{
			name: "valid config",
			cfg: resilience.BackoffConfig{
				Initial:    10 * time.Millisecond,
				Max:        100 * time.Millisecond,
				Multiplier: 2.0,
				MaxRetries: 3,
			},
			wantErr: false,
		},
		{
			name: "negative initial",
			cfg: resilience.BackoffConfig{
				Initial: -1 * time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "negative max",
			cfg: resilience.BackoffConfig{
				Max: -1 * time.Millisecond,
			},
			wantErr: true,
		},
		{
			name: "negative multiplier",
			cfg: resilience.BackoffConfig{
				Multiplier: -1.0,
			},
			wantErr: true,
		},
		{
			name: "negative retries",
			cfg: resilience.BackoffConfig{
				MaxRetries: -1,
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

func TestRetry_SuccessAfterRetries(t *testing.T) {
	cfg := resilience.BackoffConfig{
		Initial:    1 * time.Millisecond,
		Max:        5 * time.Millisecond,
		MaxRetries: 3,
	}

	attempts := 0
	err := resilience.Retry(context.Background(), cfg, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("transient error")
		}
		return nil
	})

	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestRetry_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := resilience.BackoffConfig{
		Initial:    50 * time.Millisecond,
		Max:        100 * time.Millisecond,
		MaxRetries: 5,
	}

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := resilience.Retry(ctx, cfg, func() error {
		return errors.New("temporary error")
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestRetry_ContextAlreadyDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := resilience.BackoffConfig{}

	err := resilience.Retry(ctx, cfg, func() error {
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
