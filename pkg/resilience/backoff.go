package resilience

import (
	"context"
	"crypto/rand"
	"errors"
	"math"
	"math/big"
	"time"
)

// BackoffConfig defines exponential backoff with jitter parameters.
type BackoffConfig struct {
	Initial    time.Duration
	Max        time.Duration
	Multiplier float64
	MaxRetries int
}

// Validate ensures non-negative durations and positive multiplier.
func (b *BackoffConfig) Validate() error {
	if b.Initial < 0 || b.Max < 0 || b.Multiplier < 0 || b.MaxRetries < 0 {
		return errors.New("backoff config cannot have negative values")
	}
	return nil
}

// FullJitter calculates an exponential duration bounded by [0, min(Max, Initial * Multiplier^attempt)].
func FullJitter(attempt int, cfg BackoffConfig) time.Duration {
	if err := cfg.Validate(); err != nil || cfg.Initial <= 0 {
		return 0
	}
	multiplier := cfg.Multiplier
	if multiplier <= 0 {
		multiplier = 2.0
	}
	maxExp := float64(cfg.Initial) * math.Pow(multiplier, float64(attempt))
	capDuration := maxExp
	if cfg.Max > 0 && float64(cfg.Max) < capDuration {
		capDuration = float64(cfg.Max)
	}
	if capDuration <= 0 {
		return 0
	}

	n, err := rand.Int(rand.Reader, big.NewInt(int64(capDuration)))
	if err != nil {
		return time.Duration(capDuration)
	}
	return time.Duration(n.Int64())
}

// Retry executes an operation with exponential backoff and jitter.
func Retry(ctx context.Context, cfg BackoffConfig, op func() error) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	var lastErr error
	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		lastErr = op()
		if lastErr == nil {
			return nil
		}
		if attempt < cfg.MaxRetries {
			delay := FullJitter(attempt, cfg)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
	}
	return lastErr
}
