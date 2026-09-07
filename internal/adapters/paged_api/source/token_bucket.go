package source

import (
	"context"
	"sync"
	"time"
)

// TokenBucket implements a simple thread-safe rate limiter based on the token bucket algorithm.
type TokenBucket struct {
	rate       float64
	capacity   float64
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
}

// NewTokenBucket creates a new TokenBucket rate limiter. Returns nil if rps <= 0.
func NewTokenBucket(rps float64) *TokenBucket {
	if rps <= 0 {
		return nil
	}
	return &TokenBucket{
		rate:       rps,
		capacity:   1.0,
		tokens:     1.0,
		lastRefill: time.Now(),
	}
}

// Wait blocks until a token is available or the context is cancelled.
func (tb *TokenBucket) Wait(ctx context.Context) error {
	if tb == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		tb.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(tb.lastRefill).Seconds()
		tb.tokens += elapsed * tb.rate
		if tb.tokens > tb.capacity {
			tb.tokens = tb.capacity
		}
		tb.lastRefill = now

		if tb.tokens >= 1.0 {
			tb.tokens -= 1.0
			tb.mu.Unlock()
			return nil
		}

		missing := 1.0 - tb.tokens
		waitTime := time.Duration((missing / tb.rate) * float64(time.Second))
		tb.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitTime):
		}
	}
}
