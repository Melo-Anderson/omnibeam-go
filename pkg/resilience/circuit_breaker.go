package resilience

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State represents the current state of the Circuit Breaker.
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "Closed"
	case StateHalfOpen:
		return "HalfOpen"
	case StateOpen:
		return "Open"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

var ErrCircuitOpen = errors.New("circuit breaker is open")

// Settings configures the Circuit Breaker.
type Settings struct {
	Name          string
	MaxFailures   uint32
	Timeout       time.Duration
	HalfOpenLimit uint32
}

// CircuitBreaker implements an atomic state-machine circuit breaker.
type CircuitBreaker struct {
	mu            sync.Mutex
	name          string
	maxFailures   uint32
	halfOpenLimit uint32
	timeout       time.Duration
	state         State
	failures      uint32
	successes     uint32
	lastStateChg  time.Time
}

// NewCircuitBreaker creates a configured CircuitBreaker instance.
// Returns an error if settings contain invalid (negative) values.
func NewCircuitBreaker(st Settings) (*CircuitBreaker, error) {
	if st.Timeout < 0 {
		return nil, fmt.Errorf("circuit breaker %q: timeout cannot be negative, got %v", st.Name, st.Timeout)
	}
	if st.MaxFailures == 0 {
		st.MaxFailures = 1
	}
	if st.HalfOpenLimit == 0 {
		st.HalfOpenLimit = 1
	}
	if st.Timeout == 0 {
		st.Timeout = 1 * time.Second
	}
	return &CircuitBreaker{
		name:          st.Name,
		maxFailures:   st.MaxFailures,
		halfOpenLimit: st.HalfOpenLimit,
		timeout:       st.Timeout,
		state:         StateClosed,
		lastStateChg:  time.Now(),
	}, nil
}

// State returns the current State.
func (cb *CircuitBreaker) State() State {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.checkStateTransition()
	return cb.state
}

func (cb *CircuitBreaker) checkStateTransition() {
	if cb.state == StateOpen && time.Since(cb.lastStateChg) > cb.timeout {
		cb.state = StateHalfOpen
		cb.failures = 0
		cb.successes = 0
		cb.lastStateChg = time.Now()
	}
}

// Execute wraps an operation with circuit breaking logic.
func (cb *CircuitBreaker) Execute(fn func() error) error {
	cb.mu.Lock()
	cb.checkStateTransition()
	if cb.state == StateOpen {
		cb.mu.Unlock()
		return ErrCircuitOpen
	}
	cb.mu.Unlock()

	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()
	if err != nil {
		cb.failures++
		if cb.state == StateHalfOpen || cb.failures >= cb.maxFailures {
			cb.state = StateOpen
			cb.lastStateChg = time.Now()
		}
		return err
	}

	if cb.state == StateHalfOpen {
		cb.successes++
		if cb.successes >= cb.halfOpenLimit {
			cb.state = StateClosed
			cb.failures = 0
			cb.successes = 0
			cb.lastStateChg = time.Now()
		}
	}
	return nil
}
