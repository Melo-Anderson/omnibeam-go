package resilience

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// SetupSignalTrap listens for termination signals and returns a cancellable context.
func SetupSignalTrap(signals ...os.Signal) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)

	if len(signals) == 0 {
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	} else {
		signal.Notify(sigCh, signals...)
	}

	go func() {
		select {
		case <-sigCh:
			cancel()
		case <-ctx.Done():
		}
		signal.Stop(sigCh)
	}()

	return ctx, cancel
}

// DrainHandler is a callback executed during graceful shutdown.
type DrainHandler struct {
	Name string
	Fn   func(context.Context) error
}

// DrainManager coordinates graceful flushing of active buffers before shutdown.
type DrainManager struct {
	mu       sync.Mutex
	timeout  time.Duration
	handlers []DrainHandler
}

// NewDrainManager creates a new DrainManager with a designated timeout.
func NewDrainManager(timeout time.Duration) (*DrainManager, error) {
	if timeout <= 0 {
		return nil, errors.New("drain timeout must be positive")
	}
	return &DrainManager{timeout: timeout}, nil
}

// Register adds a drain handler to be called in order.
func (dm *DrainManager) Register(name string, fn func(context.Context) error) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	dm.handlers = append(dm.handlers, DrainHandler{Name: name, Fn: fn})
}

// Drain executes all registered handlers within the drain timeout.
func (dm *DrainManager) Drain(ctx context.Context) error {
	dm.mu.Lock()
	handlers := make([]DrainHandler, len(dm.handlers))
	copy(handlers, dm.handlers)
	dm.mu.Unlock()

	drainCtx, cancel := context.WithTimeout(ctx, dm.timeout)
	defer cancel()

	for _, h := range handlers {
		if err := h.Fn(drainCtx); err != nil {
			return fmt.Errorf("drain handler %q failed: %w", h.Name, err)
		}
	}
	return nil
}
