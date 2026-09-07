package resilience_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/pkg/resilience"
)

func TestDrainManager_ExecutesAllHandlers(t *testing.T) {
	dm, err := resilience.NewDrainManager(200 * time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	drained1 := false
	drained2 := false

	dm.Register("sink1", func(ctx context.Context) error {
		drained1 = true
		return nil
	})
	dm.Register("sink2", func(ctx context.Context) error {
		drained2 = true
		return nil
	})

	err = dm.Drain(context.Background())
	if err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !drained1 || !drained2 {
		t.Fatalf("expected both handlers to execute, got %v, %v", drained1, drained2)
	}
}

func TestDrainManager_HandlerFails(t *testing.T) {
	dm, err := resilience.NewDrainManager(100 * time.Millisecond)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	expectedErr := errors.New("flush failed")
	dm.Register("failing-sink", func(ctx context.Context) error {
		return expectedErr
	})

	err = dm.Drain(context.Background())
	if !errors.Is(err, expectedErr) {
		t.Fatalf("expected error wrapped, got %v", err)
	}
}

func TestDrainManager_ValidationAndDefaults(t *testing.T) {
	_, err := resilience.NewDrainManager(-1 * time.Second)
	if err == nil {
		t.Fatal("expected error for negative timeout")
	}

	_, err = resilience.NewDrainManager(0)
	if err == nil {
		t.Fatal("expected error for 0 timeout")
	}
}

func TestSetupSignalTrap(t *testing.T) {
	ctx, cancel := resilience.SetupSignalTrap()
	defer cancel()

	if ctx.Err() != nil {
		t.Fatal("context should not be cancelled yet")
	}
}
