package resilience_test

import (
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/pkg/resilience"
)

func TestAdaptiveBatchSizer_ExpandsAndThrottles(t *testing.T) {
	sizer := resilience.NewAdaptiveBatchSizer(500, 10000)

	if sizer.CurrentBatchSize() != 500 {
		t.Fatalf("expected initial batch size 500, got %d", sizer.CurrentBatchSize())
	}

	// Fast flush (< 200ms) -> expand
	for i := 0; i < 5; i++ {
		sizer.RecordFlush(50 * time.Millisecond)
	}
	if sizer.CurrentBatchSize() <= 500 {
		t.Errorf("expected batch size to expand under low latency, got %d", sizer.CurrentBatchSize())
	}

	// Slow flush (> 1500ms) -> throttle
	sizer.RecordFlush(2 * time.Second)
	if sizer.CurrentBatchSize() > 5000 {
		t.Errorf("expected batch size to throttle under high latency, got %d", sizer.CurrentBatchSize())
	}
}

func TestAdaptiveBatchSizer_DefaultsAndBounds(t *testing.T) {
	sizer := resilience.NewAdaptiveBatchSizer(0, 0)
	if sizer.CurrentBatchSize() != 500 {
		t.Errorf("expected default min size 500, got %d", sizer.CurrentBatchSize())
	}
}
