package resilience

import (
	"runtime"
	"sync"
	"time"
)

const (
	// DefaultAdaptiveMinBatchSize is the fallback minimum records per batch.
	DefaultAdaptiveMinBatchSize = 500
	// DefaultAdaptiveMaxBatchFactor is the default multiplier to compute maxSize from minSize.
	DefaultAdaptiveMaxBatchFactor = 10
	// DefaultAdaptiveFastThreshold is the latency below which batch sizes expand.
	DefaultAdaptiveFastThreshold = 200 * time.Millisecond
	// DefaultAdaptiveSlowThreshold is the latency above which batch sizes contract.
	DefaultAdaptiveSlowThreshold = 1500 * time.Millisecond
	// DefaultAdaptiveStepSize is the minimum increment/decrement step when resizing batches.
	DefaultAdaptiveStepSize = 50
	// DefaultAdaptiveMaxHeapBytes is the memory headroom limit (1 GiB) before throttling to minimum batch.
	DefaultAdaptiveMaxHeapBytes = 1024 * 1024 * 1024
)

// AdaptiveBatchSizer dynamically calculates optimal batch sizes for sinks based on flush latency and memory headroom.
type AdaptiveBatchSizer struct {
	mu            sync.RWMutex
	minSize       int
	maxSize       int
	currentSize   int
	fastThreshold time.Duration
	slowThreshold time.Duration
}

// NewAdaptiveBatchSizer creates a thread-safe adaptive batch sizer with standard defaults.
func NewAdaptiveBatchSizer(minSize, maxSize int) *AdaptiveBatchSizer {
	if minSize <= 0 {
		minSize = DefaultAdaptiveMinBatchSize
	}
	if maxSize < minSize {
		maxSize = minSize * DefaultAdaptiveMaxBatchFactor
	}
	return &AdaptiveBatchSizer{
		minSize:       minSize,
		maxSize:       maxSize,
		currentSize:   minSize,
		fastThreshold: DefaultAdaptiveFastThreshold,
		slowThreshold: DefaultAdaptiveSlowThreshold,
	}
}

// CurrentBatchSize returns the currently calculated optimal batch size.
func (s *AdaptiveBatchSizer) CurrentBatchSize() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentSize
}

// RecordFlush updates the adaptive batch size based on the elapsed duration of the last batch flush.
func (s *AdaptiveBatchSizer) RecordFlush(latency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	if m.HeapInuse > DefaultAdaptiveMaxHeapBytes {
		s.currentSize = s.minSize
		return
	}

	switch {
	case latency < s.fastThreshold:
		s.currentSize = min(s.maxSize, s.currentSize+max(s.currentSize/5, DefaultAdaptiveStepSize))
	case latency > s.slowThreshold:
		s.currentSize = max(s.minSize, s.currentSize-max(s.currentSize/3, DefaultAdaptiveStepSize))
	}
}
