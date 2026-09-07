// package paged_api provides Splittable DoFn (SDF) restriction trackers for distributed source reading.
package paged_api

import (
	"fmt"
	"math"
	"sync"

	beamsdf "github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
)

var _ beamsdf.RTracker = (*PageRangeTracker)(nil)

// PageRange is the restriction for discrete page index or cursor sequence splitting.
// The range is [Start, End) — Start is inclusive, End is exclusive.
type PageRange struct {
	Start int `json:"start"`
	End   int `json:"end"`
}

// PageRangeTracker manages claiming and splitting discrete API page ranges.
// Thread-safe: TryClaim, TrySplit, and IsDone may be called concurrently.
type PageRangeTracker struct {
	mu      sync.Mutex
	rest    PageRange
	claimed int
	stopped bool
	err     error
}

// NewPageRangeTracker initializes a tracker for the given page restriction.
func NewPageRangeTracker(rest PageRange) *PageRangeTracker {
	return &PageRangeTracker{
		rest:    rest,
		claimed: rest.Start - 1,
	}
}

// TryClaim attempts to claim the given page index.
func (t *PageRangeTracker) TryClaim(rawIndex any) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped {
		return false
	}

	var index int
	switch idx := rawIndex.(type) {
	case int:
		index = idx
	case int64:
		index = int(idx)
	default:
		t.err = fmt.Errorf("unsupported claim page index type %T: %v", rawIndex, rawIndex)
		t.stopped = true
		return false
	}

	if index < t.rest.Start || index >= t.rest.End || index <= t.claimed {
		t.stopped = true
		return false
	}
	t.claimed = index
	return true
}

// TrySplit dynamically splits the remaining unread pages at the given fraction.
func (t *PageRangeTracker) TrySplit(fraction float64) (primary, residual any, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped || fraction <= 0.0 || fraction >= 1.0 {
		return nil, nil, nil
	}

	remaining := t.rest.End - t.claimed - 1
	splitPoint := t.claimed + 1 + int(math.Round(float64(remaining)*fraction))
	if splitPoint <= t.claimed+1 || splitPoint >= t.rest.End {
		return nil, nil, nil
	}

	primaryRange := PageRange{Start: t.rest.Start, End: splitPoint}
	residualRange := PageRange{Start: splitPoint, End: t.rest.End}
	t.rest.End = splitPoint
	return primaryRange, residualRange, nil
}

// GetRestriction returns the current page range restriction.
func (t *PageRangeTracker) GetRestriction() any {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rest
}

// GetProgress returns page progress metrics.
func (t *PageRangeTracker) GetProgress() (done, remaining float64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	done = float64(t.claimed - t.rest.Start + 1)
	remaining = float64(t.rest.End - t.claimed - 1)
	if remaining < 0 {
		remaining = 0
	}
	return done, remaining
}

// GetError returns any tracker error encountered.
func (t *PageRangeTracker) GetError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// MarkDone marks the tracker as fully processed.
func (t *PageRangeTracker) MarkDone() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
}

// IsDone returns true when all pages in the range have been claimed or stopped.
func (t *PageRangeTracker) IsDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped || t.claimed >= t.rest.End-1
}
