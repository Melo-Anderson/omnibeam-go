// package streams provides Splittable DoFn (SDF) restriction trackers for distributed source reading.
package streams

import (
	"bytes"
	"fmt"
	"math"
	"sync"

	beamsdf "github.com/apache/beam/sdks/v2/go/pkg/beam/core/sdf"
)

var _ beamsdf.RTracker = (*ByteOffsetTracker)(nil)

// ByteOffsetRange is the restriction for stream byte-offset splitting.
// The range is [Start, End) — Start is inclusive, End is exclusive.
type ByteOffsetRange struct {
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// ByteOffsetTracker manages claiming and splitting byte offset ranges.
// Thread-safe: TryClaim, TrySplit, and IsDone may be called concurrently.
type ByteOffsetTracker struct {
	mu      sync.Mutex
	rest    ByteOffsetRange
	claimed int64
	stopped bool
	err     error
}

// NewByteOffsetTracker initializes a tracker for the given restriction.
func NewByteOffsetTracker(rest ByteOffsetRange) *ByteOffsetTracker {
	return &ByteOffsetTracker{
		rest:    rest,
		claimed: rest.Start - 1,
	}
}

// ClaimOffset attempts to claim the given byte offset directly without any interface conversions.
func (t *ByteOffsetTracker) ClaimOffset(pos int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped {
		return false
	}

	if pos < t.rest.Start || pos >= t.rest.End || pos <= t.claimed {
		t.stopped = true
		return false
	}
	t.claimed = pos
	return true
}

// TryClaim attempts to claim the given byte position (satisfying beamsdf.RTracker).
func (t *ByteOffsetTracker) TryClaim(rawPos any) bool {
	var pos int64
	switch p := rawPos.(type) {
	case int64:
		pos = p
	case int:
		pos = int64(p)
	default:
		t.mu.Lock()
		t.err = fmt.Errorf("unsupported claim position type %T: %v", rawPos, rawPos)
		t.stopped = true
		t.mu.Unlock()
		return false
	}
	return t.ClaimOffset(pos)
}

// TrySplit dynamically splits the remaining restriction at fraction of remaining bytes.
func (t *ByteOffsetTracker) TrySplit(fraction float64) (primary, residual any, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.stopped || fraction <= 0.0 || fraction >= 1.0 {
		return nil, nil, nil
	}

	current := t.claimed
	if current < t.rest.Start {
		current = t.rest.Start
	}

	remaining := t.rest.End - current
	if remaining < 2 {
		return nil, nil, nil
	}

	splitPoint := current + int64(math.Round(float64(remaining)*fraction))
	if splitPoint <= current || splitPoint >= t.rest.End {
		return nil, nil, nil
	}

	primaryRange := ByteOffsetRange{Start: t.rest.Start, End: splitPoint}
	residualRange := ByteOffsetRange{Start: splitPoint, End: t.rest.End}
	t.rest.End = splitPoint
	return primaryRange, residualRange, nil
}

// GetRestriction returns the current restriction.
func (t *ByteOffsetTracker) GetRestriction() any {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.rest
}

// GetProgress returns work completed and work remaining.
func (t *ByteOffsetTracker) GetProgress() (done, remaining float64) {
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
func (t *ByteOffsetTracker) GetError() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// MarkDone marks the tracker as fully processed.
func (t *ByteOffsetTracker) MarkDone() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stopped = true
}

// IsDone returns true when all bytes have been claimed or stopped.
func (t *ByteOffsetTracker) IsDone() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped || t.claimed >= t.rest.End-1
}

// SyncToNextRecordBoundary finds the start of the next record at or after startOffset,
// respecting RFC 4180 quote parity so embedded newlines inside quoted fields are not split.
// If multiline is false, it uses a high-performance fast-path searching directly for '\n'.
func SyncToNextRecordBoundary(data []byte, startOffset int64, quoteChar byte, multiline ...bool) int64 {
	if startOffset <= 0 || int(startOffset) >= len(data) {
		return startOffset
	}

	isMulti := true
	if len(multiline) > 0 {
		isMulti = multiline[0]
	}

	if !isMulti {
		idx := bytes.IndexByte(data[startOffset:], '\n')
		if idx == -1 {
			return int64(len(data))
		}
		return startOffset + int64(idx) + 1
	}

	if quoteChar == 0 {
		quoteChar = '"'
	}

	inQuotes := false
	for i := 0; i < len(data); i++ {
		b := data[i]
		if b == quoteChar {
			if inQuotes && i+1 < len(data) && data[i+1] == quoteChar {
				i++ // skip doubled quote escape ""
				continue
			}
			if i > 0 && data[i-1] == '\\' {
				continue // skip backslash escape \"
			}
			inQuotes = !inQuotes
		} else if b == '\n' && !inQuotes {
			nextStart := int64(i + 1)
			if nextStart >= startOffset {
				return nextStart
			}
		}
	}
	return int64(len(data))
}

// SyncToNextNewline returns the byte offset immediately after the next '\n' found at or after startOffset.
func SyncToNextNewline(data []byte, startOffset int64) int64 {
	return SyncToNextRecordBoundary(data, startOffset, '"', false)
}
