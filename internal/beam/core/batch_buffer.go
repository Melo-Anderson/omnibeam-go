package core

import "context"

// BatchFlushFunc defines the signature of the callback invoked when a batch reaches capacity or bundle completes.
type BatchFlushFunc[T any] func(ctx context.Context, batch []T) error

// BundleBatchBuffer coordinates in-memory buffering, zero-allocation slicing,
// and flush lifecycle for micro-batched Beam DoFn bundles.
type BundleBatchBuffer[T any] struct {
	batchSize int
	items     []T
	flushFn   BatchFlushFunc[T]
}

// NewBundleBatchBuffer constructs a new BundleBatchBuffer with a target batch size and flush callback.
func NewBundleBatchBuffer[T any](batchSize int, flushFn BatchFlushFunc[T]) *BundleBatchBuffer[T] {
	if batchSize <= 0 {
		batchSize = 100
	}
	return &BundleBatchBuffer[T]{
		batchSize: batchSize,
		items:     make([]T, 0, batchSize),
		flushFn:   flushFn,
	}
}

// Reset clears the buffer for a new bundle and updates target batch size if requested.
// Existing slice memory capacity is preserved to ensure zero-allocation bundle execution.
func (b *BundleBatchBuffer[T]) Reset(batchSize int) {
	if batchSize > 0 {
		b.batchSize = batchSize
	}
	if cap(b.items) < b.batchSize {
		b.items = make([]T, 0, b.batchSize)
	} else {
		b.items = b.items[:0]
	}
}

// SetBatchSize dynamically updates the target batch size (e.g., when modulated by adaptive sizers).
func (b *BundleBatchBuffer[T]) SetBatchSize(size int) {
	if size > 0 {
		b.batchSize = size
	}
}

// TargetBatchSize returns the configured batch size threshold.
func (b *BundleBatchBuffer[T]) TargetBatchSize() int {
	return b.batchSize
}

// Count returns the number of items currently buffered in the active batch.
func (b *BundleBatchBuffer[T]) Count() int {
	return len(b.items)
}

// Items returns a view of the items currently buffered in the active batch.
func (b *BundleBatchBuffer[T]) Items() []T {
	return b.items
}

// Add appends an item to the active batch and automatically flushes when threshold is reached.
func (b *BundleBatchBuffer[T]) Add(ctx context.Context, item T) error {
	b.items = append(b.items, item)
	if len(b.items) >= b.batchSize {
		return b.Flush(ctx)
	}
	return nil
}

// Flush triggers the flush callback with currently buffered items and clears the slice without deallocating.
func (b *BundleBatchBuffer[T]) Flush(ctx context.Context) error {
	if len(b.items) == 0 {
		return nil
	}
	if b.flushFn != nil {
		if err := b.flushFn(ctx, b.items); err != nil {
			return err
		}
	}
	b.items = b.items[:0]
	return nil
}
