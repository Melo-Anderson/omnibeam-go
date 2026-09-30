package core

import (
	"context"
	"errors"
	"testing"
)

func TestBundleBatchBuffer_AddAndFlush(t *testing.T) {
	ctx := context.Background()
	var flushedBatches [][]int

	flushFn := func(_ context.Context, batch []int) error {
		copyBatch := make([]int, len(batch))
		copy(copyBatch, batch)
		flushedBatches = append(flushedBatches, copyBatch)
		return nil
	}

	buf := NewBundleBatchBuffer(3, flushFn)
	if buf.TargetBatchSize() != 3 {
		t.Fatalf("expected batchSize 3, got %d", buf.TargetBatchSize())
	}

	// Add 2 items -> no flush yet
	_ = buf.Add(ctx, 1)
	_ = buf.Add(ctx, 2)
	if len(flushedBatches) != 0 {
		t.Fatalf("expected 0 flushes, got %d", len(flushedBatches))
	}
	if buf.Count() != 2 {
		t.Fatalf("expected 2 items, got %d", buf.Count())
	}

	// Add 3rd item -> triggers auto-flush
	_ = buf.Add(ctx, 3)
	if len(flushedBatches) != 1 {
		t.Fatalf("expected 1 flush, got %d", len(flushedBatches))
	}
	if len(flushedBatches[0]) != 3 {
		t.Fatalf("expected 3 items in batch, got %d", len(flushedBatches[0]))
	}
	if buf.Count() != 0 {
		t.Fatalf("expected 0 items remaining, got %d", buf.Count())
	}

	// Add 4th item -> buffer holds 1 item
	_ = buf.Add(ctx, 4)
	if buf.Count() != 1 {
		t.Fatalf("expected 1 item, got %d", buf.Count())
	}

	// Explicit Flush (FinishBundle) -> flushes remaining item
	if err := buf.Flush(ctx); err != nil {
		t.Fatalf("Flush failed: %v", err)
	}
	if len(flushedBatches) != 2 {
		t.Fatalf("expected 2 flushes total, got %d", len(flushedBatches))
	}
	if len(flushedBatches[1]) != 1 || flushedBatches[1][0] != 4 {
		t.Fatalf("unexpected flushed batch: %v", flushedBatches[1])
	}
}

func TestBundleBatchBuffer_ResetAndResize(t *testing.T) {
	buf := NewBundleBatchBuffer[string](2, nil)
	ctx := context.Background()

	_ = buf.Add(ctx, "a")
	buf.Reset(5)

	if buf.Count() != 0 {
		t.Errorf("expected count 0 after reset, got %d", buf.Count())
	}
	if buf.TargetBatchSize() != 5 {
		t.Errorf("expected targetBatchSize 5, got %d", buf.TargetBatchSize())
	}

	buf.SetBatchSize(10)
	if buf.TargetBatchSize() != 10 {
		t.Errorf("expected targetBatchSize 10, got %d", buf.TargetBatchSize())
	}
}

func TestBundleBatchBuffer_FlushError(t *testing.T) {
	ctx := context.Background()
	errFlush := errors.New("flush failed")
	buf := NewBundleBatchBuffer(1, func(_ context.Context, _ []int) error {
		return errFlush
	})

	err := buf.Add(ctx, 42)
	if !errors.Is(err, errFlush) {
		t.Errorf("expected error %v, got %v", errFlush, err)
	}
}
