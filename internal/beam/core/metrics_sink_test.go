package core_test

import (
	"context"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestMetricsSinkDoFn_ProcessElement_WritesJSON(t *testing.T) {
	ctx := context.Background()
	storage := newFakeStorageWriter()

	fn := core.NewMetricsSinkDoFn(storage, "/tmp/output")
	_ = fn.Setup(ctx)

	m := domain.PipelineMetrics{
		TotalRecordsRead: 100,
		RowsWritten:      97,
		DeadLetterCount:  3,
	}
	if err := fn.ProcessElement(ctx, m); err != nil {
		t.Fatalf("ProcessElement: %v", err)
	}

	if len(storage.comitted) != 1 {
		t.Fatalf("expected 1 committed metrics file, got %d", len(storage.comitted))
	}

	for _, buf := range storage.created {
		content := buf.String()
		if !strings.Contains(content, `"total_records_read": 100`) {
			t.Errorf("expected JSON to contain total_records_read: 100, got: %s", content)
		}
	}
}

func TestMetricsSinkDoFn_ProcessElement_NilStorageReturnsError(t *testing.T) {
	orig := core.GetStorageFactory()
	core.SetStorageFactory(nil)
	defer core.SetStorageFactory(orig)

	fn := core.NewMetricsSinkDoFn(nil, "/tmp/output")
	err := fn.ProcessElement(context.Background(), domain.PipelineMetrics{})
	if err == nil {
		t.Fatal("expected error when storage is nil")
	}
}
