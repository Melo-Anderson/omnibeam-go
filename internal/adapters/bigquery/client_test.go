package bigquery_test

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/bigquery"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestStorageWriteAdapter_Lifecycle(t *testing.T) {
	ctx := context.Background()
	client, err := bigquery.NewStorageWriteAdapter(ctx)
	if err != nil {
		t.Fatalf("NewStorageWriteAdapter: %v", err)
	}
	defer client.Close()

	stream, err := client.OpenStream(ctx, "proj", "ds", "tbl")
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}
	schema.Index()

	rec1 := domain.NewGenericRecord("test", 1)
	rec1.SetInt64(0, 1)

	rec2 := domain.NewGenericRecord("test", 1)
	rec2.SetInt64(0, 2)

	records := []*domain.GenericRecord{rec1, rec2}

	newOffset, dlq, err := client.AppendRecords(ctx, stream, 0, records, schema)
	if err != nil {
		t.Fatalf("AppendRecords: %v", err)
	}
	if newOffset != 2 {
		t.Errorf("expected offset 2, got %d", newOffset)
	}
	if len(dlq) != 0 {
		t.Errorf("expected 0 DLQ records, got %d", len(dlq))
	}

	if err := client.CommitStream(ctx, stream); err != nil {
		t.Fatalf("CommitStream: %v", err)
	}
}
