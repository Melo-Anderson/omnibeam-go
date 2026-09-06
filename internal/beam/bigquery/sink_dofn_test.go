package bigquery_test

import (
	"context"
	"fmt"
	"testing"

	bigquery_beam "github.com/omnibeam/dataflow-compute-go/internal/beam/bigquery"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type mockBigQueryWriter struct {
	openCalled   bool
	commitCalled bool
	failAppend   bool
	recordsCount int
	dlqRecords   []*domain.DeadLetterRecord
}

var _ ports.BigQueryWriter = (*mockBigQueryWriter)(nil)

func (m *mockBigQueryWriter) OpenStream(_ context.Context, projectID, datasetID, tableID string) (string, error) {
	m.openCalled = true
	return fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/mock_stream", projectID, datasetID, tableID), nil
}

func (m *mockBigQueryWriter) AppendRecords(_ context.Context, _ string, offset int64, records []*domain.GenericRecord, _ *domain.Schema) (int64, []*domain.DeadLetterRecord, error) {
	if m.failAppend {
		return 0, nil, fmt.Errorf("simulated append failure")
	}
	m.recordsCount += len(records)
	return offset + int64(len(records)), m.dlqRecords, nil
}

func (m *mockBigQueryWriter) CommitStream(_ context.Context, _ string) error {
	m.commitCalled = true
	return nil
}

func (m *mockBigQueryWriter) Close() error {
	return nil
}

func TestBigQuerySinkDoFn_HappyPath(t *testing.T) {
	ctx := context.Background()
	writer := &mockBigQueryWriter{}

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "val", Type: domain.TypeString, Nullable: false},
		},
	}
	cfg := domain.BigQuerySinkConfig{ProjectID: "proj", DatasetID: "ds", TableID: "tbl", BatchSize: 10}

	fn := bigquery_beam.NewBigQuerySinkDoFn(writer, cfg, schema)

	if err := fn.Setup(ctx); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if err := fn.StartBundle(ctx); err != nil {
		t.Fatalf("StartBundle: %v", err)
	}

	rec := domain.NewGenericRecord("test", 2)
	rec.SetInt64(0, 42)
	rec.SetString(1, "hello")

	var dlqEmitted *domain.DeadLetterRecord
	emitDLQ := func(d *domain.DeadLetterRecord) { dlqEmitted = d }

	if err := fn.ProcessElement(ctx, rec, emitDLQ); err != nil {
		t.Fatalf("ProcessElement: %v", err)
	}
	if dlqEmitted != nil {
		t.Fatalf("unexpected DLQ record: %v", dlqEmitted)
	}
	if err := fn.FinishBundle(ctx); err != nil {
		t.Fatalf("FinishBundle: %v", err)
	}
	if !writer.openCalled || !writer.commitCalled || writer.recordsCount != 1 {
		t.Fatalf("writer state invalid: open=%v, commit=%v, count=%d", writer.openCalled, writer.commitCalled, writer.recordsCount)
	}
}

func TestBigQuerySinkDoFn_DLQRouting(t *testing.T) {
	ctx := context.Background()
	dlqRec := &domain.DeadLetterRecord{ErrorMessage: "bad row"}
	writer := &mockBigQueryWriter{dlqRecords: []*domain.DeadLetterRecord{dlqRec}}

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}
	cfg := domain.BigQuerySinkConfig{ProjectID: "p", DatasetID: "d", TableID: "t", BatchSize: 1}
	fn := bigquery_beam.NewBigQuerySinkDoFn(writer, cfg, schema)
	_ = fn.Setup(ctx)
	_ = fn.StartBundle(ctx)

	rec := domain.NewGenericRecord("test", 1)

	var dlqEmitted *domain.DeadLetterRecord
	_ = fn.ProcessElement(ctx, rec, func(d *domain.DeadLetterRecord) { dlqEmitted = d })

	if dlqEmitted == nil {
		t.Fatal("expected a DLQ record when writer returns DLQ items")
	}
}

func TestBigQuerySinkDoFn_FinishBundle_DLQNotLost(t *testing.T) {
	ctx := context.Background()
	dlqRec := &domain.DeadLetterRecord{ErrorMessage: "serialization error in final flush"}
	writer := &mockBigQueryWriter{dlqRecords: []*domain.DeadLetterRecord{dlqRec}}

	schema := domain.Schema{Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}}}
	cfg := domain.BigQuerySinkConfig{
		ProjectID: "p", DatasetID: "d", TableID: "t",
		BatchSize: 100, // large enough so mid-bundle flush does NOT trigger
	}

	fn := bigquery_beam.NewBigQuerySinkDoFn(writer, cfg, schema)
	_ = fn.Setup(ctx)
	_ = fn.StartBundle(ctx)

	var dlqEmitted *domain.DeadLetterRecord
	emitDLQ := func(d *domain.DeadLetterRecord) { dlqEmitted = d }

	rec := domain.NewGenericRecord("test", 1)
	_ = fn.ProcessElement(ctx, rec, emitDLQ)

	if err := fn.FinishBundle(ctx); err != nil {
		t.Fatalf("FinishBundle: %v", err)
	}

	if dlqEmitted == nil {
		t.Fatal("expected DLQ record to be emitted during FinishBundle flush — conservation invariant violated")
	}
}
