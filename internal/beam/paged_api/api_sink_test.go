package paged_api

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// mockBatchAPIWriter simulates an API that fails all records to DLQ.
type mockBatchAPIWriter struct {
	failAll bool
}

func (m *mockBatchAPIWriter) WriteBatch(_ context.Context, records []*domain.GenericRecord, _ *domain.Schema) ([]*domain.GenericRecord, []*domain.DeadLetterRecord, error) {
	if m.failAll {
		var dlqs []*domain.DeadLetterRecord
		for range records {
			dlqs = append(dlqs, &domain.DeadLetterRecord{
				RawPayload:   "err",
				ErrorMessage: "api failure",
				FailedAt:     time.Now().UTC(),
			})
		}
		return nil, dlqs, nil
	}
	return records, nil, nil
}

func TestAPISinkDoFn_SerializationRoundTrip(t *testing.T) {
	t.Run("exported fields survive JSON round-trip and Setup does not panic", func(t *testing.T) {
		schema := domain.Schema{
			Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}},
		}
		endpoint := domain.APIEndpointConfig{
			BaseURL: "http://api.example.com/items",
		}
		opts := domain.APISinkOptions{BatchSize: 10}

		original := NewAPISinkDoFn(
			endpoint,
			opts,
			schema,
			"testdata/output/dlq",
			"secret_token_123",
		)

		data, err := json.Marshal(original)
		if err != nil {
			t.Fatalf("json.Marshal: %v", err)
		}

		var worker APISinkDoFn
		if err := json.Unmarshal(data, &worker); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}

		if worker.Endpoint.BaseURL != "http://api.example.com/items" {
			t.Errorf("Endpoint.BaseURL not preserved: got %q", worker.Endpoint.BaseURL)
		}
		if worker.APIOptions.BatchSize != 10 {
			t.Errorf("APIOptions.BatchSize not preserved: got %d", worker.APIOptions.BatchSize)
		}
		if worker.DLQPath != "testdata/output/dlq" {
			t.Errorf("DLQPath not preserved: got %q", worker.DLQPath)
		}
		if worker.SecretToken != "secret_token_123" {
			t.Errorf("SecretToken not preserved: got %q", worker.SecretToken)
		}

		ctx := context.Background()
		if err := worker.Setup(ctx); err != nil {
			t.Logf("Setup returned expected notice without global factory: %v", err)
		}
		if err := worker.StartBundle(ctx); err != nil {
			t.Fatalf("StartBundle: %v", err)
		}
	})
}

func TestAPISinkDoFn_DLQRouting_CommitsOnFailure(t *testing.T) {
	fake := &fakeStorageWriter{}
	writer := &mockBatchAPIWriter{failAll: true}
	schema := domain.Schema{}
	endpoint := domain.APIEndpointConfig{BaseURL: "http://example.com"}
	opts := domain.APISinkOptions{BatchSize: 1}

	fn := NewAPISinkDoFn(endpoint, opts, schema, "gs://test-bucket/api-dlq", "").
		WithWriter(writer).
		WithDLQSink(fake)

	ctx := context.Background()
	if err := fn.StartBundle(ctx); err != nil {
		t.Fatalf("StartBundle failed: %v", err)
	}

	rec := domain.NewGenericRecord("test", 1)
	if err := fn.ProcessElement(ctx, rec); err != nil {
		t.Fatalf("ProcessElement failed: %v", err)
	}
	if err := fn.FinishBundle(ctx); err != nil {
		t.Fatalf("FinishBundle failed: %v", err)
	}

	if !fake.tempCommitted {
		t.Errorf("expected DLQ temp file to be committed, but it was not")
	}
}

func TestAPISinkDoFn_NoDLQ_WhenAllSucceed(t *testing.T) {
	fake := &fakeStorageWriter{}
	writer := &mockBatchAPIWriter{failAll: false}
	schema := domain.Schema{}
	endpoint := domain.APIEndpointConfig{BaseURL: "http://example.com"}
	opts := domain.APISinkOptions{BatchSize: 2}

	fn := NewAPISinkDoFn(endpoint, opts, schema, "gs://test-bucket/api-dlq", "").
		WithWriter(writer).
		WithDLQSink(fake)

	ctx := context.Background()
	if err := fn.StartBundle(ctx); err != nil {
		t.Fatalf("StartBundle failed: %v", err)
	}

	rec := domain.NewGenericRecord("test", 1)
	if err := fn.ProcessElement(ctx, rec); err != nil {
		t.Fatalf("ProcessElement failed: %v", err)
	}
	if err := fn.FinishBundle(ctx); err != nil {
		t.Fatalf("FinishBundle failed: %v", err)
	}

	if fake.tempCommitted {
		t.Errorf("expected no DLQ commit when all records succeed")
	}
}

type errBatchWriter struct{}

func (errBatchWriter) WriteBatch(_ context.Context, _ []*domain.GenericRecord, _ *domain.Schema) ([]*domain.GenericRecord, []*domain.DeadLetterRecord, error) {
	return nil, nil, errors.New("network failure")
}

func TestAPISinkDoFn_SetupWithFactory_And_FlushError(t *testing.T) {
	SetAPIWriterFactory(func(endpoint domain.APIEndpointConfig, opts domain.APISinkOptions, secretToken string) ports.BatchAPIWriter {
		return &mockBatchAPIWriter{failAll: false}
	})
	t.Cleanup(func() { SetAPIWriterFactory(nil) })

	fn := NewAPISinkDoFn(domain.APIEndpointConfig{BaseURL: "http://example.com"}, domain.APISinkOptions{}, domain.Schema{}, "", "")
	err := fn.Setup(context.Background())
	if err != nil {
		t.Fatalf("unexpected Setup error: %v", err)
	}

	// Test flush with hard writer error
	fnErr := NewAPISinkDoFn(domain.APIEndpointConfig{}, domain.APISinkOptions{BatchSize: 1}, domain.Schema{}, "", "").
		WithWriter(&errBatchWriter{})
	_ = fnErr.StartBundle(context.Background())
	rec := domain.NewGenericRecord("test", 1)
	err = fnErr.ProcessElement(context.Background(), rec)
	if err == nil {
		t.Error("expected error from ProcessElement on hard write failure, got nil")
	}

	// Test DLQ write on partial/complete DLQ fail
	fakeDLQ := &fakeStorageWriter{}
	fnDLQ := NewAPISinkDoFn(domain.APIEndpointConfig{}, domain.APISinkOptions{BatchSize: 1}, domain.Schema{}, "/tmp/dlq", "").
		WithWriter(&mockBatchAPIWriter{failAll: true}).
		WithDLQSink(fakeDLQ)
	_ = fnDLQ.StartBundle(context.Background())
	err = fnDLQ.ProcessElement(context.Background(), rec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	err = fnDLQ.FinishBundle(context.Background())
	if err != nil {
		t.Fatalf("unexpected FinishBundle error: %v", err)
	}
	if !fakeDLQ.tempCommitted {
		t.Error("expected DLQ temp file to be committed")
	}
}
