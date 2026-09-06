// package streams validates the ParquetSinkDoFn atomic bundle lifecycle.
package streams

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

		"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

type fakeStorageWriter struct {
	tempCreated   string
	finalURI      string
	createCalls   int
	tempCommitted bool
	tempAborted   bool
	buf           bytes.Buffer
	createErr     error
	commitErr     error
}

func (f *fakeStorageWriter) CreateTemp(_ context.Context, finalURI string) (string, io.WriteCloser, error) {
	f.createCalls++
	if f.createErr != nil {
		return "", nil, f.createErr
	}
	f.finalURI = finalURI
	f.tempCreated = "temp-" + finalURI
	return f.tempCreated, &nopWriteCloser{&f.buf}, nil
}

func (f *fakeStorageWriter) CommitTemp(_ context.Context, _, _ string) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.tempCommitted = true
	return nil
}

func (f *fakeStorageWriter) AbortTemp(_ context.Context, _ string) error {
	f.tempAborted = true
	return nil
}

type nopWriteCloser struct {
	io.Writer
}

func (n *nopWriteCloser) Close() error {
	return nil
}

func TestParquetSinkDoFn_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		recordIDs   []int64
		wantCreate  int
		wantCommit  bool
		wantAbort   bool
	}{
		{
			name:        "writes records and commits temp file",
			recordIDs:   []int64{10, 20, 30},
			wantCreate:  1,
			wantCommit:  true,
			wantAbort:   false,
		},
		{
			name:        "empty bundle does not create or commit temp file",
			recordIDs:   []int64{},
			wantCreate:  0,
			wantCommit:  false,
			wantAbort:   false,
		},
	}

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fake := &fakeStorageWriter{}
			fn := NewParquetSinkDoFn(fake, "gs://test-bucket/output", "snappy", schema)

			ctx := context.Background()
			if err := fn.StartBundle(ctx); err != nil {
				t.Fatalf("StartBundle failed: %v", err)
			}

			for _, id := range tc.recordIDs {
				rec := domain.NewGenericRecord("test-schema", 1)
				rec.SetInt64(0, id)
				if err := fn.ProcessElement(ctx, rec); err != nil {
					t.Fatalf("ProcessElement failed: %v", err)
				}
			}

			if err := fn.FinishBundle(ctx); err != nil {
				t.Fatalf("FinishBundle failed: %v", err)
			}

			if fake.createCalls != tc.wantCreate {
				t.Errorf("createCalls = %d, want %d", fake.createCalls, tc.wantCreate)
			}
			if fake.tempCommitted != tc.wantCommit {
				t.Errorf("tempCommitted = %v, want %v", fake.tempCommitted, tc.wantCommit)
			}
			if fake.tempAborted != tc.wantAbort {
				t.Errorf("tempAborted = %v, want %v", fake.tempAborted, tc.wantAbort)
			}
		})
	}
}

func TestParquetSinkDoFn_CreateTempError(t *testing.T) {
	schema := domain.Schema{Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}}}
	fake := &fakeStorageWriter{createErr: fmt.Errorf("storage permission denied")}
	fn := NewParquetSinkDoFn(fake, "gs://test-bucket/output", "none", schema)

	ctx := context.Background()
	if err := fn.StartBundle(ctx); err != nil {
		t.Fatalf("StartBundle unexpected error: %v", err)
	}

	rec := domain.NewGenericRecord("test-schema", 1)
	rec.SetInt64(0, 1)
	err := fn.ProcessElement(ctx, rec)
	if err == nil {
		t.Error("ProcessElement expected error on CreateTemp failure, got nil")
	}
}

func TestParquetSinkDoFn_BatchFlush(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}
	fake := &fakeStorageWriter{}
	fn := NewParquetSinkDoFn(fake, "gs://test-bucket/output", "snappy", schema)

	ctx := context.Background()
	_ = fn.StartBundle(ctx)

	for _, id := range []int64{100, 200, 300} {
		rec := domain.NewGenericRecord("schema", 1)
		rec.SetInt64(0, id)
		if err := fn.ProcessElement(ctx, rec); err != nil {
			t.Fatalf("ProcessElement: %v", err)
		}
	}

	if err := fn.FinishBundle(ctx); err != nil {
		t.Fatalf("FinishBundle: %v", err)
	}

	if fake.createCalls != 1 {
		t.Errorf("createCalls = %d, want 1", fake.createCalls)
	}
	if !fake.tempCommitted {
		t.Errorf("expected temp file to be committed")
	}
}
