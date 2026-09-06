package streams

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type countingStorageWriter struct {
	createCalls int
	commitCalls int
	abortCalls  int
	buf         bytes.Buffer
}

func (c *countingStorageWriter) CreateTemp(_ context.Context, uri string) (string, io.WriteCloser, error) {
	c.createCalls++
	return "tmp-" + uri, &nopWriteCloser{Writer: &c.buf}, nil
}

func (c *countingStorageWriter) CommitTemp(_ context.Context, _, _ string) error {
	c.commitCalls++
	return nil
}

func (c *countingStorageWriter) AbortTemp(_ context.Context, _ string) error {
	c.abortCalls++
	return nil
}

type fakeFormatter struct{}

func (f *fakeFormatter) FormatHeader(s *domain.Schema) ([]byte, error) {
	return []byte("id\n"), nil
}

func (f *fakeFormatter) FormatRecord(rec *domain.GenericRecord, s *domain.Schema) ([]byte, error) {
	return []byte("1\n"), nil
}

func TestDelimitedFileSinkDoFn_EmptyBundle_NoStorageCalls(t *testing.T) {
	t.Run("empty bundle calls neither CreateTemp nor CommitTemp nor AbortTemp", func(t *testing.T) {
		fake := &countingStorageWriter{}
		core.SetStorageFactory(func(_ string) ports.StorageWriter { return fake })
		t.Cleanup(func() { core.SetStorageFactory(nil) })

		fn := NewDelimitedFileSinkDoFn(fake, &fakeFormatter{}, "testdata/output/e2e_01", "csv", "none", false, domain.Schema{})

		ctx := context.Background()
		if err := fn.StartBundle(ctx); err != nil {
			t.Fatalf("StartBundle: %v", err)
		}
		// No ProcessElement calls — empty bundle.
		if err := fn.FinishBundle(ctx); err != nil {
			t.Fatalf("FinishBundle: %v", err)
		}

		if fake.createCalls != 0 {
			t.Errorf("CreateTemp called %d times for empty bundle, want 0", fake.createCalls)
		}
		if fake.commitCalls != 0 {
			t.Errorf("CommitTemp called %d times for empty bundle, want 0", fake.commitCalls)
		}
		if fake.abortCalls != 0 {
			t.Errorf("AbortTemp called %d times for empty bundle, want 0", fake.abortCalls)
		}
	})

	t.Run("non-empty bundle calls CreateTemp on first record and commits on finish", func(t *testing.T) {
		fake := &countingStorageWriter{}
		fn := NewDelimitedFileSinkDoFn(fake, &fakeFormatter{}, "testdata/output/e2e_01", "csv", "none", false, domain.Schema{})

		ctx := context.Background()
		if err := fn.StartBundle(ctx); err != nil {
			t.Fatalf("StartBundle: %v", err)
		}

		rec := domain.NewGenericRecord("test.csv", 1)
		rec.SetInt64(0, 1)
		if err := fn.ProcessElement(ctx, rec); err != nil {
			t.Fatalf("ProcessElement: %v", err)
		}

		if fake.createCalls != 1 {
			t.Errorf("CreateTemp called %d times after 1 record, want 1", fake.createCalls)
		}

		if err := fn.FinishBundle(ctx); err != nil {
			t.Fatalf("FinishBundle: %v", err)
		}

		if fake.commitCalls != 1 {
			t.Errorf("CommitTemp called %d times on finish, want 1", fake.commitCalls)
		}

		if len(fn.Checksum()) == 0 {
			t.Error("expected non-empty checksum after writing records")
		}
	})

	t.Run("abort cleans up temp files properly", func(t *testing.T) {
		fake := &countingStorageWriter{}
		fn := NewDelimitedFileSinkDoFn(fake, &fakeFormatter{}, "testdata/output", "csv", "none", false, domain.Schema{})
		ctx := context.Background()
		_ = fn.StartBundle(ctx)

		rec := domain.NewGenericRecord("test.csv", 1)
		_ = fn.ProcessElement(ctx, rec)

		fn.abort(ctx)
		if fake.abortCalls != 1 {
			t.Errorf("expected 1 abortCall, got %d", fake.abortCalls)
		}
	})
}
