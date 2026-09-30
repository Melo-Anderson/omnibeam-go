package streams

import (
	"bytes"
	"context"
	"encoding/json"
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

		fn := NewDelimitedFileSinkDoFn(fake, &fakeFormatter{}, "testdata/output/e2e_01", "csv", "none", domain.EncryptionConfig{Type: "none"}, false, domain.FormatOptions{}, domain.Schema{})

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
		fn := NewDelimitedFileSinkDoFn(fake, &fakeFormatter{}, "testdata/output/e2e_01", "csv", "none", domain.EncryptionConfig{Type: "none"}, false, domain.FormatOptions{}, domain.Schema{})

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
		fn := NewDelimitedFileSinkDoFn(fake, &fakeFormatter{}, "testdata/output", "csv", "none", domain.EncryptionConfig{Type: "none"}, false, domain.FormatOptions{}, domain.Schema{})
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

// TestDelimitedFileSinkDoFn_FormatOptions_SerializationFidelity validates that
// FormatOptions survive Beam's JSON serialization and are correctly propagated
// to the formatter on worker reconstruction (Phase 2 regression test).
func TestDelimitedFileSinkDoFn_FormatOptions_SerializationFidelity(t *testing.T) {
	opts := domain.FormatOptions{
		Delimiter:      ";",
		IncludeHeader:  false,
		LineTerminator: "\r\n",
	}

	var capturedOpts domain.FormatOptions
	SetRecordFormatterProvider(func(format string, got domain.FormatOptions, schema *domain.Schema) ports.RecordFormatter {
		capturedOpts = got
		return &fakeFormatter{}
	})
	t.Cleanup(func() { SetRecordFormatterProvider(nil) })

	fake := &countingStorageWriter{}
	fn := NewDelimitedFileSinkDoFn(fake, nil, "/out", "csv", "none", domain.EncryptionConfig{Type: "none"}, false, opts, domain.Schema{})

	// Simulate JSON round-trip (Beam worker deserialization)
	jsonBytes, err := json.Marshal(fn)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var restored DelimitedFileSinkDoFn
	if err := json.Unmarshal(jsonBytes, &restored); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	ctx := context.Background()
	// triggers recordFormatterProvider with restored opts
	if err := restored.StartBundle(ctx); err != nil {
		t.Fatalf("StartBundle: %v", err)
	}

	if capturedOpts.Delimiter != ";" {
		t.Errorf("delimiter not preserved: got %q, want %q", capturedOpts.Delimiter, ";")
	}
	if capturedOpts.IncludeHeader != false {
		t.Errorf("include_header not preserved: got %v, want false", capturedOpts.IncludeHeader)
	}
	if capturedOpts.LineTerminator != "\r\n" {
		t.Errorf("line_terminator not preserved: got %q, want %q", capturedOpts.LineTerminator, "\r\n")
	}
}

type trackingWriteCloser struct {
	io.Writer
	closed bool
}

func (t *trackingWriteCloser) Close() error {
	t.closed = true
	return nil
}

func TestDelimitedFileSinkDoFn_EncryptionPipeline(t *testing.T) {
	t.Run("encrypts through registered stream encryptor and closes trailer", func(t *testing.T) {
		encTrack := &trackingWriteCloser{}
		SetStreamEncryptorProvider(func(ctx context.Context, w io.Writer, enc domain.EncryptionConfig) (io.WriteCloser, error) {
			encTrack.Writer = w
			return encTrack, nil
		})
		t.Cleanup(func() { SetStreamEncryptorProvider(nil) })

		fake := &countingStorageWriter{}
		encCfg := domain.EncryptionConfig{Type: "pgp", PublicKeyRef: "keys/pub.asc"}
		fn := NewDelimitedFileSinkDoFn(fake, &fakeFormatter{}, "/out", "csv", "none", encCfg, false, domain.FormatOptions{}, domain.Schema{})

		ctx := context.Background()
		if err := fn.StartBundle(ctx); err != nil {
			t.Fatalf("StartBundle: %v", err)
		}

		rec := domain.NewGenericRecord("test.csv", 1)
		rec.SetInt64(0, 42)
		if err := fn.ProcessElement(ctx, rec); err != nil {
			t.Fatalf("ProcessElement: %v", err)
		}

		if err := fn.FinishBundle(ctx); err != nil {
			t.Fatalf("FinishBundle: %v", err)
		}

		if !encTrack.closed {
			t.Error("expected encryption WriteCloser to be finalized on FinishBundle")
		}
		if fake.commitCalls != 1 {
			t.Errorf("expected commitCalls = 1, got %d", fake.commitCalls)
		}
	})

	t.Run("missing stream encryptor provider fails fail-fast", func(t *testing.T) {
		SetStreamEncryptorProvider(nil)

		fake := &countingStorageWriter{}
		encCfg := domain.EncryptionConfig{Type: "pgp", PublicKeyRef: "keys/pub.asc"}
		fn := NewDelimitedFileSinkDoFn(fake, &fakeFormatter{}, "/out", "csv", "none", encCfg, false, domain.FormatOptions{}, domain.Schema{})

		ctx := context.Background()
		_ = fn.StartBundle(ctx)

		rec := domain.NewGenericRecord("test.csv", 1)
		rec.SetInt64(0, 42)
		err := fn.ProcessElement(ctx, rec)
		if err == nil {
			t.Fatal("expected error when encryptor provider is not initialized")
		}
	})
}
