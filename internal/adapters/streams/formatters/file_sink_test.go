package formatters

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

type memoryStorage struct {
	files map[string]*bytes.Buffer
}

func newMemoryStorage() *memoryStorage {
	return &memoryStorage{files: make(map[string]*bytes.Buffer)}
}

func (m *memoryStorage) CreateTemp(ctx context.Context, finalURI string) (string, io.WriteCloser, error) {
	buf := &bytes.Buffer{}
	tempURI := finalURI + ".tmp"
	m.files[tempURI] = buf
	return tempURI, &nopCloser{buf}, nil
}

func (m *memoryStorage) CommitTemp(ctx context.Context, tempURI, finalURI string) error {
	m.files[finalURI] = m.files[tempURI]
	delete(m.files, tempURI)
	return nil
}

func (m *memoryStorage) AbortTemp(ctx context.Context, tempURI string) error {
	delete(m.files, tempURI)
	return nil
}

type nopCloser struct {
	*bytes.Buffer
}

func (n *nopCloser) Close() error {
	return nil
}

func TestDelimitedFileWriter_GzipAndChecksum(t *testing.T) {
	storage := newMemoryStorage()
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
	}

	opts := domain.FormatOptions{
		Delimiter:     ",",
		IncludeHeader: true,
	}
	formatter := NewCSVFormatter(opts)

	writer := NewDelimitedFileWriter(storage, formatter, "gzip")

	finalURI := "/tmp/test.csv.gz"
	ctx := context.Background()

	wCtx, err := writer.Open(ctx, finalURI, schema)
	if err != nil {
		t.Fatalf("failed to open writer: %v", err)
	}

	rec := domain.NewGenericRecord("src", 2)
	rec.SetInt64(0, 1)
	rec.SetString(1, "Test")

	if err := writer.Write(wCtx, rec); err != nil {
		t.Fatalf("failed to write record: %v", err)
	}

	metrics, err := writer.Close(ctx, wCtx)
	if err != nil {
		t.Fatalf("failed to close writer: %v", err)
	}

	if metrics.RowsWritten != 1 {
		t.Errorf("expected 1 row written, got %d", metrics.RowsWritten)
	}
	if metrics.BytesWritten <= 0 {
		t.Errorf("expected positive bytes written, got %d", metrics.BytesWritten)
	}
	if metrics.Checksum == "" {
		t.Errorf("expected non-empty checksum, got empty string")
	}

	// Verify gzip contents
	buf := storage.files[finalURI]
	if buf == nil {
		t.Fatalf("file %s was not written to storage", finalURI)
	}

	gzReader, err := gzip.NewReader(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to create gzip reader: %v", err)
	}
	decompressed, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("failed to read decompressed gzip data: %v", err)
	}

	expectedContent := "id,name\n1,Test\n"
	if string(decompressed) != expectedContent {
		t.Errorf("expected %q, got %q", expectedContent, string(decompressed))
	}

	// Verify checksum matches sha256 of the written output
	hasher := sha256.New()
	hasher.Write(buf.Bytes())
	expectedHash := hex.EncodeToString(hasher.Sum(nil))
	if metrics.Checksum != expectedHash {
		t.Errorf("expected checksum %s, got %s", expectedHash, metrics.Checksum)
	}
}

func TestDelimitedFileWriter_None_And_Empty(t *testing.T) {
	storage := newMemoryStorage()
	schema := &domain.Schema{Fields: []domain.Field{{Name: "col", Type: "string"}}}
	formatter := NewJSONLFormatter()

	t.Run("Write plain uncompressed JSONL", func(t *testing.T) {
		writer := NewDelimitedFileWriter(storage, formatter, "none")
		wCtx, err := writer.Open(context.Background(), "/tmp/out.jsonl", schema)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		rec := domain.NewGenericRecord("src", 1)
		rec.SetString(0, "val")
		if err := writer.Write(wCtx, rec); err != nil {
			t.Fatalf("Write: %v", err)
		}
		m, err := writer.Close(context.Background(), wCtx)
		if err != nil || m.RowsWritten != 1 {
			t.Errorf("unexpected close metrics: %+v, err: %v", m, err)
		}
	})

	t.Run("Empty file aborts temp", func(t *testing.T) {
		writer := NewDelimitedFileWriter(storage, formatter, "none")
		wCtx, err := writer.Open(context.Background(), "/tmp/empty.jsonl", schema)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		m, err := writer.Close(context.Background(), wCtx)
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
		if m.RowsWritten != 0 {
			t.Errorf("expected 0 rows written for empty file, got %d", m.RowsWritten)
		}
	})

	t.Run("Manual abort cleans up temp", func(t *testing.T) {
		writer := NewDelimitedFileWriter(storage, formatter, "none")
		wCtx, _ := writer.Open(context.Background(), "/tmp/aborted.jsonl", schema)
		err := writer.abort(context.Background(), wCtx)
		if err != nil {
			t.Errorf("unexpected error on abort: %v", err)
		}
	})

	t.Run("Zstd compression", func(t *testing.T) {
		writer := NewDelimitedFileWriter(storage, formatter, "zstd")
		wCtx, err := writer.Open(context.Background(), "/tmp/out.jsonl.zst", schema)
		if err != nil {
			t.Fatalf("Open with zstd: %v", err)
		}
		rec := domain.NewGenericRecord("src", 1)
		rec.SetString(0, "compressed-zstd")
		_ = writer.Write(wCtx, rec)
		m, err := writer.Close(context.Background(), wCtx)
		if err != nil || m.RowsWritten != 1 {
			t.Errorf("Close with zstd failed: %+v, %v", m, err)
		}
	})
}

type failingStorage struct {
	failCreate bool
	failCommit bool
}

func (f *failingStorage) CreateTemp(ctx context.Context, finalURI string) (string, io.WriteCloser, error) {
	if f.failCreate {
		return "", nil, io.ErrUnexpectedEOF
	}
	return "/tmp/fail.tmp", &nopCloser{&bytes.Buffer{}}, nil
}
func (f *failingStorage) CommitTemp(ctx context.Context, _, _ string) error {
	if f.failCommit {
		return io.ErrClosedPipe
	}
	return nil
}
func (f *failingStorage) AbortTemp(ctx context.Context, _ string) error { return nil }

func TestDelimitedFileWriter_StorageErrors(t *testing.T) {
	schema := &domain.Schema{Fields: []domain.Field{{Name: "x", Type: "string"}}}
	formatter := NewJSONLFormatter()

	t.Run("CreateTemp failure", func(t *testing.T) {
		st := &failingStorage{failCreate: true}
		w := NewDelimitedFileWriter(st, formatter, "none")
		_, err := w.Open(context.Background(), "/tmp/fail.csv", schema)
		if err == nil {
			t.Error("expected error on CreateTemp failure, got nil")
		}
	})

	t.Run("CommitTemp failure", func(t *testing.T) {
		st := &failingStorage{failCommit: true}
		w := NewDelimitedFileWriter(st, formatter, "none")
		wCtx, _ := w.Open(context.Background(), "/tmp/fail.csv", schema)
		rec := domain.NewGenericRecord("src", 1)
		rec.SetString(0, "a")
		_ = w.Write(wCtx, rec)
		_, err := w.Close(context.Background(), wCtx)
		if err == nil {
			t.Error("expected error on CommitTemp failure, got nil")
		}
	})
}
