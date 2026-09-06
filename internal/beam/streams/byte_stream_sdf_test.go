package streams

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type fakeReader struct {
	data []byte
}

func (f *fakeReader) Open(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(f.data)), nil
}

func (f *fakeReader) List(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (f *fakeReader) Size(_ context.Context, _ string) (int64, error) {
	return int64(len(f.data)), nil
}

type fakeDecoder struct{}

func (f *fakeDecoder) Decode(_ context.Context, r io.Reader, _ *domain.Schema, _ string) (<-chan *domain.GenericRecord, <-chan error, error) {
	scanner := bufio.NewScanner(r)
	recChan := make(chan *domain.GenericRecord, 10)
	errChan := make(chan error, 1)

	go func() {
		defer close(recChan)
		defer close(errChan)
		for scanner.Scan() {
			rec := domain.NewGenericRecord("schema-test", 1)
			rec.SetString(0, scanner.Text())
			recChan <- rec
		}
	}()
	return recChan, errChan, nil
}

type blockingReader struct {
	blockCh chan struct{}
}

func (b *blockingReader) Open(ctx context.Context, _ string) (io.ReadCloser, error) {
	return &blockingReadCloser{ctx: ctx, blockCh: b.blockCh}, nil
}
func (b *blockingReader) List(_ context.Context, _ string) ([]string, error) { return nil, nil }
func (b *blockingReader) Size(_ context.Context, _ string) (int64, error) { return 1000, nil }

type blockingReadCloser struct {
	ctx     context.Context
	blockCh chan struct{}
}

func (b *blockingReadCloser) Read(p []byte) (n int, err error) {
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-b.blockCh:
		return 0, io.EOF
	}
}

func (b *blockingReadCloser) Close() error {
	return nil
}

func TestByteStreamSourceSDF_ProcessElement_CancelUnblocksGoroutine(t *testing.T) {
	t.Run("context cancel unblocks feed goroutine without hanging", func(t *testing.T) {
		blockCh := make(chan struct{})
		blocker := &blockingReader{blockCh: blockCh}
		fn := NewByteStreamSourceSDF(blocker, nil, &fakeDecoder{}, domain.SourceConfig{Compression: "none"})

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		tracker := NewByteOffsetTracker(ByteOffsetRange{Start: 0, End: 100})

		done := make(chan error, 1)
		go func() {
			done <- fn.ProcessElement(ctx, tracker, "test://file.csv", func(*domain.GenericRecord) {})
		}()

		select {
		case err := <-done:
			if err == nil {
				t.Error("expected a context deadline or cancellation error, got nil")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ProcessElement did not unblock within 2s after context cancellation")
		}
	})
}

func TestByteStreamSourceSDF_ProcessElement(t *testing.T) {
	fileContent := []byte("first line\nsecond line\nthird line\n")
	reader := &fakeReader{data: fileContent}
	decoder := &fakeDecoder{}

	fn := NewByteStreamSourceSDF(reader, nil, decoder, domain.SourceConfig{})
	tracker := NewByteOffsetTracker(ByteOffsetRange{Start: 0, End: int64(len(fileContent))})

	var emitted []*domain.GenericRecord
	err := fn.ProcessElement(context.Background(), tracker, "test.csv", func(r *domain.GenericRecord) {
		emitted = append(emitted, r)
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(emitted) != 3 {
		t.Fatalf("expected 3 records emitted, got %d", len(emitted))
	}
}

func TestByteStreamSourceSDF_SplitRestriction_CustomChunkSize(t *testing.T) {
	srcCfg := domain.SourceConfig{
		ChunkSizeBytes: 1024 * 1024, // 1 MB chunks
	}
	fn := NewByteStreamSourceSDF(nil, nil, nil, srcCfg)

	rest := ByteOffsetRange{Start: 0, End: 5 * 1024 * 1024} // 5 MB file
	splits := fn.SplitRestriction("", rest)

	if len(splits) != 5 {
		t.Fatalf("expected 5 splits for 5MB with 1MB chunk size, got %d", len(splits))
	}

	for i, s := range splits {
		expectedStart := int64(i) * 1024 * 1024
		expectedEnd := int64(i+1) * 1024 * 1024
		if s.Start != expectedStart || s.End != expectedEnd {
			t.Errorf("split %d: expected [%d, %d), got [%d, %d)", i, expectedStart, expectedEnd, s.Start, s.End)
		}
	}
}

func TestByteStreamSourceSDF_ProcessElement_MidChunkSplit(t *testing.T) {
	fileContent := []byte("first line\nsecond line\nthird line\n")
	reader := &fakeReader{data: fileContent}
	decoder := &fakeDecoder{}

	fn := NewByteStreamSourceSDF(reader, nil, decoder, domain.SourceConfig{})
	// Start at offset 5 (in the middle of "first line\n")
	tracker := NewByteOffsetTracker(ByteOffsetRange{Start: 5, End: int64(len(fileContent))})

	var emitted []*domain.GenericRecord
	err := fn.ProcessElement(context.Background(), tracker, "test.csv", func(r *domain.GenericRecord) {
		emitted = append(emitted, r)
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(emitted) != 2 {
		t.Fatalf("expected 2 records for mid-chunk split, got %d", len(emitted))
	}
}

func TestByteStreamSourceSDF_CreateInitialRestriction(t *testing.T) {
	fileContent := []byte("line1\nline2\nline3\n")
	reader := &fakeReader{data: fileContent}
	fn := NewByteStreamSourceSDF(reader, nil, nil, domain.SourceConfig{})

	rest, err := fn.CreateInitialRestriction(context.Background(), "test.csv")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rest.Start != 0 || rest.End != int64(len(fileContent)) {
		t.Errorf("expected range [0, %d], got [%d, %d]", len(fileContent), rest.Start, rest.End)
	}
}

func TestByteStreamSourceSDF_SplitRestriction_Compressed(t *testing.T) {
	tests := []struct {
		name        string
		compression string
		wantSplits  int
	}{
		{"gzip forces single split", "gzip", 1},
		{"zstd forces single split", "zstd", 1},
		{"none allows multi-split", "none", -1}, // -1 = multiple expected
		{"empty string allows multi-split", "", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := NewByteStreamSourceSDF(nil, nil, nil, domain.SourceConfig{
				Compression:    tt.compression,
				ChunkSizeBytes: 1024,
			})
			rest := ByteOffsetRange{Start: 0, End: 100000}
			splits := fn.SplitRestriction("irrelevant-uri", rest)

			if tt.wantSplits == 1 {
				if len(splits) != 1 {
					t.Errorf("compression=%q: expected 1 split, got %d", tt.compression, len(splits))
				}
				if len(splits) == 1 && splits[0] != rest {
					t.Errorf("single split should equal original restriction")
				}
			} else {
				if len(splits) <= 1 {
					t.Errorf("compression=%q: expected multiple splits, got %d", tt.compression, len(splits))
				}
			}
		})
	}
}

func TestByteStreamSourceSDF_RestrictionSize(t *testing.T) {
	fn := NewByteStreamSourceSDF(nil, nil, nil, domain.SourceConfig{})
	rest := ByteOffsetRange{Start: 1024, End: 5120}
	size := fn.RestrictionSize("gs://bucket/file.csv", rest)
	if size != 4096.0 {
		t.Errorf("expected RestrictionSize 4096.0, got %f", size)
	}
}

func TestByteStreamSourceSDF_CreateTracker_And_Providers(t *testing.T) {
	fn := NewByteStreamSourceSDF(nil, nil, nil, domain.SourceConfig{})
	tracker := fn.CreateTracker(ByteOffsetRange{Start: 0, End: 100})
	if tracker == nil {
		t.Fatal("expected non-nil tracker from CreateTracker")
	}

	// Test ensureReader error when no reader and no global provider
	SetStorageReaderProvider(nil)
	err := fn.ensureReader("file.csv")
	if err == nil {
		t.Error("expected error from ensureReader when uninitialized")
	}

	// Test ensureReader using global provider
	SetStorageReaderProvider(func(_ string) ports.StorageReader {
		return &fakeReader{data: []byte("abc")}
	})
	err = fn.ensureReader("file.csv")
	if err != nil {
		t.Errorf("unexpected error from ensureReader with provider: %v", err)
	}

	// Test ensureDecoderAndWrapper error when no decoder and no global provider
	SetStreamDecoderProvider(nil)
	err = fn.ensureDecoderAndWrapper()
	if err == nil {
		t.Error("expected error from ensureDecoderAndWrapper when uninitialized")
	}

	// Test ensureDecoderAndWrapper using global providers
	SetStreamDecoderProvider(func(_ *domain.SourceConfig) ports.StreamDecoder {
		return &fakeDecoder{}
	})
	SetStreamWrapperProvider(func(_ *domain.SourceConfig) ports.StreamWrapper {
		return &dummyStreamWrapper{}
	})
	err = fn.ensureDecoderAndWrapper()
	if err != nil {
		t.Errorf("unexpected error with providers: %v", err)
	}
}

func TestByteStreamSourceSDF_ProcessElement_ClaimFalse(t *testing.T) {
	fn := NewByteStreamSourceSDF(&fakeReader{data: []byte("line1\n")}, nil, &fakeDecoder{}, domain.SourceConfig{})
	tracker := NewByteOffsetTracker(ByteOffsetRange{Start: 0, End: 10})
	tracker.MarkDone() // Force TryClaim to return false

	count := 0
	emit := func(r *domain.GenericRecord) { count++ }
	err := fn.ProcessElement(context.Background(), tracker, "file.csv", emit)
	if err != nil {
		t.Fatalf("unexpected error when claim is false: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 emitted records when claim fails, got %d", count)
	}
}

type errSizeReader struct{}

func (errSizeReader) Open(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }
func (errSizeReader) List(_ context.Context, _ string) ([]string, error)      { return nil, nil }
func (errSizeReader) Size(_ context.Context, _ string) (int64, error) {
	return 0, errors.New("size check failed")
}

func TestByteStreamSourceSDF_RestrictionsAndSplits(t *testing.T) {
	t.Run("CreateInitialRestriction error on Size failure", func(t *testing.T) {
		fn := NewByteStreamSourceSDF(&errSizeReader{}, nil, nil, domain.SourceConfig{})
		_, err := fn.CreateInitialRestriction(context.Background(), "error.csv")
		if err == nil {
			t.Error("expected error from CreateInitialRestriction when Size fails")
		}
	})

	t.Run("RestrictionSize on inverted range", func(t *testing.T) {
		fn := NewByteStreamSourceSDF(nil, nil, nil, domain.SourceConfig{})
		size := fn.RestrictionSize("file.csv", ByteOffsetRange{Start: 100, End: 50})
		if size != 0 {
			t.Errorf("expected 0 for inverted range, got %f", size)
		}
	})

	t.Run("SplitRestriction on compressed file returns single range", func(t *testing.T) {
		fnGz := NewByteStreamSourceSDF(nil, nil, nil, domain.SourceConfig{Compression: "gzip"})
		splits := fnGz.SplitRestriction("file.gz", ByteOffsetRange{Start: 0, End: 10000000})
		if len(splits) != 1 {
			t.Errorf("expected 1 unsplit range for gzip, got %d", len(splits))
		}

		fnRaw := NewByteStreamSourceSDF(nil, nil, nil, domain.SourceConfig{ChunkSizeBytes: 1000})
		splitsRaw := fnRaw.SplitRestriction("file.csv", ByteOffsetRange{Start: 0, End: 2500})
		if len(splitsRaw) != 3 {
			t.Errorf("expected 3 splits for 2500 bytes with 1000 chunk size, got %d", len(splitsRaw))
		}
	})
}

func TestReadRecordBytes_EscapedQuotes(t *testing.T) {
	data := "\"101\",\"Hello \"\"World\"\"\nMulti-line\",200\n\"102\",\"Simple\",300\n"
	br := bufio.NewReader(bytes.NewReader([]byte(data)))

	row1, err := readRecordBytes(br, '"', true)
	if err != nil {
		t.Fatalf("readRecordBytes row 1: %v", err)
	}
	expectedRow1 := "\"101\",\"Hello \"\"World\"\"\nMulti-line\",200\n"
	if string(row1) != expectedRow1 {
		t.Fatalf("expected row 1 %q, got %q", expectedRow1, string(row1))
	}

	row2, err := readRecordBytes(br, '"', true)
	if err != nil {
		t.Fatalf("readRecordBytes row 2: %v", err)
	}
	expectedRow2 := "\"102\",\"Simple\",300\n"
	if string(row2) != expectedRow2 {
		t.Fatalf("expected row 2 %q, got %q", expectedRow2, string(row2))
	}
}

func TestByteStreamSDF_RestrictionSize(t *testing.T) {
	fn := &ByteStreamSourceSDF{}
	tests := []struct {
		name string
		rest ByteOffsetRange
		want float64
	}{
		{"1MB range", ByteOffsetRange{Start: 0, End: 1048576}, 1048576},
		{"mid-file chunk", ByteOffsetRange{Start: 512, End: 1024}, 512},
		{"zero-length range", ByteOffsetRange{Start: 100, End: 100}, 0},
		{"inverted range returns 0", ByteOffsetRange{Start: 1000, End: 500}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := fn.RestrictionSize("dummy_file.csv", tc.rest)
			if got != tc.want {
				t.Errorf("RestrictionSize(%v) = %v, want %v", tc.rest, got, tc.want)
			}
		})
	}
}


