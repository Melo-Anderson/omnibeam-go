package compression_test

import (
	"bytes"
	"compress/gzip"
	"io"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/compression"
)

func TestWrapDecompressor_AllFormats(t *testing.T) {
	orig := "hello, dataflow compressed world 12345"

	t.Run("gzip and gz", func(t *testing.T) {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, _ = gw.Write([]byte(orig))
		_ = gw.Close()

		for _, comp := range []string{"gzip", "gz", "GZIP"} {
			reader, err := compression.WrapDecompressor(bytes.NewReader(buf.Bytes()), comp)
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", comp, err)
			}
			decompressed, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("failed to read %s: %v", comp, err)
			}
			if string(decompressed) != orig {
				t.Errorf("expected %q, got %q", orig, string(decompressed))
			}
		}
	})

	t.Run("zstd", func(t *testing.T) {
		var buf bytes.Buffer
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatalf("zstd.NewWriter: %v", err)
		}
		_, _ = zw.Write([]byte(orig))
		_ = zw.Close()

		reader, err := compression.WrapDecompressor(bytes.NewReader(buf.Bytes()), "zstd")
		if err != nil {
			t.Fatalf("unexpected error wrapping zstd: %v", err)
		}
		decompressed, err := io.ReadAll(reader)
		if err != nil {
			t.Fatalf("failed reading zstd: %v", err)
		}
		if string(decompressed) != orig {
			t.Errorf("expected %q, got %q", orig, string(decompressed))
		}
	})

	t.Run("snappy and sz", func(t *testing.T) {
		var buf bytes.Buffer
		sw, closer, err := compression.WrapCompressor(&buf, "snappy")
		if err != nil {
			t.Fatalf("WrapCompressor(snappy): %v", err)
		}
		if _, err := sw.Write([]byte(orig)); err != nil {
			t.Fatalf("sw.Write: %v", err)
		}
		if err := closer.Close(); err != nil {
			t.Fatalf("closer.Close: %v", err)
		}

		for _, comp := range []string{"snappy", "sz", "SNAPPY"} {
			reader, err := compression.WrapDecompressor(bytes.NewReader(buf.Bytes()), comp)
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", comp, err)
			}
			decompressed, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("failed reading %s: %v", comp, err)
			}
			if string(decompressed) != orig {
				t.Errorf("expected %q, got %q", orig, string(decompressed))
			}
			if rc, ok := reader.(io.ReadCloser); ok {
				_ = rc.Close()
			}
		}
	})

	t.Run("none and empty", func(t *testing.T) {
		for _, comp := range []string{"", "none", "NONE"} {
			reader, err := compression.WrapDecompressor(bytes.NewReader([]byte(orig)), comp)
			if err != nil {
				t.Fatalf("unexpected error for %s: %v", comp, err)
			}
			data, err := io.ReadAll(reader)
			if err != nil {
				t.Fatalf("failed reading: %v", err)
			}
			if string(data) != orig {
				t.Errorf("expected %q, got %q", orig, string(data))
			}
		}
	})

	t.Run("bzip2 and bz2", func(t *testing.T) {
		for _, comp := range []string{"bzip2", "bz2"} {
			r, err := compression.WrapDecompressor(bytes.NewReader([]byte("plain")), comp)
			if err != nil || r == nil {
				t.Errorf("expected valid reader for %s", comp)
			}
		}
	})

	t.Run("unsupported and corrupt", func(t *testing.T) {
		_, err := compression.WrapDecompressor(bytes.NewReader([]byte(orig)), "unsupported-rar")
		if err == nil {
			t.Error("expected error for unsupported format, got nil")
		}

		// Corrupted gzip stream
		_, err = compression.WrapDecompressor(bytes.NewReader([]byte("not-gzip-data")), "gzip")
		if err == nil {
			t.Error("expected error for corrupt gzip stream, got nil")
		}
	})
}

func TestWrapDecompressorPool_GzipPoolReuse(t *testing.T) {
	orig := []byte("hello pool reuse world")

	makeGzip := func() []byte {
		var buf bytes.Buffer
		gw := gzip.NewWriter(&buf)
		_, _ = gw.Write(orig)
		_ = gw.Close()
		return buf.Bytes()
	}

	for i := 0; i < 5; i++ {
		r, err := compression.WrapDecompressor(bytes.NewReader(makeGzip()), "gzip")
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("iteration %d ReadAll: %v", i, err)
		}
		if !bytes.Equal(data, orig) {
			t.Errorf("iteration %d content mismatch", i)
		}
		if rc, ok := r.(io.ReadCloser); ok {
			_ = rc.Close()
		}
	}
}

func TestWrapDecompressorPool_ZstdPoolReuse(t *testing.T) {
	orig := []byte("hello zstd pool reuse world")

	makeZstd := func() []byte {
		var buf bytes.Buffer
		zw, err := zstd.NewWriter(&buf)
		if err != nil {
			t.Fatalf("zstd.NewWriter: %v", err)
		}
		_, _ = zw.Write(orig)
		_ = zw.Close()
		return buf.Bytes()
	}

	for i := 0; i < 5; i++ {
		r, err := compression.WrapDecompressor(bytes.NewReader(makeZstd()), "zstd")
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("iteration %d ReadAll: %v", i, err)
		}
		if !bytes.Equal(data, orig) {
			t.Errorf("iteration %d content mismatch", i)
		}
		if rc, ok := r.(io.ReadCloser); ok {
			_ = rc.Close()
		}
	}
}

func TestWrapDecompressorPool_SnappyPoolReuse(t *testing.T) {
	orig := []byte("hello snappy pool reuse world")

	makeSnappy := func() []byte {
		var buf bytes.Buffer
		w, closer, err := compression.WrapCompressor(&buf, "snappy")
		if err != nil {
			t.Fatalf("WrapCompressor: %v", err)
		}
		_, _ = w.Write(orig)
		_ = closer.Close()
		return buf.Bytes()
	}

	for i := 0; i < 5; i++ {
		r, err := compression.WrapDecompressor(bytes.NewReader(makeSnappy()), "snappy")
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		data, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("iteration %d ReadAll: %v", i, err)
		}
		if !bytes.Equal(data, orig) {
			t.Errorf("iteration %d content mismatch", i)
		}
		if rc, ok := r.(io.ReadCloser); ok {
			_ = rc.Close()
		}
	}
}
