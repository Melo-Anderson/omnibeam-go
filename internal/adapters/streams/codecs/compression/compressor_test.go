package compression_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/compression"
)

func TestWrapCompressor_NoCompression(t *testing.T) {
	var buf bytes.Buffer
	w, closer, err := compression.WrapCompressor(&buf, "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if closer != nil {
		t.Errorf("expected nil closer for no-op compression, got %T", closer)
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if buf.String() != "hello" {
		t.Errorf("expected passthrough write, got %q", buf.String())
	}
}

func TestWrapCompressor_Gzip(t *testing.T) {
	var buf bytes.Buffer
	w, closer, err := compression.WrapCompressor(&buf, "gzip")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if closer == nil {
		t.Fatal("expected non-nil closer for gzip")
	}
	if _, err := w.Write([]byte("hello")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("expected gzip bytes written to buffer, got empty")
	}

	// Verify decompression matches original content
	decomp, err := compression.WrapDecompressor(&buf, "gzip")
	if err != nil {
		t.Fatalf("failed wrapping decompressor: %v", err)
	}
	readBack, err := io.ReadAll(decomp)
	if err != nil {
		t.Fatalf("failed reading decompressed data: %v", err)
	}
	if string(readBack) != "hello" {
		t.Errorf("expected 'hello', got %q", string(readBack))
	}
}

func TestWrapCompressor_Zstd(t *testing.T) {
	var buf bytes.Buffer
	w, closer, err := compression.WrapCompressor(&buf, "zstd")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if closer == nil {
		t.Fatal("expected non-nil closer for zstd")
	}
	if _, err := w.Write([]byte("hello zstd")); err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if err := closer.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Verify decompression matches original content
	decomp, err := compression.WrapDecompressor(&buf, "zstd")
	if err != nil {
		t.Fatalf("failed wrapping decompressor: %v", err)
	}
	readBack, err := io.ReadAll(decomp)
	if err != nil {
		t.Fatalf("failed reading decompressed data: %v", err)
	}
	if string(readBack) != "hello zstd" {
		t.Errorf("expected 'hello zstd', got %q", string(readBack))
	}
}

func TestWrapCompressor_Unsupported(t *testing.T) {
	var buf bytes.Buffer
	_, _, err := compression.WrapCompressor(&buf, "brotli")
	if err == nil {
		t.Error("expected error for unsupported compression, got nil")
	}
}
