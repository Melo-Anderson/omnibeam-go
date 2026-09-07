package compression_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/compression"
)

func BenchmarkWrapDecompressor_Gzip(b *testing.B) {
	data := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore.")
	var compressed bytes.Buffer
	w, closer, err := compression.WrapCompressor(&compressed, "gzip")
	if err != nil {
		b.Fatalf("WrapCompressor failed: %v", err)
	}
	_, _ = w.Write(data)
	_ = closer.Close()
	raw := compressed.Bytes()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := compression.WrapDecompressor(bytes.NewReader(raw), "gzip")
			if err != nil {
				b.Fatalf("WrapDecompressor failed: %v", err)
			}
			_, _ = io.Copy(io.Discard, r)
			if rc, ok := r.(io.Closer); ok {
				_ = rc.Close()
			}
		}
	})
}

func BenchmarkWrapDecompressor_Zstd(b *testing.B) {
	data := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore.")
	var compressed bytes.Buffer
	w, closer, err := compression.WrapCompressor(&compressed, "zstd")
	if err != nil {
		b.Fatalf("WrapCompressor failed: %v", err)
	}
	_, _ = w.Write(data)
	_ = closer.Close()
	raw := compressed.Bytes()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := compression.WrapDecompressor(bytes.NewReader(raw), "zstd")
			if err != nil {
				b.Fatalf("WrapDecompressor failed: %v", err)
			}
			_, _ = io.Copy(io.Discard, r)
			if rc, ok := r.(io.Closer); ok {
				_ = rc.Close()
			}
		}
	})
}

func BenchmarkWrapDecompressor_Snappy(b *testing.B) {
	data := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore.")
	var compressed bytes.Buffer
	w, closer, err := compression.WrapCompressor(&compressed, "snappy")
	if err != nil {
		b.Fatalf("WrapCompressor failed: %v", err)
	}
	_, _ = w.Write(data)
	_ = closer.Close()
	raw := compressed.Bytes()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r, err := compression.WrapDecompressor(bytes.NewReader(raw), "snappy")
			if err != nil {
				b.Fatalf("WrapDecompressor failed: %v", err)
			}
			_, _ = io.Copy(io.Discard, r)
			if rc, ok := r.(io.Closer); ok {
				_ = rc.Close()
			}
		}
	})
}
