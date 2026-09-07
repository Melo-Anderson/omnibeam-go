package codecs

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestPipelineBuilder_BuildDecoder_OCP(t *testing.T) {
	pb := NewPipelineBuilder()

	t.Run("Resolves registered CSV decoder", func(t *testing.T) {
		cfg := &domain.SourceConfig{
			Format:    "csv",
			Delimiter: ",",
		}
		decoder, err := pb.BuildDecoder(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decoder == nil {
			t.Fatalf("expected non-nil decoder")
		}
	})

	t.Run("Resolves registered JSONL decoder", func(t *testing.T) {
		cfg := &domain.SourceConfig{
			Format: "jsonl",
		}
		decoder, err := pb.BuildDecoder(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if decoder == nil {
			t.Fatalf("expected non-nil decoder")
		}
	})

	t.Run("Unregistered format returns descriptive error", func(t *testing.T) {
		cfg := &domain.SourceConfig{
			Format: "unknown_format",
		}
		_, err := pb.BuildDecoder(cfg)
		if err == nil {
			t.Fatalf("expected error for unregistered format, got nil")
		}
		if !strings.Contains(err.Error(), "unsupported source format") {
			t.Errorf("expected error message to mention unsupported source format, got: %v", err)
		}
	})
}

func TestPipelineBuilder_WrapStream(t *testing.T) {
	pb := NewPipelineBuilder()

	t.Run("Happy path with none compression and utf8", func(t *testing.T) {
		cfg := &domain.SourceConfig{
			Compression: "none",
			Charset:     "utf-8",
		}
		r, err := pb.WrapStream(strings.NewReader("sample"), cfg)
		if err != nil || r == nil {
			t.Fatalf("unexpected error wrapping stream: %v", err)
		}
	})

	t.Run("Decompression error", func(t *testing.T) {
		cfg := &domain.SourceConfig{
			Compression: "unsupported-comp",
		}
		_, err := pb.WrapStream(strings.NewReader("sample"), cfg)
		if err == nil {
			t.Error("expected error for unsupported compression, got nil")
		}
	})

	t.Run("Charset error", func(t *testing.T) {
		cfg := &domain.SourceConfig{
			Compression: "none",
			Charset:     "unsupported-charset",
		}
		_, err := pb.WrapStream(strings.NewReader("sample"), cfg)
		if err == nil {
			t.Error("expected error for unsupported charset, got nil")
		}
	})

	t.Run("WithDecryptor success", func(t *testing.T) {
		dec := &mockDecryptor{}
		b := NewPipelineBuilder().WithDecryptor(dec, "gcp:secret/key", nil)
		cfg := &domain.SourceConfig{
			Compression: "none",
			Charset:     "utf-8",
		}
		r, err := b.WrapStream(strings.NewReader("sample payload"), cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		buf, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("unexpected ReadAll error: %v", err)
		}
		if string(buf) != "sample payload" {
			t.Errorf("content mismatch: got %q", string(buf))
		}
		if !dec.called {
			t.Errorf("expected decryptor to be called")
		}
	})

	t.Run("WithDecryptor error", func(t *testing.T) {
		dec := &mockDecryptor{returnErr: errors.New("decryption failed")}
		b := NewPipelineBuilder().WithDecryptor(dec, "gcp:secret/key", nil)
		cfg := &domain.SourceConfig{
			Compression: "none",
			Charset:     "utf-8",
		}
		_, err := b.WrapStream(strings.NewReader("sample"), cfg)
		if err == nil {
			t.Errorf("expected error from decryptor, got nil")
		}
	})
}

type mockDecryptor struct {
	called    bool
	returnErr error
}

func (m *mockDecryptor) DecryptStream(_ context.Context, r io.Reader, _ string, _ ports.SecretResolver) (io.Reader, error) {
	m.called = true
	if m.returnErr != nil {
		return nil, m.returnErr
	}
	return r, nil
}
