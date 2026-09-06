package parsers_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/parsers"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type customMockDecoder struct{}

func (customMockDecoder) Decode(_ context.Context, _ io.Reader, _ *domain.Schema, _ string) (<-chan *domain.GenericRecord, <-chan error, error) {
	return nil, nil, nil
}

func TestParserRegistry(t *testing.T) {
	t.Run("Resolves registered csv parser", func(t *testing.T) {
		cfg := &domain.SourceConfig{Format: "csv", Delimiter: ";"}
		dec, err := parsers.Build("csv", cfg)
		if err != nil || dec == nil {
			t.Fatalf("expected valid csv decoder, got: %v, %v", dec, err)
		}
	})

	t.Run("Resolves registered jsonl parser", func(t *testing.T) {
		cfg := &domain.SourceConfig{Format: "jsonl"}
		dec, err := parsers.Build("jsonl", cfg)
		if err != nil || dec == nil {
			t.Fatalf("expected valid jsonl decoder, got: %v, %v", dec, err)
		}
	})

	t.Run("Resolves empty format to default csv", func(t *testing.T) {
		dec, err := parsers.Build("", nil)
		if err != nil || dec == nil {
			t.Fatalf("expected fallback csv decoder, got: %v, %v", dec, err)
		}
	})

	t.Run("Unregistered format returns descriptive error", func(t *testing.T) {
		_, err := parsers.Build("unknown_ext_xyz", nil)
		if err == nil {
			t.Fatalf("expected error for unregistered format, got nil")
		}
		if !strings.Contains(err.Error(), "unsupported source format") {
			t.Errorf("expected error to mention unsupported source format, got: %v", err)
		}
	})

	t.Run("Custom format registration and build", func(t *testing.T) {
		parsers.Register("custom_mock", func(_ *domain.SourceConfig) ports.StreamDecoder {
			return &customMockDecoder{}
		})
		dec, err := parsers.Build("custom_mock", nil)
		if err != nil || dec == nil {
			t.Fatalf("expected custom mock decoder, got: %v, %v", dec, err)
		}
	})

	t.Run("SupportedFormats returns non-empty list", func(t *testing.T) {
		formats := parsers.SupportedFormats()
		if len(formats) < 4 {
			t.Errorf("expected at least 4 formats, got: %v", formats)
		}
	})
}
