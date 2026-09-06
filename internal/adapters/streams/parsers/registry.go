package parsers

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// DecoderFactory constructs a ports.StreamDecoder from a source configuration.
type DecoderFactory func(cfg *domain.SourceConfig) ports.StreamDecoder

var (
	decoderMu       sync.RWMutex
	decoderRegistry = make(map[string]DecoderFactory)
)

// Register registers a DecoderFactory for a given format name or alias.
func Register(format string, factory DecoderFactory) {
	decoderMu.Lock()
	defer decoderMu.Unlock()
	clean := strings.ToLower(strings.TrimSpace(format))
	decoderRegistry[clean] = factory
}

// Build resolves and constructs the registered StreamDecoder for the format.
func Build(format string, cfg *domain.SourceConfig) (ports.StreamDecoder, error) {
	clean := strings.ToLower(strings.TrimSpace(format))

	decoderMu.RLock()
	factory, exists := decoderRegistry[clean]
	decoderMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unsupported source format: %q (supported formats: %v)", format, SupportedFormats())
	}
	return factory(cfg), nil
}

// SupportedFormats returns a sorted list of all registered source formats.
func SupportedFormats() []string {
	decoderMu.RLock()
	defer decoderMu.RUnlock()

	seen := make(map[string]struct{})
	formats := make([]string, 0, len(decoderRegistry))
	for f := range decoderRegistry {
		if f != "" {
			if _, ok := seen[f]; !ok {
				seen[f] = struct{}{}
				formats = append(formats, f)
			}
		}
	}
	sort.Strings(formats)
	return formats
}

func init() {
	csvFactory := func(cfg *domain.SourceConfig) ports.StreamDecoder {
		delim := ","
		if cfg != nil && cfg.Delimiter != "" {
			delim = cfg.Delimiter
		}
		return NewCSVParser(delim, true)
	}

	jsonlFactory := func(_ *domain.SourceConfig) ports.StreamDecoder {
		return NewJSONLParser()
	}

	Register("", csvFactory)
	Register("csv", csvFactory)
	Register("txt", csvFactory)
	Register("tsv", func(_ *domain.SourceConfig) ports.StreamDecoder {
		return NewCSVParser("\t", true)
	})
	Register("json", jsonlFactory)
	Register("jsonl", jsonlFactory)
	Register("jsonlines", jsonlFactory)
}
