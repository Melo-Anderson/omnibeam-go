package formatters

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// FormatterFactory constructs a ports.RecordFormatter from domain format options.
type FormatterFactory func(opts domain.FormatOptions) ports.RecordFormatter

var (
	formatterMu       sync.RWMutex
	formatterRegistry = make(map[string]FormatterFactory)
)

// RegisterFormatter registers a FormatterFactory for a format name or extension.
func RegisterFormatter(format string, factory FormatterFactory) {
	formatterMu.Lock()
	defer formatterMu.Unlock()
	clean := strings.ToLower(strings.TrimSpace(format))
	formatterRegistry[clean] = factory
}

// BuildFormatter resolves and constructs the registered RecordFormatter for format.
func BuildFormatter(format string, opts domain.FormatOptions) (ports.RecordFormatter, error) {
	clean := strings.ToLower(strings.TrimSpace(format))

	formatterMu.RLock()
	factory, exists := formatterRegistry[clean]
	formatterMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unsupported format for delimited formatter: %q (supported formats: %v)", format, SupportedFormats())
	}
	return factory(opts), nil
}

// SupportedFormats returns a sorted list of registered format names.
func SupportedFormats() []string {
	formatterMu.RLock()
	defer formatterMu.RUnlock()

	seen := make(map[string]struct{})
	formats := make([]string, 0, len(formatterRegistry))
	for f := range formatterRegistry {
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
	csvFactory := func(opts domain.FormatOptions) ports.RecordFormatter {
		return NewCSVFormatter(opts)
	}
	jsonlFactory := func(_ domain.FormatOptions) ports.RecordFormatter {
		return NewJSONLFormatter()
	}

	RegisterFormatter("", jsonlFactory)
	RegisterFormatter("csv", csvFactory)
	RegisterFormatter("txt", csvFactory)
	RegisterFormatter("tsv", func(opts domain.FormatOptions) ports.RecordFormatter {
		opts.Delimiter = "\t"
		return NewCSVFormatter(opts)
	})
	RegisterFormatter("json", jsonlFactory)
	RegisterFormatter("jsonl", jsonlFactory)
	RegisterFormatter("jsonlines", jsonlFactory)
}
