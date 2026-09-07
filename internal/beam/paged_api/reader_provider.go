package paged_api

import (
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var (
	globalPagedAPIReaderProvider func(cfg *domain.APISourceConfig) ports.PagedAPIReader
)

// SetPagedAPIReaderProvider sets the process-level reader provider for PagedAPI SDFs.
func SetPagedAPIReaderProvider(p func(cfg *domain.APISourceConfig) ports.PagedAPIReader) {
	globalPagedAPIReaderProvider = p
}

// GetPagedAPIReaderProvider returns the process-level reader provider for PagedAPI SDFs.
func GetPagedAPIReaderProvider() func(cfg *domain.APISourceConfig) ports.PagedAPIReader {
	return globalPagedAPIReaderProvider
}

// HasPagedAPIReaderProvider reports whether the global paged API reader provider is registered.
func HasPagedAPIReaderProvider() bool {
	return globalPagedAPIReaderProvider != nil
}
