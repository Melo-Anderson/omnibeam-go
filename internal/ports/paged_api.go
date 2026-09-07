// Package ports defines domain-level abstract contracts and interfaces.
package ports

import (
	"context"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// PageSlice defines an API page index, offset window, or cursor token.
type PageSlice struct {
	PageIndex int               `json:"page_index"`
	PageSize  int               `json:"page_size,omitempty"`
	Cursor    string            `json:"cursor,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// PagedAPIReader abstracts paginated HTTP/REST/GraphQL external endpoints.
type PagedAPIReader interface {
	EstimatePages(ctx context.Context, apiCfg any) ([]PageSlice, error)
	ReadPage(ctx context.Context, apiCfg any, page PageSlice) (<-chan *domain.GenericRecord, <-chan error, error)
}
