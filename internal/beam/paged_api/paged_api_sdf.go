// package paged_api provides Splittable DoFn (SDF) implementations for paginated API reading.
// DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
package paged_api

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*PageRange)(nil)).Elem())
	beam.RegisterDoFn(reflect.TypeOf((*PagedAPISourceSDF)(nil)).Elem())
	// NOTE: ports.PageSlice coder is registered canonically in
	// internal/beam/core/graph_builder.go to avoid duplicate registration panic.
}

// PagedAPISourceSDF is a generic Splittable DoFn that reads paginated and cursor-based API endpoints.
type PagedAPISourceSDF struct {
	APICfg *domain.APISourceConfig `json:"api_cfg"`
	reader ports.PagedAPIReader
}

// NewPagedAPISourceSDF creates a new PagedAPISourceSDF DoFn with injected reader.
func NewPagedAPISourceSDF(reader ports.PagedAPIReader, apiCfg *domain.APISourceConfig) *PagedAPISourceSDF {
	return &PagedAPISourceSDF{
		reader: reader,
		APICfg: apiCfg,
	}
}

// Setup initializes the API reader once per DoFn bundle on each worker.
// Signature: func(ctx context.Context) error — valid per Beam Go SDK (fn.go validation).
func (fn *PagedAPISourceSDF) Setup(_ context.Context) error {
	if fn.reader != nil {
		return nil
	}
	if globalPagedAPIReaderProvider == nil {
		return fmt.Errorf("PagedAPISourceSDF: globalPagedAPIReaderProvider not registered — call SetPagedAPIReaderProvider in a beam.RegisterInit before beam.Init()")
	}
	fn.reader = globalPagedAPIReaderProvider(fn.APICfg)
	if fn.reader == nil {
		return fmt.Errorf("PagedAPISourceSDF.Setup: provider returned nil reader")
	}
	return nil
}

// Teardown is called when the DoFn bundle completes.
// Signature: func() error — valid per Beam Go SDK (fn.go validation).
func (fn *PagedAPISourceSDF) Teardown() error {
	fn.reader = nil
	return nil
}

// CreateInitialRestriction creates the initial page range restriction.
func (fn *PagedAPISourceSDF) CreateInitialRestriction(_ context.Context, _ ports.PageSlice) (PageRange, error) {
	return PageRange{Start: 0, End: 1}, nil
}

// SplitRestriction returns the single page restriction.
func (fn *PagedAPISourceSDF) SplitRestriction(_ ports.PageSlice, rest PageRange) []PageRange {
	return []PageRange{rest}
}

// RestrictionSize computes the estimated record count of the page range.
// Weighted by PageSize so the Dataflow Autoscaler receives accurate backlog estimates.
func (fn *PagedAPISourceSDF) RestrictionSize(page ports.PageSlice, rest PageRange) float64 {
	if rest.End < rest.Start {
		return 0
	}
	pageSize := float64(domain.DefaultAPIPageSize)
	if page.PageSize > 0 {
		pageSize = float64(page.PageSize)
	}
	return pageSize * float64(rest.End-rest.Start)
}

// CreateTracker returns a new PageRangeTracker initialized with the given restriction.
func (fn *PagedAPISourceSDF) CreateTracker(rest PageRange) *PageRangeTracker {
	return NewPageRangeTracker(rest)
}

// ProcessElement reads the API page claimed by tracker and emits records.
func (fn *PagedAPISourceSDF) ProcessElement(
	ctx context.Context,
	tracker *PageRangeTracker,
	page ports.PageSlice,
	emit func(*domain.GenericRecord),
) error {
	if !tracker.TryClaim(0) {
		return nil
	}

	if fn.reader == nil {
		return fmt.Errorf("PagedAPISourceSDF: reader not initialized — Setup() must run before ProcessElement()")
	}

	recChan, errChan, err := fn.reader.ReadPage(ctx, fn.APICfg, page)
	if err != nil {
		return fmt.Errorf("failed reading API page %d: %w", page.PageIndex, err)
	}

	for rec := range recChan {
		if rec != nil {
			emit(rec)
		}
	}

	for err := range errChan {
		if err != nil {
			return fmt.Errorf("error in API page %d: %w", page.PageIndex, err)
		}
	}

	return nil
}
