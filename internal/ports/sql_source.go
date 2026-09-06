// Package ports defines domain-level abstract contracts and interfaces
// for storage, streaming, codecs, database readers, and secret resolvers.
package ports

import (
	"context"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// SQLSlice represents a single query range boundary for parallel database workers.
// Aliased to PartitionSlice for polymorphic compatibility across all datastores.
type SQLSlice = PartitionSlice

// SQLReader abstracts relational database query execution and parallel range slicing.
// Concrete implementations must be thread-safe across concurrent slice workers.
type SQLReader interface {
	PartitionedReader

	// CalculateSlices introspects source bounds and returns balanced partition slices.
	CalculateSlices(ctx context.Context, cfg *domain.DatabaseSourceConfig) ([]SQLSlice, error)

	// ReadSlice queries a single slice and streams parsed records and scan errors.
	ReadSlice(ctx context.Context, cfg *domain.DatabaseSourceConfig, slice SQLSlice) (<-chan *domain.GenericRecord, <-chan error, error)
}
