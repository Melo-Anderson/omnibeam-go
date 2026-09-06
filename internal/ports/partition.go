// Package ports defines domain-level abstract contracts and interfaces
// for storage, streaming, codecs, database readers, paged APIs, and extractors.
package ports

import (
	"context"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// PartitionSlice defines a discrete query boundary across relational and NoSQL datastores.
type PartitionSlice struct {
	SliceIndex int               `json:"slice_index"`
	LowerBound any               `json:"lower_bound,omitempty"`
	UpperBound any               `json:"upper_bound,omitempty"`
	IsFirst    bool              `json:"is_first"`
	IsLast     bool              `json:"is_last"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

// PartitionedReader abstracts any datastore supporting analytical partition slicing.
// Concrete implementations: SQLSource (Postgres/MySQL), MongoSource, CassandraSource.
type PartitionedReader interface {
	CalculatePartitions(ctx context.Context, srcCfg any) ([]PartitionSlice, error)
	ReadPartition(ctx context.Context, srcCfg any, slice PartitionSlice) (<-chan *domain.GenericRecord, <-chan error, error)
	// Close releases all resources held by the reader (e.g., connection pools).
	Close() error
}

