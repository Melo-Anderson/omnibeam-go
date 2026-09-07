package partitions

import (
	"context"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var (
	globalPartitionedReaderProvider func(ctx context.Context, cfg *domain.DatabaseSourceConfig, secCfg *domain.SecretsConfig) (ports.PartitionedReader, error)
)

// SetPartitionedReaderProvider sets the process-level reader provider for PartitionQuery SDFs.
func SetPartitionedReaderProvider(p func(ctx context.Context, cfg *domain.DatabaseSourceConfig, secCfg *domain.SecretsConfig) (ports.PartitionedReader, error)) {
	globalPartitionedReaderProvider = p
}

// GetPartitionedReaderProvider returns the process-level reader provider for PartitionQuery SDFs.
func GetPartitionedReaderProvider() func(ctx context.Context, cfg *domain.DatabaseSourceConfig, secCfg *domain.SecretsConfig) (ports.PartitionedReader, error) {
	return globalPartitionedReaderProvider
}

// HasPartitionedReaderProvider reports whether the global partitioned reader provider is registered.
func HasPartitionedReaderProvider() bool {
	return globalPartitionedReaderProvider != nil
}
