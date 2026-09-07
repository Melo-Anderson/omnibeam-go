package partitions

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestPartitionedReaderProvider(t *testing.T) {
	SetPartitionedReaderProvider(nil)
	if HasPartitionedReaderProvider() || GetPartitionedReaderProvider() != nil {
		t.Error("expected nil partitioned reader provider")
	}

	SetPartitionedReaderProvider(func(_ context.Context, _ *domain.DatabaseSourceConfig, _ *domain.SecretsConfig) (ports.PartitionedReader, error) {
		return &dummyPartitionedReader{}, nil
	})

	if !HasPartitionedReaderProvider() || GetPartitionedReaderProvider() == nil {
		t.Error("expected registered partitioned reader provider")
	}
}
