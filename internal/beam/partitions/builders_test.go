package partitions

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type dummyPartitionedReader struct{}

func (dummyPartitionedReader) CalculatePartitions(_ context.Context, _ any) ([]ports.PartitionSlice, error) {
	return []ports.PartitionSlice{{SliceIndex: 0}}, nil
}

func (dummyPartitionedReader) ReadPartition(_ context.Context, _ any, _ ports.PartitionSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	recCh := make(chan *domain.GenericRecord)
	errCh := make(chan error)
	close(recCh)
	close(errCh)
	return recCh, errCh, nil
}

func (dummyPartitionedReader) Close() error { return nil }

func TestPartitionedBeamSource_BuildSource(t *testing.T) {
	p, _ := beam.NewPipelineWithRoot()
	s := p.Root()

	sourceBuilder := &PartitionedBeamSource{
		Slices: []ports.PartitionSlice{
			{SliceIndex: 0, LowerBound: 1, UpperBound: 100},
			{SliceIndex: 1, LowerBound: 101, UpperBound: 200},
		},
		Reader: &dummyPartitionedReader{},
		Config: &domain.DatabaseSourceConfig{
			Driver: "postgres",
		},
	}

	col := sourceBuilder.BuildSource(s)
	if !col.IsValid() {
		t.Error("expected valid PCollection from PartitionedBeamSource")
	}
}
