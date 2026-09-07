package partitions

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// PartitionedBeamSource builds a partitioned datastore PCollection using PartitionQuerySourceSDF (Família 2).
type PartitionedBeamSource struct {
	Slices        []ports.PartitionSlice
	Reader        ports.PartitionedReader
	Config        *domain.DatabaseSourceConfig
	SecretsConfig *domain.SecretsConfig
}

// Compile-time assertion: PartitionedBeamSource must satisfy ports.BeamSourceBuilder (LSP).
var _ ports.BeamSourceBuilder = (*PartitionedBeamSource)(nil)

// BuildSource implements ports.BeamSourceBuilder for partitioned datastores.
// Reshuffle ensures partition slices fan out across all available workers,
// preventing runner fusion from assigning all slices to one thread.
func (b *PartitionedBeamSource) BuildSource(s beam.Scope) beam.PCollection {
	sliceCol := beam.CreateList(s, b.Slices)
	shuffled := beam.Reshuffle(s.Scope("ReshuffleSlices"), sliceCol)
	partSDF := NewPartitionQuerySourceSDF(b.Reader, b.Config, b.SecretsConfig)
	return beam.ParDo(s, partSDF, shuffled)
}
