package paged_api

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// PagedAPIBeamSource builds a paginated API PCollection using PagedAPISourceSDF (Família 3).
type PagedAPIBeamSource struct {
	Pages  []ports.PageSlice
	Reader ports.PagedAPIReader
	Config *domain.APISourceConfig
}

// Compile-time assertion: PagedAPIBeamSource must satisfy ports.BeamSourceBuilder (LSP).
var _ ports.BeamSourceBuilder = (*PagedAPIBeamSource)(nil)

// BuildSource implements ports.BeamSourceBuilder for paginated APIs.
// Reshuffle distributes page slices across workers after the discovery phase.
func (b *PagedAPIBeamSource) BuildSource(s beam.Scope) beam.PCollection {
	pageCol := beam.CreateList(s, b.Pages)
	shuffled := beam.Reshuffle(s.Scope("ReshufflePages"), pageCol)
	pagedSDF := NewPagedAPISourceSDF(b.Reader, b.Config)
	return beam.ParDo(s, pagedSDF, shuffled)
}

// BatchAPIBeamSink builds an API micro-batching sink in the Beam DAG.
type BatchAPIBeamSink struct {
	Endpoint    domain.APIEndpointConfig
	APIOptions  domain.APISinkOptions
	Schema      domain.Schema
	DLQPath     string
	SecretToken string
	Writer      ports.BatchAPIWriter
	DLQSink     ports.StorageWriter
}

// Compile-time assertion: BatchAPIBeamSink must satisfy ports.BeamSinkBuilder (LSP).
var _ ports.BeamSinkBuilder = (*BatchAPIBeamSink)(nil)

// BuildSink builds the API batch sink transform.
func (s *BatchAPIBeamSink) BuildSink(scope beam.Scope, validRecords beam.PCollection) {
	dofn := NewAPISinkDoFn(
		s.Endpoint,
		s.APIOptions,
		s.Schema,
		s.DLQPath,
		s.SecretToken,
	)
	if s.Writer != nil {
		dofn.WithWriter(s.Writer)
	}
	if s.DLQSink != nil {
		dofn.WithDLQSink(s.DLQSink)
	}
	beam.ParDo0(scope, dofn, validRecords)
}
