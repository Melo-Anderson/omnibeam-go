package bigquery

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// BigQuerySinkBuilder implements ports.BeamSinkBuilder for the BigQuery destination.
type BigQuerySinkBuilder struct {
	Writer ports.BigQueryWriter
	Config domain.BigQuerySinkConfig
	Schema domain.Schema
}

var _ ports.BeamSinkBuilder = (*BigQuerySinkBuilder)(nil)

// NewBigQuerySinkBuilder creates a new builder.
func NewBigQuerySinkBuilder(w ports.BigQueryWriter, cfg domain.BigQuerySinkConfig, schema domain.Schema) *BigQuerySinkBuilder {
	return &BigQuerySinkBuilder{Writer: w, Config: cfg, Schema: schema}
}

func (b *BigQuerySinkBuilder) BuildSink(s beam.Scope, validRecords beam.PCollection) {
	beam.ParDo0(s.Scope("BigQuerySink"), NewBigQuerySinkDoFn(b.Writer, b.Config, b.Schema), validRecords)
}
