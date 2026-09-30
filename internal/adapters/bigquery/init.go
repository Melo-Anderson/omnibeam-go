package bigquery

import (
	"context"

	bq_beam "github.com/omnibeam/dataflow-compute-go/internal/beam/bigquery"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	ports.RegisterSink("bigquery", bqSinkFactory)
}

func bqSinkFactory(ctx context.Context, cfg *domain.PipelineConfig, deps ports.SinkDeps) (ports.BeamSinkBuilder, error) {
	// deps.Storage is intentionally unused — BigQuery does not write files.
	if err := cfg.Destination.BigQueryOptions.Validate(); err != nil {
		return nil, err
	}
	writer, err := NewStorageWriteAdapter(ctx)
	if err != nil {
		return nil, err
	}
	return bq_beam.NewBigQuerySinkBuilder(writer, cfg.Destination.BigQueryOptions, cfg.GetSchema()), nil
}
