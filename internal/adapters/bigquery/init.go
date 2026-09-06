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

func bqSinkFactory(ctx context.Context, cfg *domain.PipelineConfig, _ ports.SecretResolver, _ ports.StorageBackend) (ports.BeamSinkBuilder, error) {
	if err := cfg.Destination.BigQueryOptions.Validate(); err != nil {
		return nil, err
	}
	writer, err := NewStorageWriteAdapter(ctx)
	if err != nil {
		return nil, err
	}
	return bq_beam.NewBigQuerySinkBuilder(writer, cfg.Destination.BigQueryOptions, cfg.GetSchema()), nil
}
