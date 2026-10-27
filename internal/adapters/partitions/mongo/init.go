package mongo

import (
	"context"
	"fmt"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	ports.RegisterSource("mongo", mongoReaderFactory)
	ports.RegisterSource("mongodb", mongoReaderFactory)
}

func mongoReaderFactory(ctx context.Context, cfg *domain.PipelineConfig, sec ports.SecretResolver) (ports.PartitionedReader, error) {
	if cfg.DatabaseSource == nil {
		return nil, fmt.Errorf("database_source configuration is missing")
	}
	if cfg.DatabaseSource.Watermark != nil {
		return nil, fmt.Errorf("watermark is not supported for mongodb source")
	}
	client, err := OpenClient(ctx, cfg.DatabaseSource, sec)
	if err != nil {
		return nil, fmt.Errorf("failed connecting to mongodb: %w", err)
	}
	return NewMongoSource(client, cfg.DatabaseSource.Database), nil
}
