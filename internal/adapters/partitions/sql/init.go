package sql

import (
	"context"
	"fmt"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	ports.RegisterSource("postgres", sqlReaderFactory)
	ports.RegisterSource("pgx", sqlReaderFactory)
	ports.RegisterSource("cockroach", sqlReaderFactory)
	ports.RegisterSource("mysql", sqlReaderFactory)
	ports.RegisterSource("mariadb", sqlReaderFactory)
}

func sqlReaderFactory(ctx context.Context, cfg *domain.PipelineConfig, sec ports.SecretResolver) (ports.PartitionedReader, error) {
	if cfg.DatabaseSource == nil {
		return nil, fmt.Errorf("database_source configuration is missing")
	}
	db, err := GetOrCreateDB(ctx, cfg.DatabaseSource, sec)
	if err != nil {
		return nil, fmt.Errorf("failed opening database pool: %w", err)
	}
	reader, err := NewSQLReader(cfg.DatabaseSource.Driver, db)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed creating sql reader: %w", err)
	}
	return reader, nil
}
