package main

import (
	"context"
	"fmt"
	"strings"

	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/paged_api/sink"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/secrets"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/formatters"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/paged_api"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/partitions"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/streams"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// ============================================================================
// 1. Modular Source Builders (SRP & OCP)
// ============================================================================

func buildDatabaseSource(
	ctx context.Context,
	cfg *domain.PipelineConfig,
	secretResolver ports.SecretResolver,
) (ports.BeamSourceBuilder, error) {
	if cfg.DatabaseSource == nil {
		return nil, fmt.Errorf("database_source config is required")
	}
	if cfg.SecretsConfig != nil {
		secrets.ApplyDatabaseHostOverrides(cfg.DatabaseSource, cfg.SecretsConfig.HostOverride)
	}

	reader, err := ports.BuildSource(ctx, cfg.DatabaseSource.Driver, cfg, secretResolver)
	if err != nil {
		return nil, fmt.Errorf("failed creating discovery reader: %w", err)
	}
	defer reader.Close()

	slices, err := reader.CalculatePartitions(ctx, cfg.DatabaseSource)
	if err != nil {
		return nil, fmt.Errorf("failed calculating partitions: %w", err)
	}
	return &partitions.PartitionedBeamSource{
		Slices:        slices,
		Reader:        nil, // Workers instantiate their own pools via DoFn.Setup()
		Config:        cfg.DatabaseSource,
		SecretsConfig: cfg.SecretsConfig,
	}, nil
}

func buildAPISource(
	ctx context.Context,
	cfg *domain.PipelineConfig,
	secretResolver ports.SecretResolver,
) (ports.BeamSourceBuilder, error) {
	restReader, err := ports.BuildPagedAPISource(ctx, "rest_api", cfg, secretResolver)
	if err != nil {
		return nil, fmt.Errorf("failed resolving paged api source: %w", err)
	}
	paged_api.SetPagedAPIReaderProvider(func(_ *domain.APISourceConfig) ports.PagedAPIReader {
		return restReader
	})
	pages, err := restReader.EstimatePages(ctx, cfg.APISource)
	if err != nil {
		return nil, fmt.Errorf("failed estimating API pages: %w", err)
	}
	return &paged_api.PagedAPIBeamSource{
		Pages:  pages,
		Reader: restReader,
		Config: cfg.APISource,
	}, nil
}

func buildByteStreamSource(
	ctx context.Context,
	cfg *domain.PipelineConfig,
	storageBackend ports.StorageBackend,
) (ports.BeamSourceBuilder, error) {
	pb := codecs.NewPipelineBuilder()
	decoder, err := pb.BuildDecoder(&cfg.Source)
	if err != nil {
		return nil, fmt.Errorf("decoder initialization failed: %w", err)
	}

	var files []string
	if len(cfg.Source.Paths) > 0 {
		for _, p := range cfg.Source.Paths {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if strings.Contains(p, "*") {
				expanded, err := storageBackend.List(ctx, p)
				if err != nil {
					return nil, fmt.Errorf("failed listing source files for pattern %s: %w", p, err)
				}
				files = append(files, expanded...)
			} else {
				files = append(files, p)
			}
		}
	} else if strings.TrimSpace(cfg.Source.Path) != "" {
		listed, err := storageBackend.List(ctx, cfg.Source.Path)
		if err != nil {
			return nil, fmt.Errorf("failed listing source files: %w", err)
		}
		files = listed
	}

	if len(files) == 0 {
		files = cfg.Source.AllPaths()
	}

	return &streams.ByteStreamBeamSource{
		URIs:          files,
		Storage:       storageBackend,
		StreamWrapper: pb,
		Decoder:       decoder,
		SourceConfig:  cfg.Source,
	}, nil
}

// buildSourceEngine dispatches source construction to dedicated builders (OCP & SRP).
func buildSourceEngine(
	ctx context.Context,
	cfg *domain.PipelineConfig,
	storageBackend ports.StorageBackend,
	secretResolver ports.SecretResolver,
) (ports.BeamSourceBuilder, error) {
	switch {
	case cfg.DatabaseSource != nil:
		return buildDatabaseSource(ctx, cfg, secretResolver)
	case cfg.APISource != nil:
		return buildAPISource(ctx, cfg, secretResolver)
	default:
		return buildByteStreamSource(ctx, cfg, storageBackend)
	}
}

// ============================================================================
// 2. Modular Destination Sink Builders (OCP & SRP)
// ============================================================================

// buildDestinationSink dispatches sink construction to dedicated builders (OCP & SRP).
func buildDestinationSink(
	ctx context.Context,
	cfg *domain.PipelineConfig,
	storageBackend ports.StorageBackend,
	secretResolver ports.SecretResolver,
	_ domain.Schema,
) (ports.BeamSinkBuilder, error) {
	return ports.BuildSink(ctx, cfg.Destination.Type, cfg, secretResolver, storageBackend)
}

// ============================================================================
// 3. DLQ & Audit Sinks
// ============================================================================

func buildDLQSink(cfg *domain.PipelineConfig, storageBackend ports.StorageBackend) ports.BeamDLQSinkBuilder {
	return &core.DefaultDLQBeamSink{
		Storage: storageBackend,
		DLQPath: cfg.DLQConfig.QuarantinePath,
	}
}

func buildAuditSink(cfg *domain.PipelineConfig, storageBackend ports.StorageBackend) ports.BeamAuditSinkBuilder {
	if cfg.DLQConfig.QuarantinePath == "" {
		return nil
	}
	return &core.DefaultAuditBeamSink{
		Storage:  storageBackend,
		AuditDir: cfg.DLQConfig.QuarantinePath,
	}
}
