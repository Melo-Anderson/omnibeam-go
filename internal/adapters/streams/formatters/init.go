package formatters

import (
	"context"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/streams"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	// Delimited file formats
	ports.RegisterSink("csv", delimitedSinkFactory)
	ports.RegisterSink("txt", delimitedSinkFactory)
	ports.RegisterSink("tsv", delimitedSinkFactory)
	ports.RegisterSink("json", delimitedSinkFactory)
	ports.RegisterSink("jsonl", delimitedSinkFactory)
	ports.RegisterSink("jsonlines", delimitedSinkFactory)

	// Columnar Parquet format
	ports.RegisterSink("parquet", parquetSinkFactory)

	// File / Storage generic dispatchers
	ports.RegisterSink("file", fileDispatcherSinkFactory)
	ports.RegisterSink("storage", fileDispatcherSinkFactory)
	ports.RegisterSink("local_storage", fileDispatcherSinkFactory)
	ports.RegisterSink("gcs", fileDispatcherSinkFactory)
	ports.RegisterSink("s3", fileDispatcherSinkFactory)
}

func delimitedSinkFactory(_ context.Context, cfg *domain.PipelineConfig, deps ports.SinkDeps) (ports.BeamSinkBuilder, error) {
	outFormat := cfg.Destination.OutputFormat
	formatter, err := BuildFormatter(outFormat, cfg.Destination.FormatOptions)
	if err != nil {
		return nil, err
	}
	return &streams.StreamFileBeamSink{
		Storage:       deps.Storage,
		Formatter:     formatter,
		OutputDir:     cfg.Destination.OutputPath,
		Format:        outFormat,
		Compression:   cfg.Destination.Compression,
		Encryption:    cfg.Destination.Encryption,
		FormatOptions: cfg.Destination.FormatOptions, // forwarded for remote worker serialization
		SingleFile:    cfg.Destination.SingleFile,
		Schema:        cfg.GetSchema(),
	}, nil
}

func parquetSinkFactory(_ context.Context, cfg *domain.PipelineConfig, deps ports.SinkDeps) (ports.BeamSinkBuilder, error) {
	includeSourceFile := cfg.DatabaseSource == nil && cfg.APISource == nil
	return &streams.ParquetBeamSink{
		Storage:           deps.Storage,
		OutputPath:        cfg.Destination.OutputPath,
		Compression:       cfg.Destination.Compression,
		Encryption:        cfg.Destination.Encryption.Type,
		IncludeSourceFile: includeSourceFile,
		Schema:            cfg.GetSchema(),
	}, nil
}

func fileDispatcherSinkFactory(ctx context.Context, cfg *domain.PipelineConfig, deps ports.SinkDeps) (ports.BeamSinkBuilder, error) {
	targetFormat := cfg.Destination.OutputFormat
	if targetFormat == "" || targetFormat == cfg.Destination.Type {
		targetFormat = domain.DefaultDestinationOutputFormat
	}
	return ports.BuildSink(ctx, targetFormat, cfg, deps)
}
