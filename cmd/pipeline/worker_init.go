package main

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/storage"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/codecs/compression"
	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/formatters"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/partitions"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/streams"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	// beam.RegisterInit hooks execute automatically on both the submitter process
	// and inside each remote worker binary before the Beam harness starts bundles.
	beam.RegisterInit(registerWorkerProviders)
}

func registerWorkerProviders() {
	registerWorkerDBProviders()
	registerWorkerStorageProviders()
	registerWorkerStreamProviders()
}

// registerWorkerStorageProviders registers storage reader and writer factories
// so ByteStreamSourceSDF and sink DoFns can reconstruct storage handles in remote workers.
func registerWorkerStorageProviders() {
	local := storage.NewLocalStorage()
	ctx := context.Background()
	var gcsBackend ports.StorageBackend
	if gcs, err := storage.NewGCSStorage(ctx); err == nil {
		gcsBackend = gcs
	}
	resolver := storage.NewStorageResolver(local, gcsBackend)

	streams.SetStorageReaderProvider(func(_ string) ports.StorageReader {
		return resolver
	})
	core.SetStorageFactory(func(_ string) ports.StorageWriter {
		return resolver
	})
}

// registerWorkerStreamProviders registers stream wrapper and decoder factories
// so ByteStreamSourceSDF can reconstruct decompression and parsing pipelines on workers.
func registerWorkerStreamProviders() {
	streams.SetStreamWrapperProvider(func(cfg *domain.SourceConfig) ports.StreamWrapper {
		return codecs.NewPipelineBuilder()
	})
	streams.SetStreamDecoderProvider(func(cfg *domain.SourceConfig) ports.StreamDecoder {
		dec, _ := codecs.NewPipelineBuilder().BuildDecoder(cfg)
		return dec
	})
	streams.SetRecordFormatterProvider(func(format string, schema *domain.Schema) ports.RecordFormatter {
		formatter, _ := formatters.BuildFormatter(format, domain.FormatOptions{Delimiter: ",", IncludeHeader: true})
		return formatter
	})
	streams.SetCompressorProvider(compression.WrapCompressor)
}

func registerWorkerDBProviders() {
	partitions.SetPartitionedReaderProvider(workerDBProvider)
}

func workerDBProvider(ctx context.Context, dbCfg *domain.DatabaseSourceConfig, secCfg *domain.SecretsConfig) (ports.PartitionedReader, error) {
	secretResolver := buildSecretResolver(ctx, secCfg)
	pCfg := &domain.PipelineConfig{
		DatabaseSource: dbCfg,
		SecretsConfig:  secCfg,
	}
	return ports.BuildSource(ctx, dbCfg.Driver, pCfg, secretResolver)
}
