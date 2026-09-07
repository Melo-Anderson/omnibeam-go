package ports

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// ErrUnregisteredConnector is returned when no factory is registered for the requested driver/target.
var ErrUnregisteredConnector = errors.New("unregistered connector")

// SourceFactory constructs a PartitionedReader from a pipeline config.
type SourceFactory func(ctx context.Context, cfg *domain.PipelineConfig, sec SecretResolver) (PartitionedReader, error)

// PagedAPISourceFactory constructs a PagedAPIReader from a pipeline config.
type PagedAPISourceFactory func(ctx context.Context, cfg *domain.PipelineConfig, sec SecretResolver) (PagedAPIReader, error)

// BatchAPIWriterFactory constructs a BatchAPIWriter from a pipeline config.
type BatchAPIWriterFactory func(ctx context.Context, cfg *domain.PipelineConfig, sec SecretResolver) (BatchAPIWriter, error)

// SinkFactory constructs a BeamSinkBuilder from a pipeline config.
type SinkFactory func(ctx context.Context, cfg *domain.PipelineConfig, sec SecretResolver, st StorageBackend) (BeamSinkBuilder, error)

var (
	sourceMu       sync.RWMutex
	sourceRegistry = make(map[string]SourceFactory)

	pagedSourceMu       sync.RWMutex
	pagedSourceRegistry = make(map[string]PagedAPISourceFactory)

	batchWriterMu       sync.RWMutex
	batchWriterRegistry = make(map[string]BatchAPIWriterFactory)

	sinkMu       sync.RWMutex
	sinkRegistry = make(map[string]SinkFactory)
)

// RegisterSource registers a SourceFactory for partitioned databases (postgres, mysql, mongodb).
func RegisterSource(driver string, factory SourceFactory) {
	sourceMu.Lock()
	defer sourceMu.Unlock()
	if _, exists := sourceRegistry[driver]; exists {
		panic(fmt.Sprintf("ports: source driver %q already registered", driver))
	}
	sourceRegistry[driver] = factory
}

// RegisterPagedAPISource registers a factory for paginated API sources.
func RegisterPagedAPISource(sourceType string, factory PagedAPISourceFactory) {
	pagedSourceMu.Lock()
	defer pagedSourceMu.Unlock()
	if _, exists := pagedSourceRegistry[sourceType]; exists {
		panic(fmt.Sprintf("ports: paged api source %q already registered", sourceType))
	}
	pagedSourceRegistry[sourceType] = factory
}

// RegisterBatchAPIWriter registers a factory for micro-batching API sinks.
func RegisterBatchAPIWriter(targetType string, factory BatchAPIWriterFactory) {
	batchWriterMu.Lock()
	defer batchWriterMu.Unlock()
	if _, exists := batchWriterRegistry[targetType]; exists {
		panic(fmt.Sprintf("ports: batch api writer %q already registered", targetType))
	}
	batchWriterRegistry[targetType] = factory
}

// RegisterSink registers a SinkFactory for Beam sink builders.
func RegisterSink(targetType string, factory SinkFactory) {
	sinkMu.Lock()
	defer sinkMu.Unlock()
	if _, exists := sinkRegistry[targetType]; exists {
		panic(fmt.Sprintf("ports: sink target %q already registered", targetType))
	}
	sinkRegistry[targetType] = factory
}

// BuildSource resolves and invokes the factory registered for driver.
func BuildSource(ctx context.Context, driver string, cfg *domain.PipelineConfig, sec SecretResolver) (PartitionedReader, error) {
	sourceMu.RLock()
	factory, ok := sourceRegistry[driver]
	sourceMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: source driver %q", ErrUnregisteredConnector, driver)
	}
	return factory(ctx, cfg, sec)
}

// BuildPagedAPISource resolves and invokes the paged API factory.
func BuildPagedAPISource(ctx context.Context, sourceType string, cfg *domain.PipelineConfig, sec SecretResolver) (PagedAPIReader, error) {
	pagedSourceMu.RLock()
	factory, ok := pagedSourceRegistry[sourceType]
	pagedSourceMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: paged api source %q", ErrUnregisteredConnector, sourceType)
	}
	return factory(ctx, cfg, sec)
}

// BuildBatchAPIWriter resolves and invokes the batch API writer factory.
func BuildBatchAPIWriter(ctx context.Context, targetType string, cfg *domain.PipelineConfig, sec SecretResolver) (BatchAPIWriter, error) {
	batchWriterMu.RLock()
	factory, ok := batchWriterRegistry[targetType]
	batchWriterMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: batch api writer %q", ErrUnregisteredConnector, targetType)
	}
	return factory(ctx, cfg, sec)
}

// BuildSink resolves and invokes the sink factory.
func BuildSink(ctx context.Context, targetType string, cfg *domain.PipelineConfig, sec SecretResolver, st StorageBackend) (BeamSinkBuilder, error) {
	sinkMu.RLock()
	factory, ok := sinkRegistry[targetType]
	sinkMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: sink target %q", ErrUnregisteredConnector, targetType)
	}
	return factory(ctx, cfg, sec, st)
}
