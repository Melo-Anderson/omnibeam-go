package streams

import (
	"sync"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var (
	providerMu                  sync.RWMutex
	globalStorageReaderProvider func(uri string) ports.StorageReader
	globalStreamWrapperProvider func(cfg *domain.SourceConfig) ports.StreamWrapper
	globalStreamDecoderProvider func(cfg *domain.SourceConfig) ports.StreamDecoder
)

// SetStorageReaderProvider sets the process-level reader provider for ByteStream SDFs.
func SetStorageReaderProvider(p func(uri string) ports.StorageReader) {
	providerMu.Lock()
	defer providerMu.Unlock()
	globalStorageReaderProvider = p
}

// GetStorageReaderProvider returns the process-level reader provider for ByteStream SDFs.
func GetStorageReaderProvider() func(uri string) ports.StorageReader {
	providerMu.RLock()
	defer providerMu.RUnlock()
	return globalStorageReaderProvider
}

// HasStorageReaderProvider reports whether the global storage reader provider is registered.
func HasStorageReaderProvider() bool {
	providerMu.RLock()
	defer providerMu.RUnlock()
	return globalStorageReaderProvider != nil
}

// SetStreamWrapperProvider sets the process-level stream wrapper provider for ByteStream SDFs.
func SetStreamWrapperProvider(p func(cfg *domain.SourceConfig) ports.StreamWrapper) {
	providerMu.Lock()
	defer providerMu.Unlock()
	globalStreamWrapperProvider = p
}

// GetStreamWrapperProvider returns the process-level stream wrapper provider for ByteStream SDFs.
func GetStreamWrapperProvider() func(cfg *domain.SourceConfig) ports.StreamWrapper {
	providerMu.RLock()
	defer providerMu.RUnlock()
	return globalStreamWrapperProvider
}

// HasStreamWrapperProvider reports whether the global stream wrapper provider is registered.
func HasStreamWrapperProvider() bool {
	providerMu.RLock()
	defer providerMu.RUnlock()
	return globalStreamWrapperProvider != nil
}

// SetStreamDecoderProvider sets the process-level stream decoder provider for ByteStream SDFs.
func SetStreamDecoderProvider(p func(cfg *domain.SourceConfig) ports.StreamDecoder) {
	providerMu.Lock()
	defer providerMu.Unlock()
	globalStreamDecoderProvider = p
}

// GetStreamDecoderProvider returns the process-level stream decoder provider for ByteStream SDFs.
func GetStreamDecoderProvider() func(cfg *domain.SourceConfig) ports.StreamDecoder {
	providerMu.RLock()
	defer providerMu.RUnlock()
	return globalStreamDecoderProvider
}

// HasStreamDecoderProvider reports whether the global stream decoder provider is registered.
func HasStreamDecoderProvider() bool {
	providerMu.RLock()
	defer providerMu.RUnlock()
	return globalStreamDecoderProvider != nil
}
