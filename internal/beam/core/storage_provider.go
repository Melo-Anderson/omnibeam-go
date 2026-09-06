package core

import (
	"sync"

	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var (
	storageMu            sync.RWMutex
	globalStorageFactory func(uri string) ports.StorageWriter
)

// SetStorageFactory configures the process-level storage factory for Beam worker bundles.
func SetStorageFactory(f func(uri string) ports.StorageWriter) {
	storageMu.Lock()
	defer storageMu.Unlock()
	globalStorageFactory = f
}

// GetStorageFactory returns the process-level storage factory.
func GetStorageFactory() func(uri string) ports.StorageWriter {
	storageMu.RLock()
	defer storageMu.RUnlock()
	return globalStorageFactory
}
