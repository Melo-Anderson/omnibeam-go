package main

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/streams"
)

func TestWorkerInit_RegistersAllProviders(t *testing.T) {
	// Clear providers to simulate an uninitialized state.
	streams.SetStorageReaderProvider(nil)
	streams.SetStreamWrapperProvider(nil)
	streams.SetStreamDecoderProvider(nil)
	core.SetStorageFactory(nil)

	registerWorkerStorageProviders()
	registerWorkerStreamProviders()

	if !streams.HasStorageReaderProvider() {
		t.Error("storage reader provider was not registered by registerWorkerStorageProviders")
	}
	if !streams.HasStreamWrapperProvider() {
		t.Error("stream wrapper provider was not registered")
	}
	if !streams.HasStreamDecoderProvider() {
		t.Error("stream decoder provider was not registered")
	}
	if core.GetStorageFactory() == nil {
		t.Error("storage factory was not registered in core")
	}
}
