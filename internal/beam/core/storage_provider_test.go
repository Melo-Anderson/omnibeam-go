package core

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestStorageProvider(t *testing.T) {
	SetStorageFactory(nil)
	if GetStorageFactory() != nil {
		t.Error("expected nil storage factory")
	}

	mockFactory := func(_ string) ports.StorageWriter {
		return &fakeStorage{}
	}

	SetStorageFactory(mockFactory)
	if GetStorageFactory() == nil {
		t.Error("expected non-nil storage factory after SetStorageFactory")
	}
	writer := GetStorageFactory()("test-uri")
	if writer == nil {
		t.Error("expected valid writer from configured factory")
	}
}
