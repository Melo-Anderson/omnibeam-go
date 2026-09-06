package streams

import (
	"context"
	"io"
	"sync"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type dummyStorageReader struct{}

func (dummyStorageReader) Open(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }
func (dummyStorageReader) List(_ context.Context, _ string) ([]string, error)      { return nil, nil }
func (dummyStorageReader) Size(_ context.Context, _ string) (int64, error)         { return 0, nil }

type dummyStreamWrapper struct{}

func (dummyStreamWrapper) WrapStream(r io.Reader, _ *domain.SourceConfig) (io.Reader, error) {
	return r, nil
}

type dummyStreamDecoder struct{}

func (dummyStreamDecoder) Decode(_ context.Context, _ io.Reader, _ *domain.Schema, _ string) (<-chan *domain.GenericRecord, <-chan error, error) {
	return nil, nil, nil
}

func TestReaderProvider(t *testing.T) {
	// Storage Reader Provider
	SetStorageReaderProvider(nil)
	if HasStorageReaderProvider() || GetStorageReaderProvider() != nil {
		t.Error("expected nil storage reader provider")
	}
	SetStorageReaderProvider(func(_ string) ports.StorageReader {
		return &dummyStorageReader{}
	})
	if !HasStorageReaderProvider() || GetStorageReaderProvider() == nil {
		t.Error("expected registered storage reader provider")
	}

	// Stream Wrapper Provider
	SetStreamWrapperProvider(nil)
	if HasStreamWrapperProvider() || GetStreamWrapperProvider() != nil {
		t.Error("expected nil stream wrapper provider")
	}
	SetStreamWrapperProvider(func(_ *domain.SourceConfig) ports.StreamWrapper {
		return &dummyStreamWrapper{}
	})
	if !HasStreamWrapperProvider() || GetStreamWrapperProvider() == nil {
		t.Error("expected registered stream wrapper provider")
	}

	// Stream Decoder Provider
	SetStreamDecoderProvider(nil)
	if HasStreamDecoderProvider() || GetStreamDecoderProvider() != nil {
		t.Error("expected nil stream decoder provider")
	}
	SetStreamDecoderProvider(func(_ *domain.SourceConfig) ports.StreamDecoder {
		return &dummyStreamDecoder{}
	})
	if !HasStreamDecoderProvider() || GetStreamDecoderProvider() == nil {
		t.Error("expected registered stream decoder provider")
	}
}

func TestReaderProvider_ConcurrentAccess(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			SetStorageReaderProvider(func(uri string) ports.StorageReader { return nil })
		}()
		go func() {
			defer wg.Done()
			_ = GetStorageReaderProvider()
		}()
	}
	wg.Wait()
}

