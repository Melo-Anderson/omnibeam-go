package paged_api

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type dummyPagedReader2 struct{}

func (dummyPagedReader2) EstimatePages(_ context.Context, _ any) ([]ports.PageSlice, error) {
	return []ports.PageSlice{{PageIndex: 0}}, nil
}

func (dummyPagedReader2) ReadPage(_ context.Context, _ any, _ ports.PageSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	return nil, nil, nil
}

func TestPagedAPIReaderProvider(t *testing.T) {
	SetPagedAPIReaderProvider(nil)
	if HasPagedAPIReaderProvider() || GetPagedAPIReaderProvider() != nil {
		t.Error("expected nil paged API reader provider")
	}

	SetPagedAPIReaderProvider(func(_ *domain.APISourceConfig) ports.PagedAPIReader {
		return &dummyPagedReader2{}
	})

	if !HasPagedAPIReaderProvider() || GetPagedAPIReaderProvider() == nil {
		t.Error("expected registered paged API reader provider")
	}
}
