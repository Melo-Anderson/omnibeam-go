package paged_api

import (
	"context"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	_ "github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type dummyPagedReader struct{}

func (dummyPagedReader) EstimatePages(_ context.Context, _ any) ([]ports.PageSlice, error) {
	return []ports.PageSlice{{PageIndex: 0}}, nil
}

func (dummyPagedReader) ReadPage(_ context.Context, _ any, _ ports.PageSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	recCh := make(chan *domain.GenericRecord)
	errCh := make(chan error)
	close(recCh)
	close(errCh)
	return recCh, errCh, nil
}

type dummyBatchWriter struct{}

func (dummyBatchWriter) WriteBatch(_ context.Context, _ []*domain.GenericRecord, _ *domain.Schema) ([]*domain.GenericRecord, []*domain.DeadLetterRecord, error) {
	return nil, nil, nil
}

func TestPagedAPIBuilders(t *testing.T) {
	p, _ := beam.NewPipelineWithRoot()
	s := p.Root()

	sourceBuilder := &PagedAPIBeamSource{
		Pages: []ports.PageSlice{
			{PageIndex: 0, PageSize: 100},
			{PageIndex: 1, PageSize: 100},
		},
		Reader: &dummyPagedReader{},
		Config: &domain.APISourceConfig{
			BaseURL:  "https://api.test.com",
			Endpoint: "/items",
		},
	}

	col := sourceBuilder.BuildSource(s)
	if !col.IsValid() {
		t.Error("expected valid PCollection from PagedAPIBeamSource")
	}

	sinkBuilder := &BatchAPIBeamSink{
		Endpoint: domain.APIEndpointConfig{
			BaseURL: "https://api.test.com/batch",
		},
		APIOptions: domain.APISinkOptions{
			BatchSize: 100,
		},
		Schema: domain.Schema{},
		Writer: &dummyBatchWriter{},
	}
	sinkBuilder.BuildSink(s, col)
}
