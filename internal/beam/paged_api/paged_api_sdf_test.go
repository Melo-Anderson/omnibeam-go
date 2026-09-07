package paged_api

import (
	"context"
	"errors"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type fakePagedAPIReader struct {
	records []*domain.GenericRecord
}

func (r *fakePagedAPIReader) EstimatePages(_ context.Context, _ any) ([]ports.PageSlice, error) {
	return []ports.PageSlice{{PageIndex: 0, PageSize: 50}}, nil
}

func (r *fakePagedAPIReader) ReadPage(_ context.Context, _ any, _ ports.PageSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	recChan := make(chan *domain.GenericRecord, len(r.records))
	errChan := make(chan error)
	for _, rec := range r.records {
		recChan <- rec
	}
	close(recChan)
	close(errChan)
	return recChan, errChan, nil
}

func TestPagedAPISourceSDF_ProcessElement(t *testing.T) {
	rec := domain.NewGenericRecord("schema-api", 1)
	rec.SetString(0, "ACC-001")

	tests := []struct {
		name        string
		records     []*domain.GenericRecord
		page        ports.PageSlice
		wantEmitted int
		wantVal     string
	}{
		{
			name:        "process valid page",
			records:     []*domain.GenericRecord{rec},
			page:        ports.PageSlice{PageIndex: 0, PageSize: 50},
			wantEmitted: 1,
			wantVal:     "ACC-001",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakePagedAPIReader{records: tc.records}
			fn := NewPagedAPISourceSDF(reader, nil)

			tracker := NewPageRangeTracker(PageRange{Start: 0, End: 1})

			var emitted []*domain.GenericRecord
			err := fn.ProcessElement(context.Background(), tracker, tc.page, func(r *domain.GenericRecord) {
				emitted = append(emitted, r)
			})

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(emitted) != tc.wantEmitted {
				t.Errorf("expected %d emitted record, got %d", tc.wantEmitted, len(emitted))
			}
			if len(emitted) > 0 && emitted[0].Values[0].StringVal() != tc.wantVal {
				t.Errorf("expected payload %s, got %v", tc.wantVal, emitted[0].Values[0].StringVal())
			}
		})
	}
}

func TestPagedAPISourceSDF_RestrictionSize_WithPageSize(t *testing.T) {
	fn := NewPagedAPISourceSDF(nil, nil)
	page := ports.PageSlice{PageIndex: 0, PageSize: 250}
	rest := PageRange{Start: 0, End: 1}
	size := fn.RestrictionSize(page, rest)
	if size != 250.0 {
		t.Errorf("expected 250.0, got %f", size)
	}
}

func TestPagedAPISourceSDF_RestrictionSize_DefaultPageSize(t *testing.T) {
	fn := NewPagedAPISourceSDF(nil, nil)
	page := ports.PageSlice{PageIndex: 0, PageSize: 0}
	rest := PageRange{Start: 0, End: 1}
	size := fn.RestrictionSize(page, rest)
	if size != 100.0 {
		t.Errorf("expected default 100.0, got %f", size)
	}

	invertedSize := fn.RestrictionSize(page, PageRange{Start: 5, End: 2})
	if invertedSize != 0.0 {
		t.Errorf("expected 0.0 for inverted range, got %f", invertedSize)
	}
}

func TestPagedAPISourceSDF_SDFMethods(t *testing.T) {
	fn := NewPagedAPISourceSDF(nil, nil)

	// CreateInitialRestriction
	initRest, err := fn.CreateInitialRestriction(context.Background(), ports.PageSlice{PageIndex: 0})
	if err != nil || initRest.Start != 0 || initRest.End != 1 {
		t.Errorf("unexpected initRest: %+v, err: %v", initRest, err)
	}

	// SplitRestriction
	splits := fn.SplitRestriction(ports.PageSlice{PageIndex: 0}, initRest)
	if len(splits) != 1 || splits[0] != initRest {
		t.Errorf("unexpected splits: %+v", splits)
	}

	// CreateTracker
	tracker := fn.CreateTracker(initRest)
	if tracker == nil {
		t.Fatal("expected non-nil tracker from CreateTracker")
	}

	// ProcessElement when tracker is stopped
	tracker.MarkDone()
	count := 0
	err = fn.ProcessElement(context.Background(), tracker, ports.PageSlice{PageIndex: 0}, func(r *domain.GenericRecord) { count++ })
	if err != nil {
		t.Errorf("unexpected error when claim fails: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records when claim fails, got %d", count)
	}
}

func TestPagedAPISourceSDF_Setup_InitializesReader(t *testing.T) {
	mockReader := &fakePagedAPIReader{}
	SetPagedAPIReaderProvider(func(_ *domain.APISourceConfig) ports.PagedAPIReader {
		return mockReader
	})
	defer SetPagedAPIReaderProvider(nil)

	fn := NewPagedAPISourceSDF(nil, &domain.APISourceConfig{})
	if err := fn.Setup(context.Background()); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	if fn.reader == nil {
		t.Fatal("expected reader initialized by Setup, got nil")
	}
}

func TestPagedAPISourceSDF_Teardown_DoesNotPanic(t *testing.T) {
	fn := NewPagedAPISourceSDF(nil, &domain.APISourceConfig{})
	if err := fn.Teardown(); err != nil {
		t.Fatalf("Teardown on uninitialized SDF: %v", err)
	}
}

type errPagedReader struct{}

func (errPagedReader) EstimatePages(_ context.Context, _ any) ([]ports.PageSlice, error) {
	return nil, nil
}

func (errPagedReader) ReadPage(_ context.Context, _ any, _ ports.PageSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	recChan := make(chan *domain.GenericRecord)
	errChan := make(chan error, 1)
	errChan <- errors.New("read page stream failed")
	close(recChan)
	close(errChan)
	return recChan, errChan, nil
}

func TestPagedAPISourceSDF_ReadPageError(t *testing.T) {
	fn := NewPagedAPISourceSDF(&errPagedReader{}, nil)
	tracker := NewPageRangeTracker(PageRange{Start: 0, End: 1})
	err := fn.ProcessElement(context.Background(), tracker, ports.PageSlice{}, func(r *domain.GenericRecord) {})
	if err == nil {
		t.Error("expected error when ReadPage errChan yields an error, got nil")
	}
}
