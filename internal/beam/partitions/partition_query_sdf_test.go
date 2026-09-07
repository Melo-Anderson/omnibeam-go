package partitions

import (
	"context"
	"errors"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type fakePartitionedReader struct {
	records []*domain.GenericRecord
	closed  bool
}

func (r *fakePartitionedReader) CalculatePartitions(_ context.Context, _ any) ([]ports.PartitionSlice, error) {
	return []ports.PartitionSlice{{SliceIndex: 0}}, nil
}

func (r *fakePartitionedReader) ReadPartition(_ context.Context, _ any, _ ports.PartitionSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	recChan := make(chan *domain.GenericRecord, len(r.records))
	errChan := make(chan error)
	for _, rec := range r.records {
		recChan <- rec
	}
	close(recChan)
	close(errChan)
	return recChan, errChan, nil
}

func (r *fakePartitionedReader) Close() error {
	r.closed = true
	return nil
}

func TestPartitionQuerySourceSDF_ProcessElement(t *testing.T) {
	rec1 := domain.NewGenericRecord("schema-1", 1)
	rec1.SetInt64(0, 1)
	rec2 := domain.NewGenericRecord("schema-1", 1)
	rec2.SetInt64(0, 2)

	tests := []struct {
		name        string
		records     []*domain.GenericRecord
		slice       ports.PartitionSlice
		wantEmitted int
	}{
		{
			name:        "process valid slice",
			records:     []*domain.GenericRecord{rec1, rec2},
			slice:       ports.PartitionSlice{SliceIndex: 0},
			wantEmitted: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := &fakePartitionedReader{records: tc.records}
			fn := NewPartitionQuerySourceSDF(reader, nil, nil)

			tracker := NewPartitionRangeTracker(PartitionRange{Start: 0, End: 1})

			var emitted []*domain.GenericRecord
			err := fn.ProcessElement(context.Background(), tracker, tc.slice, func(r *domain.GenericRecord) {
				emitted = append(emitted, r)
			})

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(emitted) != tc.wantEmitted {
				t.Errorf("expected %d emitted records, got %d", tc.wantEmitted, len(emitted))
			}
		})
	}
}

func TestPartitionQuerySourceSDF_Lifecycle(t *testing.T) {
	t.Run("Setup initializes reader from provider when none pre-injected", func(t *testing.T) {
		mockReader := &fakePartitionedReader{}
		SetPartitionedReaderProvider(func(_ context.Context, dbCfg *domain.DatabaseSourceConfig, _ *domain.SecretsConfig) (ports.PartitionedReader, error) {
			if dbCfg == nil {
				return nil, errors.New("dbCfg must not be nil")
			}
			return mockReader, nil
		})
		defer SetPartitionedReaderProvider(nil)

		dbCfg := &domain.DatabaseSourceConfig{Driver: "postgres"}
		fn := NewPartitionQuerySourceSDF(nil, dbCfg, nil)

		if err := fn.Setup(context.Background()); err != nil {
			t.Fatalf("unexpected Setup error: %v", err)
		}

		if err := fn.Teardown(); err != nil {
			t.Fatalf("unexpected Teardown error: %v", err)
		}

		if !mockReader.closed {
			t.Errorf("expected reader to be closed during Teardown")
		}
	})

	t.Run("Setup skips provider when reader is pre-injected (test path)", func(t *testing.T) {
		preInjected := &fakePartitionedReader{}
		SetPartitionedReaderProvider(nil)

		fn := NewPartitionQuerySourceSDF(preInjected, nil, nil)

		if err := fn.Setup(context.Background()); err != nil {
			t.Fatalf("unexpected Setup error with pre-injected reader: %v", err)
		}

		if err := fn.Teardown(); err != nil {
			t.Fatalf("unexpected Teardown error: %v", err)
		}

		if !preInjected.closed {
			t.Errorf("expected pre-injected reader to be closed during Teardown")
		}
	})

	t.Run("Setup returns error when no reader and no provider registered", func(t *testing.T) {
		SetPartitionedReaderProvider(nil)
		fn := NewPartitionQuerySourceSDF(nil, &domain.DatabaseSourceConfig{Driver: "postgres"}, nil)
		if err := fn.Setup(context.Background()); err == nil {
			t.Fatal("expected error when no provider is set, got nil")
		}
	})
}

func TestPartitionQuerySourceSDF_RestrictionSize_WithBatchSize(t *testing.T) {
	dbCfg := &domain.DatabaseSourceConfig{
		PartitionConfig: domain.PartitionConfig{BatchSize: 5000},
	}
	fn := NewPartitionQuerySourceSDF(nil, dbCfg, nil)
	rest := PartitionRange{Start: 0, End: 1}
	size := fn.RestrictionSize(ports.PartitionSlice{SliceIndex: 0}, rest)
	if size != 5000.0 {
		t.Errorf("expected 5000.0, got %f", size)
	}
}

func TestPartitionQuerySourceSDF_RestrictionSize_DefaultBatchSize(t *testing.T) {
	fn := NewPartitionQuerySourceSDF(nil, nil, nil)
	rest := PartitionRange{Start: 0, End: 1}
	size := fn.RestrictionSize(ports.PartitionSlice{SliceIndex: 0}, rest)
	if size != 2500.0 {
		t.Errorf("expected default 2500.0, got %f", size)
	}

	// Inverted range
	invertedSize := fn.RestrictionSize(ports.PartitionSlice{}, PartitionRange{Start: 5, End: 2})
	if invertedSize != 0.0 {
		t.Errorf("expected 0.0 for inverted range, got %f", invertedSize)
	}
}

func TestPartitionQuerySourceSDF_SDFMethods(t *testing.T) {
	fn := NewPartitionQuerySourceSDF(nil, nil, nil)

	// CreateInitialRestriction
	initRest, err := fn.CreateInitialRestriction(context.Background(), ports.PartitionSlice{SliceIndex: 0})
	if err != nil || initRest.Start != 0 || initRest.End != 1 {
		t.Errorf("unexpected initial restriction: %+v, err: %v", initRest, err)
	}

	// SplitRestriction
	splits := fn.SplitRestriction(ports.PartitionSlice{SliceIndex: 0}, initRest)
	if len(splits) != 1 || splits[0] != initRest {
		t.Errorf("unexpected splits: %+v", splits)
	}

	// CreateTracker
	tracker := fn.CreateTracker(initRest)
	if tracker == nil {
		t.Fatal("expected non-nil tracker from CreateTracker")
	}

	// ProcessElement when tracker claim fails
	tracker.MarkDone()
	count := 0
	err = fn.ProcessElement(context.Background(), tracker, ports.PartitionSlice{SliceIndex: 0}, func(r *domain.GenericRecord) { count++ })
	if err != nil {
		t.Errorf("unexpected error when claim fails: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records when claim fails, got %d", count)
	}
}

type errPartitionedReader struct{}

func (errPartitionedReader) CalculatePartitions(_ context.Context, _ any) ([]ports.PartitionSlice, error) {
	return nil, nil
}
func (errPartitionedReader) ReadPartition(_ context.Context, _ any, _ ports.PartitionSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	recCh := make(chan *domain.GenericRecord)
	errCh := make(chan error, 1)
	errCh <- errors.New("read failure")
	close(recCh)
	close(errCh)
	return recCh, errCh, nil
}
func (errPartitionedReader) Close() error { return nil }

func TestPartitionQuerySourceSDF_ProcessElement_ChannelError(t *testing.T) {
	fn := NewPartitionQuerySourceSDF(&errPartitionedReader{}, nil, nil)
	tracker := NewPartitionRangeTracker(PartitionRange{Start: 0, End: 1})

	err := fn.ProcessElement(context.Background(), tracker, ports.PartitionSlice{SliceIndex: 0}, func(r *domain.GenericRecord) {})
	if err == nil {
		t.Error("expected error from channel error, got nil")
	}
}
