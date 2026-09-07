package ports_test

import (
	"context"
	"errors"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestSinkRegistry_RegisterAndBuild(t *testing.T) {
	t.Run("registered sink is resolved by BuildSink", func(t *testing.T) {
		testType := "test-sink-" + t.Name()
		ports.RegisterSink(testType, func(_ context.Context, _ *domain.PipelineConfig, _ ports.SecretResolver, _ ports.StorageBackend) (ports.BeamSinkBuilder, error) {
			return nil, nil
		})
		_, err := ports.BuildSink(context.Background(), testType, &domain.PipelineConfig{}, nil, nil)
		if err != nil {
			t.Errorf("BuildSink returned unexpected error: %v", err)
		}
	})

	t.Run("unregistered sink returns ErrUnregisteredConnector", func(t *testing.T) {
		_, err := ports.BuildSink(context.Background(), "no-such-sink-xyz", &domain.PipelineConfig{}, nil, nil)
		if err == nil {
			t.Fatal("expected error for unregistered sink, got nil")
		}
		if !errors.Is(err, ports.ErrUnregisteredConnector) {
			t.Errorf("expected ErrUnregisteredConnector, got: %v", err)
		}
	})
}

func TestSourceRegistry_DuplicatePanics(t *testing.T) {
	testDriver := "test-source-dup-" + t.Name()
	ports.RegisterSource(testDriver, func(_ context.Context, _ *domain.PipelineConfig, _ ports.SecretResolver) (ports.PartitionedReader, error) {
		return nil, nil
	})
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on duplicate registration, got none")
		}
	}()
	ports.RegisterSource(testDriver, func(_ context.Context, _ *domain.PipelineConfig, _ ports.SecretResolver) (ports.PartitionedReader, error) {
		return nil, nil
	})
}

func TestSourceRegistry_RegisterAndBuild(t *testing.T) {
	t.Run("registered source is resolved by BuildSource", func(t *testing.T) {
		testDriver := "test-source-" + t.Name()
		ports.RegisterSource(testDriver, func(_ context.Context, _ *domain.PipelineConfig, _ ports.SecretResolver) (ports.PartitionedReader, error) {
			return nil, nil
		})
		_, err := ports.BuildSource(context.Background(), testDriver, &domain.PipelineConfig{}, nil)
		if err != nil {
			t.Errorf("BuildSource returned unexpected error: %v", err)
		}
	})

	t.Run("unregistered source returns ErrUnregisteredConnector", func(t *testing.T) {
		_, err := ports.BuildSource(context.Background(), "no-such-driver-xyz", &domain.PipelineConfig{}, nil)
		if err == nil {
			t.Fatal("expected error for unregistered source, got nil")
		}
		if !errors.Is(err, ports.ErrUnregisteredConnector) {
			t.Errorf("expected ErrUnregisteredConnector, got: %v", err)
		}
	})
}

func TestPagedAPISourceRegistry_RegisterAndBuild(t *testing.T) {
	testType := "test-paged-" + t.Name()
	ports.RegisterPagedAPISource(testType, func(_ context.Context, _ *domain.PipelineConfig, _ ports.SecretResolver) (ports.PagedAPIReader, error) {
		return nil, nil
	})

	_, err := ports.BuildPagedAPISource(context.Background(), testType, &domain.PipelineConfig{}, nil)
	if err != nil {
		t.Errorf("BuildPagedAPISource returned unexpected error: %v", err)
	}

	_, err = ports.BuildPagedAPISource(context.Background(), "unregistered-paged", &domain.PipelineConfig{}, nil)
	if err == nil || !errors.Is(err, ports.ErrUnregisteredConnector) {
		t.Errorf("expected ErrUnregisteredConnector, got %v", err)
	}
}

func TestBatchAPIWriterRegistry_RegisterAndBuild(t *testing.T) {
	testType := "test-batch-writer-" + t.Name()
	ports.RegisterBatchAPIWriter(testType, func(_ context.Context, _ *domain.PipelineConfig, _ ports.SecretResolver) (ports.BatchAPIWriter, error) {
		return nil, nil
	})

	_, err := ports.BuildBatchAPIWriter(context.Background(), testType, &domain.PipelineConfig{}, nil)
	if err != nil {
		t.Errorf("BuildBatchAPIWriter returned unexpected error: %v", err)
	}

	_, err = ports.BuildBatchAPIWriter(context.Background(), "unregistered-writer", &domain.PipelineConfig{}, nil)
	if err == nil || !errors.Is(err, ports.ErrUnregisteredConnector) {
		t.Errorf("expected ErrUnregisteredConnector, got %v", err)
	}
}

func TestDuplicateRegistrations_Panic(t *testing.T) {
	t.Run("Duplicate Sink panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic on duplicate sink registration")
			}
		}()
		ports.RegisterSink("dup-sink", nil)
		ports.RegisterSink("dup-sink", nil)
	})

	t.Run("Duplicate PagedAPISource panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic on duplicate paged api source registration")
			}
		}()
		ports.RegisterPagedAPISource("dup-paged", nil)
		ports.RegisterPagedAPISource("dup-paged", nil)
	})

	t.Run("Duplicate BatchAPIWriter panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic on duplicate batch api writer registration")
			}
		}()
		ports.RegisterBatchAPIWriter("dup-writer", nil)
		ports.RegisterBatchAPIWriter("dup-writer", nil)
	})
}
