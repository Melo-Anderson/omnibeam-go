package main

import (
	"context"
	"io"
	"strings"
	"testing"

	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/paged_api/sink"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/paged_api/source"
	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/sql"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

type dummyStorage struct{}

func (dummyStorage) Open(_ context.Context, _ string) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}
func (dummyStorage) List(_ context.Context, _ string) ([]string, error) {
	return []string{"test.csv"}, nil
}
func (dummyStorage) Size(_ context.Context, _ string) (int64, error) {
	return 100, nil
}
func (dummyStorage) CreateTemp(_ context.Context, _ string) (string, io.WriteCloser, error) {
	return "", nil, nil
}
func (dummyStorage) CommitTemp(_ context.Context, _, _ string) error { return nil }
func (dummyStorage) AbortTemp(_ context.Context, _ string) error     { return nil }

type mockPipelineDBReader struct{}

func (mockPipelineDBReader) CalculatePartitions(_ context.Context, _ any) ([]ports.PartitionSlice, error) {
	return []ports.PartitionSlice{{IsFirst: true, IsLast: true}}, nil
}
func (mockPipelineDBReader) ReadPartition(_ context.Context, _ any, _ ports.PartitionSlice) (<-chan *domain.GenericRecord, <-chan error, error) {
	return nil, nil, nil
}
func (mockPipelineDBReader) Close() error { return nil }

func init() {
	ports.RegisterSource("mock_pipeline_db", func(_ context.Context, _ *domain.PipelineConfig, _ ports.SecretResolver) (ports.PartitionedReader, error) {
		return &mockPipelineDBReader{}, nil
	})
}

func TestBuildSourceEngine_Validation(t *testing.T) {
	ctx := context.Background()
	st := &dummyStorage{}

	t.Run("Byte stream source builds successfully", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Source: domain.SourceConfig{
				Path:   "testdata/input.csv",
				Format: "csv",
				Schema: domain.Schema{Fields: []domain.Field{{Name: "id", Type: "int64"}}},
			},
		}
		builder, err := buildSourceEngine(ctx, cfg, st, nil)
		if err != nil || builder == nil {
			t.Errorf("buildSourceEngine for stream: %v, %v", builder, err)
		}
	})

	t.Run("Byte stream source builds successfully with explicit Paths list", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Source: domain.SourceConfig{
				Paths:  []string{"testdata/input1.csv", "testdata/input2.csv"},
				Format: "csv",
				Schema: domain.Schema{Fields: []domain.Field{{Name: "id", Type: "int64"}}},
			},
		}
		builder, err := buildSourceEngine(ctx, cfg, st, nil)
		if err != nil || builder == nil {
			t.Errorf("buildSourceEngine for stream with paths: %v, %v", builder, err)
		}
	})

	t.Run("Database source builds successfully with registered driver", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			DatabaseSource: &domain.DatabaseSourceConfig{
				Driver: "mock_pipeline_db",
				Table:  "users",
			},
		}
		builder, err := buildSourceEngine(ctx, cfg, st, nil)
		if err != nil || builder == nil {
			t.Errorf("buildSourceEngine for mock db: %v, %v", builder, err)
		}
	})

	t.Run("Database source returns error on unregistered driver", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			DatabaseSource: &domain.DatabaseSourceConfig{
				Driver:        "unknown_driver",
				Table:         "users",
				ConnectionURI: "dummy://uri",
			},
		}
		_, err := buildSourceEngine(ctx, cfg, st, nil)
		if err == nil {
			t.Error("expected error for unregistered database driver")
		}
	})

	t.Run("API source builds successfully", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			APISource: &domain.APISourceConfig{
				BaseURL:  "http://api.example.com",
				Endpoint: "/users",
				Pagination: domain.APIPaginationConfig{
					TotalPagesHint: 1,
				},
			},
		}
		builder, err := buildSourceEngine(ctx, cfg, st, nil)
		if err != nil || builder == nil {
			t.Errorf("buildSourceEngine for api: %v, %v", builder, err)
		}
	})

	t.Run("Unsupported format returns error", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Source: domain.SourceConfig{
				Format: "unsupported_fmt",
			},
		}
		_, err := buildSourceEngine(ctx, cfg, st, nil)
		if err == nil {
			t.Error("expected error for unsupported source format")
		}
	})
}

func TestBuildDestinationSink_Validation(t *testing.T) {
	ctx := context.Background()
	st := &dummyStorage{}
	schema := domain.Schema{Fields: []domain.Field{{Name: "id", Type: "int64"}}}

	t.Run("File destination sink builds successfully", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Destination: domain.DestinationConfig{
				Type:         "storage",
				OutputFormat: "parquet",
				OutputPath:   "/tmp/out.parquet",
			},
		}
		sink, err := buildDestinationSink(ctx, cfg, st, nil, schema)
		if err != nil || sink == nil {
			t.Errorf("buildDestinationSink for file: %v, %v", sink, err)
		}
	})

	t.Run("Delimited destination sink builds successfully", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Destination: domain.DestinationConfig{
				Type:         "storage",
				OutputFormat: "jsonl",
				OutputPath:   "/tmp/out.jsonl",
			},
		}
		sink, err := buildDestinationSink(ctx, cfg, st, nil, schema)
		if err != nil || sink == nil {
			t.Errorf("buildDestinationSink for jsonl: %v, %v", sink, err)
		}
	})

	t.Run("API destination sink builds successfully", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Destination: domain.DestinationConfig{
				Type: "rest_api",
				Endpoint: domain.APIEndpointConfig{
					BaseURL: "http://api.example.com",
				},
				APIOptions: domain.APISinkOptions{
					BatchSize: 10,
				},
			},
		}
		sink, err := buildDestinationSink(ctx, cfg, st, nil, schema)
		if err != nil || sink == nil {
			t.Errorf("buildDestinationSink for api: %v, %v", sink, err)
		}
	})

	t.Run("BigQuery destination resolves correctly", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			Destination: domain.DestinationConfig{
				Type: "bigquery",
				BigQueryOptions: domain.BigQuerySinkConfig{
					ProjectID: "test-project",
					DatasetID: "analytics",
					TableID:   "orders",
					BatchSize: 500,
				},
			},
			Source: domain.SourceConfig{Schema: schema},
		}
		sink, err := buildDestinationSink(ctx, cfg, st, nil, schema)
		if err != nil {
			t.Fatalf("buildDestinationSink(bigquery) error: %v", err)
		}
		if sink == nil {
			t.Fatal("expected non-nil BigQuery sink builder")
		}
	})

	t.Run("Dynamic OCP resolution across all registered sink targets", func(t *testing.T) {
		targets := []string{"parquet", "csv", "jsonl", "txt", "file", "storage", "rest_api", "http_api", "bigquery"}
		for _, target := range targets {
			cfg := &domain.PipelineConfig{
				Destination: domain.DestinationConfig{
					Type:         target,
					OutputPath:   "/tmp/out",
					OutputFormat: target,
					BigQueryOptions: domain.BigQuerySinkConfig{
						DatasetID: "ds",
						TableID:   "tbl",
					},
				},
			}
			sink, err := buildDestinationSink(ctx, cfg, st, nil, schema)
			if err != nil {
				t.Fatalf("failed resolving dynamic sink for target %q: %v", target, err)
			}
			if sink == nil {
				t.Fatalf("expected non-nil sink for target %q", target)
			}
		}
	})
}

func TestBuildDLQAndAuditSinks(t *testing.T) {
	st := &dummyStorage{}

	t.Run("DLQ sink always non-nil", func(t *testing.T) {
		cfg := &domain.PipelineConfig{
			DLQConfig: domain.DLQConfig{QuarantinePath: "/tmp/dlq"},
		}
		if buildDLQSink(cfg, st) == nil {
			t.Error("expected non-nil DLQ sink")
		}
	})

	t.Run("Audit sink when quarantine path is empty vs set", func(t *testing.T) {
		cfgEmpty := &domain.PipelineConfig{
			DLQConfig: domain.DLQConfig{QuarantinePath: ""},
		}
		if buildAuditSink(cfgEmpty, st) != nil {
			t.Error("expected nil audit sink when quarantine path is empty")
		}

		cfgSet := &domain.PipelineConfig{
			DLQConfig: domain.DLQConfig{QuarantinePath: "/tmp/audit"},
		}
		if buildAuditSink(cfgSet, st) == nil {
			t.Error("expected non-nil audit sink when quarantine path is set")
		}
	})
}
