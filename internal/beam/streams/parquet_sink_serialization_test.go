package streams_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/streams"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestParquetSinkDoFn_SerializationSanity(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
			{Name: "amount", Type: domain.TypeFloat64},
		},
	}

	sink := streams.NewParquetSinkDoFn(nil, "/tmp/parquet_test", "snappy", schema)

	// Simulate serialization across Beam distributed worker nodes
	data, err := json.Marshal(sink)
	if err != nil {
		t.Fatalf("failed serializing ParquetSinkDoFn: %v", err)
	}

	var remoteSink streams.ParquetSinkDoFn
	if err := json.Unmarshal(data, &remoteSink); err != nil {
		t.Fatalf("failed deserializing ParquetSinkDoFn: %v", err)
	}

	if remoteSink.OutputDir != "/tmp/parquet_test" {
		t.Errorf("expected OutputDir=/tmp/parquet_test, got %s", remoteSink.OutputDir)
	}
	if remoteSink.Compression != "snappy" {
		t.Errorf("expected Compression=snappy, got %s", remoteSink.Compression)
	}

	ctx := context.Background()
	if err := remoteSink.Setup(ctx); err != nil {
		t.Fatalf("remote worker Setup failed: %v", err)
	}
	if err := remoteSink.StartBundle(ctx); err != nil {
		t.Fatalf("remote worker StartBundle failed: %v", err)
	}
}
