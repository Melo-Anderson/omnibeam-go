package domain_test

import (
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestBuildOutputURI(t *testing.T) {
	t.Run("Explicit File Path", func(t *testing.T) {
		uri := domain.BuildOutputURI("/path/to/orders.parquet", "parquet", "none", "none", false)
		if uri != "/path/to/orders.parquet" {
			t.Errorf("expected explicit file path, got %q", uri)
		}
	})

	t.Run("Directory with Single File", func(t *testing.T) {
		uri := domain.BuildOutputURI("/path/to/export", "csv", "gzip", "none", true)
		if uri != "/path/to/export/output.csv.gz" {
			t.Errorf("expected single file output, got %q", uri)
		}
	})

	t.Run("Directory with Shards", func(t *testing.T) {
		uri := domain.BuildOutputURI("/path/to/export", "jsonl", "none", "none", false)
		if !strings.HasPrefix(uri, "/path/to/export/shard-") || !strings.HasSuffix(uri, ".jsonl") {
			t.Errorf("expected shard file pattern, got %q", uri)
		}
	})
}

func TestBuildOutputURI_ComposedExtensions(t *testing.T) {
	tests := []struct {
		name        string
		format      string
		compression string
		encryption  string
		wantSuffix  string
	}{
		{"csv only", "csv", "none", "none", ".csv"},
		{"csv.gz", "csv", "gzip", "none", ".csv.gz"},
		{"csv.zst", "csv", "zstd", "none", ".csv.zst"},
		{"csv.snappy", "csv", "snappy", "none", ".csv.snappy"},
		{"csv.gz.pgp", "csv", "gzip", "pgp", ".csv.gz.pgp"},
		{"parquet.enc (kms)", "parquet", "none", "kms", ".parquet.enc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uri := domain.BuildOutputURI("/out", tc.format, tc.compression, tc.encryption, true)
			if !strings.HasSuffix(uri, tc.wantSuffix) {
				t.Errorf("expected suffix %q, got URI: %q", tc.wantSuffix, uri)
			}
		})
	}
}
