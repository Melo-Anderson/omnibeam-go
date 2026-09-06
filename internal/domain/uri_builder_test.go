package domain_test

import (
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestBuildOutputURI(t *testing.T) {
	t.Run("Explicit File Path", func(t *testing.T) {
		uri := domain.BuildOutputURI("/path/to/orders.parquet", "parquet", "none", false)
		if uri != "/path/to/orders.parquet" {
			t.Errorf("expected explicit file path, got %q", uri)
		}
	})

	t.Run("Directory with Single File", func(t *testing.T) {
		uri := domain.BuildOutputURI("/path/to/export", "csv", "gzip", true)
		if uri != "/path/to/export/output.csv.gz" {
			t.Errorf("expected single file output, got %q", uri)
		}
	})

	t.Run("Directory with Shards", func(t *testing.T) {
		uri := domain.BuildOutputURI("/path/to/export", "jsonl", "none", false)
		if !strings.HasPrefix(uri, "/path/to/export/shard-") || !strings.HasSuffix(uri, ".jsonl") {
			t.Errorf("expected shard file pattern, got %q", uri)
		}
	})
}
