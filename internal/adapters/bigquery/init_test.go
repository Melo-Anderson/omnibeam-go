package bigquery

import (
	"context"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// TestBQSinkFactory_DoesNotNeedStorage verifies that the BigQuery factory
// does not fail because Storage is nil (ISP compliance check).
func TestBQSinkFactory_DoesNotNeedStorage(t *testing.T) {
	cfg := &domain.PipelineConfig{
		Destination: domain.DestinationConfig{
			Type: "bigquery",
			BigQueryOptions: domain.BigQuerySinkConfig{
				DatasetID: "ds",
				TableID:   "tbl",
				BatchSize: 500,
			},
		},
		Source: domain.SourceConfig{
			Schema: domain.Schema{
				Fields: []domain.Field{{Name: "id", Type: "int64"}},
			},
		},
	}
	deps := ports.SinkDeps{
		Storage: nil, // explicitly nil — BigQuery must not require it
		Secrets: nil,
	}

	_, err := bqSinkFactory(context.Background(), cfg, deps)
	// We expect no error related to nil Storage; any actual error should
	// come from BigQuery API connectivity, not from missing storage.
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "storage") {
		t.Errorf("BigQuery factory should not require Storage: %v", err)
	}
}
