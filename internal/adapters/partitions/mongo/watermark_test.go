package mongo_test

import (
	"context"
	"strings"
	"testing"

	_ "github.com/omnibeam/dataflow-compute-go/internal/adapters/partitions/mongo"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func TestMongoFactory_RejectsWatermark(t *testing.T) {
	cfg := &domain.PipelineConfig{
		DatabaseSource: &domain.DatabaseSourceConfig{
			Driver:   "mongo",
			Database: "test_db",
			Watermark: &domain.WatermarkConfig{
				Column: "id",
				Type:   domain.WatermarkTypeInteger,
			},
		},
	}

	_, err := ports.BuildSource(context.Background(), "mongo", cfg, nil)
	if err == nil || !strings.Contains(err.Error(), "watermark is not supported for mongodb source") {
		t.Fatalf("expected error mentioning mongodb watermark unsupported, got: %v", err)
	}
}
