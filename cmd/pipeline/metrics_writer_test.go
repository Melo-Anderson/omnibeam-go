package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestWriteExecutionMetrics_LocalFile(t *testing.T) {
	tempDir := t.TempDir()
	outputPath := filepath.Join(tempDir, "output.csv")
	if err := os.WriteFile(outputPath, []byte("id,name\n1,Alice\n2,Bob\n"), 0644); err != nil {
		t.Fatalf("failed writing test file: %v", err)
	}

	cfg := &domain.PipelineConfig{
		PipelineID: "test-pipeline",
		RunID:      "run-001",
		Destination: domain.DestinationConfig{
			OutputPath:   outputPath,
			OutputFormat: "csv",
			FormatOptions: domain.FormatOptions{
				IncludeHeader: true,
			},
		},
	}

	writeExecutionMetrics(cfg)

	metricsFile := filepath.Join(tempDir, "metrics.json")
	if _, err := os.Stat(metricsFile); os.IsNotExist(err) {
		t.Fatalf("expected metrics.json to be created")
	}
}
