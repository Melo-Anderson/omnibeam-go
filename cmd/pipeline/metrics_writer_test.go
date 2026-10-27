package main

import (
	"encoding/json"
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

	if err := writeExecutionMetrics(cfg); err != nil {
		t.Fatalf("writeExecutionMetrics failed: %v", err)
	}

	metricsFile := filepath.Join(tempDir, "metrics.json")
	if _, err := os.Stat(metricsFile); os.IsNotExist(err) {
		t.Fatalf("expected metrics.json to be created")
	}
}

func TestWriteExecutionMetrics_WritesWatermarkIntoMetricsJSON(t *testing.T) {
	tempDir := t.TempDir()
	pPath := createTestParquetFile(t, tempDir, "output-0.parquet", []int64{10, 25, 42}, nil)

	incoming := "5"
	cfg := &domain.PipelineConfig{
		PipelineID: "test-pipeline",
		RunID:      "run-002",
		Destination: domain.DestinationConfig{
			OutputPath:   pPath,
			OutputFormat: "parquet",
		},
		DatabaseSource: &domain.DatabaseSourceConfig{
			Watermark: &domain.WatermarkConfig{
				Column: "id",
				Type:   domain.WatermarkTypeInteger,
				Value:  &incoming,
			},
		},
	}

	if err := writeExecutionMetrics(cfg); err != nil {
		t.Fatalf("writeExecutionMetrics failed: %v", err)
	}

	metricsFile := filepath.Join(tempDir, "metrics.json")
	data, err := os.ReadFile(metricsFile)
	if err != nil {
		t.Fatalf("failed reading metrics.json: %v", err)
	}

	var metrics domain.PipelineMetrics
	if err := json.Unmarshal(data, &metrics); err != nil {
		t.Fatalf("failed unmarshaling metrics.json: %v", err)
	}

	if metrics.Watermark == nil {
		t.Fatalf("expected Watermark to be populated in metrics")
	}
	if metrics.Watermark.Column != "id" || metrics.Watermark.Value == nil || *metrics.Watermark.Value != "42" {
		t.Fatalf("expected watermark value 42, got %+v", metrics.Watermark)
	}
}
