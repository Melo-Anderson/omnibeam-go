package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

func createTestParquetFile(t *testing.T, dir, filename string, ids []int64, timestamps []time.Time) string {
	t.Helper()
	type TestRow struct {
		ID        int64  `parquet:"id"`
		UpdatedAt int64  `parquet:"updated_at,timestamp(microsecond)"`
		Name      string `parquet:"name"`
	}

	rows := make([]TestRow, len(ids))
	for i := range ids {
		var ts int64
		if i < len(timestamps) {
			ts = timestamps[i].UnixMicro()
		}
		rows[i] = TestRow{
			ID:        ids[i],
			UpdatedAt: ts,
			Name:      "item",
		}
	}

	filePath := filepath.Join(dir, filename)
	f, err := os.Create(filePath)
	if err != nil {
		t.Fatalf("failed creating file: %v", err)
	}
	defer f.Close()

	w := parquet.NewGenericWriter[TestRow](f)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("failed writing rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed closing writer: %v", err)
	}
	return filePath
}

func TestMaxParquetColumn_IntegerNumericOrderAcrossFiles(t *testing.T) {
	tempDir := t.TempDir()
	f1 := createTestParquetFile(t, tempDir, "f1.parquet", []int64{1, 9, 5}, nil)
	f2 := createTestParquetFile(t, tempDir, "f2.parquet", []int64{2, 10, 8}, nil)

	maxVal, err := maxParquetColumn([]string{f1, f2}, "id", domain.WatermarkTypeInteger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if maxVal == nil || maxVal.String() != "10" {
		t.Fatalf("expected max 10 (numeric, not 9 lexicographical), got %+v", maxVal)
	}
}

func TestMaxParquetColumn_TimestampMicrosToCanonical(t *testing.T) {
	tempDir := t.TempDir()
	t1 := time.Date(2026, 10, 24, 8, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 10, 24, 10, 14, 22, 0, time.UTC)
	f1 := createTestParquetFile(t, tempDir, "f1.parquet", []int64{1, 2}, []time.Time{t1, t2})

	maxVal, err := maxParquetColumn([]string{f1}, "updated_at", domain.WatermarkTypeTimestamp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if maxVal == nil || maxVal.String() != "2026-10-24T10:14:22+00:00" {
		t.Fatalf("expected canonical timestamp 2026-10-24T10:14:22+00:00, got %+v", maxVal)
	}
}

func TestMaxParquetColumn_MissingColumnFails(t *testing.T) {
	tempDir := t.TempDir()
	f1 := createTestParquetFile(t, tempDir, "f1.parquet", []int64{1}, nil)

	_, err := maxParquetColumn([]string{f1}, "non_existent_column", domain.WatermarkTypeInteger)
	if err == nil {
		t.Fatalf("expected error for missing column, got nil")
	}
}

func TestResolveWatermarkOutput_ZeroRowsPreservesIncoming(t *testing.T) {
	incoming := "2026-10-24T00:00:00+00:00"
	cfg := &domain.PipelineConfig{
		DatabaseSource: &domain.DatabaseSourceConfig{
			Watermark: &domain.WatermarkConfig{
				Column: "updated_at",
				Type:   domain.WatermarkTypeTimestamp,
				Value:  &incoming,
			},
		},
	}

	out, err := resolveWatermarkOutput(cfg, t.TempDir(), 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil || out.Value == nil || *out.Value != incoming {
		t.Fatalf("expected preserved incoming value %q, got %+v", incoming, out)
	}
}

func TestResolveWatermarkOutput_ZeroRowsFirstRunEmitsNullValue(t *testing.T) {
	cfg := &domain.PipelineConfig{
		DatabaseSource: &domain.DatabaseSourceConfig{
			Watermark: &domain.WatermarkConfig{
				Column: "updated_at",
				Type:   domain.WatermarkTypeTimestamp,
				Value:  nil,
			},
		},
	}

	out, err := resolveWatermarkOutput(cfg, t.TempDir(), 0, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil || out.Value != nil {
		t.Fatalf("expected nil value for zero-rows first run, got %+v", out)
	}
}

func TestResolveWatermarkOutput_NeverRegressesBelowIncoming(t *testing.T) {
	tempDir := t.TempDir()
	// Parquet file has max 100
	createTestParquetFile(t, tempDir, "f1.parquet", []int64{50, 100}, nil)

	// Incoming watermark is already 200
	incoming := "200"
	cfg := &domain.PipelineConfig{
		DatabaseSource: &domain.DatabaseSourceConfig{
			Watermark: &domain.WatermarkConfig{
				Column: "id",
				Type:   domain.WatermarkTypeInteger,
				Value:  &incoming,
			},
		},
	}

	out, err := resolveWatermarkOutput(cfg, tempDir, 2, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == nil || out.Value == nil || *out.Value != "200" {
		t.Fatalf("expected watermark not to regress below incoming 200, got %+v", out)
	}
}

func TestResolveWatermarkOutput_OffsetAdvancesByTotalReadIncludingDLQ(t *testing.T) {
	incoming := "1500"
	cfg := &domain.PipelineConfig{
		APISource: &domain.APISourceConfig{
			Pagination: domain.APIPaginationConfig{
				Type:      domain.PaginationOffsetLimit,
				PageParam: "offset",
			},
			Watermark: &domain.WatermarkConfig{
				Column: "offset",
				Type:   domain.WatermarkTypeInteger,
				Value:  &incoming,
			},
		},
	}

	// 200 valid + 50 DLQ = 250 total read
	out, err := resolveWatermarkOutput(cfg, t.TempDir(), 250, 200)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// 1500 + 250 = 1750
	if out == nil || out.Value == nil || *out.Value != "1750" {
		t.Fatalf("expected offset to advance to 1750, got %+v", out)
	}
}

func TestResolveWatermarkOutput_NoWatermarkConfigReturnsNil(t *testing.T) {
	cfg := &domain.PipelineConfig{}
	out, err := resolveWatermarkOutput(cfg, t.TempDir(), 10, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != nil {
		t.Fatalf("expected nil output for config without watermark, got %+v", out)
	}
}
