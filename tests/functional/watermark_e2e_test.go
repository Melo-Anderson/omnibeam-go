package functional_test

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

type watermarkMetricsOutput struct {
	PipelineID       string                  `json:"pipeline_id"`
	TotalRecordsRead int64                   `json:"total_records_read"`
	RowsWritten      int64                   `json:"rows_written"`
	DeadLetterCount  int64                   `json:"dead_letter_count"`
	Watermark        *domain.WatermarkOutput `json:"watermark"`
}

func TestE2E_SQLIncrementalWatermark(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:5432", 500*time.Millisecond)
	if err != nil {
		t.Skip("skipping SQL test: postgres container not running on 127.0.0.1:5432")
	}
	_ = conn.Close()

	tmpDir := t.TempDir()
	outDir1 := filepath.Join(tmpDir, "run1")
	outDir2 := filepath.Join(tmpDir, "run2")

	// Base config pointing to orders_int table (created in migrations for e2e-sql)
	baseSQLCfg := domain.PipelineConfig{
		PipelineID:   "e2e-sql-watermark",
		RunID:        "run-wm-001",
		PipelineType: "sql",
		Runner:       "direct",
		DatabaseSource: &domain.DatabaseSourceConfig{
			Driver:        "postgres",
			ConnectionURI: "postgres://testuser:testpassword@localhost:5432/testdb?sslmode=disable",
			Query:         "SELECT id, customer_id, amount, status, created_at FROM orders_int",
			PartitionConfig: domain.PartitionConfig{
				PartitionColumn: "id",
				BatchSize:       2500,
			},
			Watermark: &domain.WatermarkConfig{
				Column: "id",
				Type:   domain.WatermarkTypeInteger,
				Value:  nil, // first run: null
			},
			Schema: domain.Schema{
				Fields: []domain.Field{
					{Name: "id", Type: domain.TypeInt64, Nullable: false},
					{Name: "customer_id", Type: domain.TypeString, Nullable: false},
					{Name: "amount", Type: domain.TypeFloat64, Nullable: false},
					{Name: "status", Type: domain.TypeString, Nullable: false},
					{Name: "created_at", Type: domain.TypeTimestamp, Nullable: false},
				},
			},
		},
		Destination: domain.DestinationConfig{
			Type:         "storage",
			OutputFormat: "parquet",
			OutputPath:   filepath.Join(outDir1, "orders.parquet"),
		},
		DLQConfig: domain.DLQConfig{
			Enabled:        true,
			QuarantinePath: filepath.Join(outDir1, "dlq.jsonl"),
		},
	}

	// Run 1: First ingestion with null watermark
	cfg1Path := filepath.Join(tmpDir, "config_run1.json")
	cfg1Bytes, _ := json.Marshal(baseSQLCfg)
	if err := os.WriteFile(cfg1Path, cfg1Bytes, 0644); err != nil {
		t.Fatalf("failed writing config1: %v", err)
	}

	cmd1 := exec.Command(testPipelineBin, "--config", cfg1Path)
	cmd1.Dir = repoRoot
	out1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("run1 pipeline execution failed: %v\nOutput: %s", err, string(out1))
	}

	metrics1Path := filepath.Join(outDir1, "metrics.json")
	mData1, err := os.ReadFile(metrics1Path)
	if err != nil {
		t.Fatalf("failed reading metrics1: %v", err)
	}
	var m1 watermarkMetricsOutput
	if err := json.Unmarshal(mData1, &m1); err != nil {
		t.Fatalf("failed unmarshaling metrics1: %v", err)
	}

	if m1.Watermark == nil || m1.Watermark.Value == nil {
		t.Fatalf("expected watermark output in run 1, got %+v", m1.Watermark)
	}
	newWatermark := *m1.Watermark.Value
	t.Logf("Run 1 emitted watermark: %s (rows written: %d)", newWatermark, m1.RowsWritten)

	// Run 2: Second run using the new watermark
	cfg2 := baseSQLCfg
	cfg2.RunID = "run-wm-002"
	cfg2.DatabaseSource.Watermark.Value = &newWatermark
	cfg2.Destination.OutputPath = filepath.Join(outDir2, "orders.parquet")
	cfg2.DLQConfig.QuarantinePath = filepath.Join(outDir2, "dlq.jsonl")

	cfg2Path := filepath.Join(tmpDir, "config_run2.json")
	cfg2Bytes, _ := json.Marshal(cfg2)
	if err := os.WriteFile(cfg2Path, cfg2Bytes, 0644); err != nil {
		t.Fatalf("failed writing config2: %v", err)
	}

	cmd2 := exec.Command(testPipelineBin, "--config", cfg2Path)
	cmd2.Dir = repoRoot
	out2, err := cmd2.CombinedOutput()
	if err != nil {
		t.Fatalf("run2 pipeline execution failed: %v\nOutput: %s", err, string(out2))
	}

	metrics2Path := filepath.Join(outDir2, "metrics.json")
	mData2, err := os.ReadFile(metrics2Path)
	if err != nil {
		t.Fatalf("failed reading metrics2: %v", err)
	}
	var m2 watermarkMetricsOutput
	if err := json.Unmarshal(mData2, &m2); err != nil {
		t.Fatalf("failed unmarshaling metrics2: %v", err)
	}

	if m2.RowsWritten != 0 {
		t.Errorf("expected 0 rows written on incremental run 2, got %d", m2.RowsWritten)
	}
	if m2.Watermark == nil || m2.Watermark.Value == nil || *m2.Watermark.Value != newWatermark {
		t.Errorf("expected preserved watermark %s, got %+v", newWatermark, m2.Watermark)
	}
}

func TestE2E_REST_WatermarkPagination(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		offsetStr := r.URL.Query().Get("offset")
		offset, _ := strconv.Atoi(offsetStr)

		switch offset {
		case 0:
			w.Write([]byte(`{
				"total": 4,
				"items": [
					{"id": 1, "name": "item-1"},
					{"id": 2, "name": "item-2"}
				]
			}`))
		case 2:
			w.Write([]byte(`{
				"total": 4,
				"items": [
					{"id": 3, "name": "item-3"},
					{"id": 4, "name": "item-4"}
				]
			}`))
		default:
			w.Write([]byte(`{"total": 4, "items": []}`))
		}
	}))
	defer server.Close()

	tmpDir := t.TempDir()
	outDir1 := filepath.Join(tmpDir, "run1")
	outDir2 := filepath.Join(tmpDir, "run2")

	initialOffset := "0"
	apiCfg1 := domain.PipelineConfig{
		PipelineID:   "e2e-rest-watermark",
		RunID:        "run-api-001",
		PipelineType: "rest_api",
		Runner:       "direct",
		APISource: &domain.APISourceConfig{
			BaseURL:     server.URL,
			Endpoint:    "/items",
			RecordsPath: "items",
			Pagination: domain.APIPaginationConfig{
				Type:           domain.PaginationOffsetLimit,
				PageParam:      "offset",
				SizeParam:      "limit",
				PageSize:       2,
				TotalCountPath: "total",
			},
			Watermark: &domain.WatermarkConfig{
				Column: "offset",
				Type:   domain.WatermarkTypeInteger,
				Value:  &initialOffset,
			},
			Schema: domain.Schema{
				Fields: []domain.Field{
					{Name: "id", Type: domain.TypeInt64, Nullable: false},
					{Name: "name", Type: domain.TypeString, Nullable: false},
				},
			},
		},
		Destination: domain.DestinationConfig{
			Type:         "storage",
			OutputFormat: "parquet",
			OutputPath:   filepath.Join(outDir1, "items.parquet"),
		},
		DLQConfig: domain.DLQConfig{
			Enabled:        true,
			QuarantinePath: filepath.Join(outDir1, "dlq.jsonl"),
		},
	}

	cfg1Path := filepath.Join(tmpDir, "manifest_run1.json")
	b1, _ := json.Marshal(apiCfg1)
	if err := os.WriteFile(cfg1Path, b1, 0644); err != nil {
		t.Fatalf("failed writing manifest1: %v", err)
	}

	cmd1 := exec.Command(testPipelineBin, "--config", cfg1Path)
	cmd1.Dir = repoRoot
	out1, err := cmd1.CombinedOutput()
	if err != nil {
		t.Fatalf("api run1 failed: %v\nOutput: %s", err, string(out1))
	}

	// Verify metrics for run 1
	m1Bytes, err := os.ReadFile(filepath.Join(outDir1, "metrics.json"))
	if err != nil {
		t.Fatalf("failed reading metrics 1: %v", err)
	}
	var m1 watermarkMetricsOutput
	if err := json.Unmarshal(m1Bytes, &m1); err != nil {
		t.Fatalf("failed unmarshaling metrics 1: %v", err)
	}

	if m1.RowsWritten != 4 {
		t.Fatalf("expected 4 rows written in run 1, got %d", m1.RowsWritten)
	}
	if m1.Watermark == nil || m1.Watermark.Value == nil || *m1.Watermark.Value != "4" {
		t.Fatalf("expected watermark output 4, got %+v", m1.Watermark)
	}

	// Run 2: starts from offset 4
	offset4 := "4"
	apiCfg2 := apiCfg1
	apiCfg2.RunID = "run-api-002"
	apiCfg2.APISource.Watermark.Value = &offset4
	apiCfg2.Destination.OutputPath = filepath.Join(outDir2, "items.parquet")
	apiCfg2.DLQConfig.QuarantinePath = filepath.Join(outDir2, "dlq.jsonl")

	cfg2Path := filepath.Join(tmpDir, "manifest_run2.json")
	b2, _ := json.Marshal(apiCfg2)
	if err := os.WriteFile(cfg2Path, b2, 0644); err != nil {
		t.Fatalf("failed writing manifest2: %v", err)
	}

	cmd2 := exec.Command(testPipelineBin, "--config", cfg2Path)
	cmd2.Dir = repoRoot
	out2, err := cmd2.CombinedOutput()
	if err != nil {
		t.Fatalf("api run2 failed: %v\nOutput: %s", err, string(out2))
	}

	m2Bytes, err := os.ReadFile(filepath.Join(outDir2, "metrics.json"))
	if err != nil {
		t.Fatalf("failed reading metrics 2: %v", err)
	}
	var m2 watermarkMetricsOutput
	if err := json.Unmarshal(m2Bytes, &m2); err != nil {
		t.Fatalf("failed unmarshaling metrics 2: %v", err)
	}

	if m2.RowsWritten != 0 {
		t.Fatalf("expected 0 rows written in run 2, got %d", m2.RowsWritten)
	}
	if m2.Watermark == nil || m2.Watermark.Value == nil || *m2.Watermark.Value != "4" {
		t.Fatalf("expected preserved watermark 4 in run 2, got %+v", m2.Watermark)
	}
}

// Ensure parquet import is used for verification if needed
var _ = parquet.OpenFile
