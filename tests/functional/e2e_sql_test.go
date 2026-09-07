//go:build integration
// +build integration

package functional_test

import (
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

// sqlMetricsOutput mirrors the JSON shape of metrics.json written by MetadataSink.
// Kept separate from the Phase 1 type in e2e_test.go to avoid symbol conflicts.
type sqlMetricsOutput struct {
	PipelineID       string `json:"pipeline_id"`
	TotalRecordsRead int64  `json:"total_records_read"`
	RowsWritten      int64  `json:"rows_written"`
	DeadLetterCount  int64  `json:"dead_letter_count"`
}

func TestE2E_SQL_AllScenarios(t *testing.T) {
	conn, err := net.DialTimeout("tcp", "127.0.0.1:5432", 500*time.Millisecond)
	if err != nil {
		t.Skip("skipping SQL test: postgres container not running on 127.0.0.1:5432")
	}
	_ = conn.Close()
	binPath := testPipelineBin

	scenarios := []struct {
		name             string
		configRelPath    string
		outputParquetRel string
		outputDLQRel     string
		expectedRead     int64
		expectedWritten  int64
		expectedDLQ      int64
	}{
		{
			name:             "E2E-SQL-01: PostgreSQL Integer PK Parallel Range Slicing",
			configRelPath:    "testdata/configs/manifest_pg_int_pk.json",
			outputParquetRel: "testdata/output/e2e-sql-01/orders.parquet",
			expectedRead:     10000,
			expectedWritten:  10000,
			expectedDLQ:      0,
		},
		{
			name:             "E2E-SQL-02: PostgreSQL UUID PK NTILE Parallel Range Slicing",
			configRelPath:    "testdata/configs/manifest_pg_uuid_pk.json",
			outputParquetRel: "testdata/output/e2e-sql-02/users.parquet",
			expectedRead:     10000,
			expectedWritten:  10000,
			expectedDLQ:      0,
		},
		{
			name:             "E2E-SQL-03: MySQL Mixed Types Ingestion",
			configRelPath:    "testdata/configs/manifest_mysql_types.json",
			outputParquetRel: "testdata/output/e2e-sql-03/products.parquet",
			expectedRead:     10000,
			expectedWritten:  10000,
			expectedDLQ:      0,
		},
		{
			name:             "E2E-SQL-04: PostgreSQL Corrupted Rows DLQ Quarantine",
			configRelPath:    "testdata/configs/manifest_sql_dlq.json",
			outputParquetRel: "testdata/output/e2e-sql-04/payments.parquet",
			outputDLQRel:     "testdata/output/e2e-sql-04/dlq.jsonl",
			expectedRead:     1050,
			expectedWritten:  1000,
			expectedDLQ:      50,
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			cfgPath := filepath.Join(repoRoot, sc.configRelPath)
			outDir := filepath.Dir(filepath.Join(repoRoot, sc.outputParquetRel))
			_ = os.RemoveAll(outDir)

			cmd := exec.Command(binPath, "--config", cfgPath)
			cmd.Dir = repoRoot
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("pipeline execution failed: %v\nOutput: %s", err, string(out))
			}

			// Verify Parquet row count
			parquetPath := filepath.Join(repoRoot, sc.outputParquetRel)
			f, err := os.Open(parquetPath)
			if err != nil {
				t.Fatalf("failed to open output parquet: %v", err)
			}
			fi, err := f.Stat()
			if err != nil {
				t.Fatalf("failed to stat parquet file: %v", err)
			}
			pf, err := parquet.OpenFile(f, fi.Size())
			if err != nil {
				t.Fatalf("failed reading parquet file: %v", err)
			}
			f.Close()

			if pf.NumRows() != sc.expectedWritten {
				t.Errorf("expected %d rows in parquet, got %d", sc.expectedWritten, pf.NumRows())
			}

			// Verify metrics.json
			metricsPath := filepath.Join(outDir, "metrics.json")
			mData, err := os.ReadFile(metricsPath)
			if err != nil {
				t.Fatalf("failed reading metrics.json: %v", err)
			}

			var m sqlMetricsOutput
			if err := json.Unmarshal(mData, &m); err != nil {
				t.Fatalf("failed unmarshaling metrics.json: %v", err)
			}

			if m.TotalRecordsRead != sc.expectedRead {
				t.Errorf("metrics total_records_read mismatch: expected %d, got %d", sc.expectedRead, m.TotalRecordsRead)
			}
			if m.RowsWritten != sc.expectedWritten {
				t.Errorf("metrics rows_written mismatch: expected %d, got %d", sc.expectedWritten, m.RowsWritten)
			}
			if m.DeadLetterCount != sc.expectedDLQ {
				t.Errorf("metrics dead_letter_count mismatch: expected %d, got %d", sc.expectedDLQ, m.DeadLetterCount)
			}

			// Verify strict conservation invariant
			if m.TotalRecordsRead != (m.RowsWritten + m.DeadLetterCount) {
				t.Errorf("Strict Conservation Invariant VIOLATED: %d != %d + %d",
					m.TotalRecordsRead, m.RowsWritten, m.DeadLetterCount)
			}
		})
	}
}
