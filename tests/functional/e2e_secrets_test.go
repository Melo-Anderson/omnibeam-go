//go:build integration
// +build integration

package functional_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

type secretsMetricsOutput struct {
	PipelineID       string `json:"pipeline_id"`
	TotalRecordsRead int64  `json:"total_records_read"`
	RowsWritten      int64  `json:"rows_written"`
	DeadLetterCount  int64  `json:"dead_letter_count"`
}

func seedOpenBao(baseURL, token string) error {
	client := &http.Client{Timeout: 5 * time.Second}

	// 1. Seed KV v2 secret for Postgres
	pgBody, _ := json.Marshal(map[string]any{
		"data": map[string]any{
			"password": "testpassword",
			"user":     "testuser",
			"host":     "postgres",
			"port":     5432,
			"database": "testdb",
		},
	})
	req, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/secret/data/postgres", baseURL), bytes.NewReader(pgBody))
	req.Header.Set("X-Vault-Token", token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed seeding postgres kv v2 (request): %w", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed seeding postgres kv v2: HTTP %d", resp.StatusCode)
	}

	// 2. Enable KV v1 mount at kv1/ (ignore error if already mounted)
	mountBody, _ := json.Marshal(map[string]any{
		"type": "kv",
		"options": map[string]string{
			"version": "1",
		},
	})
	mountReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/sys/mounts/kv1", baseURL), bytes.NewReader(mountBody))
	mountReq.Header.Set("X-Vault-Token", token)
	mountReq.Header.Set("Content-Type", "application/json")
	mountResp, _ := client.Do(mountReq)
	if mountResp != nil {
		mountResp.Body.Close()
	}

	// 3. Seed KV v1 secret for MySQL
	myBody, _ := json.Marshal(map[string]any{
		"password": "testpassword",
		"user":     "testuser",
	})
	myReq, _ := http.NewRequest(http.MethodPost, fmt.Sprintf("%s/v1/kv1/mysql", baseURL), bytes.NewReader(myBody))
	myReq.Header.Set("X-Vault-Token", token)
	myReq.Header.Set("Content-Type", "application/json")
	myResp, err := client.Do(myReq)
	if err != nil {
		return fmt.Errorf("failed seeding mysql kv v1 (request): %w", err)
	}
	myResp.Body.Close()
	if myResp.StatusCode != http.StatusOK && myResp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("failed seeding mysql kv v1: HTTP %d", myResp.StatusCode)
	}

	return nil
}

func TestE2E_Secrets_AllScenarios(t *testing.T) {
	const (
		openbaoURL   = "http://127.0.0.1:8200"
		openbaoToken = "testtoken"
	)

	conn, err := net.DialTimeout("tcp", "127.0.0.1:8200", 500*time.Millisecond)
	if err != nil {
		t.Skip("skipping Secrets test: openbao container not running on 127.0.0.1:8200")
	}
	_ = conn.Close()

	// Seed OpenBao secrets before running scenarios
	if err := seedOpenBao(openbaoURL, openbaoToken); err != nil {
		t.Fatalf("failed seeding openbao secrets: %v", err)
	}
	binPath := testPipelineBin

	scenarios := []struct {
		name             string
		configRelPath    string
		outputParquetRel string
		expectedRead     int64
		expectedWritten  int64
		expectedDLQ      int64
	}{
		{
			name:             "E2E-SEC-01: OpenBao KV v2 Path Normalization and #password Selector",
			configRelPath:    "testdata/configs/manifest_sec_openbao_kv2.json",
			outputParquetRel: "testdata/output/e2e_sec_01/orders_int.parquet",
			expectedRead:     10000,
			expectedWritten:  10000,
			expectedDLQ:      0,
		},
		{
			name:             "E2E-SEC-02: OpenBao KV v2 Explicit vault: Prefix and Preserved /data/",
			configRelPath:    "testdata/configs/manifest_sec_openbao_full_prefix.json",
			outputParquetRel: "testdata/output/e2e_sec_02/orders_int.parquet",
			expectedRead:     10000,
			expectedWritten:  10000,
			expectedDLQ:      0,
		},
		{
			name:             "E2E-SEC-03: OpenBao KV v1 404 Fallback to Direct Path",
			configRelPath:    "testdata/configs/manifest_sec_openbao_kv1.json",
			outputParquetRel: "testdata/output/e2e_sec_03/products.parquet",
			expectedRead:     10000,
			expectedWritten:  10000,
			expectedDLQ:      0,
		},
		{
			name:             "E2E-SEC-05: Plaintext Literal Password Passthrough",
			configRelPath:    "testdata/configs/manifest_sec_literal.json",
			outputParquetRel: "testdata/output/e2e_sec_05/orders_int.parquet",
			expectedRead:     10000,
			expectedWritten:  10000,
			expectedDLQ:      0,
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

			var m secretsMetricsOutput
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

			// Strict Conservation Invariant
			if m.TotalRecordsRead != (m.RowsWritten + m.DeadLetterCount) {
				t.Errorf("Strict Conservation Invariant VIOLATED: %d != %d + %d",
					m.TotalRecordsRead, m.RowsWritten, m.DeadLetterCount)
			}
		})
	}
}
