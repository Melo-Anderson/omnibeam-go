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

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

func isPortOpen(hostPort string) bool {
	conn, err := net.DialTimeout("tcp", hostPort, 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func TestE2E_BeamSDF_AllScenarios(t *testing.T) {
	binPath := testPipelineBin

	t.Run("E2E-BEAM-01: CSV File Ingestion via Beam SDF Graph", func(t *testing.T) {
		outputDir := filepath.Join(repoRoot, "testdata", "output", "e2e_beam_01")
		_ = os.RemoveAll(outputDir)
		_ = os.MkdirAll(outputDir, 0755)

		manifestPath := filepath.Join(repoRoot, "testdata", "configs", "manifest_happy_path.json")
		manifestBytes, err := os.ReadFile(manifestPath)
		if err != nil {
			t.Fatalf("failed reading manifest: %v", err)
		}

		var cfg domain.PipelineConfig
		if err := json.Unmarshal(manifestBytes, &cfg); err != nil {
			t.Fatalf("failed unmarshaling manifest: %v", err)
		}
		cfg.Runner = "dataflow"
		cfg.Destination.OutputPath = outputDir
		cfg.DLQConfig.QuarantinePath = filepath.Join(outputDir, "dlq")

		updatedManifest, _ := json.Marshal(cfg)
		testManifestPath := filepath.Join(outputDir, "manifest.json")
		_ = os.WriteFile(testManifestPath, updatedManifest, 0644)

		cmd := exec.Command(binPath,
			"--config="+testManifestPath,
			"--runner=direct",
		)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "GOTMPDIR="+filepath.Join(repoRoot, ".gotmp"))
		cmdOut, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("pipeline run failed: %v\nOutput:\n%s", err, string(cmdOut))
		}

		// Verify shards in output directory
		matches, err := filepath.Glob(filepath.Join(outputDir, "shard-*.parquet"))
		if err != nil || len(matches) == 0 {
			t.Fatalf("no output parquet shards generated in %s: %v", outputDir, err)
		}

		var totalRows int64
		for _, shardPath := range matches {
			f, err := os.Open(shardPath)
			if err != nil {
				t.Fatalf("failed opening shard %s: %v", shardPath, err)
			}
			fi, _ := f.Stat()
			pf, err := parquet.OpenFile(f, fi.Size())
			if err != nil {
				f.Close()
				t.Fatalf("failed reading parquet shard %s: %v", shardPath, err)
			}
			totalRows += pf.NumRows()
			f.Close()
		}

		if totalRows == 0 {
			t.Errorf("expected > 0 rows written across shards, got %d", totalRows)
		}
	})

	t.Run("E2E-BEAM-02: Malformed Input and DLQ Routing via Beam SDF Graph", func(t *testing.T) {
		outputDir := filepath.Join(repoRoot, "testdata", "output", "e2e_beam_02")
		_ = os.RemoveAll(outputDir)
		_ = os.MkdirAll(outputDir, 0755)

		manifestPath := filepath.Join(repoRoot, "testdata", "configs", "manifest_dlq_quarantine.json")
		manifestBytes, err := os.ReadFile(manifestPath)
		if err != nil {
			t.Fatalf("failed reading manifest: %v", err)
		}

		var cfg domain.PipelineConfig
		if err := json.Unmarshal(manifestBytes, &cfg); err != nil {
			t.Fatalf("failed unmarshaling manifest: %v", err)
		}
		cfg.Runner = "dataflow"
		cfg.Destination.OutputPath = outputDir
		cfg.DLQConfig.QuarantinePath = filepath.Join(outputDir, "dlq")

		updatedManifest, _ := json.Marshal(cfg)
		testManifestPath := filepath.Join(outputDir, "manifest.json")
		_ = os.WriteFile(testManifestPath, updatedManifest, 0644)

		cmd := exec.Command(binPath,
			"--config="+testManifestPath,
			"--runner=direct",
		)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "GOTMPDIR="+filepath.Join(repoRoot, ".gotmp"))
		cmdOut, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("pipeline run failed: %v\nOutput:\n%s", err, string(cmdOut))
		}

		// Verify DLQ files
		dlqMatches, err := filepath.Glob(filepath.Join(outputDir, "dlq", "*.jsonl"))
		if err != nil || len(dlqMatches) == 0 {
			t.Fatalf("no DLQ jsonl files generated in %s/dlq: %v", outputDir, err)
		}
	})

	t.Run("E2E-BEAM-03: SQL Database Ingestion via Beam SDF Graph", func(t *testing.T) {
		if !isPortOpen("127.0.0.1:5432") {
			t.Skip("skipping PostgreSQL Beam SDF test: postgres container not running on 127.0.0.1:5432")
		}

		outputDir := filepath.Join(repoRoot, "testdata", "output", "e2e_beam_03")
		_ = os.RemoveAll(outputDir)
		_ = os.MkdirAll(outputDir, 0755)

		manifestPath := filepath.Join(repoRoot, "testdata", "configs", "manifest_pg_int_pk.json")
		manifestBytes, err := os.ReadFile(manifestPath)
		if err != nil {
			t.Fatalf("failed reading manifest: %v", err)
		}

		var cfg domain.PipelineConfig
		if err := json.Unmarshal(manifestBytes, &cfg); err != nil {
			t.Fatalf("failed unmarshaling manifest: %v", err)
		}
		cfg.Runner = "dataflow"
		cfg.Destination.OutputPath = outputDir
		cfg.DLQConfig.QuarantinePath = filepath.Join(outputDir, "dlq")

		updatedManifest, _ := json.Marshal(cfg)
		testManifestPath := filepath.Join(outputDir, "manifest.json")
		_ = os.WriteFile(testManifestPath, updatedManifest, 0644)

		cmd := exec.Command(binPath,
			"--config="+testManifestPath,
			"--runner=direct",
		)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "GOTMPDIR="+filepath.Join(repoRoot, ".gotmp"))
		cmdOut, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("pipeline run failed: %v\nOutput:\n%s", err, string(cmdOut))
		}

		matches, err := filepath.Glob(filepath.Join(outputDir, "shard-*.parquet"))
		if err != nil || len(matches) == 0 {
			t.Fatalf("no output parquet shards generated in %s: %v", outputDir, err)
		}
	})
}
