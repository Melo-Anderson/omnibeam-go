//go:build integration
// +build integration

package functional_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/pkg/testutils"
	"github.com/parquet-go/parquet-go"
)

type scaleParquetRecord struct {
	ID         int64   `parquet:"id"`
	CustomerID string  `parquet:"customer_id"`
	Amount     float64 `parquet:"amount"`
	Status     string  `parquet:"status"`
	CreatedAt  string  `parquet:"created_at"`
}

func TestByteStream_Scale_5Million_CSV(t *testing.T) {
	totalRecords := int64(5000000)
	chunkSize := int64(16 * 1024 * 1024) // 16 MB chunks -> ~20-30 parallel splits
	if testing.Short() {
		totalRecords = 50000
		chunkSize = 512 * 1024 // 512 KB chunks -> ~5-10 splits
	}

	tmpDir := t.TempDir()
	csvFilePath := filepath.Join(tmpDir, "input_scale.csv")
	outputDir := filepath.Join(tmpDir, "output_parquet")
	_ = os.MkdirAll(outputDir, 0755)

	t.Logf("Generating %d CSV records into %s...", totalRecords, csvFilePath)
	genStart := time.Now()
	f, err := os.Create(csvFilePath)
	if err != nil {
		t.Fatalf("failed creating csv file: %v", err)
	}
	if err := testutils.GenerateCSVFixture(f, totalRecords); err != nil {
		f.Close()
		t.Fatalf("failed generating csv data: %v", err)
	}
	f.Close()
	t.Logf("Fixture generation complete in %v", time.Since(genStart))

	fi, err := os.Stat(csvFilePath)
	if err != nil {
		t.Fatalf("failed stating csv file: %v", err)
	}
	t.Logf("CSV file size: %.2f MB", float64(fi.Size())/(1024*1024))

	binPath := testPipelineBin

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: "int64", Nullable: false},
			{Name: "customer_id", Type: "string", Nullable: true},
			{Name: "amount", Type: "float64", Nullable: false},
			{Name: "status", Type: "string", Nullable: true},
			{Name: "created_at", Type: "string", Nullable: true},
		},
	}

	cfg := domain.PipelineConfig{
		PipelineID:   "scale-bytestream-01",
		RunID:        "run-scale-01",
		PipelineType: "file",
		Runner:       "dataflow",
		Source: domain.SourceConfig{
			Type:           "local_file",
			Path:           csvFilePath,
			Format:         "csv",
			Delimiter:      ",",
			Charset:        "utf-8",
			Compression:    "none",
			ChunkSizeBytes: chunkSize,
			Schema:         schema,
		},
		Destination: domain.DestinationConfig{
			Type:         "local_storage",
			OutputFormat: "parquet",
			OutputPath:   outputDir,
			Compression:  "snappy",
		},
		DLQConfig: domain.DLQConfig{
			Enabled:        true,
			QuarantinePath: filepath.Join(outputDir, "dlq"),
		},
	}

	manifestBytes, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("failed marshaling config: %v", err)
	}
	manifestPath := filepath.Join(tmpDir, "manifest.json")
	if err := os.WriteFile(manifestPath, manifestBytes, 0644); err != nil {
		t.Fatalf("failed writing manifest: %v", err)
	}

	t.Logf("Running pipeline with SDF ByteStreamSource (ChunkSize: %d bytes)...", chunkSize)
	pipeStart := time.Now()
	cmd := exec.Command(binPath,
		"--config="+manifestPath,
		"--runner=direct",
	)
	cmd.Dir = repoRoot
	cmd.Env = append(os.Environ(), "GOTMPDIR="+filepath.Join(repoRoot, ".gotmp"))
	cmdOut, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("pipeline run failed: %v\nOutput:\n%s", err, string(cmdOut))
	}
	pipeElapsed := time.Since(pipeStart)
	throughput := float64(totalRecords) / pipeElapsed.Seconds()
	t.Logf("Pipeline completed in %v (Throughput: %.0f records/sec)", pipeElapsed, throughput)

	// Validate output Parquet files
	matches, err := filepath.Glob(filepath.Join(outputDir, "shard-*.parquet"))
	if err != nil || len(matches) == 0 {
		t.Fatalf("no output parquet shards generated in %s: %v", outputDir, err)
	}
	t.Logf("Generated %d Parquet shard files", len(matches))

	seenIDs := make([]bool, totalRecords+1)
	var totalRowsRead int64

	for _, shardPath := range matches {
		sf, err := os.Open(shardPath)
		if err != nil {
			t.Fatalf("failed opening shard %s: %v", shardPath, err)
		}
		sfi, _ := sf.Stat()
		pf, err := parquet.OpenFile(sf, sfi.Size())
		if err != nil {
			sf.Close()
			t.Fatalf("failed reading parquet file %s: %v", shardPath, err)
		}

		totalRowsRead += pf.NumRows()
		reader := parquet.NewGenericReader[scaleParquetRecord](sf)
		buf := make([]scaleParquetRecord, 10000)
		for {
			n, err := reader.Read(buf)
			for i := 0; i < n; i++ {
				recID := buf[i].ID
				if recID < 1 || recID > totalRecords {
					t.Fatalf("out-of-bounds record ID %d encountered", recID)
				}
				if seenIDs[recID] {
					t.Fatalf("duplicate record ID %d encountered across shard boundaries", recID)
				}
				seenIDs[recID] = true
			}
			if err != nil {
				break
			}
		}
		reader.Close()
		sf.Close()
	}

	// 1. Assert Volume Conservation
	if totalRowsRead != totalRecords {
		t.Fatalf("Volume Conservation Violation: expected %d rows, got %d", totalRecords, totalRowsRead)
	}

	// 2. Assert Zero Gaps
	var missingCount int64
	for i := int64(1); i <= totalRecords; i++ {
		if !seenIDs[i] {
			missingCount++
		}
	}
	if missingCount > 0 {
		t.Fatalf("Boundary Gap Violation: %d missing IDs detected across splits", missingCount)
	}

	t.Logf("SUCCESS: All %d records verified with 0 duplicates and 0 gaps across %d shards.", totalRecords, len(matches))
}
