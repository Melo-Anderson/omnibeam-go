package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

func writeExecutionMetrics(cfg *domain.PipelineConfig) error {
	outDir := resolveOutputDir(cfg)
	if outDir == "" || isRemoteURI(outDir) {
		return nil
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("failed creating metrics directory: %w", err)
	}

	rowsWritten, bytesWritten, filesWritten, lastChecksum := scanOutputMetrics(outDir, cfg.Destination.FormatOptions.IncludeHeader)

	dlqPath := cfg.DLQConfig.QuarantinePath
	if dlqPath == "" {
		dlqPath = filepath.Join(outDir, "dlq.jsonl")
	}
	dlqCount := countDLQRecords(dlqPath)
	totalRead := rowsWritten + dlqCount

	metrics := domain.NewPipelineMetrics(cfg.PipelineID, cfg.RunID)
	metrics.TotalRecordsRead = totalRead
	metrics.RowsWritten = rowsWritten
	metrics.RowCount = rowsWritten
	metrics.DeadLetterCount = dlqCount
	metrics.BytesWritten = bytesWritten
	metrics.FilesWritten = filesWritten
	metrics.Checksum = lastChecksum

	for _, f := range cfg.GetSchema().Fields {
		metrics.ColumnNullCounts["null_count_"+f.Name] = 0
		metrics.InvalidValueCounts["invalid_value_count_"+f.Name] = 0
	}

	wmOutput, err := resolveWatermarkOutput(cfg, outDir, totalRead, rowsWritten)
	if err != nil {
		return fmt.Errorf("failed resolving watermark output: %w", err)
	}
	metrics.Watermark = wmOutput

	mData, err := metrics.MarshalJSON()
	if err != nil {
		return fmt.Errorf("failed marshaling metrics: %w", err)
	}
	if err := os.WriteFile(filepath.Join(outDir, "metrics.json"), mData, 0644); err != nil {
		return fmt.Errorf("failed writing metrics file: %w", err)
	}
	return nil
}

func resolveOutputDir(cfg *domain.PipelineConfig) string {
	if cfg.Destination.OutputPath != "" {
		return filepath.Dir(cfg.Destination.OutputPath)
	}
	if cfg.DLQConfig.QuarantinePath != "" {
		return filepath.Dir(cfg.DLQConfig.QuarantinePath)
	}
	return ""
}

func scanOutputMetrics(outDir string, includeHeader bool) (rowsWritten, bytesWritten, filesWritten int64, lastChecksum string) {
	entries, _ := os.ReadDir(outDir)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == "metrics.json" || name == "dlq.jsonl" || strings.HasSuffix(name, ".jsonl") {
			continue
		}
		fi, err := entry.Info()
		if err != nil {
			continue
		}
		filePath := filepath.Join(outDir, name)
		bytesWritten += fi.Size()
		filesWritten++

		switch {
		case strings.HasSuffix(name, ".parquet"):
			rowsWritten += countParquetRows(filePath, fi.Size())
		case strings.HasSuffix(name, ".csv"), strings.HasSuffix(name, ".txt"):
			rowsWritten += countDelimitedRows(filePath, includeHeader)
		}

		if sum := fileMD5(filePath); sum != "" {
			lastChecksum = sum
		}
	}
	return rowsWritten, bytesWritten, filesWritten, lastChecksum
}

func countParquetRows(path string, size int64) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()

	pf, err := parquet.OpenFile(f, size)
	if err != nil {
		return 0
	}
	return pf.NumRows()
}

func countDelimitedRows(path string, includeHeader bool) int64 {
	f, err := os.Open(path)
	if err != nil {
		return 0
	}
	defer func() { _ = f.Close() }()

	var lines int64
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) != "" {
			lines++
		}
	}
	if lines > 0 && includeHeader {
		lines--
	}
	return lines
}

func fileMD5(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return ""
	}
	return hex.EncodeToString(h.Sum(nil))
}

func countDLQRecords(dlqPath string) int64 {
	return countDelimitedRows(dlqPath, false)
}
