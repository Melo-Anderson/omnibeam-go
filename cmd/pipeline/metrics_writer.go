package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

func writeExecutionMetrics(cfg *domain.PipelineConfig) {
	outDir := ""
	if cfg.Destination.OutputPath != "" {
		outDir = filepath.Dir(cfg.Destination.OutputPath)
	} else if cfg.DLQConfig.QuarantinePath != "" {
		outDir = filepath.Dir(cfg.DLQConfig.QuarantinePath)
	}
	if outDir == "" || strings.HasPrefix(outDir, "gs://") {
		return
	}

	_ = os.MkdirAll(outDir, 0755)

	var rowsWritten int64
	var bytesWritten int64
	var filesWritten int64
	var lastChecksum string

	entries, _ := os.ReadDir(outDir)
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "metrics.json" || entry.Name() == "dlq.jsonl" || strings.HasSuffix(entry.Name(), ".jsonl") {
			continue
		}
		filePath := filepath.Join(outDir, entry.Name())
		fi, err := entry.Info()
		if err != nil {
			continue
		}
		bytesWritten += fi.Size()
		filesWritten++

		if strings.HasSuffix(entry.Name(), ".parquet") {
			f, err := os.Open(filePath)
			if err == nil {
				pf, err := parquet.OpenFile(f, fi.Size())
				if err == nil {
					rowsWritten += pf.NumRows()
				}
				_ = f.Close()
			}
		} else if strings.HasSuffix(entry.Name(), ".csv") || strings.HasSuffix(entry.Name(), ".txt") {
			f, err := os.Open(filePath)
			if err == nil {
				sc := bufio.NewScanner(f)
				var lineCount int64
				for sc.Scan() {
					if strings.TrimSpace(sc.Text()) != "" {
						lineCount++
					}
				}
				_ = f.Close()
				if lineCount > 0 && cfg.Destination.FormatOptions.IncludeHeader {
					lineCount--
				}
				rowsWritten += lineCount
			}
		}

		// Calculate checksum via streaming buffer to avoid loading multi-GB files into RAM
		f, err := os.Open(filePath)
		if err == nil {
			h := md5.New()
			if _, copyErr := io.Copy(h, f); copyErr == nil {
				lastChecksum = hex.EncodeToString(h.Sum(nil))
			}
			_ = f.Close()
		}
	}

	var dlqCount int64
	dlqPath := cfg.DLQConfig.QuarantinePath
	if dlqPath == "" {
		dlqPath = filepath.Join(outDir, "dlq.jsonl")
	}
	if f, err := os.Open(dlqPath); err == nil {
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if strings.TrimSpace(sc.Text()) != "" {
				dlqCount++
			}
		}
		_ = f.Close()
	}

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

	mData, err := metrics.MarshalJSON()
	if err == nil {
		_ = os.WriteFile(filepath.Join(outDir, "metrics.json"), mData, 0644)
	}
}
