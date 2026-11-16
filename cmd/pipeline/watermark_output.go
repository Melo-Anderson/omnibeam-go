package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

// maxParquetColumn scans files and returns the maximum typed value for the designated column.
func maxParquetColumn(files []string, column string, wType domain.WatermarkType) (*domain.WatermarkValue, error) {
	var maxVal *domain.WatermarkValue
	for _, file := range files {
		fileMax, err := maxParquetColumnInFile(file, column, wType)
		if err != nil {
			return nil, err
		}
		if fileMax != nil && (maxVal == nil || fileMax.Compare(*maxVal) > 0) {
			maxVal = fileMax
		}
	}
	return maxVal, nil
}

func maxParquetColumnInFile(file, column string, wType domain.WatermarkType) (*domain.WatermarkValue, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, fmt.Errorf("failed opening parquet file %q: %w", file, err)
	}
	defer func() { _ = f.Close() }()

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed stating parquet file %q: %w", file, err)
	}

	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return nil, fmt.Errorf("failed reading parquet header %q: %w", file, err)
	}

	colIdx := findParquetColumnIndex(pf, column)
	if colIdx == -1 {
		return nil, fmt.Errorf("watermark column %q not found in parquet file %q", column, file)
	}

	var maxVal *domain.WatermarkValue
	for _, rg := range pf.RowGroups() {
		chunks := rg.ColumnChunks()
		if colIdx >= len(chunks) {
			continue
		}
		rgMax, err := maxParquetColumnInChunk(chunks[colIdx], file, column, wType)
		if err != nil {
			return nil, err
		}
		if rgMax != nil && (maxVal == nil || rgMax.Compare(*maxVal) > 0) {
			maxVal = rgMax
		}
	}
	return maxVal, nil
}

func findParquetColumnIndex(pf *parquet.File, column string) int {
	for _, c := range pf.Root().Columns() {
		if c.Name() == column {
			return c.Index()
		}
	}
	return -1
}

func maxParquetColumnInChunk(chunk parquet.ColumnChunk, file, column string, wType domain.WatermarkType) (*domain.WatermarkValue, error) {
	pages := chunk.Pages()
	defer func() { _ = pages.Close() }()

	var maxVal *domain.WatermarkValue
	buf := make([]parquet.Value, 128)
	for {
		p, err := pages.ReadPage()
		if err == io.EOF {
			return maxVal, nil
		}
		if err != nil {
			return nil, fmt.Errorf("failed reading page from %q: %w", file, err)
		}

		valReader := p.Values()
		for {
			n, readErr := valReader.ReadValues(buf)
			for i := 0; i < n; i++ {
				if buf[i].IsNull() {
					continue
				}
				parsed, parseErr := parseParquetValue(buf[i], wType)
				if parseErr != nil {
					return nil, fmt.Errorf("failed parsing column %q in %q: %w", column, file, parseErr)
				}
				if maxVal == nil || parsed.Compare(*maxVal) > 0 {
					maxVal = &parsed
				}
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return nil, fmt.Errorf("failed reading values from %q: %w", file, readErr)
			}
		}
	}
}

func parseParquetValue(v parquet.Value, wType domain.WatermarkType) (domain.WatermarkValue, error) {
	switch wType {
	case domain.WatermarkTypeInteger:
		var s string
		switch v.Kind() {
		case parquet.Int64:
			s = strconv.FormatInt(v.Int64(), 10)
		case parquet.Int32:
			s = strconv.FormatInt(int64(v.Int32()), 10)
		default:
			s = v.String()
		}
		return domain.ParseWatermarkValue(wType, s)

	case domain.WatermarkTypeFloat:
		var s string
		switch v.Kind() {
		case parquet.Double:
			s = strconv.FormatFloat(v.Double(), 'f', -1, 64)
		case parquet.Float:
			s = strconv.FormatFloat(float64(v.Float()), 'f', -1, 64)
		default:
			s = v.String()
		}
		return domain.ParseWatermarkValue(wType, s)

	case domain.WatermarkTypeTimestamp:
		if v.Kind() == parquet.Int64 {
			tm := time.UnixMicro(v.Int64()).UTC()
			layout := "2006-01-02T15:04:05-07:00"
			if tm.Nanosecond() != 0 {
				layout = "2006-01-02T15:04:05.999999-07:00"
			}
			return domain.ParseWatermarkValue(wType, tm.Format(layout))
		}
		return domain.ParseWatermarkValue(wType, string(v.ByteArray()))

	case domain.WatermarkTypeDate:
		if v.Kind() == parquet.Int64 {
			tm := time.UnixMicro(v.Int64()).UTC()
			return domain.ParseWatermarkValue(wType, tm.Format("2006-01-02"))
		}
		return domain.ParseWatermarkValue(wType, string(v.ByteArray()))

	default: // String
		return domain.ParseWatermarkValue(wType, string(v.ByteArray()))
	}
}

// resolveWatermarkOutput computes the final WatermarkOutput for metrics.json.
func resolveWatermarkOutput(cfg *domain.PipelineConfig, outDir string, totalRead, rowsWritten int64) (*domain.WatermarkOutput, error) {
	if cfg == nil {
		return nil, nil
	}
	wm, isOffset := activeWatermark(cfg)
	if wm == nil {
		return nil, nil
	}
	if isOffset {
		return wm.AdvanceOffsetOutput(totalRead)
	}
	if rowsWritten == 0 {
		return wm.PassthroughOutput(), nil
	}

	parquetFiles, err := listParquetFiles(outDir)
	if err != nil {
		return nil, err
	}
	maxVal, err := maxParquetColumn(parquetFiles, wm.Column, wm.Type)
	if err != nil {
		return nil, err
	}
	if maxVal == nil {
		return wm.PassthroughOutput(), nil
	}
	return wm.MonotonicOutput(*maxVal), nil
}

func activeWatermark(cfg *domain.PipelineConfig) (*domain.WatermarkConfig, bool) {
	if cfg.DatabaseSource != nil && cfg.DatabaseSource.Watermark != nil {
		return cfg.DatabaseSource.Watermark, false
	}
	if cfg.APISource != nil && cfg.APISource.Watermark != nil {
		return cfg.APISource.Watermark, cfg.APISource.WatermarkIsOffset()
	}
	return nil, false
}

func listParquetFiles(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("reading output dir %q for watermark: %w", dir, err)
	}
	var files []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".parquet") {
			files = append(files, filepath.Join(dir, entry.Name()))
		}
	}
	return files, nil
}
