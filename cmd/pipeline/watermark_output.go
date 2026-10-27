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
		f, err := os.Open(file)
		if err != nil {
			return nil, fmt.Errorf("failed opening parquet file %q: %w", file, err)
		}

		fi, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed stating parquet file %q: %w", file, err)
		}

		pf, err := parquet.OpenFile(f, fi.Size())
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("failed reading parquet header %q: %w", file, err)
		}

		colIdx := -1
		for _, c := range pf.Root().Columns() {
			if c.Name() == column {
				colIdx = c.Index()
				break
			}
		}
		if colIdx == -1 {
			_ = f.Close()
			return nil, fmt.Errorf("watermark column %q not found in parquet file %q", column, file)
		}

		for _, rg := range pf.RowGroups() {
			chunks := rg.ColumnChunks()
			if colIdx >= len(chunks) {
				continue
			}
			chunk := chunks[colIdx]
			pages := chunk.Pages()

			for {
				p, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					_ = pages.Close()
					_ = f.Close()
					return nil, fmt.Errorf("failed reading page from %q: %w", file, err)
				}

				valReader := p.Values()
				buf := make([]parquet.Value, 128)
				for {
					n, err := valReader.ReadValues(buf)
					for i := 0; i < n; i++ {
						v := buf[i]
						if v.IsNull() {
							continue
						}
						parsed, parseErr := parseParquetValue(v, wType)
						if parseErr != nil {
							_ = pages.Close()
							_ = f.Close()
							return nil, fmt.Errorf("failed parsing column %q in %q: %w", column, file, parseErr)
						}
						if maxVal == nil || parsed.Compare(*maxVal) > 0 {
							maxVal = &parsed
						}
					}
					if err == io.EOF {
						break
					}
					if err != nil {
						_ = pages.Close()
						_ = f.Close()
						return nil, fmt.Errorf("failed reading values from %q: %w", file, err)
					}
				}
			}
			_ = pages.Close()
		}
		_ = f.Close()
	}

	return maxVal, nil
}

func parseParquetValue(v parquet.Value, wType domain.WatermarkType) (domain.WatermarkValue, error) {
	switch wType {
	case domain.WatermarkTypeInteger:
		var s string
		if v.Kind() == parquet.Int64 {
			s = strconv.FormatInt(v.Int64(), 10)
		} else if v.Kind() == parquet.Int32 {
			s = strconv.FormatInt(int64(v.Int32()), 10)
		} else {
			s = v.String()
		}
		return domain.ParseWatermarkValue(wType, s)

	case domain.WatermarkTypeFloat:
		var s string
		if v.Kind() == parquet.Double {
			s = strconv.FormatFloat(v.Double(), 'f', -1, 64)
		} else if v.Kind() == parquet.Float {
			s = strconv.FormatFloat(float64(v.Float()), 'f', -1, 64)
		} else {
			s = v.String()
		}
		return domain.ParseWatermarkValue(wType, s)

	case domain.WatermarkTypeTimestamp:
		if v.Kind() == parquet.Int64 {
			tm := time.UnixMicro(v.Int64()).UTC()
			var s string
			if tm.Nanosecond() != 0 {
				s = tm.Format("2006-01-02T15:04:05.999999-07:00")
			} else {
				s = tm.Format("2006-01-02T15:04:05-07:00")
			}
			return domain.ParseWatermarkValue(wType, s)
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
	var wm *domain.WatermarkConfig
	var isOffset bool
	if cfg.DatabaseSource != nil && cfg.DatabaseSource.Watermark != nil {
		wm = cfg.DatabaseSource.Watermark
	} else if cfg.APISource != nil && cfg.APISource.Watermark != nil {
		wm = cfg.APISource.Watermark
		isOffset = cfg.APISource.WatermarkIsOffset()
	}
	if wm == nil {
		return nil, nil
	}

	if isOffset {
		startOffset := 0
		if wm.Value != nil && strings.TrimSpace(*wm.Value) != "" {
			startOffset, _ = strconv.Atoi(strings.TrimSpace(*wm.Value))
		}
		newOffset := startOffset + int(totalRead)
		newOffsetStr := strconv.Itoa(newOffset)
		return &domain.WatermarkOutput{
			Column: wm.Column,
			Type:   wm.Type,
			Value:  &newOffsetStr,
		}, nil
	}

	if rowsWritten == 0 {
		if wm.Value != nil && strings.TrimSpace(*wm.Value) != "" {
			trimmed := strings.TrimSpace(*wm.Value)
			return &domain.WatermarkOutput{
				Column: wm.Column,
				Type:   wm.Type,
				Value:  &trimmed,
			}, nil
		}
		return &domain.WatermarkOutput{
			Column: wm.Column,
			Type:   wm.Type,
			Value:  nil,
		}, nil
	}

	var parquetFiles []string
	entries, _ := os.ReadDir(outDir)
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".parquet") {
			parquetFiles = append(parquetFiles, filepath.Join(outDir, entry.Name()))
		}
	}

	maxVal, err := maxParquetColumn(parquetFiles, wm.Column, wm.Type)
	if err != nil {
		return nil, err
	}

	if maxVal == nil {
		if wm.Value != nil && strings.TrimSpace(*wm.Value) != "" {
			trimmed := strings.TrimSpace(*wm.Value)
			return &domain.WatermarkOutput{
				Column: wm.Column,
				Type:   wm.Type,
				Value:  &trimmed,
			}, nil
		}
		return &domain.WatermarkOutput{
			Column: wm.Column,
			Type:   wm.Type,
			Value:  nil,
		}, nil
	}

	finalVal := *maxVal
	if incoming, err := wm.Incoming(); err == nil && incoming != nil {
		if incoming.Compare(finalVal) > 0 {
			finalVal = *incoming
		}
	}

	canonicalStr := finalVal.String()
	return &domain.WatermarkOutput{
		Column: wm.Column,
		Type:   wm.Type,
		Value:  &canonicalStr,
	}, nil
}
