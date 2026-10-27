package domain

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type WatermarkType string

const (
	WatermarkTypeInteger   WatermarkType = "integer"
	WatermarkTypeFloat     WatermarkType = "float"
	WatermarkTypeTimestamp WatermarkType = "timestamp"
	WatermarkTypeDate      WatermarkType = "date"
	WatermarkTypeString    WatermarkType = "string"
)

func (t WatermarkType) Validate() error {
	switch t {
	case WatermarkTypeInteger, WatermarkTypeFloat, WatermarkTypeTimestamp, WatermarkTypeDate, WatermarkTypeString:
		return nil
	default:
		return fmt.Errorf("unsupported watermark type %q (supported: integer, float, timestamp, date, string)", t)
	}
}

// WatermarkValue is an immutable typed representation of a scalar watermark boundary.
type WatermarkValue struct {
	wType    WatermarkType
	intVal   int64
	floatVal float64
	timeVal  time.Time
	strVal   string
}

// ParseWatermarkValue parses and validates raw scalar into a typed WatermarkValue.
func ParseWatermarkValue(t WatermarkType, raw string) (WatermarkValue, error) {
	if err := t.Validate(); err != nil {
		return WatermarkValue{}, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return WatermarkValue{}, fmt.Errorf("watermark value cannot be empty")
	}

	switch t {
	case WatermarkTypeInteger:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return WatermarkValue{}, fmt.Errorf("invalid integer watermark %q: %w", raw, err)
		}
		return WatermarkValue{wType: t, intVal: n}, nil

	case WatermarkTypeFloat:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return WatermarkValue{}, fmt.Errorf("invalid float watermark %q: %w", raw, err)
		}
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return WatermarkValue{}, fmt.Errorf("watermark float %q cannot be NaN or Inf", raw)
		}
		return WatermarkValue{wType: t, floatVal: f}, nil

	case WatermarkTypeTimestamp:
		tm, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			tm, err = time.Parse(time.RFC3339, raw)
			if err != nil {
				return WatermarkValue{}, fmt.Errorf("invalid timestamp watermark %q (must be RFC3339/ISO-8601): %w", raw, err)
			}
		}
		return WatermarkValue{wType: t, timeVal: tm}, nil

	case WatermarkTypeDate:
		tm, err := time.Parse("2006-01-02", raw)
		if err != nil {
			return WatermarkValue{}, fmt.Errorf("invalid date watermark %q (must be YYYY-MM-DD): %w", raw, err)
		}
		return WatermarkValue{wType: t, timeVal: tm}, nil

	case WatermarkTypeString:
		return WatermarkValue{wType: t, strVal: raw}, nil

	default:
		return WatermarkValue{}, fmt.Errorf("unsupported watermark type %q", t)
	}
}

// Compare returns -1 if v < other, 0 if v == other, 1 if v > other.
func (v WatermarkValue) Compare(other WatermarkValue) int {
	switch v.wType {
	case WatermarkTypeInteger:
		if v.intVal < other.intVal {
			return -1
		} else if v.intVal > other.intVal {
			return 1
		}
		return 0

	case WatermarkTypeFloat:
		if v.floatVal < other.floatVal {
			return -1
		} else if v.floatVal > other.floatVal {
			return 1
		}
		return 0

	case WatermarkTypeTimestamp, WatermarkTypeDate:
		if v.timeVal.Before(other.timeVal) {
			return -1
		} else if v.timeVal.After(other.timeVal) {
			return 1
		}
		return 0

	default: // WatermarkTypeString
		if v.strVal < other.strVal {
			return -1
		} else if v.strVal > other.strVal {
			return 1
		}
		return 0
	}
}

// String returns the canonical string representation of the watermark value.
func (v WatermarkValue) String() string {
	switch v.wType {
	case WatermarkTypeInteger:
		return strconv.FormatInt(v.intVal, 10)
	case WatermarkTypeFloat:
		return strconv.FormatFloat(v.floatVal, 'f', -1, 64)
	case WatermarkTypeTimestamp:
		if v.timeVal.Nanosecond() != 0 {
			return v.timeVal.Format("2006-01-02T15:04:05.999999-07:00")
		}
		return v.timeVal.Format("2006-01-02T15:04:05-07:00")
	case WatermarkTypeDate:
		return v.timeVal.Format("2006-01-02")
	default:
		return v.strVal
	}
}

// SQLLiteral formats the value as a safe literal to inject into SQL queries.
func (v WatermarkValue) SQLLiteral() string {
	switch v.wType {
	case WatermarkTypeInteger:
		return strconv.FormatInt(v.intVal, 10)
	case WatermarkTypeFloat:
		return strconv.FormatFloat(v.floatVal, 'f', -1, 64)
	case WatermarkTypeTimestamp, WatermarkTypeDate:
		return "'" + v.String() + "'"
	default:
		return "'" + strings.ReplaceAll(v.strVal, "'", "''") + "'"
	}
}
