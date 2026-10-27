package domain

import (
	"fmt"
	"regexp"
	"strings"
)

var validColumnIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// WatermarkConfig defines declarative incremental watermark parameters.
type WatermarkConfig struct {
	Column string        `json:"column"`
	Type   WatermarkType `json:"type"`
	Value  *string       `json:"value,omitempty"`
}

// Validate checks column identifier safety and supported watermark types.
func (w *WatermarkConfig) Validate() error {
	if w == nil {
		return nil
	}
	col := strings.TrimSpace(w.Column)
	if col == "" {
		return fmt.Errorf("watermark column cannot be empty")
	}
	if !validColumnIdentifier.MatchString(col) {
		return fmt.Errorf("watermark column %q contains invalid characters (must match ^[A-Za-z_][A-Za-z0-9_]*$)", col)
	}
	return w.Type.Validate()
}

// Incoming returns the parsed WatermarkValue if present, or nil if this is an initial execution.
func (w *WatermarkConfig) Incoming() (*WatermarkValue, error) {
	if w == nil || w.Value == nil || strings.TrimSpace(*w.Value) == "" {
		return nil, nil
	}
	val, err := ParseWatermarkValue(w.Type, *w.Value)
	if err != nil {
		return nil, err
	}
	return &val, nil
}

// WatermarkOutput represents the emitted watermark state in metrics.json.
type WatermarkOutput struct {
	Column string        `json:"column"`
	Type   WatermarkType `json:"type"`
	Value  *string       `json:"value"` // Emits null when value is nil (no omitempty)
}
