package domain

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
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

// PassthroughOutput preserves the trimmed incoming watermark value (or nil on first run).
func (w *WatermarkConfig) PassthroughOutput() *WatermarkOutput {
	if w.Value != nil && strings.TrimSpace(*w.Value) != "" {
		trimmed := strings.TrimSpace(*w.Value)
		return &WatermarkOutput{
			Column: w.Column,
			Type:   w.Type,
			Value:  &trimmed,
		}
	}
	return &WatermarkOutput{
		Column: w.Column,
		Type:   w.Type,
		Value:  nil,
	}
}

// AdvanceOffsetOutput computes the next offset watermark by adding totalRead to the incoming offset.
func (w *WatermarkConfig) AdvanceOffsetOutput(totalRead int64) (*WatermarkOutput, error) {
	startOffset := 0
	if w.Value != nil && strings.TrimSpace(*w.Value) != "" {
		parsed, err := strconv.Atoi(strings.TrimSpace(*w.Value))
		if err != nil {
			return nil, fmt.Errorf("invalid incoming offset watermark %q: %w", *w.Value, err)
		}
		startOffset = parsed
	}
	newOffsetStr := strconv.Itoa(startOffset + int(totalRead))
	return &WatermarkOutput{
		Column: w.Column,
		Type:   w.Type,
		Value:  &newOffsetStr,
	}, nil
}

// MonotonicOutput returns a WatermarkOutput whose value never regresses below the incoming watermark.
func (w *WatermarkConfig) MonotonicOutput(observed WatermarkValue) *WatermarkOutput {
	finalVal := observed
	if incoming, err := w.Incoming(); err == nil && incoming != nil && incoming.Compare(finalVal) > 0 {
		finalVal = *incoming
	}
	canonicalStr := finalVal.String()
	return &WatermarkOutput{
		Column: w.Column,
		Type:   w.Type,
		Value:  &canonicalStr,
	}
}

func (c *PipelineConfig) validateWatermark() error {
	if c.Source.Watermark != nil && c.DatabaseSource == nil && c.APISource == nil {
		return errors.New("watermark is not supported for file sources")
	}

	var wm *WatermarkConfig
	var isOffset bool
	if c.DatabaseSource != nil && c.DatabaseSource.Watermark != nil {
		wm = c.DatabaseSource.Watermark
	} else if c.APISource != nil && c.APISource.Watermark != nil {
		wm = c.APISource.Watermark
		isOffset = c.APISource.WatermarkIsOffset()
	}

	if wm == nil {
		return nil
	}

	if err := wm.Validate(); err != nil {
		return err
	}
	if _, err := wm.Incoming(); err != nil {
		return err
	}

	// Destination restrictions
	destFormat := strings.ToLower(strings.TrimSpace(c.Destination.OutputFormat))
	if destFormat != "parquet" {
		return fmt.Errorf("incremental watermark requires destination output_format to be parquet, got %q", destFormat)
	}
	if c.Destination.Encryption.Type != "" && strings.ToLower(c.Destination.Encryption.Type) != "none" {
		return fmt.Errorf("incremental watermark is not supported with destination encryption %q", c.Destination.Encryption.Type)
	}
	if c.Destination.Type == "rest_api" || c.Destination.Type == "bigquery" {
		return fmt.Errorf("incremental watermark is not supported for destination type %q", c.Destination.Type)
	}
	if strings.HasPrefix(c.Destination.OutputPath, "gs://") || strings.HasPrefix(c.Destination.OutputPath, "gs:\\") {
		return errors.New("incremental watermark is currently not supported for gcs destinations")
	}

	// Schema verification
	if !isOffset {
		schema := c.GetSchema()
		if len(schema.Fields) > 0 {
			var field *Field
			for i := range schema.Fields {
				if schema.Fields[i].Name == wm.Column {
					field = &schema.Fields[i]
					break
				}
			}
			if field == nil {
				return fmt.Errorf("watermark column %q not found in schema", wm.Column)
			}
			if err := checkWatermarkTypeCompatibility(wm.Type, field.Type); err != nil {
				return fmt.Errorf("watermark column %q type incompatible: %w", wm.Column, err)
			}
		}
	}

	return nil
}

func checkWatermarkTypeCompatibility(wmType WatermarkType, fieldType DataType) error {
	expected := map[WatermarkType]DataType{
		WatermarkTypeInteger:   TypeInt64,
		WatermarkTypeFloat:     TypeFloat64,
		WatermarkTypeTimestamp: TypeTimestamp,
		WatermarkTypeDate:      TypeTimestamp,
		WatermarkTypeString:    TypeString,
	}
	if req, ok := expected[wmType]; ok && fieldType != req {
		return fmt.Errorf("watermark type %s requires schema %s, got %s", wmType, req, fieldType)
	}
	return nil
}
