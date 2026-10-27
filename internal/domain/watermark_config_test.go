package domain_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestParsePipelineConfig_RejectsWatermarkOnFileSource(t *testing.T) {
	manifest := `{
		"pipeline_id": "test_pipe",
		"run_id": "run_01",
		"source": {
			"path": "testdata/fixtures/sample.csv",
			"format": "csv",
			"schema": {
				"fields": [{"name": "id", "type": "int64"}]
			},
			"watermark": {
				"column": "id",
				"type": "integer"
			}
		},
		"destination": {
			"output_path": "testdata/output/test.parquet",
			"output_format": "parquet"
		}
	}`
	_, err := domain.ParsePipelineConfig([]byte(manifest))
	if err == nil || !strings.Contains(err.Error(), "file sources") {
		t.Fatalf("expected error mentioning 'file sources', got: %v", err)
	}
}

func TestWatermarkConfig_RejectsUnsafeColumn(t *testing.T) {
	invalidCols := []string{
		"updated_at; DROP TABLE x",
		"col-name",
		"123col",
		"col name",
		"col$name",
	}
	for _, col := range invalidCols {
		cfg := domain.WatermarkConfig{
			Column: col,
			Type:   domain.WatermarkTypeTimestamp,
		}
		if err := cfg.Validate(); err == nil {
			t.Errorf("expected validation error for column %q, got nil", col)
		}
	}

	validCols := []string{"updated_at", "id", "_created_at", "col123", "OrderDate"}
	for _, col := range validCols {
		cfg := domain.WatermarkConfig{
			Column: col,
			Type:   domain.WatermarkTypeInteger,
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected valid column %q, got %v", col, err)
		}
	}
}

func TestParsePipelineConfig_RejectsWatermarkColumnMissingFromSchema(t *testing.T) {
	manifest := `{
		"pipeline_id": "test_pipe",
		"run_id": "run_01",
		"database_source": {
			"driver": "postgres",
			"host": "localhost",
			"table": "orders",
			"schema": {
				"fields": [{"name": "id", "type": "int64"}]
			},
			"watermark": {
				"column": "non_existent_col",
				"type": "integer"
			}
		},
		"destination": {
			"output_path": "testdata/output/test.parquet",
			"output_format": "parquet"
		}
	}`
	_, err := domain.ParsePipelineConfig([]byte(manifest))
	if err == nil || !strings.Contains(err.Error(), "not found in schema") {
		t.Fatalf("expected error mentioning column missing from schema, got: %v", err)
	}
}

func TestParsePipelineConfig_RejectsWatermarkTypeIncompatibleWithSchema(t *testing.T) {
	manifest := `{
		"pipeline_id": "test_pipe",
		"run_id": "run_01",
		"database_source": {
			"driver": "postgres",
			"host": "localhost",
			"table": "orders",
			"schema": {
				"fields": [{"name": "name", "type": "string"}]
			},
			"watermark": {
				"column": "name",
				"type": "integer"
			}
		},
		"destination": {
			"output_path": "testdata/output/test.parquet",
			"output_format": "parquet"
		}
	}`
	_, err := domain.ParsePipelineConfig([]byte(manifest))
	if err == nil || !strings.Contains(err.Error(), "incompatible") {
		t.Fatalf("expected error mentioning type incompatible, got: %v", err)
	}
}

func TestParsePipelineConfig_RejectsWatermarkWithNonParquetOrEncryptedOrGCSDestination(t *testing.T) {
	// Non-parquet
	manifestCSV := `{
		"pipeline_id": "test_pipe",
		"run_id": "run_01",
		"database_source": {
			"driver": "postgres",
			"host": "localhost",
			"table": "orders",
			"schema": {
				"fields": [{"name": "id", "type": "int64"}]
			},
			"watermark": {
				"column": "id",
				"type": "integer"
			}
		},
		"destination": {
			"output_path": "testdata/output/test.csv",
			"output_format": "csv"
		}
	}`
	if _, err := domain.ParsePipelineConfig([]byte(manifestCSV)); err == nil {
		t.Fatalf("expected error for non-parquet output with watermark, got nil")
	}

	// Encrypted destination
	manifestEnc := `{
		"pipeline_id": "test_pipe",
		"run_id": "run_01",
		"database_source": {
			"driver": "postgres",
			"host": "localhost",
			"table": "orders",
			"schema": {
				"fields": [{"name": "id", "type": "int64"}]
			},
			"watermark": {
				"column": "id",
				"type": "integer"
			}
		},
		"destination": {
			"output_path": "testdata/output/test.parquet",
			"output_format": "parquet",
			"encryption": {
				"type": "pgp",
				"public_key_ref": "sec:key"
			}
		}
	}`
	if _, err := domain.ParsePipelineConfig([]byte(manifestEnc)); err == nil {
		t.Fatalf("expected error for encrypted destination with watermark, got nil")
	}
}

func TestParsePipelineConfig_AcceptsOffsetWatermarkWithoutSchemaColumn(t *testing.T) {
	manifest := `{
		"pipeline_id": "test_pipe",
		"run_id": "run_01",
		"api_source": {
			"base_url": "https://api.example.com",
			"endpoint": "/users",
			"pagination": {
				"type": "offset_limit",
				"page_size": 100,
				"page_param": "offset"
			},
			"schema": {
				"fields": [{"name": "user_id", "type": "string"}]
			},
			"watermark": {
				"column": "offset",
				"type": "integer",
				"value": "1500"
			}
		},
		"destination": {
			"output_path": "testdata/output/test.parquet",
			"output_format": "parquet"
		}
	}`
	cfg, err := domain.ParsePipelineConfig([]byte(manifest))
	if err != nil {
		t.Fatalf("expected valid config for offset_limit watermark, got: %v", err)
	}
	if !cfg.APISource.WatermarkIsOffset() {
		t.Fatalf("expected WatermarkIsOffset to be true")
	}
}

func TestPipelineMetrics_MarshalJSON_EmitsNullWatermarkValue(t *testing.T) {
	m := domain.NewPipelineMetrics("test_p", "run_1")
	m.Watermark = &domain.WatermarkOutput{
		Column: "id",
		Type:   domain.WatermarkTypeInteger,
		Value:  nil,
	}

	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("failed marshaling: %v", err)
	}
	str := string(data)
	if !strings.Contains(str, `"watermark":{"column":"id","type":"integer","value":null}`) {
		t.Fatalf("expected exact null watermark value in JSON, got: %s", str)
	}
}

func TestPipelineMetrics_Snapshot_CopiesWatermark(t *testing.T) {
	val := "2026-10-24T10:00:00+00:00"
	m := domain.NewPipelineMetrics("test_p", "run_1")
	m.Watermark = &domain.WatermarkOutput{
		Column: "updated_at",
		Type:   domain.WatermarkTypeTimestamp,
		Value:  &val,
	}

	snap := m.Snapshot()
	if snap.Watermark == nil || snap.Watermark.Value == nil || *snap.Watermark.Value != val {
		t.Fatalf("expected Snapshot to copy watermark, got %+v", snap.Watermark)
	}
}
