package jsonutils_test

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/pkg/jsonutils"
)

func TestExtractRecords_NestedPayload(t *testing.T) {
	payload := []byte(`{
		"response": {
			"users": [
				{"id": 1, "profile": {"name": "Alice"}},
				{"id": 2, "profile": {"name": "Bob"}}
			]
		}
	}`)

	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
		},
	}
	fieldMapping := map[string]string{
		"id":   "id",
		"name": "profile.name",
	}

	records, err := jsonutils.ExtractRecords(payload, "response.users", fieldMapping, schema, "api_source")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	if records[0].Values[1].StringVal() != "Alice" {
		t.Errorf("expected Alice, got %s", records[0].Values[1].StringVal())
	}
}

func TestExtractPath_RootArray(t *testing.T) {
	payload := []byte(`[{"id": 10}, {"id": 20}]`)
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
		},
	}

	records, err := jsonutils.ExtractRecords(payload, "$", nil, schema, "api_source")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestExtractRecords_ErrorsAndEdgeCases(t *testing.T) {
	schema := domain.Schema{Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}}}

	t.Run("Invalid JSON returns parse error", func(t *testing.T) {
		_, err := jsonutils.ExtractRecords([]byte(`{bad json`), "data", nil, schema, "src")
		if err == nil {
			t.Error("expected error for invalid json, got nil")
		}
	})

	t.Run("Path pointing to non-array returns error", func(t *testing.T) {
		payload := []byte(`{"data": {"single_user": "not_an_array"}}`)
		_, err := jsonutils.ExtractRecords(payload, "data.single_user", nil, schema, "src")
		if err == nil {
			t.Error("expected error for non-array target, got nil")
		}
	})

	t.Run("Missing path returns nil without error", func(t *testing.T) {
		payload := []byte(`{"other": 123}`)
		records, err := jsonutils.ExtractRecords(payload, "missing.path", nil, schema, "src")
		if err != nil || records != nil {
			t.Errorf("expected nil records for missing path: %v, %v", records, err)
		}
	})

	t.Run("Array with non-map elements skips non-maps", func(t *testing.T) {
		payload := []byte(`[{"id":1}, "string_element", 12345, {"id":2}]`)
		records, err := jsonutils.ExtractRecords(payload, "$", nil, schema, "src")
		if err != nil || len(records) != 2 {
			t.Errorf("expected 2 records ignoring non-maps: %d, err: %v", len(records), err)
		}
	})
}
