package jsonutils

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// ExtractPath traverses a JSON object using dot-notation ("data.items.0.id").
func ExtractPath(body []byte, dotPath string) (any, error) {
	if strings.TrimSpace(dotPath) == "" || dotPath == "$" {
		var root any
		if err := json.Unmarshal(body, &root); err != nil {
			return nil, err
		}
		return root, nil
	}

	var root any
	if err := json.Unmarshal(body, &root); err != nil {
		return nil, fmt.Errorf("failed to parse json body: %w", err)
	}

	return GetNestedValue(root, strings.Split(dotPath, "."))
}

// GetNestedValue extracts a value from a nested map structure using path segments.
func GetNestedValue(current any, parts []string) (any, error) {
	if len(parts) == 0 || current == nil {
		return current, nil
	}

	head := parts[0]
	tail := parts[1:]

	switch val := current.(type) {
	case map[string]any:
		child, ok := val[head]
		if !ok {
			return nil, nil
		}
		return GetNestedValue(child, tail)
	case []any:
		return nil, fmt.Errorf("cannot index slice with key %s", head)
	default:
		return nil, fmt.Errorf("value at %s is neither map nor slice", head)
	}
}

// ExtractRecords extracts a list of GenericRecord from raw JSON bytes.
func ExtractRecords(
	body []byte,
	recordsPath string,
	fieldMapping map[string]string,
	schema domain.Schema,
	sourceID string,
) ([]*domain.GenericRecord, error) {
	rawNode, err := ExtractPath(body, recordsPath)
	if err != nil {
		return nil, fmt.Errorf("error navigating records_path '%s': %w", recordsPath, err)
	}
	if rawNode == nil {
		return nil, nil
	}

	sliceNode, ok := rawNode.([]any)
	if !ok {
		return nil, fmt.Errorf("records_path '%s' did not resolve to a JSON array", recordsPath)
	}

	results := make([]*domain.GenericRecord, 0, len(sliceNode))
	for _, item := range sliceNode {
		itemMap, isMap := item.(map[string]any)
		if !isMap {
			continue
		}

		rec := domain.NewGenericRecord(sourceID, len(schema.Fields))
		for i, field := range schema.Fields {
			lookupPath := field.Name
			if mappedPath, ok := fieldMapping[field.Name]; ok && mappedPath != "" {
				lookupPath = mappedPath
			}

			var val any
			if strings.Contains(lookupPath, ".") {
				val, _ = GetNestedValue(itemMap, strings.Split(lookupPath, "."))
			} else {
				val = itemMap[lookupPath]
			}

			if val != nil {
				rec.SetString(i, fmt.Sprintf("%v", val))
			} else {
				rec.SetNull(i, field.Type)
			}
		}
		results = append(results, rec)
	}

	return results, nil
}
