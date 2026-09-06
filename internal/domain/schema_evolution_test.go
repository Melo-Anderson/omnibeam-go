package domain_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestSchemaEvolution_AdditiveFieldCompatible(t *testing.T) {
	v1Bytes, err := os.ReadFile("../../testdata/golden/schema_v1.json")
	if err != nil {
		t.Fatalf("failed reading schema v1: %v", err)
	}
	v2Bytes, err := os.ReadFile("../../testdata/golden/schema_v2_evolved.json")
	if err != nil {
		t.Fatalf("failed reading schema v2: %v", err)
	}

	var s1, s2 domain.Schema
	if err := json.Unmarshal(v1Bytes, &s1); err != nil {
		t.Fatalf("failed unmarshaling s1: %v", err)
	}
	if err := json.Unmarshal(v2Bytes, &s2); err != nil {
		t.Fatalf("failed unmarshaling s2: %v", err)
	}

	// Build lookup indexes before calling FieldByName.
	s1.Index()
	s2.Index()

	// Invariant: All fields from v1 must exist in v2 with the same type.
	for _, f1 := range s1.Fields {
		f2, _, ok := s2.FieldByName(f1.Name)
		if !ok {
			t.Errorf("evolved schema missing field %q", f1.Name)
			continue
		}
		if f2.Type != f1.Type {
			t.Errorf("field %q type changed from %s to %s", f1.Name, f1.Type, f2.Type)
		}
	}

	// Invariant: Any new field added in v2 must be nullable (backward compatibility).
	for _, f2 := range s2.Fields {
		if _, _, ok := s1.FieldByName(f2.Name); !ok {
			if !f2.Nullable {
				t.Errorf("new field %q in v2 must be nullable for backward compatibility", f2.Name)
			}
		}
	}
}
