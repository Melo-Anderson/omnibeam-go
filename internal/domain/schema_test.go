package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestSchema_FieldByName(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "customer_id", Type: domain.TypeString, Nullable: false},
			{Name: "amount", Type: domain.TypeFloat64, Nullable: true},
			{Name: "created_at", Type: domain.TypeTimestamp, Nullable: false},
			{Name: "is_active", Type: domain.TypeBool, Nullable: true},
		},
	}

	t.Run("Existing field returns correct index and metadata", func(t *testing.T) {
		field, idx, found := schema.FieldByName("amount")
		if !found {
			t.Fatalf("expected field 'amount' to be found")
		}
		if idx != 2 {
			t.Errorf("expected index 2, got %d", idx)
		}
		if field.Type != domain.TypeFloat64 {
			t.Errorf("expected TypeFloat64, got %s", field.Type)
		}
		if !field.Nullable {
			t.Errorf("expected Nullable to be true")
		}
	})

	t.Run("Non-existing field returns not found", func(t *testing.T) {
		_, _, found := schema.FieldByName("non_existent")
		if found {
			t.Errorf("expected field 'non_existent' to not be found")
		}
	})
}

func TestSchema_IndexAndFieldByName(t *testing.T) {
	s := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "amount", Type: domain.TypeDecimal, Scale: 2, OnOverflow: "round"},
			{Name: "name", Type: domain.TypeString},
		},
	}
	s.Index()

	f, idx, ok := s.FieldByName("amount")
	if !ok || idx != 1 || f.Type != domain.TypeDecimal || f.Scale != 2 || f.OnOverflow != "round" {
		t.Fatalf("expected amount field at idx 1, got %+v, idx=%d, ok=%v", f, idx, ok)
	}

	_, _, notFound := s.FieldByName("non_existent")
	if notFound {
		t.Error("expected non_existent field to not be found")
	}
}

func TestSchema_PolymorphicJSONUnmarshal(t *testing.T) {
	// 1. Direct array JSON format (like in dataflow_compute_specification.md)
	arrayJSON := []byte(`[
		{"name": "id", "type": "int64", "nullable": false},
		{"name": "amount", "type": "decimal", "scale": 2, "on_overflow": "round", "nullable": true}
	]`)
	var s1 domain.Schema
	if err := json.Unmarshal(arrayJSON, &s1); err != nil {
		t.Fatalf("unmarshal array schema: %v", err)
	}
	if len(s1.Fields) != 2 || s1.Fields[1].Scale != 2 || s1.Fields[1].OnOverflow != "round" {
		t.Fatalf("unexpected fields in s1: %+v", s1)
	}

	// 2. Object format {"fields": [...]}
	objJSON := []byte(`{"fields": [
		{"name": "id", "type": "int64", "nullable": false}
	]}`)
	var s2 domain.Schema
	if err := json.Unmarshal(objJSON, &s2); err != nil {
		t.Fatalf("unmarshal object schema: %v", err)
	}
	if len(s2.Fields) != 1 {
		t.Fatalf("unexpected fields in s2: %+v", s2)
	}
}

func TestSchema_ApplyDefaultsAndValidate(t *testing.T) {
	s := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "amount", Type: domain.TypeDecimal, Scale: 2},
		},
	}
	s.ApplyDefaults()
	if s.Fields[1].OnOverflow != "round" {
		t.Errorf("expected default OnOverflow='round', got %q", s.Fields[1].OnOverflow)
	}

	if err := s.Validate(); err != nil {
		t.Fatalf("expected valid schema, got %v", err)
	}

	// Negative scale
	sBadScale := domain.Schema{
		Fields: []domain.Field{
			{Name: "amount", Type: domain.TypeDecimal, Scale: -1},
		},
	}
	if err := sBadScale.Validate(); err == nil {
		t.Error("expected error for negative scale, got nil")
	}

	// Bad OnOverflow
	sBadOverflow := domain.Schema{
		Fields: []domain.Field{
			{Name: "amount", Type: domain.TypeDecimal, Scale: 2, OnOverflow: "invalid"},
		},
	}
	if err := sBadOverflow.Validate(); err == nil {
		t.Error("expected error for invalid on_overflow, got nil")
	}
}

