// Package domain defines the core data types, entities, value objects,
// and validation rules for the dataflow compute engine.
package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type DataType string

const (
	TypeString    DataType = "string"
	TypeInt64     DataType = "int64"
	TypeFloat64   DataType = "float64"
	TypeBool      DataType = "bool"
	TypeTimestamp DataType = "timestamp"
	TypeBytes     DataType = "bytes"
	TypeDate      DataType = "date"
	TypeDecimal   DataType = "decimal"
	TypeJSON      DataType = "json"
)

type Field struct {
	Name       string   `json:"name"`
	Type       DataType `json:"type"`
	Nullable   bool     `json:"nullable"`
	Scale      int32    `json:"scale,omitempty"`
	OnOverflow string   `json:"on_overflow,omitempty"` // "round" (default) or "fail"
}

type Schema struct {
	Fields  []Field        `json:"fields"`
	nameIdx map[string]int `json:"-"`
}

func (s *Schema) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) > 0 && data[0] == '[' {
		return json.Unmarshal(data, &s.Fields)
	}
	type Alias Schema
	var a Alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	s.Fields = a.Fields
	return nil
}

func (s *Schema) Index() {
	if s == nil || len(s.Fields) == 0 {
		return
	}
	s.nameIdx = make(map[string]int, len(s.Fields))
	for i, f := range s.Fields {
		s.nameIdx[f.Name] = i
	}
}

func (s *Schema) ApplyDefaults() {
	if s == nil {
		return
	}
	for i := range s.Fields {
		if s.Fields[i].Type == TypeDecimal {
			if s.Fields[i].OnOverflow == "" {
				s.Fields[i].OnOverflow = "round"
			}
		}
	}
}

func (s *Schema) Validate() error {
	if s == nil || len(s.Fields) == 0 {
		return errors.New("schema must contain at least one field")
	}
	for _, f := range s.Fields {
		if strings.TrimSpace(f.Name) == "" {
			return errors.New("field name cannot be empty")
		}
		if f.Type == TypeDecimal {
			if f.Scale < 0 {
				return fmt.Errorf("field %q: scale cannot be negative, got %d", f.Name, f.Scale)
			}
			if f.OnOverflow != "" && f.OnOverflow != "round" && f.OnOverflow != "fail" {
				return fmt.Errorf("field %q: unsupported on_overflow %q (must be 'round' or 'fail')", f.Name, f.OnOverflow)
			}
		}
	}
	return nil
}

func (s *Schema) FieldByName(name string) (Field, int, bool) {
	if s == nil {
		return Field{}, -1, false
	}
	if s.nameIdx != nil {
		if idx, ok := s.nameIdx[name]; ok {
			return s.Fields[idx], idx, true
		}
		return Field{}, -1, false
	}
	for i, f := range s.Fields {
		if f.Name == name {
			return f, i, true
		}
	}
	return Field{}, -1, false
}
