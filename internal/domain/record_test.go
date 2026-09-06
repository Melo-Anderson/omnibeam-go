package domain_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestGenericRecord_GetAndSet(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
			{Name: "amount", Type: domain.TypeFloat64},
			{Name: "is_active", Type: domain.TypeBool},
			{Name: "created_at", Type: domain.TypeTimestamp},
			{Name: "payload", Type: domain.TypeBytes},
			{Name: "price", Type: domain.TypeDecimal, Scale: 2},
		},
	}

	rec := domain.NewGenericRecord("orders_schema", len(schema.Fields))
	rec.SetInt64(0, 1001)
	rec.SetString(1, "Alice")
	rec.SetFloat64(2, 99.50)
	rec.SetBool(3, true)
	now := time.Now().UTC()
	rec.SetTimestamp(4, now)
	rec.SetBytes(5, []byte("raw_data"))
	rec.SetDecimal(6, 12550, 2)
	rec.AuditFields["_source_file"] = "orders.csv"
	rec.AuditFields["_ingested_at"] = now.Format(time.RFC3339)

	if rec.Values[0].Int64Val() != 1001 {
		t.Errorf("expected 1001, got %d", rec.Values[0].Int64Val())
	}
	if rec.Values[1].StringVal() != "Alice" {
		t.Errorf("expected 'Alice', got %s", rec.Values[1].StringVal())
	}
	if rec.Values[2].Float64Val() != 99.50 {
		t.Errorf("expected 99.50, got %f", rec.Values[2].Float64Val())
	}
	if !rec.Values[3].BoolVal() {
		t.Errorf("expected true, got false")
	}
	if !rec.Values[4].TimeVal().Equal(now) {
		t.Errorf("expected %v, got %v", now, rec.Values[4].TimeVal())
	}
	if string(rec.Values[5].BytesVal()) != "raw_data" {
		t.Errorf("expected 'raw_data', got %s", string(rec.Values[5].BytesVal()))
	}
	m, s := rec.Values[6].DecimalVal()
	if m != 12550 || s != 2 {
		t.Errorf("expected (12550, 2), got (%d, %d)", m, s)
	}
	if rec.AuditFields["_source_file"] != "orders.csv" {
		t.Errorf("expected _source_file 'orders.csv', got %s", rec.AuditFields["_source_file"])
	}
}

func TestGenericRecord_NullValues(t *testing.T) {
	rec := domain.NewGenericRecord("test_schema", 1)
	rec.SetNull(0, domain.TypeString)

	if !rec.Values[0].IsNull {
		t.Errorf("expected IsNull to be true")
	}
	if rec.Values[0].Type != domain.TypeString {
		t.Errorf("expected TypeString, got %s", rec.Values[0].Type)
	}
}

func TestFieldValue_String(t *testing.T) {
	tests := []struct {
		name     string
		build    func() domain.FieldValue
		expected string
	}{
		{name: "Null value", build: func() domain.FieldValue { return domain.FieldValue{IsNull: true, Type: domain.TypeString} }, expected: ""},
		{name: "String value", build: func() domain.FieldValue {
			r := domain.NewGenericRecord("s", 1)
			r.SetString(0, "hello")
			return *r.Get(0)
		}, expected: "hello"},
		{name: "Int64 value", build: func() domain.FieldValue { r := domain.NewGenericRecord("s", 1); r.SetInt64(0, 42); return *r.Get(0) }, expected: "42"},
		{name: "Float64 value", build: func() domain.FieldValue {
			r := domain.NewGenericRecord("s", 1)
			r.SetFloat64(0, 3.14)
			return *r.Get(0)
		}, expected: "3.14"},
		{name: "Bool value true", build: func() domain.FieldValue { r := domain.NewGenericRecord("s", 1); r.SetBool(0, true); return *r.Get(0) }, expected: "true"},
		{name: "Bool value false", build: func() domain.FieldValue { r := domain.NewGenericRecord("s", 1); r.SetBool(0, false); return *r.Get(0) }, expected: "false"},
		{name: "Decimal value", build: func() domain.FieldValue {
			r := domain.NewGenericRecord("s", 1)
			r.SetDecimal(0, 14999, 2)
			return *r.Get(0)
		}, expected: "149.99"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val := tt.build()
			if got := val.String(); got != tt.expected {
				t.Errorf("val.String() = %q, expected %q", got, tt.expected)
			}
		})
	}
}

func TestGenericRecord_ToMap(t *testing.T) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
			{Name: "optional", Type: domain.TypeString, Nullable: true},
		},
	}

	rec := domain.NewGenericRecord("test_schema", len(schema.Fields))
	rec.SetInt64(0, 42)
	rec.SetString(1, "Bob")
	rec.SetNull(2, domain.TypeString)

	m := rec.ToMap(schema)
	if m == nil {
		t.Fatal("expected non-nil map")
	}
	if m["id"] != int64(42) {
		t.Errorf("expected id 42, got %v", m["id"])
	}
	if m["name"] != "Bob" {
		t.Errorf("expected name 'Bob', got %v", m["name"])
	}
	if m["optional"] != nil {
		t.Errorf("expected optional to be nil, got %v", m["optional"])
	}

	// Nil safety
	if rec.ToMap(nil) != nil {
		t.Errorf("expected nil map on nil schema")
	}
}

func TestFieldValue_Interface_And_WriteJSON(t *testing.T) {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	rec := domain.NewGenericRecord("test", 7)
	rec.SetInt64(0, 100)
	rec.SetString(1, "Text")
	rec.SetFloat64(2, 50.25)
	rec.SetBool(3, true)
	rec.SetTimestamp(4, now)
	rec.SetBytes(5, []byte("blob"))
	rec.SetDecimal(6, 12345, 2)

	// Interface() calls
	if rec.Values[0].Interface() != int64(100) {
		t.Errorf("expected 100 from Interface()")
	}
	if rec.Values[1].Interface() != "Text" {
		t.Errorf("expected Text from Interface()")
	}
	if rec.Values[2].Interface() != 50.25 {
		t.Errorf("expected 50.25 from Interface()")
	}
	if rec.Values[3].Interface() != true {
		t.Errorf("expected true from Interface()")
	}
	if !rec.Values[4].Interface().(time.Time).Equal(now) {
		t.Errorf("expected timestamp from Interface()")
	}
	if string(rec.Values[5].Interface().([]byte)) != "blob" {
		t.Errorf("expected blob from Interface()")
	}
	if rec.Values[6].Interface() != "123.45" {
		t.Errorf("expected 123.45 from decimal Interface()")
	}

	nullVal := domain.FieldValue{IsNull: true}
	if nullVal.Interface() != nil {
		t.Errorf("expected nil for null value Interface()")
	}

	// WriteJSON on FieldValue for all types
	var buf bytes.Buffer
	for i, expected := range []string{"100", `"Text"`, "50.25", "true", `"` + now.Format(time.RFC3339) + `"`, `"YmxvYg=="`, "123.45"} {
		buf.Reset()
		rec.Values[i].WriteJSON(&buf)
		if buf.String() != expected {
			t.Errorf("field %d WriteJSON got %s, expected %s", i, buf.String(), expected)
		}
	}
	buf.Reset()
	nullVal.WriteJSON(&buf)
	if buf.String() != "null" {
		t.Errorf("expected null from null FieldValue WriteJSON, got %s", buf.String())
	}

	// String() on all types
	if rec.Values[4].String() != now.Format(time.RFC3339) {
		t.Errorf("expected RFC3339 timestamp from String()")
	}
	if rec.Values[5].String() != "blob" {
		t.Errorf("expected 'blob' from String()")
	}

	// Get() method
	if rec.Get(0).Int64Val() != 100 {
		t.Errorf("expected 100 from rec.Get(0)")
	}
}

func TestFieldValue_Decimal(t *testing.T) {
	rec := domain.NewGenericRecord("test_schema", 2)
	rec.SetDecimal(0, 12345, 2) // 123.45
	rec.SetNull(1, domain.TypeDecimal)

	fv := rec.Get(0)
	if fv == nil || fv.Type != domain.TypeDecimal {
		t.Fatalf("expected TypeDecimal, got %v", fv)
	}
	if fv.String() != "123.45" {
		t.Errorf("expected '123.45', got %q", fv.String())
	}
	mantissa, scale := fv.DecimalVal()
	if mantissa != 12345 || scale != 2 {
		t.Errorf("expected 12345, scale 2, got %d, %d", mantissa, scale)
	}

	var buf bytes.Buffer
	fv.WriteJSON(&buf)
	if buf.String() != "123.45" {
		t.Errorf("expected JSON '123.45', got %q", buf.String())
	}

	nullFv := rec.Get(1)
	if !nullFv.IsNull || nullFv.String() != "" {
		t.Errorf("expected null decimal, got %v", nullFv)
	}
}

func TestFormatDecimal_ZeroPaddingAndNegative(t *testing.T) {
	tests := []struct {
		mantissa int64
		scale    int32
		want     string
	}{
		{5, 2, "0.05"},
		{50, 2, "0.50"},
		{500, 2, "5.00"},
		{-5, 2, "-0.05"},
		{-50, 2, "-0.50"},
		{-500, 2, "-5.00"},
		{0, 2, "0.00"},
		{12345, 0, "12345"},
		{-9223372036854775808, 2, "-92233720368547758.08"},
	}
	for _, tc := range tests {
		got := domain.FormatDecimal(tc.mantissa, tc.scale)
		if got != tc.want {
			t.Errorf("FormatDecimal(%d, %d) = %q, want %q", tc.mantissa, tc.scale, got, tc.want)
		}
	}
}
