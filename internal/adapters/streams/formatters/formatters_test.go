package formatters

import (
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestCSVFormatter(t *testing.T) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
			{Name: "amount", Type: "float64"},
		},
	}

	opts := domain.FormatOptions{
		Delimiter:      ";",
		IncludeHeader:  true,
		LineTerminator: "\r\n",
	}
	formatter := NewCSVFormatter(opts)

	header, err := formatter.FormatHeader(schema)
	if err != nil {
		t.Fatalf("failed formatting header: %v", err)
	}
	expectedHeader := "id;name;amount\r\n"
	if string(header) != expectedHeader {
		t.Errorf("expected %q, got %q", expectedHeader, string(header))
	}

	rec := domain.NewGenericRecord("src", 3)
	rec.SetInt64(0, 100)
	rec.SetString(1, "Alice")
	rec.SetFloat64(2, 50.5)

	rowBytes, err := formatter.FormatRecord(rec, schema)
	if err != nil {
		t.Fatalf("failed formatting record: %v", err)
	}
	expectedRow := "100;Alice;50.5\r\n"
	if string(rowBytes) != expectedRow {
		t.Errorf("expected %q, got %q", expectedRow, string(rowBytes))
	}

	// Fail-fast test
	if _, err := formatter.FormatRecord(nil, schema); err == nil {
		t.Errorf("expected error on nil record, got nil")
	}
	if _, err := formatter.FormatRecord(rec, nil); err == nil {
		t.Errorf("expected error on nil schema, got nil")
	}

	// FormatHeader edge cases
	noHeaderFormatter := NewCSVFormatter(domain.FormatOptions{IncludeHeader: false})
	h, err := noHeaderFormatter.FormatHeader(schema)
	if err != nil || h != nil {
		t.Errorf("expected nil header when includeHeader=false: %v, %v", h, err)
	}
	h, err = formatter.FormatHeader(nil)
	if err != nil || h != nil {
		t.Errorf("expected nil header for nil schema: %v, %v", h, err)
	}
	h, err = formatter.FormatHeader(&domain.Schema{})
	if err != nil || h != nil {
		t.Errorf("expected nil header for empty fields: %v, %v", h, err)
	}

	// Short record padded with empty strings
	shortRec := domain.NewGenericRecord("src", 1)
	shortRec.SetInt64(0, 5)
	shortRow, err := formatter.FormatRecord(shortRec, schema)
	if err != nil {
		t.Fatalf("FormatRecord on short record: %v", err)
	}
	if string(shortRow) != "5;;\r\n" {
		t.Errorf("expected '5;;\\r\\n', got %q", string(shortRow))
	}
}

func TestJSONLFormatter(t *testing.T) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: "int64"},
			{Name: "name", Type: "string"},
		},
	}

	formatter := NewJSONLFormatter()

	header, err := formatter.FormatHeader(schema)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if header != nil {
		t.Errorf("expected nil header for JSONL, got %q", string(header))
	}

	rec := domain.NewGenericRecord("src", 2)
	rec.SetInt64(0, 42)
	rec.SetString(1, "Bob")

	rowBytes, err := formatter.FormatRecord(rec, schema)
	if err != nil {
		t.Fatalf("failed formatting record: %v", err)
	}
	expectedJSON := "{\"id\":42,\"name\":\"Bob\"}\n"
	if string(rowBytes) != expectedJSON {
		t.Errorf("expected %q, got %q", expectedJSON, string(rowBytes))
	}

	// Null value serialization test
	recWithNull := domain.NewGenericRecord("src", 2)
	recWithNull.SetInt64(0, 42)
	recWithNull.SetNull(1, domain.TypeString)

	nullBytes, err := formatter.FormatRecord(recWithNull, schema)
	if err != nil {
		t.Fatalf("failed formatting record with null: %v", err)
	}
	expectedNullJSON := "{\"id\":42,\"name\":null}\n"
	if string(nullBytes) != expectedNullJSON {
		t.Errorf("expected %q, got %q", expectedNullJSON, string(nullBytes))
	}

	// Fail-fast test
	if _, err := formatter.FormatRecord(nil, schema); err == nil {
		t.Errorf("expected error on nil record, got nil")
	}
}

func TestBuildFormatter_Registry(t *testing.T) {
	t.Run("Resolves csv formatter", func(t *testing.T) {
		f, err := BuildFormatter("csv", domain.FormatOptions{Delimiter: ","})
		if err != nil || f == nil {
			t.Fatalf("expected csv formatter, got: %v, %v", f, err)
		}
	})

	t.Run("Resolves jsonl formatter", func(t *testing.T) {
		f, err := BuildFormatter("jsonl", domain.FormatOptions{})
		if err != nil || f == nil {
			t.Fatalf("expected jsonl formatter, got: %v, %v", f, err)
		}
	})

	t.Run("Resolves empty format to jsonl fallback", func(t *testing.T) {
		f, err := BuildFormatter("", domain.FormatOptions{})
		if err != nil || f == nil {
			t.Fatalf("expected jsonl formatter for empty format, got: %v, %v", f, err)
		}
	})

	t.Run("Unregistered format returns error", func(t *testing.T) {
		_, err := BuildFormatter("unknown_fmt_xyz", domain.FormatOptions{})
		if err == nil {
			t.Fatal("expected error for unregistered format, got nil")
		}
	})

	t.Run("SupportedFormats returns non-empty list", func(t *testing.T) {
		formats := SupportedFormats()
		if len(formats) < 4 {
			t.Errorf("expected at least 4 formats, got: %v", formats)
		}
	})
}
