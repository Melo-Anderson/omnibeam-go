package parsers_test

import (
	"context"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/parsers"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestCSVParser_Parse(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeString},
			{Name: "name", Type: domain.TypeString},
			{Name: "amount", Type: domain.TypeString},
		},
	}

	t.Run("Standard comma delimited with header", func(t *testing.T) {
		csvData := "id,name,amount\n1,Alice,10.50\n2,Bob,20.00\n"
		parser := parsers.NewCSVParser(",", true)
		records, errors, err := parser.Decode(context.Background(), strings.NewReader(csvData), &schema, "test.csv")
		if err != nil {
			t.Fatalf("unexpected error starting decode: %v", err)
		}

		var parsed []*domain.GenericRecord
		for rec := range records {
			parsed = append(parsed, rec)
		}

		for parseErr := range errors {
			if parseErr != nil {
				t.Fatalf("unexpected parsing error: %v", parseErr)
			}
		}

		if len(parsed) != 2 {
			t.Fatalf("expected 2 records, got %d", len(parsed))
		}
		if parsed[0].Values[1].StringVal() != "Alice" {
			t.Errorf("expected 'Alice', got %s", parsed[0].Values[1].StringVal())
		}
		if parsed[1].Values[0].StringVal() != "2" {
			t.Errorf("expected '2', got %s", parsed[1].Values[0].StringVal())
		}
	})

	t.Run("Semicolon delimiter without header", func(t *testing.T) {
		csvData := "10;Charlie;50.00\n11;David;60.00\n"
		parser := parsers.NewCSVParser(";", false)
		records, errors, err := parser.Decode(context.Background(), strings.NewReader(csvData), &schema, "test.csv")
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		var parsed []*domain.GenericRecord
		for r := range records {
			parsed = append(parsed, r)
		}
		for e := range errors {
			if e != nil {
				t.Fatalf("error: %v", e)
			}
		}

		if len(parsed) != 2 {
			t.Fatalf("expected 2 records, got %d", len(parsed))
		}
		if parsed[0].Values[1].StringVal() != "Charlie" {
			t.Errorf("expected 'Charlie', got %s", parsed[0].Values[1].StringVal())
		}
	})

	t.Run("Default delimiter when empty string provided", func(t *testing.T) {
		parser := parsers.NewCSVParser("", true)
		if parser.Delimiter != "," {
			t.Errorf("expected default comma delimiter, got %q", parser.Delimiter)
		}
	})

	t.Run("Row with missing columns padded with nulls", func(t *testing.T) {
		csvData := "id,name,amount\n100,Eve\n"
		parser := parsers.NewCSVParser(",", true)
		records, _, _ := parser.Decode(context.Background(), strings.NewReader(csvData), &schema, "test.csv")

		var parsed []*domain.GenericRecord
		for r := range records {
			parsed = append(parsed, r)
		}
		if len(parsed) != 1 {
			t.Fatalf("expected 1 record, got %d", len(parsed))
		}
		if !parsed[0].Values[2].IsNull {
			t.Errorf("expected amount column to be null when missing from row")
		}
	})

	t.Run("Context cancellation stops parsing", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		parser := parsers.NewCSVParser(",", false)
		records, errors, _ := parser.Decode(ctx, strings.NewReader("1,2,3\n"), &schema, "test.csv")
		for range records {
		}
		for range errors {
		}
	})

	t.Run("Raw string ingestion preserving raw text", func(t *testing.T) {
		typedSchema := &domain.Schema{
			Fields: []domain.Field{
				{Name: "id", Type: domain.TypeInt64},
				{Name: "price", Type: domain.TypeDecimal, Scale: 2, OnOverflow: "round"},
				{Name: "active", Type: domain.TypeBool},
				{Name: "name", Type: domain.TypeString},
			},
		}
		input := "1,19.99,true,foo\n"
		p := parsers.NewCSVParser(",", false)
		recChan, errChan, err := p.Decode(context.Background(), strings.NewReader(input), typedSchema, "test.csv")
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		rec := <-recChan
		for range errChan {
		}

		if rec == nil {
			t.Fatal("expected record, got nil")
		}
		if rec.Values[0].StringVal() != "1" {
			t.Errorf("id: expected '1', got %s", rec.Values[0].StringVal())
		}
		if rec.Values[1].StringVal() != "19.99" {
			t.Errorf("price: expected '19.99', got %s", rec.Values[1].StringVal())
		}
		if rec.Values[2].StringVal() != "true" {
			t.Errorf("active: expected 'true', got %s", rec.Values[2].StringVal())
		}
		if rec.Values[3].StringVal() != "foo" {
			t.Errorf("name: expected 'foo', got %s", rec.Values[3].StringVal())
		}
	})
}
