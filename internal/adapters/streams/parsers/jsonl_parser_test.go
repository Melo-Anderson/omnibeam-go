package parsers_test

import (
	"context"
	"strings"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/parsers"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestJSONLParser_Decode(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeString},
			{Name: "amount", Type: domain.TypeString},
			{Name: "status", Type: domain.TypeString},
		},
	}

	t.Run("Valid JSONL lines and empty lines", func(t *testing.T) {
		jsonlData := "{\"id\":\"101\",\"amount\":45.50,\"status\":\"active\"}\n\n{\"id\":\"102\",\"status\":null}\n"
		parser := parsers.NewJSONLParser()
		records, errors, err := parser.Decode(context.Background(), strings.NewReader(jsonlData), &schema, "data.jsonl")
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		var parsed []*domain.GenericRecord
		for r := range records {
			parsed = append(parsed, r)
		}

		for e := range errors {
			if e != nil {
				t.Fatalf("unexpected error: %v", e)
			}
		}

		if len(parsed) != 2 {
			t.Fatalf("expected 2 parsed records, got %d", len(parsed))
		}
		if parsed[0].Values[0].StringVal() != "101" {
			t.Errorf("expected 101, got %s", parsed[0].Values[0].StringVal())
		}
		if !parsed[1].Values[1].IsNull {
			t.Errorf("expected amount in row 2 to be null")
		}
	})

	t.Run("Malformed JSON line emits error", func(t *testing.T) {
		badData := "{\"id\":101}\n{bad json}\n{\"id\":103}\n"
		parser := parsers.NewJSONLParser()
		records, errors, err := parser.Decode(context.Background(), strings.NewReader(badData), &schema, "bad.jsonl")
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		var parsed []*domain.GenericRecord
		for r := range records {
			parsed = append(parsed, r)
		}

		var errs []error
		for e := range errors {
			if e != nil {
				errs = append(errs, e)
			}
		}

		if len(parsed) != 2 {
			t.Errorf("expected 2 valid parsed records, got %d", len(parsed))
		}
		if len(errs) != 1 {
			t.Errorf("expected 1 error for malformed line, got %d", len(errs))
		}
	})

	t.Run("Context cancellation stops decoding", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		parser := parsers.NewJSONLParser()
		records, errors, _ := parser.Decode(ctx, strings.NewReader("{\"id\":\"1\"}\n"), &schema, "data.jsonl")

		for range records {
		}
		for range errors {
		}
	})

	t.Run("Raw JSON primitive extraction", func(t *testing.T) {
		typedSchema := &domain.Schema{
			Fields: []domain.Field{
				{Name: "id", Type: domain.TypeInt64},
				{Name: "amount", Type: domain.TypeDecimal, Scale: 2, OnOverflow: "round"},
				{Name: "active", Type: domain.TypeBool},
				{Name: "name", Type: domain.TypeString},
				{Name: "ts", Type: domain.TypeTimestamp},
			},
		}
		input := `{"id":42,"amount":"12.50","active":true,"name":"foo","ts":"2024-01-15T10:00:00Z"}`
		p := parsers.NewJSONLParser()
		recChan, errChan, err := p.Decode(context.Background(), strings.NewReader(input), typedSchema, "test.jsonl")
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}
		rec := <-recChan
		for range errChan {
		}

		if rec == nil {
			t.Fatal("expected record, got nil")
		}
		// id was parsed as JSON float64(42)
		if rec.Values[0].Type != domain.TypeFloat64 || rec.Values[0].Float64Val() != 42 {
			t.Errorf("id: got %+v", rec.Values[0])
		}
		// amount was parsed as JSON string "12.50"
		if rec.Values[1].Type != domain.TypeString || rec.Values[1].StringVal() != "12.50" {
			t.Errorf("amount: got %+v", rec.Values[1])
		}
		// active was parsed as JSON bool true
		if rec.Values[2].Type != domain.TypeBool || !rec.Values[2].BoolVal() {
			t.Errorf("active: got %+v", rec.Values[2])
		}
		// name was parsed as JSON string "foo"
		if rec.Values[3].Type != domain.TypeString || rec.Values[3].StringVal() != "foo" {
			t.Errorf("name: got %+v", rec.Values[3])
		}
		// ts was parsed as JSON string
		if rec.Values[4].Type != domain.TypeString || rec.Values[4].StringVal() != "2024-01-15T10:00:00Z" {
			t.Errorf("ts: got %+v", rec.Values[4])
		}
	})

	t.Run("Standard JSON Array parsing", func(t *testing.T) {
		jsonArrayData := `[
			{"id": "session-1", "amount": "100.00", "status": "active"},
			{"id": "session-2", "amount": "200.50", "status": "closed"}
		]`
		parser := parsers.NewJSONLParser()
		records, errors, err := parser.Decode(context.Background(), strings.NewReader(jsonArrayData), &schema, "sessions.json")
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}

		var parsed []*domain.GenericRecord
		for r := range records {
			parsed = append(parsed, r)
		}
		for e := range errors {
			if e != nil {
				t.Fatalf("unexpected error in array parsing: %v", e)
			}
		}

		if len(parsed) != 2 {
			t.Fatalf("expected 2 parsed records from array, got %d", len(parsed))
		}
		if parsed[0].Values[0].StringVal() != "session-1" {
			t.Errorf("expected session-1, got %s", parsed[0].Values[0].StringVal())
		}
		if parsed[1].Values[0].StringVal() != "session-2" {
			t.Errorf("expected session-2, got %s", parsed[1].Values[0].StringVal())
		}
	})

	t.Run("Deadlock resilience with more than 20 malformed lines", func(t *testing.T) {
		var badLines strings.Builder
		for i := 0; i < 50; i++ {
			badLines.WriteString("invalid json line not parsable\n")
		}
		badLines.WriteString("{\"id\":\"valid-end\",\"amount\":\"10.0\",\"status\":\"ok\"}\n")

		parser := parsers.NewJSONLParser()
		records, _, err := parser.Decode(context.Background(), strings.NewReader(badLines.String()), &schema, "stress.json")
		if err != nil {
			t.Fatalf("Decode failed: %v", err)
		}

		var count int
		for range records {
			count++
		}
		if count != 1 {
			t.Errorf("expected 1 valid record emitted, got %d", count)
		}
	})
}
