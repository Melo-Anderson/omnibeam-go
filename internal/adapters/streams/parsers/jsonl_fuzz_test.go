package parsers_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/parsers"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func FuzzJSONLParser(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"id": 1, "name": "Alice"}`),
		[]byte(`{"id": 2, "name": null}`),
		[]byte(`{"id": "invalid"`),
		[]byte(``),
		[]byte(`   {"unclosed": "brace`),
		[]byte("\x00\x01\x02"),
	}
	for _, seed := range seeds {
		f.Add(seed)
	}
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: true},
			{Name: "name", Type: domain.TypeString, Nullable: true},
		},
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		// Invariant: must never panic, must drain all channels to completion.
		parser := parsers.NewJSONLParser()
		recChan, errChan, err := parser.Decode(context.Background(), bytes.NewReader(data), schema, "fuzz.jsonl")
		if err != nil {
			return
		}
		for recChan != nil || errChan != nil {
			select {
			case _, ok := <-recChan:
				if !ok {
					recChan = nil
				}
			case _, ok := <-errChan:
				if !ok {
					errChan = nil
				}
			}
		}
	})
}
