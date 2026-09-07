package parsers_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/adapters/streams/parsers"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func FuzzCSVParser(f *testing.F) {
	f.Add([]byte("id,name,val\n1,Alice,10.5\n2,Bob,20.0\n"))
	f.Add([]byte("corrupted,\"unclosed quote,3\n"))
	f.Add([]byte("\x00\xFF\xFE"))
	f.Add([]byte(""))

	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
			{Name: "val", Type: domain.TypeFloat64},
		},
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		p := parsers.NewCSVParser(",", true)
		recChan, errChan, err := p.Decode(context.Background(), bytes.NewReader(data), schema, "fuzz.csv")
		if err != nil {
			return
		}
		for range recChan {
			// drain
		}
		for range errChan {
			// drain
		}
	})
}
