package parsers

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var _ ports.StreamDecoder = (*CSVParser)(nil)

type CSVParser struct {
	Delimiter string
	HasHeader bool
}

func NewCSVParser(delimiter string, hasHeader bool) *CSVParser {
	if delimiter == "" {
		delimiter = ","
	}
	return &CSVParser{
		Delimiter: delimiter,
		HasHeader: hasHeader,
	}
}

func (p *CSVParser) Decode(ctx context.Context, r io.Reader, schema *domain.Schema, sourceFile string) (<-chan *domain.GenericRecord, <-chan error, error) {
	reader := csv.NewReader(r)
	if len(p.Delimiter) > 0 {
		reader.Comma = rune(p.Delimiter[0])
	}
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true

	recChan := make(chan *domain.GenericRecord, 100)
	errChan := make(chan error, 10)

	go func() {
		defer close(recChan)
		defer close(errChan)

		isFirstRow := true
		for {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			row, err := reader.Read()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				errChan <- fmt.Errorf("csv read error in %s: %w", sourceFile, err)
				return
			}

			if isFirstRow && p.HasHeader {
				isFirstRow = false
				if len(row) > 0 && schema != nil && len(schema.Fields) > 0 && strings.EqualFold(strings.TrimSpace(row[0]), schema.Fields[0].Name) {
					continue
				}
			}
			isFirstRow = false

			rec := domain.NewGenericRecord(sourceFile, len(schema.Fields))
			for i := 0; i < len(schema.Fields); i++ {
				f := schema.Fields[i]
				if i >= len(row) {
					rec.SetNull(i, f.Type)
					continue
				}
				str := strings.TrimSpace(row[i])
				if str == "" {
					rec.SetNull(i, f.Type)
					continue
				}
				rec.SetString(i, str)
			}
			rec.AuditFields["_source_file"] = sourceFile
			recChan <- rec
		}
	}()

	return recChan, errChan, nil
}
