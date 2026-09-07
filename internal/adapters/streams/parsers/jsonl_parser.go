package parsers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var _ ports.StreamDecoder = (*JSONLParser)(nil)

type JSONLParser struct{}

func NewJSONLParser() *JSONLParser {
	return &JSONLParser{}
}

func (p *JSONLParser) Decode(ctx context.Context, r io.Reader, schema *domain.Schema, sourceFile string) (<-chan *domain.GenericRecord, <-chan error, error) {
	recChan := make(chan *domain.GenericRecord, 100)
	errChan := make(chan error, 10)

	br := bufio.NewReaderSize(r, 64*1024)

	go func() {
		defer close(recChan)
		defer close(errChan)

		// Peek first non-whitespace byte to determine if it's a JSON array `[` or JSON lines `{`
		var firstByte byte
		for {
			b, err := br.ReadByte()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					select {
					case errChan <- fmt.Errorf("read error in %s: %w", sourceFile, err):
					default:
					}
				}
				return
			}
			if b != ' ' && b != '\t' && b != '\r' && b != '\n' {
				firstByte = b
				_ = br.UnreadByte()
				break
			}
		}

		if firstByte == '[' {
			// Parse JSON Array
			dec := json.NewDecoder(br)
			t, err := dec.Token()
			if err != nil {
				select {
				case errChan <- fmt.Errorf("invalid json array start in %s: %w", sourceFile, err):
				default:
				}
				return
			}
			if delim, ok := t.(json.Delim); !ok || delim != '[' {
				select {
				case errChan <- fmt.Errorf("expected json array start in %s, got %v", sourceFile, t):
				default:
				}
				return
			}

			for dec.More() {
				select {
				case <-ctx.Done():
					select {
					case errChan <- ctx.Err():
					default:
					}
					return
				default:
				}

				var rowMap map[string]any
				if err := dec.Decode(&rowMap); err != nil {
					select {
					case errChan <- fmt.Errorf("json decode error in %s: %w", sourceFile, err):
					default:
					}
					continue
				}

				rec := mapToGenericRecord(rowMap, schema, sourceFile)
				select {
				case recChan <- rec:
				case <-ctx.Done():
					return
				}
			}
			return
		}

		// Otherwise parse as JSON Lines (one JSON object per line)
		scanner := bufio.NewScanner(br)
		scanner.Buffer(make([]byte, 1024*1024), 64*1024*1024)

		for scanner.Scan() {
			select {
			case <-ctx.Done():
				select {
				case errChan <- ctx.Err():
				default:
				}
				return
			default:
			}

			line := bytes.TrimSpace(scanner.Bytes())
			if len(line) == 0 {
				continue
			}

			var rowMap map[string]any
			if err := json.Unmarshal(line, &rowMap); err != nil {
				select {
				case errChan <- fmt.Errorf("json parse error in %s: %w", sourceFile, err):
				default:
				}
				continue
			}

			rec := mapToGenericRecord(rowMap, schema, sourceFile)
			select {
			case recChan <- rec:
			case <-ctx.Done():
				return
			}
		}

		if err := scanner.Err(); err != nil {
			select {
			case errChan <- fmt.Errorf("scanner error in %s: %w", sourceFile, err):
			default:
			}
		}
	}()

	return recChan, errChan, nil
}

func mapToGenericRecord(rowMap map[string]any, schema *domain.Schema, sourceFile string) *domain.GenericRecord {
	if schema == nil || len(schema.Fields) == 0 {
		rec := domain.NewGenericRecord(sourceFile, 0)
		rec.AuditFields["_source_file"] = sourceFile
		return rec
	}

	rec := domain.NewGenericRecord(sourceFile, len(schema.Fields))
	for i, field := range schema.Fields {
		val, exists := rowMap[field.Name]
		if !exists || val == nil {
			rec.SetNull(i, field.Type)
			continue
		}

		switch v := val.(type) {
		case string:
			rec.SetString(i, v)
		case float64:
			rec.SetFloat64(i, v)
		case bool:
			rec.SetBool(i, v)
		default:
			rec.SetString(i, fmt.Sprintf("%v", v))
		}
	}
	rec.AuditFields["_source_file"] = sourceFile
	return rec
}
