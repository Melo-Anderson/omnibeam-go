package formatters

import (
	"encoding/csv"
	"errors"
	"strings"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var _ ports.RecordFormatter = (*CSVFormatter)(nil)

type CSVFormatter struct {
	delimiter      rune
	includeHeader  bool
	lineTerminator string
}

func NewCSVFormatter(opts domain.FormatOptions) *CSVFormatter {
	delim := ','
	if len(opts.Delimiter) > 0 {
		delim = rune(opts.Delimiter[0])
	}
	term := "\n"
	if opts.LineTerminator != "" {
		term = opts.LineTerminator
	}
	return &CSVFormatter{
		delimiter:      delim,
		includeHeader:  opts.IncludeHeader,
		lineTerminator: term,
	}
}

func (f *CSVFormatter) FormatHeader(schema *domain.Schema) ([]byte, error) {
	if !f.includeHeader || schema == nil || len(schema.Fields) == 0 {
		return nil, nil
	}
	names := make([]string, len(schema.Fields))
	for i, field := range schema.Fields {
		names[i] = field.Name
	}
	return f.formatRow(names)
}

func (f *CSVFormatter) FormatRecord(rec *domain.GenericRecord, schema *domain.Schema) ([]byte, error) {
	if rec == nil || schema == nil {
		return nil, errors.New("record and schema are required")
	}
	values := make([]string, len(schema.Fields))
	for i := 0; i < len(schema.Fields); i++ {
		val := rec.Get(i)
		if val == nil || val.IsNull {
			values[i] = ""
		} else {
			values[i] = val.String()
		}
	}
	return f.formatRow(values)
}

func (f *CSVFormatter) formatRow(cols []string) ([]byte, error) {
	buf := GetBuffer()
	defer PutBuffer(buf)

	w := csv.NewWriter(buf)
	w.Comma = f.delimiter
	if err := w.Write(cols); err != nil {
		return nil, err
	}
	w.Flush()
	res := strings.TrimRight(buf.String(), "\r\n") + f.lineTerminator
	return []byte(res), nil
}
