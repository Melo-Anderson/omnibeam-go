package formatters

import (
	"bytes"
	"errors"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

var _ ports.RecordFormatter = (*JSONLFormatter)(nil)

type JSONLFormatter struct{}

func NewJSONLFormatter() *JSONLFormatter {
	return &JSONLFormatter{}
}

func (f *JSONLFormatter) FormatHeader(_ *domain.Schema) ([]byte, error) {
	return nil, nil
}

func (f *JSONLFormatter) FormatRecord(rec *domain.GenericRecord, schema *domain.Schema) ([]byte, error) {
	if rec == nil || schema == nil {
		return nil, errors.New("record and schema are required")
	}

	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, field := range schema.Fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.WriteByte('"')
		buf.WriteString(field.Name)
		buf.WriteString(`":`)

		val := rec.Get(i)
		if val == nil || val.IsNull {
			buf.WriteString("null")
		} else {
			val.WriteJSON(&buf)
		}
	}
	buf.WriteString("}\n")
	return buf.Bytes(), nil
}
