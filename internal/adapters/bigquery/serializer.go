package bigquery

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// SerializeRecordToJSONProto serializes a GenericRecord into JSON bytes
// suitable for the BigQuery Storage Write API self-describing proto stream.
func SerializeRecordToJSONProto(rec *domain.GenericRecord, schema *domain.Schema) ([]byte, error) {
	if rec == nil || schema == nil {
		return nil, fmt.Errorf("record and schema are required")
	}

	var buf bytes.Buffer
	buf.WriteByte('{')
	first := true

	for i, f := range schema.Fields {
		val := rec.Get(i)
		if val == nil || val.IsNull {
			if !f.Nullable {
				return nil, fmt.Errorf("non-nullable field %q is null", f.Name)
			}
			continue
		}

		if !first {
			buf.WriteByte(',')
		}
		first = false

		keyBytes, _ := json.Marshal(f.Name)
		buf.Write(keyBytes)
		buf.WriteByte(':')
		val.WriteJSON(&buf)
	}

	buf.WriteByte('}')
	return buf.Bytes(), nil
}
