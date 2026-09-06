// Package domain defines the core data types, entities, value objects,
// and validation rules for the dataflow compute engine.
package domain

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/omnibeam/dataflow-compute-go/pkg/intern"
)

type FieldValue struct {
	Type   DataType `json:"type"`
	IsNull bool     `json:"is_null"`
	Scale  int32    `json:"scale,omitempty"`
	NumVal uint64   `json:"num_val,omitempty"`
	StrVal string   `json:"str_val,omitempty"`
}

func (f FieldValue) Int64Val() int64 {
	return int64(f.NumVal)
}

func (f FieldValue) Float64Val() float64 {
	return math.Float64frombits(f.NumVal)
}

func (f FieldValue) BoolVal() bool {
	return f.NumVal == 1
}

func (f FieldValue) TimeVal() time.Time {
	return time.Unix(0, int64(f.NumVal)).UTC()
}

func (f FieldValue) StringVal() string {
	return f.StrVal
}

func (f FieldValue) BytesVal() []byte {
	return []byte(f.StrVal)
}

func (f FieldValue) DecimalVal() (int64, int32) {
	return int64(f.NumVal), f.Scale
}

func (f FieldValue) String() string {
	if f.IsNull {
		return ""
	}
	switch f.Type {
	case TypeString:
		return f.StrVal
	case TypeInt64:
		return strconv.FormatInt(f.Int64Val(), 10)
	case TypeFloat64:
		return strconv.FormatFloat(f.Float64Val(), 'f', -1, 64)
	case TypeBool:
		return strconv.FormatBool(f.BoolVal())
	case TypeTimestamp:
		return f.TimeVal().Format(time.RFC3339)
	case TypeBytes:
		return f.StrVal
	case TypeDecimal:
		return FormatDecimal(f.Int64Val(), f.Scale)
	default:
		return f.StrVal
	}
}

func (f FieldValue) Interface() any {
	if f.IsNull {
		return nil
	}
	switch f.Type {
	case TypeString:
		return f.StrVal
	case TypeInt64:
		return f.Int64Val()
	case TypeFloat64:
		return f.Float64Val()
	case TypeBool:
		return f.BoolVal()
	case TypeTimestamp:
		return f.TimeVal()
	case TypeBytes:
		return []byte(f.StrVal)
	case TypeDecimal:
		return FormatDecimal(f.Int64Val(), f.Scale)
	default:
		return f.StrVal
	}
}

func (f FieldValue) WriteJSON(buf *bytes.Buffer) {
	if f.IsNull {
		buf.WriteString("null")
		return
	}
	switch f.Type {
	case TypeString:
		b, _ := json.Marshal(f.StrVal)
		buf.Write(b)
	case TypeInt64:
		buf.WriteString(strconv.FormatInt(f.Int64Val(), 10))
	case TypeFloat64:
		buf.WriteString(strconv.FormatFloat(f.Float64Val(), 'f', -1, 64))
	case TypeBool:
		buf.WriteString(strconv.FormatBool(f.BoolVal()))
	case TypeTimestamp:
		buf.WriteByte('"')
		buf.WriteString(f.TimeVal().Format(time.RFC3339))
		buf.WriteByte('"')
	case TypeBytes:
		b, _ := json.Marshal([]byte(f.StrVal))
		buf.Write(b)
	case TypeDecimal:
		buf.WriteString(FormatDecimal(f.Int64Val(), f.Scale))
	default:
		b, _ := json.Marshal(f.StrVal)
		buf.Write(b)
	}
}

func FormatDecimal(mantissa int64, scale int32) string {
	if scale <= 0 {
		return strconv.FormatInt(mantissa, 10)
	}
	neg := false
	var uval uint64
	if mantissa < 0 {
		neg = true
		uval = uint64(0 - mantissa)
	} else {
		uval = uint64(mantissa)
	}
	str := strconv.FormatUint(uval, 10)
	scaleInt := int(scale)

	var b strings.Builder
	if neg {
		b.Grow(len(str) + scaleInt + 3)
		b.WriteByte('-')
	} else {
		b.Grow(len(str) + scaleInt + 2)
	}

	if len(str) <= scaleInt {
		b.WriteString("0.")
		for i := 0; i < scaleInt-len(str); i++ {
			b.WriteByte('0')
		}
		b.WriteString(str)
	} else {
		dotPos := len(str) - scaleInt
		b.WriteString(str[:dotPos])
		b.WriteByte('.')
		b.WriteString(str[dotPos:])
	}
	return b.String()
}

type GenericRecord struct {
	SchemaID    string            `json:"schema_id"`
	Values      []FieldValue      `json:"values"`
	AuditFields map[string]string `json:"audit_fields"`
}

func NewGenericRecord(schemaID string, numFields int) *GenericRecord {
	return &GenericRecord{
		SchemaID:    schemaID,
		Values:      make([]FieldValue, numFields),
		AuditFields: make(map[string]string),
	}
}

func (r *GenericRecord) Get(idx int) *FieldValue {
	if idx < 0 || idx >= len(r.Values) {
		return nil
	}
	return &r.Values[idx]
}

func (r *GenericRecord) SetNull(idx int, t DataType) {
	r.Values[idx] = FieldValue{Type: t, IsNull: true}
}

func (r *GenericRecord) SetString(idx int, val string) {
	r.Values[idx] = FieldValue{Type: TypeString, StrVal: intern.String(val)}
}

func (r *GenericRecord) SetInt64(idx int, val int64) {
	r.Values[idx] = FieldValue{Type: TypeInt64, NumVal: uint64(val)}
}

func (r *GenericRecord) SetFloat64(idx int, val float64) {
	r.Values[idx] = FieldValue{Type: TypeFloat64, NumVal: math.Float64bits(val)}
}

func (r *GenericRecord) SetBool(idx int, val bool) {
	num := uint64(0)
	if val {
		num = 1
	}
	r.Values[idx] = FieldValue{Type: TypeBool, NumVal: num}
}

func (r *GenericRecord) SetTimestamp(idx int, val time.Time) {
	r.Values[idx] = FieldValue{Type: TypeTimestamp, NumVal: uint64(val.UnixNano())}
}

func (r *GenericRecord) SetBytes(idx int, val []byte) {
	r.Values[idx] = FieldValue{Type: TypeBytes, StrVal: string(val)}
}

func (r *GenericRecord) SetDecimal(idx int, mantissa int64, scale int32) {
	r.Values[idx] = FieldValue{Type: TypeDecimal, NumVal: uint64(mantissa), Scale: scale}
}

// ToMap converts the record values into a map keyed by schema field names.
func (r *GenericRecord) ToMap(schema *Schema) map[string]any {
	if r == nil || schema == nil {
		return nil
	}
	m := make(map[string]any, len(schema.Fields))
	for i, f := range schema.Fields {
		val := r.Get(i)
		if val == nil || val.IsNull {
			m[f.Name] = nil
		} else {
			m[f.Name] = val.Interface()
		}
	}
	return m
}

type DeadLetterRecord struct {
	RawPayload   string    `json:"raw_payload"`
	ErrorMessage string    `json:"error_message"`
	FailedColumn string    `json:"failed_column,omitempty"`
	SourceFile   string    `json:"_source_file"`
	FailedAt     time.Time `json:"_failed_at"`
}
