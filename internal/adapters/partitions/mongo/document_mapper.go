// Package mongo implements the MongoDB NoSQL source adapter.
package mongo

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// FlattenDocument recursively flattens a nested BSON document into dot-separated keys.
func FlattenDocument(prefix string, doc bson.M, out map[string]any) {
	for k, v := range doc {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}
		if subDoc, ok := v.(bson.M); ok {
			FlattenDocument(fullKey, subDoc, out)
		} else if subD, ok := v.(bson.D); ok {
			subM := bson.M{}
			for _, e := range subD {
				subM[e.Key] = e.Value
			}
			FlattenDocument(fullKey, subM, out)
		} else {
			out[fullKey] = v
		}
	}
}

// MapBSONToRecord converts a BSON map document into a typed domain.GenericRecord according to Schema.
func MapBSONToRecord(doc bson.M, schema *domain.Schema, flattenNested bool) (*domain.GenericRecord, error) {
	rec := domain.NewGenericRecord("", len(schema.Fields))

	var lookup map[string]any
	if flattenNested {
		lookup = make(map[string]any)
		FlattenDocument("", doc, lookup)
	} else {
		lookup = doc
	}

	for i, f := range schema.Fields {
		val, exists := lookup[f.Name]
		if !exists && (f.Name == "id" || f.Name == "_id") {
			val = lookup["_id"]
			if val == nil {
				val = lookup["id"]
			}
		}

		if val == nil {
			if !f.Nullable {
				return nil, fmt.Errorf("non-nullable field %q is missing or null", f.Name)
			}
			rec.SetNull(i, f.Type)
			continue
		}

		// Handle BSON specific wrapper types before generic domain coercion
		if objID, ok := val.(bson.ObjectID); ok && f.Type == domain.TypeString {
			rec.SetString(i, objID.Hex())
			continue
		}
		if dt, ok := val.(bson.DateTime); ok && f.Type == domain.TypeTimestamp {
			rec.SetTimestamp(i, dt.Time().UTC())
			continue
		}
		if bin, ok := val.(bson.Binary); ok && f.Type == domain.TypeBytes {
			rec.SetBytes(i, bin.Data)
			continue
		}
		if f.Type == domain.TypeString {
			switch v := val.(type) {
			case bson.M, bson.D, bson.A, []any:
				bytes, err := json.Marshal(v)
				if err != nil {
					return nil, fmt.Errorf("failed serializing field %q to json: %w", f.Name, err)
				}
				rec.SetString(i, string(bytes))
				continue
			}
		}

		switch f.Type {
		case domain.TypeInt64:
			switch v := val.(type) {
			case int64:
				rec.SetInt64(i, v)
			case int32:
				rec.SetInt64(i, int64(v))
			case int:
				rec.SetInt64(i, int64(v))
			case float64:
				rec.SetInt64(i, int64(v))
			case string:
				if parsed, err := domain.CoerceInt64Fast(v); err == nil {
					rec.SetInt64(i, parsed)
				} else {
					return nil, fmt.Errorf("incompatible value %v for %s field %q: %w", val, f.Type, f.Name, err)
				}
			default:
				return nil, fmt.Errorf("incompatible value %v for %s field %q", val, f.Type, f.Name)
			}
		case domain.TypeFloat64:
			switch v := val.(type) {
			case float64:
				rec.SetFloat64(i, v)
			case float32:
				rec.SetFloat64(i, float64(v))
			case int64:
				rec.SetFloat64(i, float64(v))
			case int32:
				rec.SetFloat64(i, float64(v))
			case int:
				rec.SetFloat64(i, float64(v))
			case string:
				if parsed, err := domain.CoerceFloat64Fast(v); err == nil {
					rec.SetFloat64(i, parsed)
				} else {
					return nil, fmt.Errorf("incompatible value %v for %s field %q: %w", val, f.Type, f.Name, err)
				}
			default:
				return nil, fmt.Errorf("incompatible value %v for %s field %q", val, f.Type, f.Name)
			}
		case domain.TypeBool:
			switch v := val.(type) {
			case bool:
				rec.SetBool(i, v)
			case string:
				if parsed, err := domain.CoerceBoolFast(v); err == nil {
					rec.SetBool(i, parsed)
				} else {
					return nil, fmt.Errorf("incompatible value %v for %s field %q: %w", val, f.Type, f.Name, err)
				}
			default:
				return nil, fmt.Errorf("incompatible value %v for %s field %q", val, f.Type, f.Name)
			}
		case domain.TypeTimestamp:
			switch v := val.(type) {
			case time.Time:
				rec.SetTimestamp(i, v.UTC())
			case string:
				if parsed, err := domain.CoerceTimestampFast(v); err == nil {
					rec.SetTimestamp(i, parsed)
				} else {
					return nil, fmt.Errorf("incompatible value %v for %s field %q: %w", val, f.Type, f.Name, err)
				}
			default:
				return nil, fmt.Errorf("incompatible value %v for %s field %q", val, f.Type, f.Name)
			}
		case domain.TypeDecimal:
			switch v := val.(type) {
			case string:
				if m, s, err := domain.CoerceDecimalFast(v, f.Scale, f.OnOverflow); err == nil {
					rec.SetDecimal(i, m, s)
				} else {
					return nil, fmt.Errorf("incompatible value %v for %s field %q: %w", val, f.Type, f.Name, err)
				}
			default:
				s := fmt.Sprintf("%v", v)
				if m, sc, err := domain.CoerceDecimalFast(s, f.Scale, f.OnOverflow); err == nil {
					rec.SetDecimal(i, m, sc)
				} else {
					return nil, fmt.Errorf("incompatible value %v for %s field %q: %w", val, f.Type, f.Name, err)
				}
			}
		case domain.TypeBytes:
			switch v := val.(type) {
			case []byte:
				rec.SetBytes(i, v)
			case bson.Binary:
				rec.SetBytes(i, v.Data)
			default:
				return nil, fmt.Errorf("incompatible value %v for %s field %q", val, f.Type, f.Name)
			}
		default: // TypeString
			rec.SetString(i, fmt.Sprintf("%v", val))
		}
	}

	return rec, nil
}
