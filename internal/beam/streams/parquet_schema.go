// package streams contains Beam DoFn transformations and parquet schema utilities.
package streams

import (
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

// BuildParquetSchema converts a canonical domain.Schema into a parquet-go schema tree.
func BuildParquetSchema(domainSchema *domain.Schema) *parquet.Schema {
	fields := make(map[string]parquet.Node, len(domainSchema.Fields)+2)
	for _, f := range domainSchema.Fields {
		var node parquet.Node
		switch f.Type {
		case domain.TypeInt64:
			node = parquet.Int(64)
		case domain.TypeFloat64:
			node = parquet.Leaf(parquet.DoubleType)
		case domain.TypeBool:
			node = parquet.Leaf(parquet.BooleanType)
		case domain.TypeTimestamp:
			node = parquet.Int(64) // Epoch micros
		case domain.TypeBytes:
			node = parquet.Leaf(parquet.ByteArrayType)
		case domain.TypeDecimal:
			scale := int(f.Scale)
			node = parquet.Decimal(18, scale, parquet.Int64Type)
		default:
			node = parquet.String()
		}
		if f.Nullable {
			node = parquet.Optional(node)
		} else {
			node = parquet.Required(node)
		}
		fields[f.Name] = node
	}

	fields["_ingested_at"] = parquet.Required(parquet.String())
	fields["_source_file"] = parquet.Required(parquet.String())

	return parquet.NewSchema("record", parquet.Group(fields))
}

// ParquetColumnExtractor extracts one FieldValue into a parquet-compatible rowMap entry.
type ParquetColumnExtractor func(val *domain.FieldValue, rowMap map[string]any)

// ParquetRowMapper converts a GenericRecord into a map[string]any for parquet-go.
type ParquetRowMapper func(rec *domain.GenericRecord) map[string]any

// CompileParquetRowMapper pre-compiles per-column extractor closures for the given schema.
// The switch over field types executes ONCE at Setup() time.
func CompileParquetRowMapper(domainSchema *domain.Schema) ParquetRowMapper {
	numExtra := 2 // _ingested_at, _source_file
	extractors := make([]ParquetColumnExtractor, len(domainSchema.Fields))

	for i, f := range domainSchema.Fields {
		name := f.Name
		switch f.Type {
		case domain.TypeInt64, domain.TypeDecimal:
			extractors[i] = func(val *domain.FieldValue, rowMap map[string]any) {
				if val != nil && !val.IsNull {
					rowMap[name] = val.Int64Val()
				}
			}
		case domain.TypeFloat64:
			extractors[i] = func(val *domain.FieldValue, rowMap map[string]any) {
				if val != nil && !val.IsNull {
					rowMap[name] = val.Float64Val()
				}
			}
		case domain.TypeBool:
			extractors[i] = func(val *domain.FieldValue, rowMap map[string]any) {
				if val != nil && !val.IsNull {
					rowMap[name] = val.BoolVal()
				}
			}
		case domain.TypeTimestamp:
			extractors[i] = func(val *domain.FieldValue, rowMap map[string]any) {
				if val != nil && !val.IsNull {
					rowMap[name] = val.TimeVal().UnixMicro()
				}
			}
		case domain.TypeBytes:
			extractors[i] = func(val *domain.FieldValue, rowMap map[string]any) {
				if val != nil && !val.IsNull {
					rowMap[name] = val.BytesVal()
				}
			}
		default: // TypeString
			extractors[i] = func(val *domain.FieldValue, rowMap map[string]any) {
				if val != nil && !val.IsNull {
					rowMap[name] = val.StringVal()
				}
			}
		}
	}

	return func(rec *domain.GenericRecord) map[string]any {
		rowMap := make(map[string]any, len(extractors)+numExtra)
		for i, ext := range extractors {
			if i < len(rec.Values) {
				ext(&rec.Values[i], rowMap)
			}
		}

		if rec.AuditFields != nil && rec.AuditFields["_ingested_at"] != "" {
			rowMap["_ingested_at"] = rec.AuditFields["_ingested_at"]
		} else {
			rowMap["_ingested_at"] = time.Now().UTC().Format(time.RFC3339)
		}

		if rec.AuditFields != nil && rec.AuditFields["_source_file"] != "" {
			rowMap["_source_file"] = rec.AuditFields["_source_file"]
		} else {
			rowMap["_source_file"] = "unknown"
		}

		return rowMap
	}
}

// RecordToParquetRow maps a domain.GenericRecord into a serializable map based on domain.Schema.
func RecordToParquetRow(rec *domain.GenericRecord, domainSchema *domain.Schema) map[string]any {
	mapper := CompileParquetRowMapper(domainSchema)
	return mapper(rec)
}

// ParquetDirectRowEncoder encodes a GenericRecord directly into a destination parquet.Value slice.
type ParquetDirectRowEncoder func(rec *domain.GenericRecord, dest []parquet.Value) parquet.Row

// CompileParquetDirectRowEncoder pre-compiles per-column typed closures mapping GenericRecord
// fields to physical parquet.Value elements at known column indices without reflection or maps.
func CompileParquetDirectRowEncoder(domainSchema *domain.Schema) (ParquetDirectRowEncoder, int) {
	pqSchema := BuildParquetSchema(domainSchema)
	numCols := len(pqSchema.Columns())

	type colEncoder func(val *domain.FieldValue, dest []parquet.Value)
	encoders := make([]colEncoder, len(domainSchema.Fields))

	for i, f := range domainSchema.Fields {
		col, ok := pqSchema.Lookup(f.Name)
		if !ok {
			continue
		}
		colIdx := col.ColumnIndex
		maxDef := col.MaxDefinitionLevel

		switch f.Type {
		case domain.TypeInt64, domain.TypeDecimal:
			encoders[i] = func(val *domain.FieldValue, dest []parquet.Value) {
				if val == nil || val.IsNull {
					dest[colIdx] = parquet.NullValue().Level(0, 0, colIdx)
				} else {
					dest[colIdx] = parquet.Int64Value(val.Int64Val()).Level(0, maxDef, colIdx)
				}
			}
		case domain.TypeFloat64:
			encoders[i] = func(val *domain.FieldValue, dest []parquet.Value) {
				if val == nil || val.IsNull {
					dest[colIdx] = parquet.NullValue().Level(0, 0, colIdx)
				} else {
					dest[colIdx] = parquet.DoubleValue(val.Float64Val()).Level(0, maxDef, colIdx)
				}
			}
		case domain.TypeBool:
			encoders[i] = func(val *domain.FieldValue, dest []parquet.Value) {
				if val == nil || val.IsNull {
					dest[colIdx] = parquet.NullValue().Level(0, 0, colIdx)
				} else {
					dest[colIdx] = parquet.BooleanValue(val.BoolVal()).Level(0, maxDef, colIdx)
				}
			}
		case domain.TypeTimestamp:
			encoders[i] = func(val *domain.FieldValue, dest []parquet.Value) {
				if val == nil || val.IsNull {
					dest[colIdx] = parquet.NullValue().Level(0, 0, colIdx)
				} else {
					dest[colIdx] = parquet.Int64Value(val.TimeVal().UnixMicro()).Level(0, maxDef, colIdx)
				}
			}
		case domain.TypeBytes:
			encoders[i] = func(val *domain.FieldValue, dest []parquet.Value) {
				if val == nil || val.IsNull {
					dest[colIdx] = parquet.NullValue().Level(0, 0, colIdx)
				} else {
					dest[colIdx] = parquet.ByteArrayValue(val.BytesVal()).Level(0, maxDef, colIdx)
				}
			}
		default: // TypeString
			encoders[i] = func(val *domain.FieldValue, dest []parquet.Value) {
				if val == nil || val.IsNull {
					dest[colIdx] = parquet.NullValue().Level(0, 0, colIdx)
				} else {
					dest[colIdx] = parquet.ByteArrayValue([]byte(val.StringVal())).Level(0, maxDef, colIdx)
				}
			}
		}
	}

	ingestedCol, _ := pqSchema.Lookup("_ingested_at")
	sourceCol, _ := pqSchema.Lookup("_source_file")

	return func(rec *domain.GenericRecord, dest []parquet.Value) parquet.Row {
		if len(dest) < numCols {
			dest = make([]parquet.Value, numCols)
		} else {
			dest = dest[:numCols]
		}

		for i, enc := range encoders {
			if i < len(rec.Values) {
				enc(&rec.Values[i], dest)
			}
		}

		ingestedAt := ""
		if rec.AuditFields != nil && rec.AuditFields["_ingested_at"] != "" {
			ingestedAt = rec.AuditFields["_ingested_at"]
		} else {
			ingestedAt = time.Now().UTC().Format(time.RFC3339)
		}
		dest[ingestedCol.ColumnIndex] = parquet.ByteArrayValue([]byte(ingestedAt)).Level(0, ingestedCol.MaxDefinitionLevel, ingestedCol.ColumnIndex)

		sourceFile := "unknown"
		if rec.AuditFields != nil && rec.AuditFields["_source_file"] != "" {
			sourceFile = rec.AuditFields["_source_file"]
		}
		dest[sourceCol.ColumnIndex] = parquet.ByteArrayValue([]byte(sourceFile)).Level(0, sourceCol.MaxDefinitionLevel, sourceCol.ColumnIndex)

		return parquet.Row(dest)
	}, numCols
}
