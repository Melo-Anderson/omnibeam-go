// Package sql implements relational database ingestion adapters and range slicing.
package sql

import (
	"database/sql"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// ScanTargetHolder manages column scan pointers for a schema row.
type ScanTargetHolder struct {
	targets []any
}

// NewScanTargetHolder allocates null-safe scan destination pointers.
func NewScanTargetHolder(schema *domain.Schema) *ScanTargetHolder {
	targets := make([]any, len(schema.Fields))
	for i, f := range schema.Fields {
		switch f.Type {
		case domain.TypeInt64:
			targets[i] = new(sql.NullInt64)
		case domain.TypeFloat64:
			targets[i] = new(sql.NullFloat64)
		case domain.TypeBool:
			targets[i] = new(sql.NullBool)
		case domain.TypeTimestamp:
			targets[i] = new(sql.NullTime)
		case domain.TypeBytes:
			targets[i] = new([]byte)
		default:
			targets[i] = new(sql.NullString)
		}
	}
	return &ScanTargetHolder{targets: targets}
}

// GetScanTargets returns the slice of pointers passed to rows.Scan(...).
func (h *ScanTargetHolder) GetScanTargets() []any {
	return h.targets
}

// ExtractRecord converts scanned values into domain.GenericRecord enforcing nullability.
func (h *ScanTargetHolder) ExtractRecord(schema *domain.Schema) (*domain.GenericRecord, error) {
	rec := domain.NewGenericRecord("sql-schema", len(schema.Fields))

	for i, f := range schema.Fields {
		if err := h.mapColumn(i, f, rec); err != nil {
			return nil, err
		}
	}

	rec.AuditFields["_ingested_at"] = time.Now().UTC().Format(time.RFC3339)
	return rec, nil
}

func (h *ScanTargetHolder) mapColumn(i int, f domain.Field, rec *domain.GenericRecord) error {
	switch f.Type {
	case domain.TypeInt64:
		v := h.targets[i].(*sql.NullInt64)
		if !v.Valid {
			rec.SetNull(i, domain.TypeInt64)
			return nil
		}
		rec.SetInt64(i, v.Int64)

	case domain.TypeFloat64:
		v := h.targets[i].(*sql.NullFloat64)
		if !v.Valid {
			rec.SetNull(i, domain.TypeFloat64)
			return nil
		}
		rec.SetFloat64(i, v.Float64)

	case domain.TypeBool:
		v := h.targets[i].(*sql.NullBool)
		if !v.Valid {
			rec.SetNull(i, domain.TypeBool)
			return nil
		}
		rec.SetBool(i, v.Bool)

	case domain.TypeTimestamp:
		v := h.targets[i].(*sql.NullTime)
		if !v.Valid {
			rec.SetNull(i, domain.TypeTimestamp)
			return nil
		}
		rec.SetTimestamp(i, v.Time.UTC())

	case domain.TypeBytes:
		v := h.targets[i].(*[]byte)
		if v == nil || *v == nil {
			rec.SetNull(i, domain.TypeBytes)
			return nil
		}
		rec.SetBytes(i, *v)

	default:
		v := h.targets[i].(*sql.NullString)
		if !v.Valid {
			rec.SetNull(i, domain.TypeString)
			return nil
		}
		rec.SetString(i, v.String)
	}
	return nil
}
