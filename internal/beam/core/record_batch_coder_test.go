package core_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestRecordBatchCoder_RoundTrip(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "val", Type: domain.TypeFloat64},
			{Name: "desc", Type: domain.TypeString},
			{Name: "active", Type: domain.TypeBool},
			{Name: "amount", Type: domain.TypeDecimal, Scale: 2},
			{Name: "created_at", Type: domain.TypeTimestamp},
		},
	}
	schema.Index()

	batch := domain.NewRecordBatch(schema, 10)
	now := time.Now().UTC().Truncate(time.Microsecond)

	// Row 1: Valid row
	rec1 := domain.NewGenericRecord("schema-1", 6)
	rec1.SetInt64(0, 42)
	rec1.SetFloat64(1, 3.1415)
	rec1.SetString(2, "Batch Test")
	rec1.SetBool(3, true)
	rec1.SetDecimal(4, 9990, 2)
	rec1.SetTimestamp(5, now)
	rec1.AuditFields = map[string]string{"_job_id": "job-123"}
	_ = batch.AppendRow(rec1)

	// Row 2: Row with nulls and marked invalid (DLQ quarantined)
	rec2 := domain.NewGenericRecord("schema-1", 6)
	rec2.SetInt64(0, 99)
	rec2.SetNull(1, domain.TypeFloat64)
	rec2.SetNull(2, domain.TypeString)
	rec2.SetBool(3, false)
	rec2.SetNull(4, domain.TypeDecimal)
	rec2.SetTimestamp(5, now)
	_ = batch.AppendRow(rec2)
	batch.SetRowValid(1, false)

	var buf bytes.Buffer
	enc := core.NewRecordBatchEncoder(&buf)
	if err := enc.Encode(batch); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	dec := core.NewRecordBatchDecoder(&buf)
	decodedBatch, err := dec.Decode(schema)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decodedBatch.RowCount() != 2 {
		t.Fatalf("expected 2 rows, got %d", decodedBatch.RowCount())
	}
	if decodedBatch.ValidRowCount() != 1 {
		t.Fatalf("expected 1 valid row, got %d", decodedBatch.ValidRowCount())
	}
	if decodedBatch.IsRowValid(1) {
		t.Fatalf("expected row 1 to be marked invalid in decoded batch")
	}

	it := decodedBatch.Iterator()
	if !it.Next() {
		t.Fatalf("expected next item in decoded batch")
	}
	r := it.Record()
	if r.Values[0].Int64Val() != 42 || r.Values[2].StringVal() != "Batch Test" || !r.Values[3].BoolVal() {
		t.Fatalf("decoded data mismatch: %+v", r)
	}
	if r.Values[1].Float64Val() != 3.1415 {
		t.Fatalf("decoded float mismatch: %f", r.Values[1].Float64Val())
	}
	m, s := r.Values[4].DecimalVal()
	if m != 9990 || s != 2 {
		t.Fatalf("decoded decimal mismatch: %d scale %d", m, s)
	}
	if !r.Values[5].TimeVal().Equal(now) {
		t.Fatalf("decoded time mismatch: expected %v, got %v", now, r.Values[5].TimeVal())
	}

	// Should not have more valid items
	if it.Next() {
		t.Fatalf("expected iterator to have finished")
	}
}

func TestRecordBatchCoder_SchemaMismatch(t *testing.T) {
	schema1 := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
		},
	}
	schema1.Index()

	schema2 := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
		},
	}
	schema2.Index()

	batch := domain.NewRecordBatch(schema1, 10)
	rec := domain.NewGenericRecord("", 1)
	rec.SetInt64(0, 1)
	_ = batch.AppendRow(rec)

	var buf bytes.Buffer
	enc := core.NewRecordBatchEncoder(&buf)
	if err := enc.Encode(batch); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	dec := core.NewRecordBatchDecoder(&buf)
	_, err := dec.Decode(schema2)
	if err == nil {
		t.Fatalf("expected schema mismatch error, got nil")
	}
}
