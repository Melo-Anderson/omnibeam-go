package domain_test

import (
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestRecordBatch_AppendAndIterate(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
			{Name: "active", Type: domain.TypeBool},
			{Name: "score", Type: domain.TypeFloat64},
			{Name: "amount", Type: domain.TypeDecimal, Scale: 2},
			{Name: "created_at", Type: domain.TypeTimestamp},
			{Name: "payload", Type: domain.TypeBytes},
		},
	}
	schema.Index()

	batch := domain.NewRecordBatch(schema, 100)
	if batch.Capacity() != 100 {
		t.Fatalf("expected capacity 100, got %d", batch.Capacity())
	}

	now := time.Now().UTC().Truncate(time.Microsecond)

	// Row 1: all populated
	rec1 := domain.NewGenericRecord("schema-1", 7)
	rec1.SetInt64(0, 1)
	rec1.SetString(1, "Alice")
	rec1.SetBool(2, true)
	rec1.SetFloat64(3, 99.5)
	rec1.SetDecimal(4, 12345, 2)
	rec1.SetTimestamp(5, now)
	rec1.SetBytes(6, []byte("rawbytes"))
	rec1.AuditFields = map[string]string{"_job_id": "job-1"}

	if err := batch.AppendRow(rec1); err != nil {
		t.Fatalf("AppendRow 1 failed: %v", err)
	}

	// Row 2: with nulls
	rec2 := domain.NewGenericRecord("schema-1", 7)
	rec2.SetInt64(0, 2)
	rec2.SetNull(1, domain.TypeString)
	rec2.SetBool(2, false)
	rec2.SetNull(3, domain.TypeFloat64)
	rec2.SetNull(4, domain.TypeDecimal)
	rec2.SetTimestamp(5, now)
	rec2.SetNull(6, domain.TypeBytes)
	rec2.AuditFields = map[string]string{"_job_id": "job-2"}

	if err := batch.AppendRow(rec2); err != nil {
		t.Fatalf("AppendRow 2 failed: %v", err)
	}

	if batch.RowCount() != 2 {
		t.Fatalf("expected RowCount 2, got %d", batch.RowCount())
	}

	it := batch.Iterator()

	// Verify Row 1
	if !it.Next() {
		t.Fatalf("expected iterator to have first item")
	}
	r1 := it.Record()
	if r1.Values[0].Int64Val() != 1 || r1.Values[1].StringVal() != "Alice" || !r1.Values[2].BoolVal() {
		t.Fatalf("r1 values mismatch: %+v", r1)
	}
	if r1.Values[3].Float64Val() != 99.5 {
		t.Fatalf("r1 float mismatch: %f", r1.Values[3].Float64Val())
	}
	m, s := r1.Values[4].DecimalVal()
	if m != 12345 || s != 2 {
		t.Fatalf("r1 decimal mismatch: %d scale %d", m, s)
	}
	if !r1.Values[5].TimeVal().Equal(now) {
		t.Fatalf("r1 time mismatch: expected %v, got %v", now, r1.Values[5].TimeVal())
	}
	if string(r1.Values[6].BytesVal()) != "rawbytes" {
		t.Fatalf("r1 bytes mismatch: %s", r1.Values[6].BytesVal())
	}
	if r1.AuditFields["_job_id"] != "job-1" {
		t.Fatalf("r1 audit mismatch: %+v", r1.AuditFields)
	}

	// Verify Row 2
	if !it.Next() {
		t.Fatalf("expected iterator to have second item")
	}
	r2 := it.Record()
	if r2.Values[0].Int64Val() != 2 || !r2.Values[1].IsNull || r2.Values[2].BoolVal() != false {
		t.Fatalf("r2 values mismatch: %+v", r2)
	}
	if !r2.Values[3].IsNull || !r2.Values[4].IsNull || !r2.Values[6].IsNull {
		t.Fatalf("r2 expected nulls, got: %+v", r2)
	}
	if r2.AuditFields["_job_id"] != "job-2" {
		t.Fatalf("r2 audit mismatch: %+v", r2.AuditFields)
	}

	if it.Next() {
		t.Fatalf("expected iterator to finish after 2 items")
	}
}

func TestRecordBatchPool_GetPut(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
		},
	}
	schema.Index()

	pool := domain.NewRecordBatchPool(schema, 100)
	batch := pool.Get()
	if batch.Capacity() != 100 {
		t.Fatalf("expected capacity 100, got %d", batch.Capacity())
	}

	rec := domain.NewGenericRecord("", 1)
	rec.SetInt64(0, 10)
	_ = batch.AppendRow(rec)

	if batch.RowCount() != 1 {
		t.Fatalf("expected row count 1")
	}

	pool.Put(batch)

	batch2 := pool.Get()
	if batch2.RowCount() != 0 {
		t.Fatalf("expected row count 0 after reset from pool, got %d", batch2.RowCount())
	}
	if batch2.Capacity() != 100 {
		t.Fatalf("expected capacity preserved 100, got %d", batch2.Capacity())
	}
}

func TestRecordBatch_BitmaskDLQSegregation(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
		},
	}
	schema.Index()
	batch := domain.NewRecordBatch(schema, 10)

	rec1 := domain.NewGenericRecord("", 1)
	rec1.SetInt64(0, 10)
	rec2 := domain.NewGenericRecord("", 1)
	rec2.SetInt64(0, 20)

	_ = batch.AppendRow(rec1)
	_ = batch.AppendRow(rec2)

	// Mark row 0 as invalid (DLQ segregation)
	batch.SetRowValid(0, false)

	if batch.IsRowValid(0) {
		t.Fatalf("expected row 0 to be marked invalid")
	}
	if !batch.IsRowValid(1) {
		t.Fatalf("expected row 1 to be valid")
	}
	if batch.ValidRowCount() != 1 {
		t.Fatalf("expected ValidRowCount 1, got %d", batch.ValidRowCount())
	}

	// Iterator should skip invalid rows
	it := batch.Iterator()
	if !it.Next() {
		t.Fatalf("expected iterator to find valid row 1")
	}
	if it.Record().Values[0].Int64Val() != 20 {
		t.Fatalf("expected record value 20, got %d", it.Record().Values[0].Int64Val())
	}
	if it.Next() {
		t.Fatalf("expected iterator to end after 1 valid row")
	}
}

func TestRecordBatch_CapacityExceeded(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
		},
	}
	schema.Index()
	batch := domain.NewRecordBatch(schema, 64) // minimum capacity is 64

	rec := domain.NewGenericRecord("", 1)
	rec.SetInt64(0, 1)

	for i := 0; i < 64; i++ {
		if err := batch.AppendRow(rec); err != nil {
			t.Fatalf("unexpected error at %d: %v", i, err)
		}
	}

	err := batch.AppendRow(rec)
	if err == nil {
		t.Fatalf("expected capacity exceeded error, got nil")
	}
}

func TestRecordBatch_Reset(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
		},
	}
	schema.Index()
	batch := domain.NewRecordBatch(schema, 64)

	rec := domain.NewGenericRecord("", 2)
	rec.SetInt64(0, 1)
	rec.SetString(1, "test")
	rec.AuditFields = map[string]string{"k": "v"}
	_ = batch.AppendRow(rec)

	batch.SetRowValid(0, false)
	batch.Reset()

	if batch.RowCount() != 0 {
		t.Fatalf("expected RowCount 0, got %d", batch.RowCount())
	}
	if batch.ValidRowCount() != 0 {
		t.Fatalf("expected ValidRowCount 0, got %d", batch.ValidRowCount())
	}
}
