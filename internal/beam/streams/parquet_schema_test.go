package streams

import (
	"bytes"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/parquet-go/parquet-go"
)

func TestBuildParquetSchema(t *testing.T) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "amount", Type: domain.TypeFloat64, Nullable: true},
			{Name: "active", Type: domain.TypeBool, Nullable: false},
			{Name: "created_at", Type: domain.TypeTimestamp, Nullable: true},
			{Name: "payload", Type: domain.TypeBytes, Nullable: true},
			{Name: "name", Type: domain.TypeString, Nullable: true},
			{Name: "price", Type: domain.TypeDecimal, Scale: 2, Nullable: false},
		},
	}

	pqSchema := BuildParquetSchema(schema)
	if pqSchema == nil {
		t.Fatal("expected non-nil parquet schema")
	}

	fields := pqSchema.Fields()
	fieldNames := make(map[string]bool)
	for _, f := range fields {
		fieldNames[f.Name()] = true
	}

	if !fieldNames["_ingested_at"] {
		t.Error("expected _ingested_at audit field in schema")
	}
	if !fieldNames["_source_file"] {
		t.Error("expected _source_file audit field in schema")
	}
	if !fieldNames["id"] || !fieldNames["amount"] || !fieldNames["price"] {
		t.Error("expected declared domain fields in schema")
	}
}

func TestCompileParquetRowMapper(t *testing.T) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
			{Name: "amount", Type: domain.TypeFloat64},
			{Name: "active", Type: domain.TypeBool},
			{Name: "created_at", Type: domain.TypeTimestamp},
			{Name: "payload", Type: domain.TypeBytes},
			{Name: "price", Type: domain.TypeDecimal, Scale: 2},
		},
	}

	mapper := CompileParquetRowMapper(schema)
	if mapper == nil {
		t.Fatal("expected non-nil mapper")
	}

	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	rec := domain.NewGenericRecord("orders_schema", 7)
	rec.SetInt64(0, 1001)
	rec.SetString(1, "Alice")
	rec.SetFloat64(2, 99.95)
	rec.SetBool(3, true)
	rec.SetTimestamp(4, now)
	rec.SetBytes(5, []byte("raw-bytes"))
	rec.SetDecimal(6, 12550, 2)
	rec.AuditFields["_source_file"] = "orders.csv"
	rec.AuditFields["_ingested_at"] = now.Format(time.RFC3339)

	row := mapper(rec)
	if row["id"] != int64(1001) {
		t.Errorf("expected id=1001, got %v", row["id"])
	}
	if row["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", row["name"])
	}
	if row["amount"] != 99.95 {
		t.Errorf("expected amount=99.95, got %v", row["amount"])
	}
	if row["active"] != true {
		t.Errorf("expected active=true, got %v", row["active"])
	}
	if row["created_at"] != now.UnixMicro() {
		t.Errorf("expected created_at=%d, got %v", now.UnixMicro(), row["created_at"])
	}
	if string(row["payload"].([]byte)) != "raw-bytes" {
		t.Errorf("expected payload=raw-bytes, got %v", row["payload"])
	}
	if row["price"] != int64(12550) {
		t.Errorf("expected price=12550, got %v", row["price"])
	}
	if row["_source_file"] != "orders.csv" {
		t.Errorf("expected _source_file=orders.csv, got %v", row["_source_file"])
	}
	if row["_ingested_at"] != now.Format(time.RFC3339) {
		t.Errorf("expected _ingested_at=%s, got %v", now.Format(time.RFC3339), row["_ingested_at"])
	}
}

func BenchmarkCompileParquetRowMapper(b *testing.B) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
			{Name: "amount", Type: domain.TypeFloat64},
			{Name: "active", Type: domain.TypeBool},
			{Name: "created_at", Type: domain.TypeTimestamp},
			{Name: "payload", Type: domain.TypeBytes},
			{Name: "price", Type: domain.TypeDecimal, Scale: 2},
		},
	}

	mapper := CompileParquetRowMapper(schema)
	rec := domain.NewGenericRecord("orders_schema", 7)
	rec.SetInt64(0, 1001)
	rec.SetString(1, "Alice")
	rec.SetFloat64(2, 99.95)
	rec.SetBool(3, true)
	rec.SetTimestamp(4, time.Now())
	rec.SetBytes(5, []byte("raw-bytes"))
	rec.SetDecimal(6, 12550, 2)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mapper(rec)
	}
}

func TestCompileParquetDirectRowEncoder(t *testing.T) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString, Nullable: true},
			{Name: "amount", Type: domain.TypeFloat64},
			{Name: "active", Type: domain.TypeBool},
			{Name: "created_at", Type: domain.TypeTimestamp, Nullable: true},
			{Name: "payload", Type: domain.TypeBytes, Nullable: true},
			{Name: "price", Type: domain.TypeDecimal, Scale: 2},
		},
	}

	encoder, numCols := CompileParquetDirectRowEncoder(schema)
	if encoder == nil {
		t.Fatal("expected non-nil encoder")
	}
	if numCols != len(schema.Fields)+2 {
		t.Fatalf("expected %d columns, got %d", len(schema.Fields)+2, numCols)
	}

	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	rec := domain.NewGenericRecord("orders_schema", 7)
	rec.SetInt64(0, 1001)
	rec.SetString(1, "Alice")
	rec.SetFloat64(2, 99.95)
	rec.SetBool(3, true)
	rec.SetTimestamp(4, now)
	rec.SetBytes(5, []byte("raw-bytes"))
	rec.SetDecimal(6, 12550, 2)
	rec.AuditFields["_source_file"] = "orders.csv"
	rec.AuditFields["_ingested_at"] = now.Format(time.RFC3339)

	buf := make([]parquet.Value, numCols)
	row := encoder(rec, buf)
	if len(row) != numCols {
		t.Fatalf("expected row length %d, got %d", numCols, len(row))
	}

	// Verify writing row to parquet.Writer works without errors
	var out bytes.Buffer
	pqWriter := parquet.NewWriter(&out, BuildParquetSchema(schema))
	if _, err := pqWriter.WriteRows([]parquet.Row{row}); err != nil {
		t.Fatalf("failed writing direct row: %v", err)
	}
	if err := pqWriter.Close(); err != nil {
		t.Fatalf("failed closing parquet writer: %v", err)
	}

	// Read back and verify
	reader := bytes.NewReader(out.Bytes())
	file, err := parquet.OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatalf("failed opening written parquet file: %v", err)
	}
	if file.NumRows() != 1 {
		t.Errorf("expected 1 row in file, got %d", file.NumRows())
	}
}

func BenchmarkCompileParquetDirectRowEncoder(b *testing.B) {
	schema := &domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64},
			{Name: "name", Type: domain.TypeString},
			{Name: "amount", Type: domain.TypeFloat64},
			{Name: "active", Type: domain.TypeBool},
			{Name: "created_at", Type: domain.TypeTimestamp},
			{Name: "payload", Type: domain.TypeBytes},
			{Name: "price", Type: domain.TypeDecimal, Scale: 2},
		},
	}

	encoder, numCols := CompileParquetDirectRowEncoder(schema)
	rec := domain.NewGenericRecord("orders_schema", 7)
	rec.SetInt64(0, 1001)
	rec.SetString(1, "Alice")
	rec.SetFloat64(2, 99.95)
	rec.SetBool(3, true)
	rec.SetTimestamp(4, time.Now())
	rec.SetBytes(5, []byte("raw-bytes"))
	rec.SetDecimal(6, 12550, 2)

	rowBuf := make([]parquet.Value, numCols)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = encoder(rec, rowBuf)
	}
}

