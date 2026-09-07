package core

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestDecodeGenericRecord_CorruptFieldCount(t *testing.T) {
	var buf bytes.Buffer
	_ = writeString(&buf, "schema_1")
	_ = writeVarint(&buf, int64(domain.DefaultMaxRecordFields+500))

	_, err := DecodeGenericRecord(&buf)
	if err == nil {
		t.Fatal("expected error on oversized fieldCount, got nil")
	}
	if !strings.Contains(err.Error(), "field count") {
		t.Fatalf("expected field count error message, got: %v", err)
	}
}

func TestDecodeGenericRecord_NilRecordEncoding(t *testing.T) {
	var buf bytes.Buffer
	err := EncodeGenericRecord(nil, &buf)
	if err != nil {
		t.Fatalf("unexpected error encoding nil record: %v", err)
	}

	rec, err := DecodeGenericRecord(&buf)
	if err != nil {
		t.Fatalf("unexpected error decoding nil record: %v", err)
	}
	if rec != nil {
		t.Fatalf("expected nil record, got: %v", rec)
	}
}

func TestGenericRecordCoder_RoundTrip(t *testing.T) {
	now := time.Date(2026, 8, 24, 10, 0, 0, 0, time.UTC)
	rec := domain.NewGenericRecord("schema-v1", 6)
	rec.SetInt64(0, 42)
	rec.SetFloat64(1, 123.45)
	rec.SetString(2, "omnibeam")
	rec.SetBool(3, true)
	rec.SetTimestamp(4, now)
	rec.SetNull(5, domain.TypeBytes)
	rec.AuditFields["_source"] = "test.csv"
	rec.AuditFields["_ingested_at"] = "2026-08-24T10:00:00Z"

	var buf bytes.Buffer
	if err := EncodeGenericRecord(rec, &buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeGenericRecord(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded.SchemaID != rec.SchemaID {
		t.Errorf("SchemaID: got %q, want %q", decoded.SchemaID, rec.SchemaID)
	}
	if v := decoded.Get(0); v == nil || v.Int64Val() != 42 {
		t.Errorf("field[0] Int64Val: got %v", v)
	}
	if v := decoded.Get(1); v == nil || v.Float64Val() != 123.45 {
		t.Errorf("field[1] Float64Val: got %v", v)
	}
	if v := decoded.Get(2); v == nil || v.StringVal() != "omnibeam" {
		t.Errorf("field[2] StringVal: got %v", v)
	}
	if v := decoded.Get(3); v == nil || !v.BoolVal() {
		t.Errorf("field[3] BoolVal: got %v", v)
	}
	if v := decoded.Get(4); v == nil || !v.TimeVal().Equal(now) {
		t.Errorf("field[4] TimeVal: got %v, want %v", v, now)
	}
	if v := decoded.Get(5); v == nil || !v.IsNull {
		t.Errorf("field[5] IsNull: got %v", v)
	}
	if decoded.AuditFields["_source"] != "test.csv" {
		t.Errorf("audit _source mismatch: %v", decoded.AuditFields)
	}
}

func TestGenericRecordCoder_BytesAndBoolFalse(t *testing.T) {
	rec := domain.NewGenericRecord("schema-bytes", 2)
	rec.SetBytes(0, []byte("rawbytes"))
	rec.SetBool(1, false)

	var buf bytes.Buffer
	if err := EncodeGenericRecord(rec, &buf); err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	decoded, err := DecodeGenericRecord(&buf)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if v := decoded.Get(0); v == nil || string(v.BytesVal()) != "rawbytes" {
		t.Errorf("field[0] BytesVal: got %v", v)
	}
	if v := decoded.Get(1); v == nil || v.BoolVal() {
		t.Errorf("field[1] BoolVal: got %v", v)
	}
}

func TestEncodeDecodeGenericRecord_Decimal(t *testing.T) {
	rec := domain.NewGenericRecord("dec_schema", 2)
	rec.SetDecimal(0, 99950, 2) // 999.50
	rec.SetNull(1, domain.TypeDecimal)

	var buf bytes.Buffer
	if err := EncodeGenericRecord(rec, &buf); err != nil {
		t.Fatalf("failed encoding decimal record: %v", err)
	}

	decoded, err := DecodeGenericRecord(&buf)
	if err != nil {
		t.Fatalf("failed decoding decimal record: %v", err)
	}

	if decoded == nil {
		t.Fatal("expected non-nil decoded record")
	}

	v0 := decoded.Get(0)
	if v0 == nil || v0.Type != domain.TypeDecimal {
		t.Fatalf("expected TypeDecimal, got %v", v0)
	}
	m, s := v0.DecimalVal()
	if m != 99950 || s != 2 {
		t.Errorf("expected (99950, 2), got (%d, %d)", m, s)
	}

	v1 := decoded.Get(1)
	if v1 == nil || !v1.IsNull {
		t.Errorf("expected null decimal, got %v", v1)
	}
}

func TestGenericRecordCoder_BeamAdapters(t *testing.T) {
	rec := domain.NewGenericRecord("s1", 1)
	rec.SetString(0, "beam")

	data, err := encGenericRecord(*rec)
	if err != nil {
		t.Fatalf("encGenericRecord failed: %v", err)
	}
	decoded, err := decGenericRecord(data)
	if err != nil {
		t.Fatalf("decGenericRecord failed: %v", err)
	}
	if decoded.SchemaID != "s1" || decoded.Get(0).StringVal() != "beam" {
		t.Errorf("decGenericRecord mismatch: %+v", decoded)
	}

	// Nil encode
	var buf bytes.Buffer
	if err := EncodeGenericRecord(nil, &buf); err != nil {
		t.Errorf("EncodeGenericRecord nil failed: %v", err)
	}
}

func TestDeadLetterRecordCoder_RoundTrip(t *testing.T) {
	now := time.Date(2026, 8, 24, 11, 0, 0, 0, time.UTC)
	dlq := &domain.DeadLetterRecord{
		RawPayload:   "1,bad,row",
		ErrorMessage: "cannot parse column 2 as int64",
		FailedColumn: "age",
		SourceFile:   "gs://bucket/file.csv",
		FailedAt:     now,
	}

	var buf bytes.Buffer
	if err := EncodeDeadLetterRecord(dlq, &buf); err != nil {
		t.Fatalf("Encode DLQ failed: %v", err)
	}
	decoded, err := DecodeDeadLetterRecord(&buf)
	if err != nil {
		t.Fatalf("Decode DLQ failed: %v", err)
	}
	if decoded.RawPayload != dlq.RawPayload || decoded.ErrorMessage != dlq.ErrorMessage {
		t.Errorf("DLQ payload mismatch: %+v", decoded)
	}
	if !decoded.FailedAt.Equal(dlq.FailedAt) {
		t.Errorf("DLQ FailedAt mismatch: got %v, want %v", decoded.FailedAt, dlq.FailedAt)
	}

	// Adapter roundtrip
	data, err := encDeadLetterRecord(*dlq)
	if err != nil {
		t.Fatalf("encDeadLetterRecord failed: %v", err)
	}
	decAdapter, err := decDeadLetterRecord(data)
	if err != nil {
		t.Fatalf("decDeadLetterRecord failed: %v", err)
	}
	if decAdapter.ErrorMessage != dlq.ErrorMessage {
		t.Errorf("decDeadLetterRecord mismatch: %+v", decAdapter)
	}

	// Nil encode
	var nilBuf bytes.Buffer
	if err := EncodeDeadLetterRecord(nil, &nilBuf); err != nil {
		t.Errorf("EncodeDeadLetterRecord nil failed: %v", err)
	}
}

func TestPipelineMetricsCoder(t *testing.T) {
	metrics := domain.PipelineMetrics{
		PipelineID:       "p1",
		RunID:            "r1",
		TotalRecordsRead: 100,
		RowsWritten:      90,
		DeadLetterCount:  10,
	}

	data, err := encPipelineMetrics(metrics)
	if err != nil {
		t.Fatalf("encPipelineMetrics failed: %v", err)
	}

	decoded, err := decPipelineMetrics(data)
	if err != nil {
		t.Fatalf("decPipelineMetrics failed: %v", err)
	}

	if decoded.TotalRecordsRead != 100 || decoded.RowsWritten != 90 || decoded.DeadLetterCount != 10 {
		t.Errorf("decPipelineMetrics mismatch: %+v", decoded)
	}
}

func TestEncodeDecodePipelineMetrics_RoundTrip(t *testing.T) {
	orig := domain.PipelineMetrics{
		PipelineID:          "pipe-99",
		RunID:               "run-88",
		TotalRecordsRead:    5000,
		RowCount:            4800,
		RowsWritten:         4800,
		DeadLetterCount:     200,
		BytesWritten:        1024000,
		FilesWritten:        4,
		APICallsMade:        20,
		ExecutionDurationMs: 1250,
		Checksum:            "chk-12345",
		ColumnNullCounts: map[string]int64{
			"col_a": 5,
			"col_b": 10,
		},
		InvalidValueCounts: map[string]int64{
			"col_c": 2,
		},
	}

	var buf bytes.Buffer
	if err := EncodePipelineMetrics(orig, &buf); err != nil {
		t.Fatalf("EncodePipelineMetrics: %v", err)
	}

	decoded, err := DecodePipelineMetrics(&buf)
	if err != nil {
		t.Fatalf("DecodePipelineMetrics: %v", err)
	}

	if decoded.PipelineID != orig.PipelineID || decoded.TotalRecordsRead != orig.TotalRecordsRead ||
		decoded.RowsWritten != orig.RowsWritten || decoded.DeadLetterCount != orig.DeadLetterCount ||
		decoded.Checksum != orig.Checksum || decoded.ColumnNullCounts["col_a"] != 5 ||
		decoded.InvalidValueCounts["col_c"] != 2 {
		t.Fatalf("mismatch in decoded pipeline metrics: %+v", decoded)
	}
}

func TestFlagFunctions(t *testing.T) {
	rec := domain.NewGenericRecord("s", 1)
	if validRecordFlagFn(rec) != false {
		t.Error("validRecordFlagFn expected false")
	}

	dlq := &domain.DeadLetterRecord{}
	if dlqRecordFlagFn(dlq) != true {
		t.Error("dlqRecordFlagFn expected true")
	}
}

func BenchmarkGenericRecord_BinaryCoder(b *testing.B) {
	rec := domain.NewGenericRecord("benchmark-schema", 5)
	rec.SetInt64(0, 12345678)
	rec.SetFloat64(1, 9876.5432)
	rec.SetString(2, "benchmark_string_val")
	rec.SetBool(3, true)
	rec.SetTimestamp(4, time.Now())

	var buf bytes.Buffer
	buf.Grow(128)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_ = EncodeGenericRecord(rec, &buf)
		_, _ = DecodeGenericRecord(&buf)
	}
}

func BenchmarkGenericRecord_JSONMarshalUnmarshal(b *testing.B) {
	rec := domain.NewGenericRecord("benchmark-schema", 5)
	rec.SetInt64(0, 12345678)
	rec.SetFloat64(1, 9876.5432)
	rec.SetString(2, "benchmark_string_val")
	rec.SetBool(3, true)
	rec.SetTimestamp(4, time.Now())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, _ := json.Marshal(rec)
		var out domain.GenericRecord
		_ = json.Unmarshal(data, &out)
	}
}
