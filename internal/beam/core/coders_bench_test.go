package core_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func BenchmarkGenericRecordCoder_EncodeDecode(b *testing.B) {
	rec := domain.NewGenericRecord("benchmark_schema", 6)
	rec.SetInt64(0, 123456789)
	rec.SetFloat64(1, 9876.54321)
	rec.SetString(2, "customer_alpha_beta_gamma")
	rec.SetBool(3, true)
	rec.SetTimestamp(4, time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC))
	rec.SetDecimal(5, 1234550, 2)
	rec.AuditFields["_source_file"] = "gs://bucket/data.csv"

	var buf bytes.Buffer
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		if err := core.EncodeGenericRecord(rec, &buf); err != nil {
			b.Fatalf("EncodeGenericRecord failed: %v", err)
		}
		reader := bytes.NewReader(buf.Bytes())
		decoded, err := core.DecodeGenericRecord(reader)
		if err != nil {
			b.Fatalf("DecodeGenericRecord failed: %v", err)
		}
		_ = decoded
	}
}
