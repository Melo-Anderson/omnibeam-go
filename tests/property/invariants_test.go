package property_test

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/pkg/fastnum"
	"pgregory.net/rapid"
)

// TestProperty_CoderRoundTrip validates Decode(Encode(x)) == x for any generated GenericRecord.
func TestProperty_CoderRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		schemaID := rapid.StringMatching(`[a-z]{3,8}`).Draw(t, "schemaID")
		fieldCount := rapid.IntRange(1, 8).Draw(t, "fieldCount")

		orig := domain.NewGenericRecord(schemaID, fieldCount)
		for i := 0; i < fieldCount; i++ {
			isNull := rapid.Bool().Draw(t, "isNull")
			if isNull {
				orig.SetNull(i, domain.TypeString)
				continue
			}

			dataTypeChoice := rapid.IntRange(0, 5).Draw(t, "dataTypeChoice")
			switch dataTypeChoice {
			case 0:
				s := rapid.String().Draw(t, "stringVal")
				orig.SetString(i, s)
			case 1:
				v := rapid.Int64().Draw(t, "int64Val")
				orig.SetInt64(i, v)
			case 2:
				f := rapid.Float64().Draw(t, "float64Val")
				if math.IsNaN(f) {
					f = 0.0
				}
				orig.SetFloat64(i, f)
			case 3:
				b := rapid.Bool().Draw(t, "boolVal")
				orig.SetBool(i, b)
			case 4:
				// Timestamps within valid unix range
				sec := rapid.Int64Range(0, 2000000000).Draw(t, "timeSec")
				orig.SetTimestamp(i, time.Unix(sec, 0).UTC())
			case 5:
				m := rapid.Int64().Draw(t, "decimalMantissa")
				s := int32(rapid.IntRange(0, 6).Draw(t, "decimalScale"))
				orig.SetDecimal(i, m, s)
			}
		}

		// Add audit fields
		traceID := rapid.StringMatching(`[a-f0-9]{32}`).Draw(t, "traceID")
		spanID := rapid.StringMatching(`[a-f0-9]{16}`).Draw(t, "spanID")
		orig.AuditFields["_trace_id"] = traceID
		orig.AuditFields["_span_id"] = spanID

		var buf bytes.Buffer
		if err := core.EncodeGenericRecord(orig, &buf); err != nil {
			t.Fatalf("EncodeGenericRecord failed: %v", err)
		}

		decoded, err := core.DecodeGenericRecord(&buf)
		if err != nil {
			t.Fatalf("DecodeGenericRecord failed: %v", err)
		}

		if decoded.SchemaID != orig.SchemaID {
			t.Fatalf("SchemaID mismatch: got %q, want %q", decoded.SchemaID, orig.SchemaID)
		}
		if len(decoded.Values) != len(orig.Values) {
			t.Fatalf("Values length mismatch: got %d, want %d", len(decoded.Values), len(orig.Values))
		}
		for i := 0; i < len(orig.Values); i++ {
			if orig.Values[i].IsNull != decoded.Values[i].IsNull {
				t.Fatalf("field[%d] IsNull mismatch: got %v, want %v", i, decoded.Values[i].IsNull, orig.Values[i].IsNull)
			}
			if !orig.Values[i].IsNull {
				if orig.Values[i].Type != decoded.Values[i].Type {
					t.Fatalf("field[%d] Type mismatch: got %v, want %v", i, decoded.Values[i].Type, orig.Values[i].Type)
				}
				if orig.Values[i].StringVal() != decoded.Values[i].StringVal() {
					t.Fatalf("field[%d] StringVal mismatch", i)
				}
				if orig.Values[i].NumVal != decoded.Values[i].NumVal {
					t.Fatalf("field[%d] NumVal mismatch", i)
				}
				if orig.Values[i].Scale != decoded.Values[i].Scale {
					t.Fatalf("field[%d] Scale mismatch", i)
				}
			}
		}
		if decoded.AuditFields["_trace_id"] != orig.AuditFields["_trace_id"] {
			t.Fatalf("AuditFields trace_id mismatch")
		}
		if decoded.AuditFields["_span_id"] != orig.AuditFields["_span_id"] {
			t.Fatalf("AuditFields span_id mismatch")
		}
	})
}

// TestProperty_DLQCoderRoundTrip validates DeadLetterRecord binary coder round-trip.
func TestProperty_DLQCoderRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		payload := rapid.String().Draw(t, "payload")
		errMsg := rapid.String().Draw(t, "errMsg")
		failedCol := rapid.String().Draw(t, "failedCol")
		sourceFile := rapid.String().Draw(t, "sourceFile")
		sec := rapid.Int64Range(0, 2000000000).Draw(t, "timeSec")

		orig := &domain.DeadLetterRecord{
			RawPayload:   payload,
			ErrorMessage: errMsg,
			FailedColumn: failedCol,
			SourceFile:   sourceFile,
			FailedAt:     time.Unix(sec, 0).UTC(),
		}

		var buf bytes.Buffer
		if err := core.EncodeDeadLetterRecord(orig, &buf); err != nil {
			t.Fatalf("EncodeDeadLetterRecord failed: %v", err)
		}

		decoded, err := core.DecodeDeadLetterRecord(&buf)
		if err != nil {
			t.Fatalf("DecodeDeadLetterRecord failed: %v", err)
		}

		if decoded.RawPayload != orig.RawPayload || decoded.ErrorMessage != orig.ErrorMessage ||
			decoded.FailedColumn != orig.FailedColumn || decoded.SourceFile != orig.SourceFile {
			t.Fatalf("DLQ record field mismatch")
		}
		if !decoded.FailedAt.Equal(orig.FailedAt) {
			t.Fatalf("DLQ record FailedAt mismatch")
		}
	})
}

// TestProperty_StrictConservation validates TotalRead = RowsWritten + DeadLetterCount invariant.
func TestProperty_StrictConservation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		validCount := rapid.Int64Range(0, 10000).Draw(t, "validCount")
		dlqCount := rapid.Int64Range(0, 10000).Draw(t, "dlqCount")

		totalRead := validCount + dlqCount

		metrics := domain.NewPipelineMetrics("test-pipeline", "run-123")
		metrics.TotalRecordsRead = totalRead
		metrics.RowsWritten = validCount
		metrics.DeadLetterCount = dlqCount

		if !metrics.IsStrictlyConserved() {
			t.Fatalf("Strict conservation invariant failed: totalRead=%d, written=%d, dlq=%d",
				totalRead, validCount, dlqCount)
		}
	})
}

// TestProperty_FastnumInt64_MatchesStrconv validates that fastnum produces identical results to strconv for any int64.
func TestProperty_FastnumInt64_MatchesStrconv(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		val := rapid.Int64().Draw(t, "numericValue")
		s := fmt.Sprintf("  %d  ", val)

		got, err := fastnum.ParseInt64(s)
		if err != nil {
			t.Fatalf("fastnum failed for valid input %q: %v", s, err)
		}

		expected, err := strconv.ParseInt(fmt.Sprint(val), 10, 64)
		if err != nil {
			t.Fatalf("strconv failed: %v", err)
		}

		if got != expected {
			t.Fatalf("mismatch between fastnum (%d) and strconv (%d) for input %d", got, expected, val)
		}
	})
}
