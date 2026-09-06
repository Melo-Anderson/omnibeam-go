package core

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	pkgtelemetry "github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
)

func TestCastAndValidateFn_ValidRow(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "amount", Type: domain.TypeFloat64, Nullable: true},
			{Name: "is_active", Type: domain.TypeBool, Nullable: false},
			{Name: "date", Type: domain.TypeTimestamp, Nullable: false},
		},
	}

	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	rawRec := domain.NewGenericRecord("test.csv", 4)
	rawRec.SetString(0, "12345")
	rawRec.SetString(1, "99.95")
	rawRec.SetString(2, "true")
	rawRec.SetString(3, "2026-08-14T15:00:00Z")
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	emitValid := func(rec *domain.GenericRecord) { validOut = rec }
	emitDLQ := func(rec *domain.DeadLetterRecord) { dlqOut = rec }
	emitAudit := func(rec *domain.AuditRecord) { auditOut = rec }

	fn.ProcessElement(context.Background(), rawRec, emitValid, emitDLQ, emitAudit)

	if dlqOut != nil {
		t.Fatalf("expected no DLQ emission, got error: %s", dlqOut.ErrorMessage)
	}
	if auditOut != nil {
		t.Fatalf("expected no audit emission for clean valid record, got: %+v", auditOut)
	}
	if validOut == nil {
		t.Fatalf("expected valid record emission, got nil")
	}
	if validOut.Values[0].Int64Val() != 12345 {
		t.Errorf("expected 12345, got %d", validOut.Values[0].Int64Val())
	}
	if validOut.Values[1].Float64Val() != 99.95 {
		t.Errorf("expected 99.95, got %f", validOut.Values[1].Float64Val())
	}
}

func TestCastAndValidateFn_InvalidRow_RoutesToDLQ(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}

	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	rawRec := domain.NewGenericRecord("test.csv", 1)
	rawRec.SetString(0, "not-a-number")
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	emitValid := func(rec *domain.GenericRecord) { validOut = rec }
	emitDLQ := func(rec *domain.DeadLetterRecord) { dlqOut = rec }
	emitAudit := func(rec *domain.AuditRecord) { auditOut = rec }

	fn.ProcessElement(context.Background(), rawRec, emitValid, emitDLQ, emitAudit)

	if validOut != nil {
		t.Fatalf("expected invalid row not to emit to valid output")
	}
	if dlqOut == nil {
		t.Fatalf("expected DLQ emission, got nil")
	}
	if dlqOut.FailedColumn != "id" {
		t.Errorf("expected failed column 'id', got %s", dlqOut.FailedColumn)
	}
	if auditOut == nil {
		t.Fatalf("expected audit emission on cast failure, got nil")
	}
	if auditOut.Severity != domain.SeverityError {
		t.Errorf("Severity: got %q, want ERROR", auditOut.Severity)
	}
}

func TestCastAndValidateFn_NullInNonNullable_RoutesToDLQ(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "required_field", Type: domain.TypeString, Nullable: false},
		},
	}

	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	rawRec := domain.NewGenericRecord("test.csv", 1)
	rawRec.SetNull(0, domain.TypeString)
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	emitValid := func(rec *domain.GenericRecord) { validOut = rec }
	emitDLQ := func(rec *domain.DeadLetterRecord) { dlqOut = rec }
	emitAudit := func(rec *domain.AuditRecord) { auditOut = rec }

	fn.ProcessElement(context.Background(), rawRec, emitValid, emitDLQ, emitAudit)

	if validOut != nil {
		t.Fatalf("expected null in non-nullable field to not emit to valid output")
	}
	if dlqOut == nil {
		t.Fatalf("expected DLQ emission on null in non-nullable field, got nil")
	}
	if dlqOut.FailedColumn != "required_field" {
		t.Errorf("expected failed column 'required_field', got %s", dlqOut.FailedColumn)
	}
	if auditOut == nil {
		t.Fatalf("expected audit emission on null constraint violation, got nil")
	}
}

func TestCastAndValidateFn_NullableField_HandlesNull(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "optional_description", Type: domain.TypeString, Nullable: true},
		},
	}

	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	rawRec := domain.NewGenericRecord("test.csv", 2)
	rawRec.SetString(0, "100")
	rawRec.SetNull(1, domain.TypeString)
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	emitValid := func(rec *domain.GenericRecord) { validOut = rec }
	emitDLQ := func(rec *domain.DeadLetterRecord) { dlqOut = rec }
	emitAudit := func(rec *domain.AuditRecord) { auditOut = rec }

	fn.ProcessElement(context.Background(), rawRec, emitValid, emitDLQ, emitAudit)

	if dlqOut != nil {
		t.Fatalf("expected no DLQ for valid nullable field, got: %s", dlqOut.ErrorMessage)
	}
	if validOut == nil {
		t.Fatalf("expected valid record emission, got nil")
	}
	if !validOut.Values[1].IsNull {
		t.Errorf("expected field 1 to be null")
	}
	if auditOut != nil {
		t.Fatalf("expected no audit event, got: %+v", auditOut)
	}
}

func TestCastAndValidateFn_TimestampFormats(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "ts", Type: domain.TypeTimestamp, Nullable: false},
		},
	}
	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	rawRec := domain.NewGenericRecord("test.csv", 1)
	rawRec.SetString(0, "2026-08-20 12:00:00")
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	fn.ProcessElement(
		context.Background(),
		rawRec,
		func(r *domain.GenericRecord) { validOut = r },
		func(d *domain.DeadLetterRecord) { dlqOut = d },
		func(a *domain.AuditRecord) { auditOut = a },
	)

	if dlqOut != nil {
		t.Fatalf("expected valid row, got DLQ: %s", dlqOut.ErrorMessage)
	}
	if validOut == nil {
		t.Fatalf("expected valid record, got nil")
	}
	if validOut.Values[0].TimeVal().Year() != 2026 {
		t.Errorf("unexpected year: %v", validOut.Values[0].TimeVal())
	}
	if auditOut != nil {
		t.Fatalf("expected no audit event for valid row, got: %+v", auditOut)
	}
}

func TestCastAndValidateFn_InvalidTimestamp_RoutesToDLQ(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "ts", Type: domain.TypeTimestamp, Nullable: false},
		},
	}
	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	rawRec := domain.NewGenericRecord("test.csv", 1)
	rawRec.SetString(0, "not-a-date")
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	fn.ProcessElement(
		context.Background(),
		rawRec,
		func(r *domain.GenericRecord) { validOut = r },
		func(d *domain.DeadLetterRecord) { dlqOut = d },
		func(a *domain.AuditRecord) { auditOut = a },
	)

	if validOut != nil {
		t.Fatalf("expected DLQ, got valid record")
	}
	if dlqOut == nil || dlqOut.FailedColumn != "ts" {
		t.Fatalf("expected DLQ for column 'ts', got %+v", dlqOut)
	}
	if auditOut == nil {
		t.Fatalf("expected audit event for invalid timestamp, got nil")
	}
}

func TestCastAndValidateFn_WithContext(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}
	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})
	rawRec := domain.NewGenericRecord("test.csv", 1)
	rawRec.SetString(0, "42")
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	fn.ProcessElement(
		context.Background(),
		rawRec,
		func(r *domain.GenericRecord) { validOut = r },
		func(d *domain.DeadLetterRecord) { dlqOut = d },
		func(a *domain.AuditRecord) { auditOut = a },
	)

	if validOut == nil || validOut.Values[0].Int64Val() != 42 {
		t.Fatalf("expected valid record with id=42, got %v", validOut)
	}
	if dlqOut != nil {
		t.Fatalf("unexpected DLQ record: %s", dlqOut.ErrorMessage)
	}
	if auditOut != nil {
		t.Fatalf("expected no audit event, got: %+v", auditOut)
	}
}

func TestCastAndValidateFn_CastFailure_EmitsAudit(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}
	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	rawRec := domain.NewGenericRecord("test.csv", 1)
	rawRec.SetString(0, "not-a-number")
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	fn.ProcessElement(
		context.Background(), rawRec,
		func(r *domain.GenericRecord) { validOut = r },
		func(d *domain.DeadLetterRecord) { dlqOut = d },
		func(a *domain.AuditRecord) { auditOut = a },
	)

	if validOut != nil {
		t.Fatalf("expected no valid emission")
	}
	if dlqOut == nil {
		t.Fatal("expected DLQ emission")
	}
	if auditOut == nil {
		t.Fatal("expected audit emission on cast failure")
	}
	if auditOut.Severity != domain.SeverityError {
		t.Errorf("Severity: got %q, want ERROR", auditOut.Severity)
	}
	if auditOut.EventType != "type_cast_failure" {
		t.Errorf("EventType: got %q, want type_cast_failure", auditOut.EventType)
	}
	if auditOut.ColumnName != "id" {
		t.Errorf("ColumnName: got %q, want id", auditOut.ColumnName)
	}
	if auditOut.SampleValue != "not-a-number" {
		t.Errorf("SampleValue: got %q, want not-a-number", auditOut.SampleValue)
	}
}

func TestCastAndValidateFn_QualityWarning_EmitsAudit(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "name", Type: domain.TypeString, Nullable: true},
		},
	}
	quality := domain.QualityConfig{
		Rules: []domain.QualityRule{
			{Type: "not_null", Column: "name"},
		},
	}
	fn := NewCastAndValidateFn(schema, quality)

	rawRec := domain.NewGenericRecord("test.csv", 1)
	rawRec.SetNull(0, domain.TypeString)
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditEvents []*domain.AuditRecord

	fn.ProcessElement(
		context.Background(), rawRec,
		func(r *domain.GenericRecord) { validOut = r },
		func(d *domain.DeadLetterRecord) { dlqOut = d },
		func(a *domain.AuditRecord) { auditEvents = append(auditEvents, a) },
	)

	if dlqOut != nil {
		t.Fatalf("expected no DLQ for nullable field, got: %s", dlqOut.ErrorMessage)
	}
	if validOut == nil {
		t.Fatal("expected valid emission")
	}
	if len(auditEvents) == 0 {
		t.Fatal("expected at least one audit event for quality warning")
	}
	if auditEvents[0].Severity != domain.SeverityWarning {
		t.Errorf("Severity: got %q, want WARNING", auditEvents[0].Severity)
	}
	if auditEvents[0].EventType != "quality_warning" {
		t.Errorf("EventType: got %q, want quality_warning", auditEvents[0].EventType)
	}
}

func TestCastAndValidateFn_ValidRow_NoAudit(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
		},
	}
	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	rawRec := domain.NewGenericRecord("test.csv", 1)
	rawRec.SetString(0, "42")
	rawRec.AuditFields["_source_file"] = "test.csv"

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord
	var auditOut *domain.AuditRecord

	fn.ProcessElement(
		context.Background(), rawRec,
		func(r *domain.GenericRecord) { validOut = r },
		func(d *domain.DeadLetterRecord) { dlqOut = d },
		func(a *domain.AuditRecord) { auditOut = a },
	)

	if validOut == nil || validOut.Values[0].Int64Val() != 42 {
		t.Fatalf("expected valid record with id=42, got %v", validOut)
	}
	if dlqOut != nil {
		t.Fatalf("unexpected DLQ: %s", dlqOut.ErrorMessage)
	}
	if auditOut != nil {
		t.Fatalf("expected no audit event for valid row, got: %+v", auditOut)
	}
}

func TestCastAndValidateFn_DecimalCasting(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "price", Type: domain.TypeDecimal, Scale: 2, Nullable: false},
			{Name: "discount", Type: domain.TypeDecimal, Scale: 2, Nullable: true},
		},
	}

	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})
	_ = fn.Setup(context.Background())

	rec := domain.NewGenericRecord("in_schema", 3)
	rec.SetString(0, "101")
	rec.SetString(1, "149.99")
	rec.SetString(2, "")

	var validOut *domain.GenericRecord
	var dlqOut *domain.DeadLetterRecord

	fn.ProcessElement(
		context.Background(),
		rec,
		func(v *domain.GenericRecord) { validOut = v },
		func(d *domain.DeadLetterRecord) { dlqOut = d },
		func(a *domain.AuditRecord) {},
	)

	if dlqOut != nil {
		t.Fatalf("unexpected DLQ record: %v", dlqOut.ErrorMessage)
	}
	if validOut == nil {
		t.Fatal("expected valid record, got nil")
	}

	m, s := validOut.Get(1).DecimalVal()
	if m != 14999 || s != 2 {
		t.Errorf("expected (14999, 2), got (%d, %d)", m, s)
	}
	if !validOut.Get(2).IsNull {
		t.Errorf("expected nullable discount to be null, got %v", validOut.Get(2))
	}
}

func TestCastAndValidateFn_RawTypedValues_AndErrors(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "int_col", Type: domain.TypeInt64, Nullable: false},
			{Name: "float_col", Type: domain.TypeFloat64, Nullable: false},
			{Name: "bool_col", Type: domain.TypeBool, Nullable: false},
			{Name: "time_col", Type: domain.TypeTimestamp, Nullable: false},
			{Name: "str_col", Type: domain.TypeString, Nullable: false},
		},
	}
	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})

	t.Run("Non-string typed values coerced properly", func(t *testing.T) {
		rawRec := domain.NewGenericRecord("test.csv", 5)
		rawRec.SetInt64(0, 100)
		rawRec.SetFloat64(1, 200.5)
		rawRec.SetBool(2, true)
		rawRec.SetString(3, "2026-08-20")
		rawRec.SetInt64(4, 999) // int64 coerced to string

		var validOut *domain.GenericRecord
		var dlqOut *domain.DeadLetterRecord

		fn.ProcessElement(
			context.Background(), rawRec,
			func(r *domain.GenericRecord) { validOut = r },
			func(d *domain.DeadLetterRecord) { dlqOut = d },
			func(a *domain.AuditRecord) {},
		)

		if dlqOut != nil {
			t.Fatalf("unexpected DLQ: %s", dlqOut.ErrorMessage)
		}
		if validOut == nil {
			t.Fatal("expected valid output")
		}
		if validOut.Values[4].StringVal() != "999" {
			t.Errorf("expected string '999', got %s", validOut.Values[4].StringVal())
		}
	})

	t.Run("Invalid float/bool/timestamp conversions route to DLQ", func(t *testing.T) {
		cases := []struct {
			colIdx int
			val    string
		}{
			{1, "invalid_float"},
			{2, "invalid_bool"},
			{3, "invalid_timestamp"},
		}

		for _, tc := range cases {
			rawRec := domain.NewGenericRecord("test.csv", 5)
			rawRec.SetInt64(0, 1)
			rawRec.SetFloat64(1, 1.1)
			rawRec.SetBool(2, true)
			rawRec.SetString(3, "2026-08-20")
			rawRec.SetString(4, "hello")
			rawRec.SetString(tc.colIdx, tc.val)

			var dlqOut *domain.DeadLetterRecord
			fn.ProcessElement(
				context.Background(), rawRec,
				func(r *domain.GenericRecord) {},
				func(d *domain.DeadLetterRecord) { dlqOut = d },
				func(a *domain.AuditRecord) {},
			)

			if dlqOut == nil {
				t.Errorf("expected DLQ for column %d with value %q, got nil", tc.colIdx, tc.val)
			}
		}
	})

	t.Run("Native type passthrough and nullable empty strings", func(t *testing.T) {
		passthroughSchema := domain.Schema{
			Fields: []domain.Field{
				{Name: "raw_bytes", Type: domain.TypeBytes, Nullable: false},
				{Name: "opt_str", Type: domain.TypeString, Nullable: true},
			},
		}
		fnPass := NewCastAndValidateFn(passthroughSchema, domain.QualityConfig{})

		rawRec := domain.NewGenericRecord("native", 2)
		rawRec.SetBytes(0, []byte("my-bytes"))
		rawRec.SetString(1, "") // empty string on nullable field

		var validOut *domain.GenericRecord
		fnPass.ProcessElement(
			context.Background(), rawRec,
			func(r *domain.GenericRecord) { validOut = r },
			func(d *domain.DeadLetterRecord) { t.Fatalf("unexpected DLQ: %s", d.ErrorMessage) },
			func(a *domain.AuditRecord) {},
		)

		if validOut == nil {
			t.Fatal("expected valid output")
		}
		if string(validOut.Values[0].BytesVal()) != "my-bytes" {
			t.Errorf("expected 'my-bytes', got %s", string(validOut.Values[0].BytesVal()))
		}
		if validOut.Values[1].StringVal() != "" {
			t.Errorf("expected empty string, got %s", validOut.Values[1].StringVal())
		}
	})
}

func BenchmarkCastAndValidateFn_ProcessElement(b *testing.B) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "amount", Type: domain.TypeDecimal, Scale: 2, OnOverflow: "round", Nullable: true},
			{Name: "active", Type: domain.TypeBool, Nullable: false},
			{Name: "name", Type: domain.TypeString, Nullable: true},
		},
	}
	schema.Index()
	fn := NewCastAndValidateFn(schema, domain.QualityConfig{})
	_ = fn.Setup(context.Background())

	rec := domain.NewGenericRecord("benchmark", 4)
	rec.SetString(0, "1234567")
	rec.SetString(1, "99.95")
	rec.SetString(2, "true")
	rec.SetString(3, "John Doe")

	emitValid := func(*domain.GenericRecord) {}
	emitDLQ := func(*domain.DeadLetterRecord) {}
	emitAudit := func(*domain.AuditRecord) {}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		fn.ProcessElement(context.Background(), rec, emitValid, emitDLQ, emitAudit)
	}
}

func TestCastAndValidateFn_DLQPayloadSanitized(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "user_password", Type: domain.TypeString, Nullable: false},
			{Name: "order_id", Type: domain.TypeInt64, Nullable: false},
		},
	}
	quality := domain.QualityConfig{}
	security := domain.SecurityConfig{SensitiveFields: []string{"user_password"}}

	fn := NewCastAndValidateFn(schema, quality, security)
	ctx := context.Background()
	_ = fn.Setup(ctx)

	rec := domain.NewGenericRecord("test", 2)
	rec.SetString(0, "my_secret_pass")
	rec.SetString(1, "not_an_int") // will fail type cast → DLQ

	var dlqRecord *domain.DeadLetterRecord
	fn.ProcessElement(ctx, rec,
		func(*domain.GenericRecord) {},
		func(d *domain.DeadLetterRecord) { dlqRecord = d },
		func(*domain.AuditRecord) {},
	)

	if dlqRecord == nil {
		t.Fatal("expected a DLQ record for type cast failure")
	}
	if !pkgtelemetry.ContainsRedacted(dlqRecord.RawPayload) {
		t.Errorf("expected DLQ payload to be sanitized, got: %s", dlqRecord.RawPayload)
	}
}

func TestCastAndValidateFn_ProcessElement_NilPlanRoutesDLQ(t *testing.T) {
	// Deliberately construct struct directly so fn.plan stays nil
	fn := &CastAndValidateFn{
		Schema: domain.Schema{
			Fields: []domain.Field{{Name: "id", Type: domain.TypeInt64}},
		},
	}
	rec := domain.NewGenericRecord("test", 1)
	rec.SetInt64(0, 42)

	var emittedDLQ *domain.DeadLetterRecord
	fn.ProcessElement(
		context.Background(), rec,
		func(r *domain.GenericRecord) {},
		func(d *domain.DeadLetterRecord) { emittedDLQ = d },
		func(a *domain.AuditRecord) {},
	)

	if emittedDLQ == nil {
		t.Fatal("expected a DLQ record when plan is nil — fail-fast lifecycle guard missing")
	}
	if emittedDLQ.FailedColumn != "_setup" {
		t.Errorf("expected FailedColumn='_setup', got %q", emittedDLQ.FailedColumn)
	}
}

