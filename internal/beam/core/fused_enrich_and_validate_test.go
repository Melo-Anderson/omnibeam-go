package core_test

import (
	"context"
	"testing"

	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func TestFusedEnrichAndValidateFn_ProcessElement_ValidRecord(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "amount", Type: domain.TypeFloat64, Nullable: false},
		},
	}
	schema.Index()

	fn := core.NewFusedEnrichAndValidateFn(schema, "fused-job-1", "exec-1", domain.QualityConfig{}, domain.SecurityConfig{})
	if err := fn.Setup(context.Background()); err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	recValid := domain.NewGenericRecord("schema-1", 2)
	recValid.SetString(0, "100")
	recValid.SetString(1, "50.25")

	var emittedValid *domain.GenericRecord
	var emittedDLQ *domain.DeadLetterRecord
	var emittedAudit *domain.AuditRecord

	fn.ProcessElement(
		context.Background(),
		recValid,
		func(r *domain.GenericRecord) { emittedValid = r },
		func(d *domain.DeadLetterRecord) { emittedDLQ = d },
		func(a *domain.AuditRecord) { emittedAudit = a },
	)

	if emittedValid == nil {
		t.Fatalf("expected record to be valid")
	}
	if emittedValid.Values[0].Int64Val() != 100 || emittedValid.Values[1].Float64Val() != 50.25 {
		t.Fatalf("coerced values mismatch: %+v", emittedValid.Values)
	}
	if emittedValid.AuditFields["_ingested_at"] == "" {
		t.Fatalf("expected _ingested_at to be set: %+v", emittedValid.AuditFields)
	}
	if emittedValid.AuditFields["_job_id"] != "fused-job-1" {
		t.Fatalf("expected _job_id audit field: %+v", emittedValid.AuditFields)
	}
	if emittedValid.AuditFields["_execution_id"] != "exec-1" {
		t.Fatalf("expected _execution_id audit field: %+v", emittedValid.AuditFields)
	}
	if emittedDLQ != nil {
		t.Fatalf("expected no DLQ emission for valid record")
	}
	if emittedAudit != nil {
		t.Fatalf("expected no audit error for valid record")
	}
}

func TestFusedEnrichAndValidateFn_ProcessElement_DLQAndRedaction(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: false},
			{Name: "ssn", Type: domain.TypeString, Nullable: false},
		},
	}
	schema.Index()

	sec := domain.SecurityConfig{SensitiveFields: []string{"ssn"}}
	fn := core.NewFusedEnrichAndValidateFn(schema, "job-dlq", "exec-dlq", domain.QualityConfig{}, sec)
	_ = fn.Setup(context.Background())

	recInvalid := domain.NewGenericRecord("schema-1", 2)
	recInvalid.SetString(0, "not_an_int")
	recInvalid.SetString(1, "123-45-6789")
	recInvalid.AuditFields = map[string]string{"_source_file": "file.csv"}

	var emittedValid *domain.GenericRecord
	var emittedDLQ *domain.DeadLetterRecord
	var emittedAudit *domain.AuditRecord

	fn.ProcessElement(
		context.Background(),
		recInvalid,
		func(r *domain.GenericRecord) { emittedValid = r },
		func(d *domain.DeadLetterRecord) { emittedDLQ = d },
		func(a *domain.AuditRecord) { emittedAudit = a },
	)

	if emittedValid != nil {
		t.Fatalf("expected record to fail validation")
	}
	if emittedDLQ == nil {
		t.Fatalf("expected DLQ emission")
	}
	if emittedDLQ.FailedColumn != "id" {
		t.Fatalf("expected failed column 'id', got '%s'", emittedDLQ.FailedColumn)
	}
	if emittedDLQ.SourceFile != "file.csv" {
		t.Fatalf("expected source file 'file.csv', got '%s'", emittedDLQ.SourceFile)
	}
	if emittedAudit == nil {
		t.Fatalf("expected audit error record")
	}
	if emittedAudit.Severity != domain.SeverityError {
		t.Fatalf("expected SeverityError, got %s", emittedAudit.Severity)
	}
}

func TestFusedEnrichAndValidateFn_ProcessElement_SoftQualityWarning(t *testing.T) {
	schema := domain.Schema{
		Fields: []domain.Field{
			{Name: "id", Type: domain.TypeInt64, Nullable: true},
		},
	}
	schema.Index()

	quality := domain.QualityConfig{
		Rules: []domain.QualityRule{
			{Type: "not_null", Column: "id"},
		},
	}

	fn := core.NewFusedEnrichAndValidateFn(schema, "job-quality", "exec-quality", quality, domain.SecurityConfig{})
	_ = fn.Setup(context.Background())

	recNull := domain.NewGenericRecord("schema-1", 1)
	recNull.SetNull(0, domain.TypeInt64)

	var emittedValid *domain.GenericRecord
	var emittedDLQ *domain.DeadLetterRecord
	var emittedAudit *domain.AuditRecord

	fn.ProcessElement(
		context.Background(),
		recNull,
		func(r *domain.GenericRecord) { emittedValid = r },
		func(d *domain.DeadLetterRecord) { emittedDLQ = d },
		func(a *domain.AuditRecord) { emittedAudit = a },
	)

	if emittedValid == nil {
		t.Fatalf("expected record to still be valid despite soft warning")
	}
	if emittedDLQ != nil {
		t.Fatalf("expected no DLQ emission for soft warning")
	}
	if emittedAudit == nil {
		t.Fatalf("expected audit warning")
	}
	if emittedAudit.Severity != domain.SeverityWarning {
		t.Fatalf("expected SeverityWarning, got %s", emittedAudit.Severity)
	}
}

func TestFusedEnrichAndValidateFn_ProcessElement_NilPlanRoutesDLQ(t *testing.T) {
	fn := &core.FusedEnrichAndValidateFn{
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
		t.Fatal("expected a DLQ record when plan is nil")
	}
	if emittedDLQ.FailedColumn != "_setup" {
		t.Errorf("expected FailedColumn='_setup', got %q", emittedDLQ.FailedColumn)
	}
}
