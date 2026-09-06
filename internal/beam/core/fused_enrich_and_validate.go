package core

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	pkgtelemetry "github.com/omnibeam/dataflow-compute-go/pkg/telemetry"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*FusedEnrichAndValidateFn)(nil)).Elem())
}

var (
	fusedValidCounter          = metrics.NewCounter("omnibeam", "fused_valid_records")
	fusedDLQCounter            = metrics.NewCounter("omnibeam", "fused_dlq_records")
	fusedAuditCounter          = metrics.NewCounter("omnibeam", "fused_audit_events")
	fusedValidationLatencyDist = metrics.NewDistribution("omnibeam", "fused_validation_latency_ms")
)

// FusedEnrichAndValidateFn fuses AuditEnricherFn and CastAndValidateFn into a single ParDo,
// eliminating intermediate PCollection materialization and serialization between pipeline stages.
type FusedEnrichAndValidateFn struct {
	Schema         domain.Schema         `json:"schema"`
	QualityConfig  domain.QualityConfig  `json:"quality_config"`
	SecurityConfig domain.SecurityConfig `json:"security_config"`
	JobID          string                `json:"job_id,omitempty"`
	ExecutionID    string                `json:"execution_id,omitempty"`

	plan            []fieldCastingPlan `json:"-"`
	cachedTimestamp string             `json:"-"`
}

// NewFusedEnrichAndValidateFn constructs a FusedEnrichAndValidateFn.
func NewFusedEnrichAndValidateFn(
	schema domain.Schema,
	jobID, executionID string,
	quality domain.QualityConfig,
	security domain.SecurityConfig,
) *FusedEnrichAndValidateFn {
	fn := &FusedEnrichAndValidateFn{
		Schema:         schema,
		QualityConfig:  quality,
		SecurityConfig: security,
		JobID:          jobID,
		ExecutionID:    executionID,
	}
	_ = fn.Setup(context.Background())
	return fn
}

// Setup compiles casting plans for each schema field.
func (fn *FusedEnrichAndValidateFn) Setup(_ context.Context) error {
	fn.Schema.Index()
	cv := &CastAndValidateFn{
		Schema:         fn.Schema,
		QualityConfig:  fn.QualityConfig,
		SecurityConfig: fn.SecurityConfig,
	}
	_ = cv.Setup(context.Background())
	fn.plan = cv.plan
	return nil
}

// ProcessElement enriches metadata and validates/coerces fields in a single execution step.
func (fn *FusedEnrichAndValidateFn) ProcessElement(
	ctx context.Context,
	rec *domain.GenericRecord,
	emitValid func(*domain.GenericRecord),
	emitDLQ func(*domain.DeadLetterRecord),
	emitAudit func(*domain.AuditRecord),
) {
	if rec == nil {
		return
	}
	if fn.plan == nil {
		emitDLQ(&domain.DeadLetterRecord{
			ErrorMessage: "FusedEnrichAndValidateFn: plan not compiled — Setup() must be called before ProcessElement()",
			FailedColumn: "_setup",
			FailedAt:     time.Now().UTC(),
		})
		emitAudit(domain.NewAuditRecord(
			"lifecycle_violation", domain.SeverityError,
			"_setup", "beam_lifecycle",
			"FusedEnrichAndValidateFn.ProcessElement called without prior Setup",
			"",
		))
		return
	}
	start := time.Now()

	// 1. Audit Enrichment (in-place clone to preserve immutability across parallel threads)
	enriched := &domain.GenericRecord{
		SchemaID:    rec.SchemaID,
		Values:      rec.Values,
		AuditFields: make(map[string]string, len(rec.AuditFields)+3),
	}
	for k, v := range rec.AuditFields {
		enriched.AuditFields[k] = v
	}
	if _, exists := enriched.AuditFields["_ingested_at"]; !exists {
		enriched.AuditFields["_ingested_at"] = time.Now().UTC().Format(time.RFC3339)
	}
	if fn.JobID != "" {
		enriched.AuditFields["_job_id"] = fn.JobID
	}
	if fn.ExecutionID != "" {
		enriched.AuditFields["_execution_id"] = fn.ExecutionID
	}

	// 2. Schema Validation & Type Coercion in same cache cycle
	validated := domain.NewGenericRecord(enriched.SchemaID, len(fn.Schema.Fields))
	validated.AuditFields = enriched.AuditFields

	fail := func(col, msg, sample string) {
		fusedDLQCounter.Inc(ctx, 1)
		fusedAuditCounter.Inc(ctx, 1)

		vals := make([]string, len(enriched.Values))
		for idx, v := range enriched.Values {
			if idx < len(fn.Schema.Fields) && pkgtelemetry.IsSensitiveKey(fn.Schema.Fields[idx].Name, fn.SecurityConfig.SensitiveFields) {
				vals[idx] = "[REDACTED]"
			} else {
				vals[idx] = fmt.Sprintf("%v", v)
			}
		}
		rawPayload := pkgtelemetry.SanitizePayload(fmt.Sprintf("%v", vals), fn.SecurityConfig.SensitiveFields)
		sanitizedSample := sample
		if pkgtelemetry.IsSensitiveKey(col, fn.SecurityConfig.SensitiveFields) {
			sanitizedSample = "[REDACTED]"
		}

		emitDLQ(&domain.DeadLetterRecord{
			RawPayload:   rawPayload,
			ErrorMessage: msg,
			FailedColumn: col,
			SourceFile:   enriched.AuditFields["_source_file"],
			FailedAt:     time.Now().UTC(),
		})
		emitAudit(domain.NewAuditRecord("type_cast_failure", domain.SeverityError, col, "type_cast", msg, sanitizedSample))
	}

	for i, f := range fn.Schema.Fields {
		var raw *domain.FieldValue
		if i < len(enriched.Values) {
			raw = &enriched.Values[i]
		}
		if !fn.plan[i](raw, validated, i, f, fail) {
			return
		}
	}

	// 3. Evaluate soft quality rules — emit audit warning without routing to DLQ.
	for _, rule := range fn.QualityConfig.Rules {
		if rule.Type == "not_null" && rule.Column != "" {
			for i, field := range fn.Schema.Fields {
				if field.Name == rule.Column && i < len(validated.Values) && validated.Values[i].IsNull {
					fusedAuditCounter.Inc(ctx, 1)
					emitAudit(domain.NewAuditRecord(
						"quality_warning", domain.SeverityWarning,
						field.Name, rule.Type,
						fmt.Sprintf("soft quality warning: column %q is null", field.Name),
						"",
					))
				}
			}
		}
	}

	fusedValidCounter.Inc(ctx, 1)
	fusedValidationLatencyDist.Update(ctx, time.Since(start).Milliseconds())
	emitValid(validated)
}
