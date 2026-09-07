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
	beam.RegisterType(reflect.TypeOf((*CastAndValidateFn)(nil)).Elem())
}

// Package-level metric handles. Declared at package scope so Beam's
// worker serialization does not zero them out when crossing the wire.
var (
	validRecordsCounter   = metrics.NewCounter("omnibeam", "valid_records")
	dlqRecordsCounter     = metrics.NewCounter("omnibeam", "dlq_records")
	auditEventsCounter    = metrics.NewCounter("omnibeam", "audit_events")
	validationLatencyDist = metrics.NewDistribution("omnibeam", "validation_latency_ms")
)

type fieldCastingPlan func(
	rawVal *domain.FieldValue,
	validated *domain.GenericRecord,
	i int,
	field domain.Field,
	fail func(col, msg, sample string),
) bool

// CastAndValidateFn casts and validates GenericRecords, emitting to valid, DLQ, and audit streams.
type CastAndValidateFn struct {
	Schema         domain.Schema         `json:"schema"`
	QualityConfig  domain.QualityConfig  `json:"quality_config"`
	SecurityConfig domain.SecurityConfig `json:"security_config"`

	plan []fieldCastingPlan `json:"-"`
}

// NewCastAndValidateFn constructs a CastAndValidateFn and initializes its plan for unit testing.
func NewCastAndValidateFn(schema domain.Schema, quality domain.QualityConfig, security ...domain.SecurityConfig) *CastAndValidateFn {
	fn := &CastAndValidateFn{Schema: schema, QualityConfig: quality}
	if len(security) > 0 {
		fn.SecurityConfig = security[0]
	}
	_ = fn.Setup(context.Background())
	return fn
}

func (fn *CastAndValidateFn) Setup(_ context.Context) error {
	fn.Schema.Index()
	fn.plan = make([]fieldCastingPlan, len(fn.Schema.Fields))
	for i, f := range fn.Schema.Fields {
		fn.plan[i] = fn.compileFieldPlan(f)
	}
	return nil
}

func (fn *CastAndValidateFn) compileFieldPlan(field domain.Field) fieldCastingPlan {
	// Switch runs ONCE at Setup() time — never inside ProcessElement.
	switch field.Type {
	case domain.TypeInt64:
		return func(rawVal *domain.FieldValue, validated *domain.GenericRecord, i int, f domain.Field, fail func(col, msg, sample string)) bool {
			if rawVal == nil || rawVal.IsNull {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeInt64)
				return true
			}
			if rawVal.Type == domain.TypeInt64 {
				validated.Values[i] = *rawVal
				return true
			}
			if rawVal.Type == domain.TypeFloat64 {
				validated.SetInt64(i, int64(rawVal.Float64Val()))
				return true
			}
			str := rawVal.String()
			if str == "" {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeInt64)
				return true
			}
			v, err := domain.CoerceInt64Fast(str)
			if err != nil {
				return fn.failCast(f, str, err, fail)
			}
			validated.SetInt64(i, v)
			return true
		}

	case domain.TypeDecimal:
		scale := field.Scale
		overflow := field.OnOverflow
		return func(rawVal *domain.FieldValue, validated *domain.GenericRecord, i int, f domain.Field, fail func(col, msg, sample string)) bool {
			if rawVal == nil || rawVal.IsNull {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeDecimal)
				return true
			}
			if rawVal.Type == domain.TypeDecimal && rawVal.Scale == scale {
				validated.Values[i] = *rawVal
				return true
			}
			if rawVal.Type == domain.TypeInt64 {
				validated.SetDecimal(i, rawVal.Int64Val(), 0)
				return true
			}
			str := rawVal.String()
			if str == "" {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeDecimal)
				return true
			}
			m, s, err := domain.CoerceDecimalFast(str, scale, overflow)
			if err != nil {
				return fn.failCast(f, str, err, fail)
			}
			validated.SetDecimal(i, m, s)
			return true
		}

	case domain.TypeFloat64:
		return func(rawVal *domain.FieldValue, validated *domain.GenericRecord, i int, f domain.Field, fail func(col, msg, sample string)) bool {
			if rawVal == nil || rawVal.IsNull {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeFloat64)
				return true
			}
			if rawVal.Type == domain.TypeFloat64 {
				validated.Values[i] = *rawVal
				return true
			}
			if rawVal.Type == domain.TypeInt64 {
				validated.SetFloat64(i, float64(rawVal.Int64Val()))
				return true
			}
			str := rawVal.String()
			if str == "" {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeFloat64)
				return true
			}
			v, err := domain.CoerceFloat64Fast(str)
			if err != nil {
				return fn.failCast(f, str, err, fail)
			}
			validated.SetFloat64(i, v)
			return true
		}

	case domain.TypeBool:
		return func(rawVal *domain.FieldValue, validated *domain.GenericRecord, i int, f domain.Field, fail func(col, msg, sample string)) bool {
			if rawVal == nil || rawVal.IsNull {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeBool)
				return true
			}
			if rawVal.Type == domain.TypeBool {
				validated.Values[i] = *rawVal
				return true
			}
			str := rawVal.String()
			if str == "" {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeBool)
				return true
			}
			v, err := domain.CoerceBoolFast(str)
			if err != nil {
				return fn.failCast(f, str, err, fail)
			}
			validated.SetBool(i, v)
			return true
		}

	case domain.TypeTimestamp:
		return func(rawVal *domain.FieldValue, validated *domain.GenericRecord, i int, f domain.Field, fail func(col, msg, sample string)) bool {
			if rawVal == nil || rawVal.IsNull {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeTimestamp)
				return true
			}
			if rawVal.Type == domain.TypeTimestamp {
				validated.Values[i] = *rawVal
				return true
			}
			str := rawVal.String()
			if str == "" {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeTimestamp)
				return true
			}
			t, err := domain.CoerceTimestampFast(str)
			if err != nil {
				return fn.failCast(f, str, err, fail)
			}
			validated.SetTimestamp(i, t)
			return true
		}

	case domain.TypeBytes:
		return func(rawVal *domain.FieldValue, validated *domain.GenericRecord, i int, f domain.Field, fail func(col, msg, sample string)) bool {
			if rawVal == nil || rawVal.IsNull {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeBytes)
				return true
			}
			validated.SetBytes(i, []byte(rawVal.String()))
			return true
		}

	default: // TypeString and any unknown type
		return func(rawVal *domain.FieldValue, validated *domain.GenericRecord, i int, f domain.Field, fail func(col, msg, sample string)) bool {
			if rawVal == nil || rawVal.IsNull {
				if !f.Nullable {
					return fn.failNull(f, fail)
				}
				validated.SetNull(i, domain.TypeString)
				return true
			}
			validated.SetString(i, rawVal.String())
			return true
		}
	}
}

// ProcessElement casts and validates a GenericRecord, emitting to valid, DLQ, or audit outputs.
func (fn *CastAndValidateFn) ProcessElement(
	ctx context.Context,
	rec *domain.GenericRecord,
	emitValid func(*domain.GenericRecord),
	emitDLQ func(*domain.DeadLetterRecord),
	emitAudit func(*domain.AuditRecord),
) {
	if fn.plan == nil {
		emitDLQ(&domain.DeadLetterRecord{
			ErrorMessage: "CastAndValidateFn: plan not compiled — Setup() must be called before ProcessElement()",
			FailedColumn: "_setup",
			FailedAt:     time.Now().UTC(),
		})
		emitAudit(domain.NewAuditRecord(
			"lifecycle_violation", domain.SeverityError,
			"_setup", "beam_lifecycle",
			"CastAndValidateFn.ProcessElement called without prior Setup — check beam.RegisterDoFn and beam.RegisterInit",
			"",
		))
		return
	}
	start := time.Now()
	validated := domain.NewGenericRecord(rec.SchemaID, len(fn.Schema.Fields))
	validated.AuditFields = rec.AuditFields

	fail := func(col, msg, sample string) {
		dlqRecordsCounter.Inc(ctx, 1)
		auditEventsCounter.Inc(ctx, 1)

		vals := make([]string, len(rec.Values))
		for idx, v := range rec.Values {
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
			SourceFile:   rec.AuditFields["_source_file"],
			FailedAt:     time.Now().UTC(),
		})
		emitAudit(domain.NewAuditRecord("type_cast_failure", domain.SeverityError, col, "type_cast", msg, sanitizedSample))
	}

	for i, f := range fn.Schema.Fields {
		var raw *domain.FieldValue
		if i < len(rec.Values) {
			raw = &rec.Values[i]
		}
		if !fn.plan[i](raw, validated, i, f, fail) {
			return
		}
	}

	// Evaluate soft quality rules — emit audit warning without routing to DLQ.
	for _, rule := range fn.QualityConfig.Rules {
		if rule.Type == "not_null" && rule.Column != "" {
			for i, field := range fn.Schema.Fields {
				if field.Name == rule.Column && i < len(validated.Values) && validated.Values[i].IsNull {
					auditEventsCounter.Inc(ctx, 1)
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

	validRecordsCounter.Inc(ctx, 1)
	validationLatencyDist.Update(ctx, time.Since(start).Milliseconds())
	emitValid(validated)
}

func (fn *CastAndValidateFn) failNull(field domain.Field, fail func(col, msg, sample string)) bool {
	nullErr := &domain.FieldValidationError{
		Field: field.Name,
		Value: "",
		Type:  field.Type,
		Err:   domain.ErrNullConstraint,
	}
	fail(field.Name, nullErr.Error(), "")
	return false
}

func (fn *CastAndValidateFn) failCast(field domain.Field, str string, err error, fail func(col, msg, sample string)) bool {
	castErr := &domain.FieldValidationError{
		Field: field.Name,
		Value: str,
		Type:  field.Type,
		Err:   fmt.Errorf("%w: %w", domain.ErrTypeCastFailed, err),
	}
	fail(field.Name, castErr.Error(), str)
	return false
}
