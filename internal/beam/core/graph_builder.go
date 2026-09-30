// Package core contains the application-layer orchestration and DAG construction for Apache Beam pipelines.
// DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
package core

import (
	"bytes"
	"encoding/json"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*domain.AuditRecord)(nil)).Elem())

	beam.RegisterType(reflect.TypeOf((*ports.PartitionSlice)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*ports.PartitionSlice)(nil)).Elem(), encPartitionSlice, decPartitionSlice)

	beam.RegisterType(reflect.TypeOf((*ports.PageSlice)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*ports.PageSlice)(nil)).Elem(), encPageSlice, decPageSlice)

	beam.RegisterType(reflect.TypeOf((*domain.GenericRecord)(nil)))
	beam.RegisterCoder(reflect.TypeOf((*domain.GenericRecord)(nil)), encGenericRecordPtr, decGenericRecordPtr)
	beam.RegisterType(reflect.TypeOf((*domain.GenericRecord)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*domain.GenericRecord)(nil)).Elem(), encGenericRecord, decGenericRecord)

	beam.RegisterType(reflect.TypeOf((*domain.DeadLetterRecord)(nil)))
	beam.RegisterCoder(reflect.TypeOf((*domain.DeadLetterRecord)(nil)), encDeadLetterRecordPtr, decDeadLetterRecordPtr)
	beam.RegisterType(reflect.TypeOf((*domain.DeadLetterRecord)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*domain.DeadLetterRecord)(nil)).Elem(), encDeadLetterRecord, decDeadLetterRecord)

	beam.RegisterType(reflect.TypeOf((*domain.PipelineMetrics)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*domain.PipelineMetrics)(nil)).Elem(), encPipelineMetrics, decPipelineMetrics)

	beam.RegisterFunction(filterNonNilRecordFn)
	beam.RegisterFunction(validRecordMetricKVFn)
	beam.RegisterFunction(dlqRecordMetricKVFn)
	beam.RegisterFunction(originKVToSummaryFn)
}

func encPipelineMetrics(v domain.PipelineMetrics) ([]byte, error) {
	var buf bytes.Buffer
	err := EncodePipelineMetrics(v, &buf)
	return buf.Bytes(), err
}

func decPipelineMetrics(data []byte) (domain.PipelineMetrics, error) {
	return DecodePipelineMetrics(bytes.NewReader(data))
}

func validRecordMetricKVFn(rec *domain.GenericRecord) (string, bool) {
	origin := "default"
	if rec != nil && rec.AuditFields != nil && rec.AuditFields["_source_file"] != "" {
		origin = rec.AuditFields["_source_file"]
	} else if rec != nil && rec.SchemaID != "" {
		origin = rec.SchemaID
	}
	return origin, false
}

func dlqRecordMetricKVFn(rec *domain.DeadLetterRecord) (string, bool) {
	origin := "unknown"
	if rec != nil && rec.SourceFile != "" {
		origin = rec.SourceFile
	}
	return origin, true
}

func originKVToSummaryFn(origin string, om domain.OriginMetric) OriginSummary {
	return OriginSummary{Origin: origin, Metric: om}
}

// filterNonNilRecordFn drops nil records before passing them to the validation stage.
// Uses an emitter ParDo pattern — standard and zero-reflection in Beam Go SDK.
func filterNonNilRecordFn(rec *domain.GenericRecord, emit func(*domain.GenericRecord)) {
	if rec != nil {
		emit(rec)
	}
}

// applyIngestionStage handles pre-filtering and fused metadata enrichment & schema validation.
func applyIngestionStage(
	s beam.Scope,
	rawRecords beam.PCollection,
	schema domain.Schema,
	quality domain.QualityConfig,
	sec domain.SecurityConfig,
) (valid, dlq, audit beam.PCollection) {
	nonNil := beam.ParDo(s.Scope("FilterNils"), filterNonNilRecordFn, rawRecords)
	return beam.ParDo3(s.Scope("EnrichAndValidate"), NewFusedEnrichAndValidateFn(schema, "", "", quality, sec), nonNil)
}

// applySinkStage routes valid, DLQ and audit records to their destination builders.
func applySinkStage(
	s beam.Scope,
	valid, dlq, audit beam.PCollection,
	sink ports.BeamSinkBuilder,
	dlqSink ports.BeamDLQSinkBuilder,
	auditSink ports.BeamAuditSinkBuilder,
) {
	sink.BuildSink(s.Scope("ValidSink"), valid)
	dlqSink.BuildDLQ(s.Scope("DLQSink"), dlq)
	if auditSink != nil {
		auditSink.BuildAuditSink(s.Scope("AuditSink"), audit)
	}
}

// applyMetricsStage executes a two-stage aggregation:
// 1. CombinePerKey lifts per-origin counters locally on workers (zero map allocations).
// 2. Combine aggregates the resulting summaries into a guaranteed singleton domain.PipelineMetrics.
func applyMetricsStage(s beam.Scope, valid, dlq beam.PCollection) beam.PCollection {
	validKV := beam.ParDo(s.Scope("ValidMetricKV"), validRecordMetricKVFn, valid)
	dlqKV := beam.ParDo(s.Scope("DLQMetricKV"), dlqRecordMetricKVFn, dlq)
	allKV := beam.Flatten(s.Scope("FlattenMetricKVs"), validKV, dlqKV)

	perOrigin := beam.CombinePerKey(s.Scope("CombinePerOrigin"), &OriginCombinerFn{}, allKV)
	summaries := beam.ParDo(s.Scope("ToOriginSummary"), originKVToSummaryFn, perOrigin)
	return beam.Combine(s.Scope("CombineMetricsSummary"), &MetricsSummaryCombinerFn{}, summaries)
}

func encPartitionSlice(v ports.PartitionSlice) ([]byte, error) {
	return json.Marshal(v)
}

func decPartitionSlice(data []byte) (ports.PartitionSlice, error) {
	var v ports.PartitionSlice
	err := json.Unmarshal(data, &v)
	return v, err
}

func encPageSlice(v ports.PageSlice) ([]byte, error) {
	return json.Marshal(v)
}

func decPageSlice(data []byte) (ports.PageSlice, error) {
	var v ports.PageSlice
	err := json.Unmarshal(data, &v)
	return v, err
}

// DefaultDLQBeamSink builds a standard DLQ sink in the Beam DAG.
type DefaultDLQBeamSink struct {
	Storage ports.StorageWriter
	DLQPath string
}

// Compile-time assertion: DefaultDLQBeamSink must satisfy ports.BeamDLQSinkBuilder (LSP).
var _ ports.BeamDLQSinkBuilder = (*DefaultDLQBeamSink)(nil)

// BuildDLQ builds the DLQ sink transform.
func (s *DefaultDLQBeamSink) BuildDLQ(scope beam.Scope, dlqRecords beam.PCollection) {
	beam.ParDo0(scope, NewDLQSinkDoFn(s.Storage, s.DLQPath), dlqRecords)
}

// DefaultAuditBeamSink builds the standard AuditSinkDoFn sink in the Beam DAG.
type DefaultAuditBeamSink struct {
	Storage  ports.StorageWriter
	AuditDir string
}

// Compile-time assertion: DefaultAuditBeamSink must satisfy ports.BeamAuditSinkBuilder (LSP).
var _ ports.BeamAuditSinkBuilder = (*DefaultAuditBeamSink)(nil)

// BuildAuditSink implements ports.BeamAuditSinkBuilder.
func (s *DefaultAuditBeamSink) BuildAuditSink(scope beam.Scope, auditEvents beam.PCollection) {
	if s.Storage == nil || s.AuditDir == "" {
		return
	}
	beam.ParDo0(scope, NewAuditSinkDoFn(s.Storage, s.AuditDir), auditEvents)
}

// BuildPipeline constructs the unified Apache Beam DAG for any distributed source and sink.
// Returns a PCollection of domain.PipelineMetrics for combiner-lifted aggregation.
// auditSink is optional — pass nil to disable audit event persistence.
func BuildPipeline(
	p *beam.Pipeline,
	source ports.BeamSourceBuilder,
	sink ports.BeamSinkBuilder,
	dlqSink ports.BeamDLQSinkBuilder,
	auditSink ports.BeamAuditSinkBuilder,
	schema domain.Schema,
	quality domain.QualityConfig,
	security ...domain.SecurityConfig,
) beam.PCollection {
	s := p.Root()

	var sec domain.SecurityConfig
	if len(security) > 0 {
		sec = security[0]
	}

	// Composite: Source Stage
	rawRecords := source.BuildSource(s.Scope("SourceStage"))

	// Composite: Ingestion Stage (filter nils + fused enrich & validate)
	valid, dlq, audit := applyIngestionStage(s.Scope("IngestionStage"), rawRecords, schema, quality, sec)

	// Composite: Sink Stage (valid, DLQ, audit)
	applySinkStage(s.Scope("SinkStage"), valid, dlq, audit, sink, dlqSink, auditSink)

	// Composite: Metrics Stage (CombinePerKey + global summary combine)
	return applyMetricsStage(s.Scope("MetricsStage"), valid, dlq)
}
