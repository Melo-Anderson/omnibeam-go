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

	beam.RegisterType(reflect.TypeOf((*domain.GenericRecord)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*domain.GenericRecord)(nil)).Elem(), encGenericRecord, decGenericRecord)

	beam.RegisterType(reflect.TypeOf((*domain.DeadLetterRecord)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*domain.DeadLetterRecord)(nil)).Elem(), encDeadLetterRecord, decDeadLetterRecord)

	beam.RegisterType(reflect.TypeOf((*domain.PipelineMetrics)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*domain.PipelineMetrics)(nil)).Elem(), encPipelineMetrics, decPipelineMetrics)

	beam.RegisterFunction(validRecordMetricItemFn)
	beam.RegisterFunction(dlqRecordMetricItemFn)
	beam.RegisterFunction(validRecordFlagFn)
	beam.RegisterFunction(dlqRecordFlagFn)
}

func encPipelineMetrics(v domain.PipelineMetrics) ([]byte, error) {
	var buf bytes.Buffer
	err := EncodePipelineMetrics(v, &buf)
	return buf.Bytes(), err
}

func decPipelineMetrics(data []byte) (domain.PipelineMetrics, error) {
	return DecodePipelineMetrics(bytes.NewReader(data))
}

func validRecordMetricItemFn(rec *domain.GenericRecord) MetricItem {
	origin := "default"
	if rec != nil && rec.AuditFields != nil && rec.AuditFields["_source_file"] != "" {
		origin = rec.AuditFields["_source_file"]
	} else if rec != nil && rec.SchemaID != "" {
		origin = rec.SchemaID
	}
	return MetricItem{
		Origin: origin,
		IsDLQ:  false,
	}
}

func dlqRecordMetricItemFn(rec *domain.DeadLetterRecord) MetricItem {
	origin := "unknown"
	if rec != nil && rec.SourceFile != "" {
		origin = rec.SourceFile
	}
	return MetricItem{
		Origin: origin,
		IsDLQ:  true,
	}
}

func validRecordFlagFn(_ *domain.GenericRecord) bool {
	return false
}

func dlqRecordFlagFn(_ *domain.DeadLetterRecord) bool {
	return true
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

	rawRecords := source.BuildSource(s)
	valid, dlq, audit := beam.ParDo3(s, NewFusedEnrichAndValidateFn(schema, "", "", quality, sec), rawRecords)

	sink.BuildSink(s, valid)
	dlqSink.BuildDLQ(s, dlq)
	if auditSink != nil {
		auditSink.BuildAuditSink(s, audit)
	}

	validMetrics := beam.ParDo(s, validRecordMetricItemFn, valid)
	dlqMetrics := beam.ParDo(s, dlqRecordMetricItemFn, dlq)
	allMetrics := beam.Flatten(s, validMetrics, dlqMetrics)

	return beam.Combine(s, &MetricsCombinerFn{}, allMetrics)
}
