// Package ports defines domain-level abstract contracts and interfaces
// for storage, streaming, codecs, and sinks.
package ports

import (
	"context"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

// BeamSourceBuilder constructs a source PCollection for the Beam pipeline DAG.
type BeamSourceBuilder interface {
	BuildSource(s beam.Scope) beam.PCollection
}

// SinkDeps groups infrastructure resources injected into SinkFactory constructors.
// Fields are optional: a factory uses only the dependencies it requires.
// This struct replaces the positional (sec SecretResolver, st StorageBackend)
// pattern, making SinkFactory signatures stable under future extensions.
type SinkDeps struct {
	// Storage is the combined read/write backend for file-based sinks.
	// Must be nil for sinks that do not write files (BigQuery, REST API, Pub/Sub).
	Storage StorageBackend

	// Secrets resolves runtime secret references (API tokens, PGP keys, KMS key IDs).
	Secrets SecretResolver
}

// BeamSinkBuilder constructs destination write operations for valid records in the Beam DAG.
type BeamSinkBuilder interface {
	BuildSink(s beam.Scope, validRecords beam.PCollection)
}

// BeamDLQSinkBuilder constructs destination write operations for dead-letter records.
type BeamDLQSinkBuilder interface {
	BuildDLQ(s beam.Scope, dlqRecords beam.PCollection)
}

// BeamAuditSinkBuilder constructs the audit/telemetry sink transform in the Beam DAG.
type BeamAuditSinkBuilder interface {
	BuildAuditSink(s beam.Scope, auditEvents beam.PCollection)
}

// RecordFormatter formats a GenericRecord into serialized byte lines for file sinks.
type RecordFormatter interface {
	FormatHeader(schema *domain.Schema) ([]byte, error)
	FormatRecord(record *domain.GenericRecord, schema *domain.Schema) ([]byte, error)
}

// BatchAPIWriter is the contract for writing batches of GenericRecords to external HTTP/REST endpoints.
type BatchAPIWriter interface {
	WriteBatch(ctx context.Context, records []*domain.GenericRecord, schema *domain.Schema) ([]*domain.GenericRecord, []*domain.DeadLetterRecord, error)
}
