package streams

import (
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
)

// ByteStreamBeamSource builds a file/stream ingestion PCollection using ByteStreamSourceSDF (Família 1).
type ByteStreamBeamSource struct {
	URIs          []string
	Storage       ports.StorageReader
	StreamWrapper ports.StreamWrapper
	Decoder       ports.StreamDecoder
	SourceConfig  domain.SourceConfig
}

// Compile-time assertion: ByteStreamBeamSource must satisfy ports.BeamSourceBuilder (LSP).
var _ ports.BeamSourceBuilder = (*ByteStreamBeamSource)(nil)

// BuildSource implements ports.BeamSourceBuilder for byte 
// Reshuffle is applied when multiple URIs are present to prevent runner fusion
// from serializing all file chunks on a single worker.
func (b *ByteStreamBeamSource) BuildSource(s beam.Scope) beam.PCollection {
	files := beam.CreateList(s, b.URIs)
	if len(b.URIs) > 1 {
		files = beam.Reshuffle(s.Scope("ReshuffleFiles"), files)
	}
	streamSDF := NewByteStreamSourceSDF(b.Storage, b.StreamWrapper, b.Decoder, b.SourceConfig)
	return beam.ParDo(s, streamSDF, files)
}

// ParquetBeamSink builds a Parquet sink in the Beam DAG.
type ParquetBeamSink struct {
	Storage     ports.StorageWriter
	OutputPath  string
	Compression string
	Schema      domain.Schema
}

// Compile-time assertion: ParquetBeamSink must satisfy ports.BeamSinkBuilder (LSP).
var _ ports.BeamSinkBuilder = (*ParquetBeamSink)(nil)

// BuildSink builds the Parquet sink transform.
func (s *ParquetBeamSink) BuildSink(scope beam.Scope, validRecords beam.PCollection) {
	beam.ParDo0(scope, NewParquetSinkDoFn(s.Storage, s.OutputPath, s.Compression, s.Schema), validRecords)
}

// DelimitedFileBeamSink builds a CSV/JSONL/TXT file sink in the Beam DAG.
type DelimitedFileBeamSink struct {
	Storage     ports.StorageWriter
	Formatter   ports.RecordFormatter
	OutputDir   string
	Format      string
	Compression string
	SingleFile  bool
	Schema      domain.Schema
}

// Compile-time assertion: DelimitedFileBeamSink must satisfy ports.BeamSinkBuilder (LSP).
var _ ports.BeamSinkBuilder = (*DelimitedFileBeamSink)(nil)

// BuildSink builds the delimited file sink transform.
func (s *DelimitedFileBeamSink) BuildSink(scope beam.Scope, validRecords beam.PCollection) {
	beam.ParDo0(scope, NewDelimitedFileSinkDoFn(
		s.Storage,
		s.Formatter,
		s.OutputDir,
		s.Format,
		s.Compression,
		s.SingleFile,
		s.Schema,
	), validRecords)
}
