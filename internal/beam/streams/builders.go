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
	Storage           ports.StorageWriter
	OutputPath        string
	Compression       string
	Encryption        string
	IncludeSourceFile bool
	Schema            domain.Schema
}

// Compile-time assertion: ParquetBeamSink must satisfy ports.BeamSinkBuilder (LSP).
var _ ports.BeamSinkBuilder = (*ParquetBeamSink)(nil)

// BuildSink builds the Parquet sink transform.
func (s *ParquetBeamSink) BuildSink(scope beam.Scope, validRecords beam.PCollection) {
	sinkFn := NewParquetSinkDoFn(s.Storage, s.OutputPath, s.Compression, s.Encryption, s.Schema)
	sinkFn.IncludeSourceFile = s.IncludeSourceFile
	beam.ParDo0(scope, sinkFn, validRecords)
}

// StreamFileBeamSink builds a stream-formatted (CSV/JSONL/TXT/TSV) file sink in the Beam DAG.
type StreamFileBeamSink struct {
	Storage     ports.StorageWriter
	Formatter   ports.RecordFormatter
	OutputDir   string
	Format      string
	Compression string
	Encryption  domain.EncryptionConfig
	// FormatOptions carries delimiter, header, and line-terminator configuration
	// that must be forwarded to the DoFn for correct serialization on remote workers.
	FormatOptions domain.FormatOptions
	SingleFile    bool
	Schema        domain.Schema
}

// Compile-time assertion: StreamFileBeamSink must satisfy ports.BeamSinkBuilder (LSP).
var _ ports.BeamSinkBuilder = (*StreamFileBeamSink)(nil)

// BuildSink builds the stream file sink transform.
func (s *StreamFileBeamSink) BuildSink(scope beam.Scope, validRecords beam.PCollection) {
	beam.ParDo0(scope, NewStreamFileSinkDoFn(
		s.Storage,
		s.Formatter,
		s.OutputDir,
		s.Format,
		s.Compression,
		s.Encryption,
		s.SingleFile,
		s.FormatOptions,
		s.Schema,
	), validRecords)
}

// DelimitedFileBeamSink is a type alias for StreamFileBeamSink maintaining backward compatibility.
type DelimitedFileBeamSink = StreamFileBeamSink
