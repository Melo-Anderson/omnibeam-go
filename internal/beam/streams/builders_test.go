package streams

import (
	"context"
	"io"
	"testing"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

type dummyStorageWriter struct{}

func (dummyStorageWriter) Open(_ context.Context, _ string) (io.ReadCloser, error) { return nil, nil }
func (dummyStorageWriter) List(_ context.Context, _ string) ([]string, error)      { return nil, nil }
func (dummyStorageWriter) Size(_ context.Context, _ string) (int64, error)         { return 0, nil }
func (dummyStorageWriter) CreateTemp(_ context.Context, _ string) (string, io.WriteCloser, error) {
	return "", nil, nil
}
func (dummyStorageWriter) CommitTemp(_ context.Context, _, _ string) error { return nil }
func (dummyStorageWriter) AbortTemp(_ context.Context, _ string) error     { return nil }

type dummyRecordFormatter struct{}

func (dummyRecordFormatter) FormatHeader(_ *domain.Schema) ([]byte, error) { return nil, nil }
func (dummyRecordFormatter) FormatRecord(_ *domain.GenericRecord, _ *domain.Schema) ([]byte, error) {
	return nil, nil
}

func TestStreamBuilders(t *testing.T) {
	p, _ := beam.NewPipelineWithRoot()
	s := p.Root()

	sourceBuilderSingle := &ByteStreamBeamSource{
		URIs:          []string{"file1.csv"},
		Storage:       &dummyStorageReader{},
		StreamWrapper: &dummyStreamWrapper{},
		Decoder:       &dummyStreamDecoder{},
		SourceConfig:  domain.SourceConfig{},
	}
	col1 := sourceBuilderSingle.BuildSource(s)
	if !col1.IsValid() {
		t.Error("expected valid PCollection from ByteStreamBeamSource with 1 file")
	}

	sourceBuilderMulti := &ByteStreamBeamSource{
		URIs:          []string{"file1.csv", "file2.csv"},
		Storage:       &dummyStorageReader{},
		StreamWrapper: &dummyStreamWrapper{},
		Decoder:       &dummyStreamDecoder{},
		SourceConfig:  domain.SourceConfig{},
	}
	col2 := sourceBuilderMulti.BuildSource(s)
	if !col2.IsValid() {
		t.Error("expected valid PCollection from ByteStreamBeamSource with multi files")
	}

	parquetSink := &ParquetBeamSink{
		Storage:     &dummyStorageWriter{},
		OutputPath:  "gs://bucket/out.parquet",
		Compression: "snappy",
		Schema:      domain.Schema{},
	}
	parquetSink.BuildSink(s, col1)

	delimSink := &DelimitedFileBeamSink{
		Storage:     &dummyStorageWriter{},
		Formatter:   &dummyRecordFormatter{},
		OutputDir:   "gs://bucket/out_csv",
		Format:      "csv",
		Compression: "none",
		SingleFile:  true,
		Schema:      domain.Schema{},
	}
	delimSink.BuildSink(s, col2)
}
