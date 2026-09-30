// package streams contains Beam DoFn transformations for the compute pipeline.
// DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
package streams

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"github.com/omnibeam/dataflow-compute-go/pkg/ioutils"
	"go.opentelemetry.io/otel"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*StreamFileSinkDoFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*DelimitedFileSinkDoFn)(nil)).Elem())
}

var (
	streamRecordsWrittenCounter = metrics.NewCounter("omnibeam", "records_written")
	streamBundleWriteDist       = metrics.NewDistribution("omnibeam", "bundle_write_duration_ms")
)

// StreamFileSinkDoFn writes GenericRecords to stream-formatted files (CSV, JSONL, TXT, TSV)
// with atomic bundle staging, envelope encryption (PGP/KMS), and compression.
type StreamFileSinkDoFn struct {
	OutputDir     string                  `json:"output_dir"`
	Format        string                  `json:"format"`
	Compression   string                  `json:"compression"`
	Encryption    domain.EncryptionConfig `json:"encryption"`
	FormatOptions domain.FormatOptions    `json:"format_options"`
	SingleFile    bool                    `json:"single_file"`
	Schema        domain.Schema           `json:"schema"`

	stager    *core.BundleFileStager
	formatter ports.RecordFormatter
	streamW   *ioutils.DecoratedStreamWriter
}

// DelimitedFileSinkDoFn is a type alias for StreamFileSinkDoFn maintaining backward compatibility.
type DelimitedFileSinkDoFn = StreamFileSinkDoFn

// NewStreamFileSinkDoFn constructs a new StreamFileSinkDoFn.
func NewStreamFileSinkDoFn(
	storage ports.StorageWriter,
	formatter ports.RecordFormatter,
	outputDir string,
	format string,
	compression string,
	encryption domain.EncryptionConfig,
	singleFile bool,
	opts domain.FormatOptions,
	schema domain.Schema,
) *StreamFileSinkDoFn {
	return &StreamFileSinkDoFn{
		stager:        core.NewBundleFileStager(storage, outputDir, format, compression, encryption.Type, singleFile),
		formatter:     formatter,
		OutputDir:     strings.TrimRight(outputDir, "/"),
		Format:        strings.ToLower(format),
		Compression:   strings.ToLower(compression),
		Encryption:    encryption,
		FormatOptions: opts,
		SingleFile:    singleFile,
		Schema:        schema,
	}
}

// NewDelimitedFileSinkDoFn constructs a StreamFileSinkDoFn via the legacy constructor name.
func NewDelimitedFileSinkDoFn(
	storage ports.StorageWriter,
	formatter ports.RecordFormatter,
	outputDir string,
	format string,
	compression string,
	encryption domain.EncryptionConfig,
	singleFile bool,
	opts domain.FormatOptions,
	schema domain.Schema,
) *DelimitedFileSinkDoFn {
	return NewStreamFileSinkDoFn(storage, formatter, outputDir, format, compression, encryption, singleFile, opts, schema)
}

// recordFormatterProvider reconstructs the record formatter on remote worker nodes.
var recordFormatterProvider func(format string, opts domain.FormatOptions, schema *domain.Schema) ports.RecordFormatter

// SetRecordFormatterProvider sets the provider to reconstruct formatters on worker nodes.
func SetRecordFormatterProvider(p func(format string, opts domain.FormatOptions, schema *domain.Schema) ports.RecordFormatter) {
	recordFormatterProvider = p
}

var compressorProvider func(w io.Writer, compression string) (io.Writer, io.Closer, error) = func(w io.Writer, _ string) (io.Writer, io.Closer, error) {
	return w, nil, nil
}

// SetCompressorProvider sets the provider to wrap output compression on worker nodes.
func SetCompressorProvider(p func(w io.Writer, compression string) (io.Writer, io.Closer, error)) {
	compressorProvider = p
}

var streamEncryptorProvider func(ctx context.Context, w io.Writer, enc domain.EncryptionConfig) (io.WriteCloser, error)

// SetStreamEncryptorProvider registers the worker-side stream encryptor factory.
func SetStreamEncryptorProvider(p func(ctx context.Context, w io.Writer, enc domain.EncryptionConfig) (io.WriteCloser, error)) {
	streamEncryptorProvider = p
}

// Setup re-establishes storage on remote worker nodes.
func (fn *StreamFileSinkDoFn) Setup(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = core.NewBundleFileStager(nil, fn.OutputDir, fn.Format, fn.Compression, fn.Encryption.Type, fn.SingleFile)
	}
	return fn.stager.Setup(ctx)
}

// StartBundle prepares the sink state for the active worker bundle.
func (fn *StreamFileSinkDoFn) StartBundle(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = core.NewBundleFileStager(nil, fn.OutputDir, fn.Format, fn.Compression, fn.Encryption.Type, fn.SingleFile)
	}
	fn.stager.StartBundle(ctx)

	if fn.formatter == nil && recordFormatterProvider != nil {
		fn.formatter = recordFormatterProvider(fn.Format, fn.FormatOptions, &fn.Schema)
	}

	fn.streamW = nil
	return nil
}

// lazyOpen creates the staging temp file and wraps stream layers on the first ProcessElement call.
func (fn *StreamFileSinkDoFn) lazyOpen(ctx context.Context) error {
	if fn.streamW != nil {
		return nil
	}

	_, storageW, err := fn.stager.EnsureOpen(ctx)
	if err != nil {
		return err
	}

	opts := ioutils.StreamPipelineOptions{
		Compression:    fn.Compression,
		EncryptionType: fn.Encryption.Type,
	}

	var encFn ioutils.EncryptorProviderFunc
	if fn.Encryption.Type != "" && fn.Encryption.Type != "none" {
		encFn = func(c context.Context, w io.Writer) (io.WriteCloser, error) {
			if streamEncryptorProvider == nil {
				return nil, fmt.Errorf("stream encryptor provider not initialized for encryption type %q", fn.Encryption.Type)
			}
			return streamEncryptorProvider(c, w, fn.Encryption)
		}
	}

	var compFn ioutils.CompressorProviderFunc
	if compressorProvider != nil {
		compFn = compressorProvider
	}

	streamW, err := ioutils.BuildDecoratedStreamWriter(ctx, storageW, opts, encFn, compFn)
	if err != nil {
		_ = fn.stager.Abort(ctx)
		return err
	}
	fn.streamW = streamW

	if fn.formatter != nil {
		header, err := fn.formatter.FormatHeader(&fn.Schema)
		if err != nil {
			_ = fn.abort(ctx)
			return fmt.Errorf("failed formatting header: %w", err)
		}
		if len(header) > 0 {
			if _, err := fn.streamW.Write(header); err != nil {
				_ = fn.abort(ctx)
				return fmt.Errorf("failed writing header: %w", err)
			}
		}
	}

	return nil
}

// ProcessElement writes a single GenericRecord into the bundle staging file.
func (fn *StreamFileSinkDoFn) ProcessElement(ctx context.Context, rec *domain.GenericRecord) error {
	if err := fn.lazyOpen(ctx); err != nil {
		return err
	}

	if fn.formatter == nil {
		return fmt.Errorf("record formatter is not initialized")
	}
	rowBytes, err := fn.formatter.FormatRecord(rec, &fn.Schema)
	if err != nil {
		_ = fn.abort(ctx)
		return fmt.Errorf("failed formatting record: %w", err)
	}
	if len(rowBytes) > 0 {
		if _, err := fn.streamW.Write(rowBytes); err != nil {
			_ = fn.abort(ctx)
			return fmt.Errorf("failed writing record: %w", err)
		}
	}
	fn.stager.IncrementRecord()
	return nil
}

// FinishBundle flushes compression and encryption, closes files, and commits the output shard.
func (fn *StreamFileSinkDoFn) FinishBundle(ctx context.Context) error {
	tracer := otel.Tracer("beam")
	ctx, span := tracer.Start(ctx, "StreamFileSinkDoFn.FinishBundle")
	defer span.End()

	if fn.stager == nil || fn.stager.RecordCount() == 0 {
		return nil
	}

	start := time.Now()
	recCount := fn.stager.RecordCount()

	if err := fn.stager.CommitOrAbort(ctx, fn.streamW); err != nil {
		return err
	}

	streamRecordsWrittenCounter.Inc(ctx, recCount)
	streamBundleWriteDist.Update(ctx, time.Since(start).Milliseconds())
	return nil
}

// Checksum returns the SHA-256 hex string of the written content for this bundle.
func (fn *StreamFileSinkDoFn) Checksum() string {
	if fn.streamW != nil {
		return fn.streamW.HexSum()
	}
	return ""
}

func (fn *StreamFileSinkDoFn) abort(ctx context.Context) error {
	if fn.streamW != nil {
		_ = fn.streamW.Close()
		fn.streamW = nil
	}
	if fn.stager != nil {
		return fn.stager.Abort(ctx)
	}
	return nil
}
