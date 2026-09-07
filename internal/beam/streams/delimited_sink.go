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
	beam.RegisterType(reflect.TypeOf((*DelimitedFileSinkDoFn)(nil)).Elem())
}

var (
	delimitedRecordsWrittenCounter = metrics.NewCounter("omnibeam", "records_written")
	delimitedBundleWriteDist       = metrics.NewDistribution("omnibeam", "bundle_write_duration_ms")
)

// DelimitedFileSinkDoFn writes GenericRecords to delimited or text files with atomic commits, compression, and checksumming.
type DelimitedFileSinkDoFn struct {
	OutputDir   string        `json:"output_dir"`
	Format      string        `json:"format"`
	Compression string        `json:"compression"`
	SingleFile  bool          `json:"single_file"`
	Schema      domain.Schema `json:"schema"`

	stager       *core.BundleFileStager
	formatter    ports.RecordFormatter
	compCloser   io.Closer
	targetWriter io.Writer
	hashW        *ioutils.CountingHashWriter
}

// NewDelimitedFileSinkDoFn constructs a new DelimitedFileSinkDoFn.
func NewDelimitedFileSinkDoFn(
	storage ports.StorageWriter,
	formatter ports.RecordFormatter,
	outputDir string,
	format string,
	compression string,
	singleFile bool,
	schema domain.Schema,
) *DelimitedFileSinkDoFn {
	return &DelimitedFileSinkDoFn{
		stager:      core.NewBundleFileStager(storage, outputDir, format, compression, singleFile),
		formatter:   formatter,
		OutputDir:   strings.TrimRight(outputDir, "/"),
		Format:      strings.ToLower(format),
		Compression: strings.ToLower(compression),
		SingleFile:  singleFile,
		Schema:      schema,
	}
}

var recordFormatterProvider func(format string, schema *domain.Schema) ports.RecordFormatter

// SetRecordFormatterProvider sets the provider to reconstruct formatters on worker nodes.
func SetRecordFormatterProvider(p func(format string, schema *domain.Schema) ports.RecordFormatter) {
	recordFormatterProvider = p
}

var compressorProvider func(w io.Writer, compression string) (io.Writer, io.Closer, error) = func(w io.Writer, _ string) (io.Writer, io.Closer, error) {
	return w, nil, nil
}

// SetCompressorProvider sets the provider to wrap output compression on worker nodes.
func SetCompressorProvider(p func(w io.Writer, compression string) (io.Writer, io.Closer, error)) {
	compressorProvider = p
}

// Setup re-establishes storage on remote worker nodes.
func (fn *DelimitedFileSinkDoFn) Setup(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = core.NewBundleFileStager(nil, fn.OutputDir, fn.Format, fn.Compression, fn.SingleFile)
	}
	return fn.stager.Setup(ctx)
}

// StartBundle prepares the sink state for the active worker bundle.
func (fn *DelimitedFileSinkDoFn) StartBundle(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = core.NewBundleFileStager(nil, fn.OutputDir, fn.Format, fn.Compression, fn.SingleFile)
	}
	fn.stager.StartBundle(ctx)

	if fn.formatter == nil && recordFormatterProvider != nil {
		fn.formatter = recordFormatterProvider(fn.Format, &fn.Schema)
	}

	fn.targetWriter = nil
	fn.compCloser = nil
	fn.hashW = nil
	return nil
}

// lazyOpen creates the staging temp file on the first ProcessElement call.
func (fn *DelimitedFileSinkDoFn) lazyOpen(ctx context.Context) error {
	if fn.targetWriter != nil {
		return nil
	}

	_, storageW, err := fn.stager.EnsureOpen(ctx)
	if err != nil {
		return err
	}

	fn.hashW = ioutils.NewCountingHashWriter(storageW)
	targetWriter, compCloser, err := compressorProvider(fn.hashW, fn.Compression)
	if err != nil {
		_ = fn.stager.Abort(ctx)
		return fmt.Errorf("failed initializing compression writer: %w", err)
	}

	fn.compCloser = compCloser
	fn.targetWriter = targetWriter

	if fn.formatter != nil {
		header, err := fn.formatter.FormatHeader(&fn.Schema)
		if err != nil {
			_ = fn.stager.Abort(ctx)
			return fmt.Errorf("failed formatting header: %w", err)
		}
		if len(header) > 0 {
			if _, err := fn.targetWriter.Write(header); err != nil {
				_ = fn.stager.Abort(ctx)
				return fmt.Errorf("failed writing header: %w", err)
			}
		}
	}

	return nil
}

// ProcessElement writes a single GenericRecord into the bundle staging file.
func (fn *DelimitedFileSinkDoFn) ProcessElement(ctx context.Context, rec *domain.GenericRecord) error {
	if err := fn.lazyOpen(ctx); err != nil {
		return err
	}

	if fn.formatter == nil {
		return fmt.Errorf("record formatter is not initialized")
	}
	rowBytes, err := fn.formatter.FormatRecord(rec, &fn.Schema)
	if err != nil {
		_ = fn.stager.Abort(ctx)
		return fmt.Errorf("failed formatting record: %w", err)
	}
	if len(rowBytes) > 0 {
		if _, err := fn.targetWriter.Write(rowBytes); err != nil {
			_ = fn.stager.Abort(ctx)
			return fmt.Errorf("failed writing record: %w", err)
		}
	}
	fn.stager.IncrementRecord()
	return nil
}

// FinishBundle flushes compression, closes files, and commits the output shard.
func (fn *DelimitedFileSinkDoFn) FinishBundle(ctx context.Context) error {
	tracer := otel.Tracer("beam")
	ctx, span := tracer.Start(ctx, "DelimitedFileSinkDoFn.FinishBundle")
	defer span.End()

	if fn.stager == nil || fn.stager.RecordCount() == 0 {
		return nil
	}

	start := time.Now()
	recCount := fn.stager.RecordCount()

	if err := fn.stager.CommitOrAbort(ctx, fn.compCloser); err != nil {
		return err
	}

	delimitedRecordsWrittenCounter.Inc(ctx, recCount)
	delimitedBundleWriteDist.Update(ctx, time.Since(start).Milliseconds())
	return nil
}

// Checksum returns the SHA-256 hex string of the written content for this bundle.
func (fn *DelimitedFileSinkDoFn) Checksum() string {
	if fn.hashW != nil {
		return fn.hashW.HexSum()
	}
	return ""
}

func (fn *DelimitedFileSinkDoFn) abort(ctx context.Context) error {
	if fn.stager != nil {
		return fn.stager.Abort(ctx)
	}
	return nil
}
