// package streams contains Beam DoFn transformations for the compute pipeline.
// DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
package streams

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/otel"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*ParquetSinkDoFn)(nil)).Elem())
}

var (
	parquetRecordsWrittenCounter = metrics.NewCounter("omnibeam", "records_written")
	parquetBundleWriteDist       = metrics.NewDistribution("omnibeam", "bundle_write_duration_ms")
)

var _ interface {
	Setup(context.Context) error
	StartBundle(context.Context) error
	ProcessElement(context.Context, *domain.GenericRecord) error
	FinishBundle(context.Context) error
} = (*ParquetSinkDoFn)(nil)

// ParquetSinkDoFn coordinates atomic bundle-level Parquet shard writing using BundleFileStager.
// Rows are encoded directly into physical parquet.Row slices and streamed without intermediate map allocations.
type ParquetSinkDoFn struct {
	OutputDir   string        `json:"output_dir"`
	Compression string        `json:"compression"`
	Schema      domain.Schema `json:"schema"`

	stager        *core.BundleFileStager
	pqWriter      *parquet.Writer
	directEncoder ParquetDirectRowEncoder
	numCols       int
	rowBuffer     []parquet.Value
}

// NewParquetSinkDoFn constructs a new ParquetSinkDoFn with injected port dependencies.
func NewParquetSinkDoFn(
	storage ports.StorageWriter,
	outputDir string,
	compression string,
	schema domain.Schema,
) *ParquetSinkDoFn {
	return &ParquetSinkDoFn{
		OutputDir:   strings.TrimRight(outputDir, "/"),
		Compression: compression,
		Schema:      schema,
		stager:      core.NewBundleFileStager(storage, outputDir, "parquet", compression, false),
	}
}

// Setup re-establishes storage and compiles direct row extractor closures on remote worker nodes.
func (fn *ParquetSinkDoFn) Setup(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = core.NewBundleFileStager(nil, fn.OutputDir, "parquet", fn.Compression, false)
	}
	fn.directEncoder, fn.numCols = CompileParquetDirectRowEncoder(&fn.Schema)
	fn.rowBuffer = make([]parquet.Value, fn.numCols)
	return fn.stager.Setup(ctx)
}

// StartBundle prepares the sink state for the active worker bundle.
func (fn *ParquetSinkDoFn) StartBundle(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = core.NewBundleFileStager(nil, fn.OutputDir, "parquet", fn.Compression, false)
	}
	fn.stager.StartBundle(ctx)
	fn.pqWriter = nil
	if fn.numCols > 0 && len(fn.rowBuffer) < fn.numCols {
		fn.rowBuffer = make([]parquet.Value, fn.numCols)
	}
	return nil
}

// lazyOpen creates the staging temp file on the first ProcessElement call.
func (fn *ParquetSinkDoFn) lazyOpen(ctx context.Context) error {
	if fn.pqWriter != nil {
		return nil
	}

	_, w, err := fn.stager.EnsureOpen(ctx)
	if err != nil {
		return err
	}

	fn.pqWriter = parquet.NewWriter(w, BuildParquetSchema(&fn.Schema))
	return nil
}

// ProcessElement encodes the record into the bundle buffer and streams it to parquet.Writer.
func (fn *ParquetSinkDoFn) ProcessElement(ctx context.Context, rec *domain.GenericRecord) error {
	if err := fn.lazyOpen(ctx); err != nil {
		return err
	}

	if fn.directEncoder == nil {
		fn.directEncoder, fn.numCols = CompileParquetDirectRowEncoder(&fn.Schema)
		fn.rowBuffer = make([]parquet.Value, fn.numCols)
	}

	row := fn.directEncoder(rec, fn.rowBuffer)
	if _, err := fn.pqWriter.WriteRows([]parquet.Row{row}); err != nil {
		_ = fn.stager.Abort(ctx)
		return fmt.Errorf("parquet write row failed: %w", err)
	}
	fn.stager.IncrementRecord()
	return nil
}

// FinishBundle flushes buffered rows and atomically commits the staging file.
func (fn *ParquetSinkDoFn) FinishBundle(ctx context.Context) error {
	tracer := otel.Tracer("beam")
	ctx, span := tracer.Start(ctx, "ParquetSinkDoFn.FinishBundle")
	defer span.End()

	if fn.stager == nil || fn.stager.RecordCount() == 0 {
		return nil
	}

	start := time.Now()
	recCount := fn.stager.RecordCount()

	if err := fn.stager.CommitOrAbort(ctx, fn.pqWriter); err != nil {
		return err
	}

	parquetRecordsWrittenCounter.Inc(ctx, recCount)
	parquetBundleWriteDist.Update(ctx, time.Since(start).Milliseconds())
	return nil
}
