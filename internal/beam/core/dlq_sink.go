// package core contains Beam DoFn transformations for the compute pipeline.
// DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
package core

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.opentelemetry.io/otel"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*DLQSinkDoFn)(nil)).Elem())
}

// DLQSinkDoFn coordinates atomic bundle-level DeadLetterRecord JSONL writing.
type DLQSinkDoFn struct {
	DLQDir string `json:"dlq_dir"`
	stager *BundleFileStager
}

// NewDLQSinkDoFn constructs a new DLQSinkDoFn with injected port dependencies.
func NewDLQSinkDoFn(storage ports.StorageWriter, dlqDir string) *DLQSinkDoFn {
	return &DLQSinkDoFn{
		DLQDir: strings.TrimRight(dlqDir, "/"),
		stager: NewBundleFileStager(storage, dlqDir, "jsonl", "none", false),
	}
}

// Setup re-establishes storage factory on remote worker nodes.
func (fn *DLQSinkDoFn) Setup(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = NewBundleFileStager(nil, fn.DLQDir, "jsonl", "none", false)
	}
	return fn.stager.Setup(ctx)
}

// StartBundle resets bundle-level state for the active bundle.
func (fn *DLQSinkDoFn) StartBundle(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = NewBundleFileStager(nil, fn.DLQDir, "jsonl", "none", false)
	}
	fn.stager.StartBundle(ctx)
	return nil
}

// ProcessElement lazily opens the DLQ staging file on first error record and appends JSONL.
func (fn *DLQSinkDoFn) ProcessElement(ctx context.Context, dlq *domain.DeadLetterRecord) error {
	if fn.stager == nil {
		fn.stager = NewBundleFileStager(nil, fn.DLQDir, "jsonl", "none", false)
	}
	_, w, err := fn.stager.EnsureOpen(ctx)
	if err != nil {
		return err
	}

	data, err := json.Marshal(dlq)
	if err != nil {
		_ = fn.stager.Abort(ctx)
		return fmt.Errorf("failed marshaling DLQ record: %w", err)
	}

	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		_ = fn.stager.Abort(ctx)
		return fmt.Errorf("failed writing DLQ record: %w", err)
	}

	fn.stager.IncrementRecord()
	return nil
}

// FinishBundle closes the writer and atomically commits or aborts the DLQ staging file.
func (fn *DLQSinkDoFn) FinishBundle(ctx context.Context) error {
	tracer := otel.Tracer("beam")
	ctx, span := tracer.Start(ctx, "DLQSinkDoFn.FinishBundle")
	defer span.End()

	if fn.stager == nil {
		return nil
	}
	return fn.stager.CommitOrAbort(ctx, nil)
}
