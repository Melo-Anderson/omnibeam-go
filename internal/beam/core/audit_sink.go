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
	beam.RegisterType(reflect.TypeOf((*AuditSinkDoFn)(nil)).Elem())
}

// AuditSinkDoFn coordinates atomic bundle-level AuditRecord JSONL writing.
type AuditSinkDoFn struct {
	AuditDir string `json:"audit_dir"`
	stager   *BundleFileStager
}

// NewAuditSinkDoFn constructs an AuditSinkDoFn with injected storage.
func NewAuditSinkDoFn(storage ports.StorageWriter, auditDir string) *AuditSinkDoFn {
	return &AuditSinkDoFn{
		AuditDir: strings.TrimRight(auditDir, "/"),
		stager:   NewBundleFileStager(storage, auditDir, "jsonl", "none", false),
	}
}

// Setup re-injects storage from globalStorageFactory when crossing the worker boundary.
func (fn *AuditSinkDoFn) Setup(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = NewBundleFileStager(nil, fn.AuditDir, "jsonl", "none", false)
	}
	return fn.stager.Setup(ctx)
}

// StartBundle resets bundle-level state for the active bundle.
func (fn *AuditSinkDoFn) StartBundle(ctx context.Context) error {
	if fn.stager == nil {
		fn.stager = NewBundleFileStager(nil, fn.AuditDir, "jsonl", "none", false)
	}
	fn.stager.StartBundle(ctx)
	return nil
}

// ProcessElement lazily opens the audit staging file on the first record and appends a JSONL line.
func (fn *AuditSinkDoFn) ProcessElement(ctx context.Context, rec *domain.AuditRecord) error {
	if fn.stager == nil {
		fn.stager = NewBundleFileStager(nil, fn.AuditDir, "jsonl", "none", false)
	}
	_, w, err := fn.stager.EnsureOpen(ctx)
	if err != nil {
		return err
	}

	data, err := json.Marshal(rec)
	if err != nil {
		_ = fn.stager.Abort(ctx)
		return fmt.Errorf("audit sink: failed marshaling record: %w", err)
	}

	data = append(data, '\n')
	if _, err := w.Write(data); err != nil {
		_ = fn.stager.Abort(ctx)
		return fmt.Errorf("audit sink: failed writing record: %w", err)
	}

	fn.stager.IncrementRecord()
	return nil
}

// FinishBundle atomically commits or aborts the staging file.
func (fn *AuditSinkDoFn) FinishBundle(ctx context.Context) error {
	tracer := otel.Tracer("beam")
	ctx, span := tracer.Start(ctx, "AuditSinkDoFn.FinishBundle")
	defer span.End()

	if fn.stager == nil {
		return nil
	}
	return fn.stager.CommitOrAbort(ctx, nil)
}
