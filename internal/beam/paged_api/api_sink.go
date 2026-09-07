// package paged_api contains Beam DoFn transformations for the compute pipeline.
// DIP: this package imports ONLY internal/ports and internal/domain — zero adapter imports.
package paged_api

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/beam/core"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.opentelemetry.io/otel"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*APISinkDoFn)(nil)).Elem())
}

// BatchAPIWriterFactory constructs a ports.BatchAPIWriter given endpoint config, options, and token.
type BatchAPIWriterFactory func(endpoint domain.APIEndpointConfig, opts domain.APISinkOptions, secretToken string) ports.BatchAPIWriter

var globalAPIWriterFactory BatchAPIWriterFactory

// SetAPIWriterFactory sets the process-level factory for remote workers.
func SetAPIWriterFactory(factory BatchAPIWriterFactory) {
	globalAPIWriterFactory = factory
}

// APISinkDoFn coordinates micro-batching and dispatching records to an API endpoint.
//
// Serializable configuration fields (Endpoint, APIOptions, Schema, DLQPath, SecretToken)
// are exported so the Beam coder preserves them across submitter → worker serialization.
// Runtime handles (writer, dlqSink, buffer) are unexported and reconstructed in Setup().
type APISinkDoFn struct {
	// Serializable config — persisted by the Beam row coder.
	Endpoint    domain.APIEndpointConfig `json:"endpoint"`
	APIOptions  domain.APISinkOptions    `json:"api_options"`
	Schema      domain.Schema            `json:"schema"`
	DLQPath     string                   `json:"dlq_path"`
	SecretToken string                   `json:"secret_token,omitempty"`

	// Worker-local state — not serialized; reconstructed in Setup() / StartBundle().
	writer  ports.BatchAPIWriter    `json:"-"`
	dlqSink ports.StorageWriter     `json:"-"`
	buffer  []*domain.GenericRecord `json:"-"`
}

// Compile-time assertion: APISinkDoFn must satisfy beam.DoFn lifecycle.
var _ interface {
	Setup(ctx context.Context) error
	StartBundle(ctx context.Context) error
	ProcessElement(ctx context.Context, rec *domain.GenericRecord) error
	FinishBundle(ctx context.Context) error
} = (*APISinkDoFn)(nil)

// NewAPISinkDoFn constructs a new APISinkDoFn from serializable configuration.
func NewAPISinkDoFn(
	endpoint domain.APIEndpointConfig,
	apiOptions domain.APISinkOptions,
	schema domain.Schema,
	dlqPath string,
	secretToken string,
) *APISinkDoFn {
	return &APISinkDoFn{
		Endpoint:    endpoint,
		APIOptions:  apiOptions,
		Schema:      schema,
		DLQPath:     dlqPath,
		SecretToken: secretToken,
	}
}

// WithWriter injects a custom BatchAPIWriter (useful for testing and local DirectRunner).
func (fn *APISinkDoFn) WithWriter(w ports.BatchAPIWriter) *APISinkDoFn {
	fn.writer = w
	return fn
}

// WithDLQSink injects a custom StorageWriter for DLQ (useful for testing).
func (fn *APISinkDoFn) WithDLQSink(s ports.StorageWriter) *APISinkDoFn {
	fn.dlqSink = s
	return fn
}

// Setup reconstructs ephemeral worker-local I/O handles before bundles start.
// Called once per worker process — replaces in-memory handles that cannot be serialized.
func (fn *APISinkDoFn) Setup(_ context.Context) error {
	if fn.dlqSink == nil && core.GetStorageFactory() != nil && fn.DLQPath != "" {
		fn.dlqSink = core.GetStorageFactory()(fn.DLQPath)
	}
	if fn.writer == nil && globalAPIWriterFactory != nil {
		fn.writer = globalAPIWriterFactory(fn.Endpoint, fn.APIOptions, fn.SecretToken)
	}
	return nil
}

// StartBundle prepares the record buffer for the active worker bundle.
func (fn *APISinkDoFn) StartBundle(_ context.Context) error {
	fn.buffer = make([]*domain.GenericRecord, 0, fn.APIOptions.BatchSize)
	return nil
}

// ProcessElement adds a record to the buffer and flushes when batch size is reached.
func (fn *APISinkDoFn) ProcessElement(ctx context.Context, rec *domain.GenericRecord) error {
	fn.buffer = append(fn.buffer, rec)
	if len(fn.buffer) >= fn.APIOptions.BatchSize {
		return fn.flush(ctx)
	}
	return nil
}

// FinishBundle flushes remaining buffered records before the bundle completes.
func (fn *APISinkDoFn) FinishBundle(ctx context.Context) error {
	tracer := otel.Tracer("beam")
	ctx, span := tracer.Start(ctx, "APISinkDoFn.FinishBundle")
	defer span.End()

	return fn.flush(ctx)
}

func (fn *APISinkDoFn) flush(ctx context.Context) error {
	if len(fn.buffer) == 0 || fn.writer == nil {
		return nil
	}

	_, failed, err := fn.writer.WriteBatch(ctx, fn.buffer, &fn.Schema)
	fn.buffer = fn.buffer[:0]

	if err != nil {
		return err
	}

	if len(failed) > 0 && fn.dlqSink != nil && fn.DLQPath != "" {
		if err := fn.writeDLQ(ctx, failed); err != nil {
			return fmt.Errorf("failed writing API DLQ records: %w", err)
		}
	}

	return nil
}

func (fn *APISinkDoFn) writeDLQ(ctx context.Context, failed []*domain.DeadLetterRecord) error {
	finalURI := domain.BuildOutputURI(fn.DLQPath, "jsonl", "none", false)
	tempURI, w, err := fn.dlqSink.CreateTemp(ctx, finalURI)
	if err != nil {
		return fmt.Errorf("failed creating temp DLQ file: %w", err)
	}

	for _, rec := range failed {
		data, err := json.Marshal(rec)
		if err != nil {
			_ = w.Close()
			_ = fn.dlqSink.AbortTemp(ctx, tempURI)
			return fmt.Errorf("failed marshaling DLQ record: %w", err)
		}
		data = append(data, '\n')
		if _, err := w.Write(data); err != nil {
			_ = w.Close()
			_ = fn.dlqSink.AbortTemp(ctx, tempURI)
			return fmt.Errorf("failed writing DLQ record: %w", err)
		}
	}

	if err := w.Close(); err != nil {
		_ = fn.dlqSink.AbortTemp(ctx, tempURI)
		return fmt.Errorf("failed closing DLQ file: %w", err)
	}

	return fn.dlqSink.CommitTemp(ctx, tempURI, finalURI)
}
