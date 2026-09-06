package core

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*MetricsSinkDoFn)(nil)).Elem())
}

// MetricsSinkDoFn writes final PipelineMetrics to <outputDir>/pipeline_metrics.json,
// making the conservation invariant (Total = Valid + DLQ) observable after each pipeline run.
type MetricsSinkDoFn struct {
	OutputDir string `json:"output_dir"`
	storage   ports.StorageWriter
}

// NewMetricsSinkDoFn creates a MetricsSinkDoFn with an injected storage writer.
func NewMetricsSinkDoFn(storage ports.StorageWriter, outputDir string) *MetricsSinkDoFn {
	return &MetricsSinkDoFn{
		OutputDir: strings.TrimRight(outputDir, "/"),
		storage:   storage,
	}
}

// Setup re-establishes storage on remote worker nodes after Beam serialization.
func (fn *MetricsSinkDoFn) Setup(_ context.Context) error {
	if fn.storage == nil && GetStorageFactory() != nil {
		fn.storage = GetStorageFactory()(fn.OutputDir)
	}
	return nil
}

// ProcessElement serializes and atomically persists the metrics record.
func (fn *MetricsSinkDoFn) ProcessElement(ctx context.Context, m domain.PipelineMetrics) error {
	tracer := otel.Tracer("beam")
	ctx, span := tracer.Start(ctx, "MetricsSinkDoFn.ProcessElement")
	defer span.End()

	if fn.storage == nil && GetStorageFactory() != nil {
		fn.storage = GetStorageFactory()(fn.OutputDir)
	}
	if fn.storage == nil {
		err := fmt.Errorf("MetricsSinkDoFn: storage not initialized — call Setup first")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("MetricsSinkDoFn: marshal: %w", err)
	}

	outputDir := strings.TrimRight(fn.OutputDir, "/\\")
	if filepath.Ext(outputDir) != "" {
		outputDir = filepath.Dir(outputDir)
	}
	finalURI := outputDir + "/pipeline_metrics.json"
	tmpURI, w, err := fn.storage.CreateTemp(ctx, finalURI)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("MetricsSinkDoFn: CreateTemp: %w", err)
	}
	if _, err := w.Write(data); err != nil {
		_ = fn.storage.AbortTemp(ctx, tmpURI)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("MetricsSinkDoFn: write: %w", err)
	}
	if err := w.Close(); err != nil {
		_ = fn.storage.AbortTemp(ctx, tmpURI)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("MetricsSinkDoFn: close writer: %w", err)
	}
	if err := fn.storage.CommitTemp(ctx, tmpURI, finalURI); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("MetricsSinkDoFn: CommitTemp: %w", err)
	}
	return nil
}
