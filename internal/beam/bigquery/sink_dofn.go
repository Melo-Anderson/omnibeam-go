package bigquery

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/metrics"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
	"github.com/omnibeam/dataflow-compute-go/internal/ports"
	"github.com/omnibeam/dataflow-compute-go/pkg/resilience"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*BigQuerySinkDoFn)(nil)).Elem())
}

var (
	bqRowsWrittenCounter = metrics.NewCounter("omnibeam", "bigquery_rows_written")
	bqBatchesCounter     = metrics.NewCounter("omnibeam", "bigquery_batches_committed")
	bqDLQCounter         = metrics.NewCounter("omnibeam", "bigquery_dlq_rows")
)

var globalWriterProvider ports.BigQueryWriterProvider

// SetGlobalBigQueryWriterProvider registers the factory invoked in Setup on remote Beam workers.
// Must be called inside a beam.RegisterInit callback in cmd/pipeline/main.go.
func SetGlobalBigQueryWriterProvider(p ports.BigQueryWriterProvider) {
	globalWriterProvider = p
}

// BigQuerySinkDoFn writes GenericRecords to BigQuery via Storage Write API Pending Streams.
// Only Config and Schema are exported (serializable over Beam's gob transport).
// The writer, stream, and buffer are unexported and re-initialized per bundle.
type BigQuerySinkDoFn struct {
	Config domain.BigQuerySinkConfig `json:"config"`
	Schema domain.Schema             `json:"schema"`

	writer     ports.BigQueryWriter
	sizer      *resilience.AdaptiveBatchSizer
	streamName string
	buffer     []*domain.GenericRecord
	rowCount   int64
	emitDLQFn  func(*domain.DeadLetterRecord) `json:"-"`
}

// NewBigQuerySinkDoFn creates a BigQuerySinkDoFn with a pre-injected writer (for testing and local use).
func NewBigQuerySinkDoFn(w ports.BigQueryWriter, cfg domain.BigQuerySinkConfig, schema domain.Schema) *BigQuerySinkDoFn {
	minBatch := domain.OrDefault(cfg.BatchSize, domain.DefaultBigQueryBatchSize)
	return &BigQuerySinkDoFn{
		Config: cfg,
		Schema: schema,
		writer: w,
		sizer:  resilience.NewAdaptiveBatchSizer(minBatch, minBatch*domain.DefaultAdaptiveMaxBatchFactor),
	}
}

func (fn *BigQuerySinkDoFn) Setup(ctx context.Context) error {
	fn.Schema.Index()
	if fn.sizer == nil {
		minBatch := domain.OrDefault(fn.Config.BatchSize, domain.DefaultBigQueryBatchSize)
		fn.sizer = resilience.NewAdaptiveBatchSizer(minBatch, minBatch*domain.DefaultAdaptiveMaxBatchFactor)
	}
	if fn.writer == nil && globalWriterProvider != nil {
		w, err := globalWriterProvider(ctx, fn.Config.ProjectID)
		if err != nil {
			return fmt.Errorf("BigQuerySinkDoFn.Setup: failed acquiring writer: %w", err)
		}
		fn.writer = w
	}
	return nil
}

func (fn *BigQuerySinkDoFn) StartBundle(_ context.Context) error {
	batchCap := domain.OrDefault(fn.Config.BatchSize, domain.DefaultBigQueryBatchSize)
	fn.buffer = make([]*domain.GenericRecord, 0, batchCap)
	fn.streamName = ""
	fn.rowCount = 0
	fn.emitDLQFn = nil
	return nil
}

func (fn *BigQuerySinkDoFn) ProcessElement(ctx context.Context, rec *domain.GenericRecord, emitDLQ func(*domain.DeadLetterRecord)) error {
	fn.buffer = append(fn.buffer, rec)
	fn.rowCount++
	if fn.emitDLQFn == nil {
		fn.emitDLQFn = emitDLQ
	}

	targetBatch := domain.OrDefault(fn.Config.BatchSize, domain.DefaultBigQueryBatchSize)
	if fn.sizer != nil {
		targetBatch = fn.sizer.CurrentBatchSize()
	}

	if len(fn.buffer) >= targetBatch {
		return fn.flushBuffer(ctx, emitDLQ)
	}
	return nil
}

func (fn *BigQuerySinkDoFn) flushBuffer(ctx context.Context, emitDLQ func(*domain.DeadLetterRecord)) error {
	if len(fn.buffer) == 0 {
		return nil
	}
	if fn.writer == nil {
		return fmt.Errorf("bigquery: writer not initialized (call Setup first)")
	}
	if fn.streamName == "" {
		name, err := fn.writer.OpenStream(ctx, fn.Config.ProjectID, fn.Config.DatasetID, fn.Config.TableID)
		if err != nil {
			return fmt.Errorf("bigquery: OpenStream failed: %w", err)
		}
		fn.streamName = name
	}

	start := time.Now()
	_, dlqs, err := fn.writer.AppendRecords(ctx, fn.streamName, 0, fn.buffer, &fn.Schema)
	if fn.sizer != nil {
		fn.sizer.RecordFlush(time.Since(start))
	}
	if err != nil {
		return fmt.Errorf("bigquery: AppendRecords to stream %q failed: %w", fn.streamName, err)
	}
	for _, dlq := range dlqs {
		if emitDLQ != nil {
			bqDLQCounter.Inc(ctx, 1)
			emitDLQ(dlq)
		}
	}

	bqRowsWrittenCounter.Inc(ctx, int64(len(fn.buffer)-len(dlqs)))
	fn.buffer = fn.buffer[:0]
	return nil
}

func (fn *BigQuerySinkDoFn) FinishBundle(ctx context.Context) error {
	if err := fn.flushBuffer(ctx, fn.emitDLQFn); err != nil {
		return err
	}
	if fn.streamName != "" && fn.rowCount > 0 {
		if err := fn.writer.CommitStream(ctx, fn.streamName); err != nil {
			return fmt.Errorf("bigquery: CommitStream %q failed: %w", fn.streamName, err)
		}
		bqBatchesCounter.Inc(ctx, 1)
	}
	return nil
}

// Drain is called by the Beam runner on drain signal; delegates to FinishBundle for Exactly-Once semantics.
func (fn *BigQuerySinkDoFn) Drain(ctx context.Context) error {
	return fn.FinishBundle(ctx)
}
