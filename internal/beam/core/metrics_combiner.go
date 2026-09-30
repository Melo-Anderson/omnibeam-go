package core

import (
	"bytes"
	"errors"
	"io"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/omnibeam/dataflow-compute-go/internal/domain"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*MetricItem)(nil)).Elem())
	beam.RegisterCoder(reflect.TypeOf((*MetricItem)(nil)).Elem(), encMetricItem, decMetricItem)

	beam.RegisterType(reflect.TypeOf((*OriginAccumulator)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*OriginCombinerFn)(nil)).Elem())

	beam.RegisterType(reflect.TypeOf((*OriginSummary)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*MetricsSummaryCombinerFn)(nil)).Elem())
}

// MetricItem captures record outcome alongside its originating source partition.
type MetricItem struct {
	Origin string `json:"origin"`
	IsDLQ  bool   `json:"is_dlq"`
}

func encMetricItem(v MetricItem) ([]byte, error) {
	var buf bytes.Buffer
	err := EncodeMetricItem(v, &buf)
	return buf.Bytes(), err
}

func decMetricItem(data []byte) (MetricItem, error) {
	return DecodeMetricItem(bytes.NewReader(data))
}

// EncodeMetricItem serializes a MetricItem using compact binary format.
func EncodeMetricItem(v MetricItem, w io.Writer) error {
	var isDLQ byte
	if v.IsDLQ {
		isDLQ = 1
	}
	if _, err := w.Write([]byte{isDLQ}); err != nil {
		return err
	}
	return writeString(w, v.Origin)
}

// DecodeMetricItem deserializes a MetricItem from r.
func DecodeMetricItem(r io.Reader) (MetricItem, error) {
	var b [1]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return MetricItem{}, err
	}
	origin, err := readString(r)
	if err != nil && !errors.Is(err, ErrNilToken) {
		return MetricItem{}, err
	}
	return MetricItem{
		Origin: origin,
		IsDLQ:  b[0] == 1,
	}, nil
}

// OriginAccumulator holds counters for an individual origin key during CombinePerKey.
type OriginAccumulator struct {
	TotalRead int64 `json:"total_read"`
	Valid     int64 `json:"valid"`
	DLQ       int64 `json:"dlq"`
}

// OriginCombinerFn accumulates counts per origin key with zero heap map allocations.
type OriginCombinerFn struct{}

// CreateAccumulator initializes zero-valued counters for an origin key.
func (fn *OriginCombinerFn) CreateAccumulator() OriginAccumulator {
	return OriginAccumulator{}
}

// AddInput updates counters based on whether the record was DLQ or valid.
func (fn *OriginCombinerFn) AddInput(a OriginAccumulator, isDLQ bool) OriginAccumulator {
	a.TotalRead++
	if isDLQ {
		a.DLQ++
	} else {
		a.Valid++
	}
	return a
}

// MergeAccumulators combines counters from different bundles/workers for the same origin.
func (fn *OriginCombinerFn) MergeAccumulators(a, b OriginAccumulator) OriginAccumulator {
	return OriginAccumulator{
		TotalRead: a.TotalRead + b.TotalRead,
		Valid:     a.Valid + b.Valid,
		DLQ:       a.DLQ + b.DLQ,
	}
}

// ExtractOutput converts the accumulator into domain.OriginMetric.
func (fn *OriginCombinerFn) ExtractOutput(a OriginAccumulator) domain.OriginMetric {
	return domain.OriginMetric{
		TotalRead: a.TotalRead,
		Valid:     a.Valid,
		DLQ:       a.DLQ,
	}
}

// OriginSummary pairs an origin string with its finalized OriginMetric.
type OriginSummary struct {
	Origin string              `json:"origin"`
	Metric domain.OriginMetric `json:"metric"`
}

// MetricsSummaryCombinerFn merges per-origin summaries into the final singleton PipelineMetrics.
type MetricsSummaryCombinerFn struct{}

// CreateAccumulator initializes an empty PipelineMetrics accumulator.
func (fn *MetricsSummaryCombinerFn) CreateAccumulator() domain.PipelineMetrics {
	return domain.PipelineMetrics{
		OriginMetrics:      make(map[string]domain.OriginMetric),
		ColumnNullCounts:   make(map[string]int64),
		InvalidValueCounts: make(map[string]int64),
		CustomMetrics:      make(map[string]any),
	}
}

// AddInput folds an OriginSummary into the aggregate PipelineMetrics.
func (fn *MetricsSummaryCombinerFn) AddInput(a domain.PipelineMetrics, item OriginSummary) domain.PipelineMetrics {
	if a.OriginMetrics == nil {
		a.OriginMetrics = make(map[string]domain.OriginMetric)
	}
	a.TotalRecordsRead += item.Metric.TotalRead
	a.RowsWritten += item.Metric.Valid
	a.DeadLetterCount += item.Metric.DLQ
	a.OriginMetrics[item.Origin] = item.Metric
	return a
}

// MergeAccumulators combines partially aggregated PipelineMetrics from multiple workers.
func (fn *MetricsSummaryCombinerFn) MergeAccumulators(a, b domain.PipelineMetrics) domain.PipelineMetrics {
	res := domain.PipelineMetrics{
		TotalRecordsRead:   a.TotalRecordsRead + b.TotalRecordsRead,
		RowsWritten:        a.RowsWritten + b.RowsWritten,
		DeadLetterCount:    a.DeadLetterCount + b.DeadLetterCount,
		OriginMetrics:      make(map[string]domain.OriginMetric, len(a.OriginMetrics)+len(b.OriginMetrics)),
		ColumnNullCounts:   make(map[string]int64),
		InvalidValueCounts: make(map[string]int64),
		CustomMetrics:      make(map[string]any),
	}
	for k, v := range a.OriginMetrics {
		res.OriginMetrics[k] = v
	}
	for k, v := range b.OriginMetrics {
		res.OriginMetrics[k] = v
	}
	return res
}

// ExtractOutput returns the finalized PipelineMetrics accumulator.
func (fn *MetricsSummaryCombinerFn) ExtractOutput(a domain.PipelineMetrics) domain.PipelineMetrics {
	return a
}
